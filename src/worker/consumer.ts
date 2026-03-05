import fs from "node:fs/promises";
import path from "node:path";
import type { MysqlCommand } from "../db/mysql/commands.js";
import type { RedisDatabase } from "../db/redis.js";
import {
  isValidTask,
  type Task,
  TaskName,
  type DownloadPayload,
} from "./types.js";
import {
  handleDownloadTask,
  handleParseTask,
  handleSecondaryParseTask,
} from "./handlers.js";
import type { ParsePayload } from "./types.js";

export interface TaskConfig {
  mainQueue: string;
  holdQueue: string;
  primaryDoneKey: string;
  maxWorkers: number;
  maxRetry: number;
}

export interface TaskResult {
  taskName: TaskName;
  payload: unknown;
}

export class TaskConsumer {
  private readonly redis;
  private readonly mysqlCommand;

  private readonly maxWorkers: number;
  private readonly mainQueue: string;
  private readonly maxRetry: number;
  private isRunning = false;

  private readonly batchId;
  private workers: Promise<unknown>[] = [];

  constructor(
    redis: RedisDatabase,
    batchId: string,
    mysqlCommand: MysqlCommand,
    config: TaskConfig,
  ) {
    this.redis = redis.getClient();
    this.mysqlCommand = mysqlCommand;

    this.batchId = batchId;

    this.maxWorkers = config.maxWorkers;
    this.maxRetry = config.maxRetry;
    this.mainQueue = `${config.mainQueue}:${this.batchId}`;
  }

  async start(onTaskDone?: (result: TaskResult) => void) {
    this.isRunning = true;
    console.log(`[Consumer] started with maxWorkers: ${this.maxWorkers}`);

    while (this.isRunning) {
      const availableSlots = this.maxWorkers - this.workers.length;

      if (availableSlots <= 0) {
        await Promise.race(this.workers);
        continue;
      }

      try {
        if (!this.isRunning) break;

        const taskElements = await this.redis.lPopCount(
          this.mainQueue,
          availableSlots,
        );

        if (!taskElements || taskElements.length === 0) {
          const idleData = await this.redis.blPop(this.mainQueue, 1);
          if (!idleData) continue;
          taskElements?.push(idleData.element);
        }

        for (const element of taskElements || []) {
          const taskPromise = this.executeTask(element, onTaskDone).finally(
            () => {
              this.workers = this.workers.filter((p) => p !== taskPromise);
            },
          );
          this.workers.push(taskPromise);
        }
      } catch (err) {
        if (!this.isRunning) break;
        console.error(`Queue Error: ${(err as Error).message}`);
        await new Promise((res) => setTimeout(res, 1000));
      }
    }

    if (this.workers.length > 0) {
      console.log(`[consumer] Finalizing ${this.workers.length} tasks...`);
      await Promise.allSettled(this.workers);
    }

    console.log("[consumer] Task consumer stopped safely.");
  }

  stop() {
    this.isRunning = false;
  }

  get activeCount() {
    return this.workers.length;
  }

  async executeTask(
    taskElement: string,
    onTaskDone?: (result: TaskResult) => void,
  ) {
    let task: Task | null = null;

    try {
      task = JSON.parse(taskElement);

      if (!isValidTask(task)) {
        console.error(`invalid task data: ${taskElement}`);
        return;
      }

      switch (task.name) {
        case TaskName.DOWNLOAD:
          console.log(`[consumer] received download task: ${task.taskId}`);

          const result = await handleDownloadTask(
            this.batchId,
            task.taskId,
            task.payload as DownloadPayload,
            this.redis,
          );

          onTaskDone?.({ taskName: TaskName.DOWNLOAD, payload: result });
          break;

        case TaskName.INSERT_DATA:
        case TaskName.PARSE_PRIMARY:
          console.log(`[consumer] processing ${task.name}: ${task.taskId}`);

          const parseResult = await handleParseTask(
            this.batchId,
            task,
            this.mysqlCommand,
          );

          onTaskDone?.({ taskName: task.name, payload: parseResult });
          break;

        case TaskName.PARSE_SECONDARY:
          console.log(`[consumer] processing ${task.name}: ${task.taskId}`);

          const secondaryResult = await handleSecondaryParseTask(
            task as Task<ParsePayload>,
            this.mainQueue,
            this.redis,
          );

          onTaskDone?.({ taskName: task.name, payload: secondaryResult });
          break;

        default:
          console.warn(`unknown task name: ${task.name}`);
      }
    } catch (err) {
      console.error(`Task Error: ${(err as Error).message}`);

      if (task && task.retryCount <= this.maxRetry) {
        task.retryCount++;
        await this.redis.rPush(this.mainQueue, JSON.stringify(task));
      } else {
        console.error(`task failed with max retry: ${task?.taskId}`);
        const logPath = path.join(process.cwd(), "failed-tasks.jsonl");
        await fs.appendFile(
          logPath,
          JSON.stringify({
            task,
            error: (err as Error).message,
            failedAt: new Date().toISOString(),
          }) + "\n",
        );
      }
    }
  }
}
