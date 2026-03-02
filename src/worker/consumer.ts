import { parentPort } from "node:worker_threads";
import type { MysqlCommand } from "../db/mysql/commands.js";
import type { RedisDatabase } from "../db/redis.js";
import { isValidTask, type Task, TaskName, type DownloadPayload } from "./types.js";
import { ConsumerCommand, WorkerResponseType } from "./messages.js";
import { handleDownloadTask, handleParseTask } from "./handlers.js";

export interface TaskConfig {
  mainQueue: string;
  holdQueue: string;
  primaryDoneKey: string;
  maxWorkers: number;
  maxRetry: number;
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
    config: TaskConfig
  ) {
    this.redis = redis.getClient();
    this.mysqlCommand = mysqlCommand;

    this.batchId = batchId;

    this.maxWorkers = config.maxWorkers;
    this.maxRetry = config.maxRetry;
    this.mainQueue = `${config.mainQueue}:${this.batchId}`;
  }

  async start() {
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

        const taskElements = await this.redis.lPopCount(this.mainQueue, availableSlots);

        if (!taskElements || taskElements.length === 0) {
          const idleData = await this.redis.blPop(this.mainQueue, 1);
          if (!idleData) continue; // 1초 대기 후 다시 루프
          taskElements?.push(idleData.element);
        }

        for (const element of taskElements || []) {
          const taskPromise = this.executeTask(element).finally(() => {
            this.workers = this.workers.filter((p) => p !== taskPromise);
          });
          this.workers.push(taskPromise);
        }
      } catch (err) {
        if (!this.isRunning) break;
        console.error(`Queue Error: ${(err as Error).message}`);
        await new Promise((res) => setTimeout(res, 1000));
      }
    }

    if (this.workers.length > 0) {
      console.log(`[Consumer] Finalizing ${this.workers.length} tasks...`);
      await Promise.allSettled(this.workers);
    }

    console.log("Task runner stopped safely.");
  }

  // async start() {
  //   this.isRunning = true;
  //
  //   while (this.isRunning) {
  //     if (this.workers.length >= this.maxWorkers) {
  //       await Promise.race(this.workers);
  //       continue;
  //     }
  //
  //     try {
  //       if (!this.isRunning) break;
  //
  //       const taskData = await this.redis.blPop(this.mainQueue, 1);
  //
  //       if (!this.isRunning || !taskData) continue;
  //
  //       const taskPromise = this.executeTask(taskData.element).finally(() => {
  //         this.workers = this.workers.filter((p) => p !== taskPromise);
  //       });
  //
  //       this.workers.push(taskPromise);
  //     } catch (err) {
  //       if (!this.isRunning) break;
  //       console.error(`Queue Error: ${(err as Error).message}`);
  //     }
  //   }
  //
  //   if (this.workers.length > 0) {
  //     console.log(`waiting for ${this.workers.length} workers to finish...`);
  //     await Promise.allSettled(this.workers);
  //   }
  //
  //   console.log("task runner stopped safely");
  // }

  stop() {
    this.isRunning = false;
  }

  async executeTask(taskElement: string) {
    let task: Task | null = null;

    try {
      task = JSON.parse(taskElement);

      if (!isValidTask(task)) {
        console.error(`invalid task data: ${taskElement}`);
        return; // throw error?
      }

      switch (task.name) {
        case TaskName.DOWNLOAD:
          console.log(`[consumer] received download task: ${task.taskId}`);

          const result = await handleDownloadTask(
            this.batchId,
            task.taskId,
            task.payload as DownloadPayload,
            this.redis
          );

          if (parentPort) {
            parentPort.postMessage({
              type: WorkerResponseType.DONE,
              workerType: "CONSUMER",
              command: TaskName.DOWNLOAD,
              payload: result,
            });
          }

          break;

        case TaskName.INSERT_DATA:
        case TaskName.PARSE_PRIMARY:
        case TaskName.PARSE_SECONDARY:
          console.log(`[consumer] processing ${task.name}: ${task.taskId}`);

          const parseResult = await handleParseTask(this.batchId, task, this.mysqlCommand);

          if (parentPort) {
            parentPort.postMessage({
              type: WorkerResponseType.DONE,
              workerType: "CONSUMER",
              command: task.name,
              payload: parseResult, // { count: 1000, datasetType: "..." }
            });
          }

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
        console.error(`task failed with max retry`);
        // TODO: 따로 실패 기록 저장해두고 나중에 다시 하던지 하기
      }
    }
  }
}
