import {
  type Task,
  isValidTask,
  TaskName,
  type DownloadPayload,
  type ParsePayload,
} from "./types.js";
import { RedisDB } from "../db/index.js";
import { handleParseAndInsert, hanldeDownloadTask } from "./handlers.js";
import type { MysqlCommand } from "../db/mysql/commands.js";

export interface TaskConfig {
  mainQueue: string;
  holdQueue: string;
  primaryDoneKey: string;
  maxWorkers: number;
  maxRetry: number;
}

export class TaskRunner {
  private readonly maxWorkers: number;
  private readonly maxRetry: number;
  private readonly mainQueue: string;
  private readonly holdQueue: string;
  private readonly primaryDoneKey: string;
  private isRunning = false;

  private readonly batchId;
  private readonly redis;
  private readonly mysql;
  private workers: Promise<unknown>[] = [];

  constructor(redis: typeof RedisDB, mysql: MysqlCommand, config: TaskConfig) {
    this.redis = redis.getClient();
    this.mysql = mysql;
    this.batchId = crypto.randomUUID();

    this.maxWorkers = config.maxWorkers;
    this.maxRetry = config.maxRetry;
    this.mainQueue = `${config.mainQueue}:${this.batchId}`;
    this.holdQueue = `${config.holdQueue}:${this.batchId}`;
    this.primaryDoneKey = `${config.primaryDoneKey}:${this.batchId}`;
  }

  getBatchId() {
    return this.batchId;
  }

  async pushTask(task: Task) {
    await this.redis.lPush(this.mainQueue, JSON.stringify(task));
  }

  async start() {
    this.isRunning = true;

    while (this.isRunning) {
      if (this.workers.length >= this.maxWorkers) {
        await Promise.race(this.workers);
        continue;
      }

      try {
        if (!this.isRunning) break;

        const taskData = await this.redis.blPop(this.mainQueue, 1);

        if (!this.isRunning || !taskData) continue;

        const taskPromise = this.executeTask(taskData.element).finally(() => {
          this.workers = this.workers.filter((p) => p !== taskPromise);
        });

        this.workers.push(taskPromise);
      } catch (err) {
        if (!this.isRunning) break;
        console.error(`Queue Error: ${(err as Error).message}`);
      }
    }

    if (this.workers.length > 0) {
      console.log(`waiting for ${this.workers.length} workers to finish...`);
      await Promise.allSettled(this.workers);
    }

    console.log("task runner stopped safely");
  }

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
          const nextTask = await hanldeDownloadTask(
            this.batchId,
            task.taskId,
            task.payload as DownloadPayload,
            this.redis,
          ); // task 타입 가드에서 payload까지 체크해서 괜찮다

          const datasetType = nextTask.payload.datasetType;
          const isPrimary =
            datasetType == "TITLE_BASICS" || datasetType == "NAME_BASICS";

          if (isPrimary) {
            this.redis.rPush(this.mainQueue, JSON.stringify(nextTask));
          } else {
            this.redis.rPush(this.holdQueue, JSON.stringify(nextTask));
          }

          break;

        case TaskName.PARSE_PRIMARY:
          await handleParseAndInsert(
            this.batchId,
            task.taskId,
            task.payload as ParsePayload,
            this.mysql,
          );

          const completedCount = await this.redis.hIncrBy(
            `batch:${this.batchId}`,
            this.primaryDoneKey,
            1,
          );

          // TODO: change 2 to something else so that it can be dynamic
          if (completedCount == 2) {
            console.log("parse primary phase done, moving on to second phase");

            while (true) {
              const task = await this.redis.rPopLPush(
                this.holdQueue,
                this.mainQueue,
              );
              if (!task) break;
            }

            await this.redis.del(this.holdQueue);
          }

          break;

        case TaskName.PARSE_SECONDARY:
          await handleParseAndInsert(
            this.batchId,
            task.taskId,
            task.payload as ParsePayload,
            this.mysql,
          );

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
