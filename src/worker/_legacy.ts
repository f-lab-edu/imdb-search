/**
 * worker_threads 버전으로 전환하면서 백업
 * 성능 비교용으로 보존
 */

import {
  type Task,
  isValidTask,
  TaskName,
  type DownloadPayload,
  type ParsePayload,
} from "./types.js";
import type { RedisDatabase } from "../db/redis.js";
import { handleDownloadTask } from "./handlers.js";
import type { MysqlCommand } from "../db/mysql/commands.js";
import { isPrimary } from "../utils/helpers.js";
import { generateTSVlines } from "../utils/parse.js";
import type {
  DatasetKey,
  DatasetType,
  NameBasics,
  TitleAkas,
  TitleBasics,
  TitleCrew,
  TitleEpisode,
  TitlePrincipals,
  TitleRatings,
} from "../utils/types.js";

interface TaskConfig {
  mainQueue: string;
  holdQueue: string;
  primaryDoneKey: string;
  maxWorkers: number;
  maxRetry: number;
  primaryCount: number;
  batchSize: number;
}

export class TaskRunner {
  private readonly maxWorkers: number;
  private readonly maxRetry: number;
  private readonly mainQueue: string;
  private readonly holdQueue: string;
  private readonly primaryDoneKey: string;
  private readonly primaryCount: number;
  private readonly batchSize: number;
  private isRunning = false;

  private readonly batchId;
  private readonly redis;
  private readonly mysql;
  private workers: Promise<unknown>[] = [];

  constructor(redis: RedisDatabase, mysql: MysqlCommand, config: TaskConfig) {
    this.redis = redis.getClient();
    this.mysql = mysql;
    this.batchId = crypto.randomUUID();

    this.maxWorkers = config.maxWorkers;
    this.maxRetry = config.maxRetry;
    this.mainQueue = `${config.mainQueue}:${this.batchId}`;
    this.holdQueue = `${config.holdQueue}:${this.batchId}`;
    this.primaryDoneKey = `${config.primaryDoneKey}:${this.batchId}`;
    this.primaryCount = config.primaryCount;
    this.batchSize = config.batchSize;
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
        return;
      }

      switch (task.name) {
        case TaskName.DOWNLOAD:
          const nextTask = await handleDownloadTask(
            this.batchId,
            task.taskId,
            task.payload as DownloadPayload,
            this.redis,
          );

          const datasetType = nextTask.payload.datasetType;
          const primary = isPrimary(datasetType);

          if (primary) {
            await this.redis.rPush(this.mainQueue, JSON.stringify(nextTask));
          } else {
            await this.redis.rPush(this.holdQueue, JSON.stringify(nextTask));
          }

          break;

        case TaskName.PARSE_PRIMARY:
          await handleParseAndInsert(
            this.batchId,
            task.taskId,
            task.payload as ParsePayload,
            this.mysql,
            this.batchSize,
          );

          const completedCount = await this.redis.hIncrBy(
            `batch:${this.batchId}`,
            this.primaryDoneKey,
            1,
          );

          if (completedCount === this.primaryCount) {
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
            this.batchSize,
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
      }
    }
  }
}

export const handleParseAndInsert = async (
  batchId: string,
  taskId: string,
  payload: ParsePayload,
  mysqlCmd: MysqlCommand,
  batchSize: number,
) => {
  if (payload.skip) {
    console.log(`skipping insert: ${payload.datasetType}`);
    return;
  }

  let totalCount = 0;
  let buffer: DatasetType[] = [];

  try {
    for await (const row of generateTSVlines(payload.filePath)) {
      buffer.push(row);
      if (buffer.length >= batchSize) {
        await insertByDatasetType(mysqlCmd, payload.datasetType, buffer);
        totalCount += buffer.length;
        if (totalCount % 100000 === 0) {
          console.log(
            `${payload.datasetType}: ${totalCount.toLocaleString()} rows inserted...`,
          );
        }
        buffer = [];
      }
    }

    if (buffer.length > 0) {
      await insertByDatasetType(mysqlCmd, payload.datasetType, buffer);
      totalCount += buffer.length;
      buffer = [];
    }

    console.log(
      `${payload.datasetType}: Total ${totalCount.toLocaleString()} rows inserted.`,
    );
  } catch (err) {
    console.error(`[Error] ${payload.datasetType} insertion failed:`, err);
    throw err;
  }
};

const insertByDatasetType = async (
  mysqlCmd: MysqlCommand,
  key: DatasetKey,
  data: DatasetType[],
) => {
  switch (key) {
    case "TITLE_BASICS":
      return await mysqlCmd.insertTitleBasics(data as TitleBasics[]);
    case "NAME_BASICS":
      return await mysqlCmd.insertNameBasics(data as NameBasics[]);
    case "TITLE_AKAS":
      return await mysqlCmd.insertTitleAkas(data as TitleAkas[]);
    case "TITLE_CREW":
      return await mysqlCmd.insertTitleCrew(data as TitleCrew[]);
    case "TITLE_EPISODE":
      return await mysqlCmd.insertTitleEpisodes(data as TitleEpisode[]);
    case "TITLE_PRINCIPAL":
      return await mysqlCmd.insertTitlePrincipals(data as TitlePrincipals[]);
    case "TITLE_RATINGS":
      return await mysqlCmd.insertTitleRatings(data as TitleRatings[]);
    default:
      throw new Error(`received invalid dataset key: ${key}`);
  }
};

// consumer legacy

import { parentPort } from "node:worker_threads";
// import type { MysqlCommand } from "../db/mysql/commands.js";
// import type { RedisDatabase } from "../db/redis.js";
// import {
//   isValidTask,
//   type Task,
//   TaskName,
//   type DownloadPayload,
// } from "./types.js";
import { handleParseTask } from "./handlers.js";

interface TaskConfig {
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
    config: TaskConfig,
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

        const taskElements = await this.redis.lPopCount(
          this.mainQueue,
          availableSlots,
        );

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
            this.redis,
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

          const parseResult = await handleParseTask(
            this.batchId,
            task,
            this.mysqlCommand,
          );

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

// messages

export enum ConsumerCommand {
  START = "START",
  STOP = "STOP",
  TERMINATE = "TERMINATE",
}

export enum ProducerCommand {
  PRODUCE_DOWNLOAD = "PRODUCE_DOWNLOAD",
  PRODUCE_PARSE_PRIMARY = "PRODUCE_PARSE_PRIMARY",
  PRODUCE_PARSE_SECONDARY = "PRODUCE_PARSE_SECONDARY",
  SHUTDOWN = "SHUTDOWN",
}

export enum WorkerResponseType {
  READY = "READY", // 워커 준비 완료 (START_OK 대신)
  PROGRESS = "PROGRESS", // 작업 중 보고 (예: 다운로드 50% 완료)
  DONE = "DONE", // 단일 작업 완료
  ERROR = "ERROR", // 에러 발생
}

export interface MainRequestPayload<T = unknown> {
  type: ConsumerCommand | ProducerCommand; // START, PRODUCE_PARSE 등
  payload?: T; // 전달할 데이터 (filePath, isPrimary 등)
}

export interface WorkerResponsePayload<T = unknown> {
  type: WorkerResponseType;
  workerType: "PRODUCER" | "CONSUMER";
  command?: string; // 어떤 명령에 대한 응답인지
  payload?: T; // 결과 데이터 (fileName, isModified 등)
  error?: string;
}
