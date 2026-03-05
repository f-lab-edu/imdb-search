import path from "node:path";
import type { Tconfig } from "../config/index.js";
import { generateTSVlines } from "../utils/parse.js";
import type {
  DatasetType,
  TitleBasics,
  NameBasics,
  DatasetKey,
} from "../utils/types.js";
import { TaskName, type InsertPayload, type Task } from "./types.js";
import type { RedisDatabase } from "../db/redis.js";

/*
 * 1. 원본 TSV 파일 읽고 작업 생성 후 큐에 등록(mysql 입력용)
 * 2. mysql 데이터 읽고 작업 생성 후 큐에 등록 (오픈서치 입력용)
 */
export class TaskProducer {
  private readonly redisClient;

  private readonly mainQueue;
  private readonly holdQueue;
  private readonly batchSize = 1000;

  constructor(
    private readonly redis: RedisDatabase,
    private readonly taskCfg: Tconfig["task"],
    private readonly datasetCfg: Tconfig["datasets"],
    private readonly batchId: string,
  ) {
    this.mainQueue = `${this.taskCfg.mainQueue}:${this.batchId}`;
    this.holdQueue = `${this.taskCfg.holdQueue}:${this.batchId}`;
    this.redisClient = this.redis.getClient();
  }

  async produceDownloadTask(url: string, targetPath: string) {
    const task = {
      batchId: this.batchId,
      taskId: crypto.randomUUID(),
      name: TaskName.DOWNLOAD,
      payload: {
        url,
        targetPath,
      },
      retryCount: 0,
      createdAt: Date.now(),
    };

    await this.redisClient.rPush(this.mainQueue, JSON.stringify(task));

    console.log(`[producer] download task created: ${targetPath}`);
  }

  // phase1 task = store data from tsv into main db (mysql)
  async producePhase1Task<T extends TitleBasics | NameBasics>(
    fileName: string,
  ) {
    const batch: T[] = [];

    const filePath = path.join(this.datasetCfg.downloadDir, fileName);
    const datasetType = filePath.includes("title.basics")
      ? "TITLE_BASICS"
      : "NAME_BASICS";

    try {
      for await (const lineData of generateTSVlines<T>(filePath)) {
        try {
          batch.push(lineData);

          if (batch.length >= this.batchSize) {
            const currentBatch = [...batch];
            batch.length = 0;
            // create task and add it to redis queue
            const task: Task<InsertPayload<T>> = this.getInsertTaskFromBatch(
              datasetType,
              currentBatch,
            );

            await this.redisClient
              .rPush(this.mainQueue, JSON.stringify(task))
              .catch((err) =>
                console.error(
                  `[redis error] failed to push batch: ${(err as Error).message}`,
                ),
              );
          }
        } catch (lineErr) {
          console.error(
            `[parsing error] skipping line in ${fileName}:`,
            lineErr,
          );
          continue;
        }
      }

      if (batch.length > 0) {
        const task: Task<InsertPayload<T>> = this.getInsertTaskFromBatch(
          datasetType,
          batch,
        );

        await this.redisClient
          .rPush(this.mainQueue, JSON.stringify(task))
          .catch((err) =>
            console.error(
              `[redis error] failed to push batch: ${(err as Error).message}`,
            ),
          );
      }
    } catch (err) {
      console.error(`[parsing error] phase 1 failed for ${fileName}:`, err);
    }
  }

  async hold(datasetType: DatasetKey, filePath: string) {
    await this.queueSecondary(datasetType, filePath, this.holdQueue);
    console.log(`[producer] deferred task held: ${path.basename(filePath)}`);
  }

  async queueSecondary(datasetType: DatasetKey, filePath: string, queue = this.mainQueue) {
    const task = {
      batchId: this.batchId,
      taskId: crypto.randomUUID(),
      name: TaskName.PARSE_SECONDARY,
      payload: {
        datasetType,
        filePath: path.join(this.datasetCfg.downloadDir, filePath),
      },
      retryCount: 0,
      createdAt: Date.now(),
    };

    try {
      await this.redisClient.rPush(queue, JSON.stringify(task));
    } catch (err) {
      console.error(
        `[redis error] failed to queue secondary task: ${(err as Error).message}`,
      );
    }
  }

  private getInsertTaskFromBatch<T extends DatasetType>(
    datasetType: DatasetKey,
    batch: T[],
  ): Task<InsertPayload<T>> {
    return {
      batchId: this.batchId,
      taskId: crypto.randomUUID(),
      name: TaskName.INSERT_DATA,
      payload: {
        datasetType: datasetType,
        data: batch,
      },
      retryCount: 0,
      createdAt: Date.now(),
    };
  }
}
