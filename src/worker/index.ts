import path from "node:path";
import type { Tconfig } from "../config/index.js";
import type { RedisDatabase } from "../db/redis.js";
import type { MysqlCommand } from "../db/mysql/commands.js";
import { TaskProducer } from "./producer.js";
import { TaskConsumer, type TaskResult } from "./consumer.js";
import { TaskName, type ParsePayload, type Task } from "./types.js";
import { isPrimary } from "../utils/helpers.js";
import type { DatasetKey } from "../utils/types.js";

interface PipelineConfig {
  redis: RedisDatabase;
  mysqlCommand: MysqlCommand;
  config: Tconfig;
  skipDownload?: boolean;
}

export const runPipeline = async ({
  redis,
  mysqlCommand,
  config,
  skipDownload = false,
}: PipelineConfig) => {
  const batchId = crypto.randomUUID();
  const redisClient = redis.getClient();
  let totalInserted = 0;

  const holdQueue = `${config.task.holdQueue}:${batchId}`;
  const mainQueue = `${config.task.mainQueue}:${batchId}`;
  const primaryDoneKey = `${config.task.primaryDoneKey}:${batchId}`;
  const primaryCount = config.task.primaryConfig;

  const producer = new TaskProducer(
    redis,
    config.task,
    config.datasets,
    batchId,
  );

  const consumer = new TaskConsumer(redis, batchId, mysqlCommand, config.task);

  let primaryCompleted = 0;
  let primaryPhaseDone = false;

  const releaseSecondaryTasks = async () => {
    console.log("[pipeline] primary phase done, releasing secondary tasks");
    primaryPhaseDone = true;

    while (true) {
      const task = await redisClient.rPopLPush(holdQueue, mainQueue);
      if (!task) break;
    }

    await redisClient.del(holdQueue);
  };

  const onTaskDone = async (result: TaskResult) => {
    switch (result.taskName) {
      case TaskName.DOWNLOAD: {
        const nextTask = result.payload as Task<ParsePayload>;
        const { datasetType, filePath } = nextTask.payload;

        if (isPrimary(datasetType)) {
          await producer.producePhase1Task(path.basename(filePath));
          primaryCompleted++;

          if (primaryCompleted >= primaryCount) {
            await releaseSecondaryTasks();
          }
        } else if (primaryPhaseDone) {
          await producer.queueSecondary(datasetType, path.basename(filePath));
        } else {
          await producer.hold(datasetType, path.basename(filePath));
        }
        break;
      }

      case TaskName.INSERT_DATA:
      case TaskName.PARSE_PRIMARY: {
        const insertResult = result.payload as {
          success: boolean;
          count: number;
          datasetType?: DatasetKey;
        };
        totalInserted += insertResult.count;

        if (totalInserted % 100000 === 0 && totalInserted > 0) {
          console.log(
            `[pipeline] progress: ${totalInserted.toLocaleString()} rows inserted`,
          );
        }
        break;
      }
    }
  };

  console.log(`[pipeline] starting batch ${batchId}`);
  console.time("pipeline");

  try {
    if (skipDownload) {
      // 다운로드 건너뛰고 기존 파일로 바로 파싱 태스크 등록
      for (const file of config.datasets.files) {
        const fileName = file.name.replace(".gz", "");
        const datasetType = file.type;

        if (isPrimary(datasetType)) {
          await producer.producePhase1Task(fileName);
          primaryCompleted++;
        } else {
          await producer.hold(datasetType, fileName);
        }
      }

      // primary 전부 등록 완료 → secondary 릴리즈
      if (primaryCompleted >= primaryCount) {
        await releaseSecondaryTasks();
      }
    } else {
      // 다운로드 태스크 등록 (onTaskDone에서 primary 완료 후 릴리즈)
      for (const file of config.datasets.files) {
        await producer.produceDownloadTask(
          config.datasets.baseUrl + file.name,
          path.join(config.datasets.downloadDir, file.name),
        );
      }
    }

    // 2. Consumer 시작 (다운로드 + 파싱 + INSERT 처리)
    const consumerPromise = consumer.start(onTaskDone);

    // 3. 큐가 비워질 때까지 대기 후 종료
    const waitForCompletion = async () => {
      while (true) {
        await new Promise((res) => setTimeout(res, 2000));

        const mainLen = await redisClient.lLen(mainQueue);
        const holdLen = await redisClient.lLen(holdQueue);

        if (mainLen === 0 && holdLen === 0 && consumer.activeCount === 0) {
          await new Promise((res) => setTimeout(res, 3000));
          const finalLen = await redisClient.lLen(mainQueue);
          if (finalLen === 0 && consumer.activeCount === 0) {
            consumer.stop();
            break;
          }
        }
      }
    };

    await Promise.all([consumerPromise, waitForCompletion()]);

    // 정리
    await redisClient.del(mainQueue);
    await redisClient.del(holdQueue);
    await redisClient.del(primaryDoneKey);

    console.log(
      `[pipeline] completed. Total ${totalInserted.toLocaleString()} rows inserted.`,
    );
  } catch (err) {
    consumer.stop();
    console.error(`[pipeline] error: ${(err as Error).message}`);
    throw err;
  } finally {
    console.timeEnd("pipeline");
  }

  return { totalInserted, batchId };
};
