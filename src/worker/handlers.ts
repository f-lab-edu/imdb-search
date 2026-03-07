import path from "node:path";
import type { MysqlCommand } from "../db/index.js";
import type { RedisDatabase } from "../db/redis.js";
import { downloadFile } from "../utils/download.js";
import { getDatasetInfoByFileName } from "../utils/helpers.js";
import { generateTSVlines } from "../utils/parse.js";
import type { DatasetType } from "../utils/types.js";
import type { Producer } from "./producer.js";
import {
  TaskName,
  TaskPhase,
  type DownloadPayload,
  type InsertPayload,
  type ParsePayload,
  type Task,
  type TaskName as TaskNameType,
} from "./types.js";

export type TaskHandler = (task: Task) => Promise<void>;
export type TaskHandlerMap = Partial<Record<TaskNameType, TaskHandler>>;

export const createHandlers = (
  redis: RedisDatabase,
  mysqlCmd: MysqlCommand,
  producer: Producer,
  batchSize: number
): TaskHandlerMap => {
  const redisClient = redis.getClient();

  return {
    [TaskName.DOWNLOAD]: async (task: Task) => {
      const { url, targetPath } = task.payload as DownloadPayload;
      const { outPath, hash } = await downloadFile(targetPath, url);
      const info = getDatasetInfoByFileName(path.basename(url));

      if (!info) {
        throw new Error(`unknown dataset file: ${path.basename(url)}`);
      }

      const hashKey = `file_hash:${path.basename(targetPath)}`;
      const oldHash = await redisClient.get(hashKey);
      await redisClient.set(hashKey, hash);

      const skip = oldHash === hash;
      if (skip) {
        console.log(`[download] ${path.basename(outPath)}: hash match, skipping parse`);
      } else {
        console.log(`[download] ${path.basename(outPath)}: done`);
      }

      const phase = info.isPrimary ? TaskPhase.PRIMARY : TaskPhase.SECONDARY;
      await producer.produce(
        {
          batchId: task.batchId,
          taskId: crypto.randomUUID(),
          name: info.isPrimary ? TaskName.PARSE_PRIMARY : TaskName.PARSE_SECONDARY,
          payload: { filePath: outPath, datasetType: info.type, skip } satisfies ParsePayload,
          retryCount: 0,
          createdAt: Date.now(),
        },
        phase
      );
    },

    [TaskName.PARSE_PRIMARY]: async (task: Task) => {
      await handleParse(task, TaskPhase.PRIMARY, producer, batchSize);
    },

    [TaskName.PARSE_SECONDARY]: async (task: Task) => {
      await handleParse(task, TaskPhase.SECONDARY, producer, batchSize);
    },

    [TaskName.INSERT_DATA]: async (task: Task) => {
      const { datasetType, data } = task.payload as InsertPayload;
      if (!data || data.length === 0) return;
      await insertByDatasetType(mysqlCmd, datasetType, data as DatasetType[]);
    },
  };
};

const handleParse = async (task: Task, phase: TaskPhase, producer: Producer, batchSize: number) => {
  const { filePath, datasetType, skip } = task.payload as ParsePayload;
  if (skip) return;

  const batch: DatasetType[] = [];

  for await (const row of generateTSVlines<DatasetType>(filePath)) {
    batch.push(row);

    if (batch.length >= batchSize) {
      await producer.produce(
        {
          batchId: task.batchId,
          taskId: crypto.randomUUID(),
          name: TaskName.INSERT_DATA,
          payload: { datasetType, data: batch.splice(0, batchSize) } satisfies InsertPayload,
          retryCount: 0,
          createdAt: Date.now(),
        },
        phase
      );
    }
  }

  if (batch.length > 0) {
    await producer.produce(
      {
        batchId: task.batchId,
        taskId: crypto.randomUUID(),
        name: TaskName.INSERT_DATA,
        payload: { datasetType, data: batch } satisfies InsertPayload,
        retryCount: 0,
        createdAt: Date.now(),
      },
      phase
    );
  }
};

const insertByDatasetType = async (
  mysqlCmd: MysqlCommand,
  datasetType: string,
  data: DatasetType[]
) => {
  switch (datasetType) {
    case "TITLE_BASICS":
      return mysqlCmd.insertTitleBasics(data as any);
    case "NAME_BASICS":
      return mysqlCmd.insertNameBasics(data as any);
    case "TITLE_AKAS":
      return mysqlCmd.insertTitleAkas(data as any);
    case "TITLE_CREW":
      return mysqlCmd.insertTitleCrew(data as any);
    case "TITLE_EPISODE":
      return mysqlCmd.insertTitleEpisodes(data as any);
    case "TITLE_PRINCIPAL":
      return mysqlCmd.insertTitlePrincipals(data as any);
    case "TITLE_RATINGS":
      return mysqlCmd.insertTitleRatings(data as any);
    default:
      throw new Error(`unknown datasetType: ${datasetType}`);
  }
};
