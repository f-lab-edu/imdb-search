import path from "node:path";
import type { DownloadPayload, ParsePayload, Task } from "./types.js";
import { TaskName } from "./types.js";
import { downloadFile } from "../utils/download.js";
import { generateTSVlines } from "../utils/parse.js";
import { getDatasetInfoByFileName } from "../utils/helpers.js";
import { createClient } from "redis";
import type { MysqlCommand } from "../db/mysql/commands.js";
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

export const handleDownloadTask = async (
  batchId: string,
  taskId: string,
  payload: DownloadPayload,
  redisClient: ReturnType<typeof createClient>,
): Promise<Task<ParsePayload>> => {
  const { outPath, hash } = await downloadFile(payload.targetPath, payload.url);
  const info = getDatasetInfoByFileName(path.basename(payload.targetPath));

  console.log(`downloading ${path.basename(outPath)}`);

  if (!info) {
    throw new Error(
      `unknown dataset file: ${path.basename(payload.targetPath)}, check your dataset config`,
    );
  }

  const hashKey = `file_hash:${path.basename(payload.targetPath)}`;
  const oldHash = await redisClient.get(hashKey);

  if (oldHash === hash) {
    console.log(`${path.basename(outPath)}: hash match: skip enabled`);
  } else {
    console.log(
      `${path.basename(outPath)}: New file or hash changed. Parsing required.`,
    );
  }

  await redisClient.set(hashKey, hash);

  console.log(`successfully downloaded ${outPath}`);

  return {
    batchId: batchId,
    taskId: `${batchId}:parse_${taskId}`,
    name: info.isPrimary ? TaskName.PARSE_PRIMARY : TaskName.PARSE_SECONDARY,
    payload: {
      filePath: outPath,
      datasetType: info.type,
      skip: oldHash === hash,
    },
    retryCount: 0,
    createdAt: Date.now(),
  };
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

export const handleSecondaryParseTask = async (
  task: Task<ParsePayload>,
  mainQueue: string,
  redisClient: ReturnType<typeof createClient>,
) => {
  const { datasetType, filePath, skip } = task.payload;

  if (skip) {
    return { success: true, count: 0, skipped: true };
  }

  const batchSize = 1000;
  const batch: DatasetType[] = [];
  let totalQueued = 0;

  try {
    for await (const row of generateTSVlines<DatasetType>(filePath)) {
      batch.push(row);

      if (batch.length >= batchSize) {
        const current = batch.splice(0, batchSize);
        const insertTask: Task<{ datasetType: DatasetKey; data: DatasetType[] }> = {
          batchId: task.batchId,
          taskId: crypto.randomUUID(),
          name: TaskName.INSERT_DATA,
          payload: { datasetType, data: current },
          retryCount: 0,
          createdAt: Date.now(),
        };
        await redisClient.rPush(mainQueue, JSON.stringify(insertTask));
        totalQueued += current.length;
      }
    }

    if (batch.length > 0) {
      const insertTask: Task<{ datasetType: DatasetKey; data: DatasetType[] }> = {
        batchId: task.batchId,
        taskId: crypto.randomUUID(),
        name: TaskName.INSERT_DATA,
        payload: { datasetType, data: batch },
        retryCount: 0,
        createdAt: Date.now(),
      };
      await redisClient.rPush(mainQueue, JSON.stringify(insertTask));
      totalQueued += batch.length;
    }
  } catch (err) {
    console.error(`[Parse Error] ${datasetType}: ${(err as Error).message}`);
    throw err;
  }

  return { success: true, count: totalQueued, datasetType };
};

export const handleParseTask = async (
  batchId: string,
  task: Task<any>,
  mysqlCmd: MysqlCommand,
) => {
  const { datasetType, data, skip } = task.payload;

  if (skip) {
    return { success: true, count: 0, skipped: true };
  }

  if (!data || !Array.isArray(data) || data.length === 0) {
    return { success: true, count: 0 };
  }

  try {
    await insertByDatasetType(mysqlCmd, datasetType, data);

    return {
      success: true,
      count: data.length,
      datasetType,
    };
  } catch (err) {
    console.error(`[Insert Error] ${datasetType}: ${(err as Error).message}`);
    throw err;
  }
};
