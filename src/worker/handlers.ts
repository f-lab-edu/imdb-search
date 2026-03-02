import path from "node:path";
import type { DownloadPayload, ParsePayload, Task } from "./types.js";
import { TaskName } from "./types.js";
import { downloadFile } from "../utils/download.js";
import { getDatasetInfoByFileName } from "../utils/helpers.js";
import { createClient } from "redis";
import type { MysqlCommand } from "../db/mysql/commands.js";
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

export const handleDownloadTask = async (
  batchId: string,
  taskId: string,
  payload: DownloadPayload,
  redisClient: ReturnType<typeof createClient>
): Promise<Task<ParsePayload>> => {
  const { outPath, hash } = await downloadFile(payload.targetPath, payload.url);
  const info = getDatasetInfoByFileName(path.basename(payload.targetPath));

  console.log(`downloading ${path.basename(outPath)}`);

  if (!info) {
    throw new Error(
      `unknown dataset file: ${path.basename(payload.targetPath)}, check your dataset config`
    );
  }

  const hashKey = `file_hash:${path.basename(payload.targetPath)}`;

  const oldHash = await redisClient.get(hashKey);

  if (oldHash === hash) {
    console.log(`${path.basename(outPath)}: hash match: skip enabled`);
  } else {
    console.log(`${path.basename(outPath)}: New file or hash changed. Parsing required.`);
  }

  await redisClient.set(hashKey, hash);

  console.log(`successfully downloaded ${outPath}`);

  return {
    batchId: batchId,
    taskId: `${batchId}:parse_${taskId}`, // need to create unique id
    name: info.isPrimary ? TaskName.PARSE_PRIMARY : TaskName.PARSE_SECONDARY,
    payload: {
      filePath: outPath,
      datasetType: info.type,
      skip: oldHash == hash,
    },
    retryCount: 0,
    createdAt: Date.now(),
  };
};

// TODO: this need to be chained to search db insert
export const handleParseAndInsert = async (
  batchId: string,
  taskId: string,
  payload: ParsePayload,
  mysqlCmd: MysqlCommand
) => {
  if (payload.skip) {
    console.log(`skipping insert: ${payload.datasetType}`);
    return;
  }

  const batchSize = 1000; // TODO: get this from config

  let totalCount = 0;
  let buffer = [];

  for await (const row of generateTSVlines(payload.filePath)) {
    buffer.push(row);

    if (buffer.length >= batchSize) {
      await insertByDatasetType(mysqlCmd, payload.datasetType, buffer);
      totalCount += buffer.length;

      if (totalCount % 100000 === 0) {
        console.log(`${payload.datasetType}: ${totalCount.toLocaleString()} rows inserted...`);
      }

      buffer = [];
    }
  }

  if (buffer.length > 0) {
    await insertByDatasetType(mysqlCmd, payload.datasetType, buffer);
  }

  console.log(`${payload.datasetType}: Total ${totalCount.toLocaleString()} rows inserted.`);
};

const insertByDatasetType = async (
  mysqlCmd: MysqlCommand,
  key: DatasetKey,
  data: DatasetType[]
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

export const handleParseTask = async (batchId: string, task: Task<any>, mysqlCmd: MysqlCommand) => {
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
