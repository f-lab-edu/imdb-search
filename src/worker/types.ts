import type { DatasetKey } from "../utils/types.js";

export enum TaskPhase {
  DOWNLOAD = 1,
  LOAD_TSV = 2,
  PRIMARY = 3,
  SECONDARY = 4,
}

export interface TaskConfig {
  mainQueue: string;
  maxWorkers: number;
  maxRetry: number;
  maxQueueLength: number;
  batchSize: number;
}

export enum TaskName {
  DOWNLOAD = "DOWNLOAD",
  LOAD_TSV = "LOAD_TSV",
  PARSE_PRIMARY = "PARSE_PRIMARY",
  PARSE_SECONDARY = "PARSE_SECONDARY",
  INSERT_DATA = "INSERT_DATA",
}

export interface Task<T = unknown> {
  batchId: string;
  taskId: string;
  name: TaskName;
  payload: T;
  retryCount: number;
  createdAt: number;
}

export interface DownloadPayload {
  url: string;
  targetPath: string;
  skipDownload: boolean;
  skipLoad?: boolean;
}

export interface LoadTSVPayload {
  filePath: string;
  skip?: boolean;
}

export interface ParsePayload {
  filePath: string;
  datasetType: DatasetKey;
  skip?: boolean;
}

export interface InsertPayload {
  datasetType: DatasetKey;
  data: unknown[];
}

export const isValidTask = (task: any): task is Task => {
  return (
    task &&
    typeof task.batchId === "string" &&
    typeof task.taskId === "string" &&
    Object.values(TaskName).includes(task.name) &&
    typeof task.createdAt === "number"
  );
};

export interface PipelineOptions {
  skipDownload: boolean;
  skipLoadTSV: boolean;
}
