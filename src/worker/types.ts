import type { DatasetKey } from "../utils/types.js";

export enum TaskName {
  DOWNLOAD = "DOWNLOAD",
  PARSE_PRIMARY = "PARSE_PRIMARY",
  PARSE_SECONDARY = "PARSE_SECONDARY",

  INSERT_DATA = "INSERT_DATA",
}

export interface Task<T = unknown> {
  batchId: string;
  taskId: string; // 작업 구분용 고유 id
  name: TaskName;
  payload: T;
  retryCount: number; // 실패 시 재시도 횟수
  createdAt: number; // 작업 생성 시간
}

export interface InsertPayload<T = unknown> {
  datasetType: DatasetKey;
  data: T[];
}

export interface DownloadPayload {
  url: string;
  targetPath: string; // 다운로드 할 경로
}

export interface ParsePayload {
  filePath: string;
  datasetType: DatasetKey;
  skip?: boolean;
}

export const isValidTask = (task: any): task is Task => {
  const isVT =
    task &&
    typeof task.batchId === "string" &&
    typeof task.taskId === "string" &&
    Object.values(TaskName).includes(task.name) &&
    typeof task.createdAt === "number";

  if (!isVT) return false;

  switch (task.name) {
    case TaskName.DOWNLOAD:
      return isDownloadPayload(task.payload);
    case TaskName.PARSE_PRIMARY:
    case TaskName.PARSE_SECONDARY:
      return isParsePayload(task.payload);
    case TaskName.INSERT_DATA:
      return isInsertPayload(task.payload);
    default:
      return false;
  }
};

const isDownloadPayload = (payload: any): payload is DownloadPayload => {
  return payload && typeof payload.url === "string" && typeof payload.targetPath === "string";
};

const isParsePayload = (payload: any): payload is ParsePayload => {
  return payload && typeof payload.filePath === "string";
};

const isInsertPayload = (payload: any): payload is InsertPayload => {
  return payload && typeof payload.datasetType === "string" && Array.isArray(payload.data);
};
