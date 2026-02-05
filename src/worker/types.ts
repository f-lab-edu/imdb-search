export enum TaskName {
  DOWNLOAD = "DOWNLOAD",
  PARSE_AND_INSERT = "PARSE_AND_INSERT",
}

export interface Task<T = unknown> {
  id: string; // 작업 구분용 고유 id
  name: TaskName;
  payload: T;
  retryCount: number; // 실패 시 재시도 횟수
  createdAt: number; // 작업 생성 시간
}

export interface DownloadPayload {
  url: string;
  targetPath: string; // 다운로드 할 경로
}

export interface ParsePayload {
  filePath: string;
}

export const isValidTask = (task: any): task is Task => {
  const isVT =
    task &&
    typeof task.id === "string" &&
    Object.values(TaskName).includes(task.name) &&
    typeof task.createdAt === "number";

  if (!isVT) return false;

  switch (task.name) {
    case TaskName.DOWNLOAD:
      return isDownloadPayload(task.payload);
    case TaskName.PARSE_AND_INSERT:
      return isParsePayload(task.payload);
    default:
      return false;
  }
};

const isDownloadPayload = (payload: any): payload is DownloadPayload => {
  return (
    payload &&
    typeof payload.url === "string" &&
    typeof payload.targetPath === "string"
  );
};

const isParsePayload = (payload: any): payload is ParsePayload => {
  return payload && typeof payload.filePath === "string";
};
