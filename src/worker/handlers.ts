import type { DownloadPayload, ParsePayload, Task } from "./types.js";
import { TaskName } from "./types.js";
import { downloadFile } from "../utils/download.js";

export const hanldeDownloadTask = async (
  taskId: string,
  payload: DownloadPayload,
): Promise<Task<ParsePayload>> => {
  const savedPath = await downloadFile(payload.targetPath, payload.url);

  return {
    id: `parse_${taskId}`, // need to create unique id
    name: TaskName.PARSE_AND_INSERT,
    payload: { filePath: savedPath },
    retryCount: 0,
    createdAt: Date.now(),
  };
};

// TODO: this is for test only, need to implement proper logics
export const handleParseAndInsert = async (
  taskId: string,
  payload: ParsePayload,
) => {
  console.log(`task id: ${taskId}`);
  console.log(`file path: ${payload.filePath}`);

  return new Promise((res) => setTimeout(res, 500));
};
