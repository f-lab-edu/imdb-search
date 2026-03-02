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
