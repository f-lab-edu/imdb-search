import dotenv from "dotenv";
import { describe, it, expect, beforeAll } from "@jest/globals";
import { isValidTask, TaskName } from "../../worker/types.js";
import type {
  Task,
  DownloadPayload,
  ParsePayload,
  InsertPayload,
} from "../../worker/types.js";

describe("handleParseTask", () => {
  let handleParseTask: (typeof import("../../worker/handlers.js"))["handleParseTask"];

  beforeAll(async () => {
    dotenv.config({ path: ".env.test" });

    const handlers = await import("../../worker/handlers.js");
    handleParseTask = handlers.handleParseTask;
  });

  it("skip이 true면 count 0, skipped true 반환", async () => {
    const task = {
      batchId: "batch-001",
      taskId: "task-001",
      name: TaskName.INSERT_DATA,
      payload: {
        datasetType: "TITLE_BASICS",
        data: [{ tconst: "tt0000001" }],
        skip: true,
      },
      retryCount: 0,
      createdAt: Date.now(),
    };

    const result = await handleParseTask("batch-001", task, {} as any);

    expect(result).toEqual({ success: true, count: 0, skipped: true });
  });

  it("data가 빈 배열이면 count 0 반환", async () => {
    const task = {
      batchId: "batch-001",
      taskId: "task-001",
      name: TaskName.INSERT_DATA,
      payload: {
        datasetType: "TITLE_BASICS",
        data: [],
        skip: false,
      },
      retryCount: 0,
      createdAt: Date.now(),
    };

    const result = await handleParseTask("batch-001", task, {} as any);

    expect(result).toEqual({ success: true, count: 0 });
  });

  it("data가 undefined면 count 0 반환", async () => {
    const task = {
      batchId: "batch-001",
      taskId: "task-001",
      name: TaskName.INSERT_DATA,
      payload: {
        datasetType: "TITLE_BASICS",
        skip: false,
      },
      retryCount: 0,
      createdAt: Date.now(),
    };

    const result = await handleParseTask("batch-001", task, {} as any);

    expect(result).toEqual({ success: true, count: 0 });
  });
});
