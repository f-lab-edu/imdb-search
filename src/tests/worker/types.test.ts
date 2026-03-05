import { describe, it, expect, beforeAll } from "@jest/globals";
import { isValidTask, TaskName } from "../../worker/types.js";
import type {
  Task,
  DownloadPayload,
  ParsePayload,
  InsertPayload,
} from "../../worker/types.js";

describe("isValidTask", () => {
  const baseTask = {
    batchId: "batch-001",
    taskId: "task-001",
    retryCount: 0,
    createdAt: Date.now(),
  };

  describe("DOWNLOAD task", () => {
    it("valid download task", () => {
      const task: Task<DownloadPayload> = {
        ...baseTask,
        name: TaskName.DOWNLOAD,
        payload: {
          url: "https://datasets.imdbws.com/title.basics.tsv.gz",
          targetPath: "/data/title.basics.tsv.gz",
        },
      };

      expect(isValidTask(task)).toBe(true);
    });

    it("missing url", () => {
      const task = {
        ...baseTask,
        name: TaskName.DOWNLOAD,
        payload: { targetPath: "/data/title.basics.tsv.gz" },
      };

      expect(isValidTask(task)).toBe(false);
    });

    it("missing targetPath", () => {
      const task = {
        ...baseTask,
        name: TaskName.DOWNLOAD,
        payload: { url: "https://datasets.imdbws.com/title.basics.tsv.gz" },
      };

      expect(isValidTask(task)).toBe(false);
    });
  });

  describe("PARSE_PRIMARY task", () => {
    it("valid parse primary task", () => {
      const task: Task<ParsePayload> = {
        ...baseTask,
        name: TaskName.PARSE_PRIMARY,
        payload: {
          filePath: "/data/title.basics.tsv",
          datasetType: "TITLE_BASICS",
        },
      };

      expect(isValidTask(task)).toBe(true);
    });

    it("missing filePath", () => {
      const task = {
        ...baseTask,
        name: TaskName.PARSE_PRIMARY,
        payload: { datasetType: "TITLE_BASICS" },
      };

      expect(isValidTask(task)).toBe(false);
    });
  });

  describe("PARSE_SECONDARY task", () => {
    it("valid parse secondary task", () => {
      const task: Task<ParsePayload> = {
        ...baseTask,
        name: TaskName.PARSE_SECONDARY,
        payload: {
          filePath: "/data/title.ratings.tsv",
          datasetType: "TITLE_RATINGS",
        },
      };

      expect(isValidTask(task)).toBe(true);
    });
  });

  describe("INSERT_DATA task", () => {
    it("valid insert task", () => {
      const task: Task<InsertPayload> = {
        ...baseTask,
        name: TaskName.INSERT_DATA,
        payload: {
          datasetType: "TITLE_BASICS",
          data: [{ tconst: "tt0000001", title_type: "short" }],
        },
      };

      expect(isValidTask(task)).toBe(true);
    });

    it("data is not array", () => {
      const task = {
        ...baseTask,
        name: TaskName.INSERT_DATA,
        payload: {
          datasetType: "TITLE_BASICS",
          data: "not an array",
        },
      };

      expect(isValidTask(task)).toBe(false);
    });

    it("missing datasetType", () => {
      const task = {
        ...baseTask,
        name: TaskName.INSERT_DATA,
        payload: {
          data: [{ tconst: "tt0000001" }],
        },
      };

      expect(isValidTask(task)).toBe(false);
    });
  });

  describe("invalid base fields", () => {
    it("null input", () => {
      expect(isValidTask(null)).toBe(false);
    });

    it("undefined input", () => {
      expect(isValidTask(undefined)).toBe(false);
    });

    it("empty object", () => {
      expect(isValidTask({})).toBe(false);
    });

    it("missing batchId", () => {
      const task = {
        taskId: "task-001",
        name: TaskName.DOWNLOAD,
        payload: {
          url: "https://example.com/file.gz",
          targetPath: "/data/file.gz",
        },
        retryCount: 0,
        createdAt: Date.now(),
      };

      expect(isValidTask(task)).toBe(false);
    });

    it("invalid task name", () => {
      const task = {
        ...baseTask,
        name: "INVALID_NAME",
        payload: {},
      };

      expect(isValidTask(task)).toBe(false);
    });

    it("createdAt is not number", () => {
      const task = {
        ...baseTask,
        createdAt: "2024-01-01",
        name: TaskName.DOWNLOAD,
        payload: {
          url: "https://example.com/file.gz",
          targetPath: "/data/file.gz",
        },
      };

      expect(isValidTask(task)).toBe(false);
    });
  });
});
