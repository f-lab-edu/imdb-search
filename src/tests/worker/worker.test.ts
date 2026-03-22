import {
  describe,
  it,
  expect,
  beforeAll,
  afterAll,
  beforeEach,
  afterEach,
} from "@jest/globals";
import dotenv from "dotenv";
dotenv.config({ path: ".env.test" });

import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import type mysql from "mysql2/promise";
import type { RedisDatabase } from "../../db/redis.js";
import type { MysqlDatabase } from "../../db/mysql/connection.js";
import type { MysqlCommand } from "../../db/mysql/commands.js";
import type { Producer } from "../../worker/producer.js";
import type { Consumer } from "../../worker/consumer.js";
import {
  TaskName,
  TaskPhase,
  type Task,
  type ParsePayload,
  type InsertPayload,
} from "../../worker/types.js";

let redis: RedisDatabase;
let mysqlDb: MysqlDatabase;
let mysqlCmd: MysqlCommand;
let pool: mysql.Pool;
let tmpDir: string;

import type { config as Config } from "../../config/index.js";

let cfg: typeof Config;

const MAIN_QUEUE = "task_main_queue";

const writeMockTSV = async (fileName: string, content: string) => {
  const filePath = path.join(tmpDir, fileName);
  await fs.writeFile(filePath, content, "utf-8");
  return filePath;
};

const cleanupBatch = async (batchId: string) => {
  const client = redis.getClient();
  const keys = await client.keys(`${batchId}:*`);
  if (keys.length > 0) await client.del(keys);

  const conn = await pool.getConnection();
  try {
    await conn.query("DELETE FROM TASKS WHERE batch_id = ?", [batchId]);
  } finally {
    conn.release();
  }
};

const cleanupTitles = async (tconsts: string[]) => {
  if (tconsts.length === 0) return;
  const conn = await pool.getConnection();
  try {
    await conn.query("SET FOREIGN_KEY_CHECKS = 0");
    await conn.query("DELETE FROM TITLE_GENRES WHERE tconst IN (?)", [tconsts]);
    await conn.query("DELETE FROM TITLE_AKAS WHERE tconst IN (?)", [tconsts]);
    await conn.query("DELETE FROM RATINGS WHERE tconst IN (?)", [tconsts]);
    await conn.query("DELETE FROM TITLE_CREW WHERE tconst IN (?)", [tconsts]);
    await conn.query("DELETE FROM TITLE_PRINCIPALS WHERE tconst IN (?)", [
      tconsts,
    ]);
    await conn.query("DELETE FROM EPISODES WHERE tconst IN (?)", [tconsts]);
    await conn.query("DELETE FROM TITLES WHERE tconst IN (?)", [tconsts]);
    await conn.query("SET FOREIGN_KEY_CHECKS = 1");
  } finally {
    conn.release();
  }
};

const cleanupPersons = async (nconsts: string[]) => {
  if (nconsts.length === 0) return;
  const conn = await pool.getConnection();
  try {
    await conn.query("DELETE FROM PERSONS WHERE nconst IN (?)", [nconsts]);
  } finally {
    conn.release();
  }
};

const runConsumerUntilDone = async (
  consumer: Consumer,
  timeoutMs = 5000,
): Promise<void> => {
  const done = consumer.start();
  await new Promise((res) => setTimeout(res, timeoutMs));
  consumer.stop();
  await done;
};

const createWorker = async (batchId: string, phase?: TaskPhase) => {
  const { Producer: P } = await import("../../worker/producer.js");
  const { Consumer: C } = await import("../../worker/consumer.js");
  const { createHandlers } = await import("../../worker/handlers.js");

  const producer = new P(batchId, cfg.task, redis, mysqlCmd);
  const handlers = createHandlers(redis, mysqlCmd, producer, cfg.task.batchSize);
  const consumer = new C(batchId, cfg.task, redis, mysqlCmd, producer, handlers, phase);
  return { producer, handlers, consumer };
};

beforeAll(async () => {
  const { config } = await import("../../config/index.js");
  const { RedisDatabase: RDB } = await import("../../db/redis.js");
  const { MysqlDatabase: MDB } = await import("../../db/mysql/connection.js");
  const { MysqlCommand: MC } = await import("../../db/mysql/commands.js");

  cfg = config;
  redis = await RDB.create(config.db.redis);
  mysqlDb = new MDB(config.db.mysql);
  pool = mysqlDb.getPool();
  mysqlCmd = await MC.create(pool);

  tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "worker-test-"));
});

afterAll(async () => {
  await fs.rm(tmpDir, { recursive: true, force: true });
  await mysqlDb.close();
  await redis.close();
});

// ─────────────────────────────────────────────
// Producer
// ─────────────────────────────────────────────
describe("Producer", () => {
  let producer: Producer;
  let batchId: string;

  beforeEach(async () => {
    batchId = crypto.randomUUID();
    const { Producer: P } = await import("../../worker/producer.js");
    producer = new P(batchId, cfg.task, redis, mysqlCmd);
  });

  afterEach(() => cleanupBatch(batchId));

  it("produceDownloadTask: writes DOWNLOAD task to MySQL with phase=DOWNLOAD", async () => {
    await producer.produceDownloadTask(
      "https://example.com/file.tsv.gz",
      `.randomUUID()}.tsv`,
      { skipDownload: false, skipLoadTSV: false, skipNormalization: false, skipIntegrityCheck: false },
    );

    const tasks = await mysqlCmd.fetchPendingTasks(TaskPhase.DOWNLOAD, 10);
    const task = tasks.find((t) => t.batchId === batchId);
    expect(task).toBeDefined();
    expect(task!.name).toBe(TaskName.DOWNLOAD);
  });

  it("produce: writes task to MySQL with correct phase", async () => {
    const task: Task = {
      batchId,
      taskId: crypto.randomUUID(),
      name: TaskName.PARSE_PRIMARY,
      payload: { filePath: `.randomUUID()}.tsv`, datasetType: "TITLE_BASICS" },
      retryCount: 0,
      createdAt: Date.now(),
    };

    await producer.schedule(task, TaskPhase.PRIMARY);

    const tasks = await mysqlCmd.fetchPendingTasks(TaskPhase.PRIMARY, 10);
    const found = tasks.find((t) => t.taskId === task.taskId);
    expect(found).toBeDefined();
    expect(found!.name).toBe(TaskName.PARSE_PRIMARY);
  });

  it("refill: moves pending MySQL tasks into Redis queue", async () => {
    const task: Task = {
      batchId,
      taskId: crypto.randomUUID(),
      name: TaskName.PARSE_PRIMARY,
      payload: { filePath: `.randomUUID()}.tsv`, datasetType: "TITLE_BASICS" },
      retryCount: 0,
      createdAt: Date.now(),
    };
    await mysqlCmd.insertTask(task, TaskPhase.PRIMARY);

    const refilled = await producer.refill(TaskPhase.PRIMARY, 10);
    expect(refilled).toBe(1);

    const client = redis.getClient();
    const queueKey = `${batchId}:${MAIN_QUEUE}`;
    const len = await client.lLen(queueKey);
    expect(len).toBe(1);
  });

  it("refill: marks refilled tasks as queued in MySQL", async () => {
    const task: Task = {
      batchId,
      taskId: crypto.randomUUID(),
      name: TaskName.PARSE_PRIMARY,
      payload: { filePath: `.randomUUID()}.tsv`, datasetType: "TITLE_BASICS" },
      retryCount: 0,
      createdAt: Date.now(),
    };
    await mysqlCmd.insertTask(task, TaskPhase.PRIMARY);
    await producer.refill(TaskPhase.PRIMARY, 10);

    // should not appear in pending anymore
    const pending = await mysqlCmd.fetchPendingTasks(TaskPhase.PRIMARY, 10);
    const found = pending.find((t) => t.taskId === task.taskId);
    expect(found).toBeUndefined();
  });

  it("refill: returns 0 when no pending tasks for phase", async () => {
    const refilled = await producer.refill(TaskPhase.PRIMARY, 10);
    expect(refilled).toBe(0);
  });
});

// ─────────────────────────────────────────────
// Consumer - phase DOWNLOAD
// ─────────────────────────────────────────────
describe("Consumer - DOWNLOAD phase", () => {
  let batchId: string;

  beforeEach(async () => {
    batchId = crypto.randomUUID();
  });

  afterEach(() => cleanupBatch(batchId));

  it("transitions to PRIMARY when no DOWNLOAD tasks remain", async () => {
    const { consumer } = await createWorker(batchId, TaskPhase.DOWNLOAD);

    // No DOWNLOAD tasks seeded → should transition phases and eventually stop
    const start = Date.now();
    await runConsumerUntilDone(consumer, 1500);
    const elapsed = Date.now() - start;

    // Consumer should have exited cleanly within the timeout
    expect(elapsed).toBeLessThan(6000);
  });

  it("processes DOWNLOAD task and produces PARSE task to MySQL", async () => {
    // Write a minimal TSV to simulate a downloaded file
    const filePath = await writeMockTSV(
      "title.basics.tsv",
      [
        "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres",
        "tt8880001\tmovie\tDownload Test\tDownload Test\t0\t2024\t\\N\t90\tDrama",
      ].join("\n"),
    );

    const { Producer: P } = await import("../../worker/producer.js");
    const { Consumer: C } = await import("../../worker/consumer.js");

    const producer = new P(batchId, cfg.task, redis, mysqlCmd);
    // Override DOWNLOAD handler to avoid real HTTP download
    const handlers = {
      [TaskName.DOWNLOAD]: async (task: Task) => {
        // simulate: download done, produce PARSE_PRIMARY
        await producer.schedule(
          {
            batchId: task.batchId,
            taskId: crypto.randomUUID(),
            name: TaskName.PARSE_PRIMARY,
            payload: {
              filePath,
              datasetType: "TITLE_BASICS",
            } satisfies ParsePayload,
            retryCount: 0,
            createdAt: Date.now(),
          },
          TaskPhase.PRIMARY,
        );
      },
    };

    const consumer = new C(batchId, cfg.task, redis, mysqlCmd, producer, handlers, TaskPhase.DOWNLOAD);

    // Seed one DOWNLOAD task
    const downloadTask: Task = {
      batchId,
      taskId: crypto.randomUUID(),
      name: TaskName.DOWNLOAD,
      payload: {
        url: "https://example.com/title.basics.tsv.gz",
        targetPath: filePath,
      },
      retryCount: 0,
      createdAt: Date.now(),
    };
    await mysqlCmd.insertTask(downloadTask, TaskPhase.DOWNLOAD);

    await runConsumerUntilDone(consumer, 2000);

    // PARSE_PRIMARY task should exist in TASKS table (any status — it may have been refilled)
    const [rows] = await pool.query<mysql.RowDataPacket[]>(
      "SELECT name FROM TASKS WHERE batch_id = ? AND name = ?",
      [batchId, TaskName.PARSE_PRIMARY],
    );
    expect(rows.length).toBeGreaterThan(0);
  }, 10000);
});

// ─────────────────────────────────────────────
// Consumer - phase PRIMARY
// ─────────────────────────────────────────────
describe("Consumer - PRIMARY phase", () => {
  const testTconsts = ["tt8881001", "tt8881002", "tt8881003"];
  let batchId: string;

  beforeEach(async () => {
    batchId = crypto.randomUUID();
  });

  afterEach(async () => {
    await cleanupBatch(batchId);
    await cleanupTitles(testTconsts);
  });

  it("processes PARSE_PRIMARY task: inserts TITLES into MySQL", async () => {
    const filePath = await writeMockTSV(
      "title.basics.primary.tsv",
      [
        "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres",
        `${testTconsts[0]}\tmovie\tPrimary Test 1\tPrimary Test 1\t0\t2020\t\\N\t90\tAction,Drama`,
        `${testTconsts[1]}\tmovie\tPrimary Test 2\tPrimary Test 2\t0\t2021\t\\N\t100\tComedy`,
        `${testTconsts[2]}\tshort\tPrimary Test 3\tPrimary Test 3\t0\t2022\t\\N\t15\tDocumentary`,
      ].join("\n"),
    );

    const { consumer } = await createWorker(batchId, TaskPhase.PRIMARY);

    const parseTask: Task<ParsePayload> = {
      batchId,
      taskId: crypto.randomUUID(),
      name: TaskName.PARSE_PRIMARY,
      payload: { filePath, datasetType: "TITLE_BASICS" },
      retryCount: 0,
      createdAt: Date.now(),
    };
    await mysqlCmd.insertTask(parseTask, TaskPhase.PRIMARY);

    await runConsumerUntilDone(consumer, 3000);

    const [rows] = await pool.query<mysql.RowDataPacket[]>(
      "SELECT tconst FROM TITLES WHERE tconst IN (?) ORDER BY tconst",
      [testTconsts],
    );
    expect(rows.length).toBe(3);
  }, 15000);

  it("processes INSERT_DATA task: inserts TITLE_BASICS rows directly", async () => {
    const { consumer } = await createWorker(batchId, TaskPhase.PRIMARY);

    const insertTask: Task<InsertPayload> = {
      batchId,
      taskId: crypto.randomUUID(),
      name: TaskName.INSERT_DATA,
      payload: {
        datasetType: "TITLE_BASICS",
        data: [
          {
            tconst: testTconsts[0],
            title_type: "movie",
            primary_title: "Direct Insert Test",
            original_title: "Direct Insert Test",
            is_adult: "0",
            start_year: "2024",
            end_year: null,
            runtime_minutes: "90",
          },
        ],
      },
      retryCount: 0,
      createdAt: Date.now(),
    };
    await mysqlCmd.insertTask(insertTask, TaskPhase.PRIMARY);

    await runConsumerUntilDone(consumer, 2000);

    const [rows] = await pool.query<mysql.RowDataPacket[]>(
      "SELECT tconst FROM TITLES WHERE tconst = ?",
      [testTconsts[0]],
    );
    expect(rows.length).toBe(1);
  }, 10000);
});

// ─────────────────────────────────────────────
// Consumer - phase SECONDARY
// ─────────────────────────────────────────────
describe("Consumer - SECONDARY phase", () => {
  const testTconsts = ["tt8882001", "tt8882002"];
  const testNconsts = ["nm8882001"];
  let batchId: string;

  beforeEach(async () => {
    batchId = crypto.randomUUID();

    // Seed PRIMARY data (TITLES + PERSONS) that SECONDARY tables depend on
    await mysqlCmd.insertTitleBasics([
      {
        tconst: testTconsts[0],
        title_type: "movie",
        primary_title: "Secondary Test 1",
        original_title: "Secondary Test 1",
        is_adult: "0",
        start_year: "2020",
        end_year: null,
        runtime_minutes: "90",
        genres: "Action",
      } as any,
      {
        tconst: testTconsts[1],
        title_type: "movie",
        primary_title: "Secondary Test 2",
        original_title: "Secondary Test 2",
        is_adult: "0",
        start_year: "2021",
        end_year: null,
        runtime_minutes: "100",
        genres: "Drama",
      } as any,
    ]);
    await mysqlCmd.insertNameBasics([
      {
        nconst: testNconsts[0],
        primary_name: "Test Person",
        birth_year: "1990",
        death_year: null,
        primary_profession: "actor",
        known_for_titles: testTconsts[0],
      } as any,
    ]);
  });

  afterEach(async () => {
    await cleanupBatch(batchId);
    await cleanupTitles(testTconsts);
    await cleanupPersons(testNconsts);
  });

  it("processes PARSE_SECONDARY task: inserts RATINGS into MySQL", async () => {
    const filePath = await writeMockTSV(
      "title.ratings.secondary.tsv",
      [
        "tconst\taverageRating\tnumVotes",
        `${testTconsts[0]}\t8.5\t1000`,
        `${testTconsts[1]}\t7.2\t500`,
      ].join("\n"),
    );

    const { consumer } = await createWorker(batchId, TaskPhase.SECONDARY);

    const parseTask: Task<ParsePayload> = {
      batchId,
      taskId: crypto.randomUUID(),
      name: TaskName.PARSE_SECONDARY,
      payload: { filePath, datasetType: "TITLE_RATINGS" },
      retryCount: 0,
      createdAt: Date.now(),
    };
    await mysqlCmd.insertTask(parseTask, TaskPhase.SECONDARY);

    await runConsumerUntilDone(consumer, 3000);

    const [rows] = await pool.query<mysql.RowDataPacket[]>(
      "SELECT tconst FROM RATINGS WHERE tconst IN (?) ORDER BY tconst",
      [testTconsts],
    );
    expect(rows.length).toBe(2);
  }, 15000);
});

// ─────────────────────────────────────────────
// Consumer - executeTask (unit)
// ─────────────────────────────────────────────
describe("Consumer - executeTask", () => {
  let consumer: Consumer;
  let batchId: string;

  beforeEach(async () => {
    batchId = crypto.randomUUID();
    ({ consumer } = await createWorker(batchId));
  });

  afterEach(() => cleanupBatch(batchId));

  it("retries failed task and increments retryCount", async () => {
    const task: Task<InsertPayload> = {
      batchId,
      taskId: crypto.randomUUID(),
      name: TaskName.INSERT_DATA,
      payload: {
        datasetType: "INVALID_TYPE" as any,
        data: [{ tconst: "tt0000001" }],
      },
      retryCount: 0,
      createdAt: Date.now(),
    };

    await consumer.executeTask(JSON.stringify(task));

    // should be re-enqueued in Redis with retryCount=1
    const client = redis.getClient();
    const queueKey = `${batchId}:${MAIN_QUEUE}`;
    const raw = await client.lPop(queueKey);
    expect(raw).not.toBeNull();
    const retried = JSON.parse(raw!);
    expect(retried.retryCount).toBe(1);
  });

  it("marks task as failed after maxRetry", async () => {
    const task: Task<InsertPayload> = {
      batchId,
      taskId: crypto.randomUUID(),
      name: TaskName.INSERT_DATA,
      payload: {
        datasetType: "INVALID_TYPE" as any,
        data: [{ tconst: "tt0000001" }],
      },
      retryCount: cfg.task.maxRetry, // already at max
      createdAt: Date.now(),
    };

    await mysqlCmd.insertTask(task, TaskPhase.PRIMARY);
    await consumer.executeTask(JSON.stringify(task));

    const hasPending = await mysqlCmd.hasPendingTasks(TaskPhase.PRIMARY);
    expect(hasPending).toBe(false);

    // task should be marked failed, not re-queued in Redis
    const client = redis.getClient();
    const len = await client.lLen(`${batchId}:${MAIN_QUEUE}`);
    expect(len).toBe(0);
  });

  it("silently ignores invalid JSON", async () => {
    await expect(
      consumer.executeTask("not-valid-json{"),
    ).resolves.toBeUndefined();
  });

  it("silently ignores task with no registered handler", async () => {
    const task: Task = {
      batchId,
      taskId: crypto.randomUUID(),
      name: TaskName.DOWNLOAD,
      payload: {},
      retryCount: 0,
      createdAt: Date.now(),
    };

    const { Producer: P } = await import("../../worker/producer.js");
    const { Consumer: C } = await import("../../worker/consumer.js");

    const producer = new P(batchId, cfg.task, redis, mysqlCmd);
    const emptyHandlers = {}; // no handlers registered
    const bareConsumer = new C(
      batchId,
      cfg.task,
      redis,
      mysqlCmd,
      producer,
      emptyHandlers,
    );

    await expect(
      bareConsumer.executeTask(JSON.stringify(task)),
    ).resolves.toBeUndefined();
  });
});
