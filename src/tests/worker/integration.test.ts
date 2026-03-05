import {
  describe,
  it,
  expect,
  beforeAll,
  afterAll,
  beforeEach,
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
import { TaskName, type Task, type InsertPayload } from "../../worker/types.js";
import type { TaskProducer } from "../../worker/producer.js";
import type { TaskConsumer } from "../../worker/consumer.js";

let redis: RedisDatabase;
let mysqlDb: MysqlDatabase;
let mysqlCmd: MysqlCommand;
let pool: mysql.Pool;
let tmpDir: string;
let batchId: string;

let MAIN_QUEUE = "test_main_queue";
let HOLD_QUEUE = "test_hold_queue";
const PRIMARY_DONE_KEY = "test_primary_done";

const taskConfig = {
  mainQueue: MAIN_QUEUE,
  holdQueue: HOLD_QUEUE,
  primaryDoneKey: PRIMARY_DONE_KEY,
  maxWorkers: 2,
  maxRetry: 3,
};

const writeMockTSV = async (fileName: string, content: string) => {
  const filePath = path.join(tmpDir, fileName);
  await fs.writeFile(filePath, content, "utf-8");
  return filePath;
};

const resetMysql = async (pool: mysql.Pool) => {
  const conn = await pool.getConnection();
  try {
    await conn.query("SET FOREIGN_KEY_CHECKS = 0");
    await conn.query(
      "DROP TABLE IF EXISTS TITLE_GENRES, TITLE_AKAS, RATINGS, EPISODES, TITLE_CREW, TITLE_PRINCIPALS, TITLES, PERSONS, GENRES;",
    );
    await conn.query("SET FOREIGN_KEY_CHECKS = 1");
  } finally {
    conn.release();
  }
};

beforeAll(async () => {
  const { config } = await import("../../config/index.js");
  const { RedisDatabase } = await import("../../db/redis.js");
  const { MysqlDatabase: MysqlDB } =
    await import("../../db/mysql/connection.js");
  const { MysqlCommand: MysqlCmd } = await import("../../db/mysql/commands.js");

  redis = await RedisDatabase.create(config.db.redis);
  mysqlDb = new MysqlDB(config.db.mysql);
  pool = mysqlDb.getPool();
  mysqlCmd = await MysqlCmd.create(pool);

  tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "worker-test-"));
  batchId = crypto.randomUUID();

  MAIN_QUEUE = config.task.mainQueue;
  HOLD_QUEUE = config.task.holdQueue;
});

afterAll(async () => {
  const client = redis.getClient();
  const keys = await client.keys("test_*");
  if (keys.length > 0) {
    await client.del(keys);
  }
  const batchKeys = await client.keys(`*${batchId}*`);
  if (batchKeys.length > 0) {
    await client.del(batchKeys);
  }

  await resetMysql(pool);
  await mysqlDb.close();
  await redis.close();
  await fs.rm(tmpDir, { recursive: true, force: true });
});

describe("TaskProducer", () => {
  let producer: TaskProducer;
  let redisClient: ReturnType<RedisDatabase["getClient"]>;

  beforeAll(async () => {
    const { config } = await import("../../config/index.js");
    const { TaskProducer: Producer } = await import("../../worker/producer.js");

    const testDatasetCfg = {
      ...config.datasets,
      downloadDir: tmpDir,
    };

    producer = new Producer(redis, config.task, testDatasetCfg, batchId);
    redisClient = redis.getClient();
  });

  beforeEach(async () => {
    const mainQ = `${MAIN_QUEUE}:${batchId}`;
    const holdQ = `${HOLD_QUEUE}:${batchId}`;
    await redisClient.del(mainQ);
    await redisClient.del(holdQ);
  });

  it("produceDownloadTask: 큐에 다운로드 태스크 등록", async () => {
    await producer.produceDownloadTask(
      "https://datasets.imdbws.com/title.basics.tsv.gz",
      "/data/title.basics.tsv.gz",
    );

    const mainQ = `${MAIN_QUEUE}:${batchId}`;
    const len = await redisClient.lLen(mainQ);
    expect(len).toBe(1);

    const raw = await redisClient.lPop(mainQ);
    const task = JSON.parse(raw!);
    expect(task.name).toBe(TaskName.DOWNLOAD);
    expect(task.payload.url).toBe(
      "https://datasets.imdbws.com/title.basics.tsv.gz",
    );
    expect(task.batchId).toBe(batchId);
  });

  it("producePhase1Task: TSV 파싱 후 INSERT 태스크 등록", async () => {
    // mock title.basics TSV
    const tsv = [
      "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres",
      "tt0000001\tshort\tCarmencita\tCarmencita\t0\t1894\t\\N\t1\tDocumentary,Short",
      "tt0000002\tshort\tLe clown\tLe clown\t0\t1892\t\\N\t5\tAnimation,Short",
      "tt0000003\tshort\tTest\tTest\t0\t1900\t\\N\t3\tDrama",
    ].join("\n");

    await writeMockTSV("title.basics.tsv", tsv);

    await producer.producePhase1Task("title.basics.tsv");

    const mainQ = `${MAIN_QUEUE}:${batchId}`;
    const len = await redisClient.lLen(mainQ);
    expect(len).toBeGreaterThanOrEqual(1);

    const raw = await redisClient.lPop(mainQ);
    const task = JSON.parse(raw!) as Task<InsertPayload>;
    expect(task.name).toBe(TaskName.INSERT_DATA);
    expect(task.payload.datasetType).toBe("TITLE_BASICS");
    expect(task.payload.data.length).toBe(3);
  });

  it("hold: holdQueue에 secondary 태스크 등록", async () => {
    await producer.hold("TITLE_RATINGS", "title.ratings.tsv");

    const holdQ = `${HOLD_QUEUE}:${batchId}`;
    const len = await redisClient.lLen(holdQ);
    expect(len).toBe(1);

    const raw = await redisClient.lPop(holdQ);
    const task = JSON.parse(raw!);
    expect(task.name).toBe(TaskName.PARSE_SECONDARY);
    expect(task.payload.datasetType).toBe("TITLE_RATINGS");
  });
});

describe("TaskConsumer", () => {
  let consumer: TaskConsumer;
  let redisClient: ReturnType<RedisDatabase["getClient"]>;

  let actualMainQ: string;

  beforeAll(async () => {
    const { TaskConsumer: Consumer } = await import("../../worker/consumer.js");

    const testTaskConfig = {
      ...taskConfig,
      mainQueue: MAIN_QUEUE,
      holdQueue: HOLD_QUEUE,
    };

    consumer = new Consumer(redis, batchId, mysqlCmd, testTaskConfig);
    redisClient = redis.getClient();

    actualMainQ = `${MAIN_QUEUE}:${batchId}`;
  });

  beforeEach(async () => {
    await redisClient.del(actualMainQ);
  });

  it("executeTask: 실패 시 retry count 증가 후 큐에 재등록", async () => {
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

    const len = await redisClient.lLen(actualMainQ);
    expect(len).toBe(1);

    const raw = await redisClient.lPop(actualMainQ);
    const retriedTask = JSON.parse(raw!);
    expect(retriedTask.retryCount).toBe(1);
  });

  it("start/stop: 큐에서 태스크 소비 후 정상 종료", async () => {
    const { TaskConsumer: Consumer } = await import("../../worker/consumer.js");

    const testConsumer = new Consumer(redis, batchId, mysqlCmd, {
      ...taskConfig,
      mainQueue: MAIN_QUEUE,
      maxWorkers: 1,
    });

    const task: Task<InsertPayload> = {
      batchId,
      taskId: crypto.randomUUID(),
      name: TaskName.INSERT_DATA,
      payload: {
        datasetType: "TITLE_BASICS",
        data: [
          {
            tconst: "tt8880001",
            title_type: "movie",
            primary_title: "Start Stop Test",
            original_title: "Start Stop Test",
            is_adult: "0",
            start_year: "2023",
            end_year: null,
            runtime_minutes: "90",
            genres: "Comedy",
          },
        ],
      },
      retryCount: 0,
      createdAt: Date.now(),
    };

    await redisClient.rPush(actualMainQ, JSON.stringify(task));

    setTimeout(() => testConsumer.stop(), 500);
    await testConsumer.start();

    await new Promise((res) => setTimeout(res, 100));

    const [rows] = await pool.query<mysql.RowDataPacket[]>(
      "SELECT tconst FROM TITLES WHERE tconst = 'tt8880001'",
    );
    expect(rows.length).toBe(1);

    const remaining = await redisClient.lLen(actualMainQ);
    expect(remaining).toBe(0);
  }, 10000);
});
