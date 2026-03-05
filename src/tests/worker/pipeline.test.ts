import { describe, it, expect, beforeAll, afterAll } from "@jest/globals";
import dotenv from "dotenv";
dotenv.config({ path: ".env.test" });

import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import type mysql from "mysql2/promise";
import type { RedisDatabase } from "../../db/redis.js";
import type { MysqlDatabase } from "../../db/mysql/connection.js";
import type { MysqlCommand } from "../../db/mysql/commands.js";

let redis: RedisDatabase;
let mysqlDb: MysqlDatabase;
let mysqlCmd: MysqlCommand;
let pool: mysql.Pool;
let tmpDir: string;

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
  const { RedisDatabase: RDB } = await import("../../db/redis.js");
  const { MysqlDatabase: MDB } = await import("../../db/mysql/connection.js");
  const { MysqlCommand: MC } = await import("../../db/mysql/commands.js");

  redis = await RDB.create(config.db.redis);
  mysqlDb = new MDB(config.db.mysql);
  pool = mysqlDb.getPool();
  mysqlCmd = await MC.create(pool);

  tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "pipeline-test-"));

  // mock TSV 파일들 생성
  await writeMockTSV(
    "title.basics.tsv",
    [
      "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres",
      "tt7770001\tmovie\tPipeline Test 1\tPipeline Test 1\t0\t2020\t\\N\t120\tAction,Drama",
      "tt7770002\tmovie\tPipeline Test 2\tPipeline Test 2\t0\t2021\t\\N\t90\tComedy",
      "tt7770003\tshort\tPipeline Test 3\tPipeline Test 3\t0\t2022\t\\N\t15\tDocumentary",
    ].join("\n"),
  );

  await writeMockTSV(
    "name.basics.tsv",
    [
      "nconst\tprimaryName\tbirthYear\tdeathYear\tprimaryProfession\tknownForTitles",
      "nm7770001\tTest Actor\t1990\t\\N\tactor\ttt7770001",
      "nm7770002\tTest Director\t1980\t\\N\tdirector\ttt7770002",
    ].join("\n"),
  );

  await writeMockTSV(
    "title.ratings.tsv",
    [
      "tconst\taverageRating\tnumVotes",
      "tt7770001\t7.5\t1000",
      "tt7770002\t8.0\t2000",
    ].join("\n"),
  );

  await writeMockTSV(
    "title.akas.tsv",
    [
      "titleId\tordering\ttitle\tregion\tlanguage\ttypes\tattributes\tisOriginalTitle",
      "tt7770001\t1\tPipeline Test 1 KR\tKR\tko\t\\N\t\\N\t0",
    ].join("\n"),
  );

  await writeMockTSV(
    "title.episode.tsv",
    ["tconst\tparentTconst\tseasonNumber\tepisodeNumber"].join("\n"),
  );

  await writeMockTSV(
    "title.crew.tsv",
    ["tconst\tdirectors\twriters", "tt7770001\tnm7770002\t\\N"].join("\n"),
  );

  await writeMockTSV(
    "title.principals.tsv",
    [
      "tconst\tordering\tnconst\tcategory\tjob\tcharacters",
      'tt7770001\t1\tnm7770001\tactor\t\\N\t["Test Role"]',
    ].join("\n"),
  );
});

afterAll(async () => {
  await resetMysql(pool);

  const client = redis.getClient();
  const keys = await client.keys("*");
  for (const key of keys) {
    if (key.includes("pipeline") || key.includes("task_")) {
      await client.del(key);
    }
  }

  await mysqlDb.close();
  await redis.close();
  await fs.rm(tmpDir, { recursive: true, force: true });
});

describe("runPipeline (skipDownload)", () => {
  it("mock 데이터로 전체 파이프라인 실행", async () => {
    const { config } = await import("../../config/index.js");
    const { runPipeline } = await import("../../worker/index.js");

    const testConfig = {
      ...config,
      datasets: {
        ...config.datasets,
        downloadDir: tmpDir,
      },
    };

    const result = await runPipeline({
      redis,
      mysqlCommand: mysqlCmd,
      config: testConfig,
      skipDownload: true,
    });

    expect(result.totalInserted).toBeGreaterThan(0);
    expect(result.batchId).toBeDefined();

    // TITLES 확인
    const [titles] = await pool.query<mysql.RowDataPacket[]>(
      "SELECT tconst FROM TITLES ORDER BY tconst",
    );
    expect(titles.length).toBe(3);

    // PERSONS 확인
    const [persons] = await pool.query<mysql.RowDataPacket[]>(
      "SELECT nconst FROM PERSONS ORDER BY nconst",
    );
    expect(persons.length).toBe(2);

    // TITLE_GENRES 확인 (Action, Drama, Comedy, Documentary = 4)
    const [genres] = await pool.query<mysql.RowDataPacket[]>(
      "SELECT COUNT(*) as cnt FROM TITLE_GENRES",
    );
    expect((genres as any)[0].cnt).toBe(4);

    // RATINGS 확인 (secondary - FK 의존)
    const [ratings] = await pool.query<mysql.RowDataPacket[]>(
      "SELECT tconst FROM RATINGS ORDER BY tconst",
    );
    expect(ratings.length).toBe(2);

    // TITLE_CREW 확인
    const [crew] = await pool.query<mysql.RowDataPacket[]>(
      "SELECT tconst FROM TITLE_CREW",
    );
    expect(crew.length).toBe(1);

    // TITLE_PRINCIPALS 확인
    const [principals] = await pool.query<mysql.RowDataPacket[]>(
      "SELECT tconst FROM TITLE_PRINCIPALS",
    );
    expect(principals.length).toBe(1);

    // TITLE_AKAS 확인
    const [akas] = await pool.query<mysql.RowDataPacket[]>(
      "SELECT tconst FROM TITLE_AKAS",
    );
    expect(akas.length).toBe(1);
  }, 30000);
});
