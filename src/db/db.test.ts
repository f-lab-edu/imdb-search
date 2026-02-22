import dotenv from "dotenv";
import path from "node:path";
import fs from "node:fs";
import mysql from "mysql2/promise";
import { generateTSVlines } from "../utils/parse.js";
import type { NameBasics, TableName, TitleBasics } from "../utils/types.js";

dotenv.config({ path: ".env.dev" });

const resetMysql = async (pool: mysql.Pool) => {
  let conn = null;

  try {
    conn = await pool.getConnection();

    await conn.query("SET FOREIGN_KEY_CHECKS = 0");
    await conn.query(
      "DROP TABLE IF EXISTS TITLE_GENRES, TITLE_AKAS, RATINGS, EPISODES, TITLE_PRINCIPALS, TITLE_CREW, TITLES, PERSONS, GENRES;",
    );
    await conn.query("SET FOREIGN_KEY_CHECKS = 1");

    console.log("reset db ok");
  } catch (err) {
    console.error(`failed to reset db: ${(err as Error).message}`);
  } finally {
    conn?.release();
  }
};

// 연결 테스트만
(async () => {
  const { MysqlDB, RedisDB, OpenSearchDB } = await import("./index.js");
  const { config } = await import("../config/index.js");

  try {
    const result = await Promise.allSettled([
      RedisDB.ping(),
      OpenSearchDB.ping(),
      MysqlDB.init(),
    ]);

    console.log(result);
  } catch (error) {
    console.error(`connection failed: ${(error as Error).message}`);
  } finally {
    await RedisDB.close();
    await OpenSearchDB.close();
  }

  console.log(config.datasets.downloadDir);

  // mysql 입력 테스트
  try {
    const { MysqlCommand } = await import("./index.js");

    console.time("insert test");

    const tsv = path.join(config.datasets.downloadDir, "name.basics.tsv");
    const maxConcurrent = 15; // same as db pool thread restrictions

    let cnt = 0;
    let data: NameBasics[] = [];
    let promises = [];

    for await (const line of generateTSVlines<NameBasics>(tsv)) {
      data.push(line);

      if (data.length >= config.db.mysql.batchSize) {
        promises.push(MysqlCommand.insertNameBasics([...data]));
        cnt += data.length;
        data = [];

        if (promises.length >= maxConcurrent) {
          await Promise.all(promises);
          console.log(`inserted ${cnt} lines`);
          promises = [];
        }
      }
    }

    if (promises.length > 0) {
      await Promise.all(promises);
      console.log(`inserted ${cnt} lines`);
    }

    if (data.length > 0) {
      await MysqlCommand.insertNameBasics(data);
      cnt += data.length;
      console.log(`final insert: ${cnt} lines`);
    }
  } catch (err) {
    console.error((err as Error).message);
  } finally {
    await resetMysql(MysqlDB.getPool());
    await MysqlDB.close();
    console.timeEnd("insert test");
  }
})();

const inertTitleBasicsTest = async (config: any, mysqlCommand: any) => {
  let cnt = 0;
  const tsv = path.join(config.datasets.downloadDir, "title.basics.tsv");

  let data: TitleBasics[] = [];

  for await (const line of generateTSVlines<TitleBasics>(tsv)) {
    data.push(line);

    if ((cnt + data.length) % 100 === 0) {
      console.log(`Reading... ${cnt + data.length} lines`);
    }

    if (data.length > config.db.mysql.batchSize) {
      try {
        console.log(
          `[${new Date().toISOString()}] Inserting batch ${Math.floor(cnt / 1000) + 1}...`,
        );

        await mysqlCommand.insertTitleBasics(data);

        cnt += data.length;
        console.log(`[${new Date().toISOString()}] ${cnt} lines`);
        data.length = 0;
      } catch (err) {
        console.error(`insert error: ${(err as Error).message}`);

        fs.writeFileSync("failed.json", JSON.stringify(data));

        throw err;
      }
    }
  }

  if (data.length > 0) {
    try {
      await mysqlCommand.insertTitleBasics(data);
      cnt += data.length;

      console.log(`successfully inserted final batch: total ${cnt} lines`);

      data.length = 0;
    } catch (err) {
      console.error(`insert error: ${(err as Error).message}`);

      fs.writeFileSync("failed2.json", JSON.stringify(data));

      throw err;
    }
  }
};
