import dotenv from "dotenv";
import path from "node:path";
import mysql from "mysql2/promise";
import { generateTSVlines } from "../utils/parse.js";
import {
  isTableName,
  type DatasetType,
  type NameBasics,
  type TitleAkas,
  type TitleBasics,
  type TitleCrew,
  type TitleEpisode,
  type TitlePrincipals,
  type TitleRatings,
} from "../utils/types.js";

dotenv.config({ path: ".env.dev" });

const resetMysql = async (pool: mysql.Pool) => {
  let conn = null;

  try {
    conn = await pool.getConnection();

    await conn.query("SET FOREIGN_KEY_CHECKS = 0");
    await conn.query(
      "DROP TABLE IF EXISTS TITLE_GENRES, TITLE_AKAS, RATINGS, EPISODES, TITLE_PRINCIPALS, TITLE_CREW, TITLES, PERSONS, GENRES;"
    );
    await conn.query("SET FOREIGN_KEY_CHECKS = 1");

    console.log("reset db ok");
  } catch (err) {
    console.error(`failed to reset db: ${(err as Error).message}`);
  } finally {
    conn?.release();
  }
};

interface TestConfig {
  filePath: string;
  insertFn: (data: DatasetType[]) => Promise<void>;
  batchSize: number;
  maxConcurrent?: number;
  maxLines?: number;
}

const testInsert = async (tcfg: TestConfig) => {
  console.time("insert test");

  try {
    const maxConcurrent = tcfg.maxConcurrent || 15;
    let data = [];
    let promises = [];
    let lines = 0;
    let batchCount = 0;

    for await (const line of generateTSVlines(tcfg.filePath)) {
      data.push(line);
      lines += 1;

      if (data.length >= tcfg.batchSize) {
        console.log(`inserted ${lines} lines`);

        promises.push(tcfg.insertFn([...data]));

        if (promises.length >= maxConcurrent) {
          console.log(`Batch ${batchCount++}: lines ${lines - data.length + 1} to ${lines}`);

          await Promise.all([...promises]);
          promises = [];
        }

        data = [];
      }

      if (tcfg.maxLines && lines >= tcfg.maxLines) {
        break;
      }
    }

    if (promises.length > 0) {
      await Promise.all([...promises]);
    }

    if (data.length > 0) {
      await tcfg.insertFn(data);
      console.log(`inserted ${lines} lines`);
    }

    console.log("insert test ok");
  } catch (err) {
    console.error(`test failed: ${(err as Error).message}`);
    throw err;
  } finally {
    console.timeEnd("insert test");
  }
};

// 연결 테스트만
(async () => {
  const { MysqlDB, RedisDB, OpenSearchDB } = await import("./index.js");
  const { config } = await import("../config/index.js");

  try {
    const result = await Promise.allSettled([RedisDB.ping(), OpenSearchDB.ping(), MysqlDB.init()]);

    console.log(result);
  } catch (error) {
    console.error(`connection failed: ${(error as Error).message}`);
  } finally {
    await RedisDB.close();
    await OpenSearchDB.close();
    await MysqlDB.close();
  }

  console.log(config.datasets.downloadDir);

  // mysql 입력 테스트
  // try {
  //   const { MysqlCommand } = await import("./index.js");
  //
  //   const insertTableName = process.env.TABLE_NAME;
  //
  //   if (!insertTableName || !isTableName(insertTableName)) {
  //     throw new Error("invalid table name");
  //   }
  //
  //   const { filePath, insertFn } = (() => {
  //     switch (insertTableName) {
  //       case "TITLES":
  //       case "GENRES":
  //       case "TITLE_GENRES":
  //         return {
  //           filePath: path.join(
  //             config.datasets.downloadDir,
  //             "title.basics.tsv",
  //           ),
  //           insertFn: (data: any[]) =>
  //             MysqlCommand.insertTitleBasics(data as TitleBasics[]),
  //         };
  //       case "RATINGS":
  //         return {
  //           filePath: path.join(
  //             config.datasets.downloadDir,
  //             "title.ratings.tsv",
  //           ),
  //           insertFn: (data: any[]) =>
  //             MysqlCommand.insertTitleRatings(data as TitleRatings[]),
  //         };
  //       case "EPISODES":
  //         return {
  //           filePath: path.join(
  //             config.datasets.downloadDir,
  //             "title.episode.tsv",
  //           ),
  //           insertFn: (data: any[]) =>
  //             MysqlCommand.insertTitleEpisodes(data as TitleEpisode[]),
  //         };
  //       case "PERSONS":
  //         return {
  //           filePath: path.join(config.datasets.downloadDir, "name.basics.tsv"),
  //           insertFn: (data: any[]) =>
  //             MysqlCommand.insertNameBasics(data as NameBasics[]),
  //         };
  //       case "TITLE_PRINCIPALS":
  //         return {
  //           filePath: path.join(
  //             config.datasets.downloadDir,
  //             "title.principals.tsv",
  //           ),
  //           insertFn: (data: any[]) =>
  //             MysqlCommand.insertTitlePrincipals(data as TitlePrincipals[]),
  //         };
  //       case "TITLE_CREW":
  //         return {
  //           filePath: path.join(config.datasets.downloadDir, "title.crew.tsv"),
  //           insertFn: (data: any[]) =>
  //             MysqlCommand.insertTitleCrew(data as TitleCrew[]),
  //         };
  //       case "TITLE_AKAS":
  //         return {
  //           filePath: path.join(config.datasets.downloadDir, "title.akas.tsv"),
  //           insertFn: (data: any[]) =>
  //             MysqlCommand.insertTitleAkas(data as TitleAkas[]),
  //         };
  //     }
  //   })();
  //
  //   const tcfg: TestConfig = {
  //     filePath,
  //     insertFn,
  //     batchSize: config.db.mysql.batchSize,
  //     maxConcurrent: config.db.mysql.maxConcurrent,
  //     // maxLines: 1000,
  //   };
  //
  //   await testInsert(tcfg);
  // } catch (err) {
  //   console.error((err as Error).message);
  // } finally {
  //   await MysqlDB.close();
  // }
})();
