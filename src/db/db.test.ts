import dotenv from "dotenv";
import path from "node:path";
import { generateTSVlines } from "../utils/parse.js";
import type { TableName, TitleBasics } from "../utils/types.js";

dotenv.config({ path: ".env.dev" });

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
    console.log("starting to insert...");
    console.time("insert");

    const batch = [];
    const filePath = path.join(config.datasets.downloadDir, "title.basics.tsv");

    for await (const row of generateTSVlines<TitleBasics>(filePath)) {
      const { genres, ...titlesRow } = row;

      batch.push(titlesRow as TitleBasics);

      if (batch.length >= config.db.mysql.batchSize) {
        await MysqlDB.commands.bulkInsert<TitleBasics>("TITLES", batch);
        batch.length = 0;
      }
    }

    if (batch.length > 0) await MysqlDB.commands.bulkInsert("TITLES", batch);

    console.log("successfully inserted titles");
    console.timeEnd("insert");
  } catch (err) {
    console.error(`failed to insert: ${(err as Error).message}`);
  } finally {
    await MysqlDB.close();
  }
})();
