import dotenv from "dotenv";
import path from "node:path";
import fs from "node:fs";
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
    const { MysqlCommand } = await import("./index.js");

    console.time("insert test");

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

          await MysqlCommand.insertTitleBasics(data);

          cnt += data.length;
          console.log(`[${new Date().toISOString()}] ✓ ${cnt} lines`);
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
        await MysqlCommand.insertTitleBasics(data);
        cnt += data.length;

        console.log(`successfully inserted final batch: total ${cnt} lines`);

        data.length = 0;
      } catch (err) {
        console.error(`insert error: ${(err as Error).message}`);

        fs.writeFileSync("failed2.json", JSON.stringify(data));

        throw err;
      }
    }
  } catch (err) {
    console.error((err as Error).message);
  } finally {
    await MysqlDB.close();
    console.timeEnd("insert test");
  }
})();
