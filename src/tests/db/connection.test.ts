import dotenv from "dotenv";
import { describe, beforeAll, afterAll, it, expect } from "@jest/globals";
import { resetMysql } from "./helpers.js";

dotenv.config({ path: ".env.test" });

describe("db connection test", () => {
  let MysqlDB: any, OpenSearchDB: any, RedisDB: any;

  beforeAll(async () => {
    const db = await import("../../db/index.js");
    const { config } = await import("../../config/index.js");

    MysqlDB = new db.MysqlDatabase(config.db.mysql);
    OpenSearchDB = new db.OpenSearchDatabase(config.db.opensearch);
    RedisDB = new db.RedisDatabase(config.db.redis);
  });

  afterAll(async () => {
    await resetMysql(MysqlDB.getPool());

    await MysqlDB.close();
    await RedisDB.close();
    await OpenSearchDB.close();
  });

  it("pings all database - mysql,redis,opensearch", async () => {
    const results = await Promise.allSettled([MysqlDB.ping(), RedisDB.ping(), OpenSearchDB.ping()]);

    results.forEach((res, idx) => {
      if (res.status == "rejected") {
        console.error(`failed to connect to db: ${idx}`);
      }

      expect(res.status).toBe("fulfilled");
    });
  });
});
