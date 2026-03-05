import "dotenv/config";
import { config } from "./config/index.js";
import { RedisDatabase } from "./db/redis.js";
import { MysqlDatabase } from "./db/mysql/connection.js";
import { MysqlCommand } from "./db/mysql/commands.js";
import { runPipeline } from "./worker/index.js";

const skipDownload = process.argv.includes("--skip-download");

const redis = await RedisDatabase.create(config.db.redis);
const mysqlDb = new MysqlDatabase(config.db.mysql);
const mysqlCmd = await MysqlCommand.create(mysqlDb.getPool());

try {
  const result = await runPipeline({
    redis,
    mysqlCommand: mysqlCmd,
    config,
    skipDownload,
  });
  console.log(
    `done. inserted: ${result.totalInserted.toLocaleString()}, batchId: ${result.batchId}`
  );
} finally {
  await redis.close();
  await mysqlDb.close();
}
