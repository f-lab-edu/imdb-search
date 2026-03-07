import "dotenv/config";
import { config } from "./config/index.js";
import { MysqlCommand } from "./db/mysql/commands.js";
import { MysqlDatabase } from "./db/mysql/connection.js";
import { RedisDatabase } from "./db/redis.js";
import { OpenSearchDatabase } from "./db/opensearch/connection.js";
import { runPipeline } from "./worker/index.js";
import { runIndexPipeline } from "./worker/indexer.js";

const indexOnly = process.argv.includes("--index");

const mysqlDb = new MysqlDatabase(config.db.mysql);
const mysqlCmd = await MysqlCommand.create(mysqlDb.getPool());
const osDb = new OpenSearchDatabase(config.db.opensearch);

try {
  if (indexOnly) {
    await osDb.ping();
    await runIndexPipeline({ mysqlPool: mysqlDb.getPool(), osClient: osDb.getClient() });
  } else {
    const redis = await RedisDatabase.create(config.db.redis);
    try {
      await runPipeline(config, mysqlCmd, redis);
    } finally {
      await redis.close();
    }
  }
} finally {
  await mysqlDb.close();
  await osDb.close();
}
