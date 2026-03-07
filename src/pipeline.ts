import { type Tconfig } from "./config/index.js";
import { MysqlCommand } from "./db/mysql/commands.js";
import { MysqlDatabase } from "./db/mysql/connection.js";
import { RedisDatabase } from "./db/redis.js";
import { runPipeline } from "./worker/index.js";

// download source file -> read it and writes to mysql
export async function startPipeline(config: Tconfig) {
  const mysqlDb = new MysqlDatabase(config.db.mysql);
  const mysqlCmd = await MysqlCommand.create(mysqlDb.getPool());
  const redis = await RedisDatabase.create(config.db.redis);

  try {
    await runPipeline(config, mysqlCmd, redis);
  } finally {
    await redis.close();
    await mysqlDb.close();
  }
}
