import { type Tconfig } from "./config/index.js";
import { MysqlCommand } from "./db/mysql/commands.js";
import { MysqlDatabase } from "./db/mysql/connection.js";
import { OpenSearchDatabase } from "./db/opensearch/connection.js";
import { RedisDatabase } from "./db/redis.js";
import { runIndexPipeline } from "./worker/indexer.js";

export async function startIndexer(config: Tconfig) {
  const mysqlDb = new MysqlDatabase(config.db.mysql);
  const mysqlCmd = await MysqlCommand.create(mysqlDb.getPool());
  const osDb = new OpenSearchDatabase(config.db.opensearch);
  const redis = await RedisDatabase.create(config.db.redis);

  await osDb.ping();

  try {
    await runIndexPipeline({
      cfg: config,
      mysqlCmd,
      redis,
      osClient: osDb.getClient(),
    });
  } finally {
    await redis.close();
    await mysqlDb.close();
    await osDb.close();
  }
}
