import { type Tconfig } from "./config/index.js";
import { MysqlDatabase } from "./db/mysql/connection.js";
import { OpenSearchDatabase } from "./db/opensearch/connection.js";
import { runIndexPipeline } from "./worker/indexer.js";

// create index -> writes index to opensearch
export async function startIndexer(config: Tconfig) {
  const mysqlDb = new MysqlDatabase(config.db.mysql);
  const osDb = new OpenSearchDatabase(config.db.opensearch);

  await osDb.ping();

  try {
    await runIndexPipeline({ mysqlPool: mysqlDb.getPool(), osClient: osDb.getClient() });
  } finally {
    await mysqlDb.close();
    await osDb.close();
  }
}
