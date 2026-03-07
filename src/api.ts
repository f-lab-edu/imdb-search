import "dotenv/config";
import { config } from "./config/index.js";
import { MysqlDatabase } from "./db/mysql/connection.js";
import { MysqlQuery } from "./db/mysql/queries.js";
import { OpenSearchDatabase } from "./db/opensearch/connection.js";
import { OpenSearchQuery } from "./db/opensearch/queries.js";
import { createApp } from "./server.js";

const PORT = Number(process.env.PORT) || 3000;

const mysqlDb = new MysqlDatabase(config.db.mysql);
const osDb = new OpenSearchDatabase(config.db.opensearch);
await mysqlDb.ping();
await osDb.ping();

const osQuery = new OpenSearchQuery(osDb.getClient());
const mysqlQuery = new MysqlQuery(mysqlDb.getPool());
const app = createApp(osQuery, mysqlQuery);

app.listen(PORT, () => {
  console.log(`server running on port: ${PORT}`);
});
