import type { Client } from "@opensearch-project/opensearch";
import type { Tconfig } from "../config/index.js";
import type { MysqlCommand } from "../db/index.js";
import type { RedisDatabase } from "../db/redis.js";
import { OpenSearchCommand } from "../db/opensearch/commands.js";
import { Consumer } from "./consumer.js";
import { createHandlers } from "./handlers.js";
import { Producer } from "./producer.js";
import { TaskPhase } from "./types.js";

interface IndexPipelineConfig {
  cfg: Tconfig;
  mysqlCmd: MysqlCommand;
  redis: RedisDatabase;
  osClient: Client;
  recreateIndex?: boolean;
}

export const runIndexPipeline = async ({
  cfg,
  mysqlCmd,
  redis,
  osClient,
  recreateIndex = false,
}: IndexPipelineConfig) => {
  const osCmd = new OpenSearchCommand(osClient);
  await osCmd.createIndex(recreateIndex);

  const batchId = crypto.randomUUID();
  const producer = new Producer(batchId, cfg.task, redis, mysqlCmd);
  const handlers = createHandlers(
    redis,
    mysqlCmd,
    producer,
    cfg.task.batchSize,
    osClient,
  );
  const consumer = new Consumer(
    batchId,
    cfg.task,
    redis,
    mysqlCmd,
    producer,
    handlers,
    TaskPhase.INDEX,
  );

  console.time("indexer:schedule");
  const firstCursor = await producer.scheduleFirstIndexTask(cfg.task.batchSize);

  const schedulePromise = (
    firstCursor !== null
      ? producer.scheduleIndexTasks(cfg.task.batchSize, firstCursor)
      : Promise.resolve()
  ).then(() => console.timeEnd("indexer:schedule"));

  console.time("indexer");
  const consumerPromise = consumer
    .start()
    .then(() => console.timeEnd("indexer"));

  await Promise.all([schedulePromise, consumerPromise]);
};
