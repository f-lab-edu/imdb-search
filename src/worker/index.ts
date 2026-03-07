import path from "node:path";
import type { Tconfig } from "../config/index.js";
import type { MysqlCommand, RedisDatabase } from "../db/index.js";
import { Consumer } from "./consumer.js";
import { createHandlers } from "./handlers.js";
import { Producer } from "./producer.js";

export const runPipeline = async (cfg: Tconfig, mysqlCmd: MysqlCommand, redis: RedisDatabase) => {
  const batchId = crypto.randomUUID();
  const producer = new Producer(batchId, cfg.task, redis, mysqlCmd);
  const handlers = createHandlers(redis, mysqlCmd, producer, cfg.task.batchSize);
  const consumer = new Consumer(batchId, cfg.task, redis, mysqlCmd, producer, handlers);

  await triggerDownload(cfg, producer);

  await consumer.start();
};

const triggerDownload = async (cfg: Tconfig, producer: Producer) => {
  // trigger download job
  const downloadTaskPromises = [];
  for (const file of cfg.datasets.files) {
    const url = `${cfg.datasets.baseUrl}${file.name}`;
    const targetPath = path.join(cfg.datasets.downloadDir, file.name.replaceAll(".gz", ""));

    downloadTaskPromises.push(producer.produceDownloadTask(url, targetPath));
  }

  const results = await Promise.allSettled(downloadTaskPromises);

  const failed = results.filter((r) => r.status === "rejected");
  if (failed.length > 0) {
    failed.forEach((r) =>
      console.error(`[producer] download task failed:`, (r as PromiseRejectedResult).reason)
    );
  }
};
