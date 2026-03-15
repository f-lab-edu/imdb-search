import path from "node:path";
import type { Tconfig } from "../config/index.js";
import { MysqlCommand, type RedisDatabase } from "../db/index.js";
import { MysqlIntegrity } from "../db/mysql/integrity.js";
import { Consumer } from "./consumer.js";
import { createHandlers } from "./handlers.js";
import { Producer } from "./producer.js";
import type { PipelineOptions } from "./types.js";

export const runPipeline = async (
  cfg: Tconfig,
  mysqlCmd: MysqlCommand,
  redis: RedisDatabase,
  options?: PipelineOptions,
) => {
  const batchId = crypto.randomUUID();
  const producer = new Producer(batchId, cfg.task, redis, mysqlCmd);
  const handlers = createHandlers(
    redis,
    mysqlCmd,
    producer,
    cfg.task.batchSize,
  );

  const consumer = new Consumer(
    batchId,
    cfg.task,
    redis,
    mysqlCmd,
    producer,
    handlers,
  );

  if (!options?.skipLoadTSV) {
    await triggerDownload(
      cfg,
      producer,
      options ?? { skipDownload: false, skipLoadTSV: false, skipNormalization: false, skipIntegrityCheck: false },
    );
    await consumer.start();
    console.log("[pipe] loading data done");
  }

  if (!options?.skipNormalization) {
    await normalizePrimary(mysqlCmd);
    await normalizeSecondary(mysqlCmd);
    console.log("[pipe] data normalization done");
  }

  if (!options?.skipIntegrityCheck) {
    console.log("[pipe] running integrity check...");
    console.time("integrity");
    const results = await mysqlCmd.integrity.checkAll();
    console.timeEnd("integrity");
    MysqlIntegrity.printResults(results);
  }
};

const triggerDownload = async (
  cfg: Tconfig,
  producer: Producer,
  pipeOpts: PipelineOptions,
) => {
  // trigger download job
  const downloadTaskPromises = [];

  for (const file of cfg.datasets.files) {
    const url = `${cfg.datasets.baseUrl}${file.name}`;
    const targetPath = path.join(
      cfg.datasets.downloadDir,
      file.name.replaceAll(".gz", ""),
    );

    downloadTaskPromises.push(
      producer.produceDownloadTask(url, targetPath, pipeOpts),
    );
  }

  const results = await Promise.allSettled(downloadTaskPromises);

  const failed = results.filter((r) => r.status === "rejected");

  if (failed.length > 0) {
    failed.forEach((r) =>
      console.error(
        `[producer] download task failed:`,
        (r as PromiseRejectedResult).reason,
      ),
    );
  }
};

const normalizePrimary = async (mysqlCmd: MysqlCommand) => {
  console.log("[normalize] primary: start");
  console.time("normalize:primary");

  await Promise.all([
    mysqlCmd.normalize.genres(),
    mysqlCmd.normalize.titles(),
    mysqlCmd.normalize.persons(),
  ]);

  await mysqlCmd.normalize.titleGenres();

  console.timeEnd("normalize:primary");
};

const normalizeSecondary = async (mysqlCmd: MysqlCommand) => {
  const normalize = mysqlCmd.normalize;

  console.log("[normalize] secondary: start");
  console.time("normalize:secondary");

  await Promise.all([
    normalize.ratings(),
    normalize.episodes(),
    normalize.titleAkas(),
    normalize.titleCrew(),
    normalize.titlePrincipals(),
  ]);

  console.timeEnd("normalize:secondary");
};
