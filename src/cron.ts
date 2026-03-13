import cron from "node-cron";
import type { Tconfig } from "./config/index.js";
import { startPipeline } from "./pipeline.js";
import { startIndexer } from "./indexer.js";

const SCHEDULE = "0 2 * * *";

export const startCron = (config: Tconfig) => {
  const task = cron.schedule(SCHEDULE, async () => {
    console.log("[cron] starting pipeline...");
    await startPipeline(config);
    console.log("[cron] starting indexer...");
    await startIndexer(config);
    console.log("[cron] done.");
  });

  console.log(`[cron] scheduled: ${SCHEDULE}`);
  return task;
};
