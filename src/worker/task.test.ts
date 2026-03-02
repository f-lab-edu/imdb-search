import dotenv from "dotenv";
import { Worker } from "node:worker_threads";
import {
  ConsumerCommand,
  ProducerCommand,
  WorkerResponseType,
  type MainRequestPayload,
  type WorkerResponsePayload,
} from "./messages.js";
import { TaskName, type ParsePayload, type Task } from "./types.js";
import type { DatasetKey } from "../utils/types.js";

dotenv.config({ path: ".env.dev" });

(async () => {
  const { RedisDB, MysqlDB, MysqlCommand } = await import("../db/index.js");
  const { config } = await import("../config/index.js");

  let resolvePipeline: (value: unknown) => void;

  const pipelineFinished = new Promise((resolve) => {
    resolvePipeline = resolve;
  });

  try {
    const batchId = crypto.randomUUID();

    const producerWorker = new Worker(new URL("./produce.worker.js", import.meta.url), {
      workerData: { batchId, config },
    });

    let totalInserted = 0;

    const workers = [];

    producerWorker.on("message", () => {});

    let isDownloadStarte = false;
    let workerCnt = 0;

    for (let i = 0; i < 8; i++) {
      const consumerWorker = new Worker(new URL("./consumer.worker.js", import.meta.url), {
        workerData: { batchId, config },
      });

      consumerWorker.postMessage({ type: ConsumerCommand.START });

      consumerWorker.on("message", (msg: WorkerResponsePayload) => {
        switch (msg.type) {
          case WorkerResponseType.READY:
            console.log("consumer ready");
            if (!isDownloadStarte) {
              producerWorker.postMessage({ type: ProducerCommand.PRODUCE_DOWNLOAD });
              isDownloadStarte = true;
            }
            break;

          case WorkerResponseType.DONE:
            const taskResult = msg.payload as any;

            if (msg.command === TaskName.DOWNLOAD) {
              producerWorker.postMessage({
                type: ProducerCommand.PRODUCE_PARSE_PRIMARY,
                payload: {
                  filePath: taskResult.payload.filePath,
                  datasetType: taskResult.payload.datasetType,
                  isPrimary: taskResult.name == TaskName.PARSE_PRIMARY,
                },
              });
            }

            if (
              msg.command === TaskName.PARSE_PRIMARY ||
              msg.command === TaskName.PARSE_SECONDARY ||
              msg.command === TaskName.INSERT_DATA
            ) {
              const result = msg.payload as { count: number; datasetType: DatasetKey }; // { count: 1000, datasetType: "..." }
              totalInserted += result.count;

              if (totalInserted % 100000 === 0) {
                console.log(
                  `[Main] Progress: ${totalInserted.toLocaleString()} rows inserted into DB...`
                );
              }
            }

            if (taskResult.phase === "PHASE1") {
              workerCnt++;

              if (workerCnt === 8) {
                console.log("phase 1 finished");
              }
            }

            break;

          case WorkerResponseType.ERROR:
            console.error(`[consumer error]: ${msg.error}`);
            break;
        }
      });

      workers.push(consumerWorker);
    }

    console.log("[main] Pipeline is running. Waiting for events...");
    await pipelineFinished;
  } catch (err) {
    console.error(err);
  } finally {
    console.log("closing dbs");
    await MysqlDB.close();
    await RedisDB.close();
  }
})();

//(async () => {
//   const { MysqlDB, RedisDB, OpenSearchDB, MysqlCommand } = await import("../db/index.js");
//   const { TaskRunner } = await import("./index.js");
//   const { config } = await import("../config/index.js");
//   const { TaskName } = await import("./types.js");
//
//   console.time("task runner start");
//
//   const tr = new TaskRunner(RedisDB, MysqlCommand, config.task);
//
//   try {
//     await RedisDB.ping();
//
//     // push download tasks
//     for (const file of config.datasets.files) {
//       await tr.pushTask({
//         batchId: tr.getBatchId(),
//         taskId: crypto.randomUUID(),
//         name: TaskName.DOWNLOAD,
//         payload: {
//           url: config.datasets.baseUrl + file.name,
//           targetPath: path.join(config.datasets.downloadDir, file.name),
//         },
//         retryCount: 0,
//         createdAt: Date.now(),
//       });
//     }
//
//     const runnerPromise = tr.start();
//     await runnerPromise;
//   } catch (err) {
//     console.error(`error occured: ${err}`);
//   } finally {
//     await RedisDB.close();
//     await MysqlDB.close();
//     await OpenSearchDB.close();
//
//     console.log("test finished");
//     console.timeEnd("task runner start");
//   }
// })();
