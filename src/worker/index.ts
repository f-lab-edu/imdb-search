import { Worker } from "node:worker_threads";
import {
  ConsumerCommand,
  ProducerCommand,
  WorkerResponseType,
  type WorkerResponsePayload,
} from "./messages.js";
import { TaskName } from "./types.js";
import type { DatasetKey } from "../utils/types.js";
import type { Tconfig } from "../config/index.js";

interface PipelineConfig {
  batchId: string;
  config: Tconfig;
  consumerCount?: number;
}

export const runPipeline = async ({
  batchId,
  config,
  consumerCount = 8,
}: PipelineConfig) => {
  const workers: Worker[] = [];
  let totalInserted = 0;
  let isDownloadStarted = false;
  let finishedConsumers = 0;

  return new Promise<{ totalInserted: number }>((resolve, reject) => {
    const producerWorker = new Worker(
      new URL("./produce.worker.js", import.meta.url),
      { workerData: { batchId, config } },
    );

    producerWorker.on("error", (err) => {
      console.error("[producer error]", err);
      reject(err);
    });

    for (let i = 0; i < consumerCount; i++) {
      const consumerWorker = new Worker(
        new URL("./consumer.worker.js", import.meta.url),
        { workerData: { batchId, config } },
      );

      consumerWorker.postMessage({ type: ConsumerCommand.START });

      consumerWorker.on("message", (msg: WorkerResponsePayload) => {
        switch (msg.type) {
          case WorkerResponseType.READY:
            console.log(`[main] consumer ${i} ready`);

            if (!isDownloadStarted) {
              producerWorker.postMessage({
                type: ProducerCommand.PRODUCE_DOWNLOAD,
              });
              isDownloadStarted = true;
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
                  isPrimary: taskResult.name === TaskName.PARSE_PRIMARY,
                },
              });
            }

            if (
              msg.command === TaskName.PARSE_PRIMARY ||
              msg.command === TaskName.PARSE_SECONDARY ||
              msg.command === TaskName.INSERT_DATA
            ) {
              const result = msg.payload as {
                count: number;
                datasetType: DatasetKey;
              };
              totalInserted += result.count;

              if (totalInserted % 100000 === 0) {
                console.log(
                  `[main] progress: ${totalInserted.toLocaleString()} rows inserted`,
                );
              }
            }

            if (taskResult?.phase) {
              finishedConsumers++;

              if (finishedConsumers === consumerCount) {
                console.log("[main] all consumers finished");

                producerWorker.postMessage({
                  type: ProducerCommand.SHUTDOWN,
                });

                workers.forEach((w) =>
                  w.postMessage({ type: ConsumerCommand.TERMINATE }),
                );

                resolve({ totalInserted });
              }
            }
            break;

          case WorkerResponseType.ERROR:
            console.error(`[consumer ${i} error]: ${msg.error}`);
            break;
        }
      });

      consumerWorker.on("error", (err) => {
        console.error(`[consumer ${i} error]`, err);
      });

      workers.push(consumerWorker);
    }
  });
};
