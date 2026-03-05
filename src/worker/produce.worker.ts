// import { parentPort, workerData } from "node:worker_threads";
// import { RedisDB } from "../db/index.js";
// import { TaskProducer } from "./producer.js";
// import path from "node:path";
// import {
//   ProducerCommand,
//   WorkerResponseType,
//   type MainRequestPayload,
// } from "./messages.js";
// import type { DatasetKey } from "../utils/types.js";
// const { batchId, config } = workerData;
//
// (async () => {
//   console.log(`producing tasks for ${batchId}`);
//
//   const producer = new TaskProducer(
//     RedisDB,
//     config.task,
//     config.datasets,
//     batchId,
//   );
//
//   parentPort?.on("message", async (msg: MainRequestPayload) => {
//     try {
//       switch (msg.type) {
//         case ProducerCommand.PRODUCE_DOWNLOAD:
//           console.log(`[Producer] Starting download task generation...`);
//
//           for (const file of config.datasets.files) {
//             await producer.produceDownloadTask(
//               config.datasets.baseUrl + file.name,
//               path.join(config.datasets.downloadDir, file.name),
//             );
//           }
//
//           parentPort?.postMessage(WorkerResponseType.DONE);
//
//           break;
//
//         case ProducerCommand.PRODUCE_PARSE_PRIMARY:
//           if (!msg.payload) return;
//
//           const payload = msg.payload as {
//             filePath: string;
//             datasetType: DatasetKey;
//             isPrimary: boolean;
//           };
//
//           const { filePath, datasetType, isPrimary } = payload;
//
//           if (isPrimary) {
//             await producer.producePhase1Task(path.basename(filePath));
//           } else {
//             // send it to hold queue
//             await producer.hold(datasetType, path.basename(filePath));
//           }
//
//           parentPort?.postMessage({
//             type: WorkerResponseType.DONE,
//             workerType: "PRODUCER",
//             command: ProducerCommand.PRODUCE_PARSE_PRIMARY,
//             payload: { datasetType },
//           });
//
//           break;
//
//         case ProducerCommand.PRODUCE_PARSE_SECONDARY:
//           // exit process for now
//           parentPort?.postMessage(WorkerResponseType.DONE);
//           setTimeout(() => process.exit(0), 100);
//           break;
//
//         case ProducerCommand.SHUTDOWN:
//           console.log("[producer] shutting down...");
//           RedisDB.close();
//           process.exit(0);
//
//         default:
//           console.warn(`[producer] unknown command: ${msg}`);
//       }
//     } catch (err) {
//       console.error("[producer error]", err);
//
//       parentPort?.postMessage({
//         type: WorkerResponseType.ERROR,
//         error: (err as Error).message,
//       });
//     }
//   });
// })();
