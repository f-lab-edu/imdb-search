import { parentPort, workerData } from "node:worker_threads";
import { MysqlCommand, RedisDB } from "../db/index.js";
import { TaskConsumer } from "./consumer.js";
import {
  ConsumerCommand,
  WorkerResponseType,
  type MainRequestPayload,
  type WorkerResponsePayload,
} from "./messages.js";
const { batchId, config } = workerData;

(async () => {
  const consumer = new TaskConsumer(RedisDB, batchId, MysqlCommand, config.task);
  let isProcessing = false;
  let currentPhase = "PHASE1";

  console.log(`starting consumer for ${batchId}`);

  parentPort?.on("message", async (msg: MainRequestPayload) => {
    switch (msg.type) {
      case ConsumerCommand.START:
        if (isProcessing) {
          console.log("[consumer] already running...");
          return;
        }

        parentPort?.postMessage({
          type: WorkerResponseType.READY,
          workerType: "CONSUMER",
          command: msg.type,
        } as WorkerResponsePayload);

        isProcessing = true;

        console.log("[consumer] Starting task consumption...");

        try {
          await consumer.start();
          // parentPort?.postMessage({ type: "CONSUME_STOPPED", reason: "finished" });

          parentPort?.postMessage({
            type: WorkerResponseType.DONE,
            workerType: "CONSUMER",
            command: ConsumerCommand.START,
            payload: {
              status: "FINISHED",
              phase: currentPhase,
            },
          });
        } catch (err) {
          console.error("[consumer error]", err);
          parentPort?.postMessage({
            type: WorkerResponseType.ERROR,
            workerType: "CONSUMER",
            command: msg.type,
            error: (err as Error).message,
          });
        } finally {
          isProcessing = false;
        }

        break;

      case ConsumerCommand.STOP:
        console.log("[consumer] Stopping...");
        consumer.stop(); // consumer 내부에 isRunning = false 로직 필요
        isProcessing = false;
        break;

      case ConsumerCommand.TERMINATE:
        process.exit(0);

      default:
        console.warn(`[consumer] unknown command type: ${msg.type || "unknown"}`);
    }
  });
})();
