import { MysqlCommand } from "../db/index.js";
import { RedisDatabase } from "../db/redis.js";
import type { TaskHandlerMap } from "./handlers.js";
import type { Producer } from "./producer.js";
import { isValidTask, TaskPhase, type TaskConfig } from "./types.js";

export class Consumer {
  // cfg
  private readonly maxWorkers: number;
  private readonly maxRetry: number;
  private readonly mainQueue: string;

  // state
  private isRunning = false;
  private workers: Set<Promise<void>> = new Set();
  private currentPhase: TaskPhase;

  // dependencies
  private readonly redis: ReturnType<RedisDatabase["getClient"]>;
  private readonly mysqlCmd: MysqlCommand;
  private readonly producer: Producer;
  private readonly handlers: TaskHandlerMap;

  constructor(
    batchId: string,
    cfg: TaskConfig,
    redis: RedisDatabase,
    mysqlCmd: MysqlCommand,
    producer: Producer,
    handlers: TaskHandlerMap,
    currPhase?: TaskPhase,
  ) {
    this.maxWorkers = cfg.maxWorkers;
    this.maxRetry = cfg.maxRetry;
    this.mainQueue = `${batchId}:${cfg.mainQueue}`;

    this.currentPhase = currPhase ?? TaskPhase.DOWNLOAD;

    this.redis = redis.getClient();
    this.mysqlCmd = mysqlCmd;
    this.producer = producer;
    this.handlers = handlers;
  }

  async start() {
    this.isRunning = true;
    console.log(`[consumer] started with max workers: ${this.maxWorkers}`);

    while (this.isRunning) {
      const availableSlots = this.maxWorkers - this.workers.size;

      if (availableSlots <= 0) {
        await Promise.race(this.workers);
        continue;
      }

      try {
        if (!this.isRunning) break;

        const taskElements = await this.redis.lPopCount(
          this.mainQueue,
          availableSlots,
        );

        if (!taskElements || taskElements.length === 0) {
          const refilled = await this.producer.refill(
            this.currentPhase,
            availableSlots,
          );

          if (refilled === 0) {
            if (this.workers.size === 0) {
              if (this.currentPhase === TaskPhase.DOWNLOAD) {
                this.currentPhase = TaskPhase.PRIMARY;
              } else if (this.currentPhase === TaskPhase.PRIMARY) {
                this.currentPhase = TaskPhase.SECONDARY;
              } else {
                console.log("[consumer] no more tasks, stopping...");
                break;
              }
            }

            await new Promise((res) => setTimeout(res, 500));
          }

          continue;
        }

        for (const element of taskElements) {
          const taskPromise: Promise<void> = this.executeTask(element).finally(
            () => {
              this.workers.delete(taskPromise);
            },
          );

          this.workers.add(taskPromise);
        }
      } catch (err) {
        if (!this.isRunning) break;
        console.error(`[consumer] queue error: ${(err as Error).message}`);
        await new Promise((res) => setTimeout(res, 1000));
      }
    }

    if (this.workers.size > 0) {
      console.log(`[consumer] finalizing ${this.workers.size} tasks...`);
      await Promise.allSettled(this.workers);
    }

    console.log("[consumer] stopped safely.");
  }

  stop() {
    this.isRunning = false;
  }

  async executeTask(element: string) {
    let task: ReturnType<typeof JSON.parse>;

    try {
      task = JSON.parse(element);
    } catch {
      console.error(`[consumer] invalid task JSON: ${element}`);
      return;
    }

    if (!isValidTask(task)) {
      console.error(`[consumer] invalid task shape: ${element}`);
      return;
    }

    const handler = this.handlers[task.name];
    if (!handler) {
      console.error(`[consumer] no handler registered for: ${task.name}`);
      return;
    }

    try {
      await handler(task);
    } catch (err) {
      console.error(
        `[consumer] task ${task.taskId} failed: ${(err as Error).message}`,
      );
      if (task.retryCount < this.maxRetry) {
        task.retryCount++;
        await this.redis.rPush(this.mainQueue, JSON.stringify(task));
      } else {
        await this.mysqlCmd.markTaskFailed(task.taskId);
      }
    }
  }
}
