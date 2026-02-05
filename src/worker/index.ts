import {
  type Task,
  isValidTask,
  TaskName,
  type DownloadPayload,
  type ParsePayload,
} from "./types.js";
import { RedisDB } from "../db/index.js";
import { handleParseAndInsert, hanldeDownloadTask } from "./handlers.js";

export class TaskRunner {
  private readonly maxWorkers = 10; // TODO: 설정 받아서 거기서 가져오기
  private readonly maxRetry = 3; // TODO: 설정 받아서 거기서 가져오기
  private readonly queueName = "q_name"; // TODO: 설정 받아서 거기서 가져오기
  private isRunning = false;

  private readonly redis;
  private workers: Promise<unknown>[] = [];

  constructor(redis: typeof RedisDB) {
    this.redis = redis.getClient();
  }

  async pushTask(task: Task) {
    await this.redis.lPush(this.queueName, JSON.stringify(task));
  }

  async start() {
    this.isRunning = true;

    while (this.isRunning) {
      if (this.workers.length >= this.maxWorkers) {
        await Promise.race(this.workers);
        continue;
      }

      try {
        if (!this.isRunning) break;

        const taskData = await this.redis.blPop(this.queueName, 1);

        if (!this.isRunning || !taskData) continue;

        const taskPromise = this.executeTask(taskData.element).finally(() => {
          this.workers = this.workers.filter((p) => p !== taskPromise);
        });

        this.workers.push(taskPromise);
      } catch (err) {
        if (!this.isRunning) break;
        console.error(`Queue Error: ${(err as Error).message}`);
      }
    }

    if (this.workers.length > 0) {
      console.log(`waiting for ${this.workers.length} workers to finish...`);
      await Promise.allSettled(this.workers);
    }

    console.log("task runner stopped safely");
  }

  stop() {
    this.isRunning = false;
  }

  async executeTask(taskElement: string) {
    let task: Task | null = null;

    try {
      task = JSON.parse(taskElement);

      if (!isValidTask(task)) {
        console.error(`invalid task data: ${taskElement}`);
        return; // throw error?
      }

      switch (task.name) {
        case TaskName.DOWNLOAD:
          const nextTask = await hanldeDownloadTask(
            task.id,
            task.payload as DownloadPayload,
          ); // task 타입 가드에서 payload까지 체크해서 괜찮다

          await this.redis.rPush(this.queueName, JSON.stringify(nextTask));

          break;

        case TaskName.PARSE_AND_INSERT:
          await handleParseAndInsert(task.id, task.payload as ParsePayload);
          break;
        default:
          console.warn(`unknown task name: ${task.name}`);
      }
    } catch (err) {
      console.error(`Task Error: ${(err as Error).message}`);

      if (task && task.retryCount <= this.maxRetry) {
        task.retryCount++;
        await this.redis.rPush(this.queueName, JSON.stringify(task));
      } else {
        console.error(`task failed with max retry`);
        // TODO: 따로 실패 기록 저장해두고 나중에 다시 하던지 하기
      }
    }
  }
}
