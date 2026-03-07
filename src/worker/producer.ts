import type { RedisDatabase, MysqlCommand } from "../db/index.js";
import { type TaskConfig, type Task, type DownloadPayload, TaskName, TaskPhase } from "./types.js";

export class Producer {
  // cfg
  private readonly batchId: string;
  private readonly mainQueue: string;

  // dependencies
  private readonly redis: ReturnType<RedisDatabase["getClient"]>;
  private readonly mysqlCmd: MysqlCommand;

  constructor(
    batchId: string,
    taskConfig: TaskConfig,
    redis: RedisDatabase,
    mysqlCmd: MysqlCommand
  ) {
    this.batchId = batchId;
    this.mainQueue = `${batchId}:${taskConfig.mainQueue}`;

    this.redis = redis.getClient();
    this.mysqlCmd = mysqlCmd;
  }

  async produceDownloadTask(url: string, targetPath: string) {
    const downloadTask: Task<DownloadPayload> = {
      batchId: this.batchId,
      taskId: crypto.randomUUID(),
      name: TaskName.DOWNLOAD,
      payload: { url, targetPath },
      retryCount: 0,
      createdAt: Date.now(),
    };

    await this.mysqlCmd.insertTask(downloadTask, TaskPhase.DOWNLOAD);
  }

  async produce(task: Task, phase: TaskPhase) {
    await this.mysqlCmd.insertTask(task, phase);
  }

  async refill(phase: TaskPhase, limit: number): Promise<number> {
    const pending = await this.mysqlCmd.fetchPendingTasks(phase, limit);
    if (pending.length === 0) return 0;

    await Promise.allSettled(
      pending.map((t) => this.redis.rPush(this.mainQueue, JSON.stringify(t)))
    );
    await this.mysqlCmd.markTasksQueued(pending.map((t) => t.taskId));

    return pending.length;
  }
}
