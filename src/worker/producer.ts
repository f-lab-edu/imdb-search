import type { PipelineOptions } from "./types.js";
import type { RedisDatabase, MysqlCommand } from "../db/index.js";
import {
  type TaskConfig,
  type Task,
  type DownloadPayload,
  type IndexBatchPayload,
  TaskName,
  TaskPhase,
} from "./types.js";

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
    mysqlCmd: MysqlCommand,
  ) {
    this.batchId = batchId;
    this.mainQueue = `${batchId}:${taskConfig.mainQueue}`;

    this.redis = redis.getClient();
    this.mysqlCmd = mysqlCmd;
  }

  async produceDownloadTask(
    url: string,
    targetPath: string,
    pipeOpts: PipelineOptions,
  ) {
    const { skipDownload, skipLoadTSV } = pipeOpts;

    const downloadTask: Task<DownloadPayload> = {
      batchId: this.batchId,
      taskId: crypto.randomUUID(),
      name: TaskName.DOWNLOAD,
      payload: { url, targetPath, skipDownload, skipLoad: skipLoadTSV },
      retryCount: 0,
      createdAt: Date.now(),
    };

    await this.enqueue(downloadTask);
  }

  // add task to redis queue
  async enqueue(task: Task) {
    await this.redis.rPush(this.mainQueue, JSON.stringify(task));
  }

  // add task to db
  // TODO: need phase?
  async schedule(task: Task, phase: TaskPhase) {
    await this.mysqlCmd.insertTask(task, phase);
  }

  private async scheduleOneIndexTask(fromTconst: string | null, batchSize: number): Promise<string | null> {
    const tconsts = await this.mysqlCmd.fetchTconstPage(fromTconst, batchSize);
    if (tconsts.length === 0) return null;

    await this.schedule(
      {
        batchId: this.batchId,
        taskId: crypto.randomUUID(),
        name: TaskName.INDEX_BATCH,
        payload: { fromTconst, limit: batchSize } satisfies IndexBatchPayload,
        retryCount: 0,
        createdAt: Date.now(),
      },
      TaskPhase.INDEX,
    );

    return tconsts.length < batchSize ? null : tconsts[tconsts.length - 1]!;
  }

  async scheduleIndexTasks(batchSize: number, fromCursor: string | null): Promise<void> {
    let cursor: string | null = fromCursor;
    let scheduled = 0;

    while (cursor !== null) {
      cursor = await this.scheduleOneIndexTask(cursor, batchSize);
      scheduled++;
    }

    console.log(`[producer] scheduled ${scheduled} remaining INDEX_BATCH tasks`);
  }

  async scheduleFirstIndexTask(batchSize: number): Promise<string | null> {
    return this.scheduleOneIndexTask(null, batchSize);
  }

  async refill(phase: TaskPhase, limit: number): Promise<number> {
    const pending = await this.mysqlCmd.fetchPendingTasks(phase, limit);
    if (pending.length === 0) return 0;

    await Promise.allSettled(
      pending.map((t) => this.redis.rPush(this.mainQueue, JSON.stringify(t))),
    );

    await this.mysqlCmd.markTasksQueued(pending.map((t) => t.taskId));

    return pending.length;
  }
}
