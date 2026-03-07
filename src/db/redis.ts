import { createClient, type RedisClientOptions } from "redis";

export class RedisDatabase {
  private client: ReturnType<typeof createClient>;

  constructor(config?: RedisClientOptions) {
    this.client = createClient(config);
  }

  static async create(config?: RedisClientOptions): Promise<RedisDatabase> {
    const instance = new RedisDatabase(config);
    await instance.connect();
    return instance;
  }

  private async connect(): Promise<void> {
    try {
      if (!this.client.isOpen) {
        await this.client.connect();
      }
      await this.client.ping();
      console.log("redis connected and ready");
    } catch (err) {
      console.error(`[redis error] failed to connect:`, err);
      throw err;
    }
  }

  getClient() {
    return this.client;
  }

  async close(): Promise<void> {
    if (this.client.isOpen) {
      await this.client.quit();
    }
  }

  async ping() {
    this.connect();
  }
}
