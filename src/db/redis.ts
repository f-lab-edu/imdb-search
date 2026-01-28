import { createClient, type RedisClientOptions } from "redis";

export class RedisDatabase {
  private client: ReturnType<typeof createClient>;

  constructor(config?: RedisClientOptions) {
    this.client = createClient(config);
  }

  getClient() {
    return this.client;
  }

  async ping(): Promise<void> {
    if (!this.client.isOpen) {
      await this.client.connect();
    }

    await this.client.ping();
    console.log("redis connected");
  }

  async close(): Promise<void> {
    await this.client.quit();
  }
}
