import { Client, type ClientOptions } from "@opensearch-project/opensearch";

export class OpenSearchDatabase {
  private client: Client;

  constructor(config?: ClientOptions) {
    this.client = new Client({ ...config });
  }

  getClient() {
    return this.client;
  }

  async ping(): Promise<void> {
    await this.client.ping();
    console.log("opensearch connected");
  }

  async close() {
    await this.client.close();
  }
}
