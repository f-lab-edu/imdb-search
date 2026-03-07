import type mysql from "mysql2/promise";
import type { Client } from "@opensearch-project/opensearch";
import { OpenSearchCommand } from "../db/opensearch/commands.js";
import { fetchTitleDocuments } from "../db/mysql/queries.js";

interface IndexPipelineConfig {
  mysqlPool: mysql.Pool;
  osClient: Client;
  batchSize?: number;
  recreateIndex?: boolean;
}

export const runIndexPipeline = async ({
  mysqlPool,
  osClient,
  batchSize = 5000,
  recreateIndex = false,
}: IndexPipelineConfig) => {
  const osCmd = new OpenSearchCommand(osClient);

  await osCmd.createIndex(recreateIndex);

  let totalIndexed = 0;
  let batchCount = 0;

  console.log("[indexer] starting opensearch indexing from mysql");
  console.time("indexer");

  try {
    for await (const batch of fetchTitleDocuments(mysqlPool, batchSize)) {
      await osCmd.bulkIndex(batch);
      totalIndexed += batch.length;
      batchCount++;

      if (batchCount % 10 === 0) {
        console.log(`[indexer] progress: ${totalIndexed.toLocaleString()} documents indexed`);
      }
    }
  } finally {
    console.timeEnd("indexer");
  }

  console.log(`[indexer] done. ${totalIndexed.toLocaleString()} documents indexed`);
  return { totalIndexed };
};
