import path from "node:path";
import dotenv from "dotenv";

dotenv.config({ path: ".env.dev" });

(async () => {
  const { MysqlDB, RedisDB, OpenSearchDB, MysqlCommand } = await import("../db/index.js");
  const { TaskRunner } = await import("./index.js");
  const { config } = await import("../config/index.js");
  const { TaskName } = await import("./types.js");

  const tr = new TaskRunner(RedisDB, MysqlCommand, config.task);

  try {
    await RedisDB.ping();

    // push download tasks
    for (const file of config.datasets.files) {
      await tr.pushTask({
        batchId: tr.getBatchId(),
        taskId: crypto.randomUUID(),
        name: TaskName.DOWNLOAD,
        payload: {
          url: config.datasets.baseUrl + file.name,
          targetPath: path.join(config.datasets.downloadDir, file.name),
        },
        retryCount: 0,
        createdAt: Date.now(),
      });
    }

    const runnerPromise = tr.start();
    await runnerPromise;
  } catch (err) {
    console.error(`error occured: ${err}`);
  } finally {
    await RedisDB.close();
    await MysqlDB.close();
    await OpenSearchDB.close();

    console.log("test finished");
  }
})();
