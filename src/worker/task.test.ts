import dotenv from "dotenv";

dotenv.config({ path: ".env.dev" });

import path from "node:path";
import { TaskName } from "./types.js";

(async () => {
  const { MysqlDB, RedisDB, OpenSearchDB } = await import("../db/index.js");
  const { TaskRunner } = await import("./index.js");
  const { config } = await import("../config/index.js");

  const tr = new TaskRunner(RedisDB, config.task);

  try {
    await RedisDB.ping();

    const runnerPromise = tr.start();

    // push download tasks
    for (const file of config.datasets.files) {
      await tr.pushTask({
        id: Math.random().toString(),
        name: TaskName.DOWNLOAD,
        payload: {
          url: config.datasets.baseUrl + file.name,
          targetPath: path.join(config.datasets.downloadDir, file.name),
        },
        retryCount: 0,
        createdAt: Date.now(),
      });
    }

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
