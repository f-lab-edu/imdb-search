import { TaskRunner } from "./index.js";
import { TaskName, type ParsePayload, type Task } from "./types.js";
import { RedisDB, MysqlDB, OpenSearchDB } from "../db/index.js";

(async () => {
  const tr = new TaskRunner(RedisDB);

  try {
    await RedisDB.ping();

    const runnerPromise = tr.start();

    for (let i = 0; i < 20; i++) {
      const testTask: Task<ParsePayload> = {
        id: `taskid_${i}`,
        name: TaskName.PARSE_AND_INSERT,
        payload: { filePath: `filepath_${i}` },
        retryCount: 0,
        createdAt: Date.now(),
      };

      await tr.pushTask(testTask);
    }

    await new Promise((res) => setTimeout(res, 3000));

    tr.stop();

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
