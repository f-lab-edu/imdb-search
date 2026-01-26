import { MysqlDB, RedisDB, OpenSearchDB } from "./index.js";

// 연결 테스트만
(async () => {
  try {
    const result = await Promise.allSettled([
      MysqlDB.ping(),
      RedisDB.ping(),
      OpenSearchDB.ping(),
    ]);

    console.log(result);
  } catch (error) {
    console.error(`connection failed: ${(error as Error).message}`);
  } finally {
    await MysqlDB.close();
    await RedisDB.close();
    await OpenSearchDB.close();
  }
})();
