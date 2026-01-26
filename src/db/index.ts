import dotenv from "dotenv";
import { MysqlDatabase } from "./mysql.js";
import { RedisDatabase } from "./redis.js";
import { OpenSearchDatabase } from "./openSearch.js";

dotenv.config({ path: ".env.dev" });

// node.js 기본 모듈 캐싱 기능 활용해서 최초에만 생성하고 이후에는 모듈 재사용하는 식으로 활용
// TODO: 이거 나중에 설정으로 한번에 묶어서 처리

export const MysqlDB = new MysqlDatabase({
  host: process.env.DB_HOST || "localhost",
  user: process.env.DB_USER || "root",
  password: process.env.DB_PASSWORD || "",
  database: process.env.DB_NAME || "test",
  port: Number(process.env.DB_PORT) || 3306,
});

export const RedisDB = new RedisDatabase({
  socket: {
    host: process.env.REDIS_HOST || "localhost",
  },
  password: process.env.REDIS_PASSWORD || "",
});

export const OpenSearchDB = new OpenSearchDatabase({
  node: process.env.OPENSEARCH_NODE || "http://localhost:9200",
  auth: {
    username: process.env.OPENSEARCH_USER || "admin",
    password: process.env.OPENSEARCH_PASSWORD || "",
  },

  // TODO: 개발 환경에서만
  ssl: {
    rejectUnauthorized: false,
  },
});
