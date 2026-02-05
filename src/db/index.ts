import { config } from "../config/index.js";
import { MysqlDatabase } from "./mysql/index.js";
import { RedisDatabase } from "./redis.js";
import { OpenSearchDatabase } from "./openSearch.js";

// node.js 기본 모듈 캐싱 기능 활용해서 최초에만 생성하고 이후에는 모듈 재사용하는 식으로 활용

export const MysqlDB = new MysqlDatabase(config.db.mysql);
export const RedisDB = new RedisDatabase(config.db.redis);
export const OpenSearchDB = new OpenSearchDatabase(config.db.opensearch);
