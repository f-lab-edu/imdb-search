import mysql from "mysql2/promise";

export interface DatabaseConfig {
  host: string;
  user: string;
  password: string;
  database: string;
  port: number;
}

export class MysqlDatabase {
  private pool: mysql.Pool;

  constructor(config: DatabaseConfig) {
    this.pool = mysql.createPool({
      ...config,

      // 풀 관리 옵션
      connectionLimit: 15, // 최대 연결 수
      idleTimeout: 30000, // 30초간 미사용 시 연결 해제
      maxIdle: 10, // 유지할 최대 유휴 연결 수
      enableKeepAlive: true, // 장시간 연결 유지 시 유용
      keepAliveInitialDelay: 10000,
    });
  }

  getPool(): mysql.Pool {
    return this.pool;
  }

  async ping() {
    const conn = await this.pool.getConnection();

    try {
      await conn.ping();
      console.log("mysql connected");
    } catch (err) {
      console.error((err as Error).message);
      throw err;
    } finally {
      conn.release();
    }
  }

  async close() {
    await this.pool.end();
  }
}
