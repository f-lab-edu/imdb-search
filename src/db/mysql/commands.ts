import mysql from "mysql2/promise";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { type TableName, type DatasetType } from "../../utils/index.js";

export class MysqlCommand {
  private readonly pool: mysql.Pool;

  constructor(pool: mysql.Pool) {
    this.pool = pool;
  }

  async migrateSchema() {
    try {
      const sql = await fs.readFile(
        path.join(path.dirname(fileURLToPath(import.meta.url)), "schema.sql"),
        "utf-8",
      );

      const queries = sql
        .split(";")
        .map((q) => q.trim())
        .filter((q) => q.length > 0);

      await Promise.all(queries.map((q) => this.pool.query(q)));

      console.log("mysql table migration ok");
    } catch (err) {
      console.error((err as Error).message);
      throw err;
    }
  }

  async truncateTable(tableName: TableName) {
    await this.pool.query(`TRUNCATE TABLE ${tableName}`);
  }

  async bulkInsert<T extends DatasetType>(tableName: TableName, data: T[]) {
    if (data.length == 0) return;

    const keys = Object.keys(data[0]!) as Array<keyof T>; // 위에서 길이 체크 했음
    const values = data.map((row) => keys.map((k) => row[k]));

    await this.pool.query(
      `INSERT INTO ${tableName} (${keys.join(",")}) VALUES ?`,
      [values],
    );
  }

  // for test only
  async reset() {
    try {
      await this.pool.query(
        "DROP TABLE IF EXISTS TITLE_GENRES, TITLE_AKAS, RATINGS, EPISODES, TITLE_PRINCIPALS, TITLE_CREW;",
      );

      await this.pool.query("DROP TABLE IF EXISTS TITLES, PERSONS, GENRES;");

      console.log("reset db ok");
    } catch (err) {
      console.error(`failed to reset db: ${(err as Error).message}`);
    }
  }

  // title.basics.tsv
  async insertTitleBasics() {}
}
