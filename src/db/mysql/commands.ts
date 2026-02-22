import mysql from "mysql2/promise";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  type TableName,
  type DatasetType,
  type TitleBasics,
} from "../../utils/index.js";

export class MysqlCommand {
  private readonly pool: mysql.Pool;
  private genresMap: Map<string, number> | null;

  constructor(pool: mysql.Pool, genresMap?: Map<string, number>) {
    this.genresMap = genresMap || null;
    this.pool = pool;
  }

  static async create(pool: mysql.Pool) {
    const conn = await pool.getConnection();

    try {
      await MysqlCommand.migrateSchema(pool);

      let genresMap: Map<string, number> = new Map();
      const [rows] = await conn.query<mysql.RowDataPacket[]>(
        "SELECT id,name FROM GENRES",
      );

      rows.forEach((row: mysql.RowDataPacket) => {
        genresMap.set(row.name, row.id);
      });

      return new MysqlCommand(pool, genresMap);
    } finally {
      conn.release();
    }
  }

  static async migrateSchema(pool: mysql.Pool) {
    const conn = await pool.getConnection();

    try {
      const sql = await fs.readFile(
        path.join(path.dirname(fileURLToPath(import.meta.url)), "schema.sql"),
        "utf-8",
      );

      const queries = sql
        .split(";")
        .map((q) => q.trim())
        .filter((q) => q.length > 0);

      await Promise.all(queries.map((q) => conn.query(q)));

      console.log("mysql table migration ok");
    } catch (err) {
      console.error((err as Error).message);
      throw err;
    } finally {
      conn.release();
    }
  }

  async truncateTable(tableName: TableName) {
    const conn = await this.pool.getConnection();

    try {
      await conn.query(`TRUNCATE TABLE ${tableName}`);
    } catch (err) {
      console.error(`truncate table error: ${(err as Error).message}`);
      throw err;
    } finally {
      conn.release();
    }
  }

  async bulkInsert<T extends Partial<DatasetType>>(
    tableName: TableName,
    data: T[],
  ) {
    if (data.length == 0) return;

    const conn = await this.pool.getConnection();

    try {
      const keys = Object.keys(data[0]!) as Array<keyof T>; // 위에서 길이 체크 했음
      const values = data.map((row) => keys.map((k) => row[k]));

      await conn.query(
        `INSERT INTO ${tableName} (${keys.join(",")}) VALUES ?`,
        [values],
      );
    } catch (err) {
      console.error("bulk insert error:", (err as Error).message);
      throw err;
    } finally {
      conn.release();
    }
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

  private async insertGenres(data: TitleBasics[]) {
    const conn = await this.pool.getConnection();

    try {
      const newGenres = [
        ...new Set(data.flatMap((row) => row.genres?.split(",") ?? [])),
      ].filter((name) => !this.genresMap?.has(name));

      if (newGenres.length > 0) {
        const [result] = await conn.query<mysql.ResultSetHeader>(
          "INSERT INTO GENRES(name) VALUES ?",
          [newGenres.map((n) => [n])],
        );

        newGenres.forEach((name, i) =>
          this.genresMap?.set(name, result.insertId + i),
        );
      }
    } catch (err) {
      console.error(`insert genres error: ${(err as Error).message}`);
      throw err;
    } finally {
      conn.release();
    }
  }

  // title.basics.tsv
  async insertTitleBasics(data: TitleBasics[]) {
    if (data.length == 0) return;

    try {
      // insert genres
      await this.insertGenres(data);

      // insert titles
      const titles: Omit<TitleBasics, "genres">[] = data.map(
        ({ genres, ...rest }) => rest,
      );

      await this.bulkInsert("TITLES", titles);

      // insert titles_genres
      const titleGenres: { tconst: string; genre_id: number }[] = data.flatMap(
        (row) =>
          (row.genres?.split(",") ?? [])
            .map((genreName) => ({
              tconst: row.tconst,
              genre_id: this.genresMap?.get(genreName) || -1,
            }))
            .filter((v) => v.genre_id > -1),
      );

      await this.bulkInsert("TITLE_GENRES", titleGenres);
    } catch (err) {
      console.error(`insert title basics error: ${(err as Error).message}`);
      throw err;
    }
  }
}
