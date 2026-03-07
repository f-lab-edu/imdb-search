import mysql from "mysql2/promise";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  type TableName,
  type DatasetType,
  type TitleBasics,
  type NameBasics,
  type TitleAkas,
  type TitleRatings,
  type TitleEpisode,
  type TitleCrew,
  type TitlePrincipals,
} from "../../utils/index.js";
import type { Task } from "../../worker/types.js";

export class MysqlCommand {
  private readonly pool: mysql.Pool;
  private genresMap: Map<string, number>;
  private genreLock: Promise<void> = Promise.resolve();

  constructor(pool: mysql.Pool, genresMap?: Map<string, number>) {
    this.genresMap = genresMap || new Map();
    this.pool = pool;
  }

  static async create(pool: mysql.Pool) {
    const conn = await pool.getConnection();

    try {
      await MysqlCommand.migrateSchema(conn);

      let genresMap: Map<string, number> = new Map();
      const [rows] = await conn.query<mysql.RowDataPacket[]>("SELECT id,name FROM GENRES");

      rows.forEach((row: mysql.RowDataPacket) => {
        genresMap.set(row.name, row.id);
      });

      return new MysqlCommand(pool, genresMap);
    } finally {
      conn.release();
    }
  }

  static async migrateSchema(conn: mysql.PoolConnection) {
    try {
      const sql = await fs.readFile(
        path.join(path.dirname(fileURLToPath(import.meta.url)), "schema.sql"),
        "utf-8"
      );

      const queries = sql
        .split(";")
        .map((q) => q.trim())
        .filter((q) => q.length > 0);

      for (const query of queries) {
        await conn.query(query);
      }

      console.log("mysql table migration ok");
    } catch (err) {
      console.error((err as Error).message);
      throw err;
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

  async bulkInsert<T extends Partial<DatasetType>>(tableName: TableName, data: T[]) {
    if (data.length == 0) return;

    let conn = null;

    try {
      conn = await this.pool.getConnection();

      await conn.beginTransaction();

      const keys = Object.keys(data[0]!) as Array<keyof T>; // 위에서 길이 체크 했음
      const values = data.map((row) => keys.map((k) => row[k]));

      await conn.query(`INSERT IGNORE INTO ${tableName} (${keys.join(",")}) VALUES ?`, [values]);

      await conn.commit();
    } catch (err) {
      await conn?.rollback();

      console.error("bulk insert error:", (err as Error).message);
      throw err;
    } finally {
      conn?.release();
    }
  }

  private async insertGenres(data: TitleBasics[]): Promise<void> {
    this.genreLock = this.genreLock
      .catch(() => {
        // TODO: ignore previous errors for now, need implment error hanlding logics here
      })
      .then(async () => {
        const conn = await this.pool.getConnection();

        try {
          const newGenres = [
            ...new Set(data.flatMap((row) => row.genres?.split(",") ?? [])),
          ].filter((name) => name && !this.genresMap.has(name));

          if (newGenres.length > 0) {
            await conn.query("INSERT IGNORE INTO GENRES(name) VALUES ?", [
              newGenres.map((n) => [n]),
            ]);

            const [rows] = await conn.query<mysql.RowDataPacket[]>(
              "SELECT id, name FROM GENRES WHERE name IN (?)",
              [newGenres]
            );

            rows.forEach((row: mysql.RowDataPacket) => {
              this.genresMap.set(row.name, row.id);
            });
          }
        } catch (err) {
          console.error(`insert genres error: ${(err as Error).message}`);
          throw err;
        } finally {
          conn.release();
        }
      });

    return this.genreLock;
  }

  // title.basics.tsv - genres, titles, title_genres
  async insertTitleBasics(data: TitleBasics[]) {
    if (data.length == 0) return;

    try {
      // insert genres
      await this.insertGenres(data);

      // insert titles
      const titles: Omit<TitleBasics, "genres">[] = data.map(({ genres, ...rest }) => rest);

      await this.bulkInsert("TITLES", titles);

      // insert titles_genres
      const titleGenres: { tconst: string; genre_id: number }[] = data.flatMap((row) =>
        (row.genres?.split(",") ?? [])
          .map((genreName) => ({
            tconst: row.tconst,
            genre_id: this.genresMap.get(genreName) ?? -1,
          }))
          .filter((v) => v.genre_id > -1)
      );

      await this.bulkInsert("TITLE_GENRES", titleGenres);
    } catch (err) {
      console.error(`insert title basics error: ${(err as Error).message}`);
      throw err;
    }
  }

  // name.basics.tsv - persons
  async insertNameBasics(data: NameBasics[]) {
    if (data.length == 0) return;

    try {
      await this.bulkInsert("PERSONS", data);
    } catch (err) {
      console.error(`insert name basics error: ${(err as Error).message}`);
      throw err;
    }
  }

  // title.akas.tsv
  async insertTitleAkas(data: TitleAkas[]) {
    if (data.length == 0) return;

    try {
      await this.bulkInsert("TITLE_AKAS", data);
    } catch (err) {
      console.error(`insert title akas error: ${(err as Error).message}`);
      throw err;
    }
  }

  // title.ratings.tsv
  async insertTitleRatings(data: TitleRatings[]) {
    if (data.length == 0) return;

    try {
      await this.bulkInsert("RATINGS", data);
    } catch (err) {
      console.error(`insert title ratings error: ${(err as Error).message}`);
      throw err;
    }
  }

  // title.episodes.tsv
  async insertTitleEpisodes(data: TitleEpisode[]) {
    if (data.length == 0) return;

    try {
      await this.bulkInsert("EPISODES", data);
    } catch (err) {
      console.error(`insert title episodes error: ${(err as Error).message}`);
      throw err;
    }
  }

  // title.crew.tsv
  async insertTitleCrew(data: TitleCrew[]) {
    if (data.length == 0) return;

    try {
      await this.bulkInsert("TITLE_CREW", data);
    } catch (err) {
      console.error(`insert title crew error: ${(err as Error).message}`);
      throw err;
    }
  }

  // title.principals.tsv
  async insertTitlePrincipals(data: TitlePrincipals[]) {
    if (data.length == 0) return;

    try {
      await this.bulkInsert("TITLE_PRINCIPALS", data);
    } catch (err) {
      console.error(`insert title principals error: ${(err as Error).message}`);
      throw err;
    }
  }

  async insertTask(task: Task, phase: number) {
    const conn = await this.pool.getConnection();

    try {
      await conn.execute(
        `INSERT IGNORE INTO TASKS (task_id, batch_id, name, phase, payload, status, retry_count, created_at)
         VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)`,
        [
          task.taskId,
          task.batchId,
          task.name,
          phase,
          JSON.stringify(task.payload),
          task.retryCount,
          task.createdAt,
        ]
      );
    } catch (err) {
      console.error(`insertTask error: ${(err as Error).message}`);
      throw err;
    } finally {
      conn.release();
    }
  }

  async markTasksQueued(taskIds: string[]): Promise<void> {
    if (taskIds.length === 0) return;

    const conn = await this.pool.getConnection();

    try {
      await conn.query(`UPDATE TASKS SET status = 'queued' WHERE task_id IN (?)`, [taskIds]);
    } finally {
      conn.release();
    }
  }

  async markTaskFailed(taskId: string): Promise<void> {
    const conn = await this.pool.getConnection();

    try {
      await conn.query(`UPDATE TASKS SET status = 'failed' WHERE task_id = ?`, [taskId]);
    } finally {
      conn.release();
    }
  }

  // TODO: move to queries?
  async hasPendingTasks(phase: number): Promise<boolean> {
    const conn = await this.pool.getConnection();

    try {
      const [rows] = await conn.query<mysql.RowDataPacket[]>(
        `SELECT 1 FROM TASKS WHERE status = 'pending' AND phase = ? LIMIT 1`,
        [phase]
      );
      return rows.length > 0;
    } finally {
      conn.release();
    }
  }

  async fetchPendingTasks(phase: number, limit: number): Promise<Task[]> {
    const conn = await this.pool.getConnection();

    try {
      const [rows] = await conn.query<mysql.RowDataPacket[]>(
        `SELECT task_id, batch_id, name, payload, retry_count, created_at
         FROM TASKS WHERE status = 'pending' AND phase = ?
         ORDER BY id ASC LIMIT ?`,
        [phase, limit]
      );

      return rows.map((r) => ({
        taskId: r.task_id,
        batchId: r.batch_id,
        name: r.name,
        payload: typeof r.payload === "string" ? JSON.parse(r.payload) : r.payload,
        retryCount: r.retry_count,
        createdAt: r.created_at,
      }));
    } finally {
      conn.release();
    }
  }
}
