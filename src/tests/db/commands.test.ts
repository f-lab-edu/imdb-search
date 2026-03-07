import { describe, it, expect, beforeAll, afterAll } from "@jest/globals";
import dotenv from "dotenv";
import mysql from "mysql2/promise";
import { MysqlDatabase } from "../../db/mysql/connection.js";
import { MysqlCommand } from "../../db/mysql/commands.js";
import type { TitleBasics, TitleRatings } from "../../utils/types.js";
import { resetMysql } from "./helpers.js";

describe("MysqlCommand test", () => {
  let db: MysqlDatabase;
  let cmd: MysqlCommand;
  let pool: mysql.Pool;

  beforeAll(async () => {
    dotenv.config({ path: ".env.test" });
    const { config } = await import("../../config/index.js");

    db = new MysqlDatabase(config.db.mysql);

    pool = db.getPool();
    cmd = await MysqlCommand.create(pool);
  });

  afterAll(async () => {
    await resetMysql(pool);
    await db.close();
  });

  it("bulkInsert: insert genres", async () => {
    const mockGenres = [{ name: "Action" }, { name: "Comedy" }, { name: "Drama" }];

    await cmd.bulkInsert("GENRES", mockGenres as any);

    const [rows] = await pool.query<mysql.RowDataPacket[]>(
      "SELECT name FROM GENRES ORDER BY name ASC"
    );

    expect(rows.length).toBe(3);
    expect((rows as any)[0].name).toBe("Action");
    expect((rows as any)[1].name).toBe("Comedy");
    expect((rows as any)[2].name).toBe("Drama");
  });

  it("bulkInsert: insert titles", async () => {
    const mockTitles: Omit<TitleBasics, "genres">[] = [
      {
        tconst: "tt0000001",
        title_type: "short",
        primary_title: "Carmencita",
        original_title: "Carmencita",
        is_adult: "0",
        start_year: "1894",
        end_year: null,
        runtime_minutes: "1",
      },
      {
        tconst: "tt0000002",
        title_type: "short",
        primary_title: "Le clown et ses chiens",
        original_title: "Le clown et ses chiens",
        is_adult: "0",
        start_year: "1892",
        end_year: null,
        runtime_minutes: "5",
      },
    ];

    await cmd.bulkInsert("TITLES", mockTitles);

    const [rows] = await pool.query<mysql.RowDataPacket[]>(
      "SELECT tconst, primary_title FROM TITLES ORDER BY tconst ASC"
    );

    expect(rows.length).toBe(2);
    expect((rows as any)[0].tconst).toBe("tt0000001");
    expect((rows as any)[1].tconst).toBe("tt0000002");
  });

  it("bulkInsert: insert ratings with FK to titles", async () => {
    const mockRatings: TitleRatings[] = [
      { tconst: "tt0000001", average_rating: "5.7", num_votes: "1500" },
      { tconst: "tt0000002", average_rating: "6.1", num_votes: "200" },
    ];

    await cmd.bulkInsert("RATINGS", mockRatings);

    const [rows] = await pool.query<mysql.RowDataPacket[]>(
      "SELECT tconst, average_rating, num_votes FROM RATINGS ORDER BY tconst ASC"
    );

    expect(rows.length).toBe(2);
    expect(Number((rows as any)[0].average_rating)).toBe(5.7);
    expect((rows as any)[1].num_votes).toBe(200);
  });

  it("insertTitleBasics: titles + genres + title_genres", async () => {
    const conn = await pool.getConnection();
    try {
      await conn.query("SET FOREIGN_KEY_CHECKS = 0");
      await conn.query("TRUNCATE TABLE TITLE_GENRES");
      await conn.query("TRUNCATE TABLE RATINGS");
      await conn.query("TRUNCATE TABLE TITLES");
      await conn.query("SET FOREIGN_KEY_CHECKS = 1");
    } finally {
      conn.release();
    }

    const mockData: TitleBasics[] = [
      {
        tconst: "tt0000010",
        title_type: "movie",
        primary_title: "Test Movie",
        original_title: "Test Movie",
        is_adult: "0",
        start_year: "2020",
        end_year: null,
        runtime_minutes: "120",
        genres: "Action,Comedy",
      },
      {
        tconst: "tt0000011",
        title_type: "movie",
        primary_title: "Another Movie",
        original_title: "Another Movie",
        is_adult: "0",
        start_year: "2021",
        end_year: null,
        runtime_minutes: "90",
        genres: "Drama,Comedy",
      },
    ];

    await cmd.insertTitleBasics(mockData);

    const [titles] = await pool.query<mysql.RowDataPacket[]>(
      "SELECT tconst FROM TITLES ORDER BY tconst ASC"
    );
    expect(titles.length).toBe(2);

    const [titleGenres] = await pool.query<mysql.RowDataPacket[]>(
      "SELECT tconst, genre_id FROM TITLE_GENRES ORDER BY tconst ASC"
    );
    // tt0000010 -> Action, Comedy / tt0000011 -> Drama, Comedy = 4건
    expect(titleGenres.length).toBe(4);
  });

  it("bulkInsert: empty array does nothing", async () => {
    await cmd.bulkInsert("GENRES", [] as any);

    const [rows] = await pool.query<mysql.RowDataPacket[]>("SELECT COUNT(*) as cnt FROM GENRES");
    expect((rows as any)[0].cnt).toBeGreaterThan(0);
  });
});
