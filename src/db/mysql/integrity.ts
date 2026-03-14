import type mysql from "mysql2/promise";

interface IntegrityCheckResult {
  check: string;
  orphaned: number;
  ok: boolean;
}

const CHECKS: Array<{ name: string; sql: string }> = [
  // 중복 체크
  {
    name: "GENRES.name 중복",
    sql: `SELECT COUNT(*) AS cnt FROM (
            SELECT name FROM GENRES GROUP BY name HAVING COUNT(*) > 1
          ) t`,
  },
  {
    name: "TITLE_GENRES.(tconst, genre_id) 중복",
    sql: `SELECT COUNT(*) AS cnt FROM (
            SELECT tconst, genre_id FROM TITLE_GENRES GROUP BY tconst, genre_id HAVING COUNT(*) > 1
          ) t`,
  },
  // FK 고아 행 체크
  {
    name: "TITLE_GENRES.tconst → TITLES",
    sql: `SELECT COUNT(*) AS cnt FROM TITLE_GENRES tg
          LEFT JOIN TITLES t ON tg.tconst = t.tconst
          WHERE t.tconst IS NULL`,
  },
  {
    name: "TITLE_GENRES.genre_id → GENRES",
    sql: `SELECT COUNT(*) AS cnt FROM TITLE_GENRES tg
          LEFT JOIN GENRES g ON tg.genre_id = g.id
          WHERE g.id IS NULL`,
  },
  {
    name: "TITLE_AKAS.tconst → TITLES",
    sql: `SELECT COUNT(*) AS cnt FROM TITLE_AKAS ta
          LEFT JOIN TITLES t ON ta.tconst = t.tconst
          WHERE t.tconst IS NULL`,
  },
  {
    name: "RATINGS.tconst → TITLES",
    sql: `SELECT COUNT(*) AS cnt FROM RATINGS r
          LEFT JOIN TITLES t ON r.tconst = t.tconst
          WHERE t.tconst IS NULL`,
  },
  {
    name: "EPISODES.tconst → TITLES",
    sql: `SELECT COUNT(*) AS cnt FROM EPISODES e
          LEFT JOIN TITLES t ON e.tconst = t.tconst
          WHERE t.tconst IS NULL`,
  },
  {
    name: "EPISODES.parent_tconst → TITLES",
    sql: `SELECT COUNT(*) AS cnt FROM EPISODES e
          LEFT JOIN TITLES t ON e.parent_tconst = t.tconst
          WHERE t.tconst IS NULL`,
  },
  {
    name: "TITLE_CREW.tconst → TITLES",
    sql: `SELECT COUNT(*) AS cnt FROM TITLE_CREW tc
          LEFT JOIN TITLES t ON tc.tconst = t.tconst
          WHERE t.tconst IS NULL`,
  },
  {
    name: "TITLE_PRINCIPALS.tconst → TITLES",
    sql: `SELECT COUNT(*) AS cnt FROM TITLE_PRINCIPALS tp
          LEFT JOIN TITLES t ON tp.tconst = t.tconst
          WHERE t.tconst IS NULL`,
  },
  {
    name: "TITLE_PRINCIPALS.nconst → PERSONS",
    sql: `SELECT COUNT(*) AS cnt FROM TITLE_PRINCIPALS tp
          LEFT JOIN PERSONS p ON tp.nconst = p.nconst
          WHERE p.nconst IS NULL`,
  },
];

export class MysqlIntegrity {
  constructor(private readonly pool: mysql.Pool) {}

  async checkAll(): Promise<IntegrityCheckResult[]> {
    const conn = await this.pool.getConnection();
    const results: IntegrityCheckResult[] = [];

    try {
      for (const { name, sql } of CHECKS) {
        const [rows] = await conn.query<mysql.RowDataPacket[]>(sql);
        const orphaned = Number(rows[0]?.cnt ?? 0);
        results.push({ check: name, orphaned, ok: orphaned === 0 });
      }
    } finally {
      conn.release();
    }

    return results;
  }

  static printResults(results: IntegrityCheckResult[]): void {
    console.log("\n=== Integrity Check Results ===");
    for (const r of results) {
      const status = r.ok ? "OK" : `FAIL (${r.orphaned.toLocaleString()} orphaned)`;
      console.log(`  ${r.ok ? "✓" : "✗"} ${r.check}: ${status}`);
    }

    const failed = results.filter((r) => !r.ok);
    console.log(`\n${failed.length === 0 ? "All checks passed." : `${failed.length} check(s) failed.`}\n`);
  }
}
