import type mysql from "mysql2/promise";

interface IntegrityCheckResult {
  check: string;
  orphaned: number;
  ok: boolean;
}

const dupCheck = (table: string, ...cols: string[]) =>
  `SELECT COUNT(*) AS cnt FROM (
     SELECT ${cols.join(", ")} FROM ${table} GROUP BY ${cols.join(", ")} HAVING COUNT(*) > 1
   ) t`;

const fkCheck = (
  child: string,
  childAlias: string,
  childCol: string,
  parent: string,
  parentAlias: string,
  parentCol: string,
) =>
  `SELECT COUNT(*) AS cnt FROM ${child} ${childAlias}
   LEFT JOIN ${parent} ${parentAlias} ON ${childAlias}.${childCol} = ${parentAlias}.${parentCol}
   WHERE ${parentAlias}.${parentCol} IS NULL`;

const CHECKS: Array<{ name: string; sql: string }> = [
  // 중복 체크
  { name: "GENRES.name 중복",                       sql: dupCheck("GENRES", "name") },
  { name: "TITLE_GENRES.(tconst, genre_id) 중복",   sql: dupCheck("TITLE_GENRES", "tconst", "genre_id") },
  // FK 고아 행 체크
  { name: "TITLE_GENRES.tconst → TITLES",           sql: fkCheck("TITLE_GENRES",    "tg", "tconst",        "TITLES",  "t", "tconst") },
  { name: "TITLE_GENRES.genre_id → GENRES",         sql: fkCheck("TITLE_GENRES",    "tg", "genre_id",      "GENRES",  "g", "id") },
  { name: "TITLE_AKAS.tconst → TITLES",             sql: fkCheck("TITLE_AKAS",      "ta", "tconst",        "TITLES",  "t", "tconst") },
  { name: "RATINGS.tconst → TITLES",                sql: fkCheck("RATINGS",         "r",  "tconst",        "TITLES",  "t", "tconst") },
  { name: "EPISODES.tconst → TITLES",               sql: fkCheck("EPISODES",        "e",  "tconst",        "TITLES",  "t", "tconst") },
  { name: "EPISODES.parent_tconst → TITLES",        sql: fkCheck("EPISODES",        "e",  "parent_tconst", "TITLES",  "t", "tconst") },
  { name: "TITLE_CREW.tconst → TITLES",             sql: fkCheck("TITLE_CREW",      "tc", "tconst",        "TITLES",  "t", "tconst") },
  { name: "TITLE_PRINCIPALS.tconst → TITLES",       sql: fkCheck("TITLE_PRINCIPALS","tp", "tconst",        "TITLES",  "t", "tconst") },
  { name: "TITLE_PRINCIPALS.nconst → PERSONS",      sql: fkCheck("TITLE_PRINCIPALS","tp", "nconst",        "PERSONS", "p", "nconst") },
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
