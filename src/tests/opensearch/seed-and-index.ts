import "dotenv/config";
import { config } from "../../config/index.js";
import { MysqlDatabase } from "../../db/mysql/connection.js";
import { MysqlCommand } from "../../db/mysql/commands.js";
import { OpenSearchDatabase } from "../../db/opensearch/index.js";
import { runIndexPipeline } from "../../worker/indexer.js";

const mysqlDb = new MysqlDatabase(config.db.mysql);
const mysqlCmd = await MysqlCommand.create(mysqlDb.getPool());
const osDb = new OpenSearchDatabase(config.db.opensearch);
const pool = mysqlDb.getPool();

async function seed() {
  const conn = await pool.getConnection();
  try {
    await conn.query("SET FOREIGN_KEY_CHECKS = 0");
    await conn.query("TRUNCATE TABLE TITLE_PRINCIPALS");
    await conn.query("TRUNCATE TABLE TITLE_CREW");
    await conn.query("TRUNCATE TABLE TITLE_AKAS");
    await conn.query("TRUNCATE TABLE RATINGS");
    await conn.query("TRUNCATE TABLE TITLE_GENRES");
    await conn.query("TRUNCATE TABLE TITLES");
    await conn.query("TRUNCATE TABLE PERSONS");
    await conn.query("TRUNCATE TABLE GENRES");
    await conn.query("SET FOREIGN_KEY_CHECKS = 1");
  } finally {
    conn.release();
  }

  await mysqlCmd.insertTitleBasics([
    {
      tconst: "tt0111161",
      title_type: "movie",
      primary_title: "The Shawshank Redemption",
      original_title: "The Shawshank Redemption",
      is_adult: "0",
      start_year: "1994",
      end_year: null,
      runtime_minutes: "142",
      genres: "Drama",
    },
    {
      tconst: "tt0468569",
      title_type: "movie",
      primary_title: "The Dark Knight",
      original_title: "The Dark Knight",
      is_adult: "0",
      start_year: "2008",
      end_year: null,
      runtime_minutes: "152",
      genres: "Action,Crime,Drama",
    },
    {
      tconst: "tt6751668",
      title_type: "movie",
      primary_title: "Parasite",
      original_title: "기생충",
      is_adult: "0",
      start_year: "2019",
      end_year: null,
      runtime_minutes: "132",
      genres: "Comedy,Drama,Thriller",
    },
  ]);

  await mysqlCmd.insertNameBasics([
    {
      nconst: "nm0000093",
      primary_name: "Morgan Freeman",
      birth_year: "1937",
      death_year: null,
      primary_profession: "actor",
      known_for_titles: "tt0111161",
    },
    {
      nconst: "nm0634240",
      primary_name: "Song Kang-ho",
      birth_year: "1967",
      death_year: null,
      primary_profession: "actor",
      known_for_titles: "tt6751668",
    },
  ]);

  await mysqlCmd.insertTitleRatings([
    { tconst: "tt0111161", average_rating: "9.3", num_votes: "2600000" },
    { tconst: "tt0468569", average_rating: "9.0", num_votes: "2700000" },
    { tconst: "tt6751668", average_rating: "8.5", num_votes: "800000" },
  ]);

  await mysqlCmd.insertTitleAkas([
    {
      tconst: "tt6751668",
      ordering: "1",
      title: "기생충",
      region: "KR",
      language: "ko",
      types: null,
      attributes: null,
      is_original_title: "1",
    },
  ]);

  await mysqlCmd.insertTitlePrincipals([
    {
      tconst: "tt0111161",
      ordering: "1",
      nconst: "nm0000093",
      category: "actor",
      job: null,
      characters: '["Red"]',
    },
    {
      tconst: "tt6751668",
      ordering: "1",
      nconst: "nm0634240",
      category: "actor",
      job: null,
      characters: '["Ki-taek"]',
    },
  ]);

  console.log("[seed] done");
}

async function verify() {
  const osClient = osDb.getClient();

  await osClient.indices.refresh({ index: "imdb_titles" });

  const result = await osClient.search({
    index: "imdb_titles",
    body: {
      query: { match_all: {} },
    },
  });

  const hits = result.body.hits.hits as any[];
  console.log(`\n[verify] ${hits.length} documents indexed:`);
  hits.forEach((h) => {
    const s = h._source;
    console.log(
      `  - [${s.tconst}] ${s.primary_title} (${s.start_year}) rating=${s.average_rating} korean=${s.korean_title ?? "none"} cast=${s.cast.map((c: any) => c.name).join(", ")}`
    );
  });

  // korean search test
  const koreanResult = await osClient.search({
    index: "imdb_titles",
    body: { query: { match: { korean_title: "기생충" } } },
  });
  console.log(
    `\n[verify] korean search '기생충': ${(koreanResult.body.hits.hits as any[]).length} hit(s)`
  );
}

try {
  await seed();
  await runIndexPipeline({
    mysqlPool: pool,
    osClient: osDb.getClient(),
    recreateIndex: true,
  });
  await verify();
} finally {
  await mysqlDb.close();
  await osDb.close();
}
