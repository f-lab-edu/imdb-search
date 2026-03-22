import type mysql from "mysql2/promise";

export class MysqlNormalize {
  constructor(private readonly pool: mysql.Pool) {}

  private async run(sql: string) {
    const conn = await this.pool.getConnection();
    try {
      await conn.query("SET FOREIGN_KEY_CHECKS=0");
      await conn.query(sql);
    } finally {
      await conn.query("SET FOREIGN_KEY_CHECKS=1");
      conn.release();
    }
  }

  async genres() {
    await this.run(`
      INSERT INTO GENRES (name)
      SELECT DISTINCT TRIM(j.genre)
      FROM staging_title_basics stb,
      JSON_TABLE(
        CONCAT('["', REPLACE(stb.genres, ',', '","'), '"]'),
        '$[*]' COLUMNS (genre VARCHAR(50) PATH '$')
      ) j
      WHERE stb.genres IS NOT NULL
      ON DUPLICATE KEY UPDATE name = name
    `);
  }

  async titles() {
    await this.run(`
      INSERT INTO TITLES (tconst, title_type, primary_title, original_title, is_adult, start_year, end_year, runtime_minutes)
      SELECT tconst, title_type, primary_title, original_title, is_adult,
             CAST(start_year AS UNSIGNED),
             CAST(end_year AS UNSIGNED),
             CAST(runtime_minutes AS UNSIGNED)
      FROM staging_title_basics
    `);
  }

  async titleGenres() {
    await this.run(`
      INSERT INTO TITLE_GENRES (tconst, genre_id)
      SELECT stb.tconst, g.id
      FROM staging_title_basics stb,
      JSON_TABLE(
        CONCAT('["', REPLACE(stb.genres, ',', '","'), '"]'),
        '$[*]' COLUMNS (genre VARCHAR(50) PATH '$')
      ) j
      JOIN GENRES g ON g.name = TRIM(j.genre)
      WHERE stb.genres IS NOT NULL
    `);
  }

  async persons() {
    await this.run(`
      INSERT INTO PERSONS (nconst, primary_name, birth_year, death_year, primary_profession, known_for_titles)
      SELECT nconst, primary_name,
             CAST(birth_year AS UNSIGNED),
             CAST(death_year AS UNSIGNED),
             primary_profession, known_for_titles
      FROM staging_name_basics
    `);
  }

  async titleAkas() {
    await this.run(`
      INSERT INTO TITLE_AKAS (tconst, ordering, title, region, language, types, attributes, is_original_title)
      SELECT tconst, CAST(ordering AS UNSIGNED), title, region, language, types, attributes, is_original_title
      FROM staging_title_akas
    `);
  }

  async ratings() {
    await this.run(`
      INSERT INTO RATINGS (tconst, average_rating, num_votes)
      SELECT tconst,
             CAST(average_rating AS DECIMAL(3,1)),
             CAST(num_votes AS UNSIGNED)
      FROM staging_title_ratings
    `);
  }

  async episodes() {
    await this.run(`
      INSERT INTO EPISODES (tconst, parent_tconst, season_number, episode_number)
      SELECT tconst, parent_tconst,
             CAST(season_number AS UNSIGNED),
             CAST(episode_number AS UNSIGNED)
      FROM staging_title_episode
    `);
  }

  async titleCrew() {
    await this.run(`
      INSERT INTO TITLE_CREW (tconst, directors, writers)
      SELECT tconst, directors, writers
      FROM staging_title_crew
    `);
  }

  async titlePrincipals() {
    await this.run(`
      INSERT INTO TITLE_PRINCIPALS (tconst, ordering, nconst, category, job, characters)
      SELECT tconst, CAST(ordering AS UNSIGNED), nconst, category, job, characters
      FROM staging_title_principals
    `);
  }
}
