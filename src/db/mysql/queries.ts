import type mysql from "mysql2/promise";
import type { TitleDocument, CastMember, PersonRef } from "../opensearch/commands.js";

interface RawTitle {
  tconst: string;
  primary_title: string;
  original_title: string;
  title_type: string;
  start_year: number | null;
  end_year: number | null;
  runtime_minutes: number | null;
  is_adult: number;
  average_rating: number | null;
  num_votes: number | null;
}

interface RawKoreanTitle {
  tconst: string;
  title: string;
}

interface RawGenre {
  tconst: string;
  name: string;
}

interface RawCast {
  tconst: string;
  nconst: string;
  name: string;
  category: string;
  characters: string | null;
}

interface RawCrew {
  tconst: string;
  directors: string | null;
  writers: string | null;
}

interface RawPerson {
  nconst: string;
  primary_name: string;
}

export class MysqlQuery {
  constructor(private readonly pool: mysql.Pool) {}

  async getTitleDetail(tconst: string) {
    const conn = await this.pool.getConnection();

    try {
      const [titles] = await conn.query<mysql.RowDataPacket[]>(
        `SELECT tconst, title_type, primary_title, original_title, is_adult, start_year, end_year, runtime_minutes
         FROM TITLES WHERE tconst = ?`,
        [tconst]
      );
      const [genres] = await conn.query<mysql.RowDataPacket[]>(
        `SELECT g.name FROM TITLE_GENRES tg JOIN GENRES g ON tg.genre_id = g.id WHERE tg.tconst = ?`,
        [tconst]
      );
      const [ratings] = await conn.query<mysql.RowDataPacket[]>(
        `SELECT average_rating, num_votes FROM RATINGS WHERE tconst = ?`,
        [tconst]
      );
      const [principals] = await conn.query<mysql.RowDataPacket[]>(
        `SELECT tp.nconst, p.primary_name, tp.category, tp.job, tp.characters
         FROM TITLE_PRINCIPALS tp
         LEFT JOIN PERSONS p ON tp.nconst = p.nconst
         WHERE tp.tconst = ?
         GROUP BY tp.ordering, tp.nconst, tp.category
         ORDER BY tp.ordering ASC`,
        [tconst]
      );
      const [crew] = await conn.query<mysql.RowDataPacket[]>(
        `SELECT directors, writers FROM TITLE_CREW WHERE tconst = ?`,
        [tconst]
      );

      const title = titles[0];
      if (!title) return null;

      const crewRow = crew[0];
      const directorNconsts: string[] = crewRow?.directors
        ? crewRow.directors.split(",").filter(Boolean)
        : [];
      const writerNconsts: string[] = crewRow?.writers
        ? crewRow.writers.split(",").filter(Boolean)
        : [];

      const allNconsts = [...new Set([...directorNconsts, ...writerNconsts])];
      const personMap = new Map<string, string>();

      if (allNconsts.length > 0) {
        const [persons] = await conn.query<mysql.RowDataPacket[]>(
          `SELECT nconst, primary_name FROM PERSONS WHERE nconst IN (?)`,
          [allNconsts]
        );
        (persons as mysql.RowDataPacket[]).forEach((p) => personMap.set(p.nconst, p.primary_name));
      }

      const rating = ratings[0] ?? null;

      return {
        tconst: title.tconst as string,
        title_type: title.title_type as string,
        primary_title: title.primary_title as string,
        original_title: title.original_title as string,
        is_adult: Boolean(title.is_adult),
        start_year: title.start_year as number | null,
        end_year: title.end_year as number | null,
        runtime_minutes: title.runtime_minutes as number | null,
        genres: genres.map((g) => g.name as string),
        rating: rating
          ? { average: Number(rating.average_rating), votes: rating.num_votes as number }
          : null,
        crew: {
          directors: directorNconsts.map((n) => ({ nconst: n, name: personMap.get(n) ?? null })),
          writers: writerNconsts.map((n) => ({ nconst: n, name: personMap.get(n) ?? null })),
        },
        principals: principals.map((p) => ({
          nconst: p.nconst as string,
          name: p.primary_name as string,
          category: p.category as string,
          job: p.job as string | null,
          characters: p.characters as string | null,
        })),
      };
    } finally {
      conn.release();
    }
  }

  async getPersonDetail(nconst: string) {
    const conn = await this.pool.getConnection();

    try {
      const [persons] = await conn.query<mysql.RowDataPacket[]>(
        `SELECT nconst, primary_name, birth_year, death_year, primary_profession
         FROM PERSONS WHERE nconst = ?`,
        [nconst]
      );
      const [credits] = await conn.query<mysql.RowDataPacket[]>(
        `SELECT tp.tconst, t.primary_title, t.title_type, t.start_year, tp.category, tp.characters
         FROM TITLE_PRINCIPALS tp
         JOIN TITLES t ON tp.tconst = t.tconst
         WHERE tp.nconst = ?
         ORDER BY t.start_year DESC
         LIMIT 50`,
        [nconst]
      );

      const person = persons[0];
      if (!person) return null;

      return {
        nconst: person.nconst as string,
        primary_name: person.primary_name as string,
        birth_year: person.birth_year as number | null,
        death_year: person.death_year as number | null,
        primary_profession: person.primary_profession
          ? (person.primary_profession as string).split(",")
          : [],
        credits: credits.map((c) => ({
          tconst: c.tconst as string,
          primary_title: c.primary_title as string,
          title_type: c.title_type as string,
          start_year: c.start_year as number | null,
          category: c.category as string,
          characters: c.characters as string | null,
        })),
      };
    } finally {
      conn.release();
    }
  }
}

async function buildTitleDocuments(
  conn: mysql.PoolConnection,
  titles: RawTitle[],
): Promise<TitleDocument[]> {
  if (titles.length === 0) return [];

  const tconsts = titles.map((t) => t.tconst);

  const [koreanTitles] = await conn.query<mysql.RowDataPacket[]>(
    `SELECT tconst, title FROM TITLE_AKAS WHERE tconst IN (?) AND region = 'KR'`,
    [tconsts],
  );
  const [genres] = await conn.query<mysql.RowDataPacket[]>(
    `SELECT tg.tconst, g.name FROM TITLE_GENRES tg JOIN GENRES g ON tg.genre_id = g.id WHERE tg.tconst IN (?)`,
    [tconsts],
  );
  const [cast] = await conn.query<mysql.RowDataPacket[]>(
    `SELECT tp.tconst, tp.nconst, p.primary_name AS name, tp.category, tp.characters
     FROM TITLE_PRINCIPALS tp JOIN PERSONS p ON tp.nconst = p.nconst
     WHERE tp.tconst IN (?) ORDER BY tp.ordering`,
    [tconsts],
  );
  const [crew] = await conn.query<mysql.RowDataPacket[]>(
    `SELECT tconst, directors, writers FROM TITLE_CREW WHERE tconst IN (?)`,
    [tconsts],
  );

  const nconsts = new Set<string>();
  (crew as RawCrew[]).forEach((c) => {
    c.directors?.split(",").forEach((n) => nconsts.add(n.trim()));
    c.writers?.split(",").forEach((n) => nconsts.add(n.trim()));
  });

  const personsMap = new Map<string, string>();
  if (nconsts.size > 0) {
    const [persons] = await conn.query<mysql.RowDataPacket[]>(
      `SELECT nconst, primary_name FROM PERSONS WHERE nconst IN (?)`,
      [[...nconsts]],
    );
    (persons as RawPerson[]).forEach((p) => personsMap.set(p.nconst, p.primary_name));
  }

  const koreanMap = new Map<string, string>();
  (koreanTitles as RawKoreanTitle[]).forEach((k) => koreanMap.set(k.tconst, k.title));

  const genreMap = new Map<string, string[]>();
  (genres as RawGenre[]).forEach((g) => {
    const list = genreMap.get(g.tconst) ?? [];
    list.push(g.name);
    genreMap.set(g.tconst, list);
  });

  const castMap = new Map<string, CastMember[]>();
  (cast as RawCast[]).forEach((c) => {
    const list = castMap.get(c.tconst) ?? [];
    list.push({ nconst: c.nconst, name: c.name, category: c.category, characters: c.characters });
    castMap.set(c.tconst, list);
  });

  const crewMap = new Map<string, { directors: PersonRef[]; writers: PersonRef[] }>();
  (crew as RawCrew[]).forEach((c) => {
    const directors: PersonRef[] = (c.directors?.split(",") ?? [])
      .map((n) => n.trim()).filter((n) => personsMap.has(n))
      .map((n) => ({ nconst: n, name: personsMap.get(n)! }));
    const writers: PersonRef[] = (c.writers?.split(",") ?? [])
      .map((n) => n.trim()).filter((n) => personsMap.has(n))
      .map((n) => ({ nconst: n, name: personsMap.get(n)! }));
    crewMap.set(c.tconst, { directors, writers });
  });

  return titles.map((t) => ({
    tconst: t.tconst,
    primary_title: t.primary_title,
    original_title: t.original_title,
    korean_title: koreanMap.get(t.tconst) ?? null,
    title_type: t.title_type,
    start_year: t.start_year,
    end_year: t.end_year,
    runtime_minutes: t.runtime_minutes,
    is_adult: t.is_adult === 1,
    genres: genreMap.get(t.tconst) ?? [],
    average_rating: t.average_rating,
    num_votes: t.num_votes,
    cast: castMap.get(t.tconst) ?? [],
    directors: crewMap.get(t.tconst)?.directors ?? [],
    writers: crewMap.get(t.tconst)?.writers ?? [],
  }));
}

export async function fetchTitleBatch(
  pool: mysql.Pool,
  fromTconst: string | null,
  limit: number,
): Promise<TitleDocument[]> {
  const conn = await pool.getConnection();

  try {
    const [titles] = fromTconst === null
      ? await conn.query<mysql.RowDataPacket[]>(
          `SELECT t.tconst, t.primary_title, t.original_title, t.title_type,
                  t.start_year, t.end_year, t.runtime_minutes, t.is_adult,
                  r.average_rating, r.num_votes
           FROM TITLES t LEFT JOIN RATINGS r ON t.tconst = r.tconst
           ORDER BY t.tconst LIMIT ?`,
          [limit],
        )
      : await conn.query<mysql.RowDataPacket[]>(
          `SELECT t.tconst, t.primary_title, t.original_title, t.title_type,
                  t.start_year, t.end_year, t.runtime_minutes, t.is_adult,
                  r.average_rating, r.num_votes
           FROM TITLES t LEFT JOIN RATINGS r ON t.tconst = r.tconst
           WHERE t.tconst > ? ORDER BY t.tconst LIMIT ?`,
          [fromTconst, limit],
        );

    return buildTitleDocuments(conn, titles as RawTitle[]);
  } finally {
    conn.release();
  }
}

export async function* fetchTitleDocuments(
  pool: mysql.Pool,
  batchSize = 5000,
): AsyncGenerator<TitleDocument[]> {
  let offset = 0;

  while (true) {
    const conn = await pool.getConnection();

    try {
      const [titles] = await conn.query<mysql.RowDataPacket[]>(
        `SELECT t.tconst, t.primary_title, t.original_title, t.title_type,
                t.start_year, t.end_year, t.runtime_minutes, t.is_adult,
                r.average_rating, r.num_votes
         FROM TITLES t LEFT JOIN RATINGS r ON t.tconst = r.tconst
         ORDER BY t.tconst LIMIT ? OFFSET ?`,
        [batchSize, offset],
      );

      if (titles.length === 0) break;

      yield await buildTitleDocuments(conn, titles as RawTitle[]);
      offset += batchSize;
    } finally {
      conn.release();
    }
  }
}
