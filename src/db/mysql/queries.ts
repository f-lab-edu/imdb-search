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
         FROM TITLES t
         LEFT JOIN RATINGS r ON t.tconst = r.tconst
         ORDER BY t.tconst
         LIMIT ? OFFSET ?`,
        [batchSize, offset],
      );

      if (titles.length === 0) break;

      const tconsts = (titles as RawTitle[]).map((t) => t.tconst);

      const [koreanTitles] = await conn.query<mysql.RowDataPacket[]>(
        `SELECT tconst, title FROM TITLE_AKAS
         WHERE tconst IN (?) AND region = 'KR'`,
        [tconsts],
      );

      const [genres] = await conn.query<mysql.RowDataPacket[]>(
        `SELECT tg.tconst, g.name FROM TITLE_GENRES tg
         JOIN GENRES g ON tg.genre_id = g.id
         WHERE tg.tconst IN (?)`,
        [tconsts],
      );

      const [cast] = await conn.query<mysql.RowDataPacket[]>(
        `SELECT tp.tconst, tp.nconst, p.primary_name AS name,
                tp.category, tp.characters
         FROM TITLE_PRINCIPALS tp
         JOIN PERSONS p ON tp.nconst = p.nconst
         WHERE tp.tconst IN (?)
         ORDER BY tp.ordering`,
        [tconsts],
      );

      const [crew] = await conn.query<mysql.RowDataPacket[]>(
        `SELECT tconst, directors, writers FROM TITLE_CREW
         WHERE tconst IN (?)`,
        [tconsts],
      );

      const nconsts = new Set<string>();
      (crew as RawCrew[]).forEach((c) => {
        c.directors?.split(",").forEach((n) => nconsts.add(n.trim()));
        c.writers?.split(",").forEach((n) => nconsts.add(n.trim()));
      });

      let personsMap = new Map<string, string>();
      if (nconsts.size > 0) {
        const [persons] = await conn.query<mysql.RowDataPacket[]>(
          `SELECT nconst, primary_name FROM PERSONS WHERE nconst IN (?)`,
          [[...nconsts]],
        );
        (persons as RawPerson[]).forEach((p) => {
          personsMap.set(p.nconst, p.primary_name);
        });
      }

      // build lookup maps
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
        list.push({
          nconst: c.nconst,
          name: c.name,
          category: c.category,
          characters: c.characters,
        });
        castMap.set(c.tconst, list);
      });

      const crewMap = new Map<string, { directors: PersonRef[]; writers: PersonRef[] }>();
      (crew as RawCrew[]).forEach((c) => {
        const directors: PersonRef[] = (c.directors?.split(",") ?? [])
          .map((n) => n.trim())
          .filter((n) => personsMap.has(n))
          .map((n) => ({ nconst: n, name: personsMap.get(n)! }));

        const writers: PersonRef[] = (c.writers?.split(",") ?? [])
          .map((n) => n.trim())
          .filter((n) => personsMap.has(n))
          .map((n) => ({ nconst: n, name: personsMap.get(n)! }));

        crewMap.set(c.tconst, { directors, writers });
      });

      const documents: TitleDocument[] = (titles as RawTitle[]).map((t) => ({
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

      yield documents;
      offset += batchSize;
    } finally {
      conn.release();
    }
  }
}
