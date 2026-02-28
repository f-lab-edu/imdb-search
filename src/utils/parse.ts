import fs, { type PathLike } from "node:fs";
import readline from "node:readline";
import type { DatasetType } from "./types.js";

// imdb 헤더 예외 케이스들
const IMDB_HEADER_MAP: Record<string, string> = {
  titleId: "tconst",
  parentTconst: "parent_tconst",
  averageRating: "average_rating",
  numVotes: "num_votes",
  titleType: "title_type",
  primaryTitle: "primary_title",
  originalTitle: "original_title",
  isAdult: "is_adult",
  startYear: "start_year",
  endYear: "end_year",
  runtimeMinutes: "runtime_minutes",
  isOriginalTitle: "is_original_title",
  seasonNumber: "season_number",
  episodeNumber: "episode_number",
  primaryName: "primary_name",
  birthYear: "birth_year",
  death_year: "death_year",
  primaryProfession: "primary_profession",
  knownForTitles: "known_for_titles",
};

export const toSnakeCase = (str: string) =>
  str.replace(/[A-Z]/g, (letter) => `_${letter.toLowerCase()}`);

const mapHeader = (h: string) => IMDB_HEADER_MAP[h] || toSnakeCase(h);

export async function* generateTSVlines<T extends DatasetType>(
  filePath: PathLike,
): AsyncGenerator<T> {
  const fileStream = fs.createReadStream(filePath, { encoding: "utf8" });
  const rl = readline.createInterface({ input: fileStream });

  try {
    const iterator = rl[Symbol.asyncIterator]();
    const firstLine = (await iterator.next()).value;

    if (!firstLine) throw new Error("failed to find headers");

    const headers = firstLine.split("\t").map((h) => mapHeader(h));

    for await (const line of iterator) {
      const fields = line.split("\t");
      const obj = headers.reduce(
        (acc, curr, idx) => {
          acc[curr] = fields[idx] == "\\N" || !fields[idx] ? null : fields[idx];
          return acc;
        },
        {} as Record<string, string | null>,
      );

      yield obj as unknown as T;
    }
  } finally {
    rl.close();
  }
}
