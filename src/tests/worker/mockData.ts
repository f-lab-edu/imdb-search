import fs from "node:fs/promises";
import path from "node:path";

export const createMockDatasets = async (dir: string) => {
  // 1. title.basics.tsv
  const basics = [
    "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres",
    "tt0000001\tshort\tMock Movie 1\tMock 1\t0\t2024\t\\N\t90\tAction,Drama",
    "tt0000002\tshort\tMock Movie 2\tMock 2\t0\t2025\t\\N\t120\tComedy",
  ].join("\n");

  // 2. title.ratings.tsv
  const ratings = [
    "tconst\taverageRating\tnumVotes",
    "tt0000001\t8.5\t1000",
    "tt0000002\t7.2\t500",
  ].join("\n");

  await fs.writeFile(path.join(dir, "title.basics.tsv"), basics);
  await fs.writeFile(path.join(dir, "title.ratings.tsv"), ratings);

  return dir;
};
