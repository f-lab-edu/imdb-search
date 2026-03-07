import { describe, it, expect, beforeAll, afterAll } from "@jest/globals";
import fs from "node:fs/promises";
import path from "node:path";
import os from "node:os";
import { generateTSVlines } from "../../utils/parse.js";
import type { TitleBasics, NameBasics, TitleRatings } from "../../utils/types.js";

let tmpDir: string;

beforeAll(async () => {
  tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "tsv-test-"));
});

afterAll(async () => {
  await fs.rm(tmpDir, { recursive: true, force: true });
});

const writeTSV = async (fileName: string, content: string) => {
  const filePath = path.join(tmpDir, fileName);
  await fs.writeFile(filePath, content, "utf-8");
  return filePath;
};

describe("generateTSVlines", () => {
  it("title.basics 형식 파싱", async () => {
    const tsv = [
      "tconst\ttitleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes\tgenres",
      "tt0000001\tshort\tCarmencita\tCarmencita\t0\t1894\t\\N\t1\tDocumentary,Short",
      "tt0000002\tshort\tLe clown\tLe clown\t0\t1892\t\\N\t5\tAnimation,Short",
    ].join("\n");

    const filePath = await writeTSV("title.basics.tsv", tsv);
    const results: TitleBasics[] = [];

    for await (const row of generateTSVlines<TitleBasics>(filePath)) {
      results.push(row);
    }

    expect(results.length).toBe(2);

    expect(results[0]!.tconst).toBe("tt0000001");
    expect(results[0]!.title_type).toBe("short");
    expect(results[0]!.primary_title).toBe("Carmencita");
    expect(results[0]!.is_adult).toBe("0");
    expect(results[0]!.start_year).toBe("1894");
    expect(results[0]!.end_year).toBeNull(); // \N -> null
    expect(results[0]!.runtime_minutes).toBe("1");
    expect(results[0]!.genres).toBe("Documentary,Short");
  });

  it("name.basics 형식 파싱", async () => {
    const tsv = [
      "nconst\tprimaryName\tbirthYear\tdeathYear\tprimaryProfession\tknownForTitles",
      "nm0000001\tFred Astaire\t1899\t1987\tactor,miscellaneous\ttt0053137,tt0072308",
      "nm0000002\tLauren Bacall\t1924\t2014\tactress,soundtrack\ttt0037382,tt0038355",
    ].join("\n");

    const filePath = await writeTSV("name.basics.tsv", tsv);
    const results: NameBasics[] = [];

    for await (const row of generateTSVlines<NameBasics>(filePath)) {
      results.push(row);
    }

    expect(results.length).toBe(2);

    expect(results[0]!.nconst).toBe("nm0000001");
    expect(results[0]!.primary_name).toBe("Fred Astaire");
    expect(results[0]!.birth_year).toBe("1899");
    expect(results[0]!.death_year).toBe("1987");
    expect(results[0]!.primary_profession).toBe("actor,miscellaneous");
    expect(results[0]!.known_for_titles).toBe("tt0053137,tt0072308");
  });

  it("title.ratings 형식 파싱", async () => {
    const tsv = [
      "tconst\taverageRating\tnumVotes",
      "tt0000001\t5.7\t2000",
      "tt0000002\t5.8\t300",
    ].join("\n");

    const filePath = await writeTSV("title.ratings.tsv", tsv);
    const results: TitleRatings[] = [];

    for await (const row of generateTSVlines<TitleRatings>(filePath)) {
      results.push(row);
    }

    expect(results.length).toBe(2);

    expect(results[0]!.tconst).toBe("tt0000001");
    expect(results[0]!.average_rating).toBe("5.7");
    expect(results[0]!.num_votes).toBe("2000");
  });

  it("\\N 값은 null로 변환", async () => {
    const tsv = ["tconst\tstartYear\tendYear", "tt0000001\t1894\t\\N", "tt0000002\t\\N\t\\N"].join(
      "\n"
    );

    const filePath = await writeTSV("null_test.tsv", tsv);
    const results: any[] = [];

    for await (const row of generateTSVlines(filePath)) {
      results.push(row);
    }

    expect(results[0]!.start_year).toBe("1894");
    expect(results[0]!.end_year).toBeNull();
    expect(results[1]!.start_year).toBeNull();
    expect(results[1]!.end_year).toBeNull();
  });

  it("빈 필드는 null로 변환", async () => {
    const tsv = ["tconst\tregion\tlanguage", "tt0000001\t\t"].join("\n");

    const filePath = await writeTSV("empty_field.tsv", tsv);
    const results: any[] = [];

    for await (const row of generateTSVlines(filePath)) {
      results.push(row);
    }

    expect(results[0]!.region).toBeNull();
    expect(results[0]!.language).toBeNull();
  });

  it("헤더만 있고 데이터 없으면 빈 결과", async () => {
    const tsv = "tconst\ttitleType\tprimaryTitle";

    const filePath = await writeTSV("header_only.tsv", tsv);
    const results: any[] = [];

    for await (const row of generateTSVlines(filePath)) {
      results.push(row);
    }

    expect(results.length).toBe(0);
  });

  it("camelCase 헤더가 snake_case로 매핑", async () => {
    const tsv = [
      "titleType\tprimaryTitle\toriginalTitle\tisAdult\tstartYear\tendYear\truntimeMinutes",
      "movie\tTest\tTest\t0\t2020\t\\N\t120",
    ].join("\n");

    const filePath = await writeTSV("header_map.tsv", tsv);
    const results: any[] = [];

    for await (const row of generateTSVlines(filePath)) {
      results.push(row);
    }

    const keys = Object.keys(results[0]!);
    expect(keys).toContain("title_type");
    expect(keys).toContain("primary_title");
    expect(keys).toContain("original_title");
    expect(keys).toContain("is_adult");
    expect(keys).toContain("start_year");
    expect(keys).toContain("end_year");
    expect(keys).toContain("runtime_minutes");
    expect(keys).not.toContain("titleType");
  });
});
