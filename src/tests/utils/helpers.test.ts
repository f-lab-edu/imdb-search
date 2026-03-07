import dotenv from "dotenv";
import { describe, it, expect, beforeAll } from "@jest/globals";
import { toSnakeCase } from "../../utils/parse.js";

describe("toSnakeCase", () => {
  it("camelCase를 snake_case로 변환", () => {
    expect(toSnakeCase("titleType")).toBe("title_type");
    expect(toSnakeCase("primaryTitle")).toBe("primary_title");
    expect(toSnakeCase("isAdult")).toBe("is_adult");
  });

  it("이미 snake_case면 그대로", () => {
    expect(toSnakeCase("tconst")).toBe("tconst");
    expect(toSnakeCase("nconst")).toBe("nconst");
    expect(toSnakeCase("genres")).toBe("genres");
  });

  it("연속 대문자", () => {
    expect(toSnakeCase("parseHTML")).toBe("parse_h_t_m_l");
  });

  it("빈 문자열", () => {
    expect(toSnakeCase("")).toBe("");
  });
});

describe("isPrimary", () => {
  let isPrimary: any;

  beforeAll(async () => {
    dotenv.config({ path: ".env.test" });
    const helpers = await import("../../utils/helpers.js");
    isPrimary = helpers.isPrimary;
  });

  it("TITLE_BASICS는 primary", () => {
    expect(isPrimary("TITLE_BASICS")).toBe(true);
  });

  it("NAME_BASICS는 primary", () => {
    expect(isPrimary("NAME_BASICS")).toBe(true);
  });

  it("TITLE_RATINGS는 primary 아님", () => {
    expect(isPrimary("TITLE_RATINGS")).toBe(false);
  });

  it("TITLE_AKAS는 primary 아님", () => {
    expect(isPrimary("TITLE_AKAS")).toBe(false);
  });

  it("빈 문자열은 primary 아님", () => {
    expect(isPrimary("")).toBe(false);
  });
});

describe("getDatasetInfoByFileName", () => {
  let getDatasetInfoByFileName: any;

  beforeAll(async () => {
    dotenv.config({ path: ".env.test" });
    const helpers = await import("../../utils/helpers.js");
    getDatasetInfoByFileName = helpers.getDatasetInfoByFileName;
  });

  it("title.basics.tsv.gz", () => {
    const result = getDatasetInfoByFileName("title.basics.tsv.gz");

    expect(result).not.toBeNull();
    expect(result!.type).toBe("TITLE_BASICS");
    expect(result!.isPrimary).toBe(true);
  });

  it("name.basics.tsv.gz", () => {
    const result = getDatasetInfoByFileName("name.basics.tsv.gz");

    expect(result).not.toBeNull();
    expect(result!.type).toBe("NAME_BASICS");
    expect(result!.isPrimary).toBe(true);
  });

  it("title.ratings.tsv.gz", () => {
    const result = getDatasetInfoByFileName("title.ratings.tsv.gz");

    expect(result).not.toBeNull();
    expect(result!.type).toBe("TITLE_RATINGS");
    expect(result!.isPrimary).toBe(false);
  });

  it("title.akas.tsv.gz", () => {
    const result = getDatasetInfoByFileName("title.akas.tsv.gz");

    expect(result).not.toBeNull();
    expect(result!.type).toBe("TITLE_AKAS");
    expect(result!.isPrimary).toBe(false);
  });

  it("title.episode.tsv.gz", () => {
    const result = getDatasetInfoByFileName("title.episode.tsv.gz");

    expect(result).not.toBeNull();
    expect(result!.type).toBe("TITLE_EPISODE");
    expect(result!.isPrimary).toBe(false);
  });

  it("title.principals.tsv.gz", () => {
    const result = getDatasetInfoByFileName("title.principals.tsv.gz");

    expect(result).not.toBeNull();
    expect(result!.type).toBe("TITLE_PRINCIPAL");
    expect(result!.isPrimary).toBe(false);
  });

  it("title.crew.tsv.gz", () => {
    const result = getDatasetInfoByFileName("title.crew.tsv.gz");

    expect(result).not.toBeNull();
    expect(result!.type).toBe("TITLE_CREW");
    expect(result!.isPrimary).toBe(false);
  });

  it("존재하지 않는 파일명은 null", () => {
    expect(getDatasetInfoByFileName("unknown.tsv.gz")).toBeNull();
  });

  it("빈 문자열은 null", () => {
    expect(getDatasetInfoByFileName("")).toBeNull();
  });
});
