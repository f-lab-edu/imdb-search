import dotenv from "dotenv";
dotenv.config({ path: ".env.test" });

import { describe, beforeAll, afterAll, it, expect } from "@jest/globals";
import type { OpenSearchDatabase } from "../../db/opensearch/connection.js";
import { OpenSearchQuery } from "../../db/opensearch/queries.js";

let osDb: OpenSearchDatabase;
let osQuery: OpenSearchQuery;

beforeAll(async () => {
  const { config } = await import("../../config/index.js");
  const { OpenSearchDatabase: OSDB } = await import("../../db/opensearch/connection.js");

  osDb = new OSDB(config.db.opensearch);
  osQuery = new OpenSearchQuery(osDb.getClient());
});

afterAll(async () => {
  await osDb.close();
});

describe("OpenSearchQuery.searchTitles", () => {
  it("returns results for a basic text query", async () => {
    const result = await osQuery.searchTitles({ q: "inception" });

    expect(result.total).toBeGreaterThan(0);
    expect(result.hits.length).toBeGreaterThan(0);
    expect(result.hits[0]).toHaveProperty("tconst");
    expect(result.hits[0]).toHaveProperty("primary_title");
  });

  it("filters by title type", async () => {
    const result = await osQuery.searchTitles({ q: "batman", type: "movie" });

    expect(result.hits.length).toBeGreaterThan(0);
    result.hits.forEach((h) => expect(h.title_type).toBe("movie"));
  });

  it("filters by genre", async () => {
    const result = await osQuery.searchTitles({ q: "batman", genre: "Action" });

    expect(result.hits.length).toBeGreaterThan(0);
    result.hits.forEach((h) => expect(h.genres).toContain("Action"));
  });

  it("filters by year range", async () => {
    const result = await osQuery.searchTitles({ q: "batman", yearFrom: 2000, yearTo: 2010 });

    expect(result.hits.length).toBeGreaterThan(0);
    result.hits.forEach((h) => {
      if (h.start_year !== null) {
        expect(h.start_year).toBeGreaterThanOrEqual(2000);
        expect(h.start_year).toBeLessThanOrEqual(2010);
      }
    });
  });

  it("filters by rating range", async () => {
    const result = await osQuery.searchTitles({ ratingMin: 8.0, ratingMax: 10.0, size: 10 });

    expect(result.hits.length).toBeGreaterThan(0);
    result.hits.forEach((h) => {
      if (h.average_rating !== null) {
        expect(Number(h.average_rating)).toBeGreaterThanOrEqual(8.0);
      }
    });
  });

  it("excludes adult content by default", async () => {
    const result = await osQuery.searchTitles({ q: "test" });

    result.hits.forEach((h) => expect(h.is_adult).toBe(false));
  });

  it("sorts by rating descending", async () => {
    const result = await osQuery.searchTitles({ sort: "rating", size: 10 });

    const ratings = result.hits
      .map((h) => (h.average_rating !== null ? Number(h.average_rating) : null))
      .filter((r): r is number => r !== null);

    for (let i = 1; i < ratings.length; i++) {
      expect(ratings[i]!).toBeLessThanOrEqual(ratings[i - 1]!);
    }
  });

  it("sorts by year descending", async () => {
    const result = await osQuery.searchTitles({ sort: "year", size: 10 });

    const years = result.hits.map((h) => h.start_year).filter((y): y is number => y !== null);

    for (let i = 1; i < years.length; i++) {
      expect(years[i]!).toBeLessThanOrEqual(years[i - 1]!);
    }
  });

  it("paginates correctly", async () => {
    const page1 = await osQuery.searchTitles({ q: "the", page: 1, size: 5 });
    const page2 = await osQuery.searchTitles({ q: "the", page: 2, size: 5 });

    expect(page1.hits.length).toBe(5);
    expect(page2.hits.length).toBeGreaterThan(0);
    expect(page1.hits[0]!.tconst).not.toBe(page2.hits[0]!.tconst);
  });

  it("returns empty hits for no match", async () => {
    const result = await osQuery.searchTitles({ q: "xyzzy_no_match_12345" });

    expect(result.total).toBe(0);
    expect(result.hits.length).toBe(0);
  });

  it("works without q (filter only)", async () => {
    const result = await osQuery.searchTitles({ genre: "Action", yearFrom: 2020, size: 5 });

    expect(result.hits.length).toBeGreaterThan(0);
  });
});
