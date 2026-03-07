import type { Client } from "@opensearch-project/opensearch";
import { INDEX_NAME, type TitleDocument } from "./commands.js";

export interface SearchParams {
  q?: string;
  type?: string;
  genre?: string;
  yearFrom?: number;
  yearTo?: number;
  ratingMin?: number;
  ratingMax?: number;
  adult?: boolean;
  sort?: "rating" | "year" | "votes" | "relevance";
  page?: number;
  size?: number;
}

export interface SearchResult {
  total: number;
  page: number;
  size: number;
  hits: TitleDocument[];
}

export class OpenSearchQuery {
  constructor(private readonly client: Client) {}

  async searchTitles(params: SearchParams): Promise<SearchResult> {
    const {
      q,
      type,
      genre,
      yearFrom,
      yearTo,
      ratingMin,
      ratingMax,
      adult = false,
      sort = "relevance",
      page = 1,
      size = 20,
    } = params;

    const filters: object[] = [];

    if (!adult) {
      filters.push({ term: { is_adult: false } });
    }

    if (type) {
      filters.push({ term: { title_type: type } });
    }

    if (genre) {
      filters.push({ term: { genres: genre } });
    }

    if (yearFrom !== undefined || yearTo !== undefined) {
      const yearRange: Record<string, number> = {};
      if (yearFrom !== undefined) yearRange.gte = yearFrom;
      if (yearTo !== undefined) yearRange.lte = yearTo;
      filters.push({ range: { start_year: yearRange } });
    }

    if (ratingMin !== undefined || ratingMax !== undefined) {
      const ratingRange: Record<string, number> = {};
      if (ratingMin !== undefined) ratingRange.gte = ratingMin;
      if (ratingMax !== undefined) ratingRange.lte = ratingMax;
      filters.push({ range: { average_rating: ratingRange } });
    }

    let query: object;
    if (q) {
      query = {
        bool: {
          must: [
            {
              multi_match: {
                query: q,
                fields: [
                  "primary_title^3",
                  "original_title^2",
                  "korean_title^2",
                  "cast.name",
                  "directors.name",
                ],
                type: "best_fields",
                fuzziness: "AUTO",
              },
            },
          ],
          filter: filters,
        },
      };
    } else {
      query = { bool: { filter: filters } };
    }

    let sortClause: any[];

    if (sort === "rating") {
      sortClause = [{ average_rating: { order: "desc" } }, "_score"];
    } else if (sort === "year") {
      sortClause = [{ start_year: { order: "desc" } }, "_score"];
    } else if (sort === "votes") {
      sortClause = [{ num_votes: { order: "desc" } }, "_score"];
    } else {
      sortClause = ["_score", { num_votes: { order: "desc" } }];
    }

    const from = (page - 1) * size;

    const response = await this.client.search({
      index: INDEX_NAME,
      body: {
        query,
        sort: sortClause,
        from,
        size,
        _source: { excludes: ["cast", "directors", "writers"] },
      },
    });

    const body = response.body;
    const totalRaw = body.hits.total;
    const total = typeof totalRaw === "number" ? totalRaw : (totalRaw?.value ?? 0);

    return {
      total,
      page,
      size,
      hits: body.hits.hits.map((h: any) => h._source as TitleDocument),
    };
  }
}
