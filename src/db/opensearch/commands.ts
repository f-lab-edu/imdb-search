import { Client } from "@opensearch-project/opensearch";

export interface TitleDocument {
  tconst: string;
  primary_title: string;
  original_title: string;
  korean_title: string | null;
  title_type: string;
  start_year: number | null;
  end_year: number | null;
  runtime_minutes: number | null;
  is_adult: boolean;
  genres: string[];
  average_rating: number | null;
  num_votes: number | null;
  cast: CastMember[];
  directors: PersonRef[];
  writers: PersonRef[];
}

export interface CastMember {
  nconst: string;
  name: string;
  category: string;
  characters: string | null;
}

export interface PersonRef {
  nconst: string;
  name: string;
}

export const INDEX_NAME = "imdb_titles";

const INDEX_MAPPING = {
  settings: {
    analysis: {
      analyzer: {
        korean: {
          type: "custom",
          tokenizer: "nori_tokenizer",
        },
        korean_search: {
          type: "custom",
          tokenizer: "nori_tokenizer",
          filter: ["nori_part_of_speech"],
        },
      },
    },
  },
  mappings: {
    properties: {
      tconst: { type: "keyword" },
      primary_title: {
        type: "text",
        analyzer: "standard",
        fields: { keyword: { type: "keyword" } },
      },
      original_title: { type: "text", analyzer: "standard" },
      korean_title: {
        type: "text",
        analyzer: "korean",
        search_analyzer: "korean_search",
      },
      title_type: { type: "keyword" },
      start_year: { type: "integer" },
      end_year: { type: "integer" },
      runtime_minutes: { type: "integer" },
      is_adult: { type: "boolean" },
      genres: { type: "keyword" },
      average_rating: { type: "float" },
      num_votes: { type: "integer" },
      cast: {
        type: "nested",
        properties: {
          nconst: { type: "keyword" },
          name: {
            type: "text",
            analyzer: "standard",
            fields: {
              nori: { type: "text", analyzer: "korean" },
              keyword: { type: "keyword" },
            },
          },
          category: { type: "keyword" },
          characters: { type: "text", analyzer: "standard" },
        },
      },
      directors: {
        type: "nested",
        properties: {
          nconst: { type: "keyword" },
          name: {
            type: "text",
            analyzer: "standard",
            fields: {
              nori: { type: "text", analyzer: "korean" },
              keyword: { type: "keyword" },
            },
          },
        },
      },
      writers: {
        type: "nested",
        properties: {
          nconst: { type: "keyword" },
          name: {
            type: "text",
            analyzer: "standard",
            fields: {
              nori: { type: "text", analyzer: "korean" },
              keyword: { type: "keyword" },
            },
          },
        },
      },
    },
  },
};

export class OpenSearchCommand {
  constructor(private readonly client: Client) {}

  async createIndex(recreate = false): Promise<void> {
    const exists = await this.client.indices.exists({ index: INDEX_NAME });

    if (exists.body) {
      if (!recreate) {
        console.log(`[opensearch] index '${INDEX_NAME}' already exists, skipping`);
        return;
      }
      console.log(`[opensearch] deleting existing index '${INDEX_NAME}'`);
      await this.client.indices.delete({ index: INDEX_NAME });
    }

    await this.client.indices.create({
      index: INDEX_NAME,
      body: INDEX_MAPPING as any,
    });

    console.log(`[opensearch] index '${INDEX_NAME}' created`);
  }

  async bulkIndex(documents: TitleDocument[]): Promise<number> {
    if (documents.length === 0) return 0;

    const body = documents.flatMap((doc) => [
      { index: { _index: INDEX_NAME, _id: doc.tconst } },
      doc,
    ]);

    const response = await this.client.bulk({ body });

    if (response.body.errors) {
      const errors = (response.body.items as any[])
        .filter((item) => item.index?.error)
        .map((item) => item.index.error);
      console.error(`[opensearch] bulk index errors: ${errors.length}`, errors[0]);
    }

    return documents.length - (response.body.errors ? 1 : 0);
  }

  async totalIndexed(): Promise<number> {
    const res = await this.client.count({ index: INDEX_NAME });
    return res.body.count as number;
  }
}
