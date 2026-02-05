const DB_CONFIG = {
  mysql: {
    host: process.env.DB_HOST || "localhost",
    user: process.env.DB_USER || "root",
    password: process.env.DB_PASSWORD || "",
    database: process.env.DB_NAME || "test",
    port: Number(process.env.DB_PORT) || 3306,
    batchSize: Number(process.env.DB_BATCH_SIZE) || 1000,
  },
  redis: {
    socket: {
      host: process.env.REDIS_HOST || "localhost",
    },
    password: process.env.REDIS_PASSWORD || "",
  },
  opensearch: {
    node: process.env.OPENSEARCH_NODE || "http://localhost:9200",
    auth: {
      username: process.env.OPENSEARCH_USER || "admin",
      password: process.env.OPENSEARCH_PASSWORD || "",
    },

    ssl: {
      rejectUnauthorized: process.env.NODE_ENV === "production",
    },
  },
};

const DATASET_CONFIG = {
  downloadDir:
    process.env.IMDB_DATASETS ||
    (() => {
      throw new Error("download directory has not been specified");
    })(),
  baseUrl: "https://datasets.imdbws.com/",
  files: [
    {
      name: "name.basics.tsv.gz",
      description: "인물 정보 (배우, 감독, 작가 등)",
    },
    {
      name: "title.akas.tsv.gz",
      description: "지역별 대체 제목 정보",
    },
    {
      name: "title.basics.tsv.gz",
      description: "작품 기본 정보 (제목, 장르, 연도 등)",
    },
    {
      name: "title.crew.tsv.gz",
      description: "감독 및 작가 정보",
    },
    {
      name: "title.episode.tsv.gz",
      description: "TV 시리즈 에피소드 정보",
    },
    {
      name: "title.principals.tsv.gz",
      description: "작품별 주요 출연진 및 스태프",
    },
    {
      name: "title.ratings.tsv.gz",
      description: "작품 평점 및 투표수",
    },
  ],
};

export const config = {
  db: DB_CONFIG,
  datasets: DATASET_CONFIG,
};
