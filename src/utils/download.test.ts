import { downloadFile } from "./download.js";

export interface DatasetFile {
  name: string;
  description: string;
}

export interface DatasetsConfig {
  baseUrl: string;
  files: DatasetFile[];
}

export const datasets: DatasetsConfig = {
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

const testDownload = async () => {
  const downloadList = datasets.files.map((f) => {
    const dest = `./test-data/${f.name}`;
    const url = `${datasets.baseUrl}${f.name}`;
    return downloadFile(dest, url);
  });

  const results = await Promise.allSettled(downloadList);

  results.forEach((result, i) => {
    if (result.status === "fulfilled") {
      console.log(`${datasets.files[i]?.name} ok`);
    } else {
      console.error(`${datasets.files[i]?.name} fail:`, result.reason.message);
    }
  });
};

(async () => {
  await testDownload();
})();
