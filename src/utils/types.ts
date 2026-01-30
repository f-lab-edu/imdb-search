export type DatasetType =
  | TitleBasics
  | TitleRatings
  | TitleAkas
  | TitleEpisode
  | TitlePrincipals
  | NameBasics
  | TitleCrew;

/**
 * title.basics.tsv.gz
 * 작품 기본 정보
 */
export interface TitleBasics {
  tconst: string;
  titleType: string;
  primaryTitle: string;
  originalTitle: string;
  isAdult: string; // '0' or '1'
  startYear: string | null;
  endYear: string | null;
  runtimeMinutes: string | null;
  genres: string | null; // 'Action,Drama,Thriller' 형태
}

/**
 * title.ratings.tsv.gz
 * 작품 평점 정보
 */
export interface TitleRatings {
  tconst: string;
  averageRating: string;
  numVotes: string;
}

/**
 * title.akas.tsv.gz
 * 지역별 대체 제목
 */
export interface TitleAkas {
  titleId: string;
  ordering: string;
  title: string;
  region: string | null;
  language: string | null;
  types: string | null; // 'dvd,festival' 형태
  attributes: string | null;
  isOriginalTitle: string; // '0' or '1'
}

/**
 * title.episode.tsv.gz
 * 에피소드 정보
 */
export interface TitleEpisode {
  tconst: string;
  parentTconst: string;
  seasonNumber: string | null;
  episodeNumber: string | null;
}

/**
 * title.principals.tsv.gz
 * 작품-인물 관계
 */
export interface TitlePrincipals {
  tconst: string;
  ordering: string;
  nconst: string;
  category: string;
  job: string | null;
  characters: string | null; // JSON 배열 문자열 형태
}

/**
 * name.basics.tsv.gz
 * 인물 정보
 */
export interface NameBasics {
  nconst: string;
  primaryName: string;
  birthYear: string | null;
  deathYear: string | null;
  primaryProfession: string | null; // 'actor,producer,writer' 형태
  knownForTitles: string | null; // 'tt0000001,tt0000002' 형태
}

/**
 * title.crew.tsv.gz (스키마에는 없지만 IMDb에 존재)
 * 감독/작가 정보
 */
export interface TitleCrew {
  tconst: string;
  directors: string | null; // 'nm0000001,nm0000002' 형태
  writers: string | null; // 'nm0000001,nm0000002' 형태
}
