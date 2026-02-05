export type TableName =
  | "TITLES"
  | "GENRES"
  | "TITLE_GENRES"
  | "RATINGS"
  | "EPISODES"
  | "PERSONS"
  | "TITLE_PRINCIPALS"
  | "TITLE_AKAS";

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
  title_type: string;
  primary_title: string;
  original_title: string;
  is_adult: string; // '0' or '1'
  start_year: string | null;
  end_year: string | null;
  runtime_minutes: string | null;
  genres: string | null; // 'Action,Drama,Thriller' 형태
}

/**
 * title.ratings.tsv.gz
 * 작품 평점 정보
 */
export interface TitleRatings {
  tconst: string;
  average_rating: string;
  num_votes: string;
}

/**
 * title.akas.tsv.gz
 * 지역별 대체 제목
 */
export interface TitleAkas {
  title_id: string;
  ordering: string;
  title: string;
  region: string | null;
  language: string | null;
  types: string | null; // 'dvd,festival' 형태
  attributes: string | null;
  is_original_title: string; // '0' or '1'
}

/**
 * title.episode.tsv.gz
 * 에피소드 정보
 */
export interface TitleEpisode {
  tconst: string;
  parent_tconst: string;
  season_number: string | null;
  episode_number: string | null;
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
  primary_name: string;
  birth_year: string | null;
  death_year: string | null;
  primary_profession: string | null; // 'actor,producer,writer' 형태
  known_for_titles: string | null; // 'tt0000001,tt0000002' 형태
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
