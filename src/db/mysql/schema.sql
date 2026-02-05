-- 스키마 수정 - 동시 입력 시 외래키 제약 문제 발생 - 각 테이블 간 연결을 원본 데이터에 들어있는 tconst로 연결

-- 1. 장르 (이름 자체가 고유하므로 ID와 함께 관리)
-- title.basics 처리 시 장르명을 수집하여 여기에 먼저 INSERT IGNORE 한 뒤 ID를 가져옴
CREATE TABLE IF NOT EXISTS GENRES(
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    INDEX idx_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 2. 작품 기본 정보 (title.basics.tsv.gz)
-- tconst(tt0000001)를 PK로 사용하여 숫자 ID 생성을 기다리지 않음
CREATE TABLE IF NOT EXISTS TITLES(
    tconst VARCHAR(20) PRIMARY KEY,
    title_type VARCHAR(50),
    primary_title VARCHAR(500),
    original_title VARCHAR(500),
    is_adult BOOLEAN DEFAULT FALSE,
    start_year INT,
    end_year INT,
    runtime_minutes INT,
    INDEX idx_title_type (title_type),
    INDEX idx_start_year (start_year)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 3. 작품-장르 관계
-- tconst를 사용하여 TITLES 테이블 조회 없이 바로 삽입 가능
CREATE TABLE IF NOT EXISTS TITLE_GENRES(
    id INT AUTO_INCREMENT PRIMARY KEY,
    tconst VARCHAR(20) NOT NULL,
    genre_id INT NOT NULL,
    UNIQUE KEY unique_title_genre (tconst, genre_id),
    INDEX idx_tconst (tconst)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 4. 지역별 대체 제목 (title.akas.tsv.gz)
CREATE TABLE IF NOT EXISTS TITLE_AKAS(
    id INT AUTO_INCREMENT PRIMARY KEY,
    tconst VARCHAR(20) NOT NULL,
    ordering INT NOT NULL,
    title VARCHAR(500),
    region VARCHAR(10),
    language VARCHAR(10),
    types VARCHAR(200),
    attributes TEXT,
    is_original_title BOOLEAN DEFAULT FALSE,
    INDEX idx_tconst (tconst),
    INDEX idx_region (region)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 5. 평점 정보 (title.ratings.tsv.gz)
-- 1:1 관계이므로 tconst가 PK 역할을 수행
CREATE TABLE IF NOT EXISTS RATINGS(
    tconst VARCHAR(20) PRIMARY KEY,
    average_rating DECIMAL(3,1),
    num_votes INT,
    INDEX idx_rating (average_rating)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 6. 에피소드 정보 (title.episode.tsv.gz)
CREATE TABLE IF NOT EXISTS EPISODES(
    tconst VARCHAR(20) PRIMARY KEY,
    parent_tconst VARCHAR(20) NOT NULL,
    season_number INT,
    episode_number INT,
    INDEX idx_parent_tconst (parent_tconst)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 7. 인물 기본 정보 (name.basics.tsv.gz)
-- nconst(nm0000001)를 PK로 사용
CREATE TABLE IF NOT EXISTS PERSONS(
    nconst VARCHAR(20) PRIMARY KEY,
    primary_name VARCHAR(200),
    birth_year INT,
    death_year INT,
    primary_profession VARCHAR(500),
    known_for_titles VARCHAR(500),
    INDEX idx_primary_name (primary_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 8. 제작진 및 작가 정보 (title.crew.tsv.gz)
CREATE TABLE IF NOT EXISTS TITLE_CREW(
    tconst VARCHAR(20) PRIMARY KEY,
    directors TEXT,
    writers TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 9. 작품별 주요 출연진/스태프 (title.principals.tsv.gz)
CREATE TABLE IF NOT EXISTS TITLE_PRINCIPALS(
    id INT AUTO_INCREMENT PRIMARY KEY,
    tconst VARCHAR(20) NOT NULL,
    ordering INT NOT NULL,
    nconst VARCHAR(20) NOT NULL,
    category VARCHAR(100),
    job VARCHAR(500),
    characters TEXT,
    INDEX idx_tconst (tconst),
    INDEX idx_nconst (nconst)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
