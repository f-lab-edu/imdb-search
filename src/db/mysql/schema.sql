-- 0. 작업 큐 - Redis overflow 및 phase 관리용
CREATE TABLE IF NOT EXISTS TASKS(
    id         BIGINT AUTO_INCREMENT PRIMARY KEY,
    task_id     VARCHAR(36) NOT NULL UNIQUE,
    batch_id   VARCHAR(36) NOT NULL,
    name       VARCHAR(50) NOT NULL,
    phase      TINYINT NOT NULL DEFAULT 1,
    payload    JSON NOT NULL,
    status     ENUM('pending', 'queued', 'failed') NOT NULL DEFAULT 'pending',
    retry_count TINYINT NOT NULL DEFAULT 0,
    created_at BIGINT NOT NULL,
    INDEX idx_status_phase (status, phase),
    INDEX idx_batch_id (batch_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 1. 장르 - title.basics.tsv.gz
CREATE TABLE IF NOT EXISTS GENRES(
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 2. 작품 기본 정보 - title.basics.tsv.gz
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

-- 3. 인물 기본 정보 - name.basics.tsv.gz
CREATE TABLE IF NOT EXISTS PERSONS(
    nconst VARCHAR(20) PRIMARY KEY,
    primary_name VARCHAR(200),
    birth_year INT,
    death_year INT,
    primary_profession VARCHAR(500),
    known_for_titles VARCHAR(500),
    INDEX idx_primary_name (primary_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 4. 작품-장르 관계 - title.basics.tsv.gz
CREATE TABLE IF NOT EXISTS TITLE_GENRES(
    id INT AUTO_INCREMENT PRIMARY KEY,
    tconst VARCHAR(20) NOT NULL,
    genre_id INT NOT NULL,
    UNIQUE KEY unique_title_genre (tconst, genre_id),
    INDEX idx_tconst (tconst),
    FOREIGN KEY (tconst) REFERENCES TITLES(tconst),
    FOREIGN KEY (genre_id) REFERENCES GENRES(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 5. 지역별 대체 제목 - title.akas.tsv.gz
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
    INDEX idx_region (region),
    FOREIGN KEY (tconst) REFERENCES TITLES(tconst)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 6. 평점 정보 - title.ratings.tsv.gz
CREATE TABLE IF NOT EXISTS RATINGS(
    tconst VARCHAR(20) PRIMARY KEY,
    average_rating DECIMAL(3,1),
    num_votes INT,
    INDEX idx_rating (average_rating),
    FOREIGN KEY (tconst) REFERENCES TITLES(tconst)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 7. 에피소드 정보 - title.episode.tsv.gz
CREATE TABLE IF NOT EXISTS EPISODES(
    tconst VARCHAR(20) PRIMARY KEY,
    parent_tconst VARCHAR(20) NOT NULL,
    season_number INT,
    episode_number INT,
    INDEX idx_parent_tconst (parent_tconst),
    FOREIGN KEY (tconst) REFERENCES TITLES(tconst),
    FOREIGN KEY (parent_tconst) REFERENCES TITLES(tconst)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 8. 제작진 및 작가 정보 - title.crew.tsv.gz
CREATE TABLE IF NOT EXISTS TITLE_CREW(
    tconst VARCHAR(20) PRIMARY KEY,
    directors TEXT,
    writers TEXT,
    FOREIGN KEY (tconst) REFERENCES TITLES(tconst)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 9. 작품별 주요 출연진/스태프 - title.principals.tsv.gz
CREATE TABLE IF NOT EXISTS TITLE_PRINCIPALS(
    id INT AUTO_INCREMENT PRIMARY KEY,
    tconst VARCHAR(20) NOT NULL,
    ordering INT NOT NULL,
    nconst VARCHAR(20) NOT NULL,
    category VARCHAR(100),
    job VARCHAR(500),
    characters TEXT,
    INDEX idx_tconst (tconst),
    INDEX idx_nconst (nconst),
    FOREIGN KEY (tconst) REFERENCES TITLES(tconst),
    FOREIGN KEY (nconst) REFERENCES PERSONS(nconst)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
