-- 장르 (title.basics의 genres)
CREATE TABLE GENRES(
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(50) NOT NULL UNIQUE,
    INDEX idx_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 작품 기본 정보 (title.basics.tsv.gz)
CREATE TABLE TITLES(
    id INT AUTO_INCREMENT PRIMARY KEY,
    tconst VARCHAR(20) NOT NULL UNIQUE,
    title_type VARCHAR(20),
    primary_title VARCHAR(500),
    original_title VARCHAR(500),
    is_adult BOOLEAN DEFAULT FALSE,
    start_year INT,
    end_year INT,
    runtime_minutes INT,
    INDEX idx_tconst (tconst),
    INDEX idx_title_type (title_type),
    INDEX idx_start_year (start_year)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 작품-장르 관계
CREATE TABLE TITLE_GENRES(
    id INT AUTO_INCREMENT PRIMARY KEY,
    title_id INT NOT NULL,
    genre_id INT NOT NULL,
    FOREIGN KEY (title_id) REFERENCES TITLES(id) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES GENRES(id) ON DELETE CASCADE,
    UNIQUE KEY unique_title_genre (title_id, genre_id),
    INDEX idx_title_id (title_id),
    INDEX idx_genre_id (genre_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 평점 (title.ratings.tsv.gz)
CREATE TABLE RATINGS(
    id INT AUTO_INCREMENT PRIMARY KEY,
    title_id INT NOT NULL,
    average_rating DECIMAL(3,1),
    num_votes INT,
    FOREIGN KEY (title_id) REFERENCES TITLES(id) ON DELETE CASCADE,
    INDEX idx_title_id (title_id),
    INDEX idx_average_rating (average_rating)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 에피소드 (title.episode.tsv.gz)
CREATE TABLE EPISODES(
    id INT AUTO_INCREMENT PRIMARY KEY,
    tconst VARCHAR(20) NOT NULL UNIQUE,
    parent_title_id INT NOT NULL,
    season_number INT,
    episode_number INT,
    FOREIGN KEY (parent_title_id) REFERENCES TITLES(id) ON DELETE CASCADE,
    INDEX idx_tconst (tconst),
    INDEX idx_parent_title_id (parent_title_id),
    INDEX idx_season_episode (season_number, episode_number)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 인물 (name.basics.tsv.gz)
CREATE TABLE PERSONS(
    id INT AUTO_INCREMENT PRIMARY KEY,
    nconst VARCHAR(20) NOT NULL UNIQUE,
    primary_name VARCHAR(200),
    birth_year INT,
    death_year INT,
    primary_profession VARCHAR(200),
    known_for_titles VARCHAR(200),
    INDEX idx_nconst (nconst),
    INDEX idx_primary_name (primary_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 작품-인물 관계 (title.principals.tsv.gz)
CREATE TABLE TITLE_PRINCIPALS(
    id INT AUTO_INCREMENT PRIMARY KEY,
    title_id INT NOT NULL,
    ordering INT NOT NULL,
    person_id INT NOT NULL,
    category VARCHAR(50),
    job VARCHAR(300),
    characters TEXT,
    FOREIGN KEY (title_id) REFERENCES TITLES(id) ON DELETE CASCADE,
    FOREIGN KEY (person_id) REFERENCES PERSONS(id) ON DELETE CASCADE,
    INDEX idx_title_id (title_id),
    INDEX idx_person_id (person_id),
    INDEX idx_category (category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 지역별 대체 제목 (title.akas.tsv.gz)
CREATE TABLE TITLE_AKAS(
    id INT AUTO_INCREMENT PRIMARY KEY,
    title_id INT NOT NULL,
    ordering INT NOT NULL,
    title VARCHAR(500),
    region VARCHAR(10),
    language VARCHAR(10),
    types VARCHAR(200),
    attributes TEXT,
    is_original_title BOOLEAN DEFAULT FALSE,
    FOREIGN KEY (title_id) REFERENCES TITLES(id) ON DELETE CASCADE,
    INDEX idx_title_id (title_id),
    INDEX idx_region (region),
    INDEX idx_language (language)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
