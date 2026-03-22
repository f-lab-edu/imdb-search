-- FK 정합성 체크 쿼리 (LOAD DATA LOCAL INFILE + FK_CHECKS=0 삽입 후 검증용)
-- 고아 행(orphaned rows) 수를 반환. 모두 0이면 정합성 OK.

-- 1. TITLE_GENRES.tconst → TITLES
SELECT COUNT(*) AS orphaned, 'TITLE_GENRES.tconst → TITLES' AS check_name
FROM TITLE_GENRES tg
LEFT JOIN TITLES t ON tg.tconst = t.tconst
WHERE t.tconst IS NULL;

-- 2. TITLE_GENRES.genre_id → GENRES
SELECT COUNT(*) AS orphaned, 'TITLE_GENRES.genre_id → GENRES' AS check_name
FROM TITLE_GENRES tg
LEFT JOIN GENRES g ON tg.genre_id = g.id
WHERE g.id IS NULL;

-- 3. TITLE_AKAS.tconst → TITLES
SELECT COUNT(*) AS orphaned, 'TITLE_AKAS.tconst → TITLES' AS check_name
FROM TITLE_AKAS ta
LEFT JOIN TITLES t ON ta.tconst = t.tconst
WHERE t.tconst IS NULL;

-- 4. RATINGS.tconst → TITLES
SELECT COUNT(*) AS orphaned, 'RATINGS.tconst → TITLES' AS check_name
FROM RATINGS r
LEFT JOIN TITLES t ON r.tconst = t.tconst
WHERE t.tconst IS NULL;

-- 5. EPISODES.tconst → TITLES
SELECT COUNT(*) AS orphaned, 'EPISODES.tconst → TITLES' AS check_name
FROM EPISODES e
LEFT JOIN TITLES t ON e.tconst = t.tconst
WHERE t.tconst IS NULL;

-- 6. EPISODES.parent_tconst → TITLES
SELECT COUNT(*) AS orphaned, 'EPISODES.parent_tconst → TITLES' AS check_name
FROM EPISODES e
LEFT JOIN TITLES t ON e.parent_tconst = t.tconst
WHERE t.tconst IS NULL;

-- 7. TITLE_CREW.tconst → TITLES
SELECT COUNT(*) AS orphaned, 'TITLE_CREW.tconst → TITLES' AS check_name
FROM TITLE_CREW tc
LEFT JOIN TITLES t ON tc.tconst = t.tconst
WHERE t.tconst IS NULL;

-- 8. TITLE_PRINCIPALS.tconst → TITLES
SELECT COUNT(*) AS orphaned, 'TITLE_PRINCIPALS.tconst → TITLES' AS check_name
FROM TITLE_PRINCIPALS tp
LEFT JOIN TITLES t ON tp.tconst = t.tconst
WHERE t.tconst IS NULL;

-- 9. TITLE_PRINCIPALS.nconst → PERSONS
SELECT COUNT(*) AS orphaned, 'TITLE_PRINCIPALS.nconst → PERSONS' AS check_name
FROM TITLE_PRINCIPALS tp
LEFT JOIN PERSONS p ON tp.nconst = p.nconst
WHERE p.nconst IS NULL;
