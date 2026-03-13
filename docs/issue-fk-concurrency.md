# FK 제약 조건과 병렬 처리 동시성 이슈

## 배경

- IMDb 공개 데이터셋 활용 (title.basics: 1,229만 행, name.basics: 1,500만 행)
- 단일 파일에서 여러 테이블로 데이터 분산 필요
- 배치 단위 병렬 처리로 성능 최적화 목표

---

## 문제 1: FK 제약 조건과 단일 파일 다중 테이블 처리

### 문제 상황

**초기 접근:**

- title.basics 파일 → GENRES, TITLES, TITLE_GENRES 세 테이블로 분산
- FK 제약 조건 제거하고 애플리케이션 레벨에서 무결성 관리
- 간단하지만 장기적으로 문제 발생 가능
  - 데이터 삭제/수정 시 무결성 보장 어려움
  - 테이블 조인 시 정합성 문제 발생 가능

**핵심 과제:**

- FK 제약 유지하면서 파일 한 번 읽기로 여러 테이블 처리
- 1,200만 행 파일을 여러 번 읽는 것은 비효율적

### 해결 방안

1. **배치 방식** — 데이터를 배치 단위로 쌓아서 순서대로 입력, 메모리 효율적
2. **MQ 활용** — 작업 단위로 분리하여 처리

### 최종 해결

**배치 방식 선택 및 구현:**

```ts
async insertTitleBasics(data: TitleBasics[]) {
  // 1. GENRES 먼저 처리
  await this.insertGenres(data);

  // 2. TITLES 처리
  const titles = data.map(({ genres, ...rest }) => rest);
  await this.bulkInsert("TITLES", titles);

  // 3. TITLE_GENRES 처리 (FK 참조 완료 후)
  const titleGenres = data.flatMap(row =>
    row.genres.split(",").map(genreName => ({
      tconst: row.tconst,
      genre_id: this.genresMap.get(genreName)
    }))
  );
  await this.bulkInsert("TITLE_GENRES", titleGenres);
}
```

- 배치 단위(1,000행)로 읽기
- 배치 내에서 FK 순서 보장: GENRES → TITLES → TITLE_GENRES
- GENRES 해시맵 캐싱으로 중복 조회 방지

---

## 문제 2: 병렬 처리 시 동시성 이슈

### 문제 상황

**발생 배경:**

- 작업 큐 구현을 위한 병렬 처리 테스트 중 발견
- 여러 배치(15 concurrent)를 동시에 처리할 때 문제 발생

**구체적 문제:**

1. **GENRES 테이블 데드락**

```
Error: Deadlock found when trying to get lock
```

- 여러 워커가 동시에 같은 장르 INSERT 시도
- UNIQUE 제약(name) + 동시 INSERT → 데드락 발생

2. **TITLE_GENRES 중복 입력**

```
Error: Duplicate entry 'tt0000147-7' for key 'TITLE_GENRES.unique_title_genre'
```

- UNIQUE 제약(tconst, genre_id) + 동시 INSERT → 중복 에러

### 시도한 해결 방법들

**1. LOCK TABLES (실패)**

```ts
await conn.query("LOCK TABLES GENRES WRITE");
// ... INSERT
await conn.query("UNLOCK TABLES");
```

- 문제: 전체 시스템 블로킹, 다른 쿼리까지 멈춤

**2. 재시도 로직 (부적절)**

```ts
catch (err) {
  if (err.code === 'ER_LOCK_DEADLOCK' && retries > 0) {
    return this.insertGenres(data, retries - 1);
  }
}
```

- 문제: 설계상 문제를 임시방편으로 해결

### 최종 해결

**1. GENRES 직렬화 (뮤텍스)**

```ts
private async insertGenres(data: TitleBasics[]): Promise<void> {
  this.genreLock = this.genreLock
    .catch(() => {})
    .then(async () => {
      const conn = await this.pool.getConnection();

      try {
        const newGenres = [
          ...new Set(data.flatMap((row) => row.genres?.split(",") ?? [])),
        ].filter((name) => name && !this.genresMap?.has(name));

        if (newGenres.length > 0) {
          await conn.query("INSERT IGNORE INTO GENRES(name) VALUES ?", [
            newGenres.map((n) => [n]),
          ]);

          const [rows] = await conn.query<mysql.RowDataPacket[]>(
            "SELECT id, name FROM GENRES WHERE name IN (?)",
            [newGenres],
          );

          rows.forEach((row: mysql.RowDataPacket) => {
            this.genresMap?.set(row.name, row.id);
          });
        }
      } catch (err) {
        console.error(`insert genres error: ${(err as Error).message}`);
        throw err;
      } finally {
        conn.release();
      }
    });

  return this.genreLock;
}
```

- GENRES insert만 직렬화, 나머지 테이블은 병렬 처리 유지
- 장르는 개수가 많지 않아 성능 영향 미미

**2. INSERT IGNORE 적용**

```ts
await conn.query(`INSERT IGNORE INTO ${tableName} (${keys.join(",")}) VALUES ?`, [values]);
```

- 중복 데이터 자동 무시
- 데이터 특성상 존재 여부가 정합성보다 중요

**3. 명시적 커넥션 관리**

```ts
// Before
await this.pool.query(...);

// After
const conn = await this.pool.getConnection();
try {
  await conn.query(...);
} finally {
  conn.release();  // 반드시 반환
}
```

- try-finally 패턴으로 에러 상황에서도 release 보장
- 커넥션 풀 누수 완전 방지

**4. 트랜잭션 적용**

```ts
async bulkInsert<T>(tableName: TableName, data: T[]) {
  const conn = await this.pool.getConnection();
  try {
    await conn.beginTransaction();
    await conn.query(`INSERT INTO ${tableName} ...`);
    await conn.commit();
  } catch (err) {
    await conn.rollback();
    throw err;
  } finally {
    conn.release();
  }
}
```

- 배치 단위 원자성 보장
- 부분 입력 방지

---

## 성과

### 성능

- **title.basics**: 1,229만 행, 22분 (병렬 15)
- **name.basics**: 1,500만 행, 40분 (병렬 15)
- **배치 사이즈**: 1,000행
- **동시 처리**: 15 concurrent batches

### 안정성

- FK 제약 유지하면서 효율적 처리
- 데드락 완전 방지
- 커넥션 풀 안정적 관리
- 중복 입력 방지
- 배치 단위 원자성 보장

### 학습 포인트

1. **DB 설계**: FK 제약의 중요성과 트레이드오프
2. **동시성 제어**: 뮤텍스를 통한 크리티컬 섹션 보호
3. **리소스 관리**: 커넥션 풀의 명시적 관리 필요성
4. **성능 최적화**: 병렬 처리와 안정성의 균형
