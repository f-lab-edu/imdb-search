# Insertion Performance Benchmark

## Environment
- MySQL 8, Redis, Node.js (tsx)
- 10 workers

## Before (JS parse → INSERT_DATA task queue)
- 총 소요시간: 약 4~5시간
- 방식: TSV 라인별 JS 파싱 → Redis 태스크 큐 → bulk INSERT (배치)
- 병목: FK/인덱스 유지 비용, genreLock 직렬화, JS 파싱 오버헤드

## After (LOAD DATA LOCAL INFILE → staging)
### Load 단계 (7개 파일 병렬)
| 파일 | 소요시간 |
|------|---------|
| name.basics.tsv | 5,694ms |
| title.basics.tsv | 9,110ms |
| title.akas.tsv | 10,890ms |
| title.principals.tsv | 19,954ms |
| title.ratings.tsv | 20,003ms |
| title.crew.tsv | 20,993ms |
| title.episode.tsv | 27,279ms |
| **전체 (병렬)** | **~27s** |

### Normalize 단계 (FK OFF + plain INSERT)
| 단계 | 소요시간 |
|------|---------|
| primary (genres, titles, persons → title_genres) | 17m 17s |
| secondary (title_akas, ratings, episodes, title_crew, title_principals 병렬) | 1h 39m 36s |
| **전체** | **~1h 57m** |

- primary: genres/titles/persons 병렬 → titleGenres 순차
- secondary: 5개 테이블 병렬 (title_principals 85M행이 병목)

### 전체 소요시간 비교
| 방식 | Load | Normalize | 합계 |
|------|------|-----------|------|
| 기존 (JS parse → bulk INSERT) | - | - | ~4~5시간 |
| LOAD INFILE + FK OFF | ~27s | ~1h 57m | ~2시간 |

## TODO
- INSERT IGNORE 버전 소요시간 및 데이터 누락 수 측정
- 인덱스 DROP 후 INSERT → 재생성 버전 속도 측정
- FK OFF 버전 고아 데이터 수 측정
