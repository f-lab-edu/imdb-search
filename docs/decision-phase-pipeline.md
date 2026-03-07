# 2단계 파이프라인 설계 (Phase 1/2)

## 문제

비동기 태스크 큐 구조에서 여러 파일이 동시에 다운로드되고 파싱되다 보니 처리 순서가 보장되지 않았다.

MySQL 스키마상 `TITLES`, `PERSONS` 테이블을 다른 테이블들이 FK로 참조하기 때문에, 이 두 테이블이 먼저 채워져 있어야 나머지 데이터를 삽입할 수 있다.

```
TITLES, PERSONS (부모)
    ↑ FK
TITLE_GENRES, TITLE_AKAS, RATINGS, EPISODES, TITLE_CREW, TITLE_PRINCIPALS (자식)
```

## 해결

태스크에 Phase를 부여하여 처리 순서를 제어한다.

- **Phase 1 (Primary)**: `TITLE_BASICS`, `NAME_BASICS` — TITLES, PERSONS 테이블 채우기
- **Phase 2 (Secondary)**: 나머지 데이터셋 — Phase 1 완료 후 처리

Consumer는 Redis 큐가 비었을 때 MySQL `TASKS` 테이블에서 현재 Phase의 작업을 가져온다. Phase 1 작업이 모두 소진되면 Phase 2로 넘어간다.

```ts
if (refilled === 0) {
  if (this.workers.size === 0) {
    if (this.currentPhase === TaskPhase.PRIMARY) {
      this.currentPhase = TaskPhase.SECONDARY;
    } else {
      break;
    }
  }
}
```

별도의 플래그 없이 TASKS 테이블의 작업 소진 여부만으로 단계를 관리한다.
