# 외부 데이터 오류 처리

## 배경

IMDb 공개 데이터셋은 외부 의존 데이터로, 중복 tconst와 고아 데이터(참조 대상이 없는 레코드)가 다수 포함되어 있었다.

---

## 문제

**1. 중복 키 에러**

동일한 tconst가 여러 번 등장하는 경우 UNIQUE 제약 위반으로 INSERT 실패.

**2. 고아 데이터**

FK 참조 대상(TITLES, PERSONS)이 없는 레코드가 존재하여 FK 제약 위반으로 INSERT 실패.

**3. 재시도 메커니즘의 한계**

태스크 큐에 재시도 로직을 구현했지만, 이는 DB 락이나 일시적 연결 오류 같은 상황에만 효과적이다. 중복 키나 FK 위반은 재시도해도 동일하게 실패한다.

---

## 해결

일시적 오류(DB 락, 연결 오류 등)는 재시도 후 maxRetry 초과 시 TASKS 테이블에 `failed` 상태로 기록한다.

```ts
if (task.retryCount < this.maxRetry) {
  task.retryCount++;
  await this.redis.rPush(this.mainQueue, JSON.stringify(task));
} else {
  await this.mysqlCmd.markTaskFailed(task.taskId);
}
```

---

## 결론

외부 데이터 품질 문제는 완벽히 제어할 수 없다. 복구 불가능한 오류는 재시도 후 로깅하고 넘어가는 것이 현실적이다. `failed` 상태로 기록해두면 나중에 실패한 작업만 모아 재처리하거나 오류 패턴을 분석하는 데 활용할 수 있다.
