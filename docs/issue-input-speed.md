# 데이터 입력 속도 및 메모리 이슈

## 배경

- 7개 데이터셋 파일, 최대 1,500만 행
- Redis 기반 producer-consumer 태스크 큐 구조

---

## 문제 상황

**초기 구조:**

- 파일 1개 = 태스크 1개 → 워커가 처리
- 결과: 파일 하나당 스레드 하나 할당 → 병렬 처리 불가, 처리 속도 느림

---

## 시도한 해결 방법들

**1. 배치 단위 Redis 큐 적재 (실패)**

- 파일을 배치 단위로 읽어 Redis 큐에 적재 → consumer가 소비
- 문제: producer가 consumer보다 훨씬 빠르게 작업을 생산 → Redis 메모리 초과로 OOM

**2. 백프레셔 적용 (부분 해결)**

- 큐 길이 임계값 초과 시 producer 대기
- 문제: producer/consumer 속도 차이가 커서 완화는 됐지만 근본 해결 안 됨

---

## 최종 해결

**MySQL TASKS 테이블을 영구 저장소로 활용:**

- Producer → MySQL `TASKS` 테이블에 배치 단위 태스크 적재
- Consumer → Redis 큐가 비면 MySQL에서 작업을 가져와 큐를 채운 뒤 처리

**효과:**

- Redis 메모리 초과 문제 완전 해결
- 프로세스 중단 시 MySQL에 상태 보존 → 재시작 후 이어서 처리 가능
- 실패 태스크 `failed` 상태로 기록

**트레이드오프:**

- MySQL I/O가 추가되어 전체 처리 속도는 다소 느려짐

**Redis를 남겨둔 이유:**

- 기존 구조와의 호환성
- 향후 consumer를 멀티 프로세스/스레드로 확장할 때 외부 큐가 필요 — 인메모리 큐보다 Redis가 적합

---

## 추가 최적화 적용

FK 제약 오버헤드, JS 파싱 오버헤드를 `LOAD DATA LOCAL INFILE` + staging 테이블 방식으로 해결했다.

→ [`decision-insertion-optimization.md`](decision-insertion-optimization.md) 참고
