# Step 63 — Iceberg CDC 증분 MERGE / 멱등 처리 아키텍처

## 목적과 한계

- **목적:** 58번 Olist Silver Fact의 512개 실제 레코드를 기반으로, Iceberg의 `MERGE INTO`, 이벤트 순서 관리, 중복/재전송 멱등성, 삭제 tombstone을 검증한다.
- **중요:** Olist에는 원래 CDC 변경 이벤트가 포함되지 않는다. 이번 단계는 **실제 Olist CDC 수집이 아닌 결정적 합성 이벤트 실습**이다. PostgreSQL Debezium/Kafka Connect, 정기 입력, 신규 v4 릴리스, WAP 공개는 이 단계의 범위가 아니다.
- 기존 `shared.olist58_v3.*` 7개 테이블과 `shared.olist59_v3.gold_daily_category_sales`는 **읽기 전용**이다. 59번 Gold `main` Snapshot/건수와 58번 Fact `main` Snapshot이 바뀌면 실패한다.
- 변경 가능한 유일한 테이블: `shared.olist63_cdc.fact_change_lab`.
- HDFS 증빙: `/data/olist/iceberg-validation/step63/v1/`.

## 구성

```text
58 Silver Fact (READ ONLY)          59 Published Gold (READ ONLY)
         |                                  |
         | 512개 실제 레코드                 | 승인 Snapshot 보호
         v                                  v
       Spark 3.5.6 on YARN + Iceberg 1.10.1 REST (Gravitino)
         |
         +-- seed: 512개 레코드를 별도 Iceberg 테이블로 복사
         |        \-- HDFS 불변 _SEED_REPORT.json
         |
         +-- apply: 합성 CDC 48개 생성 (키 중복·역순 이벤트)
         |        \-- 동일 키·시퀀스 상충 이벤트 거부
         |        \-- 최신 이벤트 16개 선별
         |        \-- UPDATE 8 + soft DELETE 4 + INSERT 4
         |        \-- Replay/Late event 재실행 → 0개 반영
         |        \-- HDFS 불변 _APPLY_REPORT.json
         |
         +-- verify: 원본 샘플에서 독립 계산한 기대행과 정확한 전체 다이제스트 일치
         |
         +-- Trino 484: 별도 SQL로 516행 전체를 조회하고 동일 다이제스트 검증
```

## 데이터와 처리 정책

| 기준 | 값 |
|---|---|
| 최초 Seed | 512개 Olist Fact 행 (order_id, order_item_id 순서) |
| 합성 변경 이벤트 | 48개 (16키 × 최신 2회 + 오래된 1회) |
| 고유 키 기준 결과 | UPDATE 8, tombstone DELETE 4, INSERT 4 |
| 최종 전체 테이블 | 516행 (512 live, 4 tombstones) |
| 삭제 표현 | `is_deleted=true`를 유지, `last_event_seq=200` 보존 |
| 중복/지연 이벤트 | 최신 이벤트 `event_seq`보다 작거나 같은 경우 Skip |
| 데이터 정합성 | 7개 컬럼 전체 정렬·정규화·SHA-256, Spark와 Trino 각각 검증 |

현재 처리 방식은 **Soft Delete**다. CDC 이벤트가 지연 도착하더라도 시퀀스를 보존해 과거 `INSERT/UPDATE`가 삭제 레코드를 되살리는 문제를 막는다. 실제 서비스에서 영구 삭제가 필요한 경우 별도 보관기간과 tombstone compaction 정책을 설계해야 한다.

Iceberg는 키 자체를 Primary Key 제약으로 자동 강제하지 않는다. 이 단계는 실행 전후 **중복 키 검사** 및 이벤트 상충 검사로 보완한다. 또한 `event_seq`는 **키별 비교 가능한 단조 증가 값**으로 가정하므로, 실제 CDC의 Source LSN/SCN, Transaction ID 및 다중 파티션 순서 정합성 설계는 별도 작업이 필요하다.

## 실패/재실행 원칙

- `plan`은 HDFS 승인 증빙과 REST API만 확인한다. 새로운 Snapshot 생성 없음.
- `seed`: 이미 유효한 Seed 증빙이 있다면 Snapshot과 내용 검증 후 Skip. 테이블은 있지만 Seed 증빙이 없으면 자동 재생성하지 않는다.
- `apply`: Seed 증빙과 현재 Snapshot의 일치를 선행 확인. 이미 Apply 증빙이 있으면 읽기 검증만 수행. **실제 MERGE 이후 증빙 작성 전에 장애가 나면 기존 테이블과 Seed Snapshot이 달라지므로 재실행을 차단하고 수동 점검**한다.
- `verify`: 변경 없이 증빙, Source/Gold 참조 Snapshot, 합성 이벤트 기대값과 실제 레코드를 재계산해 비교한다.
- DataFrame/PySpark 예외는 YARN Cluster 모드의 **Driver 로그**에서 확인한다. Ansible `failed=0`은 최종 판정이 아니다.

## 실제 데이터 플랫폼으로 확장하기 위한 후속 과제

1. Kafka Connect/Debezium 또는 변경 파일로부터 **실제 Change Events**를 수집한다.
2. 재처리 범위와 이벤트 Offset/LSN을 Iceberg 데이터 Commit과 연계한다.
3. CDC 도착 순서와 충돌 정책을 키별로 정의한다.
4. Silver 테이블의 증가분을 품질검사 후 Gold 재계산 및 WAP 릴리스로 전파한다.
5. 멀티테이블 릴리스의 정합된 버전 선택과 독립적인 보존 정책을 설계한다.

## 참고

- Apache Iceberg 1.10.1, Spark Writes / MERGE INTO: https://iceberg.apache.org/docs/1.10.1/spark-writes/
- Iceberg Spark Branch/WAP: https://iceberg.apache.org/docs/1.10.1/branching/
- Trino 484 Iceberg: https://trino.io/docs/484/connector/iceberg.html
