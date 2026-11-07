# Step 62 — Iceberg Operations Architecture

## 운영 방식

```text
[Read only]
Spark/YARN ── REST ── Gravitino ── PostgreSQL (catalog references)
   │                    │
   ├─ scan: 58 Silver/Gold + 59 published Gold
   ├─ compare 59 Gold main against immutable WAP approval
   │                    │
   ▼                    ▼
HDFS Iceberg metadata / manifest / Parquet

[Step62 isolated exercise: ONLY shared.olist62_ops.maintenance_smoke]
Seed 2,048 rows → multiple small files → rewrite_data_files
   → full equality check → append wrong row → new snapshot
   → rollback_to_snapshot → full equality check → HDFS immutable proof
   → optional expire-lab (unreferenced wrong-write snapshot only)

[Independent check]
Trino 484 → Gravitino REST → same Iceberg table on HDFS
```

## 메타데이터 변경과 파일 삭제는 구분

- `rewrite_data_files`는 데이터 파일을 새로운 파일로 다시 작성하고 Iceberg Snapshot을 생성할 수 있다. 기존 스냅샷에서 참조하는 파일을 당장 삭제하지 않는 것이 중요하다.
- `rollback_to_snapshot`은 현재 테이블의 `main` 포인터를 이전 유효 Snapshot으로 되돌린다. 원복 후 파일 및 Snapshot의 GC는 별도 단계다.
- `expire_snapshots`는 오래되거나 폐기된 Snapshot 참조와 더 이상 유효한 Snapshot에서 필요하지 않은 파일을 정리할 수 있는 **파괴적 유지보수 작업**이다. 이 실습은 실습 중 생성한 폐기 Snapshot 하나만 대상으로 한다.
- `remove_orphan_files`는 **실행하지 않는다**. 특히 HDFS 경로 정규화/NameNode 호스트명 변경으로 경로 문자열 불일치가 발생하면 예기치 않은 삭제 위험이 있기 때문이다.

## 대조 및 실패 모델

| 문제 | 검증 | 동작 |
|---|---|---|
| 공개 Gold Snapshot 변경 | `59` WAP 증빙 SHA-256 + `.refs` | 사전 중단 |
| 품질 증빙 FAIL | 29개 검사 | 사전 중단 |
| 작은 파일 정리 실패 | `before_files > after_files > 0` | 단계 실패, 실습 테이블 유지 |
| 정리 중 데이터 변경 | `exceptAll` 양방향 | 단계 실패, 증빙 생성 금지 |
| 잘못된 쓰기 | `id=-1` append | Snapshot Rollback 시뮬레이션 |
| 원복 실패 | main ref와 원본 2,048행 완전 일치 | 단계 실패, 조사 필요 |
| 이미 실습 테이블 존재/증빙 없음 | 소유권 불명 위험 | 삭제·덮어쓰기 없이 중단 |
| 일부 실패 후 재시도 | 불변 증빙 부재 | 자동 복구 시도하지 않고 중단 |
| Trino가 다른 Snapshot 조회 | `$refs` metadata table + proof | 실패 |

## 의도적으로 범위 밖에 둔 것

- 58/59 테이블 대상으로 실제 데이터 파일 압축, Expire, Delete Orphan, 주기적 정책 적용
- 7개 테이블 원자적 릴리스
- 장기간 Time Travel 정책 및 Backup/Restore 보장
- 멀티라이터 동시 커밋 경합 / 고가용성 Failover 실험
- 서버 재부팅 및 서비스 강제 종료를 포함한 장애 주입

이런 변경은 추후 단계에서 별도의 유지보수 윈도우, 영향 검토, rollback 계획과 정책을 수립한 뒤 수행한다.
