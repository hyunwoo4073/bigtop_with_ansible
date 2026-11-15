# Step76 D5 — DR 복구 증빙 및 Runbook (2026-11-15)

## 목적과 적용 범위

Step76 A~D4에서 확보한 메타데이터 백업 및 격리 복원 **증빙**을 최종적으로 재검증한다. D5는 기존 `verify` 명령만 실행하므로 서비스를 재시작하거나 백업·복원 데이터를 새로 생성하지 않는다. **전체 서비스 DR을 실증한 것으로 주장하지 않는다.**

### 현재 인프라 구성 (기존 Ansible 및 Step76 실행 증빙 기준)

| 서비스 / 자산 | 저장소 | 위치 / 복구 증빙 |
|---|---|---|
| Web Console 운영 DB | PostgreSQL (별도 기존 백업 구성) | Step76의 복구 검증 범위 밖 |
| Iceberg REST Catalog | PostgreSQL `iceberg_rest56` | worker3 `/var/backups/bigtop-step76/<ID>/pg/` 원본 덤프; worker1 원격 사본 |
| OpenMetadata | Docker Compose MySQL `openmetadata_db` | metadata `/var/backups/bigtop-step76/<ID>/mysql/` 원본 덤프; worker1 원격 사본 |
| OpenMetadata 검색 | Docker Compose Elasticsearch | metadata `/var/backups/bigtop-step76-es/` Snapshot; **원격 복제 미실시** |
| Iceberg 테이블 파일 | HDFS `/data/olist/iceberg-rest/...` | Step56 `catalog_smoke` **한 개만** master → worker1 백업·격리 복원 실증 |
| Kafka Broker 및 KRaft | systemd on worker1~3 | Step76 복구 대상 아님; Kafbat UI만 Docker 컨테이너 |

> 위 폴더 경로는 기존 Step76 패키지의 고정 경로이며, D5 재검증 과정에서 실파일 접근을 통해 증빙을 재확인한다. 컨테이너 가동 상태는 실제 환경 점검 결과로 판단한다.

## 실행

```bash
cd ~/bigtop-cluster-ansible
bash scripts/run_step76_dr_final.sh
```

- 사전 조건: Step76 B/C/D2/D3/D4 완료, `artifacts/step76/latest_backup_id` 존재
- 순서: `backup verify` → `scratch restore verify` → `offhost verify` → `HDFS proof verify` → `ES snapshot restore verify`
- 파일: `artifacts/step76/dr-final-evidence-<ID>.json` 및 `.md`, 모드 `0600`
- 전체 `make validate`는 반복 실행하지 않음. Git에 올리기 전 한 번만 실행
- `STEP76_D5_LAB_EVIDENCE_PASS` = 이 5가지 검증의 재통과, **전체 플랫폼 DR 완료 아님**
- 중간 실패 시 **해당 단계의 `verify`만 재실행**하거나 로그를 진단한다. `backup`, `copy`, `restore`, `configure`를 반복하지 않는다

## 장애별 복구 순서(운영 중단 시 자동 실행 금지)

1. **손실 범위 분리:** worker3 카탈로그 DB, metadata VM 검색/DB, HDFS, worker1 사본의 생존 여부 확인. 단일 VM 장애인지 호스트/스토리지 장애인지 구분.
2. **서비스 쓰기 차단 및 정합성 기준 확정:** Kafka→Spark 적재를 무조건 재시작하지 말 것. 가능한 체크포인트·마지막 커밋 시점을 먼저 기록.
3. **카탈로그 DB:** worker1의 검증된 PostgreSQL 원격 사본을 안전한 새 PostgreSQL 환경에 복구. 운영 `iceberg_rest56` DB에 바로 덮어쓰지 말 것. Catalog 설정, 스키마, 테이블 참조를 검증.
4. **Iceberg HDFS 데이터/메타데이터:** 해당 테이블의 정확한 metadata JSON, manifest, 데이터 파일을 한 시점의 일관성 있는 집합으로 복구해야 함. Step76 D3는 **단일 정적 smoke 테이블만** 보존했고 전체 warehouse 복구본이 아님. HDFS 복제·스냅샷·스토리지 백업 정책 별도 필요.
5. **OpenMetadata MySQL:** worker1에 있는 복사본을 새 격리 DB에 복원·검증한 다음 서버 연결을 절차대로 복구. 데이터 카탈로그 참조가 일치하는지 확인.
6. **Elasticsearch:** metadata VM의 파일시스템 Snapshot 저장소와 API 복구를 전제로 하는데 **동일 VM에만 저장**됨. metadata VM이 손실되면 기존 Snapshot도 손실될 수 있음. 운영 DR에는 별도 보관 정책이 선행되어야 함.
7. **서비스 재개:** 검증 완료 후 OpenMetadata, REST Catalog, Trino 및 Spark 등 의존성을 확인해 단계별 재개. Spark는 기존 Checkpoint 보존 여부를 확인하고 중복 애플리케이션이 없는지 점검.
8. **사후 검증:** Catalog 조회·실제 Iceberg SQL 읽기·메타데이터 인덱스 검색·Kafka Lag·Spark Offset 등으로 정상화 확인.

## 미완료 / 우선순위

| 항목 | 현재 상태 | 다음 조치 |
|---|---|---|
| PG·MySQL 원격 사본 | worker1 복제 및 해시 대조 | 정기화·보존 기간·서로 다른 물리 호스트 보관 |
| HDFS 전체 Warehouse | 단일 smoke 테이블만 검증 | 일관된 시점 백업·Catalog 연동·복구 대상 확대 |
| ES Snapshot | 같은 metadata VM 내부에 보존 | 원격/오브젝트 스토리지 보호 방식 설계 후 별도 복원 실증 |
| DB·HDFS·ES 복구 시점 | 서로 다른 시점 가능 | 서비스 간 일관성/버전 대응 정책 수립 |
| RPO / RTO | **미측정** | 복구 목표 정의 후 분리 환경에서 실제 장애 복구 시간 측정 |
| Web Console DB | Step76 이외의 기존 백업 | 기존 별도 백업·복구 검증을 문서에 연결 |

현재 실습 범위에 대한 증빙 정리는 Step76으로 완료할 수 있으나, 실제 DR 보장 수준과 혼동해서는 안 된다.
