# Step 63 적용 — Olist 기반 Iceberg CDC 증분 MERGE와 멱등성

> **62번 완료 이후 단계.** 기존 58번 Silver, 59번 Publish Gold, 60번 릴리스 시스템은 변경하지 않는다. 별도 `shared.olist63_cdc.fact_change_lab` 테이블만 수정한다. 실제 운영 CDC 데이터가 아닌 **합성 변경 이벤트**를 사용한다.

## 1. worker3 — ZIP 적용 및 테스트

```bash
cd ~/bigtop-cluster-ansible
unzip -o 63_olist_iceberg_incremental_ansible.zip -d .

python3 -m unittest discover \
  -s tests -p 'test_olist_iceberg_incremental63.py' -v
make lint
```

## 2. Plan (읽기 전용)

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/63-olist-iceberg-incremental.yml
```

57번 Trino, 56번 Gravitino REST, 58번 Fact와 59번 Gold가 정상인 환경을 전제로 한다. 파일이 빠졌거나 Catalog가 응답하지 않으면 fail-closed.

## 3. Seed — 512개 실제 Olist Fact로 실습 테이블 생성

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/63-olist-iceberg-incremental.yml \
  -e olist_iceberg_incremental63_mode=seed
```

- 대상: `shared.olist63_cdc.fact_change_lab`
- Seed 증빙: `/data/olist/iceberg-validation/step63/v1/_SEED_REPORT.json`
- worker3 복사본: `artifacts/63-iceberg-cdc-seed.json`
- 재실행 시 해당 테이블과 Seed Snapshot이 그대로라면 읽기 검증 후 Skip.

## 4. Apply — UPDATE/DELETE/INSERT 재현과 중복·지연 이벤트 검증

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/63-olist-iceberg-incremental.yml \
  -e olist_iceberg_incremental63_mode=apply
```

- 합성 이벤트 48개를 만들고 동일 키의 최신 이벤트 16개만 적용.
- UPDATE 8개, **tombstone DELETE 4개**, INSERT 4개.
- 동일 CDC 배치 Replay 및 `event_seq=150` 지연 이벤트를 다시 실행하되, 0개 반영 확인.
- `is_deleted=true`는 실제 SQL `DELETE FROM`과 다르다. 물리적 데이터 파일 제거가 아니라 삭제 상태와 시퀀스를 유지하는 방식.
- Apply 증빙: `/data/olist/iceberg-validation/step63/v1/_APPLY_REPORT.json`
- worker3 복사본: `artifacts/63-iceberg-cdc-apply.json`

## 5. Verify — Spark/HDFS 증빙 읽기만 수행

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/63-olist-iceberg-incremental.yml \
  -e olist_iceberg_incremental63_mode=verify
```

이어서 worker3에서 **Trino 전체 데이터 교차 검증**:

```bash
python3 scripts/63-verify-trino.py \
  --url http://192.158.2.104:8080 \
  --seed-proof artifacts/63-iceberg-cdc-seed.json \
  --apply-proof artifacts/63-iceberg-cdc-apply.json
```

기대되는 최종 출력:

```text
STEP63_TRINO_CDC_ROWS_PASS: total=516 live=512 tombstones=4
STEP63_SPARK_TRINO_INCREMENTAL_CROSS_QUERY_PASS
```

**완료 조건**: Spark 검증 PASS + HDFS Seed/Apply 증빙 PASS + Trino 전체 행 다이제스트 PASS + 58번 Source Snapshot 및 59번 Gold Main Snapshot 불변성.

## 6. YARN 및 증빙 확인

```bash
# master
sudo -u hadoop hdfs dfs -cat \
  /data/olist/iceberg-validation/step63/v1/_APPLY_REPORT.json

yarn application -list -appStates ALL | grep olist-iceberg-cdc63
# YARN cluster 모드 실패 시
# yarn logs -applicationId application_<ID> | grep -E 'STEP63_|Traceback|Caused by:|Exception'

# worker3
cat artifacts/63-iceberg-cdc-apply.json | python3 -m json.tool
```

## 7. 장애 대응 / 주의

- **기존 58/59 테이블 DROP, OVERWRITE, MERGE 금지.** 본 코드의 유일한 MERGE 대상은 63번 신규 테이블.
- Seed/Apply 중간 실패 이후 **증빙이 없고 테이블만 있는 상태**에서는 자동 초기화하지 않는다. YARN Driver 로그, `${table}.refs`, HDFS 증빙 경로로 상태를 먼저 확인한다.
- 테이블을 정리할 경우 실습 중 생성한 Snapshot와 HDFS 파일 삭제 영향까지 확인하고 별도 의사결정할 것.
- CDC 이벤트는 Olist 원본에서 실제 도착한 것이 아니며, 정기 실시간/증분 배치 스케줄러도 설치되지 않는다.
- Git 커밋은 당일 작업을 마무리할 때 README/PROJECT_GUIDE 갱신과 **함께 전체 작업 일괄**로 진행한다. 이 단계에서는 별도 문서 전용 커밋을 만들 필요 없음.
