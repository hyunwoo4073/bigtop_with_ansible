# Step 62 — Iceberg 운영 유지보수 / 작은 파일 압축 / Snapshot 복구 (Olist)

> **이 단계는 기존 58·59번 Iceberg 테이블을 변경하지 않는다.** 기존 Step59 Gold `main` Snapshot이 승인된 상태인지 검사한 뒤, 독립적인 실습 테이블에서만 파일 압축과 롤백을 수행한다.

## 환경 및 범위

- Ansible Controller: `worker3` (`~/bigtop-cluster-ansible`)
- Spark submit: `master`, Spark 3.5.6 / Java 11 / YARN cluster
- Spark Iceberg: `/opt/bigtop-iceberg/iceberg-spark-runtime-3.5_2.12-1.10.1.jar`
- REST Catalog: Gravitino `http://192.158.2.103:9001/iceberg/`
- Trino: `metadata:8080` (Trino 484)
- **읽기 전용 기준:** `shared.olist58_v3.*` 7개, `shared.olist59_v3.gold_daily_category_sales` 1개
- **유일한 변경 대상:** `shared.olist62_ops.maintenance_smoke`
- HDFS 증빙: `/data/olist/iceberg-validation/step62/v1/`
- `expire-lab` 이외에는 Snapshot 삭제 없음. `remove_orphan_files`는 **어떤 모드에서도 실행하지 않음**.

## 1. worker3에 설치, 로컬 테스트

```bash
cd ~/bigtop-cluster-ansible
unzip -o 62_olist_iceberg_maintenance_ansible.zip -d .
python3 -m unittest discover -s tests -p 'test_olist_iceberg_maintenance62.py' -v
make lint
```

## 2. 읽기 전용 사전 점검

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/62-olist-iceberg-maintenance.yml
```

기본값은 `olist_iceberg_maintenance62_mode=plan`. REST Catalog 응답과 Step59 HDFS Publish 증빙 존재를 점검한다. Spark Job과 HDFS 변경은 없다.

Trino로 현재 Gold 공개 상태만 검증할 수도 있다.

```bash
python3 scripts/62-verify-trino.py \
  --mode inspect --url http://192.158.2.104:8080
```

## 3. 기존 8개 테이블의 현황 조회 (Spark, 읽기 전용)

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/62-olist-iceberg-maintenance.yml \
  -e olist_iceberg_maintenance62_mode=inspect
```

읽기 전용 Spark on YARN 작업이 58번 7개 테이블과 59번 Gold 1개 테이블의 데이터 파일 수, 총 파일 바이트, Snapshot 개수를 확인한다. **YARN cluster mode이므로 `STEP62_READ_ONLY_INSPECT_PASS` 상세 출력은 YARN Driver 로그**에 남는다. Ansible `failed=0`만으로 검증하지 말고 필요하면 `yarn application -list -appStates ALL`과 `yarn logs -applicationId <id>`로 확인한다.

## 4. 전용 실습 테이블에서 Compaction + Rollback

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/62-olist-iceberg-maintenance.yml \
  -e olist_iceberg_maintenance62_mode=lab
```

실행 내용:

1. 승인된 Step59 WAP `main` Snapshot과 29개 DQ PASS 증빙 확인
2. **신규** `shared.olist62_ops.maintenance_smoke` 테이블에 결정적 테스트 데이터 2,048행 저장 (8개의 Spark 작성 Partition 사용)
3. Iceberg `rewrite_data_files`로 작은 데이터 파일 압축, **실제 파일 수가 감소했는지**와 **양방향 exceptAll 데이터 완전 일치** 확인
4. `id=-1` 잘못된 레코드를 INSERT하고 신규 Snapshot 생성 확인
5. `rollback_to_snapshot`으로 압축 직후의 승인된 실습 Snapshot으로 돌아감
6. 잘못된 행이 없어졌는지, 원본 2,048행이 정확히 동일한지 재검증
7. Step59 Gold 55,877건과 이전 `main` Snapshot이 그대로인지 확인
8. 불변 증빙 `_LAB_REPORT.json` 생성. controller에도 `artifacts/62-iceberg-maintenance-lab.json` 저장

**재실행 방침:** 이미 유효한 증빙이 있으면 읽기 전용 `verify`만 한다. 실습 테이블이 이미 있는데 정상 증빙이 없다면 **자동 덮어쓰기/삭제를 하지 않고 실패한다.** 이 경우 YARN Driver 로그와 테이블 Snapshot을 먼저 점검할 것.

## 5. read-only 재검증

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/62-olist-iceberg-maintenance.yml \
  -e olist_iceberg_maintenance62_mode=verify

python3 scripts/62-verify-trino.py \
  --mode lab --url http://192.158.2.104:8080
```

성공 메시지:

```text
STEP62_TRINO_LAB_PASS: rows=2048 files=<실제 개수>
STEP62_SPARK_TRINO_MAINTENANCE_CROSS_QUERY_PASS
```

실제 파일 수는 환경에 따라 변하며 고정 숫자 `1`을 보장하지 않는다. 압축 전보다 줄어야 성공이다.

## 6. 선택적: 실습에서 폐기된 Snapshot **하나만** 만료

실습 내용 확인 후에만 수행한다. Step59 Gold나 58번 Silver의 Snapshot은 변경하지 않는다.

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/62-olist-iceberg-maintenance.yml \
  -e olist_iceberg_maintenance62_mode=expire-lab \
  -e olist_iceberg_maintenance62_confirm_expire_sandbox=true

python3 scripts/62-verify-trino.py --mode lab \
  --url http://192.158.2.104:8080
```

`expire_snapshots(snapshot_ids => ARRAY(...))`를 사용해 **Step62 실습 중 삽입했다가 롤백한, 현재 어떠한 Branch/Tag도 가리키지 않는 특정 Snapshot ID만** 만료한다. 관련 미사용 파일은 삭제될 수 있고 해당 Snapshot으로의 Time Travel도 불가능해진다. 성공 증빙은 `_EXPIRE_REPORT.json`. 기존 `_LAB_REPORT.json`은 절대 덮어쓰지 않는다.

## 7. 결과 파일과 점검 명령

```bash
# worker3 (Ansible Controller)
cat artifacts/62-iceberg-maintenance-lab.json | python3 -m json.tool
cat artifacts/62-iceberg-maintenance-expire.json | python3 -m json.tool  # 선택 실습 후

# master
sudo -u hadoop hdfs dfs -cat \
  /data/olist/iceberg-validation/step62/v1/_LAB_REPORT.json

yarn application -list -appStates ALL | grep iceberg-maintenance62
```

## 장애 및 실패에 대한 정책

- `plan`, `inspect`, `verify`: 기존 Iceberg 테이블을 읽기만 하며 ETL/Publish는 실행하지 않는다.
- `lab`: **단독 테스트 테이블 한 개만** 생성/수정. 중간 실패 이후 자동으로 DROP TABLE 또는 DELETE 하지 않는다. 기존 실습 테이블을 비우거나 재생성하기 전에 Snapshot 상태와 HDFS 경로를 사람이 확인해야 한다.
- `expire-lab`: 특정 실패 쓰기 Snapshot 만료. 전체 데이터 보관기간 정책을 구성하지 않는다.
- 승인된 v3 Gold `main` Snapshot 또는 29개 DQ 증빙이 바뀌었다면 실행을 차단한다.
- `.files`, `.snapshots`, `.refs`는 Iceberg Metadata Table이다. Parquet 디렉터리를 직접 `rm -r` 하지 않는다.
- **본 단계에서는 자동 주기적 압축/삭제 타이머를 설치하지 않는다.** 테이블별 SLA, 파일 수 임계값, Time Travel 보존기간, 장기 실행 쓰기 작업을 고려한 정책은 이후 별도 적용한다.

## 공식 근거

- Iceberg 1.10.1 Spark Procedures: <https://iceberg.apache.org/docs/1.10.1/spark-procedures/>
- Iceberg 1.10.1 Maintenance: <https://iceberg.apache.org/docs/1.10.1/maintenance/>
- Trino 484 Iceberg Connector: <https://trino.io/docs/484/connector/iceberg.html>
