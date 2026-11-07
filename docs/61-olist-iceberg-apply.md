# Step 61 적용 순서 — worker3

**Step 60 완료가 선행돼야 한다.** 이 ZIP은 Playbook, Role, 실행·검증 스크립트,
테스트, 운영 문서만 추가한다. 프로젝트 `README.md`와 `docs/PROJECT_GUIDE.md`는
일일 작업 마무리 때 전체 작업과 함께 갱신한다.

## 1. 파일 적용 및 검증

```bash
cd ~/bigtop-cluster-ansible
unzip -o 61_olist_iceberg_observability_ansible.zip -d .
python3 -m unittest discover -s tests -p 'test_olist_iceberg_observability61.py' -v
make lint
```

## 2. 사전 Read-only 계획 확인

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/61-olist-iceberg-observability.yml
```

기본 실행은 읽기 전용이다. 아래를 확인한다.

- Step58 7테이블 정합성 증빙
- Step59 Stage DQ 29개 전부 PASS 및 Publish Snapshot SHA-256
- 가장 최근 Step60 실행이 PASS
- REST Catalog상 8개 테이블 존재 / HDFS 저장 경로 / 실제 Snapshot
- Trino상 8개 행 수가 증빙과 일치
- OpenMetadata 기존 Step50 `bigtop_hdfs_olist` 서비스와 v3 Container 7개 존재
- 신규 Container / Edge 생성 계획 (아직 실제 등록 안 함)

### 기존 v3 Parquet Container가 없다면

Step 50을 v3 버전으로 **먼저** 실행해 기존 HDFS 자산을 등록한다.

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/50-olist-openmetadata.yml \
  -e data_pipeline_olist_catalog_warehouse_version=v3 \
  -e data_pipeline_olist_catalog_apply=true
```

다시 Step61 Plan을 실행한다. 기존 Step50의 13개 Lineage, Step51 이벤트 등은 변경하지 않는다.

## 3. 독립적인 관측 구성 설치

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/61-olist-iceberg-observability.yml \
  -e olist_iceberg_observability61_apply=true
```

설치 대상: `/usr/local/lib/bigtop-olist/observability61/observe.py`,
Grafana 대시보드 JSON, systemd service/timer 유닛만. **설치 시 서비스 실행·Iceberg 쓰기·OpenMetadata 등록은 하지 않는다.**

## 4. OpenMetadata 등록 (명시적 1회)

```bash
cd ~/bigtop-cluster-ansible
sudo /usr/bin/python3 scripts/61-iceberg-observability.py --mode apply
```

- REST/Trino/증빙 일치 여부를 선검증
- 기존 v3 Parquet 7개 Container가 없으면 실패
- 신규 Iceberg Container 8개만 추가
- 검증된 코드 경로에 해당하는 Lineage Edge 8개만 추가
- 반복 실행 시 이미 존재하는 동일 자산/Edge는 재생성하지 않음
- 정상 등록되면 `.prom` 파일을 원자적으로 저장

**생성·삭제·갱신되는 Iceberg 데이터 테이블은 없다.**

## 5. 읽기 전용 운영 검증

```bash
sudo systemctl start bigtop-olist-iceberg-observability61.service
sudo systemctl status bigtop-olist-iceberg-observability61.service --no-pager -l
sudo journalctl -u bigtop-olist-iceberg-observability61.service -n 100 --no-pager
```

기대 성공 마커:

```text
STEP61_ICEBERG_TRINO_PASS: tables=8 ...
STEP61_OPENMETADATA_PASS: assets=8 edges=8 ...
STEP61_METRICS_PASS: .../olist_iceberg61.prom
STEP61_VERIFY_PASS: REST+Trino+OM+Prometheus textfile
```

`Type=oneshot` 서비스이므로 종료 후 `inactive (dead)`인 것은 정상이다.

## 6. Grafana / Prometheus 검증

```bash
cat /var/lib/prometheus/node-exporter/olist_iceberg61.prom
curl -fsS 'http://127.0.0.1:9090/api/v1/query?query=olist_iceberg61_observation_success' \
  | python3 -m json.tool
```

Prometheus 응답에서 `data.result`에 `{source_version="v3"}` 시계열이 있어야 한다.
비어 있다면 node_exporter textfile collector 경로, Node Exporter 대상, Prometheus Scrape를 확인한다.

Grafana에서 `Olist Iceberg Release & Quality (Step 61)` 대시보드를 찾는다.
기존 file provisioning은 새 JSON 파일을 자동 감지하므로 재시작은 불필요하다.
기존 Step49 대시보드는 그대로 유지한다.

## 7. 수동 검증이 안정화된 후 선택적 정기 확인

매시간 **읽기 전용 검증**만 반복하도록 선택적으로 활성화할 수 있다.

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/61-olist-iceberg-observability.yml \
  -e olist_iceberg_observability61_apply=true \
  -e olist_iceberg_observability61_timer_enabled=true

systemctl list-timers --all | grep iceberg-observability61
```

타이머는 58·59·60 파이프라인을 재실행하지 않고 **관측만** 수행한다.
정상 수동 검증 전에는 타이머를 활성화하지 않는다. 기본값은 false다.

## 실패 처리

- `STEP61_FAIL ... Latest Step60 receipt` → Step60 `--mode verify` 재확인
- `STEP61_FAIL ... source snapshot drift` → Step59 Stage 원천 Snapshot 증빙과 실제 Iceberg 상태 비교
- `STEP61_FAIL ... Gold published main` → Gold WAP 공개 상태 검증; 기존 Snapshot/파일 삭제 금지
- `STEP61_FAIL ... Step50 CustomStorage` → Step50 v3 자산 등록 확인
- `STEP61_FAIL ... OpenMetadata ... HTTP 401/403` → 기존 Bot token 권한 및 만료 점검
- `STEP61_FAIL ... Trino query` → Trino JDBC/REST Catalog/HDFS 접근 로그 확인
- 메트릭 파일은 존재하지만 Grafana에 없음 → node_exporter textfile collector와 Prometheus Scrape 확인

### Git 정리

매일 작업 종료 시 `README.md`/`docs/PROJECT_GUIDE.md` 최신화 후,
**그날의 코드·테스트·설정·운영문서를 모두 하나의 커밋**으로 올린다.
`vendor/`, 토큰·암호, 상태 파일/로그는 Stage에 추가하지 않는다.
