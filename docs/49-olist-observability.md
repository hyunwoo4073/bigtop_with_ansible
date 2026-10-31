# 49. Olist Batch Pipeline Observability

## 목표와 의존성

- 45: 9개 CSV의 불변 HDFS Raw v1 저장.
- 46: Spark on YARN Cluster ETL이 Curated / Quarantine / Mart Parquet 생성.
- 47: 별도의 Spark Quality 검증과 `olist_quality_*` 메트릭을 worker3 textfile collector에 게시.
- 48: `bigtop-olist-pipeline.service`에서 ETL → Quality 오케스트레이션 및 `olist_pipeline_*` 메트릭 제공.
- **49: 기존 Grafana file provider와 Prometheus datasource를 재사용하여 단일 관측 대시보드만 추가.**

## 데이터 흐름

```text
worker3 systemd timer
     -> 46 Spark YARN cluster
     -> 47 Spark quality YARN cluster
     -> node_exporter textfile: olist_pipeline.prom + olist_quality.prom
     -> Prometheus (node_exporter :9100)
     -> Grafana Olist Pipeline & Data Quality dashboard
                               -> OpenMetadata UI link (navigation only)
```

### Prometheus 지표

| Metric | 출처 | 의미 |
|---|---|---|
| `olist_pipeline_last_attempt_success` | 48 | 최신 실행 성공 여부(0/1) |
| `olist_pipeline_running` | 48 | 현재 실행 중(0/1) |
| `olist_pipeline_etl_success` | 48 | ETL 단계 성공 여부 |
| `olist_pipeline_quality_success` | 48 | 품질 단계 성공 여부 |
| `olist_pipeline_last_attempt_duration_seconds` | 48 | 최신 시도 소요 시간(gauge) |
| `olist_pipeline_last_success_timestamp_seconds` | 48 | 마지막 성공 시간 |
| `olist_quality_run_success` | 47 | 최신 품질 결과 |
| `olist_quality_checks_failed` | 47 | 실패 품질 규칙 수 |
| `olist_quality_checks_total` | 47 | 전체 품질 규칙 수 |
| `olist_quality_quarantine_rate` | 47 | 격리 비율(0~1) |
| `olist_quality_fact_rows` | 47 | 주문상품 Fact 레코드 수 |
| `olist_quality_orphan_order_items` | 47 | 참조 무결성 위반 건수 |
| `olist_quality_last_run_timestamp_seconds` | 47 | 품질 보고 시각 |

`warehouse_version`은 v1 기본값이며, 47/48과 동일하게 맞춰야 한다.
없는 메트릭을 `or vector(0)`으로 보정하지 않는다. No data는 **수집 누락**일 수 있으므로 PASS와 분리한다.

### 관리 범위 / 재실행

- 읽기 모델: `/var/lib/grafana/dashboards/olist-pipeline-overview.json`
- 소스: `roles/data_pipeline_olist_observability/templates/olist-pipeline-overview.json.j2`
- Grafana datasource UID는 기존 `grafana_datasource_uid`(기본 `prometheus`) 재사용.
- 기존 파일 provider: `/etc/grafana/provisioning/dashboards/platform.yml`(Bigtop Cluster).
- Grafana/Prometheus 설치, 재시작, 대시보드 provider/기존 대시보드 설정은 **변경하지 않음**.
- Olist Spark ETL 및 HDFS 경로는 변경하거나 실행하지 않음.
- Dashboard `OpenMetadata (catalog)` 링크는 실제 데이터 카탈로그 자동 수집이나 Lineage가 아님.
- Web Console의 기존 Grafana 바로가기에서 새 대시보드를 찾을 수 있다. **49번은 Web Console API의 바로가기 목록 자체는 변경하지 않음.**

### 운영 점검

```bash
# Grafana 자체
sudo systemctl is-active grafana-server
curl -fsS http://127.0.0.1:3000/api/health | python3 -m json.tool
sudo journalctl -u grafana-server -n 80 --no-pager

# Prometheus에서 48번 metric 조회
curl -fsSG --data-urlencode 'query=olist_pipeline_last_attempt_success{warehouse_version="v1"}' \
  http://127.0.0.1:9090/api/v1/query | python3 -m json.tool

# Dashboard JSON 상태
python3 -m json.tool /var/lib/grafana/dashboards/olist-pipeline-overview.json >/dev/null
```

실시간 데이터가 안 보이면 다음을 구분한다.

1. Grafana File Provider 문제: Dashboard 자체가 보이지 않음 → provider 경로 / 로그 확인.
2. Prometheus scrape 문제: Dashboard는 있지만 모든 패널 No data → `/metrics`, `/api/v1/query` 확인.
3. 오케스트레이션 실행 문제: quality만 있고 pipeline 지표가 없음 → `bigtop-olist-pipeline.service` 수동 실행 및 timer 상태 확인.
4. 버전 차이: 47/48의 `warehouse_version` 값이 대시보드 변수와 불일치.
5. OpenMetadata link 문제: 서버 hostname이 브라우저에서 해석되지 않음 → `-e data_pipeline_olist_observability_openmetadata_url=...`로 재배포.

### 다음 작업

50번에서 OpenMetadata에 Olist Raw/Curated/Mart 데이터 자산과 lineage를 실제로 등록하는 작업을 별도로 검토한다. Parquet/HDFS 데이터셋을 OpenMetadata가 어떻게 수집할지, OpenMetadata 2.0.6과 Spark 3.1.3의 호환성과 인증을 확인한 후 설계한다.
