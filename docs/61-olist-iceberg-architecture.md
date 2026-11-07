# Step 61 — Olist Iceberg 운영 가시성 및 증빙 기반 계보

## 목적과 완료 기준

기존 **Step 58/59/60에서 실제 검증하고 공개한 불변 Olist v3**를 대상으로
OpenMetadata 등록 및 Grafana 모니터링을 연결한다. Spark, Trino, Gravitino, HDFS,
PostgreSQL 또는 데이터 파이프라인 자체는 수정하지 않는다.

```text
Olist HDFS v3 Parquet (Step46/47/52) ─────┐
                                         │ Step58 verified materialization
                                         ▼
                                Iceberg olist58_v3 (7)
                                         │ Gold source only
                                         ▼
                         Iceberg olist59_v3 Gold main (1)
                           29 DQ checks + WAP + Step60
                                         │
                           ┌─────────────┴────────────────┐
                           ▼                              ▼
                OpenMetadata 2.0.6                  Node Exporter textfile
              8 manual Containers / 8 edges           .prom → Prometheus
                                                          ▼
                                                        Grafana
```

## 구성요소와 배치

- Ansible 컨트롤러: `worker3` (`192.158.2.103`)
- OpenMetadata API: `metadata:8585/api` (`192.158.2.104:8585`), 기존 Bot token 재사용
- Iceberg REST Catalog: Gravitino `worker3:9001/iceberg`, **읽기 전용**
- Trino 484: `metadata:8080`, **조회 전용**
- Node Exporter 텍스트 파일: `/var/lib/prometheus/node-exporter/olist_iceberg61.prom`
- Grafana file provisioner: `/var/lib/grafana/dashboards/olist-iceberg-release61.json`
- systemd One-shot: `bigtop-olist-iceberg-observability61.service`
- 선택적 hourly timer: `bigtop-olist-iceberg-observability61.timer` (기본 **비활성**)

## OpenMetadata 범위

기존 Step 50 `CustomStorage` 서비스 `bigtop_hdfs_olist`에 아래 **8개 Container**를 수동 등록한다.

| 출처 | 신규 등록 | 신규 계보 |
| --- | --- | --- |
| `curated_v3_orders` | `iceberg_58_v3_silver_orders` | Parquet → Iceberg |
| `curated_v3_order_items` | `iceberg_58_v3_silver_order_items` | Parquet → Iceberg |
| `curated_v3_customers` | `iceberg_58_v3_silver_customers` | Parquet → Iceberg |
| `curated_v3_products` | `iceberg_58_v3_silver_products` | Parquet → Iceberg |
| `curated_v3_order_item_fact` | `iceberg_58_v3_silver_order_item_fact` | Parquet → Iceberg |
| `quarantine_v3_rows` | `iceberg_58_v3_quality_quarantine_rows` | Parquet → Iceberg |
| `mart_v3_daily_category_sales` | `iceberg_58_v3_gold_daily_category_sales` | Parquet → Iceberg |
| `iceberg_58_v3_gold_daily_category_sales` | `iceberg_59_v3_gold_daily_category_sales` | Gold → published Gold |

**계보의 근거는 Step58의 전체 레코드 동등성 검증과 Step59의 Snapshot·DQ·Publish 증빙이다.**
이는 실제 SQL 작업을 자동 탐지하는 Spark/OpenLineage Listener가 아니며 컬럼 레벨 Lineage도 아니다.
OpenMetadata에는 *Iceberg의 네이티브 Table 엔티티*가 아니라, 실제 HDFS 테이블 경로를 가리키는
`CustomStorage Container`로 나타난다. 따라서 네이티브 Iceberg 메타데이터 자동 수집이라고 주장하지 않는다.

## 스냅샷·증빙 신뢰성

1. `artifacts/58-iceberg-v3-proof.json`: 7개 테이블/행 수 및 exact multiset parity
2. `artifacts/59-olist-wap-stage.json`: 29개 DQ PASS, 후보 Gold Audit Snapshot, 원천 Snapshot 7개
3. `artifacts/59-olist-wap-publish.json`: Stage 보고서 SHA-256, 승인 Snapshot, Publish 행 수
4. `artifacts/60-iceberg-release-last-attempt.json`: 최신 Step60 실행 PASS
5. Gravitino REST: 실제 8개 테이블 HDFS 경로 및 58/59 Snapshot ID
6. Trino SQL: 실제 8개 테이블 row count
7. OpenMetadata: 기존 7개 Parquet Container, 신규 8개 Container 및 8개 Edge

검증이 하나라도 실패하면 **API 쓰기를 거부하고**, 메트릭 모드라면 `olist_iceberg61_observation_success=0`으로 갱신한다.
실패 시 이전 성공을 의미하는 Gold 행 수 / DQ 수 / 계보 수 게이지는 제거한다.

## Prometheus 메트릭

모든 메트릭 라벨: `{source_version="v3"}`

| 메트릭 | 의미 |
| --- | --- |
| `olist_iceberg61_observation_success` | 전체 REST/Trino/OM 증빙이 최신 검증 시점에 통과 (1/0) |
| `olist_iceberg61_last_check_timestamp_seconds` | 최근 검증 시간 (Unix seconds) |
| `olist_iceberg61_quality_checks_passed` | 검증한 Step59 품질 항목 수 (PASS일 때만 29) |
| `olist_iceberg61_published_gold_rows` | 조회된 공개 Gold 행 수 |
| `olist_iceberg61_main_snapshot_verified` | 현재 main / audit_v3 / Publish Snapshot 일치 |
| `olist_iceberg61_openmetadata_assets` | 현재 등록/검증된 Step61 Container 수 |
| `olist_iceberg61_openmetadata_edges` | 현재 등록/검증된 Step61 Lineage 수 |

처음부터 부품이 자동 모니터링되는 것은 아니다. 기존 **Node Exporter가 해당 `.prom` 디렉터리를 textfile collector로 읽고**, Prometheus가 exporter를 Scrape하는지 반드시 확인한다.
Grafana에서는 결측 값을 성공으로 표시하지 않는다. `last_check_timestamp_seconds`는 **검증 시각**이며, Gold 데이터 신선도나 최신 수집 시각을 의미하지 않는다.

## 의도적 제한

- 현재 데이터는 **불변 v3**다. 새 Raw 데이터의 실시간·주기적 수집을 하지 않는다.
- WAP Publish 보장 범위는 **Gold 단일 테이블**이다. 7개 테이블 원자적 Publish가 아니다.
- 초기 계보는 Step58/59 코드와 검증 증빙에 근거하며 임의 Spark 앱들의 런타임 계보는 수집하지 않는다.
- OpenMetadata와 Grafana, 기존 Catalog를 재설치하지 않는다.
- 인증 토큰은 기존 root 전용 `0600` 파일에서만 읽으며 Git에 포함하지 않는다.
- 내부망 접근을 전제로 한다. HTTP 엔드포인트를 공개망에 열지 않는다.
