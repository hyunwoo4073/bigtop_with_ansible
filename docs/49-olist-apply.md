# 49. Olist Pipeline & Quality Dashboard — 배포

worker3의 `~/bigtop-cluster-ansible`에서 실행한다. ZIP을 **프로젝트 루트**에 풀 것.

```bash
.venv/bin/python -m unittest discover -s tests -p 'test_olist_observability.py' -v
make lint
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/49-olist-observability.yml -e ansible_connection=local \
  -e data_pipeline_olist_observability_openmetadata_url=http://192.158.2.104:8585
```

OpenMetadata 주소는 예전 실습 설정에서 사용한 예시다. **브라우저에서 실제 접근되는 주소로 변경**한다.
`ansible_connection=local`은 playbook 대상이 worker3(`ops`) 하나일 때만 사용한다.

```bash
python3 -m json.tool /var/lib/grafana/dashboards/olist-pipeline-overview.json >/dev/null
curl -fsS http://127.0.0.1:3000/api/health
curl -fsSG --data-urlencode 'query=olist_pipeline_last_attempt_success{warehouse_version="v1"}' \
  http://127.0.0.1:9090/api/v1/query | python3 -m json.tool
sudo ls -l /var/lib/prometheus/node-exporter/olist_{pipeline,quality}.prom
```

대시보드: `http://worker3:23000/d/bigtop-olist-pipeline/olist-pipeline-overview`
(23000은 기존 Web Console 바로가기 환경값, 실제 브라우저 접근 포트가 다른 경우 주소 수정)

Grafana file provider는 약 30초 간격으로 JSON을 읽는다. UI에 보이지 않으면 Grafana 재시작 전에 provider 설정, Grafana 로그, datasource UID를 확인한다.

49번은 Dashboard + OpenMetadata **탐색 링크**만 추가한다. 아직 Olist Parquet 데이터의 OpenMetadata 수집/실행 계보 생성은 수행하지 않는다.
