# 41. OpenMetadata Core — 전용 metadata VM

## 범위
- 대상: `inventory/hosts.ini`의 `[governance_nodes]`에서 `metadata` 한 대
- 버전: OpenMetadata **2.0.6** (이미지 태그 고정), MySQL **8.0**, Elasticsearch **9.3.0**
- 처음에는 OpenMetadata Core만 배포. **Ingestion/Airflow는 42단계**에서 메모리 측정 후 추가
- 포트: HTTP UI `192.158.2.104:8585`; Admin Health `127.0.0.1:8586`(VM 내부만)
- MySQL 3306 / Elasticsearch 9200/9300: Docker 내부 네트워크 전용
- 영속 데이터: Docker named volumes `bigtop-openmetadata_mysql-data`, `bigtop-openmetadata_es-data`
- 데이터베이스 비밀번호는 metadata VM의 `/opt/bigtop-governance/openmetadata/.env`에 처음 한 번 생성. **Git에는 저장되지 않음.**

## 설치 — worker3에서 실행
```bash
cd ~/bigtop-cluster-ansible
make lint
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/41-governance-openmetadata.yml
```

필수 조건: 40번 Docker Playbook 통과, metadata Ansible SSH 및 sudo 권한, 4 vCPU/8GB VM, 15GB 이상 사용 가능한 디스크, 메타데이터 VM에서 Docker 이미지 저장소로 접속 가능.

최초 Docker 이미지 Pull, MySQL/Elasticsearch 기동, 스키마 마이그레이션에는 시간이 걸릴 수 있다. Compose 기동 후 Ansible은 내부 healthcheck를 최대 약 4분 동안 확인한다. 8GB VM에서는 메모리 부족 또는 이미지 다운로드 지연으로 실패할 수 있으므로 아래 로그를 확인한다.

## 점검 — metadata 서버에서
```bash
sudo systemctl status bigtop-governance-openmetadata.service --no-pager
sudo docker compose -p bigtop-openmetadata -f /opt/bigtop-governance/openmetadata/compose.yaml --env-file /opt/bigtop-governance/openmetadata/.env ps
curl -f http://127.0.0.1:8586/healthcheck
sudo journalctl -u bigtop-governance-openmetadata.service -n 80 --no-pager
```

UI: `http://192.158.2.104:8585` (Windows에서 VM IP를 라우팅할 수 있는 경우). 기본 로그인/초기 관리자 정책은 공식 문서를 참고하고 즉시 변경할 것.

## 장애 발생 시
```bash
sudo docker compose -p bigtop-openmetadata -f /opt/bigtop-governance/openmetadata/compose.yaml --env-file /opt/bigtop-governance/openmetadata/.env logs --tail=100 openmetadata-server
sudo docker compose -p bigtop-openmetadata -f /opt/bigtop-governance/openmetadata/compose.yaml --env-file /opt/bigtop-governance/openmetadata/.env logs --tail=100 execute-migrate-all
sudo docker compose -p bigtop-openmetadata -f /opt/bigtop-governance/openmetadata/compose.yaml --env-file /opt/bigtop-governance/openmetadata/.env logs --tail=100 elasticsearch mysql
sudo docker stats --no-stream
free -h
```

**안전 주의사항:** `docker compose down -v`, `docker volume rm`, `docker system prune --volumes` 금지 — 메타데이터 저장소가 삭제될 수 있다. `.env` 비밀번호를 바꾸기만 하면 기존 DB 계정 비밀번호와 불일치할 수 있으므로 직접 수정하지 않는다. VM IP `192.158.2.x`는 RFC1918 사설 주소가 아니므로 외부 인터넷에 직접 노출하지 말고 VirtualBox 네트워크/방화벽을 점검한다. Docker Compose는 포트 충돌이 생기면 실패하며 기존 서버 포트는 변경하지 않는다.

이 41번 구성은 **PoC**이며 운영환경 HA, TLS/SSO, 정식 백업, 외부 노드 복제 등을 제공하지 않는다. 최소 RAM 8GB에서 무거운 수집을 동시에 돌리는 것은 권장하지 않는다.
