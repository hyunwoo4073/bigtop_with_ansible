# Step 44 — Evidence-backed PostgreSQL View Lineage

## 목적

OpenMetadata에 **실제로 존재하는 SQL 데이터 변환**의 소스-타깃 의존관계를 표시한다. PostgreSQL의 FK 관계는 데이터 계보로 간주하지 않는다.

```
public.platform_hosts ---------------------\
                                             > public.v_bigtop_service_inventory_lineage
public.platform_service_instances ---------/ 
```

- worker3 `bigtop_console` DB에서 SQL VIEW를 한 번만 생성한다. 원본 테이블의 데이터는 추가/삭제/변경하지 않는다.
- VIEW는 현재 존재하는(두 테이블 모두 `is_present = true`) 서비스·호스트 스냅샷을 JOIN한다.
- `bigtop_catalog_reader` 계정에는 VIEW의 `SELECT` 권한만 추가한다.
- **Step 42의 `includeViews: true` 수집 재실행 필수**: VIEW가 OpenMetadata에서 `tableType=View`로 인식되기 전에는 계보 API를 쓰지 않는다.
- Step 42 ingestion이 이미 ViewLineage를 만든 경우에는 **그대로 유지**한다. 생성되지 않았을 때만 44번 스크립트가 `Manual` 소스의 계보 두 건 및 실제 투영 컬럼 매핑을 보완한다.
- 기본 실행은 `--plan`이며, 명시적 `--apply` 또는 oneshot systemd 서비스 실행 시에만 API에 PUT을 전송한다.
- 수집 Bot JWT의 OpenMetadata 권한이 충분하지 않아 403을 받으면, 토큰 권한을 확인해야 한다. Git/로그/CLI 인자로 토큰을 노출하지 않는다.

## 명령어

```bash
cd ~/bigtop-cluster-ansible
python3 -m unittest discover -s tests -p 'test_governance_lineage.py' -v
make lint
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/44-governance-lineage.yml -e ansible_connection=local

# VIEW를 OpenMetadata Data Catalog에 다시 수집
sudo systemctl start bigtop-governance-pg-catalog.service
sudo journalctl -u bigtop-governance-pg-catalog.service -n 50 --no-pager

# read-only plan
sudo python3 /usr/local/lib/bigtop-governance/lineage/manage_lineage.py \
  --plan --url http://192.158.2.104:8585/api \
  --token-file /etc/bigtop-governance/pg-catalog/bot.token \
  --policy /etc/bigtop-governance/lineage/policy.yaml

# 명시적 적용
sudo systemctl start bigtop-governance-lineage.service
sudo journalctl -u bigtop-governance-lineage.service -n 50 --no-pager
```

브라우저: `http://192.158.2.104:8585` → Data Catalog → PostgreSQL Service → `bigtop_console.public.v_bigtop_service_inventory_lineage` → **Lineage**.

## 운영 제약

- 이 단계는 **SQL VIEW의 의존 관계**를 보여 준다. Spark 작업 로그나 쿼리 로그를 자동 분석한 것은 아니다. Spark 자동 계보는 OpenLineage 이벤트 수집 등 별도 단계가 필요하다.
- 기존 뷰 이름과 충돌하면 배포를 중단하며 기존 뷰를 덮어쓰지 않는다.
- VIEW는 base table의 현재 데이터 상태를 실시간 조회한다. 복제·물리화한 테이블은 생성하지 않는다.
- 실제 배포 전후 백업 및 스키마 변경 통제 기준을 확인한다.
- 커넥터가 ViewLineage를 자동 생성한 경우 별도 API 등록은 필요 없다.
- VIEW를 제거하려면 운영자 확인 후 VIEW에 대한 OpenMetadata 계보 관계를 정리하고 PostgreSQL 객체를 삭제해야 하며, 자동 제거 명령은 제공하지 않는다.
