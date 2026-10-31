# 52 — 적용 순서 (worker3에서 실행)

## 0. 배포

```bash
cd ~/bigtop-cluster-ansible
unzip -o 52_olist_quality_promotion_ansible.zip -d .
python3 -m unittest discover -s tests -p 'test_olist_promotion.py' -v
make lint
```

## 1. v3 품질 검사 수행 (이전에 완료했다면 건너뛰기)

51번의 v3 Warehouse와 실행 계보 등록이 완료됐더라도, 품질 검사는
기존 v1 이력과 별개다. 최신 v3 품질 리포트가 있어야 승인할 수 있다.

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/47-olist-data-quality.yml \
  -e data_pipeline_olist_quality_warehouse_version=v3
```

결과가 `PASS`이고 `failed=0`인지 확인한다. `FAIL`이라면
**52번의 승인 조건을 완화하지 말고 문제를 해결한다.**

## 2. 읽기 전용 승인 계획

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/52-olist-quality-promotion.yml \
  -e data_pipeline_olist_promotion_warehouse_version=v3
```

정상 예:

```text
OLIST PROMOTION PLAN: version=v3 quality_run=... checks=29 ...
OLIST PROMOTION READ-ONLY: no approval created
```

## 3. 승인을 HDFS에 기록

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/52-olist-quality-promotion.yml \
  -e data_pipeline_olist_promotion_warehouse_version=v3 \
  -e data_pipeline_olist_promotion_apply=true
```

정상 예:

```text
OLIST PROMOTION CHANGED: /data/olist/published/v3/_APPROVAL.json
```

## 4. HDFS 검증 및 재실행 멱등성

```bash
ssh master 'sudo -u hadoop hdfs dfs -cat /data/olist/published/v3/_APPROVAL.json' \
  | python3 -m json.tool

.venv/bin/ansible-playbook -i inventory/hosts.ini \
  playbooks/52-olist-quality-promotion.yml \
  -e data_pipeline_olist_promotion_warehouse_version=v3 \
  -e data_pipeline_olist_promotion_apply=true
```

두 번째 APPLY에서 `OLIST PROMOTION UNCHANGED`가 나와야 한다.

## 5. 오래된 보고서의 경우

기본 품질 리포트 최신성 제한은 168시간이다. 오래된 리포트만 있다면
47번 v3를 **새로 실행**하고, 다시 52번으로 돌아온다. 사용자가 알고
있는 위험과 승인 정책에 따라 최신성 설정을 조정할 수 있으나 무조건
최신성 제한을 끄지 않는다.

## Git

```bash
git add playbooks/52-olist-quality-promotion.yml \
  roles/data_pipeline_olist_promotion \
  tests/test_olist_promotion.py \
  docs/52-olist-apply.md docs/52-olist-quality-promotion.md
git commit -m 'feat: quality-gate immutable Olist warehouse promotion'
```

Step 48 timer, Step 49 Grafana, Step 50 OpenMetadata catalog,
Step 51 runtime lineage에는 변경을 가하지 않는다.
