# 46 적용 순서 (2026-10-30, YARN cluster 모드)

1. 45번 성공 여부 확인: `/data/olist/raw/v1/_MANIFEST.json` 및 원본 CSV 9개.
2. worker3 프로젝트 루트에서 수정된 46번 패키지의 해당 파일을 배포한다.
3. `python3 -m unittest discover -s tests -p 'test_olist_etl.py' -v` 실행.
4. `make lint` 실행. `become_user`를 사용하는 10개 Task에는 동일 Task 레벨의 `become: true`가 반드시 있어야 한다.
5. `.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/46-olist-spark-etl.yml` 실행.
6. Playbook이 Spark 종료 코드, HDFS `_ETL_MANIFEST.json`, Parquet 7개 `_SUCCESS`를 검증하는지 확인.
7. HDFS `/data/olist/warehouse/v1` 경로와 YARN Application 결과 확인.
8. 동일 명령을 재실행해 Ansible status `EXISTING_VERIFIED`를 확인하고 데이터 중복 적재가 없는지 확인.

**실행 모드:** `worker3` Ansible Controller → `master` Spark 제출 호스트 → `--master yarn --deploy-mode cluster` → Worker NodeManager 컨테이너 내 Driver/Executor. `-e ansible_connection=local`은 사용하지 않는다. Driver 로그는 `yarn logs -applicationId ...` 또는 Spark History Server에서 확인한다. 기존 Raw Zone은 변경하지 않는다.
