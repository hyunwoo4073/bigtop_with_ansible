# Step 45: Olist → HDFS Raw (9개 CSV, 2026-10-30 revision)

이 ZIP은 기존 45번 소스를 9개 데이터셋으로 확장한 전체 패키지다. **별도 `apply.sh` 없이** 프로젝트 루트에 덮어쓴다. 원본은 사용자 서버의 `olist/source/`에 있어야 하며 ZIP에는 포함되지 않는다.

```bash
cd ~/bigtop-cluster-ansible
unzip -o 45_olist_hdfs_raw_9csv_ansible.zip -d .
# 원본 파일명 검증 (9개)
find olist/source -maxdepth 1 -type f -name '*.csv' | sort
# 원본 데이터는 git 추적에서 제외
grep -qxF '/olist/' .gitignore || echo '/olist/' >> .gitignore
python3 -m unittest discover -s tests -p 'test_olist_raw.py' -v
make lint
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/45-olist-hdfs-raw.yml
```

기본 source 디렉터리는 `~/bigtop-cluster-ansible/olist/source`이며 변경하려면 `-e data_pipeline_olist_raw_local_dir=/path/to/source`로 명시한다.

Ansible Controller는 worker3이고, 실행 대상은 master다. master로 **원격 SSH**가 필요하므로 `-e ansible_connection=local`을 사용하지 않는다.

HDFS 대상은 `/data/olist/raw/v1`이고, 9개 하위 폴더 및 `_MANIFEST.json`을 만든다. 파일별 레코드 수, 원본 크기, SHA-256을 검증하며 동일 스냅샷의 변조는 덮어쓰지 않는다. 이미 v1에 4개 파일만 저장된 뒤 9개로 다시 적재하려는 경우 기존 `_MANIFEST.json`과 불일치하여 실패한다. 이 경우 **기존 v1을 지우지 말고** `-e data_pipeline_olist_raw_version=v2`를 주어 9개 전체를 새 스냅샷으로 적재한다.

기존 Hadoop/DB/OpenMetadata 서비스를 재설치하거나 종료하지 않는다. 원본 9개를 모두 scan하며 특히 geolocation CSV 크기 때문에 검증/전송 시간이 소요될 수 있다.

성공 로그: `PUT verified ...` 9개, `PUT manifest`, `OLIST RAW INGESTION OK`.
재실행 로그: `SKIP verified ...` 9개, `SKIP manifest unchanged`.
