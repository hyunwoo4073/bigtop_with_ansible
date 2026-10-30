# 45 — Olist 원본 CSV → HDFS Raw Zone

## 목적

Hadoop/HDFS/Spark 실습의 첫 실제 데이터셋을 등록한다. 이미 다운로드한 **Olist 원본 CSV** 중 주문, 주문상품, 고객, 상품, 결제, 리뷰, 판매자, 지역, 카테고리 번역 9개를 **CSV 그대로** HDFS에 버전별로 저장한다. Spark 변환/파티셔닝은 다음 작업으로 분리한다.

## 대상 및 전제 조건

- **worker3**: Ansible Controller. CSV가 `olist/` 폴더에 존재해야 한다. Git에는 원본 데이터를 올리지 않는다.
- **master**: `hdfs` CLI, `hdfs` 서비스 계정, HDFS NameNode 통신이 가능해야 한다. Ansible은 master에 SSH 접속해 작업한다.
- **worker1·worker2**: 기존 DataNode로 HDFS 데이터를 저장한다. 작업 대상 Play는 master뿐이다.
- **추가 서버 또는 패키지 설치 없음**. Python 표준 라이브러리만 이용한다.

## 필수 파일

- `olist_orders_dataset.csv`
- `olist_order_items_dataset.csv`
- `olist_customers_dataset.csv`
- `olist_products_dataset.csv`
- `olist_order_payments_dataset.csv`
- `olist_order_reviews_dataset.csv`
- `olist_sellers_dataset.csv`
- `olist_geolocation_dataset.csv`
- `product_category_name_translation.csv`

9개 CSV 전부 원본 그대로 수집한다. Spark에서 필요한 테이블만 선택적으로 처리한다.

## 소스 경로

기본값은 프로젝트 내부 `olist/source/`이다.

```text
~/bigtop-cluster-ansible/olist/source/olist_orders_dataset.csv
```

실제 소스 폴더가 다른 곳이라면 Playbook 실행 시 아래처럼 지정한다.

```bash
-e "data_pipeline_olist_raw_local_dir=$HOME/other-source"
```

데이터는 Git에 포함하지 않는다. 프로젝트 폴더에 `olist/`를 유지하려면 `.gitignore`에 `/olist/`를 추가하는 것을 권장한다. `git add .`는 피한다.

## 실행

```bash
cd ~/bigtop-cluster-ansible
python3 -m unittest discover -s tests -p 'test_olist_raw.py' -v
make lint
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/45-olist-hdfs-raw.yml
```

## 결과 경로

```text
/data/olist/raw/v1/
  _MANIFEST.json
  orders/olist_orders_dataset.csv
  order_items/olist_order_items_dataset.csv
  customers/olist_customers_dataset.csv
  products/olist_products_dataset.csv
  payments/olist_order_payments_dataset.csv
  reviews/olist_order_reviews_dataset.csv
  sellers/olist_sellers_dataset.csv
  geolocation/olist_geolocation_dataset.csv
  category_translation/product_category_name_translation.csv
```

`_MANIFEST.json`에는 각 CSV의 **레코드 수(헤더 제외), 바이트 크기, SHA-256**만 기록한다. 로컬 임시 복사본은 Playbook 종료 시 제거한다. HDFS 파일 복제 계수는 2 (현재 2개의 DataNode)로 요청한다.

## 안전한 재실행

- 이미 같은 파일이 있으면 HDFS에서 내용을 `-cat`으로 스트리밍해 SHA-256을 다시 검증하고 건너뛴다.
- 같은 버전 경로에 다른 내용이 있으면 절대 덮어쓰지 않고 실패한다. 새 버전(`v2` 등)을 사용한다.
- 파일 업로드 후 SHA-256을 확인하고 최종 이름으로 이동시킨다. 일부 실패해도 이미 게시한 파일은 유지되며 재실행으로 복구 가능하다.
- 기존 PostgreSQL, OpenMetadata 및 Hadoop 구성은 수정하지 않는다.

## 점검

master에서:

```bash
sudo -u hdfs hdfs dfs -ls -R /data/olist/raw/v1
sudo -u hdfs hdfs dfs -cat /data/olist/raw/v1/_MANIFEST.json
sudo -u hdfs hdfs dfs -du -h /data/olist/raw/v1
sudo -u hdfs hdfs dfs -stat "%r" /data/olist/raw/v1/orders/olist_orders_dataset.csv
```

실제 데이터와 `sha256`을 비교해야 하면 **`hdfs dfs -checksum` 대신**, 원본과 같은 SHA-256을 얻는 `hdfs dfs -cat ... | sha256sum`을 사용한다. HDFS 체크섬과 SHA-256은 같은 값이 아니다.

## 주의

- `v1`은 **입력 스냅샷 버전**이며, 날짜 파티션이 아니다. 주문일 파티션은 Spark에서 처리할 때 만든다.
- Olist 주문은 여러 상품을 포함할 수 있고 `order_item_id`가 있으므로 상품 행 수와 주문 건수는 다르다.
- DataNode의 실제 저장 공간과 전체 복제 상태는 별도 `hdfs dfsadmin -report`로 확인한다.
- HDFS 경로에 동일한 파일명을 덮어쓰지 않으므로 원본 데이터 변경을 실험하려면 `-e data_pipeline_olist_raw_version=v2`를 사용한다.

## 4 CSV 버전에서 확장한 경우

기존 v1에 4개 파일을 게시한 경우 v1의 `_MANIFEST.json`이 다르므로 새 v2 경로를 선택한다. 이전 v1을 강제로 삭제/덮어쓰지 않는다.

```bash
.venv/bin/ansible-playbook -i inventory/hosts.ini playbooks/45-olist-hdfs-raw.yml -e data_pipeline_olist_raw_version=v2
```
