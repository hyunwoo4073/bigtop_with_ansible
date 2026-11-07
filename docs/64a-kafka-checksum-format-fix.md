# Step 64-A — Apache Kafka SHA-512 체크섬 형식 수정

## 원인

Apache Kafka 4.2.2의 공식 `kafka_2.13-4.2.2.tgz.sha512`는 `파일명: 8자리 해시 그룹 ...` 형태로 표시되고 여러 줄로 줄바꿈됩니다. 기존 다운로드 도구는 공백 없이 붙은 128자리 16진수만 인식해서 `Invalid official Kafka SHA-512 checksum`로 중단됐습니다.

## 수정

- Apache의 여러 줄/공백 구분 형식과 일반 `sha512sum` 형식을 모두 안전하게 파싱합니다.
- **공식 Kafka 4.2.2 SHA-512 값으로 고정 검증**하고, 이어서 다운로드한 TAR.GZ의 실제 SHA-512와 비교합니다. 검증 생략 또는 해시 우회는 없습니다.
- 다운로드 검사 스크립트와 오프라인 설치 검증 스크립트를 함께 수정합니다.
- 재실행 시 기존에 정상 다운로드한 파일은 무결성을 확인하고 재사용합니다.
- 기존 Hadoop/Spark/Kafka 설정은 건드리지 않습니다. Kafka가 설치되기 전에 발생한 다운로드 단계 실패입니다.

출처: https://downloads.apache.org/kafka/4.2.2/kafka_2.13-4.2.2.tgz.sha512

## 적용

```bash
cd ~/bigtop-cluster-ansible
unzip -o 64a_kafka_sha512_format_fix.zip -d .
python3 -m unittest discover -s tests -p 'test_kafka_kraft64.py' -v
make lint
python3 scripts/64a-fetch-kafka-broker.py
python3 scripts/64a-verify-kafka-assets.py \
  vendor/kafka-broker64 \
  vendor/iceberg-rest/temurin17-jdk-linux-x64.tar.gz \
  vendor/iceberg-rest/temurin17-jdk-linux-x64.tar.gz.sha256
```

`STEP64A_KAFKA_ARCHIVE_VERIFIED`와 `STEP64A_KAFKA_JDK17_ASSETS_VERIFIED`가 나오면 Preflight 다음 단계로 진행하세요.
