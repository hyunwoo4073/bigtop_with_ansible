# 64-A Kafka smoke Java 17 isolation fix

## Problem

`kafka-topics.sh` failed with `UnsupportedClassVersionError` (`61.0` / Java 17 class file, Java 8 runtime only supports `52.0`). Kafka 4.2.2 binaries and the dedicated JDK 17 were installed, but the smoke script preserved an existing shell `JAVA_HOME` pointing to an older Java.

## Fix

`scripts/64a-smoke-kafka.sh` now explicitly sets `JAVA_HOME=/opt/bigtop-kafka-kraft64/jdk17` regardless of the caller's `JAVA_HOME`. Optionally override **only for this smoke** with `KAFKA64_JAVA_HOME`. It checks the executable and major version before running Kafka CLI, and does not change `update-alternatives`, global environment, or broker services.

## Apply on worker3

```bash
cd ~/bigtop-cluster-ansible
unzip -o 64a_kafka_smoke_java17_fix.zip -d .
python3 -m unittest discover -s tests -p 'test_kafka_kraft64.py' -v
make lint
/opt/bigtop-kafka-kraft64/jdk17/bin/java -version
bash scripts/64a-smoke-kafka.sh
```

Expect `STEP64A_KAFKA_CLI_JAVA17_OK` then `STEP64A_KAFKA_PRODUCE_CONSUME_PASS`.

If `64A verify` was not run yet, run it first. **Do not format KRaft storage again or change system Java.**
