#!/usr/bin/env bash
# Only creates/verifies the dedicated Step64 topic on installed 64-A Kafka brokers.
# Never changes existing consumer groups, deletes topics, or modifies Kafka services.
set -euo pipefail
MODE=${1:-}
BOOTSTRAP=${2:-192.158.2.101:9092,192.158.2.102:9092,192.158.2.103:9092}
RF=${3:-3}
TOPIC=bigtop_olist_step64_cdc_v1
KAFKA_BIN=${KAFKA_TOPICS_BIN:-/opt/bigtop-kafka-kraft64/kafka/bin/kafka-topics.sh}
export JAVA_HOME="${KAFKA64_JAVA_HOME:-/opt/bigtop-kafka-kraft64/jdk17}"
export PATH="$JAVA_HOME/bin:$PATH"
export KAFKA_HEAP_OPTS="${KAFKA_HEAP_OPTS:--Xms128m -Xmx256m}"
if [[ ! -x "$JAVA_HOME/bin/java" || ! -f "$KAFKA_BIN" ]]; then
  echo "Kafka CLI or dedicated Java 17 missing on this node; run on worker3." >&2
  exit 2
fi
version=$($JAVA_HOME/bin/java -version 2>&1 | head -n 1)
if ! grep -Eq '"17([.]|\")' <<<"$version"; then
  echo "Kafka topic CLI requires dedicated Java 17, got $version" >&2
  exit 2
fi
case "$MODE" in
  create)
    if [[ "$RF" != 3 ]]; then
      echo 'Step64 only supports three 64-A brokers with replication factor 3.' >&2
      exit 2
    fi
    "$KAFKA_BIN" --bootstrap-server "$BOOTSTRAP" \
      --create --if-not-exists --topic "$TOPIC" \
      --partitions 1 --replication-factor 3 \
      --config min.insync.replicas=2 \
      --config retention.ms=604800000
    ;;
  describe) ;;
  *) echo "Usage: $0 create|describe [bootstrap] [replication-factor=3]" >&2; exit 2 ;;
esac
desc=$("$KAFKA_BIN" --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC")
printf '%s\n' "$desc"
if [[ ! "$desc" =~ PartitionCount:[[:space:]]*1([^[:digit:]]|$) || \
      ! "$desc" =~ ReplicationFactor:[[:space:]]*3([^[:digit:]]|$) || \
      ! "$desc" =~ min[.]insync[.]replicas=2 ]]; then
  echo 'Step64 topic configuration mismatch. Do not modify an unrelated topic.' >&2
  exit 1
fi
if ! grep -qE 'Isr:[[:space:]]*([0-9]+,){2}[0-9]+' <<<"$desc"; then
  echo 'Step64 topic does not have 3 in-sync replicas; inspect broker health.' >&2
  exit 1
fi
printf 'STEP64_TOPIC_READY: %s RF=3 minISR=2 ISR=3\n' "$TOPIC"
