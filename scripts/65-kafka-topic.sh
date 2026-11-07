#!/usr/bin/env bash
# Step65-only topic; do not change existing broker/topic/consumer group configs.
set -euo pipefail
MODE=${1:-}
BOOTSTRAP=${2:-192.158.2.101:9092,192.158.2.102:9092,192.158.2.103:9092}
TOPIC=bigtop_olist_step65_cdc_v1
KAFKA_BIN=/opt/bigtop-kafka-kraft64/kafka/bin/kafka-topics.sh
export JAVA_HOME=/opt/bigtop-kafka-kraft64/jdk17
export PATH="$JAVA_HOME/bin:$PATH"
export KAFKA_HEAP_OPTS='-Xms128m -Xmx256m'
[[ -x "$JAVA_HOME/bin/java" && -x "$KAFKA_BIN" ]] || { echo 'Missing Kafka CLI + Java17' >&2; exit 2; }
"$JAVA_HOME/bin/java" -version 2>&1 | head -n 1 | grep -Eq '"17([.]|")' || {
  echo 'Dedicated Java17 expected' >&2; exit 2;
}
case "$MODE" in
  create)
    "$KAFKA_BIN" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
      --topic "$TOPIC" --partitions 1 --replication-factor 3 \
      --config min.insync.replicas=2 --config retention.ms=604800000 ;;
  describe) ;;
  *) echo "Usage: $0 create|describe" >&2; exit 2 ;;
esac
DESC=$("$KAFKA_BIN" --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC")
printf '%s\n' "$DESC"
[[ "$DESC" =~ PartitionCount:[[:space:]]*1([^[:digit:]]|$) ]] || exit 1
[[ "$DESC" =~ ReplicationFactor:[[:space:]]*3([^[:digit:]]|$) ]] || exit 1
[[ "$DESC" == *'min.insync.replicas=2'* ]] || exit 1
# Count ISR exactly, not merely substring 1,2,3 embedded in larger list.
PARTITION=$(grep -E 'Partition:[[:space:]]*0([[:space:]]|$)' <<<"$DESC" | head -n 1)
ISR=$(sed -nE 's/.*Isr:[[:space:]]*([0-9]+(,[0-9]+)*).*/\1/p' <<<"$PARTITION")
[[ $(tr ',' '\n' <<<"$ISR" | grep -cE '^[0-9]+$') == 3 ]] || {
  echo 'Step65 ISR !=3. Investigate KRaft quorum and replicas.' >&2; exit 1;
}
echo "STEP65_TOPIC_READY: $TOPIC RF=3 minISR=2 ISR=3"
