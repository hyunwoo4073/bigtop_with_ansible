#!/usr/bin/env bash
# Run on worker3 after 64A install; changes only the dedicated lab test topic.
set -euo pipefail
KAFKA_HOME="${KAFKA_HOME:-/opt/bigtop-kafka-kraft64/kafka}"
# Do not inherit a shell's global JAVA_HOME: it may still point to Java 8/11.
# The broker systemd unit already uses this dedicated Java 17 installation.
JAVA_HOME="${KAFKA64_JAVA_HOME:-/opt/bigtop-kafka-kraft64/jdk17}"
export JAVA_HOME
if [[ ! -x "$JAVA_HOME/bin/java" ]]; then
  echo "FAIL: dedicated Kafka Java is missing: $JAVA_HOME/bin/java" >&2
  exit 1
fi
JAVA_VERSION=$("$JAVA_HOME/bin/java" -version 2>&1) || {
  echo "FAIL: cannot execute dedicated Kafka Java: $JAVA_HOME/bin/java" >&2
  exit 1
}
if ! grep -Eq 'version "17([."]|$)' <<< "$JAVA_VERSION"; then
  echo "FAIL: Kafka smoke requires Java 17, found: $(head -n 1 <<< "$JAVA_VERSION")" >&2
  exit 1
fi
echo "STEP64A_KAFKA_CLI_JAVA17_OK: $JAVA_HOME/bin/java"
BOOTSTRAP="${BOOTSTRAP:-192.158.2.101:9092,192.158.2.102:9092,192.158.2.103:9092}"
TOPIC=bigtop_kafka64_smoke_v1
BIN="$KAFKA_HOME/bin"

"$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists   --topic "$TOPIC" --partitions 1 --replication-factor 3   --config min.insync.replicas=2
DESCRIPTION=$("$BIN/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC")
printf '%s\n' "$DESCRIPTION"
if ! grep -Eq 'ReplicationFactor: 3' <<< "$DESCRIPTION"; then
  echo 'FAIL: smoke topic RF is not 3' >&2
  exit 1
fi
if ! grep -Eq 'Isr: ([0-9]+,[0-9]+,[0-9]+)' <<< "$DESCRIPTION"; then
  echo 'FAIL: expected three healthy in-sync replicas' >&2
  exit 1
fi

# Establish the next unread offset before producing to avoid false positives
# from old test records on repeated smoke runs.
OFFSET_LINE=$("$BIN/kafka-get-offsets.sh" --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC" --time -1)
START_OFFSET=$(printf '%s\n' "$OFFSET_LINE" | awk -F: '$2==0 {print $3}')
[[ "$START_OFFSET" =~ ^[0-9]+$ ]] || { echo 'Cannot establish start offset' >&2; exit 1; }
MARKER="STEP64A_SMOKE_$(date +%s)_$$"
printf '%s\n' "$MARKER" | "$BIN/kafka-console-producer.sh"   --bootstrap-server "$BOOTSTRAP" --topic "$TOPIC"   --producer-property acks=all
RECEIVED=$("$BIN/kafka-console-consumer.sh" --bootstrap-server "$BOOTSTRAP"   --topic "$TOPIC" --partition 0 --offset "$START_OFFSET"   --max-messages 1 --timeout-ms 15000 2>/dev/null)
if [[ "$RECEIVED" != "$MARKER" ]]; then
  echo "FAIL: produced=$MARKER received=$RECEIVED" >&2
  exit 1
fi
printf 'STEP64A_KAFKA_PRODUCE_CONSUME_PASS: %s\n' "$RECEIVED"
