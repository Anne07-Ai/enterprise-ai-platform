#!/usr/bin/env bash
# scripts/seed-kafka-topics.sh
#
# Idempotently create the v1 topic set + DLQ counterparts.
# Partition counts mirror expected fan-out: chat is the highest-throughput stream,
# usage events keep up with chat, the rest are document/agent lifecycle.

set -euo pipefail

CONTAINER="${RPK_CONTAINER:-eaip-redpanda}"
REPLICATION="${TOPIC_REPLICATION:-1}"

# topic|partitions
topics=(
  "documents.uploaded.v1|12"
  "documents.parsed.v1|12"
  "documents.embedded.v1|12"
  "conversations.message.v1|24"
  "agents.run.requested.v1|12"
  "agents.run.completed.v1|12"
  "usage.recorded.v1|24"
  "audit.event.v1|12"
)

create_topic() {
  local name="$1" partitions="$2"
  if docker exec "$CONTAINER" rpk topic describe "$name" >/dev/null 2>&1; then
    echo "  · $name (exists)"
  else
    docker exec "$CONTAINER" rpk topic create "$name" \
      --partitions "$partitions" \
      --replicas "$REPLICATION" >/dev/null
    echo "  ✓ $name (partitions=$partitions, replicas=$REPLICATION)"
  fi
}

echo "Seeding Kafka topics on $CONTAINER"
for entry in "${topics[@]}"; do
  name="${entry%%|*}"
  parts="${entry#*|}"
  create_topic "$name" "$parts"
  create_topic "${name}.dlq" "$parts"
done

echo
echo "Topic listing:"
docker exec "$CONTAINER" rpk topic list
