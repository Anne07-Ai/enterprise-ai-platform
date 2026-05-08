#!/usr/bin/env bash
# scripts/healthcheck.sh
#
# Probe every service in the local stack and print a colored OK/FAIL summary.
# Used by `make up` (with --wait) to block until everything is green, and by
# `make healthcheck` for an on-demand probe.

set -euo pipefail

# Color helpers — disable when stdout is not a TTY (e.g. CI logs).
if [[ -t 1 ]]; then
  C_OK=$'\033[32m'
  C_FAIL=$'\033[31m'
  C_DIM=$'\033[2m'
  C_OFF=$'\033[0m'
else
  C_OK=''; C_FAIL=''; C_DIM=''; C_OFF=''
fi

WAIT_SECONDS=0
if [[ "${1:-}" == "--wait" && -n "${2:-}" ]]; then
  WAIT_SECONDS="$2"
fi

# Source .env if present so credentials are available for psql/redis-cli.
if [[ -f .env ]]; then
  set -a; source .env; set +a
fi

POSTGRES_USER="${POSTGRES_USER:-eaip}"
POSTGRES_DB="${POSTGRES_DB:-eaip}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-changeme_local_only}"
REDIS_PASSWORD="${REDIS_PASSWORD:-changeme_local_only}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"

# Each entry: name|command. The command must exit 0 on success.
checks=(
  "postgres|docker exec eaip-postgres pg_isready -U ${POSTGRES_USER} -d ${POSTGRES_DB}"
  "redis|docker exec eaip-redis redis-cli -a ${REDIS_PASSWORD} --no-auth-warning ping | grep -q PONG"
  "redpanda|docker exec eaip-redpanda rpk cluster health -X admin.hosts=localhost:9644 | grep -q 'Healthy:.*true'"
  "redpanda-console|curl -fsS http://localhost:8080/admin/startup >/dev/null"
  "minio|curl -fsS http://localhost:9000/minio/health/live >/dev/null"
  "otel-collector|curl -fsS http://localhost:13133/ >/dev/null"
  "prometheus|curl -fsS http://localhost:9090/-/healthy >/dev/null"
  "loki|curl -fsS http://localhost:3100/ready >/dev/null"
  "tempo|curl -fsS http://localhost:3200/ready >/dev/null"
  "grafana|curl -fsS http://localhost:3001/api/health >/dev/null"
)

run_one() {
  local name="$1" cmd="$2"
  if eval "$cmd" >/dev/null 2>&1; then
    printf "  %s✓%s %-18s %sOK%s\n" "$C_OK" "$C_OFF" "$name" "$C_OK" "$C_OFF"
    return 0
  else
    printf "  %s✗%s %-18s %sFAIL%s\n" "$C_FAIL" "$C_OFF" "$name" "$C_FAIL" "$C_OFF"
    return 1
  fi
}

probe_all() {
  local fails=0
  for entry in "${checks[@]}"; do
    local name="${entry%%|*}"
    local cmd="${entry#*|}"
    run_one "$name" "$cmd" || fails=$((fails + 1))
  done
  return "$fails"
}

if (( WAIT_SECONDS > 0 )); then
  echo "${C_DIM}Waiting up to ${WAIT_SECONDS}s for services to become healthy...${C_OFF}"
  deadline=$(( $(date +%s) + WAIT_SECONDS ))
  while :; do
    if probe_all >/dev/null 2>&1; then
      break
    fi
    if (( $(date +%s) >= deadline )); then
      echo "${C_FAIL}timeout: not all services became healthy within ${WAIT_SECONDS}s${C_OFF}" >&2
      probe_all || true
      exit 1
    fi
    sleep 2
  done
fi

echo "Health probe — eaip stack"
if probe_all; then
  echo "${C_OK}all green${C_OFF}"
  exit 0
else
  echo "${C_FAIL}one or more services unhealthy${C_OFF}" >&2
  exit 1
fi
