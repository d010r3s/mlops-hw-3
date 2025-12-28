#!/usr/bin/env bash
set -euo pipefail

for i in {1..60}; do
  if clickhouse-client --host clickhouse --query "SELECT 1" >/dev/null 2>&1; then
    echo "ClickHouse is up."
    break
  fi
  sleep 1
done

clickhouse-client --host clickhouse --multiquery < /sql/01_ddl.sql
