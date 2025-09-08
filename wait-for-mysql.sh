#!/usr/bin/env bash
set -e

: "${MYSQL_HOST:=mysql}"
: "${MYSQL_PORT:=3306}"

echo "⏳ Waiting for MySQL at $MYSQL_HOST:$MYSQL_PORT ..."
for i in {1..60}; do
  if nc -z "$MYSQL_HOST" "$MYSQL_PORT" 2>/dev/null; then
    echo "✅ MySQL is up."
    exit 0
  fi
  sleep 1
done

echo "❌ MySQL did not become ready in time."
exit 1
