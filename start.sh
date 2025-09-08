#!/usr/bin/env bash
set -e

/app/wait-for-mysql.sh

if [ "${SERVICE:-app}" = "app" ]; then
  timeout=${GUNICORN_TIMEOUT:-120}
  cert_args=()
  worker_class=()

  if [ -n "$ASYNC_WORKERS" ]; then
    workers="$ASYNC_WORKERS"
    worker_class=(--worker-class gevent)
  else
    workers=${WORKERS:-$((2 * $(nproc) + 1))}
  fi

  if [ -n "$SSL_CERT_PATH" ] && [ -n "$SSL_KEY_PATH" ]; then
    cert_args=(--certfile "$SSL_CERT_PATH" --keyfile "$SSL_KEY_PATH")
  fi

  exec gunicorn --workers "$workers" "${worker_class[@]}" --timeout "$timeout" \
    --bind "${FLASK_HOST:-0.0.0.0}:${FLASK_PORT:-5000}" \
    "${cert_args[@]}" app:app
else
  exec python -m "${SERVICE}"
fi
