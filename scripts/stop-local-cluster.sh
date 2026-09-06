#!/usr/bin/env bash
# Stops brokers started by run-local-cluster.sh.
set -uo pipefail

pids=$(pgrep -f "mini-kafka.jar broker" || true)
if [ -z "$pids" ]; then
  echo "no running brokers found"
  exit 0
fi

echo "stopping brokers: $pids"
# SIGTERM lets the JVM shutdown hook flush and close logs cleanly.
kill $pids