#!/usr/bin/env bash
# Starts a local 3-broker cluster in the background. Build first: mvn -q -DskipTests package
set -euo pipefail
cd "$(dirname "$0")/.."

JAR=target/mini-kafka.jar
if [ ! -f "$JAR" ]; then
  echo "jar not found - build it first: mvn -q -DskipTests package"
  exit 1
fi

mkdir -p logs
for id in 1 2 3; do
  java -jar "$JAR" broker "config/broker-$id.properties" > "logs/broker-$id.log" 2>&1 &
  echo "started broker $id (pid $!) -> logs/broker-$id.log"
done

echo
echo "cluster is up on localhost:9092, localhost:9093, localhost:9094"
echo "tail the logs with: tail -f logs/broker-*.log"
echo "stop it with:        scripts/stop-local-cluster.sh"