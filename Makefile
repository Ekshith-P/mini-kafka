.PHONY: build test package run-single cluster-up cluster-down bench docker-up docker-down clean

build:		## compile
	mvn -q -DskipTests compile

test:		## run the unit + integration tests
	mvn -q test

package:	## build the runnable fat jar (target/mini-kafka.jar)
	mvn -q -DskipTests package

run-single: package ## run a single broker on localhost:9092
	java -jar target/mini-kafka.jar broker config/broker-single.properties

cluster-up: package ## start a local 3-broker cluster in the background
	bash scripts/run-local-cluster.sh

cluster-down:	## stop the local cluster
	bash scripts/stop-local-cluster.sh

bench: package	## run the throughput benchmark against a running broker
	java -jar target/mini-kafka.jar bench --bootstrap localhost:9092 \
		--topic bench --create --partitions 3 --replication 3 --records 200000 --size 100

docker-up:	## build and start the 3-broker cluster in docker
	docker compose up --build

docker-down:	## stop and remove the docker cluster (and its volumes)
	docker compose down -v

clean:		## remove build output and runtime data
	mvn -q clean
	rm -rf data logs