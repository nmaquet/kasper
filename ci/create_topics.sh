#!/bin/bash

set -e

docker exec -ti ci_kafka_1 /opt/kafka_2.11-0.10.0.1/bin/kafka-topics.sh --zookeeper 172.18.0.1:2181 --create --topic characters --replication-factor 1 --partitions 1
docker exec -ti ci_kafka_1 /opt/kafka_2.11-0.10.0.1/bin/kafka-topics.sh --zookeeper 172.18.0.1:2181 --create --topic fictions --replication-factor 1 --partitions 1
docker exec -ti ci_kafka_1 /opt/kafka_2.11-0.10.0.1/bin/kafka-topics.sh --zookeeper 172.18.0.1:2181 --create --topic fictions-and-characters --replication-factor 1 --partitions 1
