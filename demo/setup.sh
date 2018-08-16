#!/usr/bin/env bash

BASEDIR=$(dirname "$0")

TOPIC=kafka-delayed-consumer-example
NO_PARTITIONS=10
REPLICATION_FACTOR=1
ZOOKEEPER_HOST=localhost
BROKER_LIST=localhost:9092


# cleanup if exists
kafka-topics --zookeeper ${ZOOKEEPER_HOST} --delete --topic ${TOPIC}

sleep 1

# create topic
kafka-topics --zookeeper ${ZOOKEEPER_HOST} --create --topic ${TOPIC} --replication-factor ${REPLICATION_FACTOR} --partitions ${NO_PARTITIONS}

