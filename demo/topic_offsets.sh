#!/usr/bin/env bash

BASEDIR=$(dirname "$0")

TOPIC=kafka-delayed-consumer-example
NO_PARTITIONS=10
REPLICATION_FACTOR=1
ZOOKEEPER_HOST=localhost
BROKER_LIST=localhost:9092


# stats
kafka-run-class kafka.tools.GetOffsetShell --broker-list ${BROKER_LIST} --topic ${TOPIC} --time -1
kafka-run-class kafka.tools.GetOffsetShell --broker-list ${BROKER_LIST} --topic ${TOPIC} --time -1 \
    | while IFS=: read topic_name partition_id number; do echo "$number"; done \
    | paste -sd+ - | bc \
    | awk '{print "total: "$1}'
