#!/usr/bin/env bash

BASEDIR=$(dirname "$0")

TOPIC=kafka-delayed-consumer-example
NO_PARTITIONS=10
REPLICATION_FACTOR=1
ZOOKEEPER_HOST=localhost
BROKER_LIST=localhost:9092
CONSUMER_GROUP=kafka-delayed-consumer-example


echo `date`

# stats
kafka-consumer-groups --bootstrap-server ${BROKER_LIST} --describe --group ${CONSUMER_GROUP}
kafka-consumer-groups --bootstrap-server ${BROKER_LIST} --describe --group ${CONSUMER_GROUP} \
    | (read; read; cat) \
    | while IFS=$' \t\n' read topic_name partition_id current_offset log_end_offset lag remaining; do echo "$lag"; done \
    | paste -sd+ - | bc \
    | awk '{print "total lag: "$1}'

echo ""
