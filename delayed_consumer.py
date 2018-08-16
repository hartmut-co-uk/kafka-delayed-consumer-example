import logging
import signal
import sys
import threading

from datetime import datetime, timedelta
from time import sleep

from confluent_kafka import KafkaError, Consumer, TopicPartition, OFFSET_STORED

### logging
logging.basicConfig(level=logging.INFO)

### config
bootstrap_servers = 'localhost:9092'
topic = 'kafka-delayed-consumer-example'
consumer_group = 'kafka-delayed-consumer-example'
client_id = 'kafka-delayed-consumer-example'
no_threads = 10

min_sleep_seconds = 0.5
kafka_keep_alive_seconds = 10
session_timeout_ms = 30 * 1000

# delay_seconds = 60 * 1   # 1min
delay_seconds = 60 * 15  # 15min
delay_timedelta = timedelta(seconds=delay_seconds)


### init
consumers = []


### register the Ctrl+C handler for graceful shutdown
def signal_handler(signal, frame):
    global consumers

    print('Ctrl+C pressed. terminating gracefully')

    for consumer in consumers:
        try:
            consumer.close()
        except:
            pass

    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)


### globals
threadLock = threading.Lock()
total_processed = 0


### functions
def process(thread_name, msg):
    global total_processed

    msg_partition = msg.partition()
    msg_key = msg.key().decode('utf-8')
    msg_value = msg.value().decode('utf-8')
    msg_timestamp = datetime.fromtimestamp(msg.timestamp()[1] / 1000.0)

    with threadLock:
        total_processed = total_processed + 1

    logging.info('[%s] %s | \'process_message()\' | timstamp=%s, partition=%s, key=%s, msg: %s | total=%d' % (
        thread_name, datetime.now().isoformat(), msg_timestamp.isoformat(), msg_partition, msg_key, msg_value,
        total_processed))


def worker():
    global consumers

    consumer = Consumer({'bootstrap.servers': bootstrap_servers, 'group.id': consumer_group, 'client.id': client_id,
                         'default.topic.config': {'auto.offset.reset': 'earliest'}, 'enable.auto.offset.store': False,
                         'session.timeout.ms': session_timeout_ms})
    consumers.append(consumer)

    consumer.subscribe([topic])

    while True:
        msg = consumer.poll(0)

        thread_name = threading.current_thread().name

        if msg == None or not msg:
            continue

        if not msg.error():
            msg_timestamp = datetime.fromtimestamp(msg.timestamp()[1] / 1000.0)

            keep_alive_counter = 0
            now = datetime.now()
            # loop/sleep to delay the message
            while now < msg_timestamp + delay_timedelta:
                keep_alive_counter = keep_alive_counter + 1

                msg_timestamp_with_delta = msg_timestamp + delay_timedelta
                diff1 = msg_timestamp_with_delta - now
                diff_seconds = diff1.total_seconds()

                if keep_alive_counter <= 1:
                    logging.info("[%s] %s | received message on partition=%d, delaying for %fs" % (
                    thread_name, now.isoformat(), msg.partition(), diff_seconds))

                # sleep for {min_sleep_seconds}s...{kafka_keep_alive_seconds}s
                sleep_seconds = min(kafka_keep_alive_seconds, max(min_sleep_seconds, diff_seconds))

                # use as 'keep alive' feedback for low (no) traffic periods... to avoid connections getting dropped by brokers - resulting in a group rebalance
                logging.debug(
                    "[%s] %s | kafka keep alive commit partition=%d" % (thread_name, now.isoformat(), msg.partition()))
                consumer.commit(
                    offsets=[TopicPartition(topic=msg.topic(), partition=msg.partition(), offset=OFFSET_STORED)])

                # go to sleep
                logging.debug("[%s] %s | going to sleep for %fs / lag: %fs" % (
                    thread_name, now.isoformat(), sleep_seconds, diff_seconds))
                sleep(sleep_seconds)
                now = datetime.now()

            process(thread_name, msg)
            consumer.store_offsets(msg)

        elif msg.error().code() == KafkaError._PARTITION_EOF:
            continue

        else:
            logging.error("kafka consumer error: %s" % msg.error())


### main
logging.info("starting kafka consumers with delay=%ds" % delay_seconds)
threads = []
for i in range(no_threads):
    t = threading.Thread(target=worker)
    threads.append(t)
    t.start()

logging.info("%d threads started" % no_threads)
