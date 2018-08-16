import datetime
import random
import logging
import signal
import sys

from time import sleep

from confluent_kafka import KafkaError, Producer

### logging
logging.basicConfig(level=logging.DEBUG)

### config
bootstrap_servers = 'localhost:9092'
topic = 'kafka-delayed-consumer-example'

# producer_random_upper_limit_seconds = 1  # produce every 1s...2s
# producer_random_upper_limit_seconds = 10       # produce every 1s...10s
# producer_random_upper_limit_seconds = 30       # produce every 1s...30s
# producer_random_upper_limit_seconds = 60 * 2   # produce every 1s...2min
# producer_random_upper_limit_seconds = 60 * 10  # produce every 1s...10min
producer_random_upper_limit_seconds = 60 * 30  # produce every 1s...30min

### init
producer = Producer({'bootstrap.servers': bootstrap_servers})

### register the Ctrl+C handler for graceful shutdown
def signal_handler(signal, frame):
    global producer

    print('Ctrl+C pressed. terminating gracefully')

    try:
        if producer:
            producer.flush()
            producer.close()
    except:
        pass

    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)


# global vars
total_produced = 0
total_delivered = 0
total_failed = 0

### functions
def delivery_report(err, msg):
    global total_delivered, total_failed

    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        total_failed = total_failed + 1
        print('Message delivery failed: %s | total failed: %d' % (err, total_failed))
    else:
        total_delivered = total_delivered + 1
        print(
            'Message delivered to %s [%d] | total delivered: %d' % (msg.topic(), msg.partition(), total_delivered))


### main
while True:
    producer.poll(0)

    key = str(random.getrandbits(128))  # put on a random partition
    msg = datetime.datetime.now().isoformat()

    producer.produce(topic, msg.encode('utf-8'), key, callback=delivery_report)

    total_produced = total_produced + 1
    logging.debug('kafka producer.produce on \'%s\' {msg=%s, key=%s} | produced=%d' % (topic, msg, key, total_produced))

    sleep_seconds = random.randint(1, producer_random_upper_limit_seconds + 1)

    logging.debug('sleep for %ds' % sleep_seconds)
    sleep(sleep_seconds)
