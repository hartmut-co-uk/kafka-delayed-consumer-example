# kafka-delayed-consumer-example
**Test/example (python) project of a client-side delayed kafka consumer approach based on kafka message timestamp & sleep.**

## !!! Caution -> Experimental
Please note the setup is to be considered **experimental** and not a production ready and battle tested strategy for a 
kafka consumer delay. 
So there's no guarantees for functionality or stability.

Though tests/simulations done have delivered promising results.

_Update: There's one known application which has been running in production for several months now performing as intended. As the author has moved on to another job after the project had been completed - there's no long term operational knowledge or further results and stats available. (Therefore should still be considered experimental <> in particular no guarantee for 100% 'exactly once' processing)._

## Approach 
As there's no support server-side (from kafka brokers) - the delay is enforced client side by pausing (sleep) the 
consumer based on the kafka message timestamp until the desired delayed time has come for the message to be processed. 

To keep brokers happy -> avoid hitting the message processing session timeout which would cause brokers to enforce a 
consumer group rebalancing - the process configures timeout, .. accordingly and sends periodic commits of current/last 
stored (local in-memory) partition offsets.

## Further Notes
This test setup was put together for a specific scenario to consume messages with a 15min delay.
Testing has taken place producing 
- high 
- medium 
- random very low (compared to desired delay) message rates 

to verify
1. performance under constant traffic / high load
2. behaviour of brokers in regards to session timeout, rebalancing => cause general non-conform usage of kafka =)
3. 'exactly once delivery' (as close to as possible)


## References
- https://github.com/confluentinc/confluent-kafka-python
- https://github.com/edenhill/librdkafka/wiki/FAQ#why-committing-each-message-is-slow
- https://github.com/confluentinc/confluent-kafka-python/issues/120#issuecomment-277682117
- https://github.com/confluentinc/confluent-kafka-python/issues/307
- https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
