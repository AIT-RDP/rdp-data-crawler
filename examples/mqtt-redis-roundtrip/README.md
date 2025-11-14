# MQTT-Redis Round-trip Example

This example demonstrates a complete round-trip data flow between two data crawler instances using Redis Streams and MQTT.

## Architecture

The example implements the following data flow:

```
Test Script -> Redis1 -> Datacrawler1 -> MQTT -> Datacrawler2 -> Redis2 -> Echo Script
     ↑                                                                           |
     |                                                                           |
     └──────── Redis1 <- Datacrawler1 <- MQTT <- Datacrawler2 <- Redis2 <────────┘
```

The complete round-trip follows these steps:

1. Test script adds message to Redis Stream `datacrawler1.outgoing` on redis1
2. Datacrawler1 reads from Redis Stream and publishes to MQTT topic `datacrawler1/request`
3. Datacrawler2 subscribes to MQTT topic and writes to Redis Stream `datacrawler2.incoming` on redis2
4. Echo service reads from Redis Stream `datacrawler2.incoming` and writes to `datacrawler2.outgoing`
5. Datacrawler2 reads from Redis Stream `datacrawler2.outgoing` and publishes to MQTT topic `datacrawler2/response`
6. Datacrawler1 subscribes to MQTT topic and writes to Redis Stream `datacrawler1.incoming` on redis1
7. Test script verifies message received from Redis Stream `datacrawler1.incoming`


### Components

1. **Datacrawler Instance 1**
   - **Flow 1 (Outgoing)**: Redis Stream Source -> MQTT Sink
     - Source: Reads from Redis Stream `datacrawler1.outgoing` on redis1
     - Sink: Publishes to MQTT topic `datacrawler1/request`
   - **Flow 2 (Incoming)**: MQTT Source -> Redis Stream Sink
     - Source: Subscribes to MQTT topic `datacrawler2/response`
     - Sink: Writes to Redis Stream `datacrawler1.incoming` on redis1

2. **Datacrawler Instance 2** 
   - **Flow 1 (Outgoing)**: Redis Stream Source -> MQTT Sink
     - Source: Reads from Redis Stream `datacrawler2.outgoing` on redis2
     - Sink: Publishes to MQTT topic `datacrawler2/response`
   - **Flow 2 (Incoming)**: MQTT Source -> Redis Stream Sink
     - Source: Subscribes to MQTT topic `datacrawler1/request`
     - Sink: Writes to Redis Stream `datacrawler2.incoming` on redis2

3. **Echo Service** 
   - Consumes messages from Redis Stream `datacrawler2.incoming` on redis2
   - Echoes messages to Redis Stream `datacrawler2.outgoing` on redis2
   - Adds metadata

4. **Test Script** 
   - Publishes test messages to Redis Stream `datacrawler1.outgoing` on redis1
   - Consumes from Redis Stream `datacrawler1.incoming` on redis1
   - Verifies round-trip completion

5. **Supporting Services**
   - **MQTT Broker** (mosquitto): Eclipse Mosquitto broker for MQTT communication
   - **Redis1**: Redis instance for datacrawler1
   - **Redis2**: Redis instance for datacrawler2

## Run the example
```bash
docker compose up
```

This starts all services and endlessly runs the test script. The first test may take longer as the mqtt service needs to connect first.

### Check MQTT Messages
Connect to the MQTT broker to monitor messages.
```bash
docker exec -it mqtt-broker mosquitto_sub -t '#' -v
```

This should show: 

```bash
datacrawler1/request {"id":"test-1-4ffb76bc","value":42.5,"timestamp":"2025-11-13T17:52:26.081939","test":"roundtrip"}
datacrawler2/response {"id":"test-1-4ffb76bc","value":42.5,"timestamp":"2025-11-13T17:52:26.081939","test":"roundtrip","echoed_at":"2025-11-13T17:52:36.137227","echo_service":"datacrawler2"}
datacrawler1/request {"id":"test-2-f8348e35","value":100.0,"timestamp":"2025-11-13T17:52:47.335736","test":"roundtrip"}
datacrawler2/response {"id":"test-2-f8348e35","value":100.0,"timestamp":"2025-11-13T17:52:47.335736","test":"roundtrip","echoed_at":"2025-11-13T17:52:47.339108","echo_service":"datacrawler2"}
datacrawler1/request {"id":"test-3-1040b527","value":-15.3,"timestamp":"2025-11-13T17:52:49.343833","test":"roundtrip"}
datacrawler2/response {"id":"test-3-1040b527","value":-15.3,"timestamp":"2025-11-13T17:52:49.343833","test":"roundtrip","echoed_at":"2025-11-13T17:52:49.348373","echo_service":"datacrawler2"}
datacrawler1/request {"id":"test-4-f93255de","value":0.0,"timestamp":"2025-11-13T17:52:51.355950","test":"roundtrip"}
datacrawler2/response {"id":"test-4-f93255de","value":0.0,"timestamp":"2025-11-13T17:52:51.355950","test":"roundtrip","echoed_at":"2025-11-13T17:52:51.360600","echo_service":"datacrawler2"}
datacrawler1/request {"id":"test-5-de0cd510","value":999.999,"timestamp":"2025-11-13T17:52:53.364454","test":"roundtrip"}
datacrawler2/response {"id":"test-5-de0cd510","value":999.999,"timestamp":"2025-11-13T17:52:53.364454","test":"roundtrip","echoed_at":"2025-11-13T17:52:53.368330","echo_service":"datacrawler2"}
```

### Check Redis Streams
Connect to Redis instances to monitor streams:
```bash
# Redis1
docker exec -it redis1 redis-cli XREAD COUNT 10 STREAMS datacrawler1.outgoing datacrawler1.incoming 0-0 0-0

# Redis2
docker exec -it redis2 redis-cli XREAD COUNT 10 STREAMS datacrawler2.incoming datacrawler2.outgoing 0-0 0-0
```