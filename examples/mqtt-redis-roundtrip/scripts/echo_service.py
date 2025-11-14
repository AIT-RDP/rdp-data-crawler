import redis
import json
import time
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Redis configuration
REDIS_HOST = 'redis2'
REDIS_PORT = 6379
INCOMING_STREAM = 'datacrawler2.incoming'
ECHO_STREAM = 'datacrawler2.outgoing'
GROUP_NAME = 'echo_service_group'
CONSUMER_NAME = 'echo_service'

def echo_service():
    """
    Listen to the incoming Redis stream and echo messages back to echo stream.
    """
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        r.ping()
        logger.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")

        # Create a consumer group if it doesn't exist
        try:
            r.xgroup_create(INCOMING_STREAM, GROUP_NAME, id='0', mkstream=True)
            logger.info(f"Created consumer group '{GROUP_NAME}' on stream '{INCOMING_STREAM}'")
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.info(f"Consumer group '{GROUP_NAME}' already exists")
            else:
                raise

        logger.info(f"Echo service started. Waiting for messages on stream '{INCOMING_STREAM}'...")

        while True:
            # Read from stream using a consumer group
            messages = r.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams={INCOMING_STREAM: '>'},
                count=1,
                block=1000  # Block for 1 second
            )

            for stream_name, stream_messages in messages:
                for message_id, message_data in stream_messages:
                    try:
                        logger.info(f"Received message {message_id}: {message_data}")

                        # Parse the message fields
                        parsed_data = {}
                        for key, value_json in message_data.items():
                            try:
                                parsed_data[key] = json.loads(value_json)
                            except json.JSONDecodeError:
                                parsed_data[key] = value_json

                        # Add echo metadata
                        echo_data = {
                            **parsed_data,
                            "echoed_at": datetime.utcnow().isoformat(),
                            "echo_service": "datacrawler2"
                        }

                        # Add to echo stream (encode values as JSON)
                        encoded_data = {key: json.dumps(val) for key, val in echo_data.items()}
                        echo_id = r.xadd(ECHO_STREAM, encoded_data)

                        logger.info(f"Echoed message to {ECHO_STREAM} with ID {echo_id}")

                    except Exception as e:
                        logger.error(f"Error processing message {message_id}: {e}")

    except redis.ConnectionError as e:
        logger.error(f"Redis connection error: {e}")
        logger.info("Retrying in 5 seconds...")
        time.sleep(5)
        echo_service()  # Retry
    except KeyboardInterrupt:
        logger.info("Echo service stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise

if __name__ == "__main__":
    echo_service()
