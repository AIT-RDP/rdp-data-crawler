import redis
import json
import time
import logging
from datetime import datetime
import uuid
import sys

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Redis configuration
REDIS_HOST = 'redis1'
REDIS_PORT = 6379
OUTGOING_STREAM = 'datacrawler1.outgoing'
INCOMING_STREAM = 'datacrawler1.incoming'
GROUP_NAME = 'test_script_group'
CONSUMER_NAME = 'test_script'

# Test configuration
TEST_TIMEOUT = 30
TEST_COUNT = 5

class RoundTripTester:
    def __init__(self):
        self.redis_client = None
        self.last_id = '0-0'  # Track last read message ID

    def connect(self):
        """Connect to Redis"""
        try:
            self.redis_client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                decode_responses=True
            )
            self.redis_client.ping()
            logger.info(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
            return True
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            return False

    def start_listener(self):
        """Set up consumer group for incoming stream"""
        try:
            self.redis_client.xgroup_create(INCOMING_STREAM, GROUP_NAME, id='0', mkstream=True)
            logger.info(f"Created consumer group '{GROUP_NAME}' on stream '{INCOMING_STREAM}'")
        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.info(f"Consumer group '{GROUP_NAME}' already exists")
            else:
                raise

    def send_message(self, message_id, value):
        """Send a test message to the outgoing stream"""
        message = {
            "id": message_id,
            "value": value,
            "timestamp": datetime.utcnow().isoformat(),
            "test": "roundtrip"
        }

        # Encode all values as JSON strings for Redis Stream
        encoded_message = {key: json.dumps(val) for key, val in message.items()}
        stream_id = self.redis_client.xadd(OUTGOING_STREAM, encoded_message)
        logger.info(f"Sent message {message_id} to stream {OUTGOING_STREAM} with ID {stream_id}")
        return message

    def wait_for_response(self, message_id, timeout=TEST_TIMEOUT):
        """Wait for a response message with matching ID"""
        start_time = time.time()

        while time.time() - start_time < timeout:
            # Read from incoming stream using a consumer group
            messages = self.redis_client.xreadgroup(
                groupname=GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams={INCOMING_STREAM: '>'},
                count=1,
                block=1000  # Block for 1 second
            )

            for stream_name, stream_messages in messages:
                for stream_id, message_data in stream_messages:
                    try:
                        logger.info(f"Received response {stream_id}: {message_data}")

                        parsed_data = {}
                        for key, value_json in message_data.items():
                            try:
                                parsed_data[key] = json.loads(value_json)
                            except json.JSONDecodeError:
                                parsed_data[key] = value_json

                        # Check if this is our message
                        if parsed_data.get('id') == message_id:
                            return parsed_data

                    except Exception as e:
                        logger.error(f"Error processing message {stream_id}: {e}")

        return None

    def verify_roundtrip(self, sent_message, received_message):
        """Verify that the received message matches the sent message"""
        if not received_message:
            logger.error("No message received!")
            return False

        checks = {
            "id": sent_message['id'] == received_message.get('id'),
            "value": sent_message['value'] == received_message.get('value'),
            "echo_metadata": 'echoed_at' in received_message,
            "echo_service": received_message.get('echo_service') == 'datacrawler2'
        }

        all_passed = all(checks.values())

        if all_passed:
            logger.info(f"Round-trip successful for message {sent_message['id']}")
        else:
            logger.error(f"Round-trip verification failed!")
            for check_name, passed in checks.items():
                logger.error(f"{check_name}")

        return all_passed

    def run_test(self, test_number, value):
        """Run a single round-trip test"""
        logger.info("-" * 80)
        logger.info(f"Starting Test #{test_number}")

        message_id = f"test-{test_number}-{uuid.uuid4().hex[:8]}"

        # Send message
        sent_message = self.send_message(message_id, value)

        # Wait for response
        logger.info(f"Waiting for response (timeout: {TEST_TIMEOUT}s)...")
        received_message = self.wait_for_response(message_id)

        # Verify
        success = self.verify_roundtrip(sent_message, received_message)

        return success

    def run_all_tests(self):
        if not self.connect():
            logger.error("Failed to connect to Redis. Exiting.")
            return False

        self.start_listener()

        # Run tests
        test_values = [
            42.5,
            100.0,
            -15.3,
            0.0,
            999.999
        ]

        results = []
        for i, value in enumerate(test_values, 1):
            success = self.run_test(i, value)
            results.append(success)

            # Wait between tests
            if i < len(test_values):
                time.sleep(2)

        passed = sum(results)
        total  = len(results)
        logger.info(f"Passed: {passed}/{total}")

        return  passed == total

def main():
    tester = RoundTripTester()
    success = tester.run_all_tests()

    # Exit with the appropriate code
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
