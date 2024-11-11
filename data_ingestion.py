from flask import Flask, request, jsonify
import pika
import json
import os
from datetime import datetime, timezone
import logging
from dotenv import load_dotenv
import traceback
import sys

# Load environment variables
load_dotenv()

# Enhanced logging setup
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG level
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler('webhook.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def format_github_feed(event_type, payload):
    """Format GitHub webhook payload into a feed based on event type"""
    logger.info(f"Formatting GitHub feed for event type: {event_type}")
    logger.debug(f"Payload to format: {json.dumps(payload, indent=2)}")

    # Your existing format_github_feed code remains the same
    base_feed = {
        "event_type": event_type,
        "repository": {
            "name": payload.get('repository', {}).get('full_name'),
            "description": payload.get('repository', {}).get('description'),
            "language": payload.get('repository', {}).get('language'),
            "visibility": payload.get('repository', {}).get('visibility')
        },
        "sender": {
            "username": payload.get('sender', {}).get('login'),
            "avatar_url": payload.get('sender', {}).get('avatar_url')
        },
        "timestamp": datetime.now().isoformat()
    }

    # Rest of your format_github_feed function...

    logger.info("Successfully formatted GitHub feed")
    logger.debug(f"Formatted message: {json.dumps(base_feed, indent=2)}")
    return base_feed


class RabbitMQService:
    def __init__(self, host, port, username, password, queue_name='default_queue'):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.queue_name = queue_name
        self._connect()

    def _connect(self):
        try:
            logger.info(f"Connecting to RabbitMQ at {self.host}:{self.port}")
            credentials = pika.PlainCredentials(self.username, self.password)
            connection_params = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                credentials=credentials,
                heartbeat=600,
                blocked_connection_timeout=300
            )
            self.connection = pika.BlockingConnection(connection_params)
            self.channel = self.connection.channel()
            logger.info("Successfully connected to RabbitMQ")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def publish_message(self, message, queue_name=None):
        try:
            queue_name = queue_name or self.queue_name
            logger.info(f"Attempting to publish message to queue: {queue_name}")

            # Reconnect if needed
            if not self.connection or self.connection.is_closed:
                logger.warning("Connection is closed. Reconnecting...")
                self._connect()

            # Prepare message
            message_body = json.dumps(message)
            logger.debug(f"Prepared message body: {message_body[:200]}...")

            # Publish message
            self.channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type='application/json',
                    timestamp=int(datetime.now(timezone.utc).timestamp())
                )
            )

            # Verify message was published
            queue_info = self.channel.queue_declare(queue=queue_name, passive=True)
            logger.info(f"Message published. Current queue size: {queue_info.method.message_count}")
            return True

        except Exception as e:
            logger.error(f"Failed to publish message: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    def declare_queue(self, queue_name=None):
        try:
            queue_name = queue_name or self.queue_name

            try:
                # Check if queue exists
                self.channel.queue_declare(queue=queue_name, passive=True)
                logger.info(f"Queue '{queue_name}' already exists")
            except:
                # Queue doesn't exist, create it
                logger.info(f"Creating new queue '{queue_name}'")
                self._connect()
                result = self.channel.queue_declare(queue=queue_name, durable=True)
                logger.info(f"Queue '{queue_name}' declared. Message count: {result.method.message_count}")

        except Exception as e:
            logger.error(f"Failed to declare queue: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logger.info("RabbitMQ connection closed")


app = Flask(__name__)


# Log all requests
@app.before_request
def log_request():
    logger.info(f"Received {request.method} request to {request.url}")
    logger.debug(f"Headers: {dict(request.headers)}")
    if request.data:
        logger.debug(f"Request data: {request.get_data(as_text=True)}")


@app.route('/', methods=['GET', 'POST'])
@app.route('/webhook', methods=['POST'])
def webhook():
    if request.method == 'GET':
        return "Webhook server is running"

    try:
        logger.info("Processing webhook request")

        # Validate request
        if not request.is_json:
            logger.error("Received non-JSON payload")
            return jsonify({"status": "error", "message": "Content-Type must be application/json"}), 400

        payload = request.json
        event_type = request.headers.get('X-GitHub-Event', 'push')

        logger.info(f"Received webhook event: {event_type}")
        logger.debug(f"Webhook payload: {json.dumps(payload, indent=2)}")

        # Format message
        formatted_message = format_github_feed(event_type, payload)
        logger.info("Message formatted successfully")

        # Publish message
        success = rabbitmq_service.publish_message(formatted_message)

        if success:
            logger.info(f"Successfully published {event_type} event to RabbitMQ")
            return jsonify({
                "status": "success",
                "message": f"Event {event_type} published to RabbitMQ"
            }), 200
        else:
            logger.error("Failed to publish message to RabbitMQ")
            return jsonify({
                "status": "error",
                "message": "Failed to publish to RabbitMQ"
            }), 500

    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500


if __name__ == '__main__':
    # Initialize RabbitMQ service
    rabbitmq_service = RabbitMQService(
        host=os.getenv('RABBITMQ_HOST', '86b9cd9e-2f65-45b4-86c5-b4f6f5b87512.hsvc.ir'),
        port=int(os.getenv('RABBITMQ_PORT', 30379)),
        username=os.getenv('RABBITMQ_USERNAME', 'rabbitmq'),
        password=os.getenv('RABBITMQ_PASSWORD', '9jjJ6s73qfIR5VisD4TZ2Sv8cakEBBVZ'),
        queue_name=os.getenv('RABBITMQ_QUEUE', 'git_data_test')
    )

    # Setup queue
    rabbitmq_service.declare_queue()

    # Start server on port 8080 to match ngrok
    port = int(os.getenv('PORT', 8080))  # Changed default port to 8080
    logger.info(f"Starting webhook server on port {port}")
    app.run(host='0.0.0.0', port=port, debug=True)
