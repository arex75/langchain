import time

from flask import Flask, request, jsonify
import pika
import json
import os
from datetime import datetime, timezone
import logging
from dotenv import load_dotenv
import traceback

# Load environment variables
load_dotenv()

# Set up logging with more detailed format
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG for more detailed logs
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger(__name__)

# Add file handler for persistent logging
file_handler = logging.FileHandler('webhook.log')
file_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)


def format_github_feed(event_type, payload):
    """Format GitHub webhook payload into a feed based on event type"""
    logger.debug(f"Formatting GitHub feed for event type: {event_type}")
    logger.debug(f"Original payload: {json.dumps(payload, indent=2)}")

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
        "timestamp": None
    }

    try:
        if event_type == 'push':
            commit = payload.get('head_commit', {})
            if not commit:
                logger.warning("No head_commit found in push event")

            base_feed.update({
                "action": "pushed",
                "branch": payload.get('ref', '').replace('refs/heads/', ''),
                "commit": {
                    "id": commit.get('id', '')[:7] if commit.get('id') else '',
                    "message": commit.get('message'),
                    "timestamp": commit.get('timestamp'),
                    "author": {
                        "name": commit.get('author', {}).get('name'),
                        "email": commit.get('author', {}).get('email')
                    },
                    "changes": {
                        "added": commit.get('added', []),
                        "modified": commit.get('modified', []),
                        "removed": commit.get('removed', [])
                    }
                },
                "compare_url": payload.get('compare')
            })
            base_feed["timestamp"] = commit.get('timestamp')

        elif event_type == 'pull_request':
            pr = payload.get('pull_request', {})
            if not pr:
                logger.warning("No pull_request data found in event")

            base_feed.update({
                "action": payload.get('action'),
                "pull_request": {
                    "number": pr.get('number'),
                    "title": pr.get('title'),
                    "body": pr.get('body'),
                    "state": pr.get('state'),
                    "merged": pr.get('merged', False),
                    "base_branch": pr.get('base', {}).get('ref'),
                    "head_branch": pr.get('head', {}).get('ref'),
                    "author": {
                        "username": pr.get('user', {}).get('login'),
                        "avatar_url": pr.get('user', {}).get('avatar_url')
                    }
                },
                "html_url": pr.get('html_url')
            })
            base_feed["timestamp"] = pr.get('updated_at')

        elif event_type == 'issues':
            issue = payload.get('issue', {})
            if not issue:
                logger.warning("No issue data found in event")

            base_feed.update({
                "action": payload.get('action'),
                "issue": {
                    "number": issue.get('number'),
                    "title": issue.get('title'),
                    "body": issue.get('body'),
                    "state": issue.get('state'),
                    "author": {
                        "username": issue.get('user', {}).get('login'),
                        "avatar_url": issue.get('user', {}).get('avatar_url')
                    },
                    "labels": [label.get('name') for label in issue.get('labels', [])]
                },
                "html_url": issue.get('html_url')
            })
            base_feed["timestamp"] = issue.get('updated_at')

        logger.debug(f"Formatted feed: {json.dumps(base_feed, indent=2)}")
        return base_feed

    except Exception as e:
        logger.error(f"Error formatting GitHub feed: {str(e)}")
        logger.error(traceback.format_exc())
        raise


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
            # Enable delivery confirmations
            self.channel.confirm_delivery()
            logger.info("Successfully connected to RabbitMQ")
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {str(e)}")
            logger.error(traceback.format_exc())
            raise

    def declare_queue(self, queue_name=None):
        try:
            queue_name = queue_name or self.queue_name

            try:
                # First try to check if queue exists
                self.channel.queue_declare(queue=queue_name, passive=True)
                logger.info(f"Queue '{queue_name}' already exists")
            except (pika.exceptions.ChannelClosedByBroker, pika.exceptions.AMQPChannelError):
                # If queue doesn't exist or there was an error, create it with our settings
                logger.info(f"Creating new queue '{queue_name}'")
                # Reconnect since the channel was closed
                self._connect()
                result = self.channel.queue_declare(
                    queue=queue_name,
                    durable=True,
                    # Remove the TTL argument since it's causing issues
                    # arguments={'x-message-ttl': 86400000}
                )
                logger.info(f"Queue '{queue_name}' declared. Message count: {result.method.message_count}")

        except Exception as e:
            logger.error(f"Failed to declare queue: {str(e)}")
            logger.error(traceback.format_exc())
            # Attempt to reconnect and declare without any special arguments
            try:
                self._connect()
                self.channel.queue_declare(queue=queue_name, durable=True)
                logger.info(f"Queue '{queue_name}' declared with basic settings")
            except Exception as inner_e:
                logger.error(f"Failed to declare queue with basic settings: {str(inner_e)}")
                raise

    def publish_message(self, message, queue_name=None):
        try:
            queue_name = queue_name or self.queue_name
            logger.debug(f"Publishing message to queue: {queue_name}")

            if not self.connection or self.connection.is_closed:
                logger.warning("Connection is closed. Reconnecting...")
                self._connect()

            # Convert message to JSON string
            message_body = json.dumps(message)
            logger.debug(f"Message body prepared: {message_body[:200]}...")  # Log first 200 chars

            # Prepare message properties
            properties = pika.BasicProperties(
                delivery_mode=2,  # Makes the message persistent
                content_type='application/json',
                timestamp=int(datetime.now(timezone.utc).timestamp())
            )
            logger.debug("Message properties prepared")

            # Publish the message
            logger.debug("Attempting to publish message...")
            self.channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=message_body,
                properties=properties
            )
            logger.info(f"Successfully published message to queue '{queue_name}'")

            # Verify queue message count after publish
            try:
                queue_info = self.channel.queue_declare(queue=queue_name, passive=True)
                logger.info(f"Current message count in queue '{queue_name}': {queue_info.method.message_count}")
            except Exception as e:
                logger.warning(f"Could not verify message count: {str(e)}")

            return True

        except Exception as e:
            logger.error(f"Failed to publish message: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logger.info("RabbitMQ connection closed")


app = Flask(__name__)


# Add request logging
@app.before_request
def log_request_info():
    """Log details about every request"""
    logger.debug('Headers: %s', dict(request.headers))
    logger.debug('Body: %s', request.get_data())


@app.after_request
def log_response_info(response):
    """Log details about every response"""
    logger.debug('Response: %s', response.get_data())
    return response


# Initialize RabbitMQ service
rabbitmq_service = RabbitMQService(
    host=os.getenv('RABBITMQ_HOST', '86b9cd9e-2f65-45b4-86c5-b4f6f5b87512.hsvc.ir'),
    port=int(os.getenv('RABBITMQ_PORT', 30379)),
    username=os.getenv('RABBITMQ_USERNAME', 'rabbitmq'),
    password=os.getenv('RABBITMQ_PASSWORD', '9jjJ6s73qfIR5VisD4TZ2Sv8cakEBBVZ'),
    queue_name=os.getenv('RABBITMQ_QUEUE', 'git_data_ingestion')
)

# Declare the queue at startup
rabbitmq_service.declare_queue()


@app.route('/', methods=['GET', 'POST'])
@app.route('/webhook', methods=['POST'])
def webhook():
    logger.debug("----------------------------------------")
    logger.debug("New webhook request received")
    logger.debug(f"Request method: {request.method}")
    logger.debug(f"Request headers: {dict(request.headers)}")

    if request.method == 'GET':
        logger.debug("GET request received, returning status")
        return "Webhook server is running"

    try:
        # Log raw request data
        logger.debug(f"Request content type: {request.content_type}")
        logger.debug(f"Raw request data: {request.get_data(as_text=True)}")

        if not request.is_json:
            logger.error("Request is not JSON")
            return jsonify({"status": "error", "message": "Content-Type must be application/json"}), 400

        payload = request.json
        event_type = request.headers.get('X-GitHub-Event', 'push')

        logger.info(f"Processing webhook event: {event_type}")
        logger.debug(f"Full payload: {json.dumps(payload, indent=2)}")

        # Validate payload
        if not payload:
            logger.error("Empty payload received")
            return jsonify({"status": "error", "message": "Empty payload"}), 400

        if not isinstance(payload, dict):
            logger.error(f"Invalid payload type: {type(payload)}")
            return jsonify({"status": "error", "message": "Invalid payload format"}), 400

        try:
            logger.debug("Formatting GitHub feed")
            formatted_message = format_github_feed(event_type, payload)
            logger.info("Successfully formatted GitHub feed")
            logger.debug(f"Formatted message: {json.dumps(formatted_message, indent=2)}")
        except Exception as format_error:
            logger.error(f"Error formatting message: {str(format_error)}")
            logger.error(traceback.format_exc())
            return jsonify({"status": "error", "message": "Error formatting webhook data"}), 500

        # Publish with retry logic
        max_retries = 3
        for attempt in range(max_retries):
            logger.debug(f"Attempting to publish message (attempt {attempt + 1}/{max_retries})")
            try:
                # Verify RabbitMQ connection before publishing
                if not rabbitmq_service.connection or rabbitmq_service.connection.is_closed:
                    logger.warning("RabbitMQ connection is closed, reconnecting...")
                    rabbitmq_service._connect()

                # Verify channel is open
                if rabbitmq_service.channel.is_closed:
                    logger.warning("RabbitMQ channel is closed, creating new channel")
                    rabbitmq_service.channel = rabbitmq_service.connection.channel()

                # Attempt to publish
                success = rabbitmq_service.publish_message(formatted_message)

                if success:
                    logger.info(f"Successfully published message on attempt {attempt + 1}")
                    response = {
                        "status": "success",
                        "message": f"Event {event_type} published to RabbitMQ",
                        "attempt": attempt + 1
                    }
                    logger.debug(f"Sending response: {json.dumps(response, indent=2)}")
                    return jsonify(response), 200
                else:
                    logger.warning(f"Failed to publish message, attempt {attempt + 1} of {max_retries}")
                    if attempt < max_retries - 1:
                        logger.debug("Waiting 1 second before retry")
                        time.sleep(1)

            except Exception as publish_error:
                logger.error(f"Error during publish attempt {attempt + 1}: {str(publish_error)}")
                logger.error(traceback.format_exc())
                if attempt < max_retries - 1:
                    logger.debug("Waiting 1 second before retry")
                    time.sleep(1)

        logger.error("Failed to publish message after all retry attempts")
        return jsonify({
            "status": "error",
            "message": f"Failed to publish to RabbitMQ after {max_retries} attempts"
        }), 500

    except Exception as e:
        logger.error(f"Unexpected error in webhook handler: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500
    finally:
        logger.debug("----------------------------------------")


@app.teardown_appcontext
def cleanup(exception=None):
    rabbitmq_service.close()


if __name__ == '__main__':
    port = int(os.getenv('PORT', 5001))
    logger.info(f"Starting webhook server on port {port}")
    app.run(host='0.0.0.0', port=port, debug=True)
