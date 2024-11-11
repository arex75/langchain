from flask import Flask, request, jsonify
import pika
import json
import os
from datetime import datetime, timezone
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG for more visibility
    format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s',
    handlers=[
        logging.FileHandler('webhook.log'),
        logging.StreamHandler()  # Add console output
    ]
)
logger = logging.getLogger(__name__)


def format_github_feed(event_type, payload):
    """Format GitHub webhook payload into a feed based on event type"""
    logger.debug(f"Formatting GitHub feed for event type: {event_type}")

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

    if event_type == 'push':
        commit = payload.get('head_commit', {})
        base_feed.update({
            "action": "pushed",
            "branch": payload.get('ref', '').replace('refs/heads/', ''),
            "commit": {
                "id": commit.get('id', '')[:7],
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
            raise

    def declare_queue(self, queue_name=None):
        try:
            queue_name = queue_name or self.queue_name
            self.channel.queue_declare(queue=queue_name, durable=True)
            logger.info(f"Queue declared: {queue_name}")
        except Exception as e:
            logger.error(f"Failed to declare queue: {str(e)}")
            self._connect()
            self.channel.queue_declare(queue=queue_name, durable=True)

    def publish_message(self, message, queue_name=None):
        try:
            queue_name = queue_name or self.queue_name
            logger.debug(f"Attempting to publish message to {queue_name}")

            if not self.connection or self.connection.is_closed:
                logger.warning("Connection is closed. Reconnecting...")
                self._connect()

            message_body = json.dumps(message)
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
            logger.info(f"Message published to queue '{queue_name}'")
            return True
        except Exception as e:
            logger.error(f"Failed to publish message: {str(e)}")
            return False

    def close(self):
        if self.connection and not self.connection.is_closed:
            self.connection.close()
            logger.info("RabbitMQ connection closed")


app = Flask(__name__)


# Add request logging middleware
@app.before_request
def log_request_info():
    logger.debug("------- New Request -------")
    logger.debug(f"Request Method: {request.method}")
    logger.debug(f"Request URL: {request.url}")
    logger.debug(f"Headers: {dict(request.headers)}")
    if request.data:
        logger.debug(f"Request Data: {request.get_data(as_text=True)}")


@app.after_request
def log_response_info(response):
    logger.debug(f"Response Status: {response.status}")
    logger.debug("-------------------------")
    return response


@app.route('/test', methods=['GET'])
def test():
    return "Webhook server is running!"


@app.route('/', methods=['GET', 'POST'])
@app.route('/webhook', methods=['POST'])
def webhook():
    logger.info("Webhook endpoint called")

    if request.method == 'GET':
        return "Webhook server is running"

    try:
        if not request.is_json:
            logger.error("Received non-JSON payload")
            return jsonify({"status": "error", "message": "Content-Type must be application/json"}), 400

        payload = request.json
        event_type = request.headers.get('X-GitHub-Event', 'push')

        logger.info(f"Received webhook event: {event_type}")
        logger.debug(f"Payload: {json.dumps(payload, indent=2)}")

        formatted_message = format_github_feed(event_type, payload)
        success = rabbitmq_service.publish_message(formatted_message)

        if success:
            return jsonify({
                "status": "success",
                "message": f"Event {event_type} published to RabbitMQ"
            }), 200
        else:
            return jsonify({
                "status": "error",
                "message": "Failed to publish to RabbitMQ"
            }), 500

    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
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

    # Declare the queue at startup
    rabbitmq_service.declare_queue()

    # Start the Flask app
    port = int(os.getenv('PORT', 8080))
    logger.info(f"Starting webhook server on port {port}")
    logger.info(f"Server will be accessible at http://localhost:{port}")
    app.run(host='0.0.0.0', port=port, debug=True)