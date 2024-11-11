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
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# RabbitMQ Configuration
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', '86b9cd9e-2f65-45b4-86c5-b4f6f5b87512.hsvc.ir')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT', 30379))
RABBITMQ_USERNAME = os.getenv('RABBITMQ_USERNAME', 'rabbitmq')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD', '9jjJ6s73qfIR5VisD4TZ2Sv8cakEBBVZ')
RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE', 'git_data_test')
WEBHOOK_PORT = int(os.getenv('GITHUB_WEBHOOK_PORT', 8080))


def get_rabbitmq_connection():
    """Create and return a new RabbitMQ connection."""
    credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300
    )
    return pika.BlockingConnection(parameters)


def send_to_rabbitmq(message):
    """Send a message to the specified RabbitMQ queue."""
    try:
        connection = get_rabbitmq_connection()
        channel = connection.channel()

        # Declare queue
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)

        # Publish message
        channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_QUEUE,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
                content_type='application/json',
                timestamp=int(datetime.now(timezone.utc).timestamp())
            )
        )

        logger.info(f"Published message to queue: {message}")
        connection.close()
        return True
    except Exception as e:
        logger.error(f"Failed to publish message: {str(e)}")
        return False


app = Flask(__name__)


@app.route('/', methods=['GET'])
def home():
    return "GitHub Webhook Server is running!"


@app.route('/webhook', methods=['POST'])
def webhook():
    # Verify content type
    if request.content_type != 'application/json':
        logger.error("Received non-JSON payload")
        return jsonify({"status": "error", "message": "Content-Type must be application/json"}), 415

    # Get payload
    payload = request.get_json(silent=True)
    if not payload:
        logger.error("No JSON payload received")
        return jsonify({"status": "error", "message": "No JSON data found"}), 400

    try:
        event_type = request.headers.get('X-GitHub-Event', 'push')
        logger.info(f"Received {event_type} event")
        logger.debug(f"Payload: {json.dumps(payload, indent=2)}")

        # Format message based on event type
        if event_type == 'push':
            if 'commits' in payload:
                repository = payload.get('repository', {})
                commit_data = payload['commits'][-1]  # Get the latest commit

                message = {
                    "event_type": "push",
                    "repository": {
                        "name": repository.get('full_name'),
                        "description": repository.get('description'),
                        "url": repository.get('html_url'),
                        "language": repository.get('language'),
                        "visibility": repository.get('visibility')
                    },
                    "branch": payload.get('ref', '').replace('refs/heads/', ''),
                    "commit": {
                        "id": commit_data['id'][:7],
                        "message": commit_data['message'],
                        "timestamp": commit_data['timestamp'],
                        "url": commit_data['url'],
                        "author": {
                            "name": commit_data['author']['name'],
                            "email": commit_data['author']['email']
                        },
                        "changes": {
                            "added": commit_data['added'],
                            "modified": commit_data['modified'],
                            "removed": commit_data['removed']
                        }
                    }
                }

                # Send to RabbitMQ
                if send_to_rabbitmq(message):
                    return jsonify({
                        "status": "success",
                        "message": "Push event processed and sent to queue"
                    }), 200
                else:
                    return jsonify({
                        "status": "error",
                        "message": "Failed to send message to queue"
                    }), 500
            else:
                return jsonify({"status": "ignored", "message": "No commits found"}), 200

        else:
            # Handle other event types if needed
            return jsonify({"status": "ignored", "message": f"Event type {event_type} not processed"}), 200

    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    logger.info(f"Starting webhook server on port {WEBHOOK_PORT}")
    app.run(host="0.0.0.0", port=WEBHOOK_PORT)
