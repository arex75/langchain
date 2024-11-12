import os
from flask import Flask, request, jsonify
import pika
import hmac
import hashlib
import json
import logging
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration from environment variables
GITHUB_SECRET = os.getenv('GITHUB_WEBHOOK_SECRET')
RABBITMQ_USERNAME = os.getenv('RABBITMQ_USERNAME')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD')
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
RABBITMQ_PORT = os.getenv('RABBITMQ_PORT')
QUEUE_NAME = os.getenv('RABBITMQ_QUEUE', 'git_data_test')


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


def get_rabbitmq_connection():
    """Create a connection to RabbitMQ using environment credentials."""
    credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=int(RABBITMQ_PORT),
        credentials=credentials,
        virtual_host='/',
        heartbeat=600,
        blocked_connection_timeout=300
    )
    return pika.BlockingConnection(parameters)


def verify_signature(payload_body, signature_header):
    """Verify that the webhook is from GitHub using the secret token."""
    if not signature_header:
        logger.warning("No signature header present in request")
        return False

    sha_name, signature = signature_header.split('=')
    if sha_name != 'sha256':
        logger.warning(f"Unexpected hash algorithm: {sha_name}")
        return False

    mac = hmac.new(GITHUB_SECRET.encode(), msg=payload_body, digestmod=hashlib.sha256)
    return hmac.compare_digest(mac.hexdigest(), signature)


def publish_to_rabbitmq(event_type, payload):
    """Publish the formatted webhook payload to RabbitMQ."""
    try:
        # Format the message
        formatted_message = format_github_feed(event_type, payload)

        # Connect to RabbitMQ
        connection = get_rabbitmq_connection()
        channel = connection.channel()

        # Declare the queue
        channel.queue_declare(queue=QUEUE_NAME, durable=True)

        # Publish formatted message
        channel.basic_publish(
            exchange='',
            routing_key=QUEUE_NAME,
            body=json.dumps(formatted_message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make message persistent
                content_type='application/json'
            )
        )

        connection.close()
        logger.info(f"Successfully published {event_type} event to queue {QUEUE_NAME}")
        return True
    except Exception as e:
        logger.error(f"Error publishing to RabbitMQ: {str(e)}")
        return False


@app.route('/webhook', methods=['POST'])
def webhook():
    # Verify signature
    signature = request.headers.get('X-Hub-Signature-256')
    if not verify_signature(request.data, signature):
        logger.warning("Invalid webhook signature")
        return jsonify({'error': 'Invalid signature'}), 401

    # Get the event type
    event_type = request.headers.get('X-GitHub-Event')
    if not event_type:
        logger.warning("No event type provided in webhook")
        return jsonify({'error': 'No event type provided'}), 400

    # Get payload
    payload = request.json

    logger.info(f"Received GitHub webhook event: {event_type}")
    logger.debug(f"Payload: {json.dumps(payload, indent=2)}")

    # Publish to RabbitMQ
    if publish_to_rabbitmq(event_type, payload):
        return jsonify({
            'status': 'success',
            'message': f'Event {event_type} published to queue {QUEUE_NAME}'
        }), 200
    else:
        return jsonify({'error': 'Failed to publish to queue'}), 500


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint to verify the service is running."""
    try:
        # Test RabbitMQ connection
        connection = get_rabbitmq_connection()
        connection.close()
        return jsonify({
            'status': 'healthy',
            'rabbitmq_connection': 'success'
        }), 200
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return jsonify({
            'status': 'unhealthy',
            'rabbitmq_connection': 'failed',
            'error': str(e)
        }), 500


if __name__ == '__main__':
    # Verify all required environment variables are set
    required_vars = [
        'RABBITMQ_USERNAME', 'RABBITMQ_PASSWORD', 'RABBITMQ_HOST',
        'RABBITMQ_PORT', 'GITHUB_WEBHOOK_SECRET'
    ]

    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        exit(1)

    logger.info(f"Starting webhook server... Queue: {QUEUE_NAME}")
    app.run(host='0.0.0.0', port=5000, debug=True)
