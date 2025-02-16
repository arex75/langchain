from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import pika
import os
from dotenv import load_dotenv
import logging
# no no
# Load environment variables
load_dotenv()
# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# RabbitMQ configuration
RABBITMQ_USERNAME = os.getenv('RABBITMQ_USERNAME')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD')
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
RABBITMQ_PORT = int(os.getenv('RABBITMQ_PORT'))
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


def publish_to_rabbitmq(event_type, payload):
    """Publish the formatted webhook payload to RabbitMQ."""
    try:
        # Format the message
        formatted_message = format_github_feed(event_type, payload)

        # Connect to RabbitMQ
        credentials = pika.PlainCredentials(RABBITMQ_USERNAME, RABBITMQ_PASSWORD)
        parameters = pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=RABBITMQ_PORT,
            credentials=credentials,
            virtual_host='/',
            heartbeat=600,
            blocked_connection_timeout=300
        )

        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()

        # Declare queue
        channel.queue_declare(queue=QUEUE_NAME, durable=True)

        # Publish message
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


class WebhookHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        try:
            # Get content length and read data
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)

            # Parse JSON payload
            payload = json.loads(post_data.decode('utf-8'))

            # Get event type from headers
            event_type = self.headers.get('X-GitHub-Event')

            # Log the received webhook
            logger.info(f"Received webhook event: {event_type}")
            logger.debug(f"Payload: {json.dumps(payload, indent=2)}")

            # Publish to RabbitMQ
            if publish_to_rabbitmq(event_type, payload):
                logger.info("Successfully published to RabbitMQ")
            else:
                logger.error("Failed to publish to RabbitMQ")

            # Send response
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b"OK")

        except Exception as e:
            logger.error(f"Error processing webhook: {str(e)}")
            self.send_response(500)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b"Error processing webhook")


if __name__ == "__main__":
    try:
        server = HTTPServer(('localhost', 8080), WebhookHandler)
        logger.info("Server running on port 8080")
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down server...")
        server.socket.close()
