from flask import Flask, request, jsonify
import pika
import json
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

app = Flask(__name__)

# Load RabbitMQ connection details from environment variables
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST')
RABBITMQ_PORT = os.getenv('RABBITMQ_PORT')
RABBITMQ_USERNAME = os.getenv('RABBITMQ_USERNAME')
RABBITMQ_PASSWORD = os.getenv('RABBITMQ_PASSWORD')
RABBITMQ_QUEUE = os.getenv('RABBITMQ_QUEUE')
WEBHOOK_PORT = int(os.getenv('GITHUB_WEBHOOK_PORT', 8080))


def get_rabbitmq_url():
    """Construct the RabbitMQ connection URL."""
    return (
        f"amqp://{RABBITMQ_USERNAME}:{RABBITMQ_PASSWORD}"
        f"@{RABBITMQ_HOST}:{RABBITMQ_PORT}/"
    )


def send_to_rabbitmq(message):
    """Send a message to the specified RabbitMQ queue."""
    rabbitmq_url = get_rabbitmq_url()
    connection = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
    channel.basic_publish(
        exchange='',
        routing_key=RABBITMQ_QUEUE,
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2,  # Make message persistent
        )
    )
    connection.close()
    print(f"Message sent to RabbitMQ: {message}")


@app.route('/github-webhook', methods=['GET', 'POST'])
def github_webhook():
    # Check if Content-Type is application/json
    if request.content_type != 'application/json':
        return jsonify({"status": "error", "message": "Unsupported Media Type"}), 415

    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({"status": "error", "message": "No JSON data found"}), 400

    if 'commits' in payload:
        # Get repository details
        repository = payload.get('repository', {})
        org_name = repository.get('owner', {}).get('login', 'unknown')
        repo_name = repository.get('name', 'unknown')
        repo_url = repository.get('html_url', 'unknown')

        # Extract the branch name from the ref field
        ref = payload.get('ref', '')
        branch_name = ref.split('/')[-1] if ref else 'unknown'

        # Get the latest commit
        commit_data = payload['commits'][-1]
        message = {
            "org_name": org_name,
            "repo_name": repo_name,
            "repo_url": repo_url,
            "branch_name": branch_name,
            "commit_id": commit_data['id'],
            "commit_message": commit_data['message'],
            "timestamp": commit_data['timestamp'],
            "commit_url": commit_data['url'],
            "author": commit_data['author']['name'],
            "added_files": commit_data['added'],
            "modified_files": commit_data['modified'],
            "removed_files": commit_data['removed']
        }

        # Send the commit message to RabbitMQ
        send_to_rabbitmq(message)

        return jsonify({"status": "success", "message": "Commit message sent to RabbitMQ"}), 200
    else:
        return jsonify({"status": "ignored", "message": "No commits found"}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=WEBHOOK_PORT)
