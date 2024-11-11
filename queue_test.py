from dotenv import load_dotenv
import os
import logging
# Import the RabbitMQ service class
from data_ingestion import RabbitMQService

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


def test_queue():
    try:
        # Create RabbitMQ service instance
        service = RabbitMQService(
            host=os.getenv('RABBITMQ_HOST', '86b9cd9e-2f65-45b4-86c5-b4f6f5b87512.hsvc.ir'),
            port=int(os.getenv('RABBITMQ_PORT', 30379)),
            username=os.getenv('RABBITMQ_USERNAME', 'rabbitmq'),
            password=os.getenv('RABBITMQ_PASSWORD', '9jjJ6s73qfIR5VisD4TZ2Sv8cakEBBVZ'),
            queue_name=os.getenv('RABBITMQ_QUEUE', 'git_data_ingestion')
        )

        # Declare queue
        service.declare_queue()

        # Test message
        test_message = {
            'event_type': "test",
            "payload": {"test": "message1"},
            "timestamp": "2024-11-11T12:00:00Z"
        }

        # Publish test message
        success = service.publish_message(test_message)

        if success:
            logger.info("Test message published successfully")
        else:
            logger.error("Failed to publish test message")

        service.close()

    except Exception as e:
        logger.error(f"Test failed: {str(e)}")


if __name__ == "__main__":
    test_queue()
