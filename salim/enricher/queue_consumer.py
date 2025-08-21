import os
import json
import pika
import logging
from datetime import datetime
from typing import Dict, Any
from dotenv import load_dotenv

from normalizer import DataNormalizer
from validator import DataValidator
from enricher import DataEnricher
from database import DatabaseManager

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class QueueConsumer:
    def __init__(self):
        load_dotenv()
        
        # Database connection
        self.database_url = os.getenv('DATABASE_URL')
        
        # RabbitMQ configuration
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.rabbitmq_port = int(os.getenv('RABBITMQ_PORT', '5672'))
        self.rabbitmq_username = os.getenv('RABBITMQ_USERNAME', 'admin')
        self.rabbitmq_password = os.getenv('RABBITMQ_PASSWORD', 'admin')
        
        # Queue configuration
        self.pricefull_queue = os.getenv('PRICEFULL_QUEUE', 'pricefull_queue')
        self.promofull_queue = os.getenv('PROMOFULL_QUEUE', 'promofull_queue')
        self.dlq_queue = os.getenv('DLQ_QUEUE', 'dead_letter_queue')
        self.batch_size = int(os.getenv('BATCH_SIZE', '10'))
        
        # Test mode configuration
        self.test_mode = os.getenv('TEST_MODE', 'false').lower() == 'true'
        self.test_limit = int(os.getenv('TEST_LIMIT', '5'))
        
        # Metrics
        self.messages_processed = 0
        self.messages_failed = 0
        self.messages_successful = 0
        
        # Initialize modules
        self.normalizer = DataNormalizer()
        self.validator = DataValidator()
        self.enricher = DataEnricher()
        self.db_manager = DatabaseManager(self.database_url)
        
        self.setup_rabbitmq()
    
    def setup_rabbitmq(self):
        """Setup RabbitMQ connection and queues"""
        try:
            credentials = pika.PlainCredentials(self.rabbitmq_username, self.rabbitmq_password)
            parameters = pika.ConnectionParameters(
                host=self.rabbitmq_host,
                port=self.rabbitmq_port,
                credentials=credentials
            )
            
            self.rabbitmq_connection = pika.BlockingConnection(parameters)
            self.rabbitmq_channel = self.rabbitmq_connection.channel()
            
            # Declare queues
            self.rabbitmq_channel.queue_declare(queue=self.pricefull_queue, durable=True)
            self.rabbitmq_channel.queue_declare(queue=self.promofull_queue, durable=True)
            self.rabbitmq_channel.queue_declare(queue=self.dlq_queue, durable=True)
            
            logger.info(f"Connected to RabbitMQ at {self.rabbitmq_host}:{self.rabbitmq_port}")
            
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise
    
    def send_to_dlq(self, message: Dict[str, Any], error: str):
        """Send failed message to Dead Letter Queue"""
        try:
            dlq_message = {
                'original_message': message,
                'error': error,
                'timestamp': datetime.now().isoformat()
            }
            
            self.rabbitmq_channel.basic_publish(
                exchange='',
                routing_key=self.dlq_queue,
                body=json.dumps(dlq_message, ensure_ascii=False),
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type='application/json'
                )
            )
            
            logger.info(f"Message sent to DLQ: {error}")
            
        except Exception as e:
            logger.error(f"Failed to send to DLQ: {e}")
    
    def process_message(self, message_body: str) -> bool:
        """Process a single message"""
        try:
            self.messages_processed += 1
            
            # Parse message
            message = json.loads(message_body)
            logger.info(f"Processing message: {message.get('metadata', {}).get('file_type', 'unknown')}")
            
            # Normalize
            normalized = self.normalizer.normalize_message(message)
            
            # Validate
            if not self.validator.validate_message(normalized):
                error = "Message validation failed"
                self.send_to_dlq(message, error)
                self.messages_failed += 1
                return False
            
            # Enrich
            enriched = self.enricher.enrich_message(normalized)
            
            # Save to database
            if self.db_manager.save_to_database(enriched):
                self.messages_successful += 1
                logger.info("Message processed successfully")
                return True
            else:
                error = "Failed to save to database"
                self.send_to_dlq(message, error)
                self.messages_failed += 1
                return False
                
        except Exception as e:
            error = f"Message processing failed: {str(e)}"
            self.send_to_dlq(message, error)
            self.messages_failed += 1
            logger.error(error)
            return False
    
    def callback(self, ch, method, properties, body):
        """RabbitMQ message callback"""
        try:
            success = self.process_message(body.decode('utf-8'))
            if success:
                ch.basic_ack(delivery_tag=method.delivery_tag)
            else:
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            
            # Check test mode limit
            if self.test_mode and self.messages_processed >= self.test_limit:
                logger.info(f"Test mode: Reached limit of {self.test_limit} messages. Stopping consumer.")
                self.stop()
                
        except Exception as e:
            logger.error(f"Callback error: {e}")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    
    def start_consuming(self):
        """Start consuming messages from RabbitMQ"""
        try:
            logger.info(f"Starting to consume messages from {self.pricefull_queue} and {self.promofull_queue}")
            logger.info(f"Batch size: {self.batch_size}")
            
            if self.test_mode:
                logger.info(f"TEST MODE ENABLED - Will process {self.test_limit} messages then stop")
            else:
                logger.info("Production mode - Will process messages continuously")
            
            # Set QoS for batch processing
            self.rabbitmq_channel.basic_qos(prefetch_count=self.batch_size)
            
            # Start consuming from both queues
            self.rabbitmq_channel.basic_consume(
                queue=self.pricefull_queue,
                on_message_callback=self.callback
            )
            self.rabbitmq_channel.basic_consume(
                queue=self.promofull_queue,
                on_message_callback=self.callback
            )
            
            logger.info("Consumer started. Press Ctrl+C to stop.")
            self.rabbitmq_channel.start_consuming()
            
        except KeyboardInterrupt:
            logger.info("Stopping consumer...")
            self.stop()
        except Exception as e:
            logger.error(f"Consumer error: {e}")
            self.stop()
    
    def stop(self):
        """Stop the consumer"""
        try:
            if hasattr(self, 'rabbitmq_channel'):
                self.rabbitmq_channel.stop_consuming()
            if hasattr(self, 'rabbitmq_connection'):
                self.rabbitmq_connection.close()
            if hasattr(self, 'db_manager'):
                self.db_manager.close()
            
            logger.info("Consumer stopped")
            logger.info(f"Metrics - Processed: {self.messages_processed}, "
                       f"Successful: {self.messages_successful}, "
                       f"Failed: {self.messages_failed}")
            
        except Exception as e:
            logger.error(f"Error stopping consumer: {e}")

if __name__ == "__main__":
    import sys
    
    # Check for test mode argument
    if len(sys.argv) > 1 and sys.argv[1] == '--test':
        # Set test mode environment variables
        os.environ['TEST_MODE'] = 'true'
        if len(sys.argv) > 2:
            try:
                test_limit = int(sys.argv[2])
                os.environ['TEST_LIMIT'] = str(test_limit)
            except ValueError:
                print("Invalid test limit. Using default of 5.")
        print("🧪 Starting queue consumer in TEST MODE")
    else:
        print("🚀 Starting queue consumer in PRODUCTION MODE")
    
    consumer = QueueConsumer()
    consumer.start_consuming()
