from kafka import KafkaAdminClient, KafkaProducer

from src.partition_processor import PartitionProcessor
from src.setup import setup_kafka_topics


def start_pipeline(config):
    # Initialize Kafka Admin client
    kafka_admin = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
    setup_kafka_topics(config, kafka_admin)
    
    # Initialize Kafka Producer
    kafka_producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    # Process each stage based on configuration
    stages = config['stages']
    processors = []
    for stage_config in stages:
        processor = PartitionProcessor(stage_config, kafka_producer)
        processors.append(processor)
    
    # Simulate receiving and processing messages (actual implementation would consume from Kafka)
    for processor in processors:
        # In reality, you would consume messages from Kafka, process, and then publish to the next topic
        print(f"Processing stage: {processor.output_topic}")

# Load config and start pipeline
config = load_config('pipeline_config.yaml')
start_pipeline(config)
