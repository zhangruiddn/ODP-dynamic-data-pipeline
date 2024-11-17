from kafka.admin import NewTopic


def setup_kafka_topics(config, kafka_admin):
    # Get topic names and partition details from config
    topics = config['topics']
    kafka_topics = []
    for topic_name, topic_details in topics.items():
        new_topic = NewTopic(
            name=topic_name,
            num_partitions=3,  # Assume 3 partitions; can be configurable
            replication_factor=1
        )
        kafka_topics.append(new_topic)

    # Create topics in Kafka
    kafka_admin.create_topics(new_topics=kafka_topics, validate_only=False)
