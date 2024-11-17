import yaml
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic

def load_config(config_path):
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
    return config
