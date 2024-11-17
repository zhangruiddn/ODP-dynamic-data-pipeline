import os
import json
import boto3
import redis


class DataProcessor:
    def __init__(self, config):
        self.config = config
        self.redis_client = redis.StrictRedis(host='localhost', port=6379, decode_responses=True)
        self.s3_client = boto3.client('s3')

    def process_stage(self, stage_name, hour):
        stage = self.config['stages'][stage_name]

        print(f"Processing {stage_name}: {stage['description']} for hour {hour}")

        # Read input data
        data = self._read_input(stage['input'], hour)

        # Handle Redis mappings
        if 'read' in stage['redis_mappings']:
            read_key_prefix = stage['redis_mappings']['read']['redis_key_prefix']
            mappings = self._read_redis_mappings(read_key_prefix)
            self._enhance_data_with_mappings(data, mappings)

        if 'write' in stage['redis_mappings']:
            write_key_prefix = stage['redis_mappings']['write']['redis_key_prefix']
            from_fields = stage['redis_mappings']['write']['from_fields']
            mappings = self._extract_mappings(data, from_fields)
            self._write_redis_mappings(write_key_prefix, mappings)

        # Write output
        output_file = stage['output_file'].format(hour=hour)
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        with open(output_file, 'w') as f:
            json.dump(data, f, indent=2)

        print(f"Stage {stage_name} completed. Output written to {output_file}")

    def _read_input(self, input_config, hour):
        if input_config['type'] == 'local_file':
            file_path = input_config['file_path'].format(hour=hour)
            with open(file_path, 'r') as f:
                return json.load(f)
        elif input_config['type'] == 's3':
            bucket = input_config['bucket']
            prefix = input_config['prefix'].format(hour=hour)
            return self._download_from_s3(bucket, prefix)
        else:
            raise ValueError(f"Unknown input type: {input_config['type']}")

    def _download_from_s3(self, bucket, prefix):
        print(f"Downloading files from S3: bucket={bucket}, prefix={prefix}")
        # Placeholder logic for S3 download
        return []

    def _read_redis_mappings(self, key_prefix):
        print(f"Reading mappings from Redis with prefix: {key_prefix}")
        return {key.split(':')[-1]: self.redis_client.get(key) for key in self.redis_client.scan_iter(f"{key_prefix}:*")}

    def _write_redis_mappings(self, key_prefix, mappings):
        print(f"Writing mappings to Redis with prefix: {key_prefix}")
        for key, value in mappings.items():
            redis_key = f"{key_prefix}:{key}"
            self.redis_client.set(redis_key, json.dumps(value))

    def _enhance_data_with_mappings(self, data, mappings):
        print("Enhancing data with Redis mappings...")
        for item in data:
            if 'spanId' in item:
                mapping = mappings.get(item['spanId'])
                if mapping:
                    item.update(json.loads(mapping))

    def _extract_mappings(self, data, from_fields):
        print(f"Extracting mappings based on fields: {from_fields}")
        mappings = {}
        for item in data:
            key = item[from_fields['key']]
            value = {field: item[field] for field in from_fields['value']}
            mappings[key] = value
        return mappings
