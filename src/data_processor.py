import os
import json
import subprocess

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
            mapping_key_field = stage['redis_mappings']['read'].get('key_field')
            print("mappings is ", mappings)
            self._enhance_data_with_mappings(data, mappings, mapping_key_field)

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
        # We have some credential issues reading from s3. Instead of using s3 client, we curl the file content
        try:
            # Construct the curl command
            # TODO: add s3 get object support based on bucket and prefix
            base_url = "https://2024-hackathon-odp-data-plane.s3.amazonaws.com/"
            file_name = "sample_trace/trace_2024111612.json" if "trace" in prefix else "sample_log/log_2024111612.json"
            url = base_url + file_name
            curl_command = [
                "curl",
                "-s",  # Silent mode to suppress progress bar
                "-L",  # Follow redirects
                url
            ]

            # Execute the curl command
            result = subprocess.run(curl_command, capture_output=True, text=True)

            # Check for errors
            if result.returncode != 0:
                raise RuntimeError(f"Curl command failed: {result.stderr}")

            # Parse the JSON content
            content = result.stdout
            json_data = json.loads(content)
            return json_data

        except json.JSONDecodeError:
            raise ValueError("Failed to decode JSON content")
        except Exception as e:
            print(f"Error downloading JSON: {e}")
            return None

    def _read_redis_mappings(self, key_prefix):
        print(f"Reading mappings from Redis with prefix: {key_prefix}")
        return {key.split(':')[-1]: self.redis_client.get(key) for key in self.redis_client.scan_iter(f"{key_prefix}:*")}

    def _write_redis_mappings(self, key_prefix, mappings):
        print(f"Writing mappings to Redis with prefix: {key_prefix}")
        for key, value in mappings.items():
            redis_key = f"{key_prefix}:{key}"
            self.redis_client.set(redis_key, json.dumps(value))

    def _enhance_data_with_mappings(self, data, mappings, mapping_key_field):
        print("Enhancing data with Redis mappings...")
        for item in data:
            # Get the mapping key from the record
            mapping_key = item.get(mapping_key_field)
            if not mapping_key:
                print(f"Skipping record without key field: {mapping_key_field}")
                continue

            mapping = mappings.get(mapping_key)
            if mapping:
                # Merge the Redis mapping values into the record
                item.update(json.loads(mapping))

    def _extract_mappings(self, data, from_fields):
        """
        Extracts mappings based on the specified 'key' and 'value' fields.

        Args:
            data (list): List of records to process.
            from_fields (dict): Configuration specifying 'key' and 'value' fields.

        Returns:
            dict: Mappings extracted based on the configuration.
        """
        print(f"Extracting mappings based on fields: {from_fields}")
        mappings = {}

        for item in data:
            # Extract the key (could be a list for nested fields like spans)
            keys = self._get_nested_field(item, from_fields['key'])

            # Ensure keys is a list for consistency
            if not isinstance(keys, list):
                keys = [keys]

            # Extract the value(s) for each key
            for key in keys:
                if not key:  # Skip if key is None or invalid
                    continue

                if isinstance(from_fields['value'], list):
                    # Extract multiple fields for the value
                    values = {field: self._get_nested_field(item, field) for field in from_fields['value']}
                else:
                    # Extract a single field for the value and wrap it into a dictionary
                    single_value = self._get_nested_field(item, from_fields['value'])
                    if single_value is not None:
                        values = {from_fields['value']: single_value}
                    else:
                        continue

                if values:  # Ensure valid values before adding
                    mappings[key] = values

        return mappings

    def _get_nested_field(self, record, field_path):
        """
        Retrieves the value of a potentially nested field from a record.

        Args:
            record (dict): The record from which to extract the field.
            field_path (str): Dot-separated path to the field (e.g., 'spans.spanId').

        Returns:
            The value of the nested field, or None if the field does not exist.
        """
        fields = field_path.split('.')
        current = record
        for field in fields:
            if isinstance(current, list):
                # If current is a list, recursively extract for each element
                return [self._get_nested_field(item, '.'.join(fields[1:])) for item in current]
            elif isinstance(current, dict) and field in current:
                current = current[field]
            else:
                return None
        return current
