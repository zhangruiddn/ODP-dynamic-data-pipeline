import os
import yaml
from data_processor import DataProcessor
from batch_tlb import BatchTLB


# Load the pipeline configuration
def load_config(config_path):
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


# Test the pipeline stages
def test_pipeline(config_path, hour):
    # Load configuration
    config = load_config(config_path)
    processor = DataProcessor(config)

    # Process each stage sequentially
    for stage_name in config['stages']:
        print(f"Running {stage_name}...")
        processor.process_stage(stage_name, hour)

    # Run the Batch TLB task
    print("Running Batch TLB...")
    user_exp_file = f"../data/user_exp_{hour}.json"
    trace_file = f"../output/trace_processed_{hour}.json"
    log_file = f"../output/log_processed_{hour}.json"
    output_file = f"../output/tlb_metrics/{hour}.json"

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    batch_tlb = BatchTLB(user_exp_file, trace_file, log_file, output_file)
    batch_tlb.compute_metrics()
    print(f"Batch TLB metrics written to: {output_file}")


if __name__ == "__main__":
    # Path to the pipeline configuration file
    config_path = "../pipelines/observability_correlation_pipeline.yaml"
    hour = "2024111612"

    test_pipeline(config_path, hour)
