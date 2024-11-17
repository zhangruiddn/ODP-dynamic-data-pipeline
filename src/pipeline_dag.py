import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime, timedelta
import yaml
from data_processor import DataProcessor
from batch_tlb import BatchTLB

# Default arguments
default_args = {
    'owner': 'local_user',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 11, 1),
}

# Initialize DAG
dag = DAG(
    'observability_correlation_pipeline',
    default_args=default_args,
    description='Correlates user experience stream with trace and log batch files from s3',
    schedule_interval=None,
)


# Load configuration
def load_config():
    dag_dir = os.path.dirname(os.path.realpath(__file__))
    config_path = os.path.join(dag_dir, '../pipelines/observability_correlation_pipeline.yaml')
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


config = load_config()
processor = DataProcessor(config)


# Airflow task for each stage
def run_stage(stage_name, hour, **kwargs):
    print(stage_name + ": " + hour)
    # processor.process_stage(stage_name, hour)


# Task for computing metrics
def compute_metrics(hour, **kwargs):
    user_exp_file = f"../data/user_experience/{hour}.json"
    trace_file = f"../output/trace_processed/{hour}.json"
    log_file = f"../output/log_processed/{hour}.json"
    output_file = f"../output/tlb_metrics/{hour}.json"

    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    batch_tlb = BatchTLB(user_exp_file, trace_file, log_file)
    batch_tlb.compute_metrics(output_file)


# HttpSensor for checking trace and log file availability
# Ideally we should use s3 key sensor to auto-detect new file drop.
# Here for simplicity we hardcoded the path
trace_file_sensor = HttpSensor(
    task_id='trace_file_sensor',
    http_conn_id='http_s3_trace_logs',
    endpoint='sample_trace/trace_2024111612.json',
    response_check=lambda response: response.status_code == 200 if response else False,
    poke_interval=5,  # Check every 5s
    timeout=60 * 60,  # Wait for up to 1 hour
    retries=100,
    retry_delay=timedelta(seconds=5),  # The file not found triggers exception, so retry here
    dag=dag,
)

# Again, we should use s3 key sensor to auto-detect new file drop.
log_file_sensor = HttpSensor(
    task_id='log_file_sensor',
    http_conn_id='http_s3_trace_logs',
    endpoint='sample_log/log_2024111612.json',
    response_check=lambda response: response.status_code == 200 if response else False,
    poke_interval=5,
    timeout=60 * 60,
    retries=100,
    retry_delay=timedelta(seconds=5),
    dag=dag,
)

# Processing tasks
stage_1_task = PythonOperator(
    task_id='process_eco_stream',
    python_callable=run_stage,
    op_kwargs={'stage_name': 'stage_1', 'hour': '{{ ds_nodash }}'},
    dag=dag,
)

stage_2_task = PythonOperator(
    task_id='process_trace_batch',
    python_callable=run_stage,
    op_kwargs={'stage_name': 'stage_2', 'hour': '{{ ds_nodash }}'},
    dag=dag,
)

stage_3_task = PythonOperator(
    task_id='process_log_batch',
    python_callable=run_stage,
    op_kwargs={'stage_name': 'stage_3', 'hour': '{{ ds_nodash }}'},
    dag=dag,
)

batch_tlb_task = PythonOperator(
    task_id='compute_batch_tlb',
    python_callable=compute_metrics,
    op_kwargs={'hour': '{{ ds_nodash }}'},
    dag=dag,
)

# DAG dependencies
[trace_file_sensor, log_file_sensor] >> stage_1_task >> stage_2_task >> stage_3_task >> batch_tlb_task
