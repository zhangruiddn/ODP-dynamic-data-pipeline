import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import datetime, timedelta
import yaml
from data_processor import DataProcessor

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
    'local_file_pipeline_with_prerequisites',
    default_args=default_args,
    description='Process local files with sequential stages and prerequisites',
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
    processor.process_stage(stage_name, hour)


# S3 Sensors to check for file availability
trace_file_sensor = S3KeySensor(
    task_id='trace_file_sensor',
    bucket_name='demo-trace-bucket',
    bucket_key='traces/{{ ds_nodash }}/*',
    wildcard_match=True,
    timeout=60 * 60,  # Wait up to 1 hour for new files
    poke_interval=300,  # Check every 5 minutes
    dag=dag,
)

log_file_sensor = S3KeySensor(
    task_id='log_file_sensor',
    bucket_name='demo-log-bucket',
    bucket_key='logs/{{ ds_nodash }}/*',
    wildcard_match=True,
    timeout=60 * 60,
    poke_interval=300,
    dag=dag,
)

# Processing tasks
stage_1_task = PythonOperator(
    task_id='process_stage_1',
    python_callable=run_stage,
    op_kwargs={'stage_name': 'stage_1', 'hour': '{{ ds_nodash }}'},
    dag=dag,
)

stage_2_task = PythonOperator(
    task_id='process_stage_2',
    python_callable=run_stage,
    op_kwargs={'stage_name': 'stage_2', 'hour': '{{ ds_nodash }}'},
    dag=dag,
)

stage_3_task = PythonOperator(
    task_id='process_stage_3',
    python_callable=run_stage,
    op_kwargs={'stage_name': 'stage_3', 'hour': '{{ ds_nodash }}'},
    dag=dag,
)

# DAG dependencies
[trace_file_sensor, log_file_sensor] >> stage_1_task >> stage_2_task >> stage_3_task
