from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define the Python function to be executed
def print_hello():
    print("Hello, Airflow!")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Instantiate the DAG
with DAG(
    'simple_test_print_dag',  # DAG ID
    default_args=default_args,
    description='A simple DAG to print a message',
    schedule_interval=None,  # Run manually
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    # Define the task
    print_task = PythonOperator(
        task_id='print_hello_task',  # Task ID
        python_callable=print_hello,  # Function to execute
    )

# Task execution order (if needed, for more tasks)
print_task
