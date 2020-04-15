import sys
import os
sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))

from airflowlib.emr_lib import *

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2016, 1, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
    'email_on_retry': False,
    'provide_context': True
}

# Initialize the DAG
# Concurrency --> Number of tasks allowed to run concurrently
dag = DAG('udacity_capstone_dag',
          concurrency=3,
          schedule_interval=None,
          default_args=default_args,
          description='Load and Transform data in EMR with Airflow'
          )
region = get_region()
client(region_name=region)


# Creates an EMR cluster
def create_emr(**kwargs):
    cluster_id = create_cluster(region_name=region, cluster_name='udacity_capstone_cluster', num_core_nodes=2)
    return cluster_id


# Waits for the EMR cluster to be ready to accept jobs
def wait_for_completion(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    wait_for_cluster_creation(cluster_id)


# Terminates the EMR cluster
def terminate_emr(**kwargs):
    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    terminate_cluster(cluster_id)


# Converts each of the datafile to parquet
def submit_script_to_emr(**kwargs):
    # Construct month_year arg
    execution_date = kwargs['execution_date']
    year_str = str(execution_date.year)
    month_str = execution_date.strftime("%b")
    script_args = "month_year = '{}'\n".format(month_str + year_str)

    file = kwargs['params']['file']

    ti = kwargs['ti']
    cluster_id = ti.xcom_pull(task_ids='create_cluster')
    cluster_dns = get_cluster_dns(cluster_id)
    headers = create_spark_session(cluster_dns, 'pyspark')
    session_url = wait_for_idle_session(cluster_dns, headers)
    statement_response = submit_statement(session_url, file, script_args)
    track_statement_progress(cluster_dns, statement_response.headers)
    kill_spark_session(session_url)


# Define the individual tasks using Python Operators
create_cluster = PythonOperator(
    task_id='create_cluster',
    python_callable=create_emr,
    dag=dag)

wait_for_cluster_completion = PythonOperator(
    task_id='wait_for_cluster_completion',
    python_callable=wait_for_completion,
    dag=dag)

transform_immigration = PythonOperator(
    task_id='transform_immigration',
    python_callable=submit_script_to_emr,
    dag=dag,
    params={"file": '/root/airflow/dags/transform/immigration.py'}
)

transform_temperature = PythonOperator(
    task_id='transform_temperature',
    python_callable=submit_script_to_emr,
    dag=dag,
    params={"file": '/root/airflow/dags/transform/temperature.py'}
)

terminate_cluster = PythonOperator(
    task_id='terminate_cluster',
    python_callable=terminate_emr,
    trigger_rule='all_done',
    dag=dag)

# construct the DAG by setting the dependencies
create_cluster >> wait_for_cluster_completion
wait_for_cluster_completion >> transform_immigration >> terminate_cluster
wait_for_cluster_completion >> transform_temperature >> terminate_cluster
