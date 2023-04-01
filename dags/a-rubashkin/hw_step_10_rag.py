from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def to_xcom(ti):
    return 'Airflow tracks everything'

def from_xcom(ti):
    sample_xcom_value = ti.xcom_pull(
        key='return_value',
        task_ids='to_xcom'
    )
    print(sample_xcom_value)

def print_task_number(ts, run_id, **kwargs):
    print(f'task number is: {str(kwargs["task_number"])}')
    print(f'ts: {str(ts)}')
    print(f'run_id: {str(run_id)}')

with DAG(
    'rag_hw_10',
    description='HW_step_10',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 1),
    catchup=False,
    tags=['rag23'],
) as dag:

    t_1 = PythonOperator(
        task_id = 'to_xcom',
        python_callable=to_xcom)

    t_2 = PythonOperator(
        task_id = 'from_xcom',
        python_callable=from_xcom)

    t_1 >> t_2    
