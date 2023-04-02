from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def put_xcom(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )

def print_xcom(ti):
    tmp = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='update_task_id'
    )
    print(tmp)

# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'hw_9_a-korenevich-1',
    start_date=datetime(2021, 1, 1),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
    catchup=False
) as dag:
    t1 = PythonOperator(
        task_id = 'update_task_id',
        python_callable=put_xcom
    )
    t2 = PythonOperator(
        task_id = 'print_task_id',
        python_callable=print_xcom
    )

    t1 >> t2