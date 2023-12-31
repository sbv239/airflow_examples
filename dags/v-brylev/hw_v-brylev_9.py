from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def func_push(ti):
    ti.xcom_push(
        key="sample_xcom_key",
        value="xcom test",
    )


def func_pull(ti):
    example = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='push'
    )
    print(example)

# Default settings applied to all tasks
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'xcom_dag',
        start_date=datetime(2023, 7, 26),
        max_active_runs=2,
        schedule_interval=timedelta(minutes=30),
        default_args=default_args,
        catchup=False
) as dag:
    t1 = PythonOperator(
        task_id='push',
        python_callable=func_push,
    )
    t2 = PythonOperator(
        task_id='pull',
        python_callable=func_pull,
    )

    t1 >> t2
