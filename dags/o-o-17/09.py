from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

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


with DAG(
        '9_o-o-17',
        start_date=datetime(2021, 1, 1),
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
