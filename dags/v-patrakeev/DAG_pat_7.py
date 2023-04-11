from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def push_var(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )


def pull_var(ti):
    print(ti.xcom_pull(key='sample_xcom_key', task_ids='t_push'))


with DAG(
    'HW_9_v-patrakeev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date=datetime(2022, 4, 22),
) as dag:
    t1 = PythonOperator(
        task_id='t_push',
        python_callable=push_var
    )

    t2 = PythonOperator(
        task_id='t_pull',
        python_callable=pull_var
    )

    t1 >> t2
