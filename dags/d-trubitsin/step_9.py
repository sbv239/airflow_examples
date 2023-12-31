from airflow import DAG

from datetime import timedelta, datetime

from airflow.operators.python import PythonOperator


def push_xcom(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )


def pull_xcom(ti):
    pull = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='push_task'
    )
    print(pull)


with DAG(
    'hw_d-trubitsin_9',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },

    description='First DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 26),
    catchup=False,
    tags=['d-trubitsin_9'],
) as dag:

    t1 = PythonOperator(
        task_id='push_task',
        dag=dag,
        python_callable=push_xcom,
    )

    t2 = PythonOperator(
        task_id='pull_task',
        dag=dag,
        python_callable=pull_xcom,
    )

    t1 >> t2
