from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def set_xcom_func(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )


def get_xcom_func(ti):
    # value_read = ti.xcom_pull(key='sample_xcom_key')
    value_read = ti.xcom_pull(task_ids='set_xcom')
    print(value_read)


with DAG(
        'dag_8_m-tsaj',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='A simple dag to try XCom',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2021, 3, 20),
        catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id='set_xcom',
        python_callable=set_xcom_func
    )

    t2 = PythonOperator(
        task_id='get_xcom'
    )

    t1 >> t2
