from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def push(ti):
    ti.xcom_push(
        key="sample_xcom_key",
        value="xcom test"
    )


def pull(ti):
    value = ti.xcom_pull(
        key="sample_xcom_key",
        task_ids="push"
    )
    print(value)


with DAG(
        '8_dm-morozov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        start_date=datetime(2023, 2, 14)
) as dag:
    t1 = PythonOperator(
        task_id='XCom_push',
        python_callable=push
    )

    t2 = PythonOperator(
        task_id='XCom_pull',
        python_callable=pull
    )

    t1 >> t2
