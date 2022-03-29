from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def set_xcom_f(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )


def get_xcom_f(ti):
    value = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='set_xcom'
    )
    print(value)


with DAG(
        'a-ts_dag_8',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='A simple dag XCom',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2021, 3, 20),
        catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id='set_xcom',
        python_callable=set_xcom_f,
    )

    t2 = PythonOperator(
        task_id='get_xcom',
        python_callable=get_xcom_f,
    )

    t1 >> t2