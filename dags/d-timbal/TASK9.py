from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def push_some(ti):
    x = "xcom test"
    ti.xcom_push(
        key="sample_xcom_key",
        value=x
    )

def pull_and_print(ti):
    x1 = ti.xcom_pull(
        key="sample_xcom_key",
        task_ids="help"
    )
    print(x1)

with DAG(
    'task9_d.timbal',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='FirstDag',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 1, 30),
        catchup=False,
        tags=['imhere_001'],
) as dag:
    t1 = PythonOperator(
        task_id = 'push_some_info',
        python_callable=push_some
    )

    t2 = PythonOperator(
        task_id = 'get_some_info',
        python_callable=pull_and_print
    )


    t1 >> t2

#help