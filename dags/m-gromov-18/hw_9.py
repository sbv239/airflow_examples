from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def test_push(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value="xcom test"
    )
def test_pull(ti):

    xcom_pull = ti.xcom_pull(
        key='testing_increase',
        task_ids='push_xcom'
    )
    print(xcom_pull)

with DAG(
        'hw_9_m-gromov-18',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG for unit 9',
        tags=['DAG-9_m-gromov-18'],
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 3, 24),

) as dag:
    t1 = PythonOperator(
        task_id='push_xcom',
        python_callable=test_push
    )
    t2 = PythonOperator(
        task_id='pull_xcom',
        python_callable=test_pull
    )
t1 >> t2
