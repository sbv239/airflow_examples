from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


with DAG(
    'intro_8th',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    description='intro_8th',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 10),
    catchup=False,
    tags=['a-jablokova'],
) as dag:

    def push(ti):
        ti.xcom_push(key='sample_xcom_key', value='xcom test')

    t1 = PythonOperator(
        task_id='push_data',
        python_callable=push,
        )

    def pull(ti):
        t = ti.xcom_pull(key='sample_xcom_key', task_ids='push_data')
        print(t)

    t2 = PythonOperator(
        task_id='pull_data', 
        python_callable=pull,
        )

    t1 >> t2