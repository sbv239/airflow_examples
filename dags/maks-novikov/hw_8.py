from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

with DAG(
    'hw_maks-novikov_8',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    
    description='HW8',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 16),
    catchup=False,
    tags=['hw_maks-novikov_8'],
) as dag:

    def push_xcom():
        return "Airflow tracks everything"

    def pull_xcom(ti):
        res = ti.xcom_pull(
            key='return_value',
            task_ids='push_xcom'
        )
        print(res)
    
    t1 = PythonOperator(
        task_id='push_xcom',
        python_callable=push_xcom,
    )

    t2 = PythonOperator(
        task_id='pull_xcom',
        python_callable=pull_xcom,
    )
    
    t1 >> t2