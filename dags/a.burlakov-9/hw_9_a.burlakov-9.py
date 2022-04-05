from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

with DAG(
    'a.burlakov-9_task_9',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    description='a.burlakov-9_DAG_task_9',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 4),
    catchup=False,
    tags=['task_9'],
    ) as dag:
    
    def airflow_tracks():
        return "Airflow tracks everything"

    def pull_xcom_test(ti):
        xcom_test = ti.xcom_pull(
            key='return_value',
            task_ids='push_xcom'
        )
        print(xcom_test)

    t1 = PythonOperator(
        task_id='push_xcom',
        python_callable=airflow_tracks
    )
    t2 = PythonOperator(
        task_id='pull_xcom',
        python_callable=pull_xcom_test,
    )

    t1 >> t2