from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

with DAG(
    'a.burlakov-9_task_8',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    description='a.burlakov-9_DAG_task_8',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 4),
    catchup=False,
    tags=['task_8'],
    ) as dag:
    
    def push_xcom_test(ti):
        ti.xcom_push(
            key='sample_xcom_key',
            value='xcom test'
        )


    def pull_xcom_test(ti):
        xcom_test = ti.xcom_pull(
            key='sample_xcom_key',
            task_ids='push_xcom'
        )
        print(xcom_test)


    t1 = PythonOperator(
        task_id='push_xcom',
        python_callable=push_xcom_test
    )
    t2 = PythonOperator(
        task_id='pull_xcom',
        python_callable=pull_xcom_test,
    )

    t1 >> t2