from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

with DAG(
        'tarasova_task9',
        # Default settings applied to all tasks
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime

        },
        description='DAG for task 9 Tarasova E',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 19),
        catchup=False,
        tags=['task9']

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
