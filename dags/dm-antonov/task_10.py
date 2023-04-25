from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
        'task_10_dm-antonov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='task_10',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 4, 23),
        catchup=False,
        tags=['task_10']
) as dag:
    def return_string():
        return 'Airflow tracks everything'


    t1 = PythonOperator(
        task_id='return_string',
        python_callable=return_string
    )


    def xcom_pull(ti):
        ti.xcom_pull(
            key='return_value',
            task_ids='return_string'
        )


    t2 = PythonOperator(
        task_id='xcom_pull',
        python_callable=xcom_pull
    )

    t1 >> t2
