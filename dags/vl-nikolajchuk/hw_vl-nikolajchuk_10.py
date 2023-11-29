from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_vl-nikolajchuk_10',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='hw10',
        start_date=datetime(2023, 11, 28),
        schedule_interval=timedelta(days=1),
        catchup=False,
        tags=['hw10']
) as dag:
    def track_everything():
        return "Airflow tracks everything"


    def get_tracked_info(ti):
        result = ti.xcom_pull(key='return_value',
                              task_ids='zip_data')
        print(result)


    t1 = PythonOperator(
        task_id='zip_data',
        python_callable=track_everything
    )
    t2 = PythonOperator(
        task_id='unzip_data',
        python_callable=get_tracked_info
    )

    t1 >> t2