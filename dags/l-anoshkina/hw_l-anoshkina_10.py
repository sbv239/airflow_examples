from airflow import DAG

from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator


with DAG(
        'hw_l-anoshkina_10',

        default_args={
        'depends_on_past': False,
                           'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
        description = 'HomeWork task10',
        schedule_interval = timedelta(days=1),
        start_date = datetime(2023, 5, 30),

        catchup = False,

        ) as dag:

        def get_return_value():
            return "Airflow tracks everything"

        def get_xcom(ti):
            temp = ti.xcom_pull(
                key='return_value',
                task_ids='get_return_value'
            )

        t1 = PythonOperator(
            task_id='get_return_value',
            python_callable=get_return_value
        )

        t2 = PythonOperator(
            task_id='get_xcom',
            python_callable=get_xcom
        )

        t1 >> t2
