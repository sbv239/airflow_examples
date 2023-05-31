from airflow import DAG

from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator


with DAG(
        'hw_l-anoshkina_9',

        default_args={
        'depends_on_past': False,
                           'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
        description = 'HomeWork task9',
        schedule_interval = timedelta(days=1),
        start_date = datetime(2023, 5, 30),

        catchup = False,

        ) as dag:

        def add_key(ti):
                ti.xcom_push(
                        key='sample_xcom_key',
                        value="xcom test"
                )
        def get_key(ti):
                temp = ti.xcom_pull(
                        key='sample_xcom_key',
                        task_ids='add_key'
                )
                print(temp)

        t1 = PythonOperator(
                task_id='add_key',
                python_callable=add_key,
        )

        t2 = PythonOperator(
                task_id='get_key',
                python_callable=get_key,
        )

        t1 >> t2