from datetime import timedelta, datetime

from airflow import DAG

from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable

with DAG(
        'hw_12_v-kovalev-6',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='First DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['hw_12'],
) as dag:
    def choosing_task():
        if Variable.get("is_startml"):
            return "startml_desc"
        else:
            return "not_startml_desc"


    t1 = DummyOperator(
        task_id='start'
    )

    t2 = BranchPythonOperator(
        task_id='choosing_task',
        python_callable=choosing_task,
    )

    t3 = PythonOperator(
        task_id='startml_desc',
        python_callable=print("StartML is a starter course for ambitious people"),
    )

    t4 = PythonOperator(
        task_id='startml_desc',
        python_callable=print("Not a startML course, sorry"),
    )

    t5 = DummyOperator(
        task_id='end'
    )

    t1 >> t2 >> [t3, t4] >> t5
