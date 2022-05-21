from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow import DAG

with DAG(
        'step12nazarov',
        # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        # Описание DAG (не тасок, а самого DAG)
        description='step12',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['nazarov12'],
) as dag:

    def tasks_ch():
        from airflow.models import Variable
        if Variable.get("is_startml"):
            return "startml_desc"
        else:
            return "not_startml_desc"

    def startml_desc():
        print("StartML is a starter course for ambitious people")

    def not_startml_desc():
        print("Not a startML course, sorry")


    t1 = DummyOperator(
        task_id='start'
    )

    t2 = BranchPythonOperator(
        task_id='tasks_ch',
        python_callable=tasks_ch
    )

    t3 = PythonOperator(
        task_id='startml_desc',
        python_callable=startml_desc
    )

    t4 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml_desc
    )

    t5 = DummyOperator(
        task_id='end',
        trigger_rule='none_failed_or_skipped'
    )

    t1 >> t2 >> [t3, t4] >> t5