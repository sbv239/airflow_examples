from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import timedelta, datetime

with DAG(
        'hw_12_m-valishevskij-7',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 14),
        catchup=False,
        tags=['valishevskij']
) as dag:
    def get_variable():
        from airflow.models import Variable
        return 'startml_desc' if Variable.get("is_startml") else 'not_startml_desc'


    def is_startml():
        print("StartML is a starter course for ambitious people")


    def not_startml():
        print("Not a startML course, sorry")


    t1 = DummyOperator(
        task_id='before_branching'
    )

    t2 = BranchPythonOperator(
        task_id='determine_course',
        python_callable=get_variable
    )

    t3 = PythonOperator(
        task_id='startml_desc',
        python_callable=is_startml
    )

    t4 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=not_startml
    )

    t5 = DummyOperator(
        task_id='after_branching',
        trigger_rule='none_failed_or_skipped'
    )

    t1 >> t2 >> [t3, t4] >> t5
