from airflow.models import Variable
from airflow import DAG

from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


with DAG(
        'hw_l-anoshkina_13',

        default_args={
        'depends_on_past': False,
                           'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
        description = 'HomeWork task13',
        schedule_interval = timedelta(days=1),
        start_date = datetime(2023, 5, 30),

        catchup = False,

        ) as dag:


        def choose_branch():
                if (Variable.get("is_startml")):
                        return 'startml_desc'
                else:
                        return 'not_startml_desc'

        def startml_desc():
                print("StartML is a starter course for ambitious people")

        def not_startml_desc():
                print("Not a startML course, sorry")

        branching = BranchPythonOperator(
                task_id='branching',
                python_callable=choose_branch,

            )
        t1 = PythonOperator(
                task_id='startml_desc',
                python_callable=startml_desc
        )

        t2 = PythonOperator(
                task_id='not_startml_desc',
                python_callable=not_startml_desc
        )

        branching


# теперь в is_prod лежит значение Variable