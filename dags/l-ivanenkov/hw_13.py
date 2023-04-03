from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

with DAG(
        'hw_13_l-ivanenkov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='hw_13_l-ivanenkov',
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['hw_13_l-ivanenkov'],
) as dag:
    def get_variable():
        print(Variable.get('is_startml'))

    def not_startml_desc():
        print("Not a startML course, sorry")

    def determine_course():
        is_startml = Variable.get("is_startml")
        if is_startml == 'True':
            return "startml_desc"
        return "not_startml_desc"

    def startml_desc():
        print("StartML is a starter course for ambitious people")


    t0 = BranchPythonOperator(
        task_id="determine_course",
        python_callable=determine_course
    )

    t1 = PythonOperator(
        task_id="startml_desc",
        python_callable=startml_desc
    )

    t2 = PythonOperator(
        task_id="not_startml_desc",
        python_callable=not_startml_desc
    )

    t0 >> [t1, t2]
