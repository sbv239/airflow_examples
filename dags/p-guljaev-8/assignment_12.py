"""
##Assignment 12 DAG documentation
"""
from datetime import datetime, timedelta

from airflow import DAG
69
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator

with DAG(
        'gul_assignment_12',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        description='BranchingOperator and Variable practice ',
        schedule_interval=None,
        start_date=datetime(2022, 5, 28),
        catchup=False,
        tags=['gul_dags']
) as dag:
    run_this_first = DummyOperator(
        task_id='run_this_first'
    )


    def determine_the_course():
        from airflow.models import Variable

        if Variable.get("is_startml") == 'True':
            return "startml_desc"
        return "not_startml_desc"


    def is_startml():
        print('StartML is a starter course for ambitious people')


    def is_not_startml():
        print('Not a startML course, sorry')


    t1 = PythonOperator(
        task_id='startml_desc',
        python_callable=is_startml
    )

    t2 = PythonOperator(
        task_id='not_startml_desc',
        python_callable=is_not_startml
    )

    determine_course = BranchPythonOperator(
        task_id='determine_course',
        python_callable=determine_the_course
    )

    course_determined = DummyOperator(
        task_id='course_determined'
    )

    run_this_first >> determine_course >> [t1, t2] >> course_determined
    dag.doc_md = __doc__
