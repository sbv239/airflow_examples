from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta


with DAG(
        'hw_13_a-donskoj-5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG in 13 step',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 25),
        catchup=False,
        tags=['task 13'],
) as dag:

    def func():
        from airflow.models import Variable
        if Variable.get('is_startml'):
            return 'startml_desc'
        return 'not_startml_desc'


    start = DummyOperator(
        task_id='before_branching',
    )

    branch_op = BranchPythonOperator(
        task_id='determine_course',
        provide_context=True,
        python_callable=func,
    )

    t1 = BashOperator(
        task_id='startml_desc',
        bash_command='echo "StartML is a starter course for ambitious people"',
    )

    t2 = BashOperator(
        task_id='not_startml_desc',
        bash_command='echo "Not a startML course, sorry"',
    )

    end = DummyOperator(
        task_id='after_branching',
    )

    start >> branch_op >> [t1, t2] >> end