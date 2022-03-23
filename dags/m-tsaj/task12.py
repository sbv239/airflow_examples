from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta


def branch_function():
    from airflow.models import Variable
    if Variable.get('is_startml'):
        return 'startml_desc'
    return 'not_startml_desc'


with DAG(
        'dag_12_m-tsaj',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='Simple branching dag',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2021, 3, 20),
        catchup=False,
) as dag:
    start = DummyOperator(
        task_id='start',
    )

    branch_op = BranchPythonOperator(
        task_id='is_startml_task',
        provide_context=True,
        python_callable=branch_function,
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
        task_id='end',
    )

    start >> branch_op >> end
