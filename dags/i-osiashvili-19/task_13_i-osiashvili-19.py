from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator


def get_branching():
    from airflow.models import Variable
    is_startml = Variable.get("is_startml")
    if not is_startml:
        task_id = "not_startml_desc"
    else:
        task_id = "startml_desc"
    return task_id


with DAG(
        'les_11_task_13_i-osiashvili-19',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        start_date=datetime(2023, 4, 20),
        schedule_interval=timedelta(days=1),
) as dag:
    dummy_task = DummyOperator(
        task_id="dummy_task",
    )

    branching = BranchPythonOperator(
        task_id="nor_or_yes",
        python_callable=get_branching,
    )

    task_desc = BashOperator(
        task_id="startml_desc",
        bash_command=f"echo StartML is a starter course for ambitious people",
    )

    task_not_desc = BashOperator(
        task_id="not_startml_desc",
        bash_command=f"echo Not a startML course, sorry",
    )
