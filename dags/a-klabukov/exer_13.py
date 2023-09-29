from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator

def get_var():
    from airflow.models import Variable

    is_prod = Variable.get("is_startml")
    if is_prod == 'True':
        return task_for_startml.task_id
    else:
        return task_for_not_startml.task_id

def func_for_startml():
    print('StartML is a starter course for ambitious people')

def func_for_not_startml():
    print('Not a startML course, sorry')


with DAG(
          'a-klabukov_hw_13',
          default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
          },
          description='A simple tutorial DAG',
          schedule_interval=timedelta(days=1),
          start_date=datetime(2022, 1, 1),
          catchup=False,
          tags=['example'],
) as dag:


  task_for_branch = BranchPythonOperator(
    task_id = 'branch_operator',
    python_callable = get_var,
  )

  task_for_startml = PythonOperator(
      task_id="startml_desc",
      python_callable = func_for_startml,
  )

  task_for_not_startml = PythonOperator(
      task_id="not_startml_desc",
      python_callable = func_for_not_startml,
  )

  before_branching = DummyOperator(
      task_id='before_branching',
  )

  after_branching = DummyOperator(
      task_id='after_branching',
  )

  before_branching >> task_for_branch
  task_for_branch >> task_for_startml >> after_branching
  task_for_branch >> task_for_not_startml >> after_branching






