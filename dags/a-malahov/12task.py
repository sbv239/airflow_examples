from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.models import Variable

with DAG(
        'a-malahov_task12',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='a-malachov task 12',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 10),
        catchup=False,
        tags=['malahov'],
) as dag:

    def print_start_ml():
        print('StartML is a starter course for ambitious people')

    def print_not_start_ml():
        print('Not a startML course, sorry')

    def get_variables_start_ml():
        if Variable.get("is_startml") == 'True':
            return 'startml_desc'
        return 'not_startml_desc'

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=get_variables_start_ml ,
    )

    start_ml = PythonOperator(
        task_id='startml_desc',
        python_callable=print_start_ml ,
    )

    not_start_ml = PythonOperator(
        task_id='not_startml_desc',
        python_callable=print_not_start_ml ,
    )

    before_branching = DummyOperator(
        task_id='before_branching',
    )

    after_branching = DummyOperator(
        task_id='after_branching',
        trigger_rule='one_success',
        dag=dag ,
    )
    # # Запускается, когда все предыдущие задачи завершены
    # all_done = DummyOperator(task_id='all_done', trigger_rule='all_done', dag=dag)
    # # Запускается, как только любая из предыдущих задач успешно отработал
    # one_success = DummyOperator(task_id='one_success', trigger_rule='one_success', dag=dag)

    before_branching >> branching >> [start_ml, not_start_ml] >> after_branching