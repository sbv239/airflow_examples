from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
from textwrap import dedent

with DAG(
    "hw_6_n_vojtova",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='DAG with Env',
    schedule_interval=timedelta(days=1),
    start_date= datetime(2023, 12,9),
    catchup=False,
    tags=['hw_6','n_vojtova'],
) as dag:
    t1 = DummyOperator(task_id="start_dag")
    t2 = DummyOperator(task_id="wait_for_all_operators")
    t3 = DummyOperator(task_id="end_dag")

    for i in range (10):
        task = BashOperator(
            task_id=f"print_date{i}",
            bash_command="echo $NUMBER",
            env={"NUMBER": str(i)},
        )
        t2 << task << t1