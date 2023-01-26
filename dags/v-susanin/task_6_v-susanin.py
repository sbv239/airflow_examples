from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator

with DAG(
    'v-susanin_task_6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='v-susanin_DAG_task_6',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 26),
    catchup=False,
    tags=['DAG_task_6'],

) as dag:
    for i in range(10):
        first=BashOperator(
            task_id="a" + str(i),
            bash_command="echo $NUMBER",
            env={"NUMBER": str(i)}
        )

    first.doc_md = dedent(
        """\
        #### Task Documentation
        Env в BashOperator 
        """
    )

