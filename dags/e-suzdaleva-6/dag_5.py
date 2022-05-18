from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
        'e-suzdaleva-6-task5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='e-suzdaleva-6_DAG_task5',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 11),
        catchup=False,
        tags=['e-suzdaleva-6-task5'],
) as dag:
    for i in range(1, 11):
        t1 = BashOperator(
            env={"NUMBER": i},
            task_id='task_number' + str(i),
            bash_command="echo $NUMBER",
        )


    def task_number(task_number):
        print(f'task number is: {task_number}')
        return None


    for i in range(11, 31):
        t2 = PythonOperator(
            task_id='bash_operator' + str(i),
            python_callable=task_number,
            op_kwargs={'task_number': i}
        )
