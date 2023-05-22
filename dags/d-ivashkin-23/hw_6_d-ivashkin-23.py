from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta, datetime

"""
Возьмите BashOperator из второго задания (где создавали task через цикл) и подбросьте туда переменную окружения NUMBER, 
чье значение будет равно i из цикла. Распечатайте это значение в команде, указанной в операторе 
(для этого используйте bash_command="echo $NUMBER").

NB! Давайте своим DAGs уникальные названия. Лучше именовать их в формате hw_{логин}_1, hw_{логин}_2
"""

with DAG(
    'hw_d-ivashkin-23_6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Homework 6-th step DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 5, 21),
    catchup=False,
    tags=['homework', 'di']
) as dag:

    for i in range(10):
        task = BashOperator(
            task_id='echo_' + str(i),
            bash_command="echo $NUMBER",
            env={'NUMBER': str(i)}
        )

task
