# Возьмите BashOperator из третьего задания (где создавали task через цикл)
# и подбросьте туда переменную окружения NUMBER, чье значение будет равно i из цикла.
# Распечатайте это значение в команде,
# указанной в операторе (для этого используйте bash_command="echo $NUMBER").

import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
    'hw_6_a-maslennikov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = "Making DAG for 6th task",
    schedule_interval = datetime.timedelta(days=1),
    start_date = datetime.datetime(2023, 5, 26),
    catchup = False,
    tags = ["hw_6_a-maslennikov"],
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id = f"print_echo_{i}",
            bash_command = "echo $NUMBER",
            env = {"NUMBER": i}
        )

    def print_task_num(task_number, **kwargs):
        print(kwargs)
        return f"task_number is: {task_number}"

    for i in range(20):
        t2 = PythonOperator(
            task_id = f"print_task_{i}",
            python_callable = print_task_num,
            op_kwargs = {"task_number": i}
        )

    t1 >> t2
