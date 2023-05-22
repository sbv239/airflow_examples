from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

"""
В прошлом примере все задачи в DAG были объявлены явно. Однако это не единственный способ задать DAG: можно использовать 
всю силу цикла for для объявления задач.

Создайте новый DAG и объявите в нем 30 задач. Первые 10 задач сделайте типа BashOperator и выполните в них произвольную 
команду, так или иначе использующую переменную цикла (например, можете указать f"echo {i}").

Оставшиеся 20 задач должны быть PythonOperator, при этом функция должна задействовать переменную из цикла. Вы можете 
добиться этого, если передадите переменную через op_kwargs и примете ее на стороне функции. Функция должна печатать 
"task number is: {task_number}", где task_number - номер задания из цикла. 

NB! Давайте своим DAGs уникальные названия. Лучше именовать их в формате hw_{логин}_1, hw_{логин}_2
"""

with DAG(
    'hw_d-ivashkin-23_2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Homework 2-nd step DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 5, 21),
    catchup=False,
    tags=['homework', 'di']
) as dag:

    for i in range(10):
        task = BashOperator(
            task_id='echo_' + str(i),
            bash_command=f"echo {i}"
        )

    def print_task(task_number):
        return f"task number is: {task_number}"


    for i in range(11, 31):
        task = PythonOperator(
            task_id="print_" + str(i),
            python_callable=print_task,
            op_kwargs={'task_number': i}
        )

task
