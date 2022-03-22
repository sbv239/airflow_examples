from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'tarasova_task3',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime

    },
    description='DAG for task 3 Tarasova E',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 19),
    catchup=False,
    tags=['task2']

,
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='echo_' + str(i),
            bash_command=f'echo {i}',
        )

    t1.doc_md = dedent(
        """\
    # Первые 10 задач   
    сделайте типа __BashOperator__  

    и *выполните в них произвольную команду*,
    так или иначе использующую переменную цикла (например, можете указать
    `"f"echo {i}"`).
        """
    )


    def get_task_number(task_number):
        print(f"task number is: {task_number}")

    for i in range(20):
        t2 = PythonOperator(
            task_id = 'task_number'+str(i),
            python_callable=get_task_number,
            op_kwargs={'task_number': i}
        )

    t2.doc_md = dedent(
        """\
    # Оставшиеся 20 задач должны быть PythonOperator,  
    __при этом функция должна задействовать переменную из цикла.__  
    *Вы можете добиться этого, если передадите переменную* через `op_kwargs`   
    и примете ее на стороне функции. 
    Функция должна печатать `"task number is: {task_number}"`, где task_number - номер задания из цикла.
        """
    )


t1 >> t2