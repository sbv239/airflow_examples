"""
#### Task Documentation

"""
from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'a-azarova-17_hw7',
    # Параметры по умолчанию для тасок
    default_args={
        # Если прошлые запуски упали, надо ли ждать их успеха
        'depends_on_past': False,
        # Кому писать при провале
        'email': ['airflow@example.com'],
        # А писать ли вообще при провале?
        'email_on_failure': False,
        # Писать ли при автоматическом перезапуске по провалу
        'email_on_retry': False,
        # Сколько раз пытаться запустить, далее помечать как failed
        'retries': 1,
        # Сколько ждать между перезапусками
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание DAG (не тасок, а самого DAG)
    description="""Создайте новый DAG и объявите в нем 30 задач. 
    Первые 10 задач сделайте типа BashOperator и выполните в них произвольную команду, 
    так или иначе использующую переменную цикла (например, можете указать f"echo {i}").
    Оставшиеся 20 задач должны быть PythonOperator, 
    при этом функция должна задействовать переменную из цикла. 
    Вы можете добиться этого, если передадите переменную через op_kwargs и примете ее на стороне функции. 
    Функция должна печатать "task number is: {task_number}", где task_number - номер задания из цикла. 
    
    Добавьте в PythonOperator из второго задания (где создавали 30 операторов в цикле) kwargs 
    и передайте в этот kwargs task_number со значением переменной цикла. 
    Также добавьте прием аргумента ts и run_id в функции, указанной в PythonOperator, и распечатайте эти значения.
    """,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['azarova'],
) as dag:

    def print_task_number(task_number, ts, run_id, **kwards):
        print(f"task number is: {task_number}")
        print(ts)
        print(run_id)


    # for i in range(10):
    #     task_b = BashOperator(
    #         task_id=f't_{i}',
    #         depends_on_past=False,
    #         # bash_command=f'echo {i}',
    #         bash_command="echo $NUMBER",
    #         env={"NUMBER": i},  # задает переменные окружения
    #     )



    for i in range(20):
        task_p = PythonOperator(
            task_id = 't_' + str(10+i),
            python_callable = print_task_number,
            op_kwargs = {'task_number': i},
        )

    # последовательность задач
    task_b
    # >> task_p
