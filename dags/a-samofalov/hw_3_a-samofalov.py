from datetime import datetime, timedelta

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'HW_3_a-samofalov',
    # Параметры по умолчанию для тасок
    default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
    description='A simple tutorial DAG june 2023',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    tags=['hw_3_a-samofalov'],
) as dag:

    for i in range(10):
        # Каждый таск будет спать некое количество секунд
        bash_task = BashOperator(
            task_id='bash_print_' + str(i),  # в id можно делать все, что разрешают строки в python
            bash_command=f'echo {i}'
        )
    def my_sleeping_function(task_number):
        print(f'task number is: {task_number}')

    # Генерируем таски в цикле - так тоже можно
    for i in range(20):
        # Каждый таск будет спать некое количество секунд
        python_task = PythonOperator(
            task_id='python_print_' + str(i),  # в id можно делать все, что разрешают строки в python
            python_callable=my_sleeping_function(),
            # передаем в аргумент с названием random_base значение float(i) / 10
            op_kwargs={'task_number': i},
        )
        # настраиваем зависимости между задачами
        # run_this - это некий таск, объявленный ранее (в этом примере не объявлен)
        bash_task >> python_task
