from datetime import datetime, timedelta, time

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from textwrap import dedent

def my_sleeping_function(task_number):
    """Заснуть на random_base секунд"""
    print(f"task number is: {task_number}")


with DAG(
    'task_4_ahmetov',
    # Параметры по умолчанию
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    start_date=datetime.now(),
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='print_cur_dir' + str(i),
            bash_command=f"echo {i}",
        )
        t1.doc_md = dedent("""
        ###Bash
        `code`
        **bold**
        __курсив__
        hello
        """)
    # Генерируем таски в цикле - так тоже можно
    for i in range(20):
        # Каждый таск будет спать некое количество секунд
        task = PythonOperator(
            task_id='sleep_for_' + str(i),  # в id можно делать все, что разрешают строки в python
            python_callable=my_sleeping_function,
            # передаем в аргумент с названием random_base значение float(i) / 10
            op_kwargs={'task_number': i},
        )
        # настраиваем зависимости между задачами
        # run_this - это некий таск, объявленный ранее (в этом примере не объявлен)
        task.doc_md = dedent("""
        ###Python
        `code`
        **bold**
        __курсив__
        hello
        """)

