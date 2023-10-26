from datetime import datetime, timedelta, time

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def print_context(task_number: int, ts, run_id, **kwargs):
    print(f"task number is: {task_number}")
    print(ts)
    print(run_id)


with DAG(
    'task_7_ahmetov',
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
            task_id='number' + str(i),
            bash_command="echo $NUMBER",
            env={'NUMBER': i},
        )
        t1
    # Генерируем таски в цикле - так тоже можно
    for i in range(20):
        # Каждый таск будет спать некое количество секунд
        task = PythonOperator(
            task_id='sleep_for_' + str(i),  # в id можно делать все, что разрешают строки в python
            python_callable=print_context,
            # передаем в аргумент с названием random_base значение float(i) / 10
            op_kwargs={'task_number': i},
        )
        # настраиваем зависимости между задачами
        # run_this - это некий таск, объявленный ранее (в этом примере не объявлен)
        task

