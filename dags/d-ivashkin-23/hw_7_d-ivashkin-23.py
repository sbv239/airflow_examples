from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

"""
> Больше аргументов
Добавьте в PythonOperator из второго задания (где создавали 30 операторов в цикле) kwargs и передайте в этот kwargs 
task_number со значением переменной цикла. Также добавьте прием аргумента ts и run_id в функции, 
указанной в PythonOperator, и распечатайте эти значения.

NB! Давайте своим DAGs уникальные названия. Лучше именовать их в формате hw_{логин}_1, hw_{логин}_2


"""

with DAG(
    'hw_d-ivashkin-23_7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Homework 7-th step DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 5, 21),
    catchup=False,
    tags=['homework', 'di']
) as dag:

    def print_task(ts, run_id, **kwargs):
        task_number = kwargs['task_number']
        print(f"Task number is: {task_number}")
        print(f"ts value is: {ts}")
        print(f"run_id value is: {run_id}")


    for i in range(11, 31):
        task = PythonOperator(
            task_id="print_" + str(i),
            python_callable=print_task,
            op_kwargs={'task_number': i}
        )

task
