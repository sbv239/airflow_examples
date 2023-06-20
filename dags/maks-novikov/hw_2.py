from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def print_date(ds):
    print(ds)
    return f'Current date: {ds}'
    
with DAG(
    'hw_-maks-novikov_3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    
    description='Try a new dag',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 16),
    catchup=False,
    tags=['hw_-maks-novikov_3'],
) as dag:
    
    for i in range(20):
        t1 = BashOperator(
            task_id='templated',
            depends_on_past=False,
            bash_command=templated_command,
            )
    
    def my_sleeping_function(random_base):
        time.sleep(random_base)

    for i in range(20):
        # Каждый таск будет спать некое количество секунд
        t2 = PythonOperator(
            task_id='sleep_for_' + str(i), # в id можно делать все, что разрешают строки в python
            python_callable=my_sleeping_function,
            # передаем в аргумент с названием random_base значение float(i) / 10
            op_kwargs={'random_base': float(i) / 10},
            )
# настраиваем зависимости между задачами
# run_this - это некий таск, объявленный ранее (в этом примере не объявлен)

t1 >> t2


    
