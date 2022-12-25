'''
Lesson 12. Task 2.
Напишите DAG, который будет содержать BashOperator и PythonOperator.
В функции PythonOperator примите аргумент ds и распечатайте его.
Можете распечатать дополнительно любое другое сообщение.
'''

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

def print_ds(ds, **kwargs):
    print(ds)
    print('Something else.')



with DAG(
    'k-shilin-15_task2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    }
) as dag:

    t1 = BashOperator(
        task_id = 'print_pwd',
        bash_command = 'pwd'
        )
    t2 = PythonOperator(
        task_id = 'print_date',
        python_callable = print_ds
        )
    t1 >> t2