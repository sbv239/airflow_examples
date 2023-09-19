"""
Test documentation
"""
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.operators.bash import BashOperator

date = "{{ ds }}"
def print_context(ds):
    print(ds)


with DAG(
    'hw-2',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}
    # Описание DAG (не тасок, а самого DAG)

) as dag:
    t1 = BashOperator(
        task_id="hw-2_bo",
        bash_command="pwd ",  # обратите внимание на пробел в конце!
        dag=dag,  # говорим, что таска принадлежит дагу из переменной dag
    )


    t2 = PythonOperator(
        task_id='print_the_context',  # нужен task_id, как и всем операторам
        python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию
    )

    t1 >> t2