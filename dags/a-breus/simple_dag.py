from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime


def task_1():
    print("Task 1")


def task_2():
    print("Task 2")


def task_3():
    print("Task 3")


default_args = {
    'start_date': datetime(2023, 5, 1),
    'retries': 1
}


dag = DAG('simple_dag_3', default_args=default_args, schedule_interval=None)


t1 = PythonOperator(task_id='task_1', python_callable=task_1)
t2 = PythonOperator(task_id='task_2', python_callable=task_2)
t3 = PythonOperator(task_id='task_3', python_callable=task_3)


t1 >> t2 >> t3