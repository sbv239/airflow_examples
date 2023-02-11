"""
Добавьте в PythonOperator из второго задания (где создавали 30 операторов в цикле) kwargs 
и передайте в этот kwargs task_number со значением переменной цикла. 
Также добавьте прием аргумента ts и run_id в функции, указанной в PythonOperator, и распечатайте эти значения.

"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator 
from datetime import timedelta, datetime


with DAG (
    'k-d-t3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'description text',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2023,1,1),
    catchup=False
) as dag:
    

    def foo(task_number, ts, run_id, **kwargs):
        print (ts)
        print(run_id)
        return f"task number is: {task_number}"

    for i in range(20):
        po = PythonOperator(
            task_id = 'po_t3_' + str(i),
            python_callable = foo,
            op_kwargs= {'task_number': i}
        )