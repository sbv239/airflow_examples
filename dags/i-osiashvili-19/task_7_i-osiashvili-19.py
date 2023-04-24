from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def task_number_is(task_number, ts, run_id, **kwargs):
    print("task number is: {task_number}")
    print(kwargs['ts'])
    print(kwargs['run_id'])



with DAG(
        'les_11_task_7_i-osiashvili-19',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        start_date=datetime(2023, 4, 20),
        schedule_interval=timedelta(days=1),

) as dag:
    for i in range(20):
            py_op_tasks = PythonOperator(
                task_id="py_task_id" + str(i),
                python_callable=task_number_is,
                op_kwargs={"task_number": i},
            )
