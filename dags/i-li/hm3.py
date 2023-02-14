from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from textwrap import dedent
with DAG(
        'hm_2',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='первая работа с DAG',
        start_date=datetime(2023, 2, 13)
) as dag:
    def print_task(task_number):
        print(f"task number is: {task_number}")


    for task in range(10):
        t = BashOperator(
            task_id='task_' + str(task),
            bash_command=f'echo {task}'
        )
    for task in range(20):
        t = PythonOperator(
            task_id='task' + str(task + 10),
            python_callable=print_task,
            op_kwargs={'task_number':task}
        )
    dag.doc_nd = dedent(
        '''
        # В этом задании нужно использовать возможность **Python**
        
        Для этого мы использовали *циклы* `for i in range()`
        '''
    )

