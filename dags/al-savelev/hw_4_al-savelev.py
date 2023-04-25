from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
    'hw_4_al-savelev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='test_11_3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 24),
    catchup=False,
    tags=['hw_4_al-savelev']
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id=f'hw_4_al-savelev_bash_{i}',
            bash_command=f'echo {i}')
    
    t1.doc_md = dedent(
        """
        #Первые 10 задач сделайте типа *BashOperator*
        и выполните в них произвольную команду,
        так или иначе использующую **переменную** цикла
        (например, можете указать `f"echo {i}"`)
        """
    )

    def print_task(task_number):
        print(f'task number is: {task_number}')

    for i in range(20):
        t2 = PythonOperator(
            task_id=f'hw_4_al-savelev_python_{i}',
            python_callable=print_task,
            op_kwargs={'task_number': i})
    
    t2.doc_md = dedent(
        """
        #Оставшиеся 20 задач должны быть *PythonOperator*.
        Функция должна печатать `task number is: {task_number}`,
        где `task_number` - **номер задания из цикла**.
        """
    )

    t1 >> t2