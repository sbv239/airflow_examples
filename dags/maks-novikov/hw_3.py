from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from textwrap import dedent
def check_num(task_number):
    return f'task number is: {task_number}'

    
with DAG(
    'hw_maks-novikov_3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    
    description='HW3',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 16),
    catchup=False,
    tags=['hw_maks-novikov_3'],
) as dag:
    dag.doc_md = """
        This is a documentation placed anywhere
    """ 
    for i in range(1, 11):
        t1 = BashOperator(
            task_id='task_' + str(i),
            bash_command=f'echo {i}',
            dag=dag
        )
    t1.doc_md = dedent(git checkout
    """\
    #### Task Documentation
    *You* can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    #rendered in the UI's **Task Instance Details page**.
    ![img](http://montcs.bloomu.edu/~bobmon/Semesters/2012-01/491/import%20soul.png)
    `for i in range(61, 31):
        t2 = PythonOperator(
            task_id='task_' + str(i),
            python_callable=check_num,
            op_kwargs={'task_number': i}`
    """
    ) # dedent - это особенность Airflow, в него нужно оборачивать всю доку    

    for i in range(11, 31):
        t2 = PythonOperator(
            task_id='task_' + str(i),
            python_callable=check_num,
            op_kwargs={'task_number': i}
        )


t1 >> t2


    
