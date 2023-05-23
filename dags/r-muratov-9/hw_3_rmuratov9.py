from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from textwrap import dedent

with DAG(

    'hw_3_r-muratov-9',

    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },

    schedule_interval=timedelta(days=1),

    start_date=datetime(2023,5,20),

    catchup=False
) as dag:
    
    for i in range(10):
        t1 = BashOperator(
            task_id=f'just_something_{i}',
            bash_command="echo $NUMBER ",
            env={'NUMBER': i}
        )

    t1.doc_md = dedent(
        """\
        #### Bash task documantation
        This bash has one attribute: `i` 
        and using for ***echo*** **bash command**
        """
    )
    
    def print_something(**kwargs):
        task_number = kwargs['task_number']
        print(f'task number is {task_number}')
        print(kwargs['ds'])
        print(kwargs['run_id'])

    for r in range(20):
        t2 = PythonOperator(
            task_id=f'print_task_number_{r}',
            python_callable=print_something,
            op_kwargs={'task_number': r},
        )


    t1 >> t2
