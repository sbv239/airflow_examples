from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'dag_task3',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date = datetime(2023, 3, 23)
) as dag:

    def my_function(task_number, ts, run_id):
        print("task number is: {task_number}")
        print(ts)
        print(run_id)
    
    for i in range(10):
        pre_task = BashOperator(
            task_id='loop_' + str(i),  
            bash_command="echo $NUMBER",
            env={"NUMBER": str(i)}
            )


    for i in range(20):
        task = PythonOperator(
            task_id='print_' + str(i),  
            python_callable=my_function,
            op_kwargs={'task_number': i},
        )
    pre_task.doc_md = dedent(
        """
        ##### This function makes echo
        The code is `echo`
        *I do not know what it does*
        **There's no explanations in this course***
        """
    )

    task.doc_md = dedent(
        """
        ##### This function makes print
        The code is `print`
        *I know what it does*
        **Thank's stepik for that***
        """
    )


    pre_task >> task