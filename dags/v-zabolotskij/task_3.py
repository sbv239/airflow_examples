from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent
with DAG\
    (
    "task_3_v_zabolotskij",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    description = "DAG for task #3",
    ) as dag:

    for task in range(30):

        if task <= 10:
            bash_task = BashOperator(
                task_id = "BO_task_" + str(task),
                bash_command = f"echo {task}"            
            )
        else:
            def task_number(task_number = task):
                return f"task number is: {task_number}"
            py_task = PythonOperator(
                task_id = "PY_task_" + str(task),
                python_callable = task_number
            )
    bash_task.doc_md = dedent(
        """ \
        #bash_task DESCRIPTION
        'f"You will know more about bash_task"`    
        **10** Auto generated *BashOperator* tasks.
        """
    )

    py_task.doc_md = dedent(
        """ \
        #py_task DESCRIPTION
        'f"You will know more about py_task"`
        **Py_task** have a function *task_number*
        which printing task number. 
        """
    )