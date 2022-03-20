from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent
with DAG\
    (
    "task_2_v_zabolotskij",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    description = "DAG for task #2",
    ) as dag:

    task_1 = BashOperator(task_id = "Bash_operator_task",
                          bash_command = "pwd"
                          )
    def print_ds(ds, **kwargs):
        print(ds)
        print(kwargs)

        return "Ok"

    task_2 = PythonOperator\
     (task_id = "print_ds_by_PythonOperator",
      python_callable = print_ds
     )

    task_1.doc_md = dedent(
        """ \
        #TASK_1 DESCRIPTION
        'f"You will know more about task_1"`    
        Bash_command **PWD** 
        by *BashOperator*  
        """
    )

    task_2.doc_md = dedent(
        """ \
        #TASK_2 DESCRIPTION
        'f"You will know more about task_2"`
        Print input **ds** attribute and 
        input **kwargs** dictionary 
        by *PythonOperator* 
        """
    )
    task_1 >> task_2



