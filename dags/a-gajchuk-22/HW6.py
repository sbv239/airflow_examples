from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import timedelta, datetime
from textwrap import dedent


with DAG('hw_a-gajchuk-22_6',
         default_args={
                        'depends_on_past': False,
                        'email': ['airflow@example.com'],
                        'email_on_failure': False,
                        'email_on_retry': False,
                        'retries': 1,
                        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
                    },
         description = 'Dag for hw6',
         start_date = datetime(2023, 7, 26),
         tags = ["agaychuk6"]) as dag:

    for k in range(10):
        t1 = BashOperator(task_id = 'print_something_' +str(k),
                      bash_command = f"echo $NUMBER",
                      env = {"NUMBER": k})

        t1.doc_md = dedent('''
            # This tasks print something iteratively
            **something bold**

            *something italic*

            `k`

            ''')
        


    t1
    
