from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import timedelta, datetime
from textwrap import dedent


with DAG('hw_a-gajchuk-22_7',
         default_args={
                        'depends_on_past': False,
                        'email': ['airflow@example.com'],
                        'email_on_failure': False,
                        'email_on_retry': False,
                        'retries': 1,
                        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
                    },
         description = 'Dag for hw3',
         start_date = datetime(2023, 7, 21),
         tags = ["agaychuk7"]) as dag:

    def print_number(ts, run_id, **kwargs):
        print(f"task number is: {task_number}")
        print(ts)
        print(run_id)

    for k in range(20):
        t2 = PythonOperator(task_id = 'print_task_number_' +str(k),
                        python_callable = print_number,
                        op_kwargs = {'task_number': k})

        t2.doc_md = dedent('''
            # This tasks print number iteratively
            **something bold**

            *something italic*

            `k`

            ''')
        


    t2
    
