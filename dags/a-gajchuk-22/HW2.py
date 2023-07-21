from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import timedelta, datetime


with DAG('hw_a-gajchuk-22_3',
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
         tag = "agaychuk3") as dag:

    for k in range(10):
        t1 = BashOperator(task_id = 'print something ' +str(k),
                      bash_command = f"echo {k}")

    def print_number(num):
        print(f"task number is: {num}")

    for k in range(20):
        t2 = PythonOperator(task_id = 'print task number ' +str(k),
                        python_callable = print_number,
                        op_kwargs = {'num': k})


    t1>>t2
    
