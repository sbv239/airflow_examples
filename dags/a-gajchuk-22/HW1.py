from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import timedelta


def print_date(ds):
    print(ds)
    print('All is OK')

with DAG('hw_a-gajchuk-22_1',
         default_args={
                        'depends_on_past': False,
                        'email': ['airflow@example.com'],
                        'email_on_failure': False,
                        'email_on_retry': False,
                        'retries': 1,
                        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
                    },
         description = 'Dag for hw1') as dag:

    t1 = BashOperator(task_id = 'print_directory',
                      bash_command = "pwd")

    t2 = PythonOperator(task_id = 'print_date',
                        python_callable = print_date,
                        op_kwargs = {'ds':ds})


    t1>>t2
    
