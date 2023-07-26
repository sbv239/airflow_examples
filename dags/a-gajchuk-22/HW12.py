from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import timedelta, datetime
from airflow.models import Variable


with DAG('hw_a-gajchuk-22_12',
         default_args={
                        'depends_on_past': False,
                        'email': ['airflow@example.com'],
                        'email_on_failure': False,
                        'email_on_retry': False,
                        'retries': 1,
                        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
                    },
         description = 'Dag for hw12',
         start_date = datetime(2023, 7, 25),
         tags = ["agaychuk12"]) as dag:

    def print_var():
        is_startml = Variable.get("is_startml")
        
        print(is_startml)

    t2 = PythonOperator(task_id = 'print_variable',
                        python_callable = print_var)
        


    t2
    
