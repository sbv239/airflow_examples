from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator



with DAG(
        'hw_12_a-tjurin',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  
        },

        description='Task 12',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 2, 18),
        catchup=False,

        tags=['Task_12'],
) as dag:
    def print_variables(var):
        from airflow.models import Variable
        is_startml = Variable.get("is_startml")
        print(is_startml)
        return 'is_startml variable print'

    t1 = PythonOperator(
        task_id='print_variable',
        python_callable=print_variables, 
     )

    t1