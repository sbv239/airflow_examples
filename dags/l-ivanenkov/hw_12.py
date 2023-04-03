from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

with DAG(
        'hw_12_l-ivanenkov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='hw_12_l-ivanenkov',
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['hw_12_l-ivanenkov'],
) as dag:
    def get_variable():
        print(Variable.get('is_startml'))

    t1 = PythonOperator(
        task_id='get_variable_print',
        python_callable=get_variable
    )