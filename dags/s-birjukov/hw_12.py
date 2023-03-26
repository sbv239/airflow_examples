from datetime import datetime, timedelta
from airflow.models import Variable

from airflow import DAG
from airflow.operators.python import PythonOperator

# Пишу функцию, по которой достаю Variable
# делаю импорт Variable
# передаю Variable.get с ключом = 'is_startml'
# печатаю переменную
def variables():
    print(Variable.get('is_startml'))


with DAG(
        'hw_12_s-birjukov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='DAG_Variables',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 2, 25),
        catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id='return_key_Variable',
        python_callable=variables
    )
