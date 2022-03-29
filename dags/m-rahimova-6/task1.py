from airflow import DAG
from datetime import timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'DAG1_rahimova',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},
    tags=['hehe'],
) as dag:

    t1 = BashOperator(
        task_id='print_folder',
        bash_command='pwd',
    )


    def print_context(ds, **kwargs):
        """Пример PythonOperator"""
        # В ds Airflow за нас подставит текущую логическую дату - строку в формате YYYY-MM-DD
        print(ds)
        print('hehe')
#        return 'Whatever you return gets printed in the logs'


    t2 = PythonOperator(
        task_id='print_date',  # нужен task_id, как и всем операторам
        python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию
    )

    t1 >> t2