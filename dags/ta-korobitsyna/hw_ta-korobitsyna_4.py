from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_ta-korobitsyna_4',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
        },
    description='hw_4_ta-korobitsyna',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 18),
    catchup=False,
    tags=['hw_ta-korobitsyna_4'],
) as dag:

    for i in range(10):

        t1 = BashOperator(
            task_id=f"echo_{i}",  # id, будет отображаться в интерфейсе
            bash_command=f"echo_{i}",  # какую bash команду выполнить в этом таске
            dag = dag, 
        )
        t1.doc_md = dedent(''' `ghhg`, *ghgh*, **GH**
         # 
         _hgh_''')

    for i in range(10, 30):

        def print_task_number(i,**kwargs):
            return(f'task number is: {i}')
    

        t2 = PythonOperator(
            task_id=f"task_number{i}",  # нужен task_id, как и всем операторам
            python_callable=print_task_number, # свойственен только для PythonOperator - передаем саму функцию
            op_kwargs={'print_task_number':i},
        )
        t2.doc_md = dedent(''' `ghhg`, *ghgh*, **GH**
         # 
         _hgh_''')
    