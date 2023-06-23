from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

with DAG(
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)}
        , description='test Gryaznov'
        , schedule_interval=timedelta(days=1)
        , start_date=datetime(2023, 6, 20)
        , catchup=False
        , tags=['example']
        , dag_id='dag_3_agryaznov')as dag:
    def task_number_f(task_number):
        print(f"task number is: {task_number}")
        return 'Whatever you return gets printed in the logs'
    for i in range(30):
        if i<=10:
            bash_gryaz_2 = BashOperator(
            task_id='gryaz_bash'+str(i)
            , bash_command=f"echo {i}")
        else:
            py_gryaz_2 = PythonOperator(task_id='gryaz_py'+str(i)
                                ,python_callable=task_number_f
                                , op_kwargs={'number_task:': task_number_f(i)})
    bash_gryaz_2 >> py_gryaz_2