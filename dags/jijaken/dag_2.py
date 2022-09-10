from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG('jijaken_dag_2'
        ,default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}) as dag:
    def context_print(ds):
        print(ds)
        return ds

    t1 = BashOperator(task_id='pwd_bash',
                      bash_command= 'pwd')\
    t2 = PythonOperator(task_id='print_python',
                        python_callable=context_print)


    t1 >> t2
