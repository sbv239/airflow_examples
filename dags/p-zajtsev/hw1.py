from airflow import DAG
from datetime import timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG('tutorial',
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}
) as dag:
    t1 = BashOperator(task_id = 'hw_2_p-zajtsev',
                      bash_command = 'pwd')
    
    def start_ (ds, **kwargs)
        return ds
    
    t2 = PythonOperator(task_id = hw_2_1_p-zajtsev,
                        python_callable = = start_)

