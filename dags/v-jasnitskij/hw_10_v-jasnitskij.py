from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
#from airflow import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args={
    'start_date': datetime(2022, 11, 18),
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}


def push_var(ti):
    #var = ti.xcom_push(key="sample_xcom_key", value="Airflow tracks everything")
    return "Airflow tracks everything"

def pull_var(ti):
    var = ti.xcom_pull(task_ids="push", key="return_value")
    print(var)

with DAG(
    "hw_10_v-jasnitskij_dag",
    default_args=default_args,
    schedule_interval=None
) as dag:
    python_push = PythonOperator(
        task_id="push",
        python_callable=push_var,
        dag=dag
    )

    python_pull = PythonOperator(
        task_id='pull',
        python_callable=pull_var,
        dag=dag
        #op_kwargs={"sample_xcom_key": "xcom test"}
    )

    python_push >> python_pull