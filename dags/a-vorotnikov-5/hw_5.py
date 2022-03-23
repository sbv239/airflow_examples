from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def check_task_num(task_number):
    return print(f"task number is: {task_number}")


with DAG('hw_5_vorotnikov', default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}, start_date=datetime(2022, 3, 20), catchup=False) as dag:
    for i in range(1, 31):
        if i <= 10:
            task = BashOperator(task_id='BashOp_t_' + str(i), bash_command="echo $NUMBER", env={"NUMBER": i})
        else:
            task = PythonOperator(task_id='P_op_t_' + str(i), python_callable=check_task_num,
                                  op_kwargs={'task_number': int(i)})
