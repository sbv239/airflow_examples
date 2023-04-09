from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.operators.bash import BashOperator

def print_task(task_number_is):
    print(f'task number is: {task_number_is}')


with DAG(
    'HW_3_v-patrakeev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    start_date=datetime.now(),
) as dag:

    # t1, t2, t3 - это операторы (они формируют таски, а таски формируют даг)

   for i in range(10):
        t1 = BashOperator(
            task_id='example ' + str(i),  # id, будет отображаться в интерфейсе
            bash_command=f"echi {i}",
        )

   for i in range(20):
        t2 =PythonOperator(
            task_id = 'task' + str(i+10),
            python_callable=print_task,
            op_kwargs ={"task_number_is": i+10}
        )

   t1 >> t2