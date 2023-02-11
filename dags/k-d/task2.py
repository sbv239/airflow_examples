"""
Напишите DAG, который будет содержать BashOperator и PythonOperator. 
В функции PythonOperator примите аргумент ds и распечатайте его. 
Можете распечатать дополнительно любое другое сообщение.

В BashOperator выполните команду pwd, которая выведет директорию, где выполняется ваш код Airflow. 
Результат может оказаться неожиданным, 
не пугайтесь - Airflow может запускать ваши задачи на разных машинах или контейнерах с разными настройками и путями по умолчанию.

Сделайте так, чтобы сначала выполнялся BashOperator, потом PythonOperator. 
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator 
from datetime import timedelta, datetime


with DAG (
    'k-d',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'description text',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2023,1,1),
    catchup=False
) as dag:
    
    bo = BashOperator(
        task_id = 'bash_operator_task_2',
        bash_command = 'pwd '
    )

    def foo(ds, **kwargs):
        return (ds)

    po = PythonOperator(
        task_id = 'python_operator_task_2',
        python_callable = foo
    )

    bo >> po
