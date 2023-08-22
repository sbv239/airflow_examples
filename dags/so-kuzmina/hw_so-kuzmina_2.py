from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from datetime import timedelta
# Устанавливаем аргументы для DAG
default_args={
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 21),
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}


# Создаем объект DAG
dag = DAG('my_dag', default_args=default_args, schedule_interval=None)

# Оператор BashOperator
bash_task = BashOperator(
    task_id='bash_task',
    bash_command='pwd',  # Команда, которую нужно выполнить
    dag=dag
)

# Функция для PythonOperator
def print_context(ds, **kwargs):
    print(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'

# Оператор PythonOperator
python_task = PythonOperator(
    task_id='python_task',
    python_callable=print_context,
    op_args=[ '{{ ds }}' ],  # Аргумент для функции print_ds
    provide_context=True,  # Передача контекста для доступа к переменным окружения
    dag=dag
)

# Определение порядка выполнения задач
bash_task >> python_task
