from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def push_xcom(ti):
    ti.xcom_push(key="sample_xcom_key", value="xcom test")

def pull_and_print_xcom(ti):
    pulled_value = ti.xcom_pull(key="sample_xcom_key", task_ids="push_to_xcom_task")
    print(f"Pulled value from XCom: {pulled_value}")

# Определите аргументы для DAG
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

# Создайте объект DAG
dag = DAG(
    'hw_s-frangulidi-23_9',
    start_date=datetime(2023, 8, 24),
    default_args=default_args,
    schedule_interval=None,
)

# Создайте PythonOperator для записи в XCom
push_to_xcom_task = PythonOperator(
    task_id='push_to_xcom_task',
    python_callable=push_xcom,
    provide_context=True,  # Передача контекста
    dag=dag,
)

# Создайте PythonOperator для чтения из XCom и печати
pull_and_print_xcom_task = PythonOperator(
    task_id='pull_and_print_xcom_task',
    python_callable=pull_and_print_xcom,
    provide_context=True,  # Передача контекста
    dag=dag,
)

# Установите последовательность выполнения задач
push_to_xcom_task >> pull_and_print_xcom_task