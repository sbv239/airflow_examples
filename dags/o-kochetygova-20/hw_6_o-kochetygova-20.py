from datetime import datetime, timedelta

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_task_6_o-kochetygova',
        # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        # Описание DAG (не тасок, а самого DAG)
        description='hw_task_6_o-kochetygova_DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 5, 31),
        catchup=False,
        tags=['task_6'],
) as dag:
    for figure in range(10):
        def echo_figure(figure):
            f"echo {figure}"

        task = BashOperator(
            task_id=f'print_date_{figure}',# нужен task_id, как и всем операторам
            bash_command='echo "doing magic $NUMBER"',
            env={"NUMBER": str(figure)}
        )

