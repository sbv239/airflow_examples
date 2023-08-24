from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Определение аргументов DAG
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
    'hw_s-frangulidi-23_5',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2023, 8, 24),
)

bash_command_template = """
for i in {0..4}; do
    echo "Value of ts: {{ ds }} {{ ts }}"
    echo "Value of run_id: {{ run_id }}"
done
"""

# Задача BashOperator с шаблонизированной командой
bash_task = BashOperator(
    task_id='bash_template_task',
    bash_command=bash_command_template,
    dag=dag,
)

# Графическое представление зависимостей задачи
bash_task