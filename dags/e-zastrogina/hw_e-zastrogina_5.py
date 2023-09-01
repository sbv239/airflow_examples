from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta, datetime
from textwrap import dedent


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

dag = DAG(
    "hw_3_e-zastrogina",
    default_args=default_args,
    start_date=datetime(2023, 8, 23),
    catchup=False,
)

bc = dedent("""
{% for i in range(5) %}
    echo "{{ ts }}"
    echo "{{ run_id }}"
{% endfor %}
""")

print_bash = BashOperator(
    task_id=f"bash_zastrogina_hw_5",
    bash_command=bc,
    depends_on_past=False,
    dag=dag,
)
