from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta,datetime
with DAG(
    'hw_5_s-birjukov'
    ,default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}
) as dag:
    #Этот оператор должен  использовать шаблонизированную команду следующего вида:
    #"Для каждого i в диапазоне от 0 до 5 не включительно распечатать значение ts и затем распечатать значение run_id".
    #Здесь ts и run_id - это шаблонные переменные (вспомните, как в лекции подставляли шаблонные переменные).
    templated_command = dedent(
            """
        {% for i in range(5) %}
            echo "{{ ds }}"
            echo "{{ macros.ds_add(ds, 7)}}"
        {% endfor %}
        """
    )

t1 = BashOperator(
    task_id = 'Templating_DAG',
    bash_command = templated_command
)

t1