import datetime
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator


BASH_COMMAND_TEMPLATE = dedent('''

    {% for i in range(5) %}
        echo {{ ts }}
    {% endfor %}
    echo {{ run_id }}

''')

dag_params = {
    'dag_id': 'xxa05-templates',
    'description': 'Разбираемся с использованием в AirFlow шаблонного движка Jinja',
    'start_date': datetime.datetime(2023, 6, 19),
    'schedule_interval': datetime.timedelta(weeks=7),
    'default_args': {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': datetime.timedelta(minutes=1),
    },
    'catchup': False,
    'tags': ['xxa'],
}

with DAG(**dag_params) as dag:
    BashOperator(
        task_id='use_template_engine',
        bash_command=BASH_COMMAND_TEMPLATE,
    )
