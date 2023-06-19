'''
`airflow.model.Variable` -- это реестр пользовательских
глобальных перменных в AirFlow.
'''
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


def get_var():
    print(Variable.get('is_startml'))


dag_params = {
    'dag_id': 'xxa12-variables',
    'description': 'Реестр пользовательских глобальных переменных',
    'start_date': datetime(2023, 6, 7),
    'schedule_interval': timedelta(days=10),
    'default_args': {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    'catchup': False,
    'tags': ['xxa'],
}

with DAG(**dag_params) as dag:

    PythonOperator(
        task_id='is_startml',
        python_callable=get_var,
    )


if __name__ == '__main__':
    # AirFlow 2.6.2
    # https://airflow.apache.org/docs/apache-airflow/2.6.2/core-concepts/executor/debug.html
    #dag.test()

    # AirFlow 2.2.4
    # https://airflow.apache.org/docs/apache-airflow/2.2.4/executor/debug.html
    from airflow.utils.state import State
    dag.clear(dag_run_state=State.NONE)
    dag.run()
