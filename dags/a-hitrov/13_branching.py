'''
# Ветвление графе AirFlow

`BranchPythonOperator` обязательно возвращает идентификатор
той задачи, которая должна выполняться после него. Кроме того,
соответствующая задача должна быть в графе связана с текущей.

`ShortCircuitOperator` возвращает булевское значение,
определяющее будут ли выполняться задачи, связанные
в ориентированном графе с текущей.
'''
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import ShortCircuitOperator


def determine_course():
    if Variable.get('is_startml') == 'True':
        return 'startml_desc'
    return 'not_startml_desc'


def startml_desc():
    print('StartML is a starter course for ambitious people')


def not_startml_desc():
    print('Not a startML course, sorry')


dag_params = {
    'dag_id': 'xxa13-branching',
    'description': 'Операторы ветвления',
    'doc_md': __doc__,
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

    start = DummyOperator(task_id='before_branching')
    dc = BranchPythonOperator(task_id='determine_course', python_callable=determine_course)
    sml = PythonOperator(task_id='startml_desc', python_callable=startml_desc)
    nsml = PythonOperator(task_id='not_startml_desc', python_callable=not_startml_desc)
    end = DummyOperator(task_id='after_branching')

    start >> dc >> [sml, nsml] >> end


if __name__ == '__main__':
    # AirFlow 2.6.2
    # https://airflow.apache.org/docs/apache-airflow/2.6.2/core-concepts/executor/debug.html
    #dag.test()

    # AirFlow 2.2.4
    # https://airflow.apache.org/docs/apache-airflow/2.2.4/executor/debug.html
    from airflow.utils.state import State
    dag.clear(dag_run_state=State.NONE)
    dag.run()
