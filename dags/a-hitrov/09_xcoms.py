'''
Дегустируем XCom (cross-communications) -- механизм передачи
небольших данных между задачами AirFlow. Если необходимо
обмениваться объемными данными, то надо смотреть в сторону
S3, HDFS и баз данных.
'''
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


SRC_TASK_ID = 'src'
XCOM_KEY = 'sample_xcom_key'


def xcom_src_func(ti):  # `ti` -- task instance (airflow builtin variable)
    ti.xcom_push(key=XCOM_KEY, value='xcom test')


def xcom_dst_func(**kwargs):  # Get `ti` from `kwargs`
    #value = ti.xcom_pull(key=XCOM_KEY, task_ids=SRC_TASK_ID)
    value = kwargs['ti'].xcom_pull(key=XCOM_KEY, task_ids=SRC_TASK_ID)
    print(value)


dag_params = {
    'dag_id': 'xxa09-xcoms',
    'description': 'Передача небольших данных между задачами с помощью XComs',
    'start_date': datetime(2023, 6, 7),
    'schedule_interval': timedelta(days=365),
    'default_args': {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    'catchup': True,
    'tags': ['xxa'],
}

with DAG(**dag_params) as dag:

    task1 = PythonOperator(
        task_id=SRC_TASK_ID,
        python_callable=xcom_src_func,
    )

    task2 = PythonOperator(
        task_id='dst',
        python_callable=xcom_dst_func,
    )

    task1 >> task2


if __name__ == '__main__':
    # AirFlow 2.6.2
    # https://airflow.apache.org/docs/apache-airflow/2.6.2/core-concepts/executor/debug.html
    #dag.test()

    # AirFlow 2.2.4
    # https://airflow.apache.org/docs/apache-airflow/2.2.4/executor/debug.html
    from airflow.utils.state import State
    dag.clear(dag_run_state=State.NONE)
    dag.run()
