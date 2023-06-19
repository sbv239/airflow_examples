from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


SRC_TASK_ID = 'src'
DEFAULT_XCOM_KEY = 'return_value'


def xcom_src_func():
    return 'Airflow tracks everything'


def xcom_dst_func(ti):
    value = ti.xcom_pull(key=DEFAULT_XCOM_KEY, task_ids=SRC_TASK_ID)
    print(value)


dag_params = {
    'dag_id': 'xxa10-xcom-implicit-push',
    'description': 'Неявная передача данных в XCom при возврате функции',
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
