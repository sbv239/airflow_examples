from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

state = 'wa'

def set_xcom_value(state, ti):
    ti.xcom_push(
        key="sample_xcom_key",
        value="xcom test"
    )


def get_and_pring_xcom_value(state, ti):
    value = ti.xcom_pull(
        key="sample_xcom_key",
        task_ids=f'set_xcom_value_{state}'
    )
    print(value)


with DAG(
        'chemelson_hw9',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='chemelson_hw9',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 9, 11),
        catchup=False,
        tags=['chemelson_hw9']
) as dag:
    opr_set_xcom_value = PythonOperator(
        task_id=f'set_xcom_value_{state}',
        python_callable=set_xcom_value,
        op_kwargs={'state': state}
    )
    opr_get_and_pring_xcom_value = PythonOperator(
        task_id='get_and_pring_xcom_value',
        python_callable=get_and_pring_xcom_value,
        op_kwargs={'state': state}
    )

    opr_set_xcom_value >> opr_get_and_pring_xcom_value
