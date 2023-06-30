"""
Test documentation
"""
from textwrap import dedent
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'hw_fe-denisenko-21_4_step',
        default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='DAG',
    start_date=datetime(2023, 6, 24),
    catchup=False,
    tags=['example'],
) as dag:
    def print_context(task_number):
        print(f"task number is: {task_number}")
    for i in range(10):
        t1 = BashOperator(
        task_id='echo_id' + str(i),  # id, будет отображаться в интерфейсе
        bash_command=f'echo,{i}',  # какую bash команду выполнить в этом таске
        )
      # свойственен только для PythonOperator - передаем саму функцию
    for i in range(20):
        run_this = PythonOperator(
            task_id='print_' + str(i),  # нужен task_id, как и всем операторам
            python_callable=print_context,
            op_kwargs = {"task_number": i}
        )
    t1.doc_md = dedent(
        """\
        #### New indent/ paragraph
        **bold text has fat lines**
        *italic text in Russian sounds like a pointer of your computer mouse*
        > Blockquots are similar to code but don't
        `import airflow` is a code type os text
        """
    )
    run_this.doc_md = dedent(
        """\
        #### New indent/ paragraph
        **bold text has fat lines**
        *italic text in Russian sounds like a pointer of your computer mouse*
        > Blockquots are similar to code but don't
        `import airflow` is a code type os text
        """
    )
    t1 >> run_this
