from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime
from textwrap import dedent


def python_task(task_number):
    print(f"task number is: {task_number}")
    return 'ok'


with DAG(

        'DAG_3_sv-ljubushkina',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },

        description='Example2.',

        schedule_interval=timedelta(days=10),

        start_date=datetime(2022, 4, 1),

        catchup=False,

        tags=['DAG_3_sv-ljubushkina-7'],
) as dag:
    for i in range(0, 10):
        t1 = BashOperator(
            task_id='bash_' + str(i),
            bash_command=f"echo {i}",
        )

    for i in range(10, 30):
        t2 = PythonOperator(
            task_id='python_' + str(i),
            python_callable=python_task,
            op_kwargs={'task_number': i},
        )

    t1.doc_md = """

    #This is a documentation placed anywhere

    `code 1`

     ```code 2```

    *text1*

    **text2**

    _italic_

    """

    t2.doc_md = """

    #This is a documentation placed anywhere

    `code 1`

    ```code 2```

    *text1*

    **text2**

    _italic_

    """

    t1 >> t2