import datetime
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain, cross_downstream


def pretty_print(task_number, ts, run_id):
    print(f'task number is: {task_number}')
    print(f'run_id: {run_id}')
    print(f'ts: {ts}')


def echo(i):
    return BashOperator(
        task_id=f'echo-{i}',
        bash_command='echo $NUMBER',
        env={'NUMBER': str(i)},
        doc_md=dedent('''

            # Задача на основе `BashOperator`

            **NB:** _Вынужены использовать в описании
            как можно больше разных типов выделения,
            в соответствии с условиями задачи 11.4._

            Задача использует консольную команду `echo`
            для вывода своего номера.

        '''),
    )


def pp(i):
    return PythonOperator(
        task_id=f'print-{i}', 
        python_callable=pretty_print,
        op_kwargs={'task_number': i},
        doc_md=dedent('''

            # Задача на основе `PythonOperator`

            **NB:** _Вынужены использовать в описании
            как можно больше разных типов выделения._

            Задача использует всю мощь *Python*,
            чтобы вывести свой порядковый номер.

        '''),
    )


dag_params = {
    'dag_id': 'xxa03-dynamic-tasks',
    'description': 'Динамические задачи',
    'start_date': datetime.datetime(2023, 6, 18),
    'schedule_interval': datetime.timedelta(days=1),
    'default_args': {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': datetime.timedelta(minutes=5),
    },
    'catchup': False,
    'tags': ['xxa'],
}

with DAG(**dag_params) as dag:

    a = [echo(i) for i in range(1, 9)]
    b = [pp(i) for i in range(9, 14)]
    c = pp(15)
    cross_downstream(a, b)
    cross_downstream(b, c)
    chain(
        c,

       [pp(16), pp(17)],
       [pp(18), pp(19)],

        pp(20),

       [pp(i) for i in range(21, 25)],
       [pp(i) for i in range(25, 29)],

      *[pp(i) for i in range(29, 31)]
    )

