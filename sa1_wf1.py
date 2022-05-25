from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

def my_function():
    return ""

with DAG(
    'ODY-TS1-DEV',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='ODY Tivoli SChedule 1',
    schedule_interval='0 16 * * *',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    t1 = FileSensor(
        task_id='FileWatcher',
        poke_interval=30
    )

    t2 = PythonOperator(
        task_id='DataStage1',
        python_callable= my_function
    )

    t3 = PythonOperator(
        task_id='DataStage2',
        python_callable= my_function
    )

    t4 = DummyOperator(
        task_id='OtherTask',
        retries=3
    )
    
    t5 = trigger_dependent_dag = TriggerDagRunOperator(
        task_id="trigger_dependent_dag",
        trigger_dag_id="dependent-dag",
        wait_for_completion=True
    )

    t6 = BashOperator(
        task_id='EmailNotification',
        depends_on_past=False,
        bash_command='sleep 5',
    )


    t1 >> t2 >> t3 >> t4 >> t5 >> t6
