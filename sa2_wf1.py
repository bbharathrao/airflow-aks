from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator

def my_function():
    return ""

with DAG(
    'LCB-TS1-DEV',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='LCB Tivoli SChedule 1',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    t1 = FileSensor(
        task_id='FileWatcher',
        poke_interval=30,
        filepath=''
    )
    
    t7 = ExternalTaskSensor(
    task_id='external_task_sensor',
    poke_interval=60,
    timeout=180,
    soft_fail=False,
    retries=2,
    external_task_id='task_to_be_sensed',
    external_dag_id='ODY-TS1-DEV',
    dag=dag
    )

    
    t2 = PythonOperator(
        task_id='DataStage1',
        python_callable= my_function
    )
    
    t3 = PythonOperator(
        task_id='DataStage2',
        python_callable= my_function
    )
    
    t4 = PythonOperator(
        task_id='DataStage3',
        python_callable= my_function
    )

    t5 = DummyOperator(
        task_id='OtherTask',
        retries=3
    )

    t6 = BashOperator(
        task_id='EmailNotification',
        depends_on_past=False,
        bash_command='sleep 5',
    )


    t1 >> t2 >> [t3 >> t4] >> t5 >> t6
