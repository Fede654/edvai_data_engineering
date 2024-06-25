from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

args = {
    'owner' : 'airflow',
}

with DAG(
    dag_id="rental_parent",
    default_args=args,
    schedule_interval='@once',
    start_date=datetime(2020, 1, 1),
    dagrun_timeout=timedelta(minutes=60),
    tags=['ingest','parent'],
) as parent_dag:

    begin = EmptyOperator(
        task_id='begin_processing',
    )

    ingest = BashOperator(
        task_id='ingest_rental',
        # Don't drop the space at the end of the command or Jinja will fail
        bash_command='/usr/bin/sh /home/hadoop/scripts/ingest_rental.sh ',
    )

    transform_child_task = TriggerDagRunOperator(
        task_id="transform_child_task",
        trigger_dag_id='rental_child',
        wait_for_completion=False,
        reset_dag_run=True,
    )

    end = EmptyOperator(
        task_id='end_processing',
    )

    begin >> ingest >> transform_child_task >> end
