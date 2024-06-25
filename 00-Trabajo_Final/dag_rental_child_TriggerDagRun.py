from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

args = {
    'owner' : 'airflow',
}

with DAG(
    dag_id="rental_child",
    default_args=args,
    schedule_interval=None,
    start_date=datetime(2020, 1, 1),
    dagrun_timeout=timedelta(minutes=60),
    tags=['transform','child'],
) as child_dag:

    begin = EmptyOperator(
        task_id='begin_processing',
    )

    transform_rental = BashOperator(
        task_id='transform_rental',
        # Don't drop the space at the end of the command or Jinja will fail
        bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/transform_rental.py ',
    )

    end = EmptyOperator(
        task_id='end_processing',
    )

    begin >> transform_rental >> end
