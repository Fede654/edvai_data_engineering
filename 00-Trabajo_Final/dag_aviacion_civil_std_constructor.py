from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

dag = DAG(
    'aviacion-000',
    schedule_interval='@once',
    start_date=datetime(2020, 1, 1),
    dagrun_timeout=timedelta(minutes=60),
    tags=['ingest', 'transform'],
)

ingest = BashOperator(
    task_id='ingest',
    dag = dag,
    # Don't drop the space at the end of the command or Jinja will fail
    bash_command='/usr/bin/sh /home/hadoop/scripts/ingest_aviacion.sh ',
)

transform = BashOperator(
    task_id='transform',
    dag = dag,
    # Don't drop the space at the end of the command or Jinja will fail
    bash_command='ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/transform_aviacion.py ',
)

end_processing = EmptyOperator(
    task_id='end_processing',
    dag = dag,
)

ingest >> transform >> end_processing
