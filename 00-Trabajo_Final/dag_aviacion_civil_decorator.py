from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta

URIs = [
    'https://edvaibucket.blob.core.windows.net/data-engineer-edvai/2021-informe-ministerio.csv?sp=r&st=2023-11-06T12:59:46Z&se=2025-11-06T20:59:46Z&sv=2022-11-02&sr=b&sig=%2BSs5xIW3qcwmRh5TTmheIY9ZBa9BJC8XQDcI%2FPLRe9Y%3D',
    'https://edvaibucket.blob.core.windows.net/data-engineer-edvai/202206-informe-ministerio.csv?sp=r&st=2023-11-06T12:52:39Z&se=2025-11-06T20:52:39Z&sv=2022-11-02&sr=c&sig=J4Ddi2c7Ep23OhQLPisbYaerlH472iigPwc1%2FkG80EM%3D',
    'https://edvaibucket.blob.core.windows.net/data-engineer-edvai/aeropuertos_detalle.csv?sp=r&st=2023-11-06T12:52:39Z&se=2025-11-06T20:52:39Z&sv=2022-11-02&sr=c&sig=J4Ddi2c7Ep23OhQLPisbYaerlH472iigPwc1%2FkG80EM%3D',
    ]
LANDING_dir = '/home/hadoop/landing'    # NO Trailing backslash
INGEST_dir = '/ingest'
TRANSFORM_script = '/home/hadoop/scripts/transform_aviacion.py'

@dag(
    schedule = "@once",
    start_date = datetime(2021, 12, 1),
    dagrun_timeout = timedelta(minutes=60),
    catchup = False,
)
def taskflow():

    # @task.bash(task_id="clear_landing")
    # def clear_landing(landing: str) -> str:
    #     return f"rm -f {landing}*.*"

    @task.bash(task_id="extract", retries=2)
    def get_files(url: str, landing: str) -> str:
        f"rm -f {landing}/*.*"
        return f"wget -P {landing} {url}"

    # clear_landing(LANDING_dir)
    get_files_status = [get_files(url=i, landing=LANDING_dir) for i in URIs]
    LoggingMixin().log.info(get_files_status)

    @task.bash(task_id="ingest", retries=2)
    def ingest_files(landing: str, ingest: str) -> str:
        f"/home/hadoop/hadoop/bin/hdfs dfs -rm {ingest}/*.*"
        return f"/home/hadoop/hadoop/bin/hdfs dfs -put {landing}/*.* {ingest}"

    ingest_files_status = ingest_files(landing=LANDING_dir, ingest=INGEST_dir)
    LoggingMixin().log.info(ingest_files_status)

    @task.bash(task_id="transform_load", retries=2)
    def transform_load(transform__script: str) -> str:
        return f"ssh hadoop@172.17.0.2 /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml {transform__script}"

    transform_load_status = transform_load(TRANSFORM_script)
    LoggingMixin().log.info(transform_load_status)

    run_this_last = EmptyOperator(task_id="run_this_last", trigger_rule=TriggerRule.ALL_DONE)

    transform_load(ingest_files(get_files()))

taskflow()


# @task.pyspark(conn_id="spark-local")
# def spark_task(spark: SparkSession, sc: SparkContext) -> pd.DataFrame:
#     df = spark.createDataFrame(
#         [
#             (1, "John Doe", 21),
#             (2, "Jane Doe", 22),
#             (3, "Joe Bloggs", 23),
#         ],
#         ["id", "name", "age"],
#     )
#     df.show()

#     return df.toPandas()
