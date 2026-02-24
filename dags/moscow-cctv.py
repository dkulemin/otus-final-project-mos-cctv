import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator

with DAG(
    dag_id = "moscow-cctv",
    start_date = datetime.datetime(2026, 1, 1),
    schedule = None,
    catchup = False,
    default_args = {"retries" : 0}
) as dag:

    bash_ls = BashOperator(
        task_id="bash_ls",
        bash_command="ls /app/data"
    )

    cctv_pg_write = SparkSubmitOperator(
        task_id = "transform_and_load",
        application = "/app/target/scala-2.12/mos-cctv-assembly-1.0.jar",
        conn_id = "spark_standalone_client",
        java_class="MosCCTV",
        executor_cores=12,
        total_executor_cores=12,
        executor_memory="4G",
        verbose=True,
        files="/app/data/cameras.json,/app/data/objects_3_1598.json"
    )
