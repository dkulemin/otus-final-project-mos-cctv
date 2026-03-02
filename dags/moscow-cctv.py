from typing import List
import datetime
import json
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import requests


TOP_ROWS = 1000


def ingest(name: str, id: int, fields: List[str]):
    api_key = Variable.get("API_KEY")

    with open("/app/data/{name}.json".format(name=name)) as file:
        last_row_num = json.loads(file.readlines()[-1])['Number']

    rows = requests.get(
        url="https://apidata.mos.ru/v1/datasets/{id}/count".format(id=id),
        params={"api_key": api_key}
    ).json()
    for i in range(last_row_num, rows, TOP_ROWS):
        data = [
            {"Number": resp["Number"] + i, **resp["Cells"]}
            for resp
            in requests.post(
                url="https://apidata.mos.ru/v1/datasets/{id}/rows".format(id=id),
                json=fields,
                params={
                    "api_key": api_key,
                    "$skip": i,
                    "$top": TOP_ROWS,
                    "$inlinecount": "allpages",
                }
            ).json()
        ]
        with open("/app/data/{name}.json".format(name=name), "a") as file:
            for row in data:
                file.write(json.dumps(row, ensure_ascii=False) + '\n')
            logging.info("Ingested %s rows" % len(data))


with DAG(
    dag_id = "moscow-cctv",
    start_date = datetime.datetime(2026, 1, 1),
    schedule = None,
    catchup = False,
    default_args = {"retries" : 0}
) as dag:
    ingest_objects = PythonOperator(
        task_id = "ingest_objects",
        python_callable=ingest,
        op_kwargs={
            "name": "objects_3_1598",
            "id": 60562,
            "fields": ["UNOM", "ADDRESS", "SIMPLE_ADDRESS", "DISTRICT", "ADM_AREA", "geoData"]
        }
    )

    ingest_cameras = PythonOperator(
        task_id = "ingest_cameras",
        python_callable=ingest,
        op_kwargs={
            "name": "cameras",
            "id": 1500,
            "fields": ["ID", "AdmArea", "District", "Address", "SimpleAddress", "UNOM", "geoData"]
        }
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

    [ingest_objects, ingest_cameras] >> cctv_pg_write  # type: ignore
