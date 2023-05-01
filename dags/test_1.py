from __future__ import annotations

import datetime

from airflow.decorators import dag, task

import threading
import pendulum
import os
import pandas as pd
import dask.dataframe as dd

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator




from pyarrow import csv, parquet
import dask.dataframe as dd

stocks_dir = "/opt/airflow/archive/stocks/"
etfs_dir = "/opt/airflow/archive/etfs/"
dir_for_modified_data = "/opt/airflow/archive/modified_data/"
final_data_path = "/opt/airflow/archive/final_data/"

with DAG(
    dag_id="big_query_test",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False
) as dag:
    big_con = "googlebigquery"
    big_pro = "ronakpatel"
    big_data = "test"
    test  = BigQueryOperator(
    task_id ="big_test",
    gcp_conn_id = big_con,
    sql="""select 1;""",

    
    )
    
    test
             
if __name__ == "__main__":
    dag.test()