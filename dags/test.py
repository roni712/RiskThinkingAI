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
    dag_id="archive_ls",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False
) as dag:
    
    @task
    def print_head():
        from pyarrow.parquet import ParquetFile
        import pyarrow as pa 

        pf = ParquetFile(f'{final_data_path}converted_file.parquet') 
        first_ten_rows = next(pf.iter_batches(batch_size = 10)) 
        df = pa.Table.from_batches([first_ten_rows]).to_pandas() 
        print(df)


    """
    Gathering names of the file to make a data model to persist in on parquet file
    """
    etfs_file_name = [names for names in os.listdir(etfs_dir)]
    stocks_file_name = [names for names in os.listdir(stocks_dir)]
    
    @task
    def make_dir_result():
        if os.path.isdir(dir_for_modified_data) == False:
            os.mkdir(dir_for_modified_data)
    
    
    @task
    def data_manupilation(list, s_name, dir):
        for names in list:
            
            csv_read = pd.read_csv(f"{dir}"+f"/{names}") 
            
            name_manipulation = names.split('.',1)
            
            csv_read["symbol"] =  name_manipulation[0]

            csv_read["security_name"] = s_name

            csv_read[["Open","High","Close","Adj Close","Volume"]] = csv_read[["Open","High","Close","Adj Close","Volume"]].astype('float64')

            csv_read.to_csv(f"{dir_for_modified_data}"+f"{names}",index= False)

   
    merger_csv = BashOperator(
        task_id="merger_csv",
        bash_command=f"[ ! -d {final_data_path} ] &&  mkdir -p {final_data_path} ; {{ head -n 1 {dir_for_modified_data}A.csv ; tail -q -n +2 {dir_for_modified_data}*.csv ; }} > {final_data_path}merged_file.csv"
    )  

    @task
    def csv_to_parquet():
        import pyarrow as pa
        import pyarrow.parquet as pq
        import pyarrow.csv

        in_path = f'{final_data_path}merged_file.csv'
        out_path = f'{final_data_path}/converted_file.parquet'

        writer = None
        with pyarrow.csv.open_csv(in_path) as reader:
            for next_chunk in reader:
                if next_chunk is None:
                    break
                if writer is None:
                    writer = pq.ParquetWriter(out_path, next_chunk.schema)
                next_table = pa.Table.from_batches([next_chunk])
                writer.write_table(next_table)
        writer.close()
    

    # [START howto_operator_bash]
    run_this = BashOperator(
        task_id="run_after_loop",
        bash_command=f"head /opt/airflow/archive/etfs/AAAU.csv",
    )
    # [END howto_operator_bash]

    run_this >> make_dir_result() >> [ data_manupilation(etfs_file_name, "ETFs",etfs_dir) , data_manupilation(stocks_file_name, "stocks", stocks_dir) ] >> merger_csv >> csv_to_parquet() >> print_head()


if __name__ == "__main__":
    dag.test()