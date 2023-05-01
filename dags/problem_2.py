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
    dag_id="problem_2",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False
) as dag:
    
   run_this = BashOperator(
        task_id="run_after_loop",
        bash_command=f"head /opt/airflow/archive/etfs/AAAU.csv",
    )
   dir_for_modified_data = "/opt/airflow/archive/modified_data/"
   final_data_path = "/opt/airflow/archive/final_data/"
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
    


   @task 
   def moving_average():
        csv_file = f'{final_data_path}merged_file.csv'
        parquet_file = f'{final_data_path}new.parquet'
        import dask.dataframe as dd

# Set the chunksize parameter to read data in smaller chunks
        chunksize = "10MB"

        # Read the Parquet file into a Dask dataframe with the given chunksize
        df = dd.read_parquet(f"{final_data_path}converted_file.parquet", chunksize=chunksize,columns=["Volume","Date","symbol"])
        print(df.compute())
    # Set the file paths

#         import pyarrow.parquet as pq
#         import pandas as pd

#         # Set the size of each chunk to read from the Parquet file
#         chunk_size = 1000000

# # Open the Parquet file in read mode
#         with pq.read_table(f'{final_data_path}converted_file.parquet') as file:
#             # Get the total number of rows in the file
#             total_rows = file.metadata.num_rows

#             # Initialize a list to hold the results of the moving average calculation
#             results = []

#             # Iterate over the file in chunks
#             for i in range(0, total_rows, chunk_size):
#                 # Read a chunk of data from the file
#                 table = file.read_row_group(i, min(chunk_size, total_rows - i)).to_pandas()

#                 # Calculate the 30-day moving average for each symbol using Pandas rolling() function
#                 table['vol_moving_avg_30_days'] = table.groupby('symbol')['Volume'].rolling(window='30D').mean().reset_index(drop=True)

#                 # Add the results to the list
#                 results.append(table)

#         # Concatenate the results into a single DataFrame
#         df = pd.concat(results)

#         # Write the result back to the same Parquet file
#         df.to_parquet(f'{final_data_path}file.parquet')

        


        

 
                
run_this >> csv_to_parquet() >> moving_average()

if __name__ == "__main__":
    dag.test()