from __future__ import annotations

from airflow.decorators import dag, task

import pendulum
import os
import pandas as pd

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.empty import EmptyOperator

stocks_dir = "/opt/airflow/archive/stocks/"
etfs_dir = "/opt/airflow/archive/etfs/"
dir_for_modified_data = "/opt/airflow/archive/modified_data/"
dir_for_stats = "/opt/airflow/archive/stats_data/"
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
    first_half = stocks_file_name[:len(stocks_file_name)//2]
    second_half = stocks_file_name[len(stocks_file_name)//2:]
    
    @task
    def make_dir_result():
        if os.path.isdir(dir_for_modified_data) == False:
            os.mkdir(dir_for_modified_data)


    def modified_data(**kwargs):
        modified_csv_names = os.listdir(dir_for_modified_data)
        
        list_length = len(modified_csv_names)
        # Calculate the size of each sublist
        sublist_size = list_length // 6
        # Calculate the remainder of the list length divided by 4
        remainder = list_length % 6
        # Divide the list into 4 sublists, with different sizes if necessary

        part1 = modified_csv_names[:sublist_size+remainder]
        part2 = modified_csv_names[sublist_size+remainder:2*sublist_size+remainder]
        part3 = modified_csv_names[2*sublist_size+remainder:3*sublist_size+remainder]
        part4 = modified_csv_names[3*sublist_size+remainder:4*sublist_size+remainder]
        part5 = modified_csv_names[4*sublist_size+remainder:5*sublist_size+remainder]
        part6 = modified_csv_names[5*sublist_size+remainder:]
        kwargs['task_instance'].xcom_push(key='list', value=modified_csv_names)
        kwargs['task_instance'].xcom_push(key='part1', value=part1)
        kwargs['task_instance'].xcom_push(key='part2', value=part2)
        kwargs['task_instance'].xcom_push(key='part3', value=part3)
        kwargs['task_instance'].xcom_push(key='part4', value=part4)
        kwargs['task_instance'].xcom_push(key='part5', value=part5)
        kwargs['task_instance'].xcom_push(key='part6', value=part6)
  
    @task
    def moving_avg_days(keys,dir,**kwargs):
        name_list = kwargs['ti'].xcom_pull(key=keys,task_ids=['passing_list_between_task']) 
        print(name_list)
        window_size = 30
        for names in name_list:
            for name  in names:
                csv_read = pd.read_csv(f"{dir}"+f"/{name}") 
                csv_read['vol_moving_avg'] = csv_read.groupby('symbol')['Volume'].rolling(window_size).mean().reset_index(level=0, drop=True)
                csv_read['adj_close_rolling_med'] = csv_read.groupby('symbol')['Adj Close'].rolling(window_size).median().reset_index(level=0, drop=True)
                csv_read[["Open","High","Close","Adj Close","Volume"]] = csv_read[["Open","High","Close","Adj Close","Volume"]].astype('float64')
                csv_read.to_csv(f"{dir_for_stats}"+f"{name}",index= False)
    
    list_passing = PythonOperator(
        task_id = 'passing_list_between_task',
        python_callable=modified_data,
        provide_context=True
    )
    
    # making this task paralle
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
        task_id="merger_manupilate_csv",
        bash_command=f"[ ! -d {final_data_path} ] &&  mkdir -p {final_data_path} ; {{ head -n 1 {dir_for_modified_data}A.csv ; tail -q -n +2 {dir_for_modified_data}*.csv ; }} > {final_data_path}merged_file.csv"
    )  

    merger_csv_1 = BashOperator(
        task_id="merger_stats_csv",
        bash_command=f"[ ! -d {final_data_path} ] &&  mkdir -p {final_data_path} ; {{ head -n 1 {dir_for_stats}A.csv ; tail -q -n +2 {dir_for_stats}*.csv ; }} > {final_data_path}stats_merged_file.csv"
    )  

    # variables for writers
    in_path = f'{final_data_path}merged_file.csv'
    stats_in_path=f'{final_data_path}stats_merged_file.csv'
    data_manupilation_path = f'{final_data_path}converted_file.parquet'
    stats_pq =f'{final_data_path}stats.parquet' 


    @task
    def csv_to_parquet(in_path, out_path):
        import pyarrow as pa
        import pyarrow.parquet as pq
        import pyarrow.csv

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
    


    run_this = BashOperator(
        task_id="run_after_loop",
        bash_command=f"head /opt/airflow/archive/etfs/AAAU.csv",
    )

    empty = EmptyOperator(
        task_id="empty"
    )

    run_this >> make_dir_result() >> [ data_manupilation(etfs_file_name, "ETFs",etfs_dir) , data_manupilation(first_half, "stocks", stocks_dir), data_manupilation(second_half, "stocks", stocks_dir) ] >> list_passing >> [ moving_avg_days('part1', dir_for_modified_data), moving_avg_days('part2', dir_for_modified_data), moving_avg_days('part3', dir_for_modified_data), moving_avg_days('part4', dir_for_modified_data) ,moving_avg_days('part5', dir_for_modified_data), moving_avg_days('part6', dir_for_modified_data) ] >> empty >> [ merger_csv >> merger_csv_1 ] >> empty >> [ csv_to_parquet(in_path, data_manupilation_path) , csv_to_parquet(stats_in_path, stats_pq) ] >> print_head()


if __name__ == "__main__":
    dag.test()