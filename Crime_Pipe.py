from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import boto3
import requests
from io import StringIO
import logging 

s3_client = boto3.client('s3')

target_bucket_name = 'redfinyanseijibucket'

url_by_city = 'https://data.lacity.org/resource/2nrs-mtv8.csv'
chunk_size = 10000

# Define the extract_data function
def extract_data(**kwargs):
    url = kwargs['url']
    
    
    # Initialize an empty DataFrame
    df = pd.DataFrame()

    # Start from index 0
    offset = 0

    while True:
        # Build the URL with offset and limit parameters
        api_url = f"{url}?$offset={offset}&$limit={chunk_size}"

        # Make the request to the API
        response = requests.get(api_url)

        # If the response is unsuccessful, terminate the loop
        if response.status_code != 200:
            break

        # Read the CSV data from the response
        content = StringIO(response.text)
        chunk_df = pd.read_csv(content)

        # Concatenate the data to the main DataFrame
        df = pd.concat([df, chunk_df], ignore_index=True)

        # Update the offset for the next request
        offset += chunk_size

        # If the number of lines read is less than chunk_size, it means there is no more data
        if len(chunk_df) < chunk_size:
            break

    # Save the DataFrame to a CSV file
    now = datetime.now()
    date_now_string = now.strftime("%d%m%Y%H%M%S")
    file_str = 'Crime_data_' + date_now_string
    df.to_csv(f"{file_str}.csv", index=False)
    output_file_path = f"/home/ubuntu/{file_str}.csv"
    out_list = [output_file_path, file_str]
    
    return out_list 

def transform_data(**kwargs):
    # Retrieve the output file path and DataFrame from the previous task
    output_file_path, file_str = kwargs['ti'].xcom_pull(task_ids='tsk_extract_Crime_data')


    # Read the CSV data into a DataFrame
    df = pd.read_csv(output_file_path)

    # Drop rows where 'mocodes' column is null
    df = df.dropna(subset=['mocodes'])

    # Fill missing values in 'vict_sex' and 'vict_descent' with 'Unknown'
    df['vict_sex'].fillna(value='Unknown', inplace=True)
    df['vict_descent'].fillna(value='Unknown', inplace=True)

    # Columns to be removed
    colunm_a_remove = ['crm_cd_2', 'crm_cd_3', 'crm_cd_4', 'cross_street', 'weapon_used_cd', 'weapon_desc']
    # Remove specified columns
    df = df.drop(columns=colunm_a_remove)

    # Drop rows with any remaining null values
    df = df.dropna()

    # Split the 'mocodes' column into separate columns with regular expression capturing digits
    split_col_mocode = df['mocodes'].str.extractall('(\d+)').unstack()

    # Dynamically rename the columns
    new_cols_mocode = [f'mocode{i}' for i in range(1, split_col_mocode.shape[1] + 1)]
    split_col_mocode.columns = new_cols_mocode

    # Add the new columns to the original DataFrame
    df = pd.concat([df, split_col_mocode], axis=1)

    # Columns to keep, only 'mocode1'
    colunas_a_manter = ['mocode1']
    # Columns to exclude, all 'mocode' columns except 'mocode1'
    colunas_a_excluir = [col for col in df.columns if 'mocode' in col and col not in colunas_a_manter]
    # Remove columns
    df = df.drop(columns=colunas_a_excluir)

    # Convert 'date_rptd' and 'date_occ' to datetime type
    df['date_rptd'] = pd.to_datetime(df['date_rptd'], errors='coerce')
    df['date_occ'] = pd.to_datetime(df['date_occ'], errors='coerce')

    # Format the datetime columns to show only year-month-day
    df['date_rptd'] = df['date_rptd'].dt.strftime('%Y-%m-%d')
    df['date_occ'] = df['date_occ'].dt.strftime('%Y-%m-%d')

    # Convert 'time_occ' to datetime and format to show only hours and minutes
    df['time_occ'] = df['time_occ'].astype(str).apply(lambda x: x.ljust(2, '0'))
    df['time_occ'] = pd.to_datetime(df['time_occ'], format='%H%M', errors='coerce').dt.strftime('%H:%M')

    # Convert 'premis_cd' and 'crm_cd_1' to integer data type
    df['premis_cd'] = df['premis_cd'].astype(int)
    df['crm_cd_1'] = df['crm_cd_1'].astype(int)

    # Continue with additional transformations if needed...

    # Save the transformed DataFrame to a new CSV file
    
    transformed_csv_data = df.to_csv(index=False)
    transformed_object_key = f"{file_str}_transformed.csv"
    s3_client.put_object(Bucket=target_bucket_name, Key=transformed_object_key, Body=transformed_csv_data)

    # Pass the path of the transformed file to the next task
    return transformed_csv_data


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 25),
    'email': 'xyz@xyz.com',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=5),
}

with DAG(
    'Crime_Pipe_Dag',
    default_args=default_args,
    schedule_interval='@weekly',
    catchup=False,
) as dag:

    extract_data = PythonOperator(
        task_id='tsk_extract_Crime_data',
        python_callable=extract_data,
        op_kwargs={'url': url_by_city},
    )

    transform_data = PythonOperator(
        task_id='tsk_transform_Crime_data',
        python_callable=transform_data,
    )

    
    load_to_s3 = BashOperator(
        task_id='tsk_load_to_s3',
        bash_command='aws s3 mv {{ ti.xcom_pull("tsk_extract_Crime_data")[0] }} s3://store-raw-data-redfin-yan',
        dag=dag
    )

# Set task dependencies
extract_data >> transform_data >>  load_to_s3
