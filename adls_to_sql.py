from airflow import DAG
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import io

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

# Read CSV from Azure Data Lake
def read_csv_from_adls(container, blob):
    hook = WasbHook(wasb_conn_id='storage_id1')
    try:
        # This returns a string (not bytes), so no decode needed
        file_content = hook.read_file(container_name=container, blob_name=blob)
        
        # Log a small preview
        print("CSV preview:\n", file_content[:300])

        # Parse the CSV using pandas
        df = pd.read_csv(io.StringIO(file_content), quotechar='"')
        print("Parsed DataFrame:\n", df.head())

        return df.to_dict(orient='records')  # XCom-compatible
    except Exception as e:
        print(f"Error reading/parsing CSV: {e}")
        return []

# Load parsed data into Azure SQL
def load_csv_to_sql(**kwargs):
    records = kwargs['ti'].xcom_pull(task_ids='read_csv_task')
    if not records:
        raise ValueError("No data pulled from XCom. Check CSV reading step.")

    df = pd.DataFrame(records)
    print("DataFrame to load into SQL:\n", df.head())

    sql_hook = MsSqlHook(mssql_conn_id='sql_id2')
    engine = sql_hook.get_sqlalchemy_engine()

    df.to_sql('swiggyanalysis', con=engine, if_exists='append', index=False)
    print("Data successfully loaded to SQL table 'swiggyanalysis'.")

# Define the DAG
with DAG(
    'adls_to_sql_dag',
    default_args=default_args,
    schedule=None,
    catchup=False,
    description='Load CSV from ADLS Gen2 to Azure SQL',
) as dag:

    # Task: Read CSV from ADLS
    read_csv_task = PythonOperator(
        task_id='read_csv_task',
        python_callable=read_csv_from_adls,
        op_kwargs={
            'container': 'source',
            'blob': 'data/Swiggy_Analysis_Source_File.csv',
        },
    )

    # Task: Load CSV into SQL
    load_sql_task = PythonOperator(
        task_id='load_sql_task',
        python_callable=load_csv_to_sql,
    )

    # Set task dependencies
    read_csv_task >> load_sql_task
