from airflow import DAG
from airflow.providers.amazon.aws.transfers.sftp_to_s3 import SFTPToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.odbc.hooks.odbc import OdbcHook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd 
import urllib
from sqlalchemy import create_engine

SFTP_CONN_ID = "sftp_default"
S3_CONN_ID = "aws_default"
MSSQL_CONN_ID = "sql_server_default"
"mssql+pyodbc://user:pass@host:port/db?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"

S3_BUCKET = "aws-s3-poc-airflow"
S3_KEY = "uploads/produtos.csv"
SFTP_PATH = "upload/arquivo_teste.csv"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def load_s3_to_sql_server_bulk(): 
    s3_hook = S3Hook(aws_conn_id = S3_CONN_ID)

    odbc_hook = OdbcHook(odbc_conn_id=MSSQL_CONN_ID)
    conn_params = odbc_hook.get_connection(MSSQL_CONN_ID)
    
    server_address = 'host.docker.internal'
    
    connection_string = (
        f"DRIVER={{ODBC Driver 18 for SQL Server}};"
        f"SERVER={server_address},1433;"  # Porta com vírgula é crucial para o driver
        "DATABASE=airflow;"               # Conecte no master primeiro para testar
        "UID=sa;"
        f"PWD=Poc_Senha_123!;"
        "Encrypt=no;"                    # Desativa a criptografia obrigatória
        "TrustServerCertificate=yes;"
        "Connection Timeout=30;"         # Nome completo do parâmetro de timeout
    )
    
    # Converte para um formato que o SQLAlchemy aceita
    params = urllib.parse.quote_plus(connection_string)
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

    temp_file_path = s3_hook.download_file(key=S3_KEY, bucket_name=S3_BUCKET)

    chunk_size = 100000

    print(f"Iniciando carga do arquivo {temp_file_path} para o SQL Server")

    for i, chunk in enumerate(pd.read_csv(temp_file_path, chunksize=chunk_size)):
        chunk.to_sql(
            name="produtos",
            con=engine,
            if_exists='append' if i > 0 else 'replace',
            index=False
        )

with DAG('poc_sftp_to_s3_to_sqlserver',
         default_args = default_args,
         schedule=None,
         catchup= False
) as dag: 
    
    #TASK 1 SFTP TO S3 
    step1_sftp_to_s3 = SFTPToS3Operator(
        task_id='transferir_sftp_para_s3',
        sftp_conn_id = SFTP_CONN_ID,
        sftp_path = SFTP_PATH,
        s3_conn_id = S3_CONN_ID,
        s3_bucket = S3_BUCKET, 
        s3_key = S3_KEY,
        use_temp_file = True
    )

    #TASK 2 S3 TO SQL
    step2_s3_to_sql = PythonOperator(
        task_id = 'carregar_s3_no_sql_server',
        python_callable = load_s3_to_sql_server_bulk
    )


    step1_sftp_to_s3 >> step2_s3_to_sql