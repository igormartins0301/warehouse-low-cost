#%%
import os
from dotenv import load_dotenv
import duckdb

def query_s3_duckdb(bucket_name):
    try:
        # Carrega as variáveis de ambiente do arquivo .env
        load_dotenv()

        # Conecta ao DuckDB e carrega o arquivo Parquet
        conn = duckdb.connect()
        conn.execute("INSTALL httpfs")
        conn.execute("LOAD httpfs")
        conn.execute(f"CREATE SECRET (TYPE S3, KEY_ID '{os.getenv('AWS_ACCESS_KEY_ID')}', SECRET '{os.getenv('AWS_SECRET_ACCESS_KEY')}', REGION 'us-east-1');")
        conn.execute(f"CREATE TABLE data AS SELECT * FROM read_parquet('s3://{bucket_name}/*.parquet');")

        # Consulta os dados
        result = conn.execute("SELECT * FROM data;")
        for row in result.fetchall():
            print(row)


        # Limpeza
        conn.close()
    except duckdb.DuckDBError as e:
        print(f"Error querying data from S3: {e}")


query_s3_duckdb('football-data-igor-0301')
# %%
