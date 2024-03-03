#%%
import boto3
from botocore.exceptions import ClientError
import logging
from dotenv import load_dotenv
import os

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Configurando o registro para exibir mensagens de depuração
logging.basicConfig(level=logging.DEBUG)

class S3Uploader:
    def __init__(self, region_name='us-east-1'):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
            region_name=region_name
        )

    def upload_object(self, file_name, bucket, object_name=None):
        if object_name is None:
            object_name = os.path.basename(file_name)

        try:
            self.s3_client.upload_file(file_name, bucket, object_name)
            logging.info(f"Upload bem-sucedido de '{file_name}' para o bucket '{bucket}' com o nome '{object_name}'")
            return True
        except ClientError as e:
            logging.error(f"Falha ao fazer upload de '{file_name}' para o bucket '{bucket}' com o nome '{object_name}': {e}")
            return False

# Exemplo de uso:
if __name__ == "__main__":
    uploader = S3Uploader()

    # Substitua 'nome-do-arquivo' e 'nome-do-bucket' pelos valores desejados
    arquivo = '/home/igor-ubuntu/projetos/warehouse-low-cost/data/football_data_2024-03-02.parquet'
    bucket = 'football-data-igor-0301'

    uploader.upload_object(arquivo, bucket)
