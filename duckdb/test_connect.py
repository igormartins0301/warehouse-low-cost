#%%
import boto3
import os
from dotenv import load_dotenv

# Carregue as variáveis de ambiente do .env
load_dotenv()

# Configure as credenciais
session = boto3.Session(
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name='us-east-1'  # substitua pela sua região
)

# Crie o cliente S3
s3_client = session.client('s3')

# Liste os buckets S3
response = s3_client.list_buckets()

# Imprima os nomes dos buckets
for bucket in response['Buckets']:
    print(bucket['Name'])