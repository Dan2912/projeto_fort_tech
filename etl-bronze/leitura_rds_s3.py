import pandas as pd
import psycopg2
import boto3
import s3fs
from getpass import getpass
from datetime import date

# === ACESSO AO RDS ===
host = input("Host do RDS: ")
database = input("Nome do banco: ")
user = input("Usuário do RDS: ")
password = getpass("Senha do RDS: ")

# Conectar ao RDS
conn = psycopg2.connect(
    host=host,
    database=database,
    user=user,
    password=password,
    port='5432'
)

# Ler dados da tabela vendas (pode ser tudo ou só os últimos dias)
query = "SELECT * FROM vendas"
df_rds = pd.read_sql(query, conn)
conn.close()

# === ACESSO AO S3 ===
aws_access_key = getpass("AWS Access Key ID: ")
aws_secret_key = getpass("AWS Secret Access Key: ")
bucket = input("Bucket do S3: ").strip()
caminho_parquet = "bronze/base_vendas/carga_inicial/vendas_01.parquet"

# Criar sistema de arquivos S3 com boto3 + s3fs
fs = s3fs.S3FileSystem(
    key=aws_access_key,
    secret=aws_secret_key
)

# Ler parquet direto do S3
df_s3 = pd.read_parquet(f"s3://{bucket}/{caminho_parquet}", filesystem=fs)

print(f"🔎 Registros no RDS: {len(df_rds)}")
print(f"📦 Registros no parquet S3: {len(df_s3)}")
