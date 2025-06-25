"""
etl_bronze.py
--------------
Pipeline incremental: RDS ➜ S3 (camada Bronze)

Este script:
1. Conecta ao banco PostgreSQL (RDS)
2. Lê os dados da tabela de vendas
3. Compara com o Parquet existente no S3 (Bronze)
4. Identifica vendas novas
5. Salva somente as novas em formato Parquet (particionado por data)

Requisitos:
- boto3, pandas, pyarrow, s3fs, psycopg2-binary, python-dotenv
"""

import os
import logging
from datetime import date

import pandas as pd
import psycopg2
import s3fs
from dotenv import load_dotenv

# === CONFIGURAÇÃO DO LOG ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)

# === FUNÇÕES AUXILIARES ===

def conectar_rds():
    """Conecta ao banco RDS PostgreSQL usando variáveis de ambiente"""
    try:
        conn = psycopg2.connect(
            host=os.environ["RDS_HOST"],
            database=os.environ["RDS_DB"],
            user=os.environ["RDS_USER"],
            password=os.environ["RDS_PASSWORD"],
            port=os.getenv("RDS_PORT", "5432")
        )
        logging.info("✅ Conectado ao RDS com sucesso.")
        return conn
    except Exception as e:
        logging.error(f"❌ Erro ao conectar no RDS: {e}")
        raise

def ler_vendas_rds(conn):
    """Lê todos os dados da tabela de vendas"""
    query = "SELECT * FROM vendas"
    df = pd.read_sql(query, conn)
    logging.info(f"📦 Registros lidos do RDS: {len(df)}")
    return df

def ler_vendas_existentes_s3(fs, bucket, caminho):
    """Lê arquivo parquet existente no S3"""
    try:
        df = pd.read_parquet(f"s3://{bucket}/{caminho}", filesystem=fs)
        logging.info(f"📂 Registros já existentes no S3: {len(df)}")
        return df
    except FileNotFoundError:
        logging.warning("⚠️ Arquivo Parquet inicial não encontrado. Criando novo.")
        return pd.DataFrame()

def salvar_novos_dados(fs, bucket, df):
    """Salva novos registros no S3 particionado por data"""
    hoje = date.today().isoformat()
    caminho = f"bronze/base_vendas/data={hoje}/vendas_{hoje}.parquet"
    df.to_parquet(f"s3://{bucket}/{caminho}", index=False, engine="pyarrow", filesystem=fs)
    logging.info(f"✅ Novos dados salvos em: s3://{bucket}/{caminho}")

# === EXECUÇÃO PRINCIPAL ===

def main():
    load_dotenv()  # Carrega .env local

    # S3
    bucket = os.environ["AWS_S3_BUCKET"]
    caminho_parquet = "bronze/base_vendas/carga_inicial/vendas_01.parquet"
    fs = s3fs.S3FileSystem(anon=False)

    # RDS
    conn = conectar_rds()
    df_rds = ler_vendas_rds(conn)
    conn.close()

    # S3 - Vendas existentes
    df_s3 = ler_vendas_existentes_s3(fs, bucket, caminho_parquet)

    # Filtrar apenas novos registros
    ids_existentes = set(df_s3["id_venda"]) if not df_s3.empty else set()
    df_novos = df_rds[~df_rds["id_venda"].isin(ids_existentes)]
    logging.info(f"🆕 Novos registros encontrados: {len(df_novos)}")

    # Salvar se houver novos
    if not df_novos.empty:
        salvar_novos_dados(fs, bucket, df_novos)
    else:
        logging.info("✅ Nenhum novo registro para salvar.")

if __name__ == "__main__":
    main()



