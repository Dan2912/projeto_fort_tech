import logging
import pandas as pd

def ler_vendas_rds(conn):
    query = "SELECT * FROM vendas"
    df = pd.read_sql(query, conn)
    logging.info(f"📦 Registros lidos do RDS: {len(df)}")
    return df

def ler_vendas_existentes_s3(fs, bucket, caminho):
    try:
        df = pd.read_parquet(f"s3://{bucket}/{caminho}", filesystem=fs)
        logging.info(f"📂 Registros já existentes no S3: {len(df)}")
        return df
    except FileNotFoundError:
        logging.warning("⚠️ Arquivo Parquet inicial não encontrado. Criando novo.")
        return pd.DataFrame()

