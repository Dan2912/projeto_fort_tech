import logging
from datetime import date

def salvar_novos_dados(fs, bucket, df):
    hoje = date.today().isoformat()
    caminho = f"bronze/base_vendas/data={hoje}/vendas_{hoje}.parquet"
    df.to_parquet(f"s3://{bucket}/{caminho}", index=False, engine="pyarrow", filesystem=fs)
    logging.info(f"✅ Novos dados salvos em: s3://{bucket}/{caminho}")

