from dotenv import load_dotenv
import logging
import os
import s3fs
from etl.bronze.conexao import conectar_rds
from etl.bronze.leitura import ler_vendas_rds, ler_vendas_existentes_s3
from etl.bronze.escrita import salvar_novos_dados

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(message)s")

def main():
    load_dotenv()

    bucket = os.environ["AWS_S3_BUCKET"]
    caminho_parquet = "bronze/base_vendas/carga_inicial/vendas_01.parquet"
    fs = s3fs.S3FileSystem(anon=False)

    conn = conectar_rds()
    df_rds = ler_vendas_rds(conn)
    conn.close()

    df_s3 = ler_vendas_existentes_s3(fs, bucket, caminho_parquet)

    ids_existentes = set(df_s3["id_venda"]) if not df_s3.empty else set()
    df_novos = df_rds[~df_rds["id_venda"].isin(ids_existentes)]

    logging.info(f"🆕 Novos registros encontrados: {len(df_novos)}")

    if not df_novos.empty:
        salvar_novos_dados(fs, bucket, df_novos)
    else:
        logging.info("✅ Nenhum novo registro para salvar.")

if __name__ == "__main__":
    main()

