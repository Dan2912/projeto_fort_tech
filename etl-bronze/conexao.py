import os
import logging
import psycopg2

def conectar_rds():
    """Conecta ao RDS PostgreSQL usando variáveis de ambiente"""
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

