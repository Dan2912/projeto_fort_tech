ETL - Bronze Pipeline

Este projeto executa um pipeline incremental que extrai dados de um banco PostgreSQL (RDS), compara com arquivos existentes no Amazon S3 (camada Bronze) e salva apenas os registros novos em arquivos Parquet particionados por data.

Objetivo

Automatizar a ingesta incremental de dados do banco para a camada Bronze de um data lake em S3, com controle por id_venda.

Tecnologias Utilizadas

Python 3.10+

pandas

pyarrow

psycopg2-binary

boto3 + s3fs

AWS RDS (PostgreSQL)

Amazon S3
