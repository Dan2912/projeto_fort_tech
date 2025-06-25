#!/usr/bin/env python3
"""
src/etl/bronze/main.py
-----------------------
Pipeline incremental: RDS ➜ S3 (camada Bronze)

Este script:
1. Conecta ao banco PostgreSQL (RDS)
2. Lê os dados da tabela de vendas
3. Compara com o Parquet existente no S3 (Bronze)
4. Identifica vendas novas
5. Salva somente os novos em formato Parquet (particionado por data)

Execução:
$ export RDS_HOST=...
$ export RDS_DB=...
$ export RDS_USER=...
$ export RDS_PASSWORD=...
$ export AWS_ACCESS_KEY_ID=...
$ export AWS_SECRET_ACCESS_KEY=...
$ python -m etl.bronze.main

Estrutura recomendada no GitHub:
etl-bronze/
├── README.md
├── .env.example
├── requirements.txt
├── .gitignore
├── src/
│   └── etl/
│       ├── __init__.py
│       └── bronze/
│           ├── main.py         # script principal (este)
│           ├── conexao.py      # conexão com RDS
│           ├── leitura.py      # leitura do RDS e do Parquet S3
│           ├── comparador.py   # identificar novos registros
│           └── escrita.py      # gravação incremental no S3
"""
