#!/usr/bin/env python3
"""
src/etl/silver/main.py
-----------------------
Pipeline de transformação: camada Bronze ➜ camada Silver (S3)

Este script:
1. Lê múltiplos arquivos Parquet da camada Bronze
2. Permite seleção de colunas manual ou automática
3. Aplica mascaramento em colunas sensíveis (ex.: cpf_cnpj)
4. Salva os dados tratados na camada Silver particionada por data

Execução:
$ export AWS_ACCESS_KEY_ID=...
$ export AWS_SECRET_ACCESS_KEY=...
$ python -m etl.silver.main \
    --bucket data-lake-eletro-fort \
    --bronze-prefix bronze/base_vendas/ \
    --bronze-subfolders carga_inicial data=2025-06-21 \
    --silver-prefix silver/base_vendas/ \
    --interactive-columns

Pasta recomendada no GitHub:
├── src/
│   └── etl/
│       ├── __init__.py
│       └── silver/
│           ├── main.py        # script principal
│           ├── leitura.py     # leitura dos Parquets Bronze
│           ├── transform.py   # escolha de colunas e mascaramento
│           └── escrita.py     # gravação na Silver
"""

from __future__ import annotations

import argparse
import os
import re
from datetime import datetime
from typing import List

import pandas as pd
import s3fs
from etl.silver.leitura import ler_parquets_bronze
from etl.silver.transform import selecionar_colunas, mascarar_cpf_cnpj
from etl.silver.escrita import salvar_em_silver


def parse_args():
    parser = argparse.ArgumentParser(description="ETL Bronze ➜ Silver (AWS S3)")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--bronze-prefix", required=True)
    parser.add_argument("--bronze-subfolders", nargs='+', required=True)
    parser.add_argument("--silver-prefix", required=True)
    parser.add_argument("--columns", help="Colunas a manter (CSV)")
    parser.add_argument("--interactive-columns", action="store_true")
    parser.add_argument("--mask-column", default="cpf_cnpj")
    return parser.parse_args()


def main():
    args = parse_args()
    fs = s3fs.S3FileSystem(anon=False)

    # 1. Carrega arquivos Bronze
    df_bronze = ler_parquets_bronze(fs, args.bucket, args.bronze_prefix, args.bronze_subfolders)
    print(f"📦 Registros totais: {len(df_bronze):,}")

    # 2. Selecionar colunas
    colunas = selecionar_colunas(df_bronze, args.columns, args.interactive_columns)
    df_silver = df_bronze[colunas].copy()

    # 3. Mascarar dados sensíveis
    df_silver = mascarar_cpf_cnpj(df_silver, args.mask_column)

    # 4. Preview
    print("\n🧪 Pré-visualização dos dados:")
    print(df_silver.head(10))

    # 5. Salvar na Silver
    caminho = salvar_em_silver(fs, df_silver, args.bucket, args.silver_prefix)
    print(f"\n🚀 Dados salvos em: {caminho}")


if __name__ == "__main__":
    main()

