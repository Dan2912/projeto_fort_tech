# Identificar IDs que já existem no parquet
ids_existentes = set(df_s3['id_venda'])

# Filtrar novos registros do RDS
df_novos = df_rds[~df_rds['id_venda'].isin(ids_existentes)]

print(f"🆕 Novos registros encontrados: {len(df_novos)}")
