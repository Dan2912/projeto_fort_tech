if len(df_novos) > 0:
    hoje = date.today().isoformat()
    novo_arquivo = f"bronze/base_vendas/data={hoje}/vendas_{hoje}.parquet"

    df_novos.to_parquet(
        f"s3://{bucket}/{novo_arquivo}",
        index=False,
        engine='pyarrow',
        filesystem=fs
    )
    print(f"✅ Novos dados salvos em: s3://{bucket}/{novo_arquivo}")
else:
    print("✅ Nenhum novo registro para salvar.")
