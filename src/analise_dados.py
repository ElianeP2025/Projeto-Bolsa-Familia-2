from leitura_dados import ler_dados
from tratamento_dados import tratar_dados
from pyspark.sql.functions import sum, avg

# Caminho do arquivo
CAMINHO_CSV = "dados/pagamentos.csv"

# 1. Leitura
df = ler_dados(CAMINHO_CSV)

# 2. Tratamento
df_tratado = tratar_dados(df)

df_tratado.show(5)
df_tratado.printSchema()


# 3. Análise
df_tratado.agg(
    sum("valor").alias("total_pago"),
    avg("valor").alias("media_pagamento")
).show(5)

df_tratado.groupBy("UF") \
    .agg(sum("valor").alias("total_pago")) \
    .orderBy("total_pago", ascending=False) \
    .show(5)