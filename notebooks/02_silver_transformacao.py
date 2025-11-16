# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Silver - Transformacao e Limpeza
# MAGIC ## Dados limpos, validados e enriquecidos

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Carregar Dados da Camada Bronze

# COMMAND ----------

from pyspark.sql.functions import *

df_produtos_bronze = spark.table("bronze.produtos")
df_clientes_bronze = spark.table("bronze.clientes")
df_vendas_bronze = spark.table("bronze.vendas")
df_inventario_bronze = spark.table("bronze.inventario")

print("OK - Dados Bronze carregados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Transformar Produtos (Bronze -> Silver)

# COMMAND ----------

df_produtos_silver = df_produtos_bronze \
    .dropDuplicates(["id_produto"]) \
    .filter(col("id_produto").isNotNull()) \
    .filter(col("preco_unitario") > 0) \
    .withColumn("margem_lucro", 
                round((col("preco_unitario") - col("custo_unitario")) / col("preco_unitario") * 100, 2)) \
    .withColumn("categoria_normalizada", upper(trim(col("categoria")))) \
    .withColumn("data_processamento", current_timestamp()) \
    .select(
        "id_produto",
        "nome_produto",
        "categoria_normalizada",
        "preco_unitario",
        "custo_unitario",
        "margem_lucro",
        "fornecedor",
        "data_processamento"
    )

print(f"OK - Produtos Silver: {df_produtos_silver.count()} registros")
display(df_produtos_silver.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Transformar Clientes (Bronze -> Silver)

# COMMAND ----------

df_clientes_silver = df_clientes_bronze \
    .dropDuplicates(["id_cliente"]) \
    .filter(col("id_cliente").isNotNull()) \
    .withColumn("data_cadastro_dt", to_date(col("data_cadastro"), "yyyy-MM-dd")) \
    .withColumn("tempo_cliente_dias", 
                datediff(current_date(), col("data_cadastro_dt"))) \
    .withColumn("cidade_normalizada", upper(trim(col("cidade")))) \
    .withColumn("segmento_normalizado", upper(trim(col("segmento")))) \
    .withColumn("data_processamento", current_timestamp()) \
    .select(
        "id_cliente",
        "nome_cliente",
        "cidade_normalizada",
        "data_cadastro_dt",
        "tempo_cliente_dias",
        "segmento_normalizado",
        "data_processamento"
    )

print(f"OK - Clientes Silver: {df_clientes_silver.count()} registros")
display(df_clientes_silver.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Transformar Vendas (Bronze -> Silver)

# COMMAND ----------

df_vendas_silver = df_vendas_bronze \
    .dropDuplicates(["id_venda"]) \
    .filter(col("id_venda").isNotNull()) \
    .filter(col("quantidade") > 0) \
    .filter(col("valor_total") > 0) \
    .withColumn("data_venda_dt", to_date(col("data_venda"), "yyyy-MM-dd")) \
    .withColumn("ano", year(col("data_venda_dt"))) \
    .withColumn("mes", month(col("data_venda_dt"))) \
    .withColumn("dia_semana", dayofweek(col("data_venda_dt"))) \
    .withColumn("nome_dia_semana", date_format(col("data_venda_dt"), "EEEE")) \
    .withColumn("preco_unitario_venda", round(col("valor_total") / col("quantidade"), 2)) \
    .withColumn("metodo_pagamento_normalizado", upper(trim(col("metodo_pagamento")))) \
    .withColumn("data_processamento", current_timestamp()) \
    .select(
        "id_venda",
        "id_produto",
        "id_cliente",
        "data_venda_dt",
        "ano",
        "mes",
        "dia_semana",
        "nome_dia_semana",
        "quantidade",
        "valor_total",
        "preco_unitario_venda",
        "metodo_pagamento_normalizado",
        "data_processamento"
    )

print(f"OK - Vendas Silver: {df_vendas_silver.count()} registros")
display(df_vendas_silver.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Transformar Inventario (Bronze -> Silver)

# COMMAND ----------

df_inventario_silver = df_inventario_bronze \
    .dropDuplicates(["id_produto"]) \
    .filter(col("id_produto").isNotNull()) \
    .withColumn("data_atualizacao_dt", to_date(col("data_atualizacao"), "yyyy-MM-dd")) \
    .withColumn("status_estoque", 
                when(col("quantidade_estoque") < col("estoque_minimo"), "CRITICO")
                .when(col("quantidade_estoque") > col("estoque_maximo"), "EXCESSO")
                .otherwise("NORMAL")) \
    .withColumn("percentual_estoque", 
                round((col("quantidade_estoque") / col("estoque_maximo")) * 100, 2)) \
    .withColumn("data_processamento", current_timestamp()) \
    .select(
        "id_produto",
        "quantidade_estoque",
        "estoque_minimo",
        "estoque_maximo",
        "status_estoque",
        "percentual_estoque",
        "data_atualizacao_dt",
        "data_processamento"
    )

print(f"OK - Inventario Silver: {df_inventario_silver.count()} registros")
display(df_inventario_silver.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Salvar Tabelas Silver (Delta Lake)

# COMMAND ----------

df_produtos_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.produtos")
df_clientes_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.clientes")
df_vendas_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.vendas")
df_inventario_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.inventario")

print("OK - Tabelas Silver criadas com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verificar Qualidade dos Dados

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'silver.produtos' as tabela, COUNT(*) as registros FROM silver.produtos
# MAGIC UNION ALL
# MAGIC SELECT 'silver.clientes', COUNT(*) FROM silver.clientes
# MAGIC UNION ALL
# MAGIC SELECT 'silver.vendas', COUNT(*) FROM silver.vendas
# MAGIC UNION ALL
# MAGIC SELECT 'silver.inventario', COUNT(*) FROM silver.inventario

# COMMAND ----------

# MAGIC %md
# MAGIC ## Camada Silver Concluida!
# MAGIC 
# MAGIC **Dados limpos e enriquecidos:**
# MAGIC - silver.produtos (com margem de lucro)
# MAGIC - silver.clientes (com tempo de cliente)
# MAGIC - silver.vendas (com dimensoes temporais)
# MAGIC - silver.inventario (com status de estoque)
# MAGIC 
# MAGIC **Proximo passo**: Execute o notebook **03_gold_agregacao**
