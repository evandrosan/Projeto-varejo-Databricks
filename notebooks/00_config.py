# Databricks notebook source
# MAGIC %md
# MAGIC # Configuracao do Projeto
# MAGIC ## Definicao de Paths e Schemas para Arquitetura Medalhao

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configurar Paths das Camadas

# COMMAND ----------

# Definir paths para as camadas Bronze, Silver e Gold
BRONZE_PATH = "/mnt/varejo/bronze"
SILVER_PATH = "/mnt/varejo/silver"
GOLD_PATH = "/mnt/varejo/gold"

# Criar databases para cada camada
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS silver")
spark.sql("CREATE DATABASE IF NOT EXISTS gold")

print("OK - Databases criados:")
print("  - bronze: Dados brutos (raw)")
print("  - silver: Dados limpos e validados")
print("  - gold: Dados agregados para analise")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuracoes Globais

# COMMAND ----------

# Configuracoes do Spark
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

# Configuracoes Delta
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

print("OK - Configuracoes do Spark aplicadas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Schemas das Tabelas

# COMMAND ----------

from pyspark.sql.types import *

# Schema para Produtos
schema_produtos = StructType([
    StructField("id_produto", IntegerType(), False),
    StructField("nome_produto", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("preco_unitario", DoubleType(), True),
    StructField("custo_unitario", DoubleType(), True),
    StructField("fornecedor", StringType(), True)
])

# Schema para Clientes
schema_clientes = StructType([
    StructField("id_cliente", IntegerType(), False),
    StructField("nome_cliente", StringType(), True),
    StructField("cidade", StringType(), True),
    StructField("data_cadastro", StringType(), True),
    StructField("segmento", StringType(), True)
])

# Schema para Vendas
schema_vendas = StructType([
    StructField("id_venda", IntegerType(), False),
    StructField("id_produto", IntegerType(), True),
    StructField("id_cliente", IntegerType(), True),
    StructField("data_venda", StringType(), True),
    StructField("quantidade", IntegerType(), True),
    StructField("valor_total", DoubleType(), True),
    StructField("metodo_pagamento", StringType(), True)
])

# Schema para Inventario
schema_inventario = StructType([
    StructField("id_produto", IntegerType(), False),
    StructField("quantidade_estoque", IntegerType(), True),
    StructField("estoque_minimo", IntegerType(), True),
    StructField("estoque_maximo", IntegerType(), True),
    StructField("data_atualizacao", StringType(), True)
])

print("OK - Schemas definidos")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Funcoes Auxiliares

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import lit, col

def adicionar_metadados(df, camada):
    """Adiciona colunas de metadados ao DataFrame"""
    return df \
        .withColumn("data_processamento", lit(datetime.now())) \
        .withColumn("camada", lit(camada))

def validar_dados(df, coluna_id):
    """Valida dados basicos"""
    # Remove duplicatas
    df_limpo = df.dropDuplicates([coluna_id])
    
    # Remove nulos na chave primaria
    df_limpo = df_limpo.filter(col(coluna_id).isNotNull())
    
    return df_limpo

print("OK - Funcoes auxiliares definidas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuracao Concluida!
# MAGIC 
# MAGIC Proximo passo: Execute o notebook **01_bronze_ingestao**
