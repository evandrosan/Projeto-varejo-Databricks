# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Bronze - Ingestao de Dados Brutos
# MAGIC ## Dados raw sem transformacao (apenas ingestao)

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Gerar Dados Simulados (Fonte de Dados)

# COMMAND ----------

from pyspark.sql.functions import *
from datetime import datetime, timedelta
import random

# Gerar dados de produtos
categorias = ["Eletronicos", "Roupas", "Alimentos", "Livros", "Casa"]
produtos_data = []

for i in range(1, 101):
    categoria = random.choice(categorias)
    produtos_data.append({
        "id_produto": i,
        "nome_produto": f"Produto_{i}",
        "categoria": categoria,
        "preco_unitario": round(random.uniform(10, 500), 2),
        "custo_unitario": round(random.uniform(5, 250), 2),
        "fornecedor": f"Fornecedor_{random.randint(1, 10)}"
    })

df_produtos_raw = spark.createDataFrame(produtos_data)
print(f"OK - Produtos gerados: {df_produtos_raw.count()}")

# COMMAND ----------

# Gerar dados de clientes
cidades = ["Sao Paulo", "Rio de Janeiro", "Belo Horizonte", "Brasilia", "Curitiba"]
clientes_data = []

for i in range(1, 501):
    clientes_data.append({
        "id_cliente": i,
        "nome_cliente": f"Cliente_{i}",
        "cidade": random.choice(cidades),
        "data_cadastro": (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
        "segmento": random.choice(["Bronze", "Prata", "Ouro"])
    })

df_clientes_raw = spark.createDataFrame(clientes_data)
print(f"OK - Clientes gerados: {df_clientes_raw.count()}")

# COMMAND ----------

# Gerar dados de vendas
vendas_data = []
data_inicio = datetime(2024, 5, 1)

for i in range(1, 10001):
    id_produto = random.randint(1, 100)
    id_cliente = random.randint(1, 500)
    quantidade = random.randint(1, 10)
    data_venda = data_inicio + timedelta(days=random.randint(0, 180))
    preco = round(random.uniform(10, 500), 2)
    
    vendas_data.append({
        "id_venda": i,
        "id_produto": id_produto,
        "id_cliente": id_cliente,
        "data_venda": data_venda.strftime("%Y-%m-%d"),
        "quantidade": quantidade,
        "valor_total": round(preco * quantidade, 2),
        "metodo_pagamento": random.choice(["Cartao", "Dinheiro", "PIX"])
    })

df_vendas_raw = spark.createDataFrame(vendas_data)
print(f"OK - Vendas geradas: {df_vendas_raw.count()}")

# COMMAND ----------

# Gerar dados de inventario
inventario_data = []

for i in range(1, 101):
    inventario_data.append({
        "id_produto": i,
        "quantidade_estoque": random.randint(0, 500),
        "estoque_minimo": random.randint(10, 50),
        "estoque_maximo": random.randint(200, 1000),
        "data_atualizacao": datetime.now().strftime("%Y-%m-%d")
    })

df_inventario_raw = spark.createDataFrame(inventario_data)
print(f"OK - Inventario gerado: {df_inventario_raw.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Salvar na Camada Bronze (Delta Lake)

# COMMAND ----------

# Adicionar metadados de ingestao
df_produtos_bronze = df_produtos_raw \
    .withColumn("data_ingestao", current_timestamp()) \
    .withColumn("fonte", lit("sistema_erp"))

df_clientes_bronze = df_clientes_raw \
    .withColumn("data_ingestao", current_timestamp()) \
    .withColumn("fonte", lit("sistema_crm"))

df_vendas_bronze = df_vendas_raw \
    .withColumn("data_ingestao", current_timestamp()) \
    .withColumn("fonte", lit("sistema_vendas"))

df_inventario_bronze = df_inventario_raw \
    .withColumn("data_ingestao", current_timestamp()) \
    .withColumn("fonte", lit("sistema_wms"))

print("OK - Metadados adicionados")

# COMMAND ----------

# Salvar como tabelas Delta na camada Bronze
df_produtos_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bronze.produtos")

df_clientes_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bronze.clientes")

df_vendas_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bronze.vendas")

df_inventario_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("bronze.inventario")

print("OK - Tabelas Bronze criadas com sucesso!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificar Tabelas Bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'bronze.produtos' as tabela, COUNT(*) as registros FROM bronze.produtos
# MAGIC UNION ALL
# MAGIC SELECT 'bronze.clientes', COUNT(*) FROM bronze.clientes
# MAGIC UNION ALL
# MAGIC SELECT 'bronze.vendas', COUNT(*) FROM bronze.vendas
# MAGIC UNION ALL
# MAGIC SELECT 'bronze.inventario', COUNT(*) FROM bronze.inventario

# COMMAND ----------

# MAGIC %md
# MAGIC ## Camada Bronze Concluida!
# MAGIC 
# MAGIC **Dados brutos ingeridos:**
# MAGIC - bronze.produtos
# MAGIC - bronze.clientes
# MAGIC - bronze.vendas
# MAGIC - bronze.inventario
# MAGIC 
# MAGIC **Proximo passo**: Execute o notebook **02_silver_transformacao**
