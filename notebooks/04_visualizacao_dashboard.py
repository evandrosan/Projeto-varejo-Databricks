# Databricks notebook source
# MAGIC %md
# MAGIC # Dashboard e Visualizacoes
# MAGIC ## Analises visuais das tabelas Gold

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 6)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Dashboard Executivo - KPIs Principais

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.dashboard_executivo

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Top 10 Produtos por Receita

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   nome_produto,
# MAGIC   categoria,
# MAGIC   receita_total,
# MAGIC   total_vendido,
# MAGIC   margem_lucro_media
# MAGIC FROM gold.top_produtos
# MAGIC WHERE ranking_receita <= 10
# MAGIC ORDER BY ranking_receita

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Giro de Estoque - Produtos Criticos

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   nome_produto,
# MAGIC   categoria,
# MAGIC   giro_estoque,
# MAGIC   estoque_atual,
# MAGIC   status_estoque,
# MAGIC   total_vendido
# MAGIC FROM gold.kpi_giro_estoque
# MAGIC WHERE status_estoque = 'CRITICO'
# MAGIC ORDER BY giro_estoque DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Analise de Clientes Recorrentes

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   tipo_cliente,
# MAGIC   COUNT(*) as num_clientes,
# MAGIC   ROUND(AVG(valor_total_gasto), 2) as valor_medio_gasto,
# MAGIC   ROUND(AVG(ticket_medio), 2) as ticket_medio,
# MAGIC   ROUND(AVG(num_compras), 2) as media_compras
# MAGIC FROM gold.kpi_retorno_clientes
# MAGIC GROUP BY tipo_cliente

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Vendas por Categoria - Evolucao Mensal

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   CONCAT(ano, '-', LPAD(mes, 2, '0')) as periodo,
# MAGIC   categoria,
# MAGIC   receita_total
# MAGIC FROM gold.vendas_por_categoria
# MAGIC ORDER BY ano, mes, receita_total DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Analise por Segmento de Cliente

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   segmento,
# MAGIC   SUM(receita_total) as receita_total,
# MAGIC   SUM(num_vendas) as total_vendas,
# MAGIC   ROUND(AVG(ticket_medio), 2) as ticket_medio
# MAGIC FROM gold.vendas_por_segmento
# MAGIC GROUP BY segmento
# MAGIC ORDER BY receita_total DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Visualizacao Python - Receita por Categoria

# COMMAND ----------

df_categoria = spark.sql("""
  SELECT 
    categoria,
    SUM(receita_total) as receita
  FROM gold.vendas_por_categoria
  GROUP BY categoria
  ORDER BY receita DESC
""").toPandas()

plt.figure(figsize=(12, 6))
plt.bar(df_categoria['categoria'], df_categoria['receita'], color='#3498db', alpha=0.8)
plt.title('Receita Total por Categoria', fontsize=16, fontweight='bold')
plt.xlabel('Categoria', fontsize=12)
plt.ylabel('Receita (R$)', fontsize=12)
plt.xticks(rotation=45)
plt.grid(axis='y', alpha=0.3)

for i, v in enumerate(df_categoria['receita']):
    plt.text(i, v, f'R$ {v:,.0f}', ha='center', va='bottom', fontsize=10)

plt.tight_layout()
display(plt.show())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Visualizacao Python - Segmentacao de Clientes

# COMMAND ----------

df_segmento = spark.sql("""
  SELECT 
    segmento,
    SUM(receita_total) as receita,
    SUM(num_clientes) as clientes
  FROM gold.vendas_por_segmento
  GROUP BY segmento
""").toPandas()

fig, axes = plt.subplots(1, 2, figsize=(16, 6))

colors = ['#CD7F32', '#C0C0C0', '#FFD700']
axes[0].pie(df_segmento['receita'], labels=df_segmento['segmento'], 
            autopct='%1.1f%%', colors=colors, startangle=90)
axes[0].set_title('Distribuicao de Receita por Segmento', fontsize=14, fontweight='bold')

axes[1].bar(df_segmento['segmento'], df_segmento['clientes'], color=colors, alpha=0.8)
axes[1].set_title('Numero de Clientes por Segmento', fontsize=14, fontweight='bold')
axes[1].set_ylabel('Numero de Clientes', fontsize=12)
axes[1].grid(axis='y', alpha=0.3)

plt.tight_layout()
display(plt.show())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Analise de Giro de Estoque

# COMMAND ----------

df_giro = spark.sql("""
  SELECT 
    categoria,
    ROUND(AVG(giro_estoque), 2) as giro_medio,
    COUNT(*) as num_produtos
  FROM gold.kpi_giro_estoque
  GROUP BY categoria
  ORDER BY giro_medio DESC
""").toPandas()

plt.figure(figsize=(12, 6))
bars = plt.barh(df_giro['categoria'], df_giro['giro_medio'], color='#2ecc71', alpha=0.8)
plt.title('Giro de Estoque Medio por Categoria', fontsize=16, fontweight='bold')
plt.xlabel('Giro de Estoque', fontsize=12)
plt.ylabel('Categoria', fontsize=12)
plt.grid(axis='x', alpha=0.3)

for i, (bar, valor) in enumerate(zip(bars, df_giro['giro_medio'])):
    plt.text(valor, bar.get_y() + bar.get_height()/2, f'{valor:.2f}', 
             ha='left', va='center', fontsize=11, fontweight='bold')

plt.tight_layout()
display(plt.show())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dashboard Completo!
# MAGIC 
# MAGIC ### Arquitetura Medalhao Implementada:
# MAGIC - **Bronze**: Dados brutos ingeridos
# MAGIC - **Silver**: Dados limpos e enriquecidos
# MAGIC - **Gold**: KPIs e agregacoes para analise
# MAGIC 
# MAGIC ### Proximos Passos:
# MAGIC 1. Criar Databricks SQL Dashboard
# MAGIC 2. Automatizar com Jobs
# MAGIC 3. Configurar alertas
# MAGIC 4. Implementar ML para previsao de demanda
