# Databricks notebook source
# MAGIC %md
# MAGIC # Camada Gold - Agregacoes e KPIs
# MAGIC ## Dados prontos para analise e dashboards

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Criar Tabela Gold: Vendas Consolidadas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.fato_vendas AS
# MAGIC SELECT 
# MAGIC   v.id_venda,
# MAGIC   v.data_venda_dt,
# MAGIC   v.ano,
# MAGIC   v.mes,
# MAGIC   v.nome_dia_semana,
# MAGIC   p.id_produto,
# MAGIC   p.nome_produto,
# MAGIC   p.categoria_normalizada as categoria,
# MAGIC   p.fornecedor,
# MAGIC   c.id_cliente,
# MAGIC   c.nome_cliente,
# MAGIC   c.cidade_normalizada as cidade,
# MAGIC   c.segmento_normalizado as segmento,
# MAGIC   v.quantidade,
# MAGIC   v.valor_total,
# MAGIC   v.preco_unitario_venda,
# MAGIC   p.custo_unitario,
# MAGIC   (v.valor_total - (p.custo_unitario * v.quantidade)) as lucro_bruto,
# MAGIC   p.margem_lucro,
# MAGIC   v.metodo_pagamento_normalizado as metodo_pagamento,
# MAGIC   current_timestamp() as data_processamento
# MAGIC FROM silver.vendas v
# MAGIC JOIN silver.produtos p ON v.id_produto = p.id_produto
# MAGIC JOIN silver.clientes c ON v.id_cliente = c.id_cliente;
# MAGIC 
# MAGIC SELECT COUNT(*) as total_registros FROM gold.fato_vendas

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Criar Tabela Gold: KPI - Giro de Estoque

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.kpi_giro_estoque AS
# MAGIC SELECT 
# MAGIC   p.id_produto,
# MAGIC   p.nome_produto,
# MAGIC   p.categoria_normalizada as categoria,
# MAGIC   COALESCE(SUM(v.quantidade), 0) as total_vendido,
# MAGIC   COALESCE(SUM(v.valor_total), 0) as receita_total,
# MAGIC   i.quantidade_estoque as estoque_atual,
# MAGIC   i.estoque_minimo,
# MAGIC   i.estoque_maximo,
# MAGIC   i.status_estoque,
# MAGIC   CASE 
# MAGIC     WHEN i.quantidade_estoque > 0 
# MAGIC     THEN ROUND(COALESCE(SUM(v.quantidade), 0) / i.quantidade_estoque, 2)
# MAGIC     ELSE 0 
# MAGIC   END as giro_estoque,
# MAGIC   current_timestamp() as data_processamento
# MAGIC FROM silver.produtos p
# MAGIC LEFT JOIN silver.vendas v ON p.id_produto = v.id_produto
# MAGIC LEFT JOIN silver.inventario i ON p.id_produto = i.id_produto
# MAGIC GROUP BY 
# MAGIC   p.id_produto, p.nome_produto, p.categoria_normalizada,
# MAGIC   i.quantidade_estoque, i.estoque_minimo, i.estoque_maximo, i.status_estoque
# MAGIC ORDER BY giro_estoque DESC;
# MAGIC 
# MAGIC SELECT * FROM gold.kpi_giro_estoque LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Criar Tabela Gold: KPI - Taxa de Retorno de Clientes

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.kpi_retorno_clientes AS
# MAGIC WITH compras_por_cliente AS (
# MAGIC   SELECT 
# MAGIC     id_cliente,
# MAGIC     COUNT(DISTINCT id_venda) as num_compras,
# MAGIC     SUM(valor_total) as valor_total_gasto,
# MAGIC     MIN(data_venda_dt) as primeira_compra,
# MAGIC     MAX(data_venda_dt) as ultima_compra
# MAGIC   FROM silver.vendas
# MAGIC   GROUP BY id_cliente
# MAGIC )
# MAGIC SELECT 
# MAGIC   c.id_cliente,
# MAGIC   c.nome_cliente,
# MAGIC   c.cidade_normalizada as cidade,
# MAGIC   c.segmento_normalizado as segmento,
# MAGIC   c.tempo_cliente_dias,
# MAGIC   cpc.num_compras,
# MAGIC   cpc.valor_total_gasto,
# MAGIC   ROUND(cpc.valor_total_gasto / cpc.num_compras, 2) as ticket_medio,
# MAGIC   CASE 
# MAGIC     WHEN cpc.num_compras >= 2 THEN 'Cliente Recorrente'
# MAGIC     ELSE 'Cliente Unico'
# MAGIC   END as tipo_cliente,
# MAGIC   cpc.primeira_compra,
# MAGIC   cpc.ultima_compra,
# MAGIC   DATEDIFF(cpc.ultima_compra, cpc.primeira_compra) as dias_entre_compras,
# MAGIC   current_timestamp() as data_processamento
# MAGIC FROM silver.clientes c
# MAGIC JOIN compras_por_cliente cpc ON c.id_cliente = cpc.id_cliente
# MAGIC ORDER BY cpc.valor_total_gasto DESC;
# MAGIC 
# MAGIC SELECT * FROM gold.kpi_retorno_clientes LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Criar Tabela Gold: Vendas por Categoria

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.vendas_por_categoria AS
# MAGIC SELECT 
# MAGIC   categoria,
# MAGIC   ano,
# MAGIC   mes,
# MAGIC   COUNT(DISTINCT id_venda) as num_vendas,
# MAGIC   COUNT(DISTINCT id_cliente) as num_clientes,
# MAGIC   COUNT(DISTINCT id_produto) as num_produtos_vendidos,
# MAGIC   SUM(quantidade) as unidades_vendidas,
# MAGIC   ROUND(SUM(valor_total), 2) as receita_total,
# MAGIC   ROUND(SUM(lucro_bruto), 2) as lucro_bruto_total,
# MAGIC   ROUND(AVG(valor_total), 2) as ticket_medio,
# MAGIC   ROUND(AVG(margem_lucro), 2) as margem_lucro_media,
# MAGIC   current_timestamp() as data_processamento
# MAGIC FROM gold.fato_vendas
# MAGIC GROUP BY categoria, ano, mes
# MAGIC ORDER BY ano DESC, mes DESC, receita_total DESC;
# MAGIC 
# MAGIC SELECT * FROM gold.vendas_por_categoria LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Criar Tabela Gold: Vendas por Segmento de Cliente

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.vendas_por_segmento AS
# MAGIC SELECT 
# MAGIC   segmento,
# MAGIC   ano,
# MAGIC   mes,
# MAGIC   COUNT(DISTINCT id_venda) as num_vendas,
# MAGIC   COUNT(DISTINCT id_cliente) as num_clientes,
# MAGIC   SUM(quantidade) as unidades_vendidas,
# MAGIC   ROUND(SUM(valor_total), 2) as receita_total,
# MAGIC   ROUND(SUM(lucro_bruto), 2) as lucro_bruto_total,
# MAGIC   ROUND(AVG(valor_total), 2) as ticket_medio,
# MAGIC   current_timestamp() as data_processamento
# MAGIC FROM gold.fato_vendas
# MAGIC GROUP BY segmento, ano, mes
# MAGIC ORDER BY ano DESC, mes DESC, receita_total DESC;
# MAGIC 
# MAGIC SELECT * FROM gold.vendas_por_segmento

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Criar Tabela Gold: Top Produtos

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.top_produtos AS
# MAGIC SELECT 
# MAGIC   id_produto,
# MAGIC   nome_produto,
# MAGIC   categoria,
# MAGIC   fornecedor,
# MAGIC   COUNT(DISTINCT id_venda) as num_vendas,
# MAGIC   SUM(quantidade) as total_vendido,
# MAGIC   ROUND(SUM(valor_total), 2) as receita_total,
# MAGIC   ROUND(SUM(lucro_bruto), 2) as lucro_bruto_total,
# MAGIC   ROUND(AVG(margem_lucro), 2) as margem_lucro_media,
# MAGIC   ROW_NUMBER() OVER (ORDER BY SUM(valor_total) DESC) as ranking_receita,
# MAGIC   ROW_NUMBER() OVER (ORDER BY SUM(quantidade) DESC) as ranking_volume,
# MAGIC   current_timestamp() as data_processamento
# MAGIC FROM gold.fato_vendas
# MAGIC GROUP BY id_produto, nome_produto, categoria, fornecedor
# MAGIC ORDER BY receita_total DESC;
# MAGIC 
# MAGIC SELECT * FROM gold.top_produtos LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Criar View Gold: Dashboard Executivo

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.dashboard_executivo AS
# MAGIC SELECT 'Total de Vendas' as metrica, CAST(COUNT(*) AS STRING) as valor, 'unidades' as unidade FROM gold.fato_vendas
# MAGIC UNION ALL
# MAGIC SELECT 'Receita Total', CONCAT('R$ ', FORMAT_NUMBER(SUM(valor_total), 2)), 'reais' FROM gold.fato_vendas
# MAGIC UNION ALL
# MAGIC SELECT 'Lucro Bruto Total', CONCAT('R$ ', FORMAT_NUMBER(SUM(lucro_bruto), 2)), 'reais' FROM gold.fato_vendas
# MAGIC UNION ALL
# MAGIC SELECT 'Ticket Medio', CONCAT('R$ ', FORMAT_NUMBER(AVG(valor_total), 2)), 'reais' FROM gold.fato_vendas
# MAGIC UNION ALL
# MAGIC SELECT 'Clientes Ativos', CAST(COUNT(DISTINCT id_cliente) AS STRING), 'clientes' FROM gold.fato_vendas
# MAGIC UNION ALL
# MAGIC SELECT 'Produtos Vendidos', CAST(COUNT(DISTINCT id_produto) AS STRING), 'produtos' FROM gold.fato_vendas;
# MAGIC 
# MAGIC SELECT * FROM gold.dashboard_executivo

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Verificar Todas as Tabelas Gold

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN gold

# COMMAND ----------

# MAGIC %md
# MAGIC ## Camada Gold Concluida!
# MAGIC 
# MAGIC **Tabelas Gold criadas:**
# MAGIC - gold.fato_vendas (fato principal)
# MAGIC - gold.kpi_giro_estoque
# MAGIC - gold.kpi_retorno_clientes
# MAGIC - gold.vendas_por_categoria
# MAGIC - gold.vendas_por_segmento
# MAGIC - gold.top_produtos
# MAGIC - gold.dashboard_executivo (view)
# MAGIC 
# MAGIC **Proximo passo**: Execute o notebook **04_visualizacao_dashboard**
