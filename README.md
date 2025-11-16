# Projeto de Analise de Varejo - Databricks
## Arquitetura Medalhao (Bronze - Silver - Gold)

## Contexto do Desafio
O setor de varejo abrange desde pequenos comercios locais ate grandes redes. A gestao de inventario e a analise de vendas sao fundamentais para prever as demandas e otimizar operacoes.

## Objetivo
Analisar dados de vendas e inventario para:
- Calcular KPIs de negocio (Giro de Estoque, Taxa de Retorno)
- Identificar produtos mais vendidos e tendencias
- Prever demandas futuras
- Otimizar niveis de estoque

## Estrutura do Projeto
```
notebooks/
├── 00_config.py                    # Configuracoes e schemas
├── 01_bronze_ingestao.py          # Bronze: Ingestao de dados brutos
├── 02_silver_transformacao.py     # Silver: Limpeza e transformacao
├── 03_gold_agregacao.py           # Gold: KPIs e agregacoes
└── 04_visualizacao_dashboard.py   # Dashboard e visualizacoes
```

## Arquitetura Medalhao
- **Bronze**: Dados brutos sem transformacao (raw data)
- **Silver**: Dados limpos, validados e enriquecidos
- **Gold**: Dados agregados, KPIs e metricas de negocio

## Tecnologias
- **Databricks**: Plataforma de analise de dados
- **PySpark**: Processamento distribuido
- **Delta Lake**: Armazenamento otimizado (ACID transactions)
- **SQL**: Consultas analiticas
- **Python**: Pandas, Matplotlib, Seaborn

## KPIs Principais
1. **Giro de Estoque**: Quantas vezes o estoque e vendido em um periodo
2. **Taxa de Retorno de Clientes**: Percentual de clientes que retornam
3. **Ticket Medio**: Valor medio por venda
4. **Produtos Top**: Mais vendidos por categoria
5. **Margem de Lucro**: Rentabilidade por produto/categoria

## Como Usar no Databricks

### Passo 1: Importar do GitHub
1. Acesse seu workspace Databricks
2. Va em **Workspace** - **Users** - seu usuario
3. Clique em **...** - **Import**
4. Cole a URL: `https://github.com/evandrosan/Projeto-varejo-Databricks`
5. Clique em **Import**

### Passo 2: Criar um Cluster
1. Va em **Compute** - **Create Cluster**
2. Configure:
   - **Cluster Name**: `cluster-varejo`
   - **Databricks Runtime**: 13.3 LTS ou superior
   - **Node Type**: Padrao (Community Edition)
3. Aguarde o cluster ficar **Running** (verde)

### Passo 3: Executar os Notebooks (na ordem)
1. **00_config.py** - Configuracao inicial (databases e schemas)
2. **01_bronze_ingestao.py** - Ingestao de dados brutos
3. **02_silver_transformacao.py** - Limpeza e enriquecimento
4. **03_gold_agregacao.py** - Criacao de KPIs e agregacoes
5. **04_visualizacao_dashboard.py** - Dashboard e insights

## Tabelas Criadas

### Camada Bronze (Raw Data)
- `bronze.produtos`
- `bronze.clientes`
- `bronze.vendas`
- `bronze.inventario`

### Camada Silver (Clean Data)
- `silver.produtos` (com margem de lucro)
- `silver.clientes` (com tempo de cliente)
- `silver.vendas` (com dimensoes temporais)
- `silver.inventario` (com status de estoque)

### Camada Gold (Business Metrics)
- `gold.fato_vendas` (fato principal consolidado)
- `gold.kpi_giro_estoque`
- `gold.kpi_retorno_clientes`
- `gold.vendas_por_categoria`
- `gold.vendas_por_segmento`
- `gold.top_produtos`
- `gold.dashboard_executivo` (view)

## Resultados Esperados
- Pipeline completo Bronze - Silver - Gold
- 10.000+ registros de vendas processados
- KPIs calculados automaticamente
- Analise de produtos, clientes e estoque
- Visualizacoes interativas
- Insights acionaveis para gestao

## Como Compartilhar
1. Compartilhe o link do GitHub
2. A pessoa segue os passos de importacao acima
3. Pronto! Ela tera o projeto funcionando

## Proximos Passos
1. Criar Databricks SQL Dashboard
2. Automatizar com Jobs
3. Integrar dados reais (S3, APIs)
4. Implementar ML para previsao de demanda
5. Configurar alertas para estoque critico

## Requisitos
- Conta Databricks (Community Edition ou superior)
- Cluster com Databricks Runtime 13.0+
- Python 3.9+

## Links Uteis
- [Databricks Community Edition](https://community.cloud.databricks.com/)
- [Documentacao Delta Lake](https://docs.delta.io/)
- [Arquitetura Medalhao](https://www.databricks.com/glossary/medallion-architecture)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
