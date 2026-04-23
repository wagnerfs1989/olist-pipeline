# Pipeline de Engenharia de Dados — Olist
> Arquitetura Medalhão com Databricks Serverless, Apache Spark 4.1 e Delta Lake

---

## Visão Geral

Pipeline de engenharia de dados construído do zero com dados reais do e-commerce brasileiro Olist (344.483 registros). Cada decisão técnica foi tomada com intenção e documentada — incluindo os 4 problemas reais encontrados e corrigidos durante o desenvolvimento.

**Stack:** Databricks Serverless · Apache Spark 4.1 · Delta Lake · Python · PySpark · Unity Catalog

---

## Arquitetura Medalhão

```
Raw Data (CSV)
     ↓
┌─────────────────────────────────────────────────────────┐
│  BRONZE — dado bruto exatamente como veio da fonte      │
│  Regra: chegou, gravou. Zero transformação.             │
└─────────────────────────────────────────────────────────┘
     ↓
┌─────────────────────────────────────────────────────────┐
│  SILVER — dado limpo, tipado, deduplicado, enriquecido  │
│  Regra: o dado vira informação.                         │
└─────────────────────────────────────────────────────────┘
     ↓
┌─────────────────────────────────────────────────────────┐
│  GOLD — agregações, métricas e KPIs para consumo        │
│  Regra: uma pergunta, uma tabela.                       │
└─────────────────────────────────────────────────────────┘
```

### Estrutura de Volumes (Unity Catalog)

```
/Volumes/workspace/olist/raw_data      → CSVs originais da Olist
/Volumes/workspace/olist/bronze        → dados brutos em Delta Lake
/Volumes/workspace/olist/silver        → dados limpos e transformados
/Volumes/workspace/olist/gold          → KPIs e agregações
/Volumes/workspace/olist/dead_letter   → registros com falha
/Volumes/workspace/olist/bronze/_log   → log estruturado em Delta
/Volumes/workspace/olist/bronze/_schemas → schema registry
```

---

## Sprints

| Sprint | Objetivo | Resultado |
|--------|----------|-----------|
| Sprint 1 | Bronze MVP — ingestão dos CSVs | 344.483 registros gravados em Delta Lake |
| Sprint 2 | Blindagem Bronze — Schema Registry, DQ Validator, Dead Letter Queue | SUCCESS: 4 \| FAILED: 0 |
| Sprint 3 | Silver MVP — limpeza, joins e Window Functions | 112.650 registros transformados |
| Sprint 4 | Blindagem Silver — validações pós-transformação | SUCCESS: 1 \| FAILED: 0 |
| Sprint 5 | Gold MVP — KPIs e métricas de negócio | 4 tabelas Gold criadas |
| Sprint 6 | Blindagem Gold — validações de consistência e negócio | SUCCESS: 1 \| FAILED: 0 |

---

## Decisões Técnicas

### Bronze — Colunas de Auditoria
Três colunas adicionadas em todos os DataFrames antes de qualquer gravação:

```python
df_bronze = df \
    .withColumn("_ingest_timestamp", F.now()) \
    .withColumn("_source_filename",  F.col("_metadata.file_path")) \
    .withColumn("_processing_date",  F.lit(str(date.today())))
```

### Silver — dense_rank vs row_number vs rank
O `dense_rank()` foi escolhido para ranquear itens dentro de pedidos por preço:
- `row_number()` — resolve empates arbitrariamente ❌
- `rank()` — cria gaps na sequência (1, 1, 3) ❌
- `dense_rank()` — empates recebem mesmo rank sem gaps (1, 1, 2) ✅

### Gold — groupBy com customer_unique_id
Na Olist, cada pedido gera um `customer_id` diferente para o mesmo cliente físico. O identificador real do cliente é `customer_unique_id` — sem esse detalhe, o groupBy gera duplicatas silenciosas.

---

## Problemas Reais Encontrados

### Problema 1 — Funções inexistentes no Spark 4.1
```python
# ERRADO
F.current_timestamp()
input_file_name()

# CORRETO
F.now()
F.col("_metadata.file_path")
```

### Problema 2 — Lógica try/except/else quebrada
O bloco `else` num `try/except` só executa se o `try` não lançar exceção — mas o `try` já tinha um `return`. O caminho de primeira execução estava morto.

### Problema 3 — 113 clientes duplicados no Gold *(encontrado pela blindagem)*
**Causa:** `groupBy` com `customer_unique_id + customer_state + customer_city` criava duplicatas quando um cliente tinha endereços diferentes em pedidos distintos.

```python
# ERRADO
groupBy("customer_unique_id", "customer_state", "customer_city")

# CORRETO
groupBy("customer_unique_id").agg(F.first("customer_state"), ...)
```

| | Antes | Depois |
|--|-------|--------|
| Clientes no Gold | 93.471 | 93.358 |
| Divergência | 113 duplicatas | 0 |

### Problema 4 — LTV incorreto para 2.484 clientes *(encontrado pela blindagem)*
**Causa:** `max(ltv_acumulado)` pegava o acumulado parcial de um único `customer_id`, não o total do cliente físico.

```python
# ERRADO
F.max("ltv_acumulado").alias("ltv_total")

# CORRETO
F.round(F.sum(F.col("price") + F.col("freight_value")), 2).alias("ltv_total")
```

---

## Resultados Gold — Insights dos Dados

**Faturamento por Estado**
- SP concentra R$ 5,7M — mais que o dobro do segundo colocado (RJ)
- Ticket médio inversamente proporcional ao volume — estados com menos pedidos têm produtos de maior valor

**Análise de Entregas**
- SP entrega em 8,7 dias em média — AP e RR chegam a 28 dias
- A desigualdade logística do Brasil aparece nos dados

**Top Clientes LTV**
- Quase todos os top clientes têm apenas 1 pedido — baixíssima taxa de recompra

---

## Limitações Descobertas (Spark 4.1 + Unity Catalog)

| Problema | Solução |
|----------|---------|
| `F.current_timestamp()` não existe | Usar `F.now()` |
| `input_file_name()` não funciona com UC Volumes | Usar `F.col("_metadata.file_path")` |
| `datetime.date.today()` causa conflito de import | Usar `from datetime import date as dt` |
| Log Delta não aceita espaços em nomes de colunas | Nomear colunas sem espaços |
| `DELTA_METADATA_MISMATCH` ao mudar schema | Adicionar `.option("overwriteSchema", "true")` |
| Window Functions encadeadas com `ATTRIBUTE_NOT_SUPPORTED` | Separar em múltiplos `withColumn` |

---

## Padrões Adotados

- Variáveis globais em MAIÚSCULO (`VOLUMES`, `TABELAS`, `SCHEMAS`)
- `try/except Exception` — nunca `except` puro
- `raise Exception` para erros críticos que exigem intervenção humana
- Log em Delta sempre em modo `append` — nunca sobrescrito
- Schema do log sempre definido manualmente — nunca inferido
- Dados problemáticos sempre para dead_letter — nunca descartados
- Idempotência em todos os comandos DDL (`IF NOT EXISTS`)

---

## Estrutura do Repositório

```
olist-pipeline/
├── 01_bronze_ingestao.ipynb      # Sprint 1 — Bronze MVP
├── 02_bronze_blindagem.ipynb     # Sprint 2 — Blindagem Bronze
├── 03_silver_transformacao.ipynb # Sprint 3 — Silver MVP
├── 04_silver_blindagem.ipynb     # Sprint 4 — Blindagem Silver
├── 05_gold_negocio.ipynb         # Sprint 5 — Gold MVP
├── 06_gold_blindagem.ipynb       # Sprint 6 — Blindagem Gold
└── README.md
```

---

*Pipeline construído com dados reais, problemas reais e soluções documentadas.*
