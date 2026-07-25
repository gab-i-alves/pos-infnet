# Aula 01 · Introdução ao Apache Spark

Dossiê de aprofundamento da primeira aula. A bibliografia que o professor passou (Luu cap. 1–2, Damji cap. 1 e início do 2) cobre o Spark 3.0; estes documentos vão além dela e marcam o que mudou até o **Spark 4.2.0**, de julho de 2026.

## Documentos

| | Documento | Do que trata |
|---|---|---|
| 01 | [Arquitetura e modelo de execução](01-arquitetura-e-modelo-de-execucao.md) | driver, executores, cluster manager; do código à task; narrow vs wide; partições; memória e OOM; como ler a Spark UI |
| 02 | [Por que o Spark existe, e quando não usar](02-por-que-spark-e-quando-nao-usar.md) | o que o MapReduce não resolvia; a tese do motor unificado; e o contra-argumento honesto de 2026 (DuckDB, Polars, warehouses) |
| 03 | [RDD, DataFrame, Dataset, Catalyst e Tungsten](03-apis-catalyst-e-tungsten.md) | a linhagem das APIs; por que Dataset tipado não existe em PySpark; ler `explain()` de verdade; a armadilha das UDFs Python |
| 04 | [Montando o ambiente em 2026](04-ambiente-2026.md) | versões e requisitos atuais; pip/uv, Docker, Spark Connect; o que a Databricks Free Edition tem e não tem; smoke tests |
| 05 | [O que mudou desde os livros](05-o-que-mudou-desde-os-livros.md) | releases 3.2 a 4.2; AQE e skew join em profundidade; DPP; Spark 4 item por item; Photon |
| 06 | [Perguntas para a aula](06-perguntas-para-a-aula.md) | ancoragem prática e as perguntas para levar à aula ao vivo, cada uma com a resposta que eu já devo saber antes de perguntar |

## Ordem sugerida

Para entender: **01 → 03 → 05**. Para decidir: **02**. Para executar: **04**. Antes da aula: **06**.
