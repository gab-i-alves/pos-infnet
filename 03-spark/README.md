# 03 · Processamento de Big Data com Apache Spark e Spark SQL

Terceira disciplina do MBA, cursada entre julho e agosto de 2026. Oito aulas.

Versão de referência do material: **Apache Spark 4.2.0**. A bibliografia oficial da disciplina (Luu, 2021; Damji et al., 2020) cobre o Spark 3.0, então boa parte do aprofundamento aqui existe justamente para marcar o que mudou.

**Entrega do Projeto de Disciplina: segunda, 24 de agosto de 2026, 23h59.** Duas tentativas permitidas.

---

## Competências

Quatro competências, e a rubrica do projeto cobra quatro itens de cada uma.

1. Processamento inicial de dados, da criação de tabelas ao salvamento de resultados, com Spark SQL.
2. Soluções de processamento analítico escaláveis com performance no ecossistema Spark SQL.
3. Processamento de fluxos de dados em tempo real com Spark Streaming.
4. Fluxos de dados distribuídos com governança usando Spark e Databricks.

---

## Trilha de leitura, aula por aula

Abreviações: **Luu** = *Beginning Apache Spark 3* · **Damji** = *Learning Spark*, 2ª ed. · **Chadha** = *Data Engineering with Databricks Cookbook* · **Girten** = *Building Modern Data Applications Using Databricks Lakehouse*. Referências completas na [bibliografia](#bibliografia).

### Aula 01 — Introdução ao Apache Spark

Arquitetura unificada do Spark, processamento em larga escala e preparação do ambiente de desenvolvimento.

- [x] Luu, cap. 1 — *Introduction to Apache Spark*: como o Spark unifica o processamento e escala para grandes volumes.
- [x] Damji, cap. 1 — *Introduction to Apache Spark: A Unified Analytics Engine*: por que o Spark é um motor unificado.
- [x] Luu, cap. 2 — *Working with Apache Spark*: configurar ambientes e executar pipelines.
- [x] Damji, cap. 2, seção 1 — *Step 1: Downloading Apache Spark*: configurar e validar o ambiente, ajustar dependências e parâmetros.

### Aula 02 — Transformação e persistência SQL

Conversão de RDDs em DataFrames e persistência de consultas e resultados em storage.

- [x] Luu, cap. 3, seção 1 — *Understanding RDD*: conversão de RDDs em DataFrames, tratamento de dados distribuídos.
- [x] Damji, cap. 3 — *Apache Spark's Structured APIs*: conversão de RDDs em DataFrames e estruturação de dados para pipelines.
- [x] Chadha, cap. 1 — *Data Ingestion and Data Extraction with Apache Spark*: RDDs em DataFrames e tabelas otimizadas no Databricks.
- [x] Luu, cap. 3, seção 2 — *Introduction to the DataFrame API*: persistir consultas, gravar resultados e tabelas.
- [x] Damji, cap. 4 — *Spark SQL and DataFrames: Introduction to Built-in Data Sources*: salvar tabelas e resultados em Parquet/Delta/S3.

### Aula 03 — Transformação e análise avançada

Operações relacionais e analíticas, joins e extensibilidade do Spark SQL.

- [ ] Luu, cap. 4, seção 1 — *Aggregations*: transformação de dados complexos com operações relacionais e analíticas.
- [ ] Damji, cap. 2, seção 2 — *Step 2: Using the Scala or PySpark Shell*: transformar dados complexos em escala.
- [ ] Chadha, cap. 2, seção *Applying basic transformations to data with Apache Spark*: operações relacionais e analíticas em pipelines.
- [ ] Luu, cap. 4, seção 2 — *Joins*: fortalecer análises e ampliar a extensibilidade das soluções.
- [ ] Damji, cap. 5 — *Spark SQL and DataFrames: Interacting with External Data Sources*: APIs, otimizações e padrões extensíveis.
- [ ] Chadha, cap. 2, seção *Filtering data with Apache Spark*: práticas que aumentam extensibilidade e desempenho.

### Aula 04 — Otimização de desempenho no Spark

Arquitetura de memória, parâmetros de execução e o plano interno de consultas.

- [ ] Luu, cap. 5, seção 1 — *Common Performance Issues*: memória e parâmetros de execução, desempenho e estabilidade de jobs.
- [ ] Damji, cap. 7, seção 1 — *Optimizing and Tuning Spark for Efficiency*: afinar arquitetura de memória e parâmetros.
- [ ] Chadha, cap. 6, seção *Monitoring Spark jobs in the Spark UI*: ajustar memória e execução via Spark UI.
- [ ] Luu, cap. 5, seção 2 — *Leverage In-Memory Computation*: ajustes no plano de execução do Spark SQL.
- [ ] Damji, cap. 7, seção 2 — *Caching and Persistence of Data*: planos e execuções, otimização de consultas.
- [ ] Chadha, cap. 6, seção *Using broadcast variables*: personalizar planos e mecanismos de execução.

### Aula 05 — Processamento em tempo real

Structured Streaming: implementação, operação de fluxos e configuração.

- [ ] Luu, cap. 6, seção 1 — *Stream Processing*: pipelines de streaming com baixa latência.
- [ ] Damji, cap. 8, seção 1 — *Evolution of the Apache Spark Stream Processing Engine*: pipelines em tempo real com Structured Streaming.
- [ ] Chadha, cap. 4, seção *Configuring Spark Structured Streaming for real-time data processing*: projetar pipelines com Structured Streaming e Delta Lake.
- [ ] Luu, cap. 6, seção 2 — *Spark Streaming Overview*: operar fluxos e ajustar configurações para desempenho e tolerância.
- [ ] Damji, cap. 8, seção 2 — *The Programming Model of Structured Streaming*: operar e ajustar fluxos e configurações.
- [ ] Chadha, cap. 4, seção *Reading data from real-time sources, such as Apache Kafka*: ingestão contínua e desempenho.

### Aula 06 — Gerenciamento de fluxos resilientes

Tempo e estado em processamento de fluxos, resiliência e monitoramento.

- [ ] Luu, cap. 7, seção 1 — *Event Time*: timestamps, watermarks, janelas e estado.
- [ ] Luu, cap. 7, seção 2 — *Arbitrary Stateful Processing*: tolerância a falhas e monitoramento de pipelines.

### Aula 07 — Integração e governança de dados

Múltiplas fontes de dados e governança com Unity Catalog.

- [ ] Damji, cap. 12 — *Epilogue: Apache Spark 3.0*: conectar e ler dados de múltiplas fontes e formatos.
- [ ] Girten, cap. 6, seção *Creating and managing data catalogs in Unity Catalog*: hierarquias de armazenamento e políticas de governança.
- [ ] Chadha, cap. 10 — *Data Governance with Unity Catalog*: camadas, estrutura de catálogos e controles de acesso.

### Aula 08 — Gestão de dados e transparência

Isolamento de catálogos, armazenamentos externos, pipelines DLT e linhagem.

- [ ] Girten, cap. 7 — *Viewing Data Lineage Using Unity Catalog*: mapear linhagem e evolução dos fluxos no Lakehouse.

---

## Bibliografia

| Sigla | Referência |
|---|---|
| Luu | Hien Luu. *Beginning Apache Spark 3: With DataFrame, Spark SQL, Structured Streaming, and Spark Machine Learning Library*. Apress, 2021. |
| Damji | Jules S. Damji, Brooke Wenig, Tathagata Das, Denny Lee. *Learning Spark: Lightning-Fast Data Analytics*, 2ª ed. O'Reilly, 2020. |
| Chadha | Pulkit Chadha. *Data Engineering with Databricks Cookbook*. Packt. |
| Girten | Will Girten. *Building Modern Data Applications Using Databricks Lakehouse*. Packt. |

Os capítulos em PDF são material licenciado e não entram neste repositório (ver `.gitignore`).

---

## Projeto da Disciplina

**Uso de IA: sinal vermelho.** O enunciado proíbe explicitamente qualquer uso de ferramentas generativas de IA na produção do trabalho, sob pena de má conduta acadêmica. O projeto é de autoria integral minha, não vive neste repositório e nada aqui além deste registro de requisitos o toca.

### Questão

Como projetar e implementar um pipeline de dados distribuído que integre ingestão em lote e em tempo real, garantindo alta performance via Spark SQL e governança centralizada através do Unity Catalog?

### Corpus de trabalho

- [ ] Volume que justifique processamento distribuído: mínimo de 1 milhão de registros ou múltiplos arquivos particionados.
- [ ] Fontes variadas: JSON, CSV ou Parquet.
- [ ] Atributos temporais que permitam Structured Streaming e window functions.
- [ ] Relacionamentos que permitam joins complexos e agregações de negócio.

### Requisitos mínimos

- [ ] **Processamento inicial e ingestão** — criar tabelas, gerenciar DataFrames e persistir resultados em storage distribuído (S3/ADLS/GCS).
- [ ] **Transformação analítica escalável** — agregação, pivotação e window functions otimizadas por Catalyst e Tungsten.
- [ ] **Processamento em tempo real** — Structured Streaming com watermarking, triggers e tratamento de dados atrasados.
- [ ] **Governança e linhagem** — Unity Catalog com isolamento entre workspaces, locais externos e lineage de ponta a ponta.

### Estrutura da solução

- [ ] **Ingestão e preparação** — configuração do cluster, bancos no Unity Catalog, carga inicial de RDDs/DataFrames.
- [ ] **Refino e transformação** — limpeza, tratamento de colunas duplicadas, joins otimizados e lógica de negócio.
- [ ] **Streaming e resiliência** — fluxos contínuos com output modes adequados e checkpoints.
- [ ] **Otimização** — parâmetros de memória, cache e análise do plano de execução.

### Entregas

- [ ] Notebook Databricks/Spark (`.dbc` ou `.ipynb`) com o código batch e streaming de ponta a ponta.
- [ ] Configuração de governança: documentação ou prints das permissões, catálogos e locais externos no Unity Catalog.
- [ ] Relatório técnico em PDF: arquitetura, justificativa das estratégias de join, análise das métricas de monitoramento e visualização da linhagem.
- [ ] ZIP nomeado `gabrielealves_processamentodebigdatacomapachesparkesparksql_pd.ZIP`, postado no Moodle.

### Rubrica

Dezesseis itens, quatro por competência. Cada um é binário: demonstrou ou não demonstrou.

**1. Processamento inicial de dados com Spark SQL**

- [ ] Compreensão da arquitetura do Spark 3.0 e do ecossistema unificado na preparação do cluster.
- [ ] Conversão correta de RDDs em DataFrames, com uso eficiente da API.
- [ ] Persistência correta de consultas e resultados em tabelas e storage externo.
- [ ] Agregações complexas, agrupamentos e pivotação para extrair métricas de negócio.

**2. Processamento analítico escalável com performance**

- [ ] Joins considerando colunas duplicadas e escolha do tipo correto para o volume.
- [ ] UDFs ou funções avançadas estendendo a capacidade analítica.
- [ ] Window functions para ranking, médias móveis ou outras análises temporais.
- [ ] Uso demonstrado de Catalyst, Tungsten ou gerenciamento de memória.

**3. Fluxos de dados em tempo real**

- [ ] Cache e parâmetros de execução configurados contra problemas comuns de performance.
- [ ] AQE ou estratégias dinâmicas para skew joins.
- [ ] Structured Streaming com escolha correta do mecanismo de processamento de fluxo.
- [ ] Watermarking e estados de agregação para dados atrasados e event time.

**4. Fluxos distribuídos com governança**

- [ ] Tolerância a falhas via triggers, output modes adequados e checkpointing.
- [ ] Acesso consolidado a JSON, Parquet e CSV respeitando as APIs de DataFrame e Dataset.
- [ ] Isolamento de catálogos e distinção entre dados gerenciados e externos.
- [ ] Rastreamento e visualização de lineage de ponta a ponta no Unity Catalog.
