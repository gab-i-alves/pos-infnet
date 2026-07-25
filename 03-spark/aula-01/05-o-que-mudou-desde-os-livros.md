---
title: "O que mudou no Spark desde os livros da disciplina"
aula: "Aula 01 - Arquitetura unificada, processamento em larga escala e preparação do ambiente"
data: 2026-07-24
tags:
  - spark
  - pos-infnet
  - aqe
  - dynamic-partition-pruning
  - spark-4
  - photon
  - lakehouse
  - atualizacao-bibliografica
---

# O que mudou no Spark desde os livros da disciplina

Os dois livros da bibliografia são bons e continuam válidos no que ensinam sobre modelo de execução, RDD, DataFrame, Catalyst e Tungsten. O problema é a data. "Learning Spark" 2ª edição saiu em 2020 e cobre o Spark 3.0. "Beginning Apache Spark 3", de Hien Luu, saiu em 2021 e cobre até o 3.1. De lá para cá o Spark passou por quatro releases de feature na linha 3.x e três na linha 4.x. Algumas coisas que os livros mandam você configurar já vêm ligadas. Outras que eles descrevem como padrão foram removidas. E o recurso que mais afeta a performance de um job real em 2026 aparece nos livros como nota de rodapé.

Este documento cobre essa lacuna.

---

## 1. Linha do tempo: do 3.2 até hoje

Os livros param aqui. O que veio depois, em ordem, com o que importa de cada release:

| Versão | Data | O que trouxe de relevante |
|---|---|---|
| 3.2.0 | 13/10/2021 | **AQE ligado por padrão**; pandas API on Spark (Koalas incorporado, "Project Zen"); RocksDB state store; push-based shuffle; modo ANSI SQL vira GA (ainda desligado); session windows; Scala 2.13 |
| 3.3.0 | 16/06/2022 | Row-level runtime filtering com **Bloom filters** em joins; pushdown agressivo no DataSource V2 (agregações, Top-N); `Trigger.AvailableNow`; primeira onda de mensagens de erro padronizadas; funções `try_*` |
| 3.4.0 | 13/04/2023 | **Spark Connect** (cliente Python, arquitetura cliente-servidor via gRPC/Arrow); Bloom filter join por padrão; `DEFAULT` em colunas; `TIMESTAMP WITHOUT TIMEZONE`; error classes com SQLSTATE; **DStream API deprecada** |
| 3.5.0 | 13/09/2023 | Cliente Scala e Go no Connect; Python UDTFs; Arrow Python UDFs; `IDENTIFIER` e named arguments em SQL; changelog checkpointing no RocksDB; aviso formal de remoção de Java 8/11 e Scala 2.12 |
| **4.0.0** | **23/05/2025** | ANSI SQL por padrão; tipo VARIANT; Spark Connect maduro; Python Data Source API; `transformWithState`; JDK 17+; Scala 2.13 |
| **4.1.0** | **16/12/2025** | Spark Declarative Pipelines; Real-Time Mode no streaming; SQL Scripting GA; VARIANT GA com shredding; UDFs Arrow-nativas |
| **4.2.0** | **14/07/2026** | Geoespacial nativo (`GEOMETRY`/`GEOGRAPHY`, funções `ST_*`); CDC nativo; UDFs Arrow-otimizadas ligadas por padrão; `NEAREST BY`; metric views; Java 25 |

A release estável mais recente é a **4.2.0, de 14 de julho de 2026** - dez dias antes de você ler isto. Há três ramos 4.x ativos em manutenção: 4.0.x (última: 4.0.4, 15/07/2026), 4.1.x (última: 4.1.3, 15/07/2026) e 4.2.x. A linha 3.5 continua viva como LTS estendido, com a 3.5.9 lançada em 16/07/2026 e suporte de segurança anunciado até novembro de 2027. Repare na pegadinha de datas: existem patches de ramos antigos com data **posterior** à 4.2.0. A versão mais nova em numeração é a 4.2.0; a mais nova em calendário é a 3.5.9.

Outra mudança que vale saber: a partir da 4.3.0 o projeto adota **cadência trimestral** para releases de feature, com major anual, janela de manutenção de seis meses e 18 meses de LTS por linha. O LTS do 4.x será o 4.5.0.

Fontes: [News](https://spark.apache.org/news/), [Downloads](https://spark.apache.org/downloads.html), [Versioning Policy](https://spark.apache.org/versioning-policy.html).

---

## 2. Adaptive Query Execution: o recurso que os livros subestimam

Se você só puder guardar uma coisa deste documento, guarde esta seção. AQE cai na rubrica do Projeto da Disciplina, e é também o item de maior impacto prático em qualquer pipeline de porte real.

### 2.1 O problema que ele resolve

O Catalyst planeja a query **antes** de executar, com base em estatísticas de catálogo: contagem de linhas, tamanho em bytes, histogramas quando existem. Em um data warehouse bem cuidado isso funciona. Em um data lake sobre Parquet em object storage, alimentado por ingestão contínua, com arquivos de tamanhos irregulares e sem `ANALYZE TABLE` rodando, a estimativa é frequentemente ruim. Um filtro seletivo demais, uma coluna com cardinalidade mal estimada, e o otimizador escolhe um sort-merge join onde cabia um broadcast, ou reparte 200 vezes um resultado que virou 3 MB.

AQE resolve isso mudando **quando** a decisão é tomada. O plano físico é quebrado em estágios delimitados por shuffle. Ao fim de cada estágio, o Spark tem estatísticas **reais** dos dados materializados: tamanho de cada partição de shuffle, contagem de registros. Com esse número em mãos, ele re-otimiza o restante do plano antes de continuar. Não é heurística nova, é o mesmo otimizador rodando com dados melhores.

### 2.2 Quando virou padrão

`spark.sql.adaptive.enabled` passou a valer `true` por padrão no **Spark 3.2.0**. Os livros, escritos sobre 3.0/3.1, ensinam a ligar manualmente. Se você repetir isso no projeto, não está errado, está redundante. O que muda de verdade é que hoje **você precisa saber quando desligar**, não quando ligar.

### 2.3 As três otimizações, com as configs

#### a) Coalescing de partições de shuffle

Junta partições pequenas contíguas depois do shuffle, para não gerar milhares de tarefas minúsculas cujo custo de agendamento supera o de processamento.

| Config | Default | Desde |
|---|---|---|
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | 3.0.0 |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | 64 MB | 3.0.0 |
| `spark.sql.adaptive.coalescePartitions.minPartitionSize` | 1 MB | 3.2.0 |
| `spark.sql.adaptive.coalescePartitions.parallelismFirst` | `true` | 3.2.0 |
| `spark.sql.adaptive.coalescePartitions.initialPartitionNum` | herda `spark.sql.shuffle.partitions` | 3.0.0 |

Isto **aposenta** o ritual de calibrar `spark.sql.shuffle.partitions` que os livros descrevem. A prática nova é o inverso da antiga: configure `spark.sql.shuffle.partitions` **alto** (500, 1000, 2000) e deixe o AQE reduzir com base no tamanho real. O número vira um teto, não um alvo.

Uma armadilha operacional: `parallelismFirst = true` ignora o tamanho alvo de 64 MB e respeita apenas o mínimo de 1 MB, maximizando paralelismo. Isso é bom em cluster ocioso e ruim em cluster compartilhado, onde você quer usar recurso com parcimônia. A própria documentação recomenda `false` nesse cenário.

```python
spark.conf.set("spark.sql.shuffle.partitions", "1000")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
spark.conf.set("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false")
```

#### b) Conversão dinâmica de estratégia de join

Se, em tempo de execução, um lado do join se revelar pequeno o bastante, o sort-merge join vira **broadcast hash join**. Se não couber em broadcast mas couber em um hash map local, vira **shuffled hash join**. É a correção do erro de estimativa mais comum e mais caro.

| Config | Default | Desde |
|---|---|---|
| `spark.sql.adaptive.autoBroadcastJoinThreshold` | herda `spark.sql.autoBroadcastJoinThreshold` (10 MB); `-1` desliga | 3.2.0 |
| `spark.sql.adaptive.localShuffleReader.enabled` | `true` | 3.0.0 |
| `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold` | 0 | 3.2.0 |

Quando o AQE converte para broadcast, o `localShuffleReader` evita um shuffle desnecessário do lado grande, lendo as partições locais diretamente. É por isso que a conversão compensa mesmo depois do shuffle já ter sido escrito.

#### c) Tratamento de skew join

Este é o item que merece mais atenção, porque skew é o modo de falha mais frequente em dados do mundo real e o mais difícil de diagnosticar sem entender o mecanismo.

O sintoma: um job com 200 tarefas onde 199 terminam em 40 segundos e uma roda por 25 minutos. A causa: a chave do join tem distribuição desbalanceada. Em dados do mundo real isso é regra, não exceção: qualquer agrupamento por entidade em que um punhado de participantes concentra a maior parte dos registros, seja cliente, região ou fornecedor. O hash da chave joga tudo isso na mesma partição de shuffle, e essa partição vira uma tarefa "straggler" que segura o estágio inteiro, porque um estágio só termina quando a última tarefa termina.

O que o AQE faz: detecta a partição desproporcional e a **divide** em sub-partições menores, replicando a partição correspondente do outro lado do join para que o resultado continue correto. Uma partição de 3 GB vira, digamos, doze pedaços de 256 MB, e o lado oposto (que é pequeno para aquela chave) é lido doze vezes. Você troca leitura redundante barata por paralelismo.

| Config | Default | Desde |
|---|---|---|
| `spark.sql.adaptive.skewJoin.enabled` | `true` | 3.0.0 |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | 5.0 | 3.0.0 |
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | 256 MB | 3.0.0 |
| `spark.sql.adaptive.forceOptimizeSkewedJoin` | `false` | 3.3.0 |

**A regra exata, e ela costuma ser cobrada errado:** uma partição é considerada *skewed* quando satisfaz **as duas** condições ao mesmo tempo. Ela precisa passar de 256 MB **e** ser maior que 5x a mediana das partições. Não é "ou". Uma consequência prática importante: se todas as suas partições forem grandes de forma uniforme, nada é considerado skew, porque nenhuma passa de 5x a mediana. E se você tem skew severo mas em escala pequena (a maior partição tem 100 MB contra mediana de 2 MB), o AQE também não age, porque o limiar absoluto não foi atingido. Nesse segundo caso, baixar `skewedPartitionThresholdInBytes` é o ajuste correto.

`forceOptimizeSkewedJoin` existe porque, por padrão, o Spark evita a divisão quando ela obrigaria a introduzir um shuffle extra. Ligar essa config diz "divida mesmo assim, o custo do shuffle adicional é menor que o do straggler". Use quando você já mediu.

Complementos úteis: `spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled` (true desde 3.2.0), `spark.sql.adaptive.rebalancePartitionsSmallPartitionFactor` (0.2 desde 3.3.0), e `spark.sql.adaptive.optimizer.excludedRules` para desabilitar regras adaptativas específicas.

### 2.4 Como enxergar o AQE trabalhando

Isso vale ouro na hora de defender o projeto. No Spark UI, o plano de uma query com AQE aparece marcado como `AdaptiveSparkPlan isFinalPlan=false` durante a execução e `isFinalPlan=true` no fim. Comparar os dois mostra literalmente a re-otimização acontecendo: um `SortMergeJoin` que virou `BroadcastHashJoin`, um `CustomShuffleReader coalesced` reduzindo 1000 partições para 14, um nó `skewed=true`.

```python
df = spark.sql("...")
df.explain(mode="formatted")   # plano antes
df.count()                     # dispara execução
# no Spark UI, aba SQL/DataFrame, o plano final substitui o inicial
```

Fonte: [Performance Tuning - Adaptive Query Execution](https://spark.apache.org/docs/latest/sql-performance-tuning.html).

---

## 3. Dynamic Partition Pruning

DPP entrou no Spark 3.0 e é **ligado por padrão** (`spark.sql.optimizer.dynamicPartitionPruning.enabled = true`). Os livros mencionam, mas raramente explicam as condições de ativação, que é justamente o que separa "sei o nome" de "sei usar".

É uma otimização de **esquema estrela**. Em um join entre uma tabela fato grande e particionada e uma tabela dimensão pequena com filtro, o Spark constrói em tempo de execução um filtro dinâmico a partir dos valores que sobraram no lado pequeno e o injeta como subquery no scan da tabela fato. O resultado é que o scan lê apenas os diretórios de partição relevantes, em vez de ler tudo e filtrar depois do join.

```sql
SELECT f.pedido_id, f.valor, d.nome_loja
FROM fato_vendas f
JOIN dim_loja d ON f.data_particao = d.data_particao
WHERE d.regiao = 'SUDESTE';
```

Sem DPP, o Spark lê todas as partições de `fato_vendas`. Com DPP, ele resolve primeiro o filtro na dimensão, descobre quais valores de `data_particao` sobreviveram e lê só esses diretórios.

**Três condições precisam valer ao mesmo tempo:**

1. a tabela fato precisa estar **fisicamente particionada pela chave do join**;
2. a tabela dimensão precisa caber em broadcast (`spark.sql.autoBroadcastJoinThreshold`, 10 MB por padrão);
3. a config de DPP precisa estar ligada.

Se qualquer uma falhar, o DPP simplesmente não acontece e você não recebe aviso. É a razão número um de gente reportar "ativei DPP e não mudou nada".

O DPP é aplicado na fase de otimização lógica, pelas regras `PartitionPruning` e `CleanupDynamicPruningFilters`, e **não se aplica a queries de streaming**. Existem sub-configs internas em `SQLConf` que não aparecem na página pública de configuração e controlam quando reutilizar o broadcast em vez de rodar uma subquery extra: `spark.sql.optimizer.dynamicPartitionPruning.useStats`, `.fallbackFilterRatio` e `.reuseBroadcastOnly`.

Uma distinção que confunde bastante: **DPP** poda **partições**, ou seja, diretórios, e é do Spark open source. **Dynamic File Pruning** poda **arquivos e row groups** usando estatísticas de min/max, e é recurso de Delta Lake / Databricks, não do Spark puro. Não são a mesma coisa e não se substituem.

Fontes: [Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html), [The Internals of Spark SQL - Dynamic Partition Pruning](https://books.japila.pl/spark-sql-internals/dynamic-partition-pruning/).

---

## 4. Spark 4.x, item por item

### 4.0.0 (23/05/2025) - mais de 5.100 tickets, 390 contribuidores

**Modo ANSI SQL por padrão (SPARK-44444).** `spark.sql.ansi.enabled = true`. Divisão por zero, overflow numérico e cast inválido agora **falham** em vez de retornar `null` em silêncio. Esta é a mudança de comportamento mais impactante para código legado, e a mais fácil de defender em prova: silenciar erro aritmético em pipeline de dados é como engolir exceção em produção. Existe migration guide e dá para reverter pela config, mas reverter é dívida.

**Tipo VARIANT (SPARK-45827).** Tipo nativo para dados semiestruturados, tipicamente JSON, armazenado em formato binário aberto, com acesso a campos aninhados sem reparse a cada leitura. Resolve um incômodo antigo: payload de API e resposta de serviço HTTP normalmente aterrissam como JSON string, e sem VARIANT isso vira `get_json_object` custando parse por linha por acesso. No **4.1** o VARIANT virou GA com **shredding** (SPARK-54454), que materializa subcampos frequentes em colunas físicas. O tipo também foi adotado no **Iceberg v3**.

**Spark Connect maduro.** Arquitetura cliente-servidor via gRPC e Arrow, introduzida no 3.4. O 4.0 trouxe o cliente Python leve `pyspark-client` (cerca de 1,5 MB, contra centenas de MB do `pyspark` completo), paridade de API no cliente Java, clientes em Go, Swift e Rust, e a config `spark.api.mode` para alternar entre clássico e Connect sem reescrever a aplicação.

Duas correções importantes contra o que se lê por aí. Primeiro: **o Connect não é o modo padrão no Spark 4**. O default continua sendo `classic`; o código só usa `connect` quando `SPARK_CONNECT_MODE=1` está setado, que é o que o tarball dedicado faz. Segundo: os release notes oficiais **nunca declaram GA para o framework Connect como um todo**, apenas para subcomponentes como o Spark ML on Connect (4.1). É produção-ready para os casos suportados, com uma lacuna conhecida: `SparkContext` e a API RDD não são acessíveis via Connect.

```bash
pip install pyspark-client==4.2.0
```

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
```

**Python Data Source API (SPARK-44076).** Dá para escrever conectores batch e streaming, leitura e escrita, em Python puro sobre o DataSource V2, sem tocar em Scala. Se você já escreveu extractor de API paginada, este é o caminho para transformá-lo em fonte Spark de primeira classe.

**Structured Streaming com estado de verdade.** A Arbitrary State API v2 (SPARK-46815) trouxe `transformWithState`, com múltiplas variáveis de estado (`ValueState`, `ListState`, `MapState`), **TTL por estado**, column families e timers, além da versão `transformWithStateInPandas`. Junto veio o **State Data Source** (SPARK-45511), que permite **ler o conteúdo do state store como se fosse uma tabela**. Antes, o estado do streaming era caixa-preta e a única depuração possível era log.

**Mensagens de erro estruturadas.** Error conditions com códigos **SQLSTATE**, contexto da query apontando o trecho exato do DataFrame que causou a falha, e comportamento consistente entre Scala e Python. Veio sendo construído desde o 3.3 e se consolidou aqui.

**Quebras de compatibilidade.** Scala 2.13 é o padrão e o 2.12 foi removido (SPARK-45314). JDK 17 virou o mínimo, Java 8 e 11 saíram (SPARK-45315). Python 3.8 foi removido (SPARK-47993). Outros itens do 4.0 que os livros não têm: SQL UDFs, session variables, collations de string, fonte XML nativa, SQL scripting e Kubernetes Operator.

### 4.1.0 (16/12/2025) - mais de 1.800 tickets

- **Spark Declarative Pipelines** (SPARK-51727): você declara datasets e queries, o Spark monta o grafo, ordena dependências, paraleliza, gerencia checkpoints e retries. É a versão open source do conceito de Delta Live Tables.
- **Real-Time Mode no Structured Streaming** (SPARK-53736): primeiro suporte oficial a latência sub-segundo contínua, com tarefas stateless chegando a milissegundos de um dígito. Isso derruba o argumento "Spark é micro-batch, logo não serve para tempo real" que ainda está nos livros.
- **SQL Scripting GA** (SPARK-54499), ligado por padrão.
- **VARIANT GA com shredding**, recursive CTE, sketches aproximados (KLL, Theta).
- **UDF e UDTF Arrow-nativas** com decorators (SPARK-52214, SPARK-52979): execução PyArrow sem o custo de converter para pandas no meio.
- **Spark ML on Connect GA** (SPARK-51236) e **driver JDBC para Connect** (SPARK-53484).
- **Shuffle com checksum e retry completo de estágio** (SPARK-51756), contra resultado incorreto por corrupção de shuffle.
- Python mínimo sobe para 3.10 (SPARK-52561).

### 4.2.0 (14/07/2026) - mais de 1.700 tickets, 250 contribuidores

- **Geoespacial nativo**: tipos `GEOMETRY` e `GEOGRAPHY`, funções `ST_*`, leitura e escrita WKB/WKT e Parquet, registro de SRID. Ligado por padrão.
- **CDC nativo**: cláusula SQL `CHANGES` mais APIs DataFrame, PySpark e Connect para ler mudanças em nível de linha, em batch e em streaming; **Auto CDC** no SDP para upsert SCD Type 1 declarativo.
- **UDFs Python Arrow-otimizadas e IPC baseado em Arrow ligados por padrão**: ganho de performance de PySpark sem alterar uma linha de código.
- **`NEAREST BY`**: join Top-K de vizinho mais próximo como primitiva do Catalyst, relevante para busca vetorial e deduplicação por similaridade.
- **DSv2** com transações, evolução de schema no `INSERT` e métricas mais ricas.
- **Metric Views** (`CREATE VIEW ... WITH METRICS`): modelagem semântica declarativa dentro do próprio Spark.
- **Web UI modernizada** com dark mode, plano SQL pesquisável e zoomável, e comparação lado a lado.
- Suporte a **Java 25**, com a ressalva de que versões anteriores à 25.0.3 já entram deprecadas. Scala empacotada: 2.13.18.
- No Connect: compatibilidade com API RDD, API `GetStatus` no servidor, liberação de sessão ao encerrar o processo, otimizações em `head()`, `take()` e `tail()`, integração com History Server.

Fontes: [4.0.0](https://spark.apache.org/releases/spark-release-4-0-0.html), [4.1.0](https://spark.apache.org/releases/spark-release-4.1.0.html), [4.2.0](https://spark.apache.org/releases/spark-release-4-2-0.html), [Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html).

---

## 5. Photon, e por que ele não é aberto

**O que é.** Motor de execução vetorizado escrito em **C++ nativo**, que substitui o runtime de execução JVM do Spark SQL. O Catalyst continua planejando a query. Na execução, os operadores suportados rodam no runtime C++ sobre batches colunares Arrow, em vez do caminho de whole-stage codegen da JVM. As APIs do Spark continuam as mesmas, então o código existente roda sem alteração. Public preview em junho de 2021; em 2026 está ligado por padrão em compute serverless e SQL warehouses da Databricks, e disponível por checkbox em compute clássico.

**Por que ele existe.** O [paper do SIGMOD 2022](https://people.eecs.berkeley.edu/~matei/papers/2022/sigmod_photon.pdf) (Behm et al., prêmio de melhor paper industrial) é explícito: as cargas viraram **CPU-bound**, e a JVM impunha tetos estruturais. O garbage collector degrada com heaps acima de aproximadamente 64 GB. O JIT tem limites duros de codegen: métodos acima de cerca de 8.000 bytes de bytecode não são compilados, e acima de cerca de 325 bytes não são inlined. Queries complexas o bastante caíam de volta no interpretador estilo Volcano, justamente as que mais precisavam de velocidade. Ganho típico relatado: 2x a 5x em cargas analíticas, chegando a cerca de 10x em queries pesadas de scan e agregação.

**A relação com o Tungsten.** Photon é a continuação e superação do Tungsten, não algo paralelo. O Tungsten (Spark 1.6 a 2.0), que os livros cobrem bem, já atacava o mesmo problema: gerenciamento de memória off-heap, formato binário compacto e whole-stage code generation. Mas fazia isso **dentro da JVM** e, no essencial, processando linha a linha com código gerado. O Photon abandona as duas amarras: sai da JVM, com memória e alocação controladas em C++ e sem GC, e troca codegen por **vetorização interpretada** sobre batches colunares, o que dá SIMD, melhor uso de cache e adaptividade em runtime sem recompilar. Mesmo objetivo, gerações diferentes de resposta.

**Por que não é open source.** Photon é produto proprietário da Databricks e um dos principais diferenciais comerciais da plataforma. Não há repositório, não há release, não há plano anunciado de abertura. O paper descreve o design em detalhe, o código não é publicado. Vale registrar a tensão intelectual: a tese central do paper de 2016 sobre o Spark é o **motor unificado**, e a empresa fundada pelos autores do Spark substituiu o motor de execução por um runtime nativo fechado. A unificação sobreviveu na **API**; o motor único não sobreviveu.

**A resposta do ecossistema aberto** foram projetos com a mesma ideia e licença permissiva: **Apache Gluten + Velox** (Apache 2.0, Top-Level Project da ASF desde março de 2026, relatando 3x a 4x em TPC-H e TPC-DS) e **Apache DataFusion Comet** (Rust). O Native Execution Engine do Microsoft Fabric é um wrapper proprietário sobre o Gluten, o que sinaliza que a execução nativa aberta virou a base padrão fora da Databricks.

Fontes: [Photon docs](https://docs.databricks.com/aws/en/compute/photon), [anúncio do preview](https://www.databricks.com/blog/2021/06/17/announcing-photon-public-preview-the-next-generation-query-engine-on-the-databricks-lakehouse-platform.html).

---

## 6. O ecossistema lakehouse em 2026

Os livros tratam Delta Lake como "o" formato transacional. Em 2026 o cenário é de três formatos e de convergência entre eles.

- **Apache Iceberg** virou o **padrão de fato**. É lido e escrito por praticamente todo cloud e engine, inclusive pela Databricks, que comprou a Tabular e trouxe os criadores do Iceberg para dentro de casa. É nativo na AWS via S3 Tables. O **v3** fechou as últimas lacunas com deletion vectors, row lineage e o tipo VARIANT.
- **Delta Lake** segue como peso-pesado em base instalada e formato nativo da Databricks, e escolheu convergência em vez de disputa: o **UniForm** escreve Delta e expõe metadados Iceberg e Hudi ao mesmo tempo.
- **Apache Hudi** é o especialista em **mutação**. Para ingestão de streaming, CDC e upsert de alta frequência, seu índice e o modo Merge-on-Read continuam sendo as ferramentas mais bem construídas para o problema.

Onde o Spark se encaixa nisso: ele deixou de ser "o sistema" e virou o **motor de compute** de um lakehouse cujo armazenamento é definido por formatos abertos. Lê e escreve os três, e a decisão de formato deixou de ser permanente graças a UniForm e Apache XTable. Ao mesmo tempo, o Spark subiu de camada. Com Declarative Pipelines (4.1) e CDC nativo (4.2) ele passou a cobrir orquestração e ingestão incremental que antes exigiam ferramenta externa. Com Spark Connect virou serviço remoto acessível de qualquer linguagem. Com Real-Time Mode entrou em faixa de latência que era território do Flink.

Fontes: [Lakehouse Table Formats in 2026](https://amdatalakehouse.substack.com/p/lakehouse-table-formats-in-2026-iceberg), [comparativo Hudi vs Delta vs Iceberg](https://www.onehouse.ai/blog/apache-hudi-vs-delta-lake-vs-apache-iceberg-lakehouse-feature-comparison).

---

## 7. O livro diz X, hoje é Y

| Tema | O livro (2020-2021, Spark 3.0-3.1) diz | Em julho de 2026 |
|---|---|---|
| Versão de referência | Spark 3.0 / 3.1 | **Spark 4.2.0** (14/07/2026); 3.5.9 como LTS estendido até nov/2027 |
| AQE | "Ative manualmente com `spark.sql.adaptive.enabled=true`" | **Ligado por padrão desde o 3.2.0**; o que se aprende hoje é quando desligar e como ler o plano final |
| `spark.sql.shuffle.partitions` | "Calibre com cuidado, o default 200 quase nunca serve" | Configure **alto** e deixe o coalescing do AQE reduzir; o número virou teto, não alvo |
| Skew | "Faça salting manual da chave" | AQE divide partições skewed automaticamente (>256 MB **e** >5x a mediana); salting é fallback |
| Modo ANSI SQL | Opcional e desligado; overflow e cast inválido retornam `null` | **Padrão desde o 4.0** (SPARK-44444); operações inválidas **falham** |
| Java | "Roda em Java 8 e 11" | **Java 17, 21 e 25**; Java 8 e 11 removidos no 4.0 |
| Scala | "Scala 2.12 (e 2.13 experimental)" | **Só Scala 2.13** no 4.x (2.13.18 no 4.2.0); 2.12 descontinuado |
| Python | "Python 3.6 ou 3.8+" | Mínimo **3.10**; suporte declarado até 3.14 |
| Arquitetura do driver | Driver roda junto com a aplicação, monólito | **Spark Connect**: cliente-servidor via gRPC/Arrow, cliente leve de 1,5 MB, clientes em Go, Rust, Swift, JDBC. Não é o default e não tem RDD |
| Estado no streaming | `mapGroupsWithState` / `flatMapGroupsWithState`, estado opaco | **`transformWithState`** com múltiplas variáveis, TTL, timers; **State Data Source** lê o estado como tabela |
| Latência do streaming | Micro-batch, piso de ~100 ms | **Real-Time Mode** (4.1) entrega sub-segundo contínuo |
| DStreams | Apresentada como API de streaming | **Deprecada desde o 3.4** |
| JSON semiestruturado | `from_json` / `get_json_object` sobre string | Tipo **VARIANT** nativo, binário, GA com shredding no 4.1 |
| Conectores customizados | Só em Scala/Java sobre DSv2 | **Python Data Source API** (4.0), batch e streaming, em Python puro |
| Mensagens de erro | Stack trace de JVM | Error conditions com **SQLSTATE** e contexto da query |
| Formato de tabela | Delta Lake como escolha natural | Três formatos, com **Iceberg** como padrão de fato; convergência via UniForm e XTable |
| Execução | Tungsten + whole-stage codegen na JVM é o estado da arte | Motores nativos: **Photon** (fechado), **Gluten+Velox** e **Comet** (abertos) substituem o runtime JVM |
| Ambiente gratuito para estudar | Databricks Community Edition | **CE aposentada em 01/01/2026**; hoje é a **Free Edition**, serverless-only, só Python e SQL, sem cluster clássico |
| Papel do Spark | "O sistema unificado de big data" | **Motor de compute** de um lakehouse de formatos abertos, que subiu para orquestração (SDP) e ingestão (CDC nativo) |

---

## Como usar isto na aula

Três coisas que valem virar pergunta ao professor, porque são exatamente os pontos onde o material oficial e a realidade de 2026 divergem:

1. Se o AQE está ligado por padrão desde o 3.2 e resolve coalescing, conversão de join e skew, o que sobrou de tuning manual que ainda vale a pena ensinar em 2026?
2. Os livros ensinam DPP como recurso do Spark, mas as três condições de ativação quase nunca são verificadas na prática. Como se checa, no plano físico, se o DPP de fato ocorreu?
3. Se a tese do paper de 2016 é o motor unificado, e a Databricks reescreveu a execução em C++ fora da JVM com o Photon, a unificação sobreviveu na API ou no motor?
