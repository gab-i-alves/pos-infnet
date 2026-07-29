---
title: "Aula 01 de Spark - Aprofundamento"
aula: "Aula 01 - Arquitetura unificada do Spark, processamento em larga escala e preparação do ambiente"
data: 2026-07-24
tags:
  - spark
  - pos-infnet
  - aula-01
  - aprofundamento
  - arquitetura-spark
  - catalyst
  - tungsten
  - aqe
  - spark-connect
  - databricks
---

# Aula 01 · Aprofundamento

Este é o documento de aprofundamento da primeira aula: o que foi estudado por conta própria, além da bibliografia que o professor passou. A bibliografia oficial cobre o Spark 3.0; aqui a versão de referência é o **Apache Spark 4.2.0**, de 14 de julho de 2026, e cada parte marca explicitamente onde os livros ficaram para trás.

As cinco partes têm propósitos diferentes. Para **entender o motor**, leia 1, 3 e 5. Para **decidir se o Spark é a ferramenta certa**, leia 2. Para **executar**, leia 4 com o terminal aberto.

## Sumário

| Parte | Do que trata |
|---|---|
| [1. Arquitetura e modelo de execução](#parte-1---arquitetura-e-modelo-de-execução) | driver, executores, cluster manager; do código à task; narrow vs wide; partições; memória e OOM; como ler a Spark UI |
| [2. Por que o Spark existe, e quando não usar](#parte-2---por-que-o-spark-existe-e-quando-não-usar) | o que o MapReduce não resolvia; a tese do motor unificado; e o contra-argumento honesto de 2026 |
| [3. RDD, DataFrame, Dataset, Catalyst e Tungsten](#parte-3---rdd-dataframe-dataset-catalyst-e-tungsten) | a linhagem das APIs; por que Dataset tipado não existe em PySpark; ler `explain()` de verdade; a armadilha das UDFs Python |
| [4. Montando o ambiente em 2026](#parte-4---montando-o-ambiente-em-2026) | versões e requisitos atuais; pip/uv, Docker, Spark Connect; o que a Databricks Free Edition tem e não tem; smoke tests |
| [5. O que mudou desde os livros](#parte-5---o-que-mudou-desde-os-livros) | releases 3.2 a 4.2; AQE e skew join em profundidade; DPP; Spark 4 item por item; Photon |

Este documento é a segunda etapa do método. A primeira etapa é o [guia de leitura](01-pre-aula.md), com as perguntas a responder lendo os capítulos; o registro do que eles de fato dizem está no [gabarito](01-gabarito.md), com 55 dúvidas numeradas e sete divergências entre Luu e Damji; a seção final dele, "O que fica para o aprofundamento", lista as perguntas que este texto responde. As perguntas que nasceram daqui estão em [03-aula.md](03-aula.md), junto com as preparadas para a aula ao vivo, e o artefato que fecha o ciclo em [04-pos-aula.md](04-pos-aula.md).

---

## Parte 1 - Arquitetura e modelo de execução

Versão de referência: **Apache Spark 4.2.0**, lançado em 14 de julho de 2026 ([release notes](https://spark.apache.org/releases/spark-release-4-2-0.html)). Os livros da bibliografia (Luu, 2021; Damji et al., 2020) descrevem o Spark 3.0. Onde o comportamento mudou, este documento marca a diferença.

---

### 1. O mapa mental: quem é quem e o que roda onde

Uma aplicação Spark é um sistema distribuído de três peças. Entender qual peça faz o quê resolve metade dos problemas que você vai enfrentar.

**Driver.** É o processo que roda o seu `main()` e hospeda a `SparkSession`. Ele não processa dados. Ele **planeja**: transforma o seu código em plano lógico, otimiza com o Catalyst, produz o plano físico, quebra em stages, monta as tasks e as distribui. Também recebe os resultados de volta. O driver precisa ser endereçável na rede, porque os executores abrem conexão **de volta** para ele (`spark.driver.host`, `spark.driver.port`). Expõe a Spark UI na porta 4040. Defaults que importam: `spark.driver.memory=1g`, `spark.driver.maxResultSize=1g`.

**Executores.** São JVMs lançadas nos nós de trabalho. Cada executor tem um número de cores (`spark.executor.cores`) e uma quantidade de memória (`spark.executor.memory`, default `1g`). Eles executam tasks em múltiplas threads e guardam dados em cache. Vivem pela duração inteira da aplicação, salvo alocação dinâmica. Os executores são o único lugar onde o dado real é tocado.

**Cluster manager.** É quem aloca recursos. Standalone, YARN e Kubernetes são os três em uso hoje (Mesos foi removido no Spark 4.0). O Spark é agnóstico: o mesmo código roda em qualquer um. O que muda é como os containers nascem e onde o driver vive.

A regra de isolamento é dura e vale a pena internalizar: **cada aplicação recebe seus próprios executores**. Dois jobs Spark não compartilham cache nem dados sem passar por storage externo. Isso simplifica o scheduling e o isolamento, ao custo de você não conseguir reaproveitar um cache entre aplicações.

#### Client mode e cluster mode

A diferença é uma só: **onde o driver roda**.

| | client mode | cluster mode |
|---|---|---|
| Driver | no processo que fez o submit (seu notebook, um edge node) | dentro do cluster (container do AM no YARN, pod no K8s) |
| Matar o cliente | mata o job | não afeta o job |
| Latência driver/executores | atravessa a rede corporativa | dentro do cluster |
| Uso típico | shells interativos, notebooks | produção, jobs agendados |

A bibliografia não diz isso em lugar nenhum: client e cluster mode aparecem só como duas linhas da Tabela 1-1 do Damji 1, presas ao YARN, e a diferença fica implícita na coluna do driver. O Luu não menciona nenhum dos dois. Shell interativo **exige** client mode: o REPL precisa do driver local. Produção usa cluster mode. Se o seu job faz `collect()` de algo grande em client mode, os dados atravessam a VPN até a sua máquina. Referência: [Cluster Mode Overview](https://spark.apache.org/docs/latest/cluster-overview.html).

#### SparkSession e SparkContext

O `SparkContext` é a conexão de baixo nível com o cluster. Ele instancia o `DAGScheduler`, o `TaskScheduler` e o `SchedulerBackend`. Só pode existir um ativo por JVM. O `SparkSession` (Spark 2.0 em diante) é o ponto de entrada unificado que absorveu `SQLContext` e `HiveContext`, dono do catálogo, do parser SQL e do Catalyst. Ele cria ou reusa um `SparkContext` internamente, acessível via `spark.sparkContext`. Vale saber que a bibliografia erra aqui: o Damji 1 escreve que no shell a `SparkSession` é acessível pela variável global `spark` **ou** `sc`, e o banner do capítulo 2 do próprio livro desmente isso separando os dois objetos; o Luu 1 apoia o word count em `sc.textFile(...)` sem apresentar o `sc` e sem escrever a palavra `SparkContext` uma vez sequer.

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("estudo-arquitetura")
         .master("local[*]")
         .getOrCreate())

sc = spark.sparkContext  # só desça aqui para RDD, broadcast, checkpointDir, setLogLevel
```

**O que os livros não cobrem.** Desde o Spark 3.4 existe o **Spark Connect**, uma arquitetura cliente-servidor onde o cliente só envia o plano (protobuf sobre gRPC) e não hospeda mais um driver JVM local. No Spark 4.x ele amadureceu bastante: cliente Python leve (`pip install pyspark-client`, cerca de 1,7 MB contra centenas de MB do `pyspark` completo), paridade de API no cliente Java, Spark ML on Connect declarado GA no cliente Python no 4.1.0. Duas ressalvas importantes: **o modo padrão continua sendo o Spark Classic**, e `SparkContext` e a API de RDD **não são acessíveis via Connect**, o que a documentação afirma com todas as letras. Isso significa que, com Connect, o seu código está restrito a DataFrame/SQL por construção. A chave que troca os dois é `spark.api.mode`, documentada em [Application Development with Spark Connect](https://spark.apache.org/docs/latest/app-dev-spark-connect.html); o valor default `classic` não aparece em nenhuma página oficial, só em discussão da lista de desenvolvimento, então trate o número como bem apoiado mas não citável. Referência do resto: [Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html).

---

### 2. Do código ao task: o caminho de uma query

Nada acontece quando você escreve uma transformação. `map`, `filter`, `select`, `join`, `groupBy` apenas constroem um grafo de dependências. Só uma **ação** (`collect`, `count`, `show`, `write`, `foreach`, `take`) dispara execução. Isso é a avaliação preguiçosa, e ela existe por um motivo concreto: sabendo a cadeia inteira antes de executar, o Spark funde operações, empurra filtros para a fonte e evita materializar intermediários.

Detalhe que engana: algumas chamadas que **não** são ações disparam jobs mesmo assim. Inferência de schema em CSV/JSON lê os dados. `Dataset.rdd` força materialização. O cálculo de estatísticas para decidir se um join vira broadcast também roda um job. Se você viu um job na UI e não chamou nenhuma ação, é um desses.

O caminho completo:

```text
código (DataFrame/SQL)
  -> plano lógico não resolvido
  -> Catalyst: análise -> otimização lógica -> planejamento físico -> codegen
  -> DAG de RDDs
  -> [AÇÃO] -> SparkContext.runJob -> DAGScheduler
  -> stages (N x ShuffleMapStage + 1 ResultStage)
  -> TaskSet por stage -> TaskScheduler -> TaskSetManager
  -> SchedulerBackend -> cluster manager -> Executor.launchTask
```

**O DAGScheduler** roda apenas no driver, em um event loop assíncrono. Ele percorre a linhagem **de trás para frente**, partindo do RDD final. Cada dependência de shuffle que encontra vira uma **fronteira de stage**. Ele cria um `ShuffleMapStage` para cada shuffle e exatamente um `ResultStage` por job. Antes de submeter um stage, verifica se os pais estão completos; se não, submete os pais e coloca o stage atual em espera. Quando submete, cria tasks **apenas para as partições ainda não computadas**. É por isso que você vê "skipped stages" na UI: o output de shuffle já existia e foi reaproveitado. Isso é bom, não é erro. Referência: [The Internals of Spark Core, DAGScheduler](https://books.japila.pl/apache-spark-internals/scheduler/DAGScheduler/).

**O TaskScheduler** recebe o `TaskSet` e agenda por **localidade**, na ordem `PROCESS_LOCAL` -> `NODE_LOCAL` -> `NO_PREF` -> `RACK_LOCAL` -> `ANY`, esperando `spark.locality.wait` antes de degradar um nível. Também cuida de retentativas (`spark.task.maxFailures=4`, ou seja três novas tentativas) e de execução especulativa (desligada por default).

A hierarquia final, que você deve saber recitar: **Application -> Job (um por ação) -> Stage (delimitado por shuffle) -> Task (uma por partição do stage)**. Uma task é a menor unidade enviada a um executor e ocupa um slot (`spark.task.cpus=1`).

Dentro de um stage, as transformações narrow são **pipelined**: uma única task lê um registro e aplica filtro, projeção e mapeamento em uma passada, sem materializar nada entre elas. O whole-stage codegen do Spark SQL vai além e compila o subplano inteiro em uma única função Java em runtime. No plano físico, isso aparece como `*(2) Filter`, `*(2) Project`: todos os nós com o mesmo `*(N)` viraram uma função só.

---

### 3. Narrow e wide: a distinção que governa performance

**Narrow**: cada partição do pai é consumida por no máximo uma partição do filho. `map`, `filter`, `flatMap`, `mapPartitions`, `union`, `coalesce` reduzindo, joins já co-particionados. Sem rede, sem barreira, sem disco intermediário. Se uma partição se perde, recomputa só ela.

**Wide** (dependência de shuffle): cada partição do pai pode alimentar várias partições do filho. `groupByKey`, `reduceByKey`, `sortByKey`, `join` por sort-merge, `distinct`, `repartition`, funções de janela.

Por que o shuffle domina o custo, em seis camadas empilhadas:

1. **Disco no lado do map.** Cada map task ordena por partição de destino e escreve arquivos locais em `spark.local.dir`.
2. **Rede all-to-all.** M map tasks vezes R reduce tasks dá até M x R conexões de fetch.
3. **Barreira global.** O reduce só começa depois que **todas** as map tasks terminaram. O straggler mais lento segura o stage inteiro.
4. **Serialização e desserialização** de tudo que atravessa, com pressão de GC junto.
5. **Spill.** Quando as estruturas de hash ou sort não cabem na memória de execução, vazam para disco.
6. **Amplificação por skew.** Uma chave quente concentra tudo em uma reduce task.

As consequências práticas caem sozinhas: prefira `reduceByKey` a `groupByKey` (o primeiro combina no lado do map); use broadcast join quando um lado couber (`spark.sql.autoBroadcastJoinThreshold`, default 10 MB, ou o hint `/*+ BROADCAST(t) */`); filtre e projete **antes** do shuffle; use `coalesce` (narrow) em vez de `repartition` (wide) quando só quiser reduzir o número de partições.

A leitura rápida de custo de um plano é literal: **conte os nós `Exchange`**. Cada `Exchange` é um shuffle. Referência: [Shuffle operations](https://spark.apache.org/docs/latest/rdd-programming-guide.html#shuffle-operations).

---

### 4. Partições: o conceito operacional do dia a dia

Partição é a unidade de paralelismo. A equação básica é curta: **1 partição = 1 task = 1 slot ocupado**. Slots totais no cluster = `número de executores x spark.executor.cores / spark.task.cpus`.

**Na leitura de arquivos**, o Spark SQL calcula o tamanho do split assim:

```text
bytesPerCore  = (soma_bytes + num_arquivos * openCostInBytes) / minPartitionNum
maxSplitBytes = min(spark.sql.files.maxPartitionBytes,
                    max(spark.sql.files.openCostInBytes, bytesPerCore))
```

Defaults: `spark.sql.files.maxPartitionBytes = 128 MB` e `spark.sql.files.openCostInBytes = 4 MB`. Esse segundo valor é um custo fictício somado a cada arquivo, e é o mecanismo que faz muitos arquivos pequenos serem empacotados na mesma partição. Formatos não divisíveis (gzip, ou um Parquet com um row group único gigante) ignoram tudo isso: um arquivo vira uma partição, e você perde o paralelismo. É por isso que **o problema dos arquivos pequenos em object storage não é só custo de listagem, é forma da partição**.

**Depois de um shuffle**, o número de partições é `spark.sql.shuffle.partitions`, que continua com default **200**. Esse é o pior default do Spark: 200 é absurdo tanto para 1 GB quanto para 10 TB.

**AQE resolve boa parte disso em runtime.** `spark.sql.adaptive.enabled` é `true` por padrão desde o Spark 3.2. Ao fim de cada query stage, o Spark tem estatísticas **reais** de shuffle e reotimiza o resto do plano:

- **Coalescing de partições pós-shuffle**: junta partições pequenas até `advisoryPartitionSizeInBytes` (default 64 MB). Na prática, isso mata o problema do 200 fixo.
- **Skew join**: divide partições enviesadas. A partição precisa violar **os dois** critérios: ser maior que 5 vezes a mediana (`skewedPartitionFactor`) **e** maior que 256 MB (`skewedPartitionThresholdInBytes`).
- **Conversão em runtime** de sort-merge join para broadcast hash join, quando o lado ficou pequeno depois dos filtros.

A regra de ouro da documentação: **2 a 3 tasks por core** do cluster, com partições na faixa de 100 a 200 MB de dados por task. Tasks de 200 ms já valem a pena; é seguro subir o paralelismo acima do número de cores. Referência: [Spark SQL Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html).

---

### 5. Memória do executor e a anatomia de um OOM

O container de um executor não é só `spark.executor.memory`. É:

```text
container = spark.executor.memory
          + spark.executor.memoryOverhead   (10% do heap, mínimo 384 MiB)
          + spark.memory.offHeap.size       (se habilitado)
          + spark.executor.pyspark.memory   (workers Python)
```

Dentro do heap:

```text
+--------------------------------------------------+
| Reserved: 300 MiB (fixo, no código)               |
+--------------------------------------------------+
| Unified Memory M = 0.6 * (heap - 300 MiB)         |
|   Execution              |  Storage               |
|   shuffle, join, sort,   |  cache, broadcast      |
|   agregação              |  R = 0.5 * M protegido |
|          <-- fronteira móvel -->                  |
+--------------------------------------------------+
| User memory: 40% - estruturas suas, metadados     |
+--------------------------------------------------+
```

Duas regras assimétricas explicam quase todo comportamento de memória:

1. **Execution pode expulsar storage**, até storage cair abaixo de `R`.
2. **Storage nunca expulsa execution.**

O motivo é simples: dados de storage podem ser recomputados pela linhagem, dados de execução no meio de um sort não podem sumir sem quebrar a query.

#### O catálogo de OOM

| Sintoma | Causa provável | Ação |
|---|---|---|
| `Container killed by YARN for exceeding memory limits` | overhead insuficiente. 10% é pouco para PySpark, UDFs, Arrow, buffers Netty | subir `spark.executor.memoryOverhead` para 15 a 25% |
| `OutOfMemoryError: Java heap space` no executor | partição grande demais, skew, chave quente | mais partições, `reduceByKey`, salting, AQE skew join |
| OOM no **driver** | `collect()` ou `toPandas()` de dataset grande, muitos broadcasts, plano com milhares de partições | escreva em vez de coletar; `spark.driver.maxResultSize` protege parcialmente |
| `Total size of serialized results is bigger than spark.driver.maxResultSize` | mesma causa | idem |
| GC Time acima de 10 a 20% do task time | cache demais pressionando a old gen | reduzir `spark.memory.fraction`, usar `MEMORY_ONLY_SER` |
| Spill (Disk) enorme | memória de execução insuficiente por task | mais partições, ou menos cores por executor |

**A armadilha específica do PySpark.** Os workers Python vivem **fora** do heap da JVM e fora do gerenciador unificado de memória. Uma UDF Python que aloca muito não gera `OutOfMemoryError`: gera o cluster manager matando o container. É a causa número um de container kill em PySpark, e o sintoma não parece um problema de memória do Spark. No Kubernetes o `memoryOverheadFactor` já sobe para 0.40 em jobs não-JVM justamente por isso.

**Dimensionamento.** Os cores de um executor dividem a mesma memória unificada. Um executor com 32 cores e 64 GB dá 2 GB por task e pausas de GC longas. O padrão prático recomendado há anos é algo em torno de 5 executores de 5 cores, o que também evita saturar o throughput de I/O. Referência: [Tuning Spark](https://spark.apache.org/docs/latest/tuning.html).

---

### 6. Tolerância a falhas por linhagem

O Spark não replica dados intermediários. Ele guarda o **grafo de transformações** que produziu cada RDD. Se uma partição se perde, recomputa aquela partição a partir dos pais. A resiliência do "R" de RDD é isso, e não replicação.

- **Falha de task**: retenta até `spark.task.maxFailures=4`.
- **`FetchFailedException`**: o caso interessante. Uma reduce task não consegue buscar um bloco de shuffle porque o executor que o produziu morreu. O DAGScheduler marca as saídas daquele executor como perdidas e **ressubmete o `ShuffleMapStage` pai**, recomputando apenas as partições de mapa faltantes. É um restart parcial, não um restart do job.
- **Arquivos de shuffle** sobrevivem enquanto os RDDs correspondentes não forem coletados pelo GC. Isso é o que impede a recomputação de cascatear para o mundo inteiro.
- **External shuffle service** desacopla os arquivos de shuffle do ciclo de vida do executor: o executor pode morrer sem levar o shuffle junto. É pré-requisito clássico da alocação dinâmica. Alternativa moderna, padrão no Kubernetes: `spark.dynamicAllocation.shuffleTracking.enabled=true`.

#### cache/persist e checkpoint não são a mesma coisa

| | `cache()` / `persist()` | `checkpoint()` |
|---|---|---|
| Onde grava | memória e disco do executor | storage confiável (HDFS, S3, GCS) via `sc.setCheckpointDir()` |
| Linhagem | **preservada** (é o plano B se o cache evaporar) | **truncada** |
| Custo | barato | escreve dados e **recomputa o RDD do zero** em um job separado |
| Se o executor morre | recupera recomputando | lê do arquivo |

O padrão canônico é `rdd.cache(); rdd.count(); rdd.checkpoint()`. Sem o cache, o RDD é computado duas vezes, porque o checkpoint dispara um job próprio.

Quando usar cada um: recomputação por linhagem é barata para linhagens curtas e narrow. Fica cara e perigosa em linhagens longas (algoritmos iterativos, loops de ML) ou muito wide, onde recomputar uma partição pode exigir refazer shuffles inteiros. Aí entra o checkpoint. O cache usa eviction LRU, e o storage level default é `MEMORY_ONLY`, que **silenciosamente recomputa** o que não coube. `MEMORY_AND_DISK` costuma ser a escolha mais previsível.

---

### 7. Como ler a Spark UI

A UI fica em `http://<driver>:4040` durante a execução, e no History Server (porta 18080) depois, se `spark.eventLog.enabled=true` estiver ligado. Ler a UI é a habilidade que separa quem ajusta configs no chute de quem diagnostica.

**Jobs.** Um job por ação, com timeline de eventos. O que procurar: **gaps na timeline entre jobs**. Gap grande significa tempo gasto no driver, tipicamente planejamento, listagem de arquivos em object storage, ou código Python single-thread. "Skipped stages" é sinal bom.

**Stages.** É onde mora o diagnóstico. Na página de detalhe:

- **Summary Metrics por percentil** (Min / 25th / Median / 75th / Max). Esta é a leitura mais importante da UI inteira: **Max muito maior que a mediana em Duration ou Shuffle Read Size significa data skew**. Um Max dez vezes a mediana explica quase todo stage lento.
- **GC Time**: acima de 10 a 20% da duração é problema de memória, não de CPU.
- **Scheduler Delay**: alto significa cluster saturado ou tasks curtas demais, com overhead de agendamento dominando.
- **Spill (Memory)** e **Spill (Disk)**: se aparecem, a memória de execução é insuficiente. Gatilho direto para aumentar o número de partições.
- **Shuffle Read Blocked Time**: rede ou executores sobrecarregados do lado do map.
- **DAG Visualization**: os nós `Exchange` são literalmente onde está o shuffle.

**SQL / DataFrame.** O grafo do plano físico com métricas inline por operador. É o lugar mais rápido para diagnosticar Spark SQL. Olhe: `number of output rows` no Scan (prova se o pushdown funcionou), a estratégia de join escolhida (`BroadcastHashJoin` é bom, `SortMergeJoin` custa dois shuffles e dois sorts, `BroadcastNestedLoopJoin` é alarme de join sem condição de igualdade), nós `AQEShuffleRead` mostrando o que o AQE fez em runtime, e `isFinalPlan=true` indicando que você está vendo o plano já reotimizado.

**Executors.** Panorama por executor: cores, memória, tasks ativas/falhas/completas, Task Time com GC Time entre parênteses. Procure: GC desproporcional, um executor com muitas falhas (nó defeituoso), distribuição desigual de tasks, executores ociosos (paralelismo abaixo da capacidade). Cada executor tem link para Thread Dump, Heap Histogram e Flame Graph. Thread dump é a ferramenta certa para stage travado sem progresso.

**Storage.** Se "Fraction Cached" está abaixo de 100%, o cache não coube e você está recomputando em silêncio.

**Environment.** A aba Spark Properties é a verdade sobre o que foi efetivamente aplicado. Antes de discutir por que um `--conf` não fez efeito, confirme aqui se ele chegou.

Referência: [Web UI](https://spark.apache.org/docs/latest/web-ui.html).

---

### 8. O que os livros de 2020 e 2021 não contam

Anote estes pontos, porque eles mudam o que você lê nos capítulos:

- **Versão.** A estável é a 4.2.0 (14/07/2026). Existem três ramos 4.x ativos (4.0.x, 4.1.x, 4.2.x) e o 3.5.x ainda recebe patches (3.5.9 em 16/07/2026). Fonte: [downloads](https://spark.apache.org/downloads.html).
- **Runtime.** Spark 4.x roda em **Java 17, 21 ou 25**, **Scala 2.13** (o 2.12 foi removido no 4.0.0) e **Python 3.10 ou superior**. Qualquer tutorial mandando instalar Java 8 está velho. SparkR está marcado como deprecated.
- **AQE ligado por padrão** desde o 3.2. O Luu 1 descreve o mecanismo e para aí; o Damji nem cita. Boa parte do tuning manual de partição que a literatura da época ensinava foi absorvida pelo runtime.
- **UDFs Python otimizadas com Arrow vêm habilitadas por padrão no 4.2.0** (SPARK-54555). No plano, uma `@udf` comum agora aparece como `ArrowEvalPython` em vez de `BatchEvalPython`. Continua sendo uma fronteira de codegen, mas o custo caiu bastante.
- **Databricks Community Edition não existe mais** desde 1º de janeiro de 2026. A oferta gratuita é a **Free Edition**, que é **serverless-only** (sem clusters clássicos configuráveis), só Python e SQL (sem Scala e sem R), com um workspace e um metastore Unity Catalog por conta. Para Structured Streaming, só `Trigger.AvailableNow()`; trigger contínuo falha com `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`. Se o professor sugerir Community Edition, esse é um bom ponto para levantar na aula. Fonte: [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations).

---

### 9. Checklist: o que você deve conseguir explicar depois de ler isto

Marque cada item respondendo em voz alta, sem olhar o texto.

- [ ] Qual é o papel do driver, do executor e do cluster manager, e por que o driver não processa dados.
- [ ] A diferença entre client mode e cluster mode em uma frase, e por que um shell interativo exige client mode.
- [ ] O que o Spark Connect muda na arquitetura, e por que ele não dá acesso a `SparkContext` nem a RDDs.
- [ ] Por que transformações são preguiçosas e quais chamadas disparam job sem serem ações.
- [ ] A cadeia Application -> Job -> Stage -> Task, e o que determina a quantidade de cada uma.
- [ ] Por que o shuffle define a fronteira de stage, e o que significa "barreira global".
- [ ] A definição formal de narrow e wide em termos de partições pai e filho.
- [ ] Seis razões pelas quais o shuffle é caro, sem repetir "porque usa rede".
- [ ] Como o Spark decide o número de partições na leitura de arquivos, e por que gzip estraga o paralelismo.
- [ ] O que o AQE faz em runtime, e quais dois critérios uma partição precisa violar para ser tratada como skew.
- [ ] O layout de memória do executor, incluindo os 300 MiB reservados e por que execution expulsa storage mas não o contrário.
- [ ] Por que uma UDF Python causa container kill em vez de `OutOfMemoryError`.
- [ ] Como uma `FetchFailedException` é tratada, e por que o restart é parcial.
- [ ] A diferença entre `cache()` e `checkpoint()` quanto à linhagem, e por que o padrão é cachear antes de checkpointar.
- [ ] Qual métrica da Spark UI prova data skew, e onde encontrá-la.
- [ ] Como provar, olhando um plano físico, que o predicate pushdown e o column pruning funcionaram.

---

## Parte 2 - Por que o Spark existe, e quando não usar

### 1. O problema que o MapReduce não resolvia

O Luu 1 diz que o MapReduce era lento e que o Spark é rápido, e para aí. O Damji 1 vai bem mais longe: nomeia quatro deficiências do Hadoop (operação difícil, API verbosa, I/O de disco repetido entre pares de tarefas MR, e não servir para outras cargas) e desenha a terceira na Figura 1-1. O que falta nos dois é o passo seguinte, e é ele que você precisa levar para a aula: **por que** materializar em disco entre estágios mata carga iterativa, e o que exatamente o RDD comprou em troca.

O MapReduce, publicado pelo Google em 2004 e reimplementado como Hadoop a partir de 2006, resolveu um problema real: rodar computação tolerante a falhas sobre milhares de máquinas de commodity. O modelo é rígido de propósito. Você tem `map`, você tem `reduce`, e entre os dois há um shuffle. A tolerância a falhas vem de uma regra simples: **cada estágio materializa seu resultado no sistema de arquivos distribuído**. Se um nó cai, o próximo estágio relê do HDFS e a computação continua.

Essa regra é a origem do problema. No HDFS, materializar significa **serializar, escrever em disco e replicar** (tipicamente três cópias). Para um job de uma passada só, isso é aceitável. Para qualquer algoritmo que **reusa** o mesmo conjunto de dados, é ruinoso.

Pense em k-means com 20 iterações. Em cada iteração você lê o dataset, calcula distâncias, atualiza centroides e escreve o resultado. O dataset é o mesmo em todas as 20 rodadas, mas o MapReduce não tem como saber disso: ele paga 20 rodadas de escrita replicada, mais 20 leituras, mais 20 desserializações, para dados que ele mesmo acabou de produzir e vai consumir imediatamente. O mesmo vale para PageRank, para regressão logística, e para **mineração interativa**, onde um analista roda dezenas de queries ad-hoc sobre o mesmo recorte.

Some a isso duas limitações que agravam a primeira:

1. **Modelo de programação pobre.** Só existem `map` e `reduce`. Um join vira uma cadeia de jobs encadeados na mão. Uma agregação composta, idem. E cada elo dessa cadeia paga o custo de materialização descrito acima.
2. **Latência de partida.** Os próprios autores do Spark descrevem, no artigo da CACM de 2016, que as implementações de MapReduce foram desenhadas para batch "com latência de minutos a horas". Isso mata exploração interativa por construção.

O diagnóstico do grupo de Berkeley, então, não foi "MapReduce é lento". Foi: **falta uma abstração de compartilhamento de dados entre estágios**. Tudo o mais decorre disso.

---

### 2. O RDD e o paper de 2012

Matei Zaharia começou o Spark no AMPLab de Berkeley em 2009. O código abriu em 2010. Em 2012 saiu o paper que define a coisa, no NSDI, com Best Paper Award: [*Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing*](https://www.usenix.org/conference/nsdi12/technical-sessions/presentation/zaharia).

Aqui está o mal-entendido mais comum sobre Spark, e vale gastar um parágrafo nele porque ele separa quem leu o resumo de quem entendeu a ideia.

A frase de marketing é: "o Spark é rápido porque é in-memory". Se fosse só isso, não seria pesquisa, seria configuração. O problema científico de verdade é: **como manter dados em memória entre estágios sem perder tolerância a falhas?** Se você não escreve no HDFS replicado, o que acontece quando um executor morre no meio de um job de seis horas?

A resposta do paper é a **linhagem** (*lineage*). Um RDD não é uma coleção de dados; é uma **descrição determinística de como aquela coleção foi derivada** de outros RDDs, por transformações grosseiras (`map`, `filter`, `join`, `groupBy`) aplicadas a todas as partições. O RDD guarda o grafo dessa derivação. Quando uma partição se perde, o Spark não precisa de réplica: ele **recomputa apenas aquela partição** seguindo o grafo de volta até uma fonte estável.

Isso é o que compra o direito de não escrever em disco. A imutabilidade e a granularidade grossa das transformações não são preciosismo funcional, são o que torna a recomputação viável e barata. Um sistema que permitisse escrita fina, célula a célula, teria que logar cada atualização e replicar, e você estaria de volta ao ponto de partida.

Dois números para calibrar:

- O paper reivindica **até 100x** de ganho sobre Hadoop MapReduce em cargas analíticas multi-passo. Note o "multi-passo": em job de uma passada só, o ganho é modesto, porque não há reuso a explorar.
- Em 2014, o Spark ganhou o **Daytona GraySort** ordenando 100 TB três vezes mais rápido que o recorde anterior do Hadoop, usando **dez vezes menos máquinas**. E aqui está o detalhe que vale a pergunta na aula: **esse recorde não foi em memória, foi em SSD**. Ou seja, a própria prova de fogo do Spark contradiz a narrativa "Spark = RAM". O ganho veio de execução mais eficiente, menos materialização replicada e melhor uso de I/O, não de manter 100 TB em heap.

Guarde isso: **a contribuição do RDD é a linhagem, não a memória**. A memória é o benefício que a linhagem viabiliza.

---

### 3. A tese do motor unificado: o que ela de fato entrega

Em novembro de 2016, o mesmo grupo publicou na Communications of the ACM o artigo [*Apache Spark: A Unified Engine for Big Data Processing*](https://dl.acm.org/doi/10.1145/2934664). É o texto por trás do "prestigioso prêmio da ACM" que o Damji 1 cita sem nomear o prêmio, o comitê nem o veículo; o Luu 1 aponta para outros dois artigos e nunca chega a este. O argumento tem duas pernas.

**Perna teórica.** MapReduce, somado a compartilhamento eficiente de dados entre rodadas, consegue emular qualquer computação distribuída, encadeando rodadas de computação local com comunicação all-to-all. As duas barreiras práticas eram exatamente o compartilhamento de estado e a latência por rodada. Os RDDs resolvem a primeira; o Spark reduz a segunda a cerca de 100 ms por passo. Logo, um motor só pode cobrir batch, SQL, streaming e ML.

**Perna prática.** Bibliotecas construídas sobre a **mesma abstração** compõem sem ETL entre sistemas. Você lê com Spark SQL, treina com MLlib e aplica sobre um stream, no mesmo programa, sem serializar para disco no meio do caminho.

#### O que a unificação entregou de verdade

Não subestime isso, porque é o lado forte do argumento e você vai precisar dele para não parecer que está só criticando.

- **Uma API e um runtime** no lugar de Hive para SQL, Storm para streaming, Mahout para ML e Giraph para grafos, cada um com seu cluster, seu formato de dados e sua equipe. O custo operacional que isso eliminou foi enorme e é fácil de esquecer em 2026, porque ninguém mais opera aquele zoológico.
- **Otimizações que atravessam bibliotecas.** Catalyst (otimizador de queries) e Tungsten (gerenciamento de memória e code generation) valem simultaneamente para Spark SQL, DataFrames, Structured Streaming e a MLlib baseada em DataFrame. Um ganho no otimizador melhora quatro produtos.
- **Adoção medida, não só anunciada.** A pesquisa da Databricks de julho de 2015, com 1.400 respondentes, mostrou 88% usando dois ou mais componentes, 60% usando três ou mais e 27% usando quatro ou mais. A unificação foi de fato consumida.

#### Onde a tese trinca, dez anos depois

Também está no paper, em letra miúda, e os autores são honestos:

- **Grafos: eles admitem perder.** Uma nota de rodapé reconhece que outros desenhos superam o Spark em certas computações de grafo, aquelas com baixa razão entre computação e comunicação, como PageRank. O GraphX nunca foi competitivo com Pregel ou GraphLab no caso de uso canônico.
- **A tolerância a falhas custa caro o tempo todo.** O paper diz que o Spark "pode incorrer em custos extras sobre alguns sistemas especializados devido à tolerância a falhas": as tarefas de map gravam a saída de shuffle em arquivos locais. Você paga o seguro em todos os jobs e usa em poucos.
- **A latência de sincronização é o teto estrutural.** "A principal limitação é o aumento de latência devido à sincronização em cada passo de comunicação." O modelo bulk synchronous parallel é o limite do desenho.

E o placar real dos componentes hoje:

| Componente | Situação em 2026 |
|---|---|
| Spark SQL / DataFrames | Venceu. Na prática, **é** o Spark. |
| Structured Streaming | Sobreviveu, mas por reescrita completa, não por unificação. O Real-Time Mode do Spark 4.1 trouxe latência sub-segundo, encerrando o argumento "Spark é só micro-batch". |
| MLlib | API baseada em RDD em modo de manutenção desde o Spark 2.0. Sem primitivas nativas de GPU e sem deep learning distribuído. Ray ocupou esse espaço. |
| GraphX | Sem desenvolvimento relevante. GraphFrames é pacote de terceiros. Não há deprecação formal, o que por si só é estranho. |

A trinca mais séria não está em nenhuma dessas linhas: é o **Photon**. A Databricks reescreveu a camada de execução do Spark SQL em **C++, fora da JVM**, com processamento vetorizado colunar no lugar do whole-stage codegen. O Catalyst continua planejando; o Photon executa. O motivo está no [paper do SIGMOD 2022](https://people.eecs.berkeley.edu/~matei/papers/2022/sigmod_photon.pdf): as cargas viraram CPU-bound e a JVM impõe tetos estruturais, com garbage collection degradando em heaps acima de cerca de 64 GB e limites de tamanho para o JIT compilar e inlinar métodos gerados.

Vale a pergunta em voz alta na aula: **se o motor unificado era a tese, por que a empresa fundada pelos autores do paper substituiu o motor de execução por um runtime nativo proprietário?** A resposta honesta parece ser que a unificação sobreviveu na **API** e não no **motor**. O ecossistema aberto respondeu com Apache Gluten e Velox (Top-Level Project da ASF desde março de 2026) e DataFusion Comet, em Rust. A direção é clara: a API do Spark permanece, o runtime por baixo está sendo trocado.

---

### 4. O contra-argumento de 2026

Agora a parte que os livros de 2020 e 2021 não têm como cobrir, e que é o motivo pelo qual esta disciplina precisa ser cursada com senso crítico.

#### 4.1 A evidência empírica: "big data" quase nunca acontece

Não é opinião de vendedor. São três estudos independentes, separados por treze anos, chegando à mesma conclusão.

**Microsoft Research, 2013.** O paper com o melhor título da área, [*Nobody ever got fired for buying a cluster*](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/msrtr-2013-2.pdf), analisou **174 mil jobs** de um cluster de analytics de produção da Microsoft. Mediana de input: **menos de 14 GB**. Oitenta por cento dos jobs abaixo de 1 TB. Dados equivalentes do Yahoo apontavam mediana abaixo de 12,5 GB, e do Facebook, 90% dos jobs abaixo de 100 GB. Um único servidor scale-up bateu um cluster de oito nós em 9 de 11 jobs. Isso foi publicado **um ano depois** do paper de RDDs: a crítica ao scale-out é contemporânea ao Spark, não uma revisão da moda do DuckDB.

**Jordan Tigani, 2023.** [*Big Data is Dead*](https://motherduck.com/blog/big-data-is-dead/), escrito por um engenheiro fundador do BigQuery. Entre clientes que gastavam mais de mil dólares por ano, **90% das queries processavam menos de 100 MB**. O argumento estrutural dele é melhor que o número: **storage e compute crescem de forma assimétrica**. Um cliente foi de 100 TB para 30 PB e o compute mal se moveu, porque quase toda query toca dado recente. Um mês recente pode ser 5% do volume e 80% dos acessos.

**RedSet, VLDB 2024, lido pela MotherDuck.** Aqui é preciso separar duas coisas que costumam ser citadas como uma. O paper [*Why TPC is Not Enough: An Analysis of the Amazon Redshift Fleet*](https://www.vldb.org/pvldb/vol17/p3694-saxena.pdf), da **AWS**, analisa a frota do Redshift e **publica** o dataset anonimizado Redset, que ele descreve como cerca de **69 milhões de queries** de 200 clusters serverless e 200 provisionados ao longo de três meses de 2024. A tabela de bytes escaneados abaixo **não está no paper**: ela vem da análise que a MotherDuck fez do dataset publicado ([Redshift Files: the Hunt for Big Data](https://motherduck.com/blog/redshift-files-hunt-for-big-data/), que descreve o Redset como meio bilhão de queries sobre 32 milhões de tabelas). Ou seja, o dado é da AWS, a leitura é da MotherDuck:

| Bytes escaneados por query | Percentual das queries |
|---|---|
| menos de 1 GB | **75%** |
| 1 a 10 GB | 19% |
| 10 a 100 GB | 4,1% |
| 100 GB a 1 TB | 1,1% |
| 1 a 10 TB | 0,21% |
| mais de 10 TB | entre 0,03% e 0,10% |

A última linha é a única que não fecha: a tabela da MotherDuck traz **0,10%** e o texto do mesmo material cita **0,03%** em outro ponto. Use a ordem de grandeza, que é o que importa (queries acima de 10 TB são raridade estatística), e **não** cite "três em cada dez mil" como número fechado. Mesmo em organizações com tabelas na casa dos terabytes, a query mediana sobre uma tabela de 10 a 100 TB escaneia cerca de **0,045% dela**.

#### 4.2 O que "cabe numa máquina" cresceu duas ordens de magnitude

Em 2006 a instância padrão da AWS tinha 1 core e 2 GB de RAM. Hoje uma instância comum tem 64 cores e 256 GB, e a família **U7i** chega a **32 TiB de RAM e 1.920 vCPUs**. O custo de VM na nuvem escala de forma **linear**: uma máquina inteira custa oito vezes um oitavo dela, e não existe desconto por distribuir.

O software acompanhou. Em dezembro de 2025, um teste público mostrou o **DuckDB processando 1 TB de Parquet no S3 em uma máquina de 64 GB de RAM, em 19 minutos**, com spill para disco. O Polars, no mesmo teste, estourou memória em lazy mode, porque o motor do DuckDB é streaming por design e o do Polars historicamente não era.

Se o teto do single-node subiu duas ordens de magnitude desde 2010, o **limiar a partir do qual vale pagar complexidade distribuída** também deveria ter subido. Os livros de 2020 e 2021 não fizeram esse ajuste.

#### 4.3 O overhead do Spark em jobs pequenos

O Spark cobra pedágio antes de processar o primeiro byte: inicialização da JVM, alocação de executores, planejamento e escalonamento de tasks. São tipicamente **2 a 5 segundos** só de partida. Em uma query de 500 MB que rodaria em um segundo, isso é 100% de overhead ou mais.

No [benchmark TPC-DS de Miles Cole](https://milescole.dev/data-engineering/2024/12/12/Should-You-Ditch-Spark-DuckDB-Polars.html), para queries ad-hoc, DuckDB e Polars foram de **2 a 6 vezes mais rápidos** que Spark **com Photon ligado**. Em 10 GB com 2 a 4 vCores, custaram cerca de **metade** do preço.

E há o custo que nenhum benchmark mede direito: **tuning e carga cognitiva**. No [benchmark TPC-H do Coiled](https://docs.coiled.io/blog/tpch.html), os autores relatam ter gasto mais tempo ajustando o Spark do que todos os outros sistemas somados. Particionamento, memória de executor, skew, shuffle, serialização. O DuckDB, nas palavras deles, "exige configuração mínima". No contexto de uma operação enxuta isso é dinheiro real: cada hora de tuning de cluster é uma hora que não foi para qualidade de dados.

#### 4.4 Warehouses e o argumento de categoria

Se o workload é SQL analítico sobre tabelas, com muitos usuários simultâneos e SLA de segundos, o Spark está na **categoria errada**. Não por ser lento, mas porque é um **motor de jobs**, não um **serviço de queries**. BigQuery e Snowflake entregam concorrência, cache de resultados, governança e otimização adaptativa como produto pronto. O próprio Photon é a admissão disso pela Databricks: para competir em SQL, foi preciso construir um motor de warehouse por baixo do Spark.

#### 4.5 Os furos do argumento "small data"

Esta seção é a que separa crítica inteligente de cinismo. Se você levar só a seção 4.1 para a aula, alguém derruba em dois minutos. Leve isto junto:

1. **Bytes escaneados não são o working set.** O RedSet mede **scan**. Um join que lê 50 GB pode materializar shuffle de duas ou três vezes o input. A estatística não captura a pressão de memória do estágio mais largo, que é exatamente onde o single-node quebra.
2. **A evidência é de leitura; a força do Spark é escrita.** Os três estudos medem queries analíticas. O caso mais forte do Spark é ELT **write-heavy**: MERGE, compaction, backfill, reprocessamento de histórico. No benchmark de Cole, a 100 GB, o Spark foi **mais de 2x mais rápido** que o DuckDB em "ler Parquet e escrever Delta" e cerca de 2x mais rápido em MERGE. Os dois lados do debate não estão medindo a mesma coisa.
3. **Conflito de interesse, e ele é maior do que parece.** Tigani é CEO da MotherDuck, que vende DuckDB gerenciado. O que é da AWS é o **dataset** e o paper da frota do Redshift; a leitura small data dele, incluindo a tabela de bytes escaneados, é **da MotherDuck**. Não dá para dizer que a evidência mais forte sustenta a tese independentemente do vendedor: o dado é independente, a interpretação não. O que continua valendo é que os números brutos são auditáveis, porque o dataset é público.
4. **Você dimensiona pela cauda, não pela mediana.** Se 0,2% dos seus jobs precisam de cluster e você não tem cluster, esses jobs simplesmente não rodam. A pergunta certa não é "qual o tamanho mediano" e sim "qual o custo de manter dois caminhos de execução".
5. **Concorrência é um eixo separado de volume.** O DuckDB tem modelo de escritor único e execução single-node. Acima de umas 20 queries concorrentes, ele não é opção, mesmo que cada query toque 200 MB.
6. **O Spark não morre.** No benchmark de Cole, a 100 GB, o DuckDB deu OOM com 2 vCores e o Polars deu OOM com 2, 4 e 8 vCores. O Spark **não falhou em nenhuma configuração**. Linhagem, spill e retry são o seguro que você paga o ano inteiro e usa no dia do incidente.
7. **O Spark aproveita hardware adicional; single-node satura.** No mesmo benchmark, a 100 GB o DuckDB é o mais rápido com 4 vCores, mas o Spark vence com 8, 16 e 32. A 32 vCores o Spark ficou 4,5x mais rápido por 2x o custo, enquanto o DuckDB ficou 2,4x mais rápido por 3,5x o custo. A economia de escala é do Spark.

---

### 5. Decisão prática

O ponto de inflexão **não é volume**. É volume multiplicado por concorrência, largura do estágio mais pesado, perfil de escrita e SLA.

| Sinal | Fica em single-node (DuckDB, Polars, DataFusion) | Vai para Spark |
|---|---|---|
| Volume por job | abaixo de 100 GB (viável até cerca de 1 TB com spill) | acima de 500 GB a 1 TB de forma recorrente |
| Paralelismo útil | até 8 vCores | 8 a 16 vCores ou mais |
| Concorrência | menos de 5 a 10 sessões | mais de 20 queries simultâneas |
| Perfil da carga | leitura, ad-hoc, exploração, BI | ELT write-heavy, MERGE, backfill, compaction |
| Duração e falha | job de 5 minutos: recomeçar é barato | job de 6 horas: recomeçar é inaceitável |
| Estágio mais largo | maior shuffle cabe na RAM da máquina | shuffle excede a maior instância viável |
| Formato e governança | arquivos, Parquet, tabela única | lakehouse com Delta/Iceberg, catálogo, lineage |

Árvore de decisão em texto, para uso rápido:

```text
1. O maior estágio do job (não o input, o shuffle) cabe na RAM de uma máquina viável?
   NÃO  -> Spark.
   SIM  -> segue.

2. O job dura horas e recomeçar do zero é inaceitável?
   SIM  -> Spark (linhagem, spill e retry).
   NÃO  -> segue.

3. A carga é write-heavy (MERGE, compaction, backfill de histórico)
   sobre tabelas Delta/Iceberg grandes?
   SIM  -> Spark.
   NÃO  -> segue.

4. Preciso servir mais de ~20 consumidores concorrentes com SLA de segundos?
   SIM  -> warehouse (BigQuery/Snowflake), não Spark e não single-node.
   NÃO  -> segue.

5. Nenhuma das acima -> single-node (DuckDB/Polars).
   Usar Spark aqui é escolha organizacional, não técnica.
```

Regras de bolso defensáveis:

- **Abaixo de ~100 GB por job:** usar Spark é conveniência organizacional (o time já sabe, a plataforma já existe), não decisão técnica. Isso é legítimo, mas chame pelo nome.
- **Entre 100 GB e 1 TB:** zona cinzenta genuína. Decide-se por concorrência, perfil de escrita e tolerância a falha, **não** por volume.
- **Acima de 1 TB recorrente, ou mais de 8 a 16 vCores de paralelismo útil, ou jobs de horas:** o Spark passa a ser a escolha racional e a economia de escala inverte a favor dele.

A resposta provavelmente certa, e a que Miles Cole defende, é **híbrida**: Spark para ELT distribuído e cargas write-heavy; DuckDB ou Polars para exploração, query interativa e manutenção leve de tabelas. A pergunta "Spark ou DuckDB?" pode ser a pergunta errada. O custo do híbrido é manter dois caminhos de execução, e esse custo precisa entrar na conta explicitamente.

#### Três cenários para rodar contra a árvore

Vale fazer o exercício antes da aula, porque os três caem em lugares diferentes:

- **Camada bruta acumulando muitos arquivos pequenos.** É caso de **compaction**, que é write-heavy, e portanto território de Spark, mesmo com volume total modesto.
- **Extração de texto de arquivos binários** é carga **CPU-bound e embaraçosamente paralela**, sem shuffle. Aqui o Spark serve como escalonador de tarefas, não como motor de dados, e a comparação honesta é contra um pool de workers simples.
- **Checagens de Data Quality sobre recortes recentes** são exatamente o perfil que o RedSet descreve: leitura, volume pequeno, alta frequência. Candidatas naturais a DuckDB.

---

## Parte 3 - RDD, DataFrame, Dataset, Catalyst e Tungsten

Versão de referência deste documento: **Apache Spark 4.2.0**, lançado em 14/07/2026 ([release notes](https://spark.apache.org/releases/spark-release-4-2-0.html)). Os livros da bibliografia descrevem o Spark 3.0/3.1, então marco explicitamente o que mudou.

---

### 1. A linhagem RDD, DataFrame e Dataset

As três APIs não são alternativas de gosto pessoal. São três gerações de resposta à mesma pergunta: **quanta informação o motor tem sobre o que você quer fazer?** Cada salto entrega mais informação ao otimizador e cobra menos controle manual de você.

#### RDD (Spark 1.0, 2010)

Um *Resilient Distributed Dataset* é uma coleção de objetos opacos da JVM, particionada entre os nós do cluster ([RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)). Ele traz três ideias que sobrevivem até hoje no núcleo do Spark: tolerância a falha por **linhagem** (recomputar a partição perdida em vez de replicar), **avaliação preguiçosa** (transformações constroem o grafo, ações disparam a execução) e **particionamento explícito**.

O que você ganha é controle total. Você escreve `map`, `mapPartitions`, `reduceByKey` e o Spark executa exatamente aquilo, na ordem em que você escreveu.

O que você perde é tudo o mais. O Spark não faz ideia do que existe dentro da sua closure. Uma lambda `x => x.idade > 21` é uma caixa preta: o otimizador não pode empurrar esse filtro para o arquivo Parquet, não pode podar colunas, não pode reordenar nada. Sem schema não há Catalyst, e sem Catalyst não há Tungsten. Os dados ficam como objetos Java no heap, com todo o custo do modelo de objetos da JVM e toda a pressão de coleta de lixo.

#### DataFrame (Spark 1.3, 2015)

Um DataFrame é um Dataset organizado em colunas nomeadas, conceitualmente equivalente a uma tabela relacional ([SQL Programming Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)).

A mudança fundamental não é sintática, é **semântica**. Você para de dizer *como* computar e passa a declarar *o que* quer. Quando você escreve `df.filter(F.col("idade") > 21)`, isso não é uma função que roda. É uma **árvore de expressões** que o Catalyst pode inspecionar, reescrever, empurrar para a fonte de dados e compilar em bytecode.

O ganho é o motor inteiro: Catalyst, Tungsten, representação binária compacta e, o detalhe que mais importa para você, **performance idêntica entre Scala, Java, Python e R**, porque todos produzem exatamente o mesmo plano lógico.

A perda é a tipagem em tempo de compilação. `df.select("nmae")` não quebra na escrita: quebra em tempo de execução, na fase de análise do Catalyst.

#### Dataset (Spark 1.6, 2016)

O Dataset tenta unir os dois mundos: tipagem forte e lambdas de um lado, motor otimizado do outro. A peça que torna isso possível é o **Encoder**, um codec gerado em tempo de compilação que mapeia objetos JVM tipados (`case class Pessoa(nome: String, idade: Int)`) para o formato binário interno do Tungsten e de volta, sem passar por serialização Java ou Kryo genérica.

Em Scala, `DataFrame` **é literalmente** um apelido de tipo para `Dataset[Row]`.

O ganho é o compilador pegando erro de coluna e de tipo antes de o job existir. A perda é sutil e importante: no momento em que você usa uma lambda tipada, o Catalyst volta a enxergar uma caixa preta. Existe custo de desserializar o `UnsafeRow` em objeto JVM e re-serializar depois, visível no plano físico como nós `DeserializeToObject`, `MapElements` e `SerializeFromObject`. Dataset tipado é mais rápido que RDD e **mais lento** que DataFrame puro.

#### Resumo comparativo

| | RDD | DataFrame | Dataset[T] |
|---|---|---|---|
| Introduzido | 1.0 | 1.3 | 1.6 |
| Linguagens | Scala, Java, Python, R | Scala, Java, Python, R | **só Scala e Java** |
| Segurança de tipo | compilação (Scala) | execução (análise) | compilação |
| Catalyst otimiza | não | sim | parcial, quebra nas lambdas |
| Tungsten / UnsafeRow | não | sim | sim, com custo de conversão |
| Whole-stage codegen | não | sim | parcial |
| Memória em cache | alta (objetos JVM) | baixa (binário) | baixa |
| PySpark vs Scala | **muito pior** | **equivalente** | não se aplica |

---

### 2. A situação de quem escreve Python

A documentação oficial é direta:

> "Python does not have the support for the Dataset API. But due to Python's dynamic nature, many of the benefits of the Dataset API are already available (i.e. you can access the field of a row by name naturally `row.columnName`)."
> [SQL Programming Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html), texto que continua válido no 4.2.0.

Isso não é preguiça do projeto. São três restrições duras:

1. **Encoders exigem tipos estáticos em tempo de compilação.** O Encoder do Scala é derivado por macro a partir da `case class`, no build. Em Python não existe esse momento: não há compilação, os tipos aparecem em tempo de execução.
2. **Não existe objeto JVM tipado do lado do Python.** `Dataset[Pessoa]` promete que `Pessoa` vive no heap da JVM. Um objeto Python vive em outro processo. A promessa é incoerente por construção.
3. **Uma lambda Python jamais rodaria dentro do codegen da JVM.** O ganho central do Dataset, que é fundir lambdas tipadas ao pipeline compilado, é inatingível.

O que isso muda na sua prática:

- Você tem **duas** camadas, não três: RDD (evite) e DataFrame (use).
- A ausência do Dataset **não é perda de performance**, é perda de segurança de tipo em tempo de escrita. Você compensa com schema explícito (`StructType`), testes e tipagem estática das funções que constroem transformações, não dos dados.
- O erro que o compilador Scala pegaria aparece em PySpark como `AnalysisException: Column 'nmae' cannot be resolved`, na fase de análise, **antes** de qualquer tarefa ser distribuída. Falha em milissegundos, não depois de quarenta minutos de job. É pior que compilação e muito melhor que estourar no meio do processamento.

```python
from pyspark.sql import DataFrame, functions as F

# O contrato de tipo em PySpark vive nas FUNÇÕES, não nos dados.
def enriquecer_pedidos(df: DataFrame) -> DataFrame:
    """Assinatura tipada documenta a transformação. O schema dos dados
    é garantido separadamente, por StructType explícito na leitura."""
    return (df
            .withColumn("ano", F.year("data_pedido"))
            .withColumn("uf_destino", F.upper(F.col("uf_destino"))))
```

Ferramentas que reduzem a lacuna e são externas ao Spark: Pandera para contrato de schema, `chispa` para asserções em testes, e `great_expectations` para validação em pipeline. Nenhuma delas é Dataset, mas juntas cobrem o motivo pelo qual você quereria um.

---

### 3. Quando ainda descer para RDD

Resposta honesta: **quase nunca em código novo, e menos ainda em PySpark.** RDDs não foram depreciados e continuam sendo o substrato sobre o qual tudo roda, mas escrever contra a API de RDD hoje exige justificativa.

Casos que ainda sobrevivem em 2026:

1. **Spark como escalonador de tarefas genéricas.** É o caso mais legítimo que resta. Você tem N tarefas paralelas que não são "processar linhas": converter N arquivos binários, chamar N APIs, rodar N simulações. `sc.parallelize(lista, numSlices=N).mapPartitions(executa)` usa o Spark puramente como distribuidor de trabalho, sem schema para otimizar. Ressalva importante: em PySpark, `mapInPandas` sobre um DataFrame de parâmetros costuma dar o mesmo resultado com integração melhor (métricas na UI, AQE, tolerância a falha por tarefa).
2. **Particionador customizado.** `partitionBy(new MeuPartitioner)` não tem equivalente exato na API de DataFrame. `repartition` e `repartitionByRange` cobrem hash e faixa, não lógica arbitrária. Cada vez mais raro com AQE.
3. **Código legado e bibliotecas RDD-only.** `org.apache.spark.mllib` (em manutenção desde o 2.0) e **GraphX**, que não tem sucessor em DataFrame dentro do core.
4. **Formatos verdadeiramente não estruturados**, antes de existir schema. Ressalva forte: `spark.read.format("binaryFile")` e `.text()` cobrem a maioria dos casos e mantêm você dentro do DataFrame.
5. **Diagnóstico.** `rdd.toDebugString` para ver fronteiras de shuffle e `df.rdd.getNumPartitions()` para inspecionar particionamento continuam úteis mesmo para quem nunca escreve um RDD.

**O anti-caso que você precisa memorizar:** em PySpark, `df.rdd.map(f)` é o pior dos mundos. Você perde Catalyst e Tungsten **e** ainda paga serialização pickle de cada elemento para um processo Python. Um `df.rdd.map(...).toDF()` inocente pode ser de 10 a 50 vezes mais lento que a expressão equivalente em DataFrame.

```python
# RUIM: sai do motor, serializa linha a linha para o Python
n = df.rdd.map(lambda r: r.valor * 1.1).sum()

# BOM: expressão de coluna, roda dentro do codegen da JVM
n = df.select(F.sum(F.col("valor") * 1.1)).first()[0]
```

---

### 4. Catalyst em quatro fases

Catalyst é um framework de otimização construído sobre **árvores** (`TreeNode`) e **regras** (`Rule[LogicalPlan]`), usando casamento de padrões do Scala. As regras são aplicadas em lotes, e cada lote roda até a árvore parar de mudar (ponto fixo) ou até um número máximo de iterações. Referência primária: [Deep Dive into Spark SQL's Catalyst Optimizer](https://www.databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html).

#### Fase 1: análise

Entrada: plano lógico **não resolvido**, vindo do parser SQL ou da API de DataFrame.

O que acontece: `UnresolvedRelation("processos")` vira uma relação concreta após consulta ao catálogo; `UnresolvedAttribute("valor")` é ligado a uma coluna específica de uma relação específica, com um `exprId` único; funções são resolvidas; coerção de tipo é aplicada; `*` é expandido; agregações são validadas.

É aqui que nasce a `AnalysisException`. Coluna inexistente, tipo incompatível, função desconhecida: tudo estoura nesta fase, antes de qualquer execução.

Saída: plano lógico analisado, com nome, tipo e nulabilidade conhecidos em cada nó.

#### Fase 2: otimização lógica

Regras aplicadas ao plano analisado. As que você precisa reconhecer no plano:

**Predicate pushdown** (`PushDownPredicates`, `PushPredicateThroughJoin`). Move filtros o mais perto possível da fonte. Um `df.join(outro).filter(col("uf") == "PR")` vira o filtro aplicado *antes* do join e, se a fonte suportar, empurrado até o leitor. Em Parquet isso vira `PushedFilters` no plano físico, e o leitor usa as estatísticas de mínimo e máximo de cada row group para **pular blocos inteiros do disco**. Frequentemente é o maior ganho isolado de um plano.

**Column pruning** (`ColumnPruning`). Elimina colunas nunca usadas. Se você lê uma tabela de 200 colunas e usa 4, o Catalyst reescreve a leitura para pedir 4. Em formato colunar isso é redução literal de entrada e saída. Consequência prática: `SELECT *` seguido de um `select` de três colunas custa o mesmo que pedir as três diretamente.

**Constant folding** (`ConstantFolding`, `ConstantPropagation`). `col("x") + (2 * 3)` vira `col("x") + 6`. `WHERE 1 = 1` desaparece. `WHERE 1 = 0` colapsa a subárvore inteira para uma relação vazia. Com propagação, `WHERE a = 5 AND b = a + 1` vira `a = 5 AND b = 6`.

**Reordenação.** `CombineFilters` funde filtros adjacentes, `CollapseProject` funde projeções, `ReorderJoin` reordena joins internos por heurística. Existe reordenação por custo real (`CostBasedJoinReorder`), mas ela exige `spark.sql.cbo.enabled=true`, `spark.sql.cbo.joinReorder.enabled=true` e estatísticas coletadas por `ANALYZE TABLE`. **O CBO vem desligado por padrão** e, na prática, em 2026 o AQE cobre boa parte do que ele prometia, usando estatísticas reais em vez de estimadas.

**`InferFiltersFromConstraints`.** A regra mais subestimada. Se `a.id = b.id` e existe `a.id > 100`, ela deduz `b.id > 100` e empurra para o outro lado do join. O efeito em joins grandes é enorme.

Vale conhecer ainda `BooleanSimplification`, `NullPropagation`, `EliminateOuterJoin` (converte outer em inner quando um filtro posterior torna as linhas nulas irrelevantes) e `ConvertToLocalRelation`. O Spark 4.2 acrescentou fusão de subplanos com condições de filtro diferentes (SPARK-40193), reduzindo varreduras redundantes.

#### Fase 3: planejamento físico

Um operador lógico vira vários candidatos físicos. `Join` pode virar `BroadcastHashJoinExec`, `ShuffledHashJoinExec`, `SortMergeJoinExec` ou `BroadcastNestedLoopJoinExec`. `Aggregate` vira `HashAggregateExec`, `ObjectHashAggregateExec` ou `SortAggregateExec`.

O uso de modelo de custo aqui é deliberadamente limitado: na prática ele serve sobretudo para escolher broadcast join quando um lado é pequeno, controlado por `spark.sql.autoBroadcastJoinThreshold` (padrão 10 MB).

Depois vêm as regras de preparação. `EnsureRequirements` insere `Exchange` (shuffle) e `Sort` onde a distribuição exigida não está satisfeita, e **é aqui que os shuffles nascem**. `ReuseExchange` reaproveita shuffles idênticos. `CollapseCodegenStages` cria os `WholeStageCodegenExec`.

**O AQE se encaixa neste ponto**, e isso é o que os livros de 2020 não contam direito: `spark.sql.adaptive.enabled` é `true` por padrão **desde o Spark 3.2.0** ([Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)). O plano físico é fatiado em estágios; ao fim de cada shuffle o Spark tem estatísticas reais e re-otimiza o resto: coalescência de partições pequenas, conversão de sort-merge join em broadcast, e divisão de partições enviesadas. Isso muda como você lê o `explain()`.

#### Fase 4: geração de código

O Catalyst transforma expressões em código-fonte Java, compilado em tempo de execução pelo Janino, com cache das classes geradas. É a ponte para o Tungsten, detalhada na seção 6.

---

### 5. Como ler o `explain()` de verdade

Modos disponíveis ([EXPLAIN, SQL Reference](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html)):

| Chamada | Saída |
|---|---|
| `df.explain()` | só o plano físico |
| `df.explain(True)` | os quatro planos: parsed, analyzed, optimized, physical |
| `df.explain("formatted")` | esqueleto numerado mais detalhe por nó |
| `df.explain("cost")` | plano lógico com estatísticas por nó |
| `df.explain("codegen")` | o código Java gerado |

O truque de diagnóstico mais útil é comparar mentalmente o plano **analisado** com o **otimizado**. Se você escreveu um filtro e ele não subiu no plano otimizado, algo bloqueou o pushdown: quase sempre uma UDF Python, uma janela ou um cast inseguro.

#### Um plano anotado, linha a linha

```text
AdaptiveSparkPlan isFinalPlan=false
+- == Current Plan ==
   *(3) HashAggregate(keys=[uf#12], functions=[sum(valor#15)])
   +- Exchange hashpartitioning(uf#12, 200), ENSURE_REQUIREMENTS
      +- *(2) HashAggregate(keys=[uf#12], functions=[partial_sum(valor#15)])
         +- *(2) Project [uf#12, valor#15]
            +- *(2) BroadcastHashJoin [cliente_id#10], [id#20], Inner, BuildRight
               :- *(2) Filter (isnotnull(cliente_id#10) AND (ano#18 = 2026))
               :  +- *(2) ColumnarToRow
               :     +- FileScan parquet [cliente_id#10,valor#15,uf#12,ano#18]
                         Batched: true,
                         PartitionFilters: [isnotnull(ano#18), (ano#18 = 2026)],
                         PushedFilters: [IsNotNull(cliente_id)],
                         ReadSchema: struct<cliente_id:bigint,valor:double,uf:string>
               +- BroadcastExchange HashedRelationBroadcastMode(...)
                  +- *(1) Filter isnotnull(id#20)
                     +- FileScan parquet [id#20] ...
```

Leia **de baixo para cima**: a folha é a leitura, a raiz é o resultado.

- **`FileScan parquet`**: a folha. É onde o dado entra. Tudo o que você conseguir empurrar para cá é I/O que não acontece.
- **`PartitionFilters: [(ano#18 = 2026)]`**: o Spark vai ignorar diretórios inteiros. É a poda mais barata que existe, porque nem abre o arquivo.
- **`PushedFilters: [IsNotNull(cliente_id)]`**: filtro delegado ao leitor Parquet, que usa mínimo e máximo por row group para pular blocos. Se o seu filtro aparece **só** no nó `Filter` acima e não aqui, você está lendo dados que vai jogar fora.
- **`ReadSchema: struct<cliente_id,valor,uf>`**: prova do column pruning. Se sua tabela tem 200 colunas e aqui aparecem 200, o pruning falhou (acontece depois de `cache()` ou de materializar `select("*")`).
- **`ColumnarToRow`**: conversão do batch colunar do leitor Parquet para `UnsafeRow`. Normal, não é problema.
- **`Filter (isnotnull(...) AND (ano = 2026))`**: o filtro residual que o leitor não conseguiu garantir sozinho. Note que `ano = 2026` aparece nos dois lugares: o Spark reaplica por segurança.
- **`BroadcastExchange`** no ramo direito: o lado pequeno da junção sendo enviado inteiro para todos os executores.
- **`BroadcastHashJoin ... BuildRight`**: a junção boa. Sem shuffle do lado grande. Se aqui estivesse `SortMergeJoin`, seriam dois shuffles mais duas ordenações. Se estivesse `BroadcastNestedLoopJoin`, seria alarme: normalmente indica junção sem condição de igualdade.
- **`HashAggregate` com `partial_sum`**: agregação parcial no lado do map, antes do shuffle. Reduz drasticamente o volume que atravessa a rede. É o padrão desejável.
- **`Exchange hashpartitioning(uf#12, 200), ENSURE_REQUIREMENTS`**: o shuffle. Cada `Exchange` é fronteira de estágio, escrita em disco e tráfego de rede. **Contar `Exchange` é a forma mais rápida de estimar o custo de um plano.** `ENSURE_REQUIREMENTS` significa que o Spark inseriu por necessidade; `REPARTITION_BY_NUM` significaria que foi você quem pediu.
- **`HashAggregate` final com `sum`**: consolida os parciais.
- **`*(N)`**: whole-stage codegen, estágio N. Todos os nós marcados `*(2)` foram compilados em uma única função Java.
- **`AdaptiveSparkPlan isFinalPlan=false`**: você está vendo o plano **antes** de executar. Com AQE ligado, o plano real pode ser bem diferente.

Sobre esse último ponto, a consequência prática: para ver o plano de verdade, execute uma ação e **depois** chame `explain()`.

```python
df = pedidos.join(clientes, "cliente_id").groupBy("uf").sum("valor")

df.explain()          # isFinalPlan=false, plano especulativo
df.count()            # força a execução; AQE re-otimiza a cada estágio
df.explain()          # agora com == Initial Plan == e == Final Plan ==
```

Alternativa melhor ainda: a aba SQL/DataFrame da Spark UI, que mostra o plano final com métricas por nó (linhas de saída, bytes de shuffle, tempo, spill). Em plano com quarenta operadores, o `explain()` padrão vira um muro de texto; use `explain("formatted")`.

---

### 6. Tungsten: UnsafeRow, cache e whole-stage codegen

Se o Catalyst decide **o que** executar, o Tungsten decide **como** executar rápido. A tese do projeto: depois de anos otimizando disco e rede, o gargalo do Spark passou a ser **CPU e memória** ([Project Tungsten](https://www.databricks.com/blog/2015/04/28/project-tungsten-bringing-spark-closer-to-bare-metal.html)).

#### UnsafeRow

O modelo de objetos da JVM é caro: uma string de 4 caracteres consome mais de 48 bytes, entre cabeçalho de objeto, cabeçalho de array, codificação UTF-16 e alinhamento. E o coletor de lixo genérico não sabe nada sobre o ciclo de vida dos dados do Spark.

A solução é gerenciar memória explicitamente via `sun.misc.Unsafe`, operando direto sobre bytes. O formato é o `UnsafeRow`: uma linha serializada em um bloco contíguo, com um bitset de nulidade, uma região de valores de tamanho fixo (8 bytes por campo) e uma região de tamanho variável para strings e arrays.

As consequências são as que importam:

- **Zero desserialização para acessar um campo.** Ler o campo 3 é aritmética de ponteiro. Nenhum objeto Java é criado.
- **Comparação e hash direto sobre bytes.**
- **Pressão de GC quase eliminada**: milhões de linhas viram um punhado de arrays grandes, não milhões de objetos.

Há também a computação consciente de cache. O exemplo canônico é a ordenação: em vez de ordenar ponteiros para registros espalhados no heap, o Spark ordena pares de (prefixo de 8 bytes da chave, ponteiro). A maioria das comparações resolve olhando só o prefixo, que está contíguo e cabe na linha de cache. Isso deu 3 vezes de ganho e é o motivo de o `SortMergeJoin` moderno ser bem mais rápido do que a descrição do algoritmo sugere.

#### Whole-stage code generation

No Spark 1.x cada operador implementava `next()`, devolvendo uma tupla por vez (modelo Volcano). Elegante e caro: bilhões de chamadas virtuais, tuplas trafegando por memória em vez de registradores da CPU, e um grafo de chamadas que impede o compilador de fazer loop unrolling e SIMD. Um laço Java escrito à mão respondia a mesma consulta 10 vezes mais rápido que o Spark 1.6.

A resposta foi gerar, em tempo de execução, bytecode que **colapsa a consulta inteira em uma única função**. A regra `CollapseCodegenStages` percorre o plano físico, encontra subárvores que suportam codegen e as funde em um `WholeStageCodegenExec`. O resultado é um único laço `while` sobre as linhas, com filtros e projeções embutidos ([Whole-Stage Code Generation](https://books.japila.pl/spark-sql-internals/whole-stage-code-generation/)).

Os números da transição 1.6 para 2.0, em nanossegundos por linha ([Apache Spark as a Compiler](https://www.databricks.com/blog/2016/05/23/apache-spark-as-a-compiler-joining-a-billion-rows-per-second-on-a-laptop.html)): filtro de 15 para 1,1; soma sem agrupamento de 14 para 0,9; hash join de 115 para 4,0; decodificação Parquet de 120 para 13.

#### Como identificar codegen no plano

Operadores fundidos aparecem com **asterisco e o id do estágio de codegen**: `*(2) Project`, `*(2) Filter`. Todos os nós com o mesmo `*(N)` viraram uma única função Java. Nós **sem** asterisco são fronteiras: ali o dado precisa ser materializado.

Fronteiras típicas:

- `Exchange` e `BroadcastExchange`, barreiras naturais;
- `BatchEvalPython` e `ArrowEvalPython`, ou seja, **UDFs Python quebram o codegen**;
- operadores com `ObjectType`, das lambdas de Dataset tipado;
- planos com mais campos que `spark.sql.codegen.maxFields` (padrão **100**). Acima disso o Spark cai para execução interpretada. Esse é o sintoma real por trás de "tabelas muito largas ficam misteriosamente lentas".

`spark.sql.codegen.wholeStage` é `true` por padrão. Desligar é ferramenta de diagnóstico, nunca de produção. Quando codegen não é viável, o Spark recorre à **vetorização**: processar lotes em formato colunar, o que reduz despacho virtual sem exigir compilação. O leitor Parquet vetorizado é cerca de 9 vezes mais rápido na decodificação, e o Spark 4.2 trouxe mais uma rodada de otimizações nele (SPARK-55722).

---

### 7. A armadilha das UDFs Python

#### Por que a API de DataFrame empata com Scala

Quando você escreve isto:

```python
(df.filter(F.col("valor") > 100)
   .groupBy("uf_destino")
   .agg(F.sum("valor").alias("total")))
```

**nenhuma linha de dado passa pelo Python.** A API Python constrói uma árvore de expressões e a envia para a JVM, via Py4J no PySpark clássico ou como protobuf sobre gRPC no Spark Connect. A partir daí, o Catalyst otimiza o mesmo plano que o Scala geraria, o Tungsten gera bytecode, os executores JVM processam `UnsafeRow`, e só o resultado agregado volta. O interpretador Python fica ocioso.

O corolário é a regra de ouro de quem escreve PySpark: **a briga "Scala contra Python" é uma não-questão enquanto você ficar dentro das expressões de coluna.** Boa parte do ofício é nunca sair delas.

#### Por que a UDF Python quebra isso

Uma função Python não pode virar expressão Catalyst. O Spark então precisa inserir um operador `BatchEvalPython` no plano (fronteira de codegen), iniciar um processo worker Python em cada executor, serializar cada linha, mandar pelo socket, executar, serializar de volta e desserializar na JVM.

Os custos empilham: serialização por linha, invocação por linha, troca de contexto entre processos, memória do worker Python **fora** do gerenciador unificado do Spark (causa clássica de OOM e de container morto pelo YARN ou pelo Kubernetes) e, o pior, perda de otimização. O Catalyst enxerga uma caixa preta e **não empurra filtros através dela** nem poda colunas depois dela. Uma UDF no meio da cadeia pode invalidar o pushdown do pipeline inteiro.

#### Como Arrow e pandas UDFs mitigam

**Apache Arrow** é um formato colunar em memória com o **mesmo layout binário** na JVM e no Python. Isso muda a natureza do problema: a transferência deixa de ser "converter objetos Java em pickle e reconstruir objetos Python" e vira "copiar um buffer", praticamente sem cópia e sem conversão por linha ([Apache Arrow in PySpark](https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_pandas.html)).

**pandas UDFs** aproveitam isso: a função recebe uma `pandas.Series` com um lote de linhas em vez de um valor, e opera vetorizada com pandas ou NumPy, que por baixo são C. Elimina o custo por linha e o custo por chamada. O benchmark original relatou de 3 vezes (operações triviais) a mais de 100 vezes (estatística pesada) de ganho ([Introducing Vectorized UDFs](https://www.databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html)).

```python
import pandas as pd
from typing import Iterator
from pyspark.sql.functions import pandas_udf

# 1) Series -> Series: transformação escalar vetorizada
@pandas_udf("double")
def corrige_inflacao(valor: pd.Series, indice: pd.Series) -> pd.Series:
    return valor * indice  # NumPy por baixo, um lote inteiro por chamada

# 2) Iterator[Series] -> Iterator[Series]: estado caro de inicializar
@pandas_udf("string")
def classifica(lotes: Iterator[pd.Series]) -> Iterator[pd.Series]:
    modelo = carregar_modelo()      # UMA vez por partição, não por lote
    for s in lotes:
        yield pd.Series(modelo.predict(s.tolist()))

# 3) mapInPandas: opera sobre pd.DataFrame inteiro e pode MUDAR o número de linhas
def explode_itens(lotes: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
    for pdf in lotes:
        yield pdf.explode("itens")

df.mapInPandas(explode_itens, schema="pedido_id string, itens string")
```

**Mudança importante no Spark 4.2 (julho de 2026):** UDFs Python otimizadas com Arrow e o IPC do PySpark baseado em Arrow passaram a vir **habilitados por padrão** (SPARK-54555, [release 4.2.0](https://spark.apache.org/releases/spark-release-4-2-0.html)). Ou seja: uma `@udf` comum já usa o caminho colunar sem você reescrever nada, e no plano ela aparece como `ArrowEvalPython` em vez de `BatchEvalPython`. Todo material anterior a 2026 que diz "ative `spark.sql.execution.arrow.pyspark.enabled`" está desatualizado.

Isso **não** anula a hierarquia de preferência:

1. **Expressão nativa de coluna** (`pyspark.sql.functions`). Sempre a primeira opção: roda dentro do codegen, sem fronteira. Antes de escrever uma UDF, procure a função nativa. O Spark 4.x tem centenas, incluindo `try_*`, colações, `parse_json` com o tipo VARIANT, e UDFs em SQL.
2. **pandas UDF ou pandas Function API**, quando a lógica exige Python de verdade (modelo, biblioteca científica, parsing complexo).
3. **UDF Python escalar**, hoje bem menos ruim que em 3.x, mas ainda fronteira de codegen e caixa preta para o Catalyst.
4. **`df.rdd.map`**, não.

Três armadilhas de Arrow que mordem em produção:

- `spark.sql.execution.arrow.maxRecordsPerBatch` tem padrão de 10.000. Reduza em tabelas muito largas: lotes grandes causam picos de memória na JVM.
- **Esse limite não se aplica a `applyInPandas`**: o grouped map carrega **o grupo inteiro** em memória. Com chave enviesada, é OOM garantido. É o erro número um com grouped map.
- `toPandas()` e `toArrow()` coletam **tudo** para o driver.

Versões mínimas no 4.2: pandas 2.2.0 e PyArrow 18.0.0.

---

## Parte 4 - Montando o ambiente em 2026

### 0. Por que você não deve seguir os capítulos ao pé da letra

Os dois livros da aula 1 são bons, mas foram escritos contra o Spark 3.0/3.1. O capítulo 2 de "Beginning Apache Spark 3" (*Working with Apache Spark*) e o capítulo 2 de "Learning Spark" 2ª edição (*Downloading Apache Spark and Getting Started*) descrevem um mundo que não existe mais. Se você seguir as instruções deles literalmente, você monta um ambiente que **não roda o Spark atual**.

O que mudou, item por item:

| O que os capítulos dizem | O que vale em 24/07/2026 |
|---|---|
| Damji 2: "Java 8 ou superior", com `JAVA_HOME` setado. Luu 2: três versões em três páginas ("basta ter Java", Java 11 preferido para o shell Scala, Python 3.7.x para o `pyspark`) | Java 8 e 11 foram **removidos** no Spark 4.0. Hoje: **Java 17, 21 ou 25** |
| Nenhum declara versão de Scala como requisito: ela só aparece em captura de tela (2.12.10 nos banners, e uma nota do site já errada dizendo 2.11) e no `build.sbt` do Damji | Spark 4.x é pré-compilado só com **Scala 2.13**. O 2.12 foi removido no 4.0 |
| Luu 2 exige **Python 3.7.x ou superior** para o `pyspark`; Damji 2 não declara requisito e roda 3.7.3 nos banners | Mínimo é **Python 3.10**. O wheel declara `requires_python >= 3.10` |
| Luu 2 só ensina tarball. Damji 2 já traz `pip install pyspark` do PyPI, disponível desde o Spark 2.2, com os extras `[sql,ml,mllib]` | `pip install pyspark` entrega tudo, inclusive `spark-submit`. Tarball só quando você precisa dos scripts de cluster |
| Os dois recomendam a **Databricks Community Edition**: Luu 2 dedica um terço do capítulo a ela, Damji 2 traz um boxe | A **Community Edition morreu em 01/01/2026**. Existe a **Free Edition**, que é outro produto, com outra arquitetura |
| Arquitetura: driver + cluster manager + executors, ponto | Existe agora um **segundo modo de operação**: Spark Connect, cliente e servidor separados por gRPC |

Guarde esta ideia: os livros ensinam bem o **modelo mental** (RDD, DataFrame, driver, executor, lazy evaluation, DAG). Você deve ler os capítulos para isso. Mas as **instruções operacionais** deles estão vencidas, e é este documento que você segue ao teclado.

---

### 1. Versões e requisitos hoje

#### 1.1 Qual Spark usar

A release estável mais recente é o **Apache Spark 4.2.0**, de **14/07/2026** (mais de 1.700 tickets do Jira, 250+ contribuidores). Fonte: [Spark Release 4.2.0](https://spark.apache.org/releases/spark-release-4-2-0.html).

Um detalhe que confunde quem olha a página de downloads rápido: existem releases de manutenção **com data posterior** à 4.2.0.

| Release | Data | O que é |
|---|---|---|
| **4.2.0** | 14/07/2026 | **estável mais nova - use esta** |
| 4.1.3 | 15/07/2026 | patch do ramo 4.1 |
| 4.0.4 | 15/07/2026 | patch do ramo 4.0 |
| 3.5.9 | 16/07/2026 | patch do último ramo 3.x mantido |

Data mais recente não significa versão mais nova. Há três ramos 4.x ativos (4.0.x, 4.1.x, 4.2.x) mais o 3.5.x em manutenção estendida. Para estudar, vá de 4.2.0. Fonte: [Downloads](https://spark.apache.org/downloads.html) e [News](https://spark.apache.org/news/).

Ignore as releases de preview (4.2.0-preview1 a preview5, de janeiro a maio de 2026). Preview não é para uso real.

#### 1.2 Requisitos oficiais

A [documentação do Spark 4.2.0](https://spark.apache.org/docs/latest/) diz textualmente:

> "Spark runs on Java 17/21/25, Scala 2.13, Python 3.10+, and R 4.0+ (Deprecated). Java 25 prior to version 25.0.3 support is deprecated as of Spark 4.2.0."

Traduzindo para decisão:

| Componente | Suportado | Escolha recomendada |
|---|---|---|
| Java (JDK) | 17, 21, 25 | **Temurin 21 LTS**. Se for 25, exija `>= 25.0.3` |
| Scala | apenas 2.13 | irrelevante se você só usa PySpark, mas importa nos JARs |
| Python | 3.10 a 3.14 | **3.12** |
| R | 4.0+, **deprecado** | não use. O SparkR está em fim de vida |

#### 1.3 O que quebra com a versão errada

Vale decorar estes erros, porque você vai encontrá-los:

- **Java 8 ou 11**: `UnsupportedClassVersionError` já no start. É o erro de quem seguiu tutorial antigo.
- **`JAVA_HOME` vazio**: mensagem clara (`JAVA_HOME is not set`) ou, pior, o Spark pega outro JDK do `PATH` e você depura fantasma.
- **JAR com sufixo `_2.12`** em Spark 4: `NoSuchMethodError` ou `ClassNotFoundException` em **runtime**, não em build. Sempre confira o sufixo `_2.13` (`spark-sql-kafka-0-10_2.13`, `delta-spark_2.13`).
- **Python 3.9 ou anterior**: o `pip` nem resolve o pacote.
- **Python diferente no driver e no worker**: `Python in worker has different version ... than that in driver`. É o erro número um de quem mistura venv com Jupyter. Corrija apontando `PYSPARK_PYTHON` e `PYSPARK_DRIVER_PYTHON` para o mesmo interpretador.
- **`py4j` instalado na mão**: o PySpark 4.2.0 fixa a faixa dele. Não mexa.

---

### 2. Caminho recomendado: local, com uv

Este é o caminho principal. Roda no seu WSL2 sem Docker, sem cluster, sem nuvem.

#### 2.1 Passo 1 - JDK

```bash
# Ubuntu / WSL2
sudo apt update
sudo apt install -y wget apt-transport-https gpg
wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public \
  | sudo gpg --dearmor -o /etc/apt/keyrings/adoptium.gpg
echo "deb [signed-by=/etc/apt/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" \
  | sudo tee /etc/apt/sources.list.d/adoptium.list
sudo apt update
sudo apt install -y temurin-21-jdk
```

Fixe o `JAVA_HOME` no shell (`~/.bashrc`):

```bash
echo 'export JAVA_HOME=/usr/lib/jvm/temurin-21-jdk-amd64' >> ~/.bashrc
echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
java -version   # deve dizer 21.x
```

Se preferir não mexer em repositório apt, o [SDKMAN](https://sdkman.io/) resolve em duas linhas: `sdk install java 21.0.8-tem` e ele já ajusta o `JAVA_HOME`.

#### 2.2 Passo 2 - projeto com uv

O `uv` resolve dependências em segundos e, o mais importante aqui, **fixa a versão do Python do projeto**, o que elimina o descasamento driver/worker.

```bash
uv init spark-lab
cd spark-lab
uv python pin 3.12
uv add "pyspark[sql,connect]==4.2.0"
uv run python -c "import pyspark; print(pyspark.__version__)"
```

Extras disponíveis, para você escolher com consciência em vez de instalar tudo:

```bash
uv add "pyspark[sql]==4.2.0"               # pandas + pyarrow (quase sempre você quer)
uv add "pyspark[connect]==4.2.0"           # cliente Connect com a JVM junto
uv add "pyspark[ml]==4.2.0"                # MLlib na API DataFrame
uv add "pyspark[pandas_on_spark]==4.2.0"   # API pandas sobre Spark
uv add "pyspark[pipelines]==4.2.0"         # Spark Declarative Pipelines
```

Execução descartável, sem criar projeto nenhum:

```bash
uv run --with "pyspark[sql]==4.2.0" python meu_script.py
uvx --from "pyspark==4.2.0" pyspark        # abre o shell PySpark
```

Se você preferir o caminho clássico, é o mesmo resultado:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install "pyspark[sql,connect]==4.2.0"
```

#### 2.3 O que o wheel traz e o que não traz

O wheel do PySpark **embarca os JARs do Spark**, em `site-packages/pyspark/jars/`, e põe `spark-submit`, `spark-shell` e `pyspark` no `PATH` do ambiente. Não é a distribuição inteira: a própria página do PyPI avisa que o pacote serve para interagir com um cluster existente e **não traz as ferramentas para subir um cluster standalone**, ou seja, os scripts de `sbin/`. Para isso, tarball. Isso torna obsoleto o ritual de tarball do Luu 2. O Damji 2 já apontava para cá: ele documenta `pip install pyspark` desde o Spark 2.2. O que mudou é que o wheel deixou de ser atalho para quem só usa Python e virou o caminho principal. Vale dizer o que isto fecha: o Luu 2 abre prometendo três formas de trabalhar com Spark, uma delas a submissão por linha de comando, e `spark-submit` não aparece em nenhuma das 28 páginas do capítulo. Quem entrega é o Damji 2, na última seção.

O que ele **não** traz: a JVM (por isso o passo 1), o Hadoop completo e conectores externos (Kafka, Delta, JDBC). Conectores vêm por `--packages` ou `spark.jars.packages`, sempre com sufixo `_2.13`.

Variante de Hadoop, se precisar:

```bash
PYSPARK_HADOOP_VERSION=3 pip install pyspark -v        # padrão
PYSPARK_HADOOP_VERSION=without pip install pyspark     # sem Hadoop empacotado
```

#### 2.4 O que `local[*]` realmente significa

Aqui os livros não ajudam: `local[*]` aparece três vezes nos quatro capítulos, todas dentro de banner de subida de shell, e nunca no texto corrido. Nenhum dos dois explica o que o asterisco significa. Em modo local **não existe cluster manager, nem master, nem worker separado**. Driver e executor são o **mesmo processo JVM**, e o paralelismo vem de threads.

| URL de master | Significado |
|---|---|
| `local` | 1 thread, zero paralelismo. Ótimo para depurar |
| `local[4]` | 4 threads de worker |
| `local[*]` | uma thread por núcleo lógico da máquina |
| `local[*,2]` | como acima, com até 2 retentativas por task (testa caminho de falha) |

Sessão local bem configurada:

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("spark-lab")
         .master("local[*]")
         .config("spark.driver.memory", "4g")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
```

Dois ajustes que quase ninguém faz e que deveriam ser padrão em local:

- **`spark.sql.shuffle.partitions`**: o default é 200. Em `local[*]` isso cria 200 tarefas minúsculas e o overhead de agendamento domina o tempo. Ponha algo próximo do número de núcleos.
- **`spark.driver.memory`**: em modo local, o driver **é** o executor. `spark.executor.memory` é simplesmente **ignorado**. Se você aumentar a memória errada, nada acontece e você não entende por quê.

---

### 3. Docker como alternativa

Use Docker quando quiser reproduzir uma versão exata sem sujar a máquina, ou quando for simular um cluster standalone de verdade.

A imagem oficial é **`apache/spark`**, com tags 4.2.0 publicadas em 16-17/07/2026. O padrão de nome é `<versão>-scala<X>-java<Y>-python3[-r]-ubuntu`, com aliases curtos:

```
4.2.0
4.2.0-python3
4.2.0-java21-python3
4.2.0-scala2.13-java21-python3-r-ubuntu
```

Shell interativo:

```bash
docker run -it --rm apache/spark:4.2.0-python3 /opt/spark/bin/pyspark
```

Job com dados montados e a Spark UI exposta:

```bash
docker run -it --rm \
  -p 4040:4040 -p 15002:15002 \
  -v "$PWD/data:/data" \
  apache/spark:4.2.0-scala2.13-java21-python3-r-ubuntu \
  /opt/spark/bin/spark-submit --master "local[*]" /data/job.py
```

**Armadilha dos tutoriais antigos:** muita gente ainda manda usar `apache/spark-py`. Essa imagem está **abandonada** - o último push foi em 15/04/2023, na versão 3.4.0. Hoje o Python vem nas variantes `-python3` da imagem `apache/spark`. Vale o mesmo para `apache/spark-r`.

Imagens de terceiros úteis:

| Imagem | Serve para |
|---|---|
| `quay.io/jupyter/pyspark-notebook` | JupyterLab com PySpark pronto (o namespace no Docker Hub foi descontinuado, use o quay.io) |
| `bitnami/spark` | master e workers standalone via compose |

Compose mínimo para praticar um cluster de verdade, com processos separados, em vez de `local[*]`:

```yaml
services:
  spark:
    image: bitnami/spark:4
    environment: [SPARK_MODE=master]
    ports: ["8080:8080", "7077:7077"]
  spark-worker:
    image: bitnami/spark:4
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark:7077
      - SPARK_WORKER_CORES=2
      - SPARK_WORKER_MEMORY=2G
    deploy: {replicas: 2}
```

Vale rodar isso pelo menos uma vez. Ver dois workers se registrando na UI da porta 8080 fixa a arquitetura driver/master/executor muito melhor do que o diagrama do livro.

---

### 4. Spark Connect: o que é e por que muda o desenvolvimento local

#### 4.1 O mecanismo

O Spark Connect é um protocolo **gRPC + Protocol Buffers** que separa cliente de servidor. Ele entrou no Spark 3.4, ganhou cliente Scala no 3.5 e amadureceu no 4.x. Fonte: [Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html).

O fluxo é este:

1. O cliente (uma biblioteca fina) traduz suas operações de DataFrame em **planos lógicos não resolvidos**.
2. O plano vira protobuf e viaja por gRPC.
3. O servidor (o driver Spark) resolve, otimiza, executa e devolve o resultado em **lotes Apache Arrow**.

#### 4.2 Por que isso importa para você

- **Sem JVM no cliente.** Existe o pacote `pyspark-client` (cerca de 1,7 MB, contra centenas de MB do `pyspark` completo). Ele só fala com um servidor remoto. Imagem de CI enxuta, container de app sem Java.
- **Depuração na IDE.** O código do cliente é um processo Python comum: breakpoint no VS Code funciona, sem `spark-submit`.
- **Isolamento.** Seu app não derruba o driver, e o driver pode ser atualizado sem tocar no app.
- **O mesmo script roda local ou remoto** trocando só a URL.
- **É o que a Databricks já usa por baixo** em serverless e no Databricks Connect. Aprender Connect agora é aprender o modo de operação padrão da plataforma comercial.

#### 4.3 Subir e conectar

```bash
# descubra onde o pyspark instalou os scripts
export SPARK_HOME=$(uv run python -c 'import pyspark,os;print(os.path.dirname(pyspark.__file__))')
$SPARK_HOME/sbin/start-connect-server.sh    # escuta na porta 15002
```

```bash
uv add "pyspark-client==4.2.0"
```

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
spark.range(5).show()
```

Alternativas de ativação, todas equivalentes: variável `SPARK_REMOTE="sc://localhost"`, flag `--remote "sc://localhost"` no `pyspark`/`spark-shell`, ou a config `spark.api.mode=connect`. Para parar: `$SPARK_HOME/sbin/stop-connect-server.sh`.

#### 4.4 Duas correções ao que você vai ler por aí

**"O Connect ainda é experimental."** Não é. No 4.0.0 vieram o cliente Python leve, paridade de API no cliente Java e um tarball com Connect habilitado por padrão. No 4.1.0 o **Spark ML on Connect virou GA** no cliente Python. O 4.2.0 seguiu evoluindo: compatibilidade com API RDD, API `GetStatus` no servidor, liberação de sessão ao encerrar o processo, `head()/take()/tail()` otimizados e integração com o History Server. Ressalva honesta: os release notes **nunca rotulam o framework inteiro como GA**, só subcomponentes. Trate como pronto para os casos suportados.

**"No Spark 4 o Connect virou o default."** Também não. O default continua sendo o **Spark Classic**. No código, `spark.api.mode` é `"connect"` apenas quando a variável de ambiente `SPARK_CONNECT_MODE=1` está setada (é o que o tarball `spark-connect` faz); caso contrário é `"classic"`. Você precisa pedir o Connect explicitamente.

#### 4.5 O limite que morde

**RDD e `SparkContext` não existem no Connect.** Nada de `spark.sparkContext.parallelize(...)`, nada de `df._jdf` ou `spark._jsc`. A consequência para a disciplina é mais estreita do que parece: dos quatro capítulos, só o Luu 1 usa RDD, no word count em `sc.textFile(...)`, e é justamente ele que falha em um notebook serverless da Databricks, que é Connect por baixo. O Damji 2 já avisa que desde o 2.x os RDDs foram relegados a API de baixo nível, e o exemplo completo dele não usa nenhum. Estude RDD no Spark Classic local, e DataFrame em qualquer um dos dois.

Há também uma diferença semântica: o Connect **adia a análise** para o momento da execução. `spark.sql("select 1 as a").filter("c > 1")` não estoura na hora no Connect; o erro só aparece quando você chama `df.columns`, `df.schema` ou uma ação. Bom saber antes de escrever um `try/except` que nunca dispara.

---

### 5. Databricks gratuito em 2026

#### 5.1 A Community Edition acabou

Isto é o mais importante desta seção, porque um terço do Luu 2 depende disso: são 23 das 44 figuras do capítulo mostrando a interface da Databricks, mais o boxe do Damji 2 recomendando o mesmo. **A Community Edition foi aposentada em 1º de janeiro de 2026.** O anúncio oficial diz literalmente que "After that, Community Edition accounts will no longer be accessible", e um community manager confirmou em 09/01/2026 que já estava fora do ar. A ferramenta de migração de um clique também deixou de funcionar depois do prazo. Fonte: [PSA: Community Edition retires on January 1, 2026](https://community.databricks.com/t5/announcements/psa-community-edition-retires-on-january-1-2026-move-to-the-free/td-p/141888).

A substituta é a **Databricks Free Edition**, e ela é um produto diferente, não uma CE renomeada. A CE era um nó único, sem Unity Catalog e sem jobs: a captura do formulário de criação no Luu 2 mostra `1 Driver: 15.3 GB Memory, 2 Cores`, e o texto do capítulo fala em 15 GB. Brinquedo pelo paralelismo, dois cores, não pela memória. A Free Edition é a plataforma **em modo serverless, com Unity Catalog obrigatório e cotas**. Fonte: [Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition).

Atenção a uma inconsistência da própria Databricks: a página de docs diz "retired in 2025", enquanto o anúncio fixa 1º de janeiro de 2026. A data operacional correta é **01/01/2026**.

#### 5.2 O que a Free Edition tem

Verificado na [página oficial de limitações](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations), atualizada em 20/07/2026.

- **Unity Catalog: sim.** A doc declara em "Administrative limitations": *"One workspace and one metastore per account"*. Esse metastore é o metastore do UC. E como a Free Edition lista "All legacy Databricks features" entre os não suportados, o ambiente é **UC puro**: sem `hive_metastore` legado, sem navegador DBFS. Isso está alinhado com a diretriz geral da Databricks de que, a partir de 30/09/2026, todo workspace novo nasce UC-only.
- **Lineage do Unity Catalog: sim.** A doc de lineage não restringe por edição e a página de limitações não lista lineage entre os indisponíveis. Requisitos genéricos: tabelas registradas no metastore UC, consultas via DataFrame ou Databricks SQL, e ao menos privilégio `BROWSE` no catálogo pai. O lineage é capturado **em nível de coluna**, para todas as linguagens suportadas, e no Catalog Explorer é retido indefinidamente (tudo capturado após 01/09/2024). Nas system tables a janela é móvel de 1 ano. Fonte: [Lineage in Unity Catalog](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-lineage).
- **External locations e storage credentials: sim, com ressalva.** A página de limitações **não** lista external locations nem storage credentials como não suportados (o que ela lista é "Custom workspace storage locations", que é outra coisa: o storage raiz do workspace). Uma community manager da Databricks confirmou em 05/08/2025 que *"Databricks Free Edition does support external locations via Unity Catalog"*. Fonte: [tópico na Community](https://community.databricks.com/t5/data-governance/if-use-databricks-free-version-not-free-trail-can-use-external/m-p/127421). A ressalva é séria: por padrão o **acesso de saída à internet é restrito a um conjunto limitado de domínios confiáveis**, o que pode bloquear o alcance efetivo ao seu bucket.
- **Structured Streaming: sim, com trigger restrito.** Ver abaixo, porque é onde o projeto quebra.

#### 5.3 A pegadinha dos triggers de streaming

A Free Edition é serverless-only e herda todas as limitações do serverless. Em notebooks e jobs serverless:

- **Suportado:** `Trigger.AvailableNow()` (recomendado) e `Trigger.Once()` (deprecado, mas funciona).
- **Não suportado:** `Trigger.ProcessingTime(interval)` e `Trigger.Continuous(interval)`. A query falha com `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`.

O detalhe cruel: se você **não** declarar trigger nenhum, o Spark usa `ProcessingTime("0 seconds")` por padrão. Ou seja, **qualquer código de streaming escrito do jeito padrão falha na Free Edition**, incluindo o trecho de `writeStream` que o Damji 1 mostra. Você é obrigada a escrever `.trigger(availableNow=True)` explicitamente. Fonte: [Structured Streaming em serverless](https://docs.databricks.com/aws/en/compute/serverless/streaming).

Na prática: processamento incremental agendado, com latência de minutos, funciona. Stream contínuo de verdade em notebook ou job, não. Contínuo só via Lakeflow pipeline, e na Free Edition você tem "one active pipeline per pipeline type".

#### 5.4 Cotas e o que não existe

| Recurso | Limite |
|---|---|
| Compute | **só serverless**, tamanho e uso limitados, sem configuração customizada |
| SQL warehouses | 1, tamanho máximo `2X-Small` |
| Jobs | máximo de 5 tasks concorrentes por conta |
| Lakeflow pipelines | 1 pipeline ativo por tipo |
| Databricks Apps | até 3, e cada app para sozinho após 24h |
| Lakebase | 1 projeto, scale-to-zero |

Não suportado, lista literal: **R e Scala** (só Python e SQL), custom workspace storage locations, online tables, clean rooms, todos os recursos legados e o Knowledge Assistant. No lado administrativo: 1 workspace e 1 metastore, sem console de conta, sem APIs de nível de conta, sem SSO nem SCIM (login por OTP de e-mail, Google ou Microsoft).

Estourou a cota, o compute do workspace é desligado pelo resto do dia - em caso extremo, pelo resto do mês. **Os dados e configurações não são apagados.**

Duas notas práticas. Primeira: existe a **verificação por LinkedIn** na conta ("Verify with LinkedIn"). Ela desbloqueia **outbound internet access** e GPU serverless limitada, sem pagar nada. Se o seu projeto precisa alcançar um bucket externo, faça essa verificação **antes** de concluir que external location não funciona. A doc avisa que a verificação não remove todas as limitações. Segunda: **uso comercial é proibido** e não há SLA nem suporte. Uso acadêmico está ok. O marketing fala em "perpetually free"; a documentação só diz "a no-cost version". Trate a perpetuidade como promessa de marketing.

E o mais importante: **não existe upgrade da Free Edition para a plataforma completa.** A doc é direta: *"If you would like access to the full Databricks platform, you must create a new Databricks account by starting a free trial"*. São produtos distintos.

---

### 6. Plano B para Unity Catalog

O projeto da disciplina exige **Unity Catalog + external locations + lineage**. Antes de comprometer semanas, gaste 15 minutos testando. No workspace da Free Edition:

```sql
SHOW CATALOGS;
SHOW EXTERNAL LOCATIONS;
SHOW STORAGE CREDENTIALS;
```

Depois vá em Catalog, Connect, External Locations, Create external location, opção Manual. Se você não conseguir criar uma storage credential apontando para o seu bucket, você tem a resposta e parte para o plano B. Faça a verificação por LinkedIn antes deste teste.

#### Plano B1 - trial de 14 dias na sua própria nuvem (o mais indicado)

- **14 dias de DBUs gratuitas.** No Azure, crie o workspace com o tier "Trial (Premium - 14-Days Free DBUs)".
- Você paga só a **infraestrutura** (VMs, storage, rede). Um lab modesto custa poucos dólares.
- Exige assinatura **pay-as-you-go**. A "Free Trial Subscription" do Azure não serve. Você precisa de role `Contributor` ou `Owner` na assinatura.
- Em troca você ganha a **plataforma completa**: console de conta, UC completo, storage credentials e external locations reais apontando para o **seu** ADLS/S3/GCS, lineage completo, compute clássico, Scala, streaming sem restrição de trigger.

Fonte: [Azure Databricks free trial](https://learn.microsoft.com/en-us/azure/databricks/getting-started/free-trial).

**Estratégia:** desenvolva e teste tudo na Free Edition, que não tem prazo, e queime os 14 dias só na fase que exige external locations e lineage de verdade. Os 14 dias correm em **tempo de calendário**, não de uso. Não ative antes de estar pronta.

#### Plano B2 - Unity Catalog OSS

O projeto [unitycatalog/unitycatalog](https://github.com/unitycatalog/unitycatalog) é Apache 2.0 e hoje sandbox project da LF AI & Data Foundation. A release mais recente é a **v0.5.1, de 18/07/2026**.

```bash
git clone https://github.com/unitycatalog/unitycatalog && cd unitycatalog
bin/start-uc-server                 # API em localhost:8080
bin/uc table list --catalog unity --schema default
```

Ele entrega catálogos, schemas, tabelas (Delta, Iceberg, Parquet, JSON, CSV), volumes, funções, e desde a v0.4.0 **storage credentials e external locations** com credential vending temporário para S3. Fala a API do Hive Metastore e a REST Catalog do Iceberg, então pluga em Spark, Trino, DuckDB. Use no mínimo a **0.4.1**, que corrigiu a CVE-2026-27478 (validação de issuer de JWT).

O furo é decisivo para você: **não existe lineage no UC OSS.** Também não há system tables, auditoria nem UI comparável ao Catalog Explorer. Serve para estudar o modelo de governança; **não fecha o requisito do projeto**.

#### Comparativo

| Requisito | Free Edition | Trial 14 dias | UC OSS |
|---|---|---|---|
| Unity Catalog | sim (obrigatório) | sim, completo | sim, subconjunto |
| External locations para bucket próprio | provável, **teste antes** | sim | sim (S3) |
| Lineage | sim, valide na conta | sim | **não** |
| Streaming | só `AvailableNow`/`Once` | sem restrição | não se aplica |
| Scala | não | sim | sim |
| Prazo | indefinido | **14 dias corridos** | ilimitado |

---

### 7. Smoke test

Rode na ordem. Cada nível pega uma classe diferente de problema.

#### Nível 0 - pré-requisitos

```bash
java -version          # 17, 21 ou 25 (se 25, >= 25.0.3)
echo "$JAVA_HOME"      # não pode estar vazio
python --version       # >= 3.10
```

#### Nível 1 - PySpark coerente

```bash
uv run python -c "import sys, pyspark; print(sys.version.split()[0], pyspark.__version__)"
# esperado: 3.12.x 4.2.0
```

#### Nível 2 - sessão local sobe, computa e faz shuffle

```bash
uv run python - <<'PY'
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder
         .appName("smoke")
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())

print("spark version:", spark.version)

df = spark.range(1_000_000)
assert df.count() == 1_000_000
assert df.select(F.sum("id").alias("s")).first().s == 499999500000

# groupBy forca shuffle real: pega erro de diretorio temporario e de rede
g = df.groupBy((F.col("id") % 7).alias("k")).count()
assert g.count() == 7

spark.stop()
print("OK: local[*] funcional")
PY
```

O `groupBy` é o ponto importante. Um `count()` simples não força shuffle e passa mesmo com o ambiente meio quebrado.

#### Nível 3 - SQL, Arrow e ida e volta com pandas

```bash
uv run python - <<'PY'
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

spark.sql("SELECT 1 AS a, 'x' AS b").createOrReplaceTempView("t")
assert spark.sql("SELECT count(*) AS c FROM t").first().c == 1

pdf = spark.range(100).toPandas()      # exercita o pyarrow
assert len(pdf) == 100

import pyarrow, pandas
print("pyarrow:", pyarrow.__version__, "| pandas:", pandas.__version__)
spark.stop()
print("OK: SQL + Arrow")
PY
```

#### Nível 4 - escrita e leitura em disco

```bash
uv run python - <<'PY'
import tempfile, os
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
p = os.path.join(tempfile.mkdtemp(), "out.parquet")
spark.range(1000).write.mode("overwrite").parquet(p)
assert spark.read.parquet(p).count() == 1000
spark.stop()
print("OK: I/O parquet")
PY
```

#### Nível 5 - spark-submit e o exemplo canônico

```bash
uv run spark-submit --master "local[*]" \
  --class org.apache.spark.examples.SparkPi \
  "$(uv run python -c 'import pyspark,os;print(os.path.dirname(pyspark.__file__))')"/examples/jars/spark-examples_2.13-*.jar 10
```

Deve imprimir `Pi is roughly 3.14...`. Repare no `_2.13` no nome do JAR: é a prova visual de que o Scala 2.12 saiu de cena.

#### Nível 6 - Spark Connect

```bash
# terminal 1
$SPARK_HOME/sbin/start-connect-server.sh
```

```bash
# terminal 2
uv run python - <<'PY'
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
assert spark.range(10).count() == 10

is_connect = hasattr(spark, "client") or not hasattr(spark, "_jsc")
assert is_connect, "caiu em Spark Classic: a URL remote não pegou"
print("modo Connect confirmado")

# prova da análise adiada
df = spark.sql("SELECT 1 AS a").filter("coluna_inexistente > 1")
print("plano não resolvido criado sem erro")
try:
    df.columns          # aqui sim a análise vai ao servidor
    print("ERRO: deveria ter falhado")
except Exception as e:
    print("OK: análise adiada confirmada ->", type(e).__name__)
spark.stop()
PY
```

Este último teste é o mais didático de todos: ele prova que você está no Connect e te acostuma com a diferença de semântica antes que ela apareça em produção.

#### Nível 7 - Databricks Free Edition (rode num notebook)

```python
print(spark.version)
print("catalogo atual:", spark.sql("SELECT current_catalog()").first()[0])
display(spark.sql("SHOW CATALOGS"))

# serverless e sempre Spark Connect
print("Connect:", hasattr(spark, "client") or not hasattr(spark, "_jsc"))

# RDD deve FALHAR: comportamento esperado, não bug
try:
    spark.sparkContext.parallelize([1, 2, 3]).collect()
    print("inesperado: RDD funcionou")
except Exception as e:
    print("OK: RDD indisponivel (esperado) ->", type(e).__name__)
```

E o teste de trigger, que é onde o projeto pode quebrar:

```python
# ERRADO na Free Edition: sem trigger explicito o default e
# ProcessingTime("0 seconds") -> INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED

q = (spark.readStream.table("origem")
     .writeStream
     .trigger(availableNow=True)                 # obrigatorio em serverless
     .option("checkpointLocation", "/Volumes/cat/sch/vol/ckpt")
     .toTable("destino"))
q.awaitTermination()
```

---

### 8. Resumo operacional

1. **Local:** Temurin 21 + Python 3.12 + `uv add "pyspark[sql,connect]==4.2.0"`. Rode `local[*]` com `spark.sql.shuffle.partitions` baixo e `spark.driver.memory` ajustado (o `executor.memory` é ignorado).
2. **Docker:** `apache/spark:4.2.0-scala2.13-java21-python3-r-ubuntu`. Nunca `apache/spark-py`, morto desde 2023.
3. **Connect:** use desde já, mas saiba que o default continua Classic e que RDD e `SparkContext` não existem por lá.
4. **Databricks:** Community Edition morreu em 01/01/2026. Free Edition tem UC obrigatório e lineage, só Python e SQL, só serverless, e streaming só com `Trigger.AvailableNow()`.
5. **Projeto com UC + external locations + lineage:** teste a Free Edition em 15 minutos, com verificação LinkedIn feita. Se travar, use o trial de 14 dias em nuvem própria e reserve os dias para a fase final. UC OSS não serve, porque não tem lineage.

---

## Parte 5 - O que mudou desde os livros

Os dois livros da bibliografia são bons e continuam válidos no que ensinam sobre modelo de execução, RDD e DataFrame. Catalyst e Tungsten eles só nomeiam nos capítulos lidos, uma linha cada, e deixam a explicação para capítulos posteriores. O problema maior é a data. "Learning Spark" 2ª edição saiu em 2020 e cobre o Spark 3.0. "Beginning Apache Spark 3", de Hien Luu, saiu em 2021 e cobre até o 3.1. De lá para cá o Spark passou por quatro releases de feature na linha 3.x e três na linha 4.x. Algumas coisas que os livros mandam você configurar já vêm ligadas. Outras que eles descrevem como padrão foram removidas. E o recurso que mais afeta a performance de um job real em 2026, o AQE, aparece em um parágrafo do Luu 1 e em lugar nenhum do Damji.

Este documento cobre essa lacuna.

---

### 1. Linha do tempo: do 3.2 até hoje

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

### 2. Adaptive Query Execution: o recurso que a bibliografia mal menciona

Se você só puder guardar uma coisa deste documento, guarde esta seção. AQE cai na rubrica do Projeto da Disciplina, e é também o item de maior impacto prático em qualquer pipeline de porte real.

#### 2.1 O problema que ele resolve

O Catalyst planeja a query **antes** de executar, com base em estatísticas de catálogo: contagem de linhas, tamanho em bytes, histogramas quando existem. Em um data warehouse bem cuidado isso funciona. Em um data lake sobre Parquet em object storage, alimentado por ingestão contínua, com arquivos de tamanhos irregulares e sem `ANALYZE TABLE` rodando, a estimativa é frequentemente ruim. Um filtro seletivo demais, uma coluna com cardinalidade mal estimada, e o otimizador escolhe um sort-merge join onde cabia um broadcast, ou reparte 200 vezes um resultado que virou 3 MB.

AQE resolve isso mudando **quando** a decisão é tomada. O plano físico é quebrado em estágios delimitados por shuffle. Ao fim de cada estágio, o Spark tem estatísticas **reais** dos dados materializados: tamanho de cada partição de shuffle, contagem de registros. Com esse número em mãos, ele re-otimiza o restante do plano antes de continuar. Não é heurística nova, é o mesmo otimizador rodando com dados melhores.

#### 2.2 Quando virou padrão

`spark.sql.adaptive.enabled` passou a valer `true` por padrão no **Spark 3.2.0**. Os capítulos da bibliografia não ensinam a ligar nem a desligar: o Luu 1 descreve o AQE em um parágrafo, sem citar a config e sem dizer se vem ligado, e o Damji não menciona AQE em nenhum dos dois capítulos. O que você precisa saber hoje, e que a leitura oficial não dá, é **quando desligar** e como reconhecer no plano final o que o AQE fez.

#### 2.3 As três otimizações, com as configs

##### a) Coalescing de partições de shuffle

Junta partições pequenas contíguas depois do shuffle, para não gerar milhares de tarefas minúsculas cujo custo de agendamento supera o de processamento.

| Config | Default | Desde |
|---|---|---|
| `spark.sql.adaptive.coalescePartitions.enabled` | `true` | 3.0.0 |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | 64 MB | 3.0.0 |
| `spark.sql.adaptive.coalescePartitions.minPartitionSize` | 1 MB | 3.2.0 |
| `spark.sql.adaptive.coalescePartitions.parallelismFirst` | `true` | 3.2.0 |
| `spark.sql.adaptive.coalescePartitions.initialPartitionNum` | herda `spark.sql.shuffle.partitions` | 3.0.0 |

Isto **aposenta** o ritual de calibrar `spark.sql.shuffle.partitions` na mão, que era prática padrão na era 3.0. Os capítulos lidos nem chegam lá: a config aparece uma única vez, no builder de exemplo do Damji 1, com valor 6 e sem explicação, e o tuning é adiado para os capítulos 3 e 7. A prática nova é o inverso da antiga: configure `spark.sql.shuffle.partitions` **alto** (500, 1000, 2000) e deixe o AQE reduzir com base no tamanho real. O número vira um teto, não um alvo.

Uma armadilha operacional: `parallelismFirst = true` ignora o tamanho alvo de 64 MB e respeita apenas o mínimo de 1 MB, maximizando paralelismo. Isso é bom em cluster ocioso e ruim em cluster compartilhado, onde você quer usar recurso com parcimônia. A própria documentação recomenda `false` nesse cenário.

```python
spark.conf.set("spark.sql.shuffle.partitions", "1000")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
spark.conf.set("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false")
```

##### b) Conversão dinâmica de estratégia de join

Se, em tempo de execução, um lado do join se revelar pequeno o bastante, o sort-merge join vira **broadcast hash join**. Se não couber em broadcast mas couber em um hash map local, vira **shuffled hash join**. É a correção do erro de estimativa mais comum e mais caro.

| Config | Default | Desde |
|---|---|---|
| `spark.sql.adaptive.autoBroadcastJoinThreshold` | herda `spark.sql.autoBroadcastJoinThreshold` (10 MB); `-1` desliga | 3.2.0 |
| `spark.sql.adaptive.localShuffleReader.enabled` | `true` | 3.0.0 |
| `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold` | 0 | 3.2.0 |

Quando o AQE converte para broadcast, o `localShuffleReader` evita um shuffle desnecessário do lado grande, lendo as partições locais diretamente. É por isso que a conversão compensa mesmo depois do shuffle já ter sido escrito.

##### c) Tratamento de skew join

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

#### 2.4 Como enxergar o AQE trabalhando

Isso vale ouro na hora de defender o projeto. No Spark UI, o plano de uma query com AQE aparece marcado como `AdaptiveSparkPlan isFinalPlan=false` durante a execução e `isFinalPlan=true` no fim. Comparar os dois mostra literalmente a re-otimização acontecendo: um `SortMergeJoin` que virou `BroadcastHashJoin`, um `CustomShuffleReader coalesced` reduzindo 1000 partições para 14, um nó `skewed=true`.

```python
df = spark.sql("...")
df.explain(mode="formatted")   # plano antes
df.count()                     # dispara execução
# no Spark UI, aba SQL/DataFrame, o plano final substitui o inicial
```

Fonte: [Performance Tuning - Adaptive Query Execution](https://spark.apache.org/docs/latest/sql-performance-tuning.html).

---

### 3. Dynamic Partition Pruning

DPP entrou no Spark 3.0 e é **ligado por padrão** (`spark.sql.optimizer.dynamicPartitionPruning.enabled = true`). O Luu 1 dedica um parágrafo ao DPP, com o número do TPC-DS, e não diz sob que condições ele dispara; o Damji não o menciona. As condições de ativação são justamente o que separa "sei o nome" de "sei usar".

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

### 4. Spark 4.x, item por item

#### 4.0.0 (23/05/2025) - mais de 5.100 tickets, 390 contribuidores

**Modo ANSI SQL por padrão (SPARK-44444).** `spark.sql.ansi.enabled = true`. Divisão por zero, overflow numérico e cast inválido agora **falham** em vez de retornar `null` em silêncio. Esta é a mudança de comportamento mais impactante para código legado, e a mais fácil de defender em prova: silenciar erro aritmético em pipeline de dados é como engolir exceção em produção. Existe migration guide e dá para reverter pela config, mas reverter é dívida.

**Tipo VARIANT (SPARK-45827).** Tipo nativo para dados semiestruturados, tipicamente JSON, armazenado em formato binário aberto, com acesso a campos aninhados sem reparse a cada leitura. Resolve um incômodo antigo: payload de API e resposta de serviço HTTP normalmente aterrissam como JSON string, e sem VARIANT isso vira `get_json_object` custando parse por linha por acesso. No **4.1** o VARIANT virou GA com **shredding** (SPARK-54454), que materializa subcampos frequentes em colunas físicas. O tipo também foi adotado no **Iceberg v3**.

**Spark Connect maduro.** Arquitetura cliente-servidor via gRPC e Arrow, introduzida no 3.4. O 4.0 trouxe o cliente Python leve `pyspark-client` (cerca de 1,7 MB, contra centenas de MB do `pyspark` completo), paridade de API no cliente Java, clientes em Go, Swift e Rust, e a config `spark.api.mode` para alternar entre clássico e Connect sem reescrever a aplicação.

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

#### 4.1.0 (16/12/2025) - mais de 1.800 tickets

- **Spark Declarative Pipelines** (SPARK-51727): você declara datasets e queries, o Spark monta o grafo, ordena dependências, paraleliza, gerencia checkpoints e retries. É a versão open source do conceito de Delta Live Tables.
- **Real-Time Mode no Structured Streaming** (SPARK-53736): primeiro suporte oficial a latência sub-segundo contínua, com tarefas stateless chegando a milissegundos de um dígito. Isso encerra o argumento de folclore "Spark é micro-batch, logo não serve para tempo real". Os capítulos lidos, aliás, prometem o oposto sem qualificar: o Luu 1 fala em processamento em tempo real com exactly-once ponta a ponta, sem uma palavra sobre o que o micro-batch custava em latência.
- **SQL Scripting GA** (SPARK-54499), ligado por padrão.
- **VARIANT GA com shredding**, recursive CTE, sketches aproximados (KLL, Theta).
- **UDF e UDTF Arrow-nativas** com decorators (SPARK-52214, SPARK-52979): execução PyArrow sem o custo de converter para pandas no meio.
- **Spark ML on Connect GA** (SPARK-51236) e **driver JDBC para Connect** (SPARK-53484).
- **Shuffle com checksum e retry completo de estágio** (SPARK-51756), contra resultado incorreto por corrupção de shuffle.
- Python mínimo sobe para 3.10 (SPARK-52561).

#### 4.2.0 (14/07/2026) - mais de 1.700 tickets, 250 contribuidores

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

### 5. Photon, e por que ele não é aberto

**O que é.** Motor de execução vetorizado escrito em **C++ nativo**, que substitui o runtime de execução JVM do Spark SQL. O Catalyst continua planejando a query. Na execução, os operadores suportados rodam no runtime C++ sobre batches colunares Arrow, em vez do caminho de whole-stage codegen da JVM. As APIs do Spark continuam as mesmas, então o código existente roda sem alteração. Public preview em junho de 2021; em 2026 está ligado por padrão em compute serverless e SQL warehouses da Databricks, e disponível por checkbox em compute clássico.

**Por que ele existe.** O [paper do SIGMOD 2022](https://people.eecs.berkeley.edu/~matei/papers/2022/sigmod_photon.pdf) (Behm et al., prêmio de melhor paper industrial) é explícito: as cargas viraram **CPU-bound**, e a JVM impunha tetos estruturais. O garbage collector degrada com heaps acima de aproximadamente 64 GB. O JIT tem limites duros de codegen: métodos acima de cerca de 8.000 bytes de bytecode não são compilados, e acima de cerca de 325 bytes não são inlined. Queries complexas o bastante caíam de volta no interpretador estilo Volcano, justamente as que mais precisavam de velocidade. Ganho típico relatado: 2x a 5x em cargas analíticas, chegando a cerca de 10x em queries pesadas de scan e agregação.

**A relação com o Tungsten.** Photon é a continuação e superação do Tungsten, não algo paralelo. O Tungsten (Spark 1.6 a 2.0), que os livros cobrem bem, já atacava o mesmo problema: gerenciamento de memória off-heap, formato binário compacto e whole-stage code generation. Mas fazia isso **dentro da JVM** e, no essencial, processando linha a linha com código gerado. O Photon abandona as duas amarras: sai da JVM, com memória e alocação controladas em C++ e sem GC, e troca codegen por **vetorização interpretada** sobre batches colunares, o que dá SIMD, melhor uso de cache e adaptividade em runtime sem recompilar. Mesmo objetivo, gerações diferentes de resposta.

**Por que não é open source.** Photon é produto proprietário da Databricks e um dos principais diferenciais comerciais da plataforma. Não há repositório, não há release, não há plano anunciado de abertura. O paper descreve o design em detalhe, o código não é publicado. Vale registrar a tensão intelectual: a tese central do paper de 2016 sobre o Spark é o **motor unificado**, e a empresa fundada pelos autores do Spark substituiu o motor de execução por um runtime nativo fechado. A unificação sobreviveu na **API**; o motor único não sobreviveu.

**A resposta do ecossistema aberto** foram projetos com a mesma ideia e licença permissiva: **Apache Gluten + Velox** (Apache 2.0, Top-Level Project da ASF desde março de 2026, relatando 3x a 4x em TPC-H e TPC-DS) e **Apache DataFusion Comet** (Rust). O Native Execution Engine do Microsoft Fabric é um wrapper proprietário sobre o Gluten, o que sinaliza que a execução nativa aberta virou a base padrão fora da Databricks.

Fontes: [Photon docs](https://docs.databricks.com/aws/en/compute/photon), [anúncio do preview](https://www.databricks.com/blog/2021/06/17/announcing-photon-public-preview-the-next-generation-query-engine-on-the-databricks-lakehouse-platform.html).

---

### 6. O ecossistema lakehouse em 2026

O Luu 1 trata Delta Lake como a solução de semântica de consistência do ecossistema, ao lado de Koalas e MLflow, os três da Databricks; o Damji não o cita nestes capítulos. Em 2026 o cenário é de três formatos e de convergência entre eles.

- **Apache Iceberg** virou o **padrão de fato**. É lido e escrito por praticamente todo cloud e engine, inclusive pela Databricks, que comprou a Tabular e trouxe os criadores do Iceberg para dentro de casa. É nativo na AWS via S3 Tables. O **v3** fechou as últimas lacunas com deletion vectors, row lineage e o tipo VARIANT.
- **Delta Lake** segue como peso-pesado em base instalada e formato nativo da Databricks, e escolheu convergência em vez de disputa: o **UniForm** escreve Delta e expõe metadados Iceberg e Hudi ao mesmo tempo.
- **Apache Hudi** é o especialista em **mutação**. Para ingestão de streaming, CDC e upsert de alta frequência, seu índice e o modo Merge-on-Read continuam sendo as ferramentas mais bem construídas para o problema.

Onde o Spark se encaixa nisso: ele deixou de ser "o sistema" e virou o **motor de compute** de um lakehouse cujo armazenamento é definido por formatos abertos. Lê e escreve os três, e a decisão de formato deixou de ser permanente graças a UniForm e Apache XTable. Ao mesmo tempo, o Spark subiu de camada. Com Declarative Pipelines (4.1) e CDC nativo (4.2) ele passou a cobrir orquestração e ingestão incremental que antes exigiam ferramenta externa. Com Spark Connect virou serviço remoto acessível de qualquer linguagem. Com Real-Time Mode entrou em faixa de latência que era território do Flink.

Fontes: [Lakehouse Table Formats in 2026](https://amdatalakehouse.substack.com/p/lakehouse-table-formats-in-2026-iceberg), [comparativo Hudi vs Delta vs Iceberg](https://www.onehouse.ai/blog/apache-hudi-vs-delta-lake-vs-apache-iceberg-lakehouse-feature-comparison).

---

### 7. O que a literatura de 2020-2021 assumia, e o que vale hoje

Esta tabela compara o **estado do Spark na época dos dois livros** com julho de 2026. A coluna do meio descreve o consenso da literatura de 2020-2021, não citação dos capítulos 1 e 2 da bibliografia: boa parte destes assuntos (tuning de partição, skew, estado no streaming, mensagens de erro) só aparece em capítulos posteriores, ou em nenhum. Onde a atribuição é de um capítulo lido, ela está dita em linha.

| Tema | O estado da arte em 2020-2021 (Spark 3.0-3.1) | Em julho de 2026 |
|---|---|---|
| Versão de referência | Spark 3.0 / 3.1 | **Spark 4.2.0** (14/07/2026); 3.5.9 como LTS estendido até nov/2027 |
| AQE | Luu 1 descreve o mecanismo em um parágrafo, sem citar a config nem dizer se vem ligado; Damji não menciona | **Ligado por padrão desde o 3.2.0**; o que se aprende hoje é quando desligar e como ler o plano final |
| `spark.sql.shuffle.partitions` | Calibrar na mão era a prática; nos capítulos lidos a config aparece uma vez, com valor 6 e sem explicação | Configure **alto** e deixe o coalescing do AQE reduzir; o número virou teto, não alvo |
| Skew | Salting manual da chave era a receita corrente; a palavra não aparece em nenhum dos quatro capítulos | AQE divide partições skewed automaticamente (>256 MB **e** >5x a mediana); salting é fallback |
| Modo ANSI SQL | Opcional e desligado; overflow e cast inválido retornam `null` | **Padrão desde o 4.0** (SPARK-44444); operações inválidas **falham** |
| Java | "Roda em Java 8 e 11" | **Java 17, 21 e 25**; Java 8 e 11 removidos no 4.0 |
| Scala | "Scala 2.12 (e 2.13 experimental)" | **Só Scala 2.13** no 4.x (2.13.18 no 4.2.0); 2.12 descontinuado |
| Python | "Python 3.6 ou 3.8+" | Mínimo **3.10**; suporte declarado até 3.14 |
| Arquitetura do driver | Driver roda junto com a aplicação, monólito | **Spark Connect**: cliente-servidor via gRPC/Arrow, cliente leve de 1,7 MB, clientes em Go, Rust, Swift, JDBC. Não é o default e não tem RDD |
| Estado no streaming | `mapGroupsWithState` / `flatMapGroupsWithState`, estado opaco | **`transformWithState`** com múltiplas variáveis, TTL, timers; **State Data Source** lê o estado como tabela |
| Latência do streaming | Micro-batch, piso de ~100 ms | **Real-Time Mode** (4.1) entrega sub-segundo contínuo |
| DStreams | Luu 1 ainda apresenta o DStream como a abstração principal de streaming; Damji 1 já o dá por obsoleto. Os dois discordam | **Deprecada desde o 3.4** |
| JSON semiestruturado | `from_json` / `get_json_object` sobre string | Tipo **VARIANT** nativo, binário, GA com shredding no 4.1 |
| Conectores customizados | Só em Scala/Java sobre DSv2 | **Python Data Source API** (4.0), batch e streaming, em Python puro |
| Mensagens de erro | Stack trace de JVM | Error conditions com **SQLSTATE** e contexto da query |
| Formato de tabela | Delta Lake como escolha natural | Três formatos, com **Iceberg** como padrão de fato; convergência via UniForm e XTable |
| Execução | Tungsten + whole-stage codegen na JVM é o estado da arte | Motores nativos: **Photon** (fechado), **Gluten+Velox** e **Comet** (abertos) substituem o runtime JVM |
| Ambiente gratuito para estudar | Databricks Community Edition | **CE aposentada em 01/01/2026**; hoje é a **Free Edition**, serverless-only, só Python e SQL, sem cluster clássico |
| Papel do Spark | "O sistema unificado de big data" | **Motor de compute** de um lakehouse de formatos abertos, que subiu para orquestração (SDP) e ingestão (CDC nativo) |

---

## Fontes

### Arquitetura e modelo de execução

- [Cluster Mode Overview](https://spark.apache.org/docs/latest/cluster-overview.html)
- [Tuning Spark](https://spark.apache.org/docs/latest/tuning.html)
- [RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
- [Spark SQL Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [Web UI](https://spark.apache.org/docs/latest/web-ui.html)
- [Configuration](https://spark.apache.org/docs/latest/configuration.html)
- [Job Scheduling](https://spark.apache.org/docs/latest/job-scheduling.html)
- [Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html)
- [Spark Release 4.2.0](https://spark.apache.org/releases/spark-release-4-2-0.html)
- [Downloads e matriz de compatibilidade](https://spark.apache.org/downloads.html)
- [The Internals of Spark Core, DAGScheduler (Jacek Laskowski)](https://books.japila.pl/apache-spark-internals/scheduler/DAGScheduler/)
- [The Internals of Spark Core, RDD Checkpointing](https://books.japila.pl/apache-spark-internals/rdd/checkpointing/)
- [Container killed by YARN for exceeding memory limits (AWS re:Post)](https://repost.aws/knowledge-center/emr-spark-yarn-memory-limit)
- [Databricks Free Edition: limitações](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)

### Por que o Spark existe, e quando não usar

**Fundacionais**
- Zaharia et al., *Resilient Distributed Datasets*, NSDI 2012 (Best Paper) - [usenix.org](https://www.usenix.org/conference/nsdi12/technical-sessions/presentation/zaharia)
- Zaharia et al., *Apache Spark: A Unified Engine for Big Data Processing*, CACM 59(11), nov/2016 - [dl.acm.org](https://dl.acm.org/doi/10.1145/2934664)
- Behm et al., *Photon: A Fast Query Engine for Lakehouse Systems*, SIGMOD 2022 - [berkeley.edu](https://people.eecs.berkeley.edu/~matei/papers/2022/sigmod_photon.pdf)

**Evidência sobre tamanho real das cargas**
- Appuswamy et al., *Nobody ever got fired for buying a cluster*, MSR-TR-2013-2 - [microsoft.com](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/msrtr-2013-2.pdf)
- Saxena et al., *Why TPC is Not Enough: An Analysis of the Amazon Redshift Fleet*, PVLDB 17, 2024 - [vldb.org](https://www.vldb.org/pvldb/vol17/p3694-saxena.pdf)
- Tigani, *Big Data is Dead*, MotherDuck, fev/2023 - [motherduck.com](https://motherduck.com/blog/big-data-is-dead/)
- MotherDuck, *Redshift Files: the Hunt for Big Data* - [motherduck.com](https://motherduck.com/blog/redshift-files-hunt-for-big-data/). É daqui, e não do paper da VLDB, que sai a tabela de bytes escaneados por query

**Benchmarks**
- Miles Cole, *Should You Ditch Spark for DuckDB or Polars?*, dez/2024 - [milescole.dev](https://milescole.dev/data-engineering/2024/12/12/Should-You-Ditch-Spark-DuckDB-Polars.html)
- Coiled, *DataFrames at Scale Comparison: TPC-H* - [docs.coiled.io](https://docs.coiled.io/blog/tpch.html)
- *DuckDB beats Polars for 1TB of data*, dez/2025 - [confessionsofadataguy.com](https://www.confessionsofadataguy.com/duckdb-beats-polars-for-1tb-of-data/)

**Estado da arte em 2026**
- Spark 4.2.0 release notes, 14/07/2026 - [spark.apache.org](https://spark.apache.org/releases/spark-release-4-2-0.html)
- Spark 4.0.0 release notes, 23/05/2025 - [spark.apache.org](https://spark.apache.org/releases/spark-release-4-0-0.html)
- Photon na Databricks - [docs.databricks.com](https://docs.databricks.com/aws/en/compute/photon)
- AWS EC2 High Memory U7i - [aws.amazon.com](https://aws.amazon.com/ec2/instance-types/u7i/)
- Alex Merced, *Single-Node Data Engineering*, mai/2026 - [iceberglakehouse.com](https://iceberglakehouse.com/posts/2026-05-23-single-node-data-engineering-duckdb-datafusion-polars-lakesail/)

**Nota de confiabilidade.** Os números fundacionais (NSDI 2012, CACM 2016, MSR 2013, VLDB 2024, benchmarks de Cole e Coiled, documentação oficial do Spark e da AWS) vêm de fonte primária. Uma exceção conferida em 26/07/2026: a **tabela de bytes escaneados por query não está no paper da VLDB**, é análise da MotherDuck sobre o dataset que a AWS publicou, e o paper descreve o Redset como cerca de 69 milhões de queries enquanto a MotherDuck fala em meio bilhão. Dado da AWS, leitura da MotherDuck. Métricas de receita da Databricks e latências divulgadas pelo próprio fornecedor devem ser lidas como ordem de grandeza. Não há deprecação formal do GraphX: a afirmação segura é "sem desenvolvimento relevante".

### RDD, DataFrame, Dataset, Catalyst e Tungsten

- [Spark SQL, DataFrames and Datasets Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
- [EXPLAIN, Spark SQL Reference](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html)
- [Performance Tuning: AQE, broadcast, caching](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [Apache Arrow in PySpark](https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_pandas.html)
- [Spark Release 4.2.0, 14/07/2026](https://spark.apache.org/releases/spark-release-4-2-0.html)
- [Deep Dive into Spark SQL's Catalyst Optimizer](https://www.databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html)
- [Project Tungsten: Bringing Apache Spark Closer to Bare Metal](https://www.databricks.com/blog/2015/04/28/project-tungsten-bringing-spark-closer-to-bare-metal.html)
- [Apache Spark as a Compiler: Joining a Billion Rows per Second on a Laptop](https://www.databricks.com/blog/2016/05/23/apache-spark-as-a-compiler-joining-a-billion-rows-per-second-on-a-laptop.html)
- [Introducing Pandas UDF (Vectorized UDFs) for PySpark](https://www.databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html)
- [A Tale of Three Apache Spark APIs: RDDs, DataFrames, and Datasets](https://www.databricks.com/blog/2016/07/14/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets.html)
- [Whole-Stage Code Generation, The Internals of Spark SQL](https://books.japila.pl/spark-sql-internals/whole-stage-code-generation/)
- [Cost-Based Optimization, The Internals of Spark SQL](https://books.japila.pl/spark-sql-internals/cost-based-optimization/)

### Montando o ambiente em 2026

- [Apache Spark - Downloads](https://spark.apache.org/downloads.html)
- [Apache Spark - News](https://spark.apache.org/news/)
- [Spark Release 4.2.0](https://spark.apache.org/releases/spark-release-4-2-0.html)
- [Spark 4.2.0 - Overview e requisitos](https://spark.apache.org/docs/latest/)
- [PySpark Installation](https://spark.apache.org/docs/latest/api/python/getting_started/install.html)
- [Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html)
- [Application development with Spark Connect](https://spark.apache.org/docs/latest/app-dev-spark-connect.html)
- [PyPI - pyspark](https://pypi.org/project/pyspark/) e [pyspark-client](https://pypi.org/project/pyspark-client/)
- [Docker Hub - apache/spark](https://hub.docker.com/r/apache/spark)
- [PSA: Community Edition retires on January 1, 2026](https://community.databricks.com/t5/announcements/psa-community-edition-retires-on-january-1-2026-move-to-the-free/td-p/141888)
- [Databricks Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition)
- [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)
- [Structured Streaming em compute serverless](https://docs.databricks.com/aws/en/compute/serverless/streaming)
- [Lineage in Unity Catalog](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-lineage)
- [External locations na Free Edition (Databricks Community)](https://community.databricks.com/t5/data-governance/if-use-databricks-free-version-not-free-trail-can-use-external/m-p/127421)
- [Azure Databricks free trial](https://learn.microsoft.com/en-us/azure/databricks/getting-started/free-trial)
- [Unity Catalog OSS - releases](https://github.com/unitycatalog/unitycatalog/releases)
