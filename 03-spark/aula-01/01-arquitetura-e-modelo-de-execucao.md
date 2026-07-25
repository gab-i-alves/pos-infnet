---
title: "Arquitetura e modelo de execução do Spark"
aula: "Aula 01 - Processamento de Big Data com Apache Spark e Spark SQL"
data: 2026-07-24
tags:
  - spark
  - arquitetura
  - execucao
  - dag
  - shuffle
  - particoes
  - spark-ui
  - pos-infnet
---

# Arquitetura e modelo de execução do Spark

Versão de referência: **Apache Spark 4.2.0**, lançado em 14 de julho de 2026 ([release notes](https://spark.apache.org/releases/spark-release-4-2-0.html)). Os livros da bibliografia (Luu, 2021; Damji et al., 2020) descrevem o Spark 3.0. Onde o comportamento mudou, este documento marca a diferença.

---

## 1. O mapa mental: quem é quem e o que roda onde

Uma aplicação Spark é um sistema distribuído de três peças. Entender qual peça faz o quê resolve metade dos problemas que você vai enfrentar.

**Driver.** É o processo que roda o seu `main()` e hospeda a `SparkSession`. Ele não processa dados. Ele **planeja**: transforma o seu código em plano lógico, otimiza com o Catalyst, produz o plano físico, quebra em stages, monta as tasks e as distribui. Também recebe os resultados de volta. O driver precisa ser endereçável na rede, porque os executores abrem conexão **de volta** para ele (`spark.driver.host`, `spark.driver.port`). Expõe a Spark UI na porta 4040. Defaults que importam: `spark.driver.memory=1g`, `spark.driver.maxResultSize=1g`.

**Executores.** São JVMs lançadas nos nós de trabalho. Cada executor tem um número de cores (`spark.executor.cores`) e uma quantidade de memória (`spark.executor.memory`, default `1g`). Eles executam tasks em múltiplas threads e guardam dados em cache. Vivem pela duração inteira da aplicação, salvo alocação dinâmica. Os executores são o único lugar onde o dado real é tocado.

**Cluster manager.** É quem aloca recursos. Standalone, YARN e Kubernetes são os três em uso hoje (Mesos foi removido no Spark 4.0). O Spark é agnóstico: o mesmo código roda em qualquer um. O que muda é como os containers nascem e onde o driver vive.

A regra de isolamento é dura e vale a pena internalizar: **cada aplicação recebe seus próprios executores**. Dois jobs Spark não compartilham cache nem dados sem passar por storage externo. Isso simplifica o scheduling e o isolamento, ao custo de você não conseguir reaproveitar um cache entre aplicações.

### Client mode e cluster mode

A diferença é uma só: **onde o driver roda**.

| | client mode | cluster mode |
|---|---|---|
| Driver | no processo que fez o submit (seu notebook, um edge node) | dentro do cluster (container do AM no YARN, pod no K8s) |
| Matar o cliente | mata o job | não afeta o job |
| Latência driver/executores | atravessa a rede corporativa | dentro do cluster |
| Uso típico | shells interativos, notebooks | produção, jobs agendados |

Shell interativo **exige** client mode: o REPL precisa do driver local. Produção usa cluster mode. Se o seu job faz `collect()` de algo grande em client mode, os dados atravessam a VPN até a sua máquina. Referência: [Cluster Mode Overview](https://spark.apache.org/docs/latest/cluster-overview.html).

### SparkSession e SparkContext

O `SparkContext` é a conexão de baixo nível com o cluster. Ele instancia o `DAGScheduler`, o `TaskScheduler` e o `SchedulerBackend`. Só pode existir um ativo por JVM. O `SparkSession` (Spark 2.0 em diante) é o ponto de entrada unificado que absorveu `SQLContext` e `HiveContext`, dono do catálogo, do parser SQL e do Catalyst. Ele cria ou reusa um `SparkContext` internamente, acessível via `spark.sparkContext`.

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("estudo-arquitetura")
         .master("local[*]")
         .getOrCreate())

sc = spark.sparkContext  # só desça aqui para RDD, broadcast, checkpointDir, setLogLevel
```

**O que os livros não cobrem.** Desde o Spark 3.4 existe o **Spark Connect**, uma arquitetura cliente-servidor onde o cliente só envia o plano (protobuf sobre gRPC) e não hospeda mais um driver JVM local. No Spark 4.x ele amadureceu bastante: cliente Python leve (`pip install pyspark-client`, cerca de 1,5 MB contra centenas de MB do `pyspark` completo), paridade de API no cliente Java, Spark ML on Connect declarado GA no cliente Python no 4.1.0. Duas ressalvas importantes: **o modo padrão continua sendo o Spark Classic** (`spark.api.mode=classic`, a menos que `SPARK_CONNECT_MODE=1` esteja setado), e `SparkContext` e a API de RDD **não são acessíveis via Connect**. Isso significa que, com Connect, o seu código está restrito a DataFrame/SQL por construção. Referência: [Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html).

---

## 2. Do código ao task: o caminho de uma query

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

## 3. Narrow e wide: a distinção que governa performance

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

## 4. Partições: o conceito operacional do dia a dia

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

## 5. Memória do executor e a anatomia de um OOM

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

### O catálogo de OOM

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

## 6. Tolerância a falhas por linhagem

O Spark não replica dados intermediários. Ele guarda o **grafo de transformações** que produziu cada RDD. Se uma partição se perde, recomputa aquela partição a partir dos pais. A resiliência do "R" de RDD é isso, e não replicação.

- **Falha de task**: retenta até `spark.task.maxFailures=4`.
- **`FetchFailedException`**: o caso interessante. Uma reduce task não consegue buscar um bloco de shuffle porque o executor que o produziu morreu. O DAGScheduler marca as saídas daquele executor como perdidas e **ressubmete o `ShuffleMapStage` pai**, recomputando apenas as partições de mapa faltantes. É um restart parcial, não um restart do job.
- **Arquivos de shuffle** sobrevivem enquanto os RDDs correspondentes não forem coletados pelo GC. Isso é o que impede a recomputação de cascatear para o mundo inteiro.
- **External shuffle service** desacopla os arquivos de shuffle do ciclo de vida do executor: o executor pode morrer sem levar o shuffle junto. É pré-requisito clássico da alocação dinâmica. Alternativa moderna, padrão no Kubernetes: `spark.dynamicAllocation.shuffleTracking.enabled=true`.

### cache/persist e checkpoint não são a mesma coisa

| | `cache()` / `persist()` | `checkpoint()` |
|---|---|---|
| Onde grava | memória e disco do executor | storage confiável (HDFS, S3, GCS) via `sc.setCheckpointDir()` |
| Linhagem | **preservada** (é o plano B se o cache evaporar) | **truncada** |
| Custo | barato | escreve dados e **recomputa o RDD do zero** em um job separado |
| Se o executor morre | recupera recomputando | lê do arquivo |

O padrão canônico é `rdd.cache(); rdd.count(); rdd.checkpoint()`. Sem o cache, o RDD é computado duas vezes, porque o checkpoint dispara um job próprio.

Quando usar cada um: recomputação por linhagem é barata para linhagens curtas e narrow. Fica cara e perigosa em linhagens longas (algoritmos iterativos, loops de ML) ou muito wide, onde recomputar uma partição pode exigir refazer shuffles inteiros. Aí entra o checkpoint. O cache usa eviction LRU, e o storage level default é `MEMORY_ONLY`, que **silenciosamente recomputa** o que não coube. `MEMORY_AND_DISK` costuma ser a escolha mais previsível.

---

## 7. Como ler a Spark UI

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

## 8. O que os livros de 2020 e 2021 não contam

Anote estes pontos, porque eles mudam o que você lê nos capítulos:

- **Versão.** A estável é a 4.2.0 (14/07/2026). Existem três ramos 4.x ativos (4.0.x, 4.1.x, 4.2.x) e o 3.5.x ainda recebe patches (3.5.9 em 16/07/2026). Fonte: [downloads](https://spark.apache.org/downloads.html).
- **Runtime.** Spark 4.x roda em **Java 17, 21 ou 25**, **Scala 2.13** (o 2.12 foi removido no 4.0.0) e **Python 3.10 ou superior**. Qualquer tutorial mandando instalar Java 8 está velho. SparkR está marcado como deprecated.
- **AQE ligado por padrão** desde o 3.2. Boa parte do tuning manual de `spark.sql.shuffle.partitions` que os livros ensinam foi absorvida pelo runtime.
- **UDFs Python otimizadas com Arrow vêm habilitadas por padrão no 4.2.0** (SPARK-54555). No plano, uma `@udf` comum agora aparece como `ArrowEvalPython` em vez de `BatchEvalPython`. Continua sendo uma fronteira de codegen, mas o custo caiu bastante.
- **Databricks Community Edition não existe mais** desde 1º de janeiro de 2026. A oferta gratuita é a **Free Edition**, que é **serverless-only** (sem clusters clássicos configuráveis), só Python e SQL (sem Scala e sem R), com um workspace e um metastore Unity Catalog por conta. Para Structured Streaming, só `Trigger.AvailableNow()`; trigger contínuo falha com `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`. Se o professor sugerir Community Edition, esse é um bom ponto para levantar na aula. Fonte: [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations).

---

## 9. Checklist: o que você deve conseguir explicar depois de ler isto

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

### Perguntas para levar à aula ao vivo

1. Com AQE ligado por padrão desde o 3.2 e reotimização por estatísticas reais, ainda faz sentido investir em CBO e `ANALYZE TABLE`, ou o esforço migrou inteiro para layout de dados (particionamento físico, bucketing, tamanho de arquivo)?
2. Em um pipeline com muitos arquivos pequenos em object storage, qual é a ordem de ataque: compactação no storage, ajuste de `maxPartitionBytes`, ou `repartition` depois da leitura? E como medir qual dominou?
3. Com Spark Connect sem acesso a RDD, quais padrões operacionais reais deixam de ser possíveis, e vale a pena adotá-lo em produção hoje?
4. Qual é o critério prático para decidir entre confiar na recomputação por linhagem e pagar um checkpoint em storage confiável?

---

## Fontes

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
