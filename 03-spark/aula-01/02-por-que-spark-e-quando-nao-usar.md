---
title: "Por que o Spark existe, e quando não usar"
aula: "Aula 01 - Arquitetura unificada do Spark e preparação do ambiente"
data: 2026-07-24
tags:
  - spark
  - mapreduce
  - rdd
  - arquitetura
  - duckdb
  - polars
  - big-data-is-dead
  - decisao-arquitetural
  - pos-infnet
---

# Por que o Spark existe, e quando não usar

---

## 1. O problema que o MapReduce não resolvia

Os capítulos que o professor mandou ler dizem que o MapReduce era lento e que o Spark é rápido. Isso está certo e é inútil. O argumento técnico de verdade é mais específico, e é ele que você precisa levar para a aula.

O MapReduce, publicado pelo Google em 2004 e reimplementado como Hadoop a partir de 2006, resolveu um problema real: rodar computação tolerante a falhas sobre milhares de máquinas de commodity. O modelo é rígido de propósito. Você tem `map`, você tem `reduce`, e entre os dois há um shuffle. A tolerância a falhas vem de uma regra simples: **cada estágio materializa seu resultado no sistema de arquivos distribuído**. Se um nó cai, o próximo estágio relê do HDFS e a computação continua.

Essa regra é a origem do problema. No HDFS, materializar significa **serializar, escrever em disco e replicar** (tipicamente três cópias). Para um job de uma passada só, isso é aceitável. Para qualquer algoritmo que **reusa** o mesmo conjunto de dados, é ruinoso.

Pense em k-means com 20 iterações. Em cada iteração você lê o dataset, calcula distâncias, atualiza centroides e escreve o resultado. O dataset é o mesmo em todas as 20 rodadas, mas o MapReduce não tem como saber disso: ele paga 20 rodadas de escrita replicada, mais 20 leituras, mais 20 desserializações, para dados que ele mesmo acabou de produzir e vai consumir imediatamente. O mesmo vale para PageRank, para regressão logística, e para **mineração interativa**, onde um analista roda dezenas de queries ad-hoc sobre o mesmo recorte.

Some a isso duas limitações que agravam a primeira:

1. **Modelo de programação pobre.** Só existem `map` e `reduce`. Um join vira uma cadeia de jobs encadeados na mão. Uma agregação composta, idem. E cada elo dessa cadeia paga o custo de materialização descrito acima.
2. **Latência de partida.** Os próprios autores do Spark descrevem, no artigo da CACM de 2016, que as implementações de MapReduce foram desenhadas para batch "com latência de minutos a horas". Isso mata exploração interativa por construção.

O diagnóstico do grupo de Berkeley, então, não foi "MapReduce é lento". Foi: **falta uma abstração de compartilhamento de dados entre estágios**. Tudo o mais decorre disso.

---

## 2. O RDD e o paper de 2012

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

## 3. A tese do motor unificado: o que ela de fato entrega

Em novembro de 2016, o mesmo grupo publicou na Communications of the ACM o artigo [*Apache Spark: A Unified Engine for Big Data Processing*](https://dl.acm.org/doi/10.1145/2934664). É o texto que os livros do professor parafraseiam. O argumento tem duas pernas.

**Perna teórica.** MapReduce, somado a compartilhamento eficiente de dados entre rodadas, consegue emular qualquer computação distribuída, encadeando rodadas de computação local com comunicação all-to-all. As duas barreiras práticas eram exatamente o compartilhamento de estado e a latência por rodada. Os RDDs resolvem a primeira; o Spark reduz a segunda a cerca de 100 ms por passo. Logo, um motor só pode cobrir batch, SQL, streaming e ML.

**Perna prática.** Bibliotecas construídas sobre a **mesma abstração** compõem sem ETL entre sistemas. Você lê com Spark SQL, treina com MLlib e aplica sobre um stream, no mesmo programa, sem serializar para disco no meio do caminho.

### O que a unificação entregou de verdade

Não subestime isso, porque é o lado forte do argumento e você vai precisar dele para não parecer que está só criticando.

- **Uma API e um runtime** no lugar de Hive para SQL, Storm para streaming, Mahout para ML e Giraph para grafos, cada um com seu cluster, seu formato de dados e sua equipe. O custo operacional que isso eliminou foi enorme e é fácil de esquecer em 2026, porque ninguém mais opera aquele zoológico.
- **Otimizações que atravessam bibliotecas.** Catalyst (otimizador de queries) e Tungsten (gerenciamento de memória e code generation) valem simultaneamente para Spark SQL, DataFrames, Structured Streaming e a MLlib baseada em DataFrame. Um ganho no otimizador melhora quatro produtos.
- **Adoção medida, não só anunciada.** A pesquisa da Databricks de julho de 2015, com 1.400 respondentes, mostrou 88% usando dois ou mais componentes, 60% usando três ou mais e 27% usando quatro ou mais. A unificação foi de fato consumida.

### Onde a tese trinca, dez anos depois

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

## 4. O contra-argumento de 2026

Agora a parte que os livros de 2020 e 2021 não têm como cobrir, e que é o motivo pelo qual esta disciplina precisa ser cursada com senso crítico.

### 4.1 A evidência empírica: "big data" quase nunca acontece

Não é opinião de vendedor. São três estudos independentes, separados por treze anos, chegando à mesma conclusão.

**Microsoft Research, 2013.** O paper com o melhor título da área, [*Nobody ever got fired for buying a cluster*](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/msrtr-2013-2.pdf), analisou **174 mil jobs** de um cluster de analytics de produção da Microsoft. Mediana de input: **menos de 14 GB**. Oitenta por cento dos jobs abaixo de 1 TB. Dados equivalentes do Yahoo apontavam mediana abaixo de 12,5 GB, e do Facebook, 90% dos jobs abaixo de 100 GB. Um único servidor scale-up bateu um cluster de oito nós em 9 de 11 jobs. Isso foi publicado **um ano depois** do paper de RDDs: a crítica ao scale-out é contemporânea ao Spark, não uma revisão da moda do DuckDB.

**Jordan Tigani, 2023.** [*Big Data is Dead*](https://motherduck.com/blog/big-data-is-dead/), escrito por um engenheiro fundador do BigQuery. Entre clientes que gastavam mais de mil dólares por ano, **90% das queries processavam menos de 100 MB**. O argumento estrutural dele é melhor que o número: **storage e compute crescem de forma assimétrica**. Um cliente foi de 100 TB para 30 PB e o compute mal se moveu, porque quase toda query toca dado recente. Um mês recente pode ser 5% do volume e 80% dos acessos.

**RedSet / VLDB 2024.** O dado mais forte, e o mais interessante porque é da **AWS**, não de fornecedor de small data. [*Why TPC is Not Enough: An Analysis of the Amazon Redshift Fleet*](https://www.vldb.org/pvldb/vol17/p3694-saxena.pdf) analisou **meio bilhão de queries** reais em três meses:

| Bytes escaneados por query | Percentual das queries |
|---|---|
| menos de 1 GB | **75%** |
| 1 a 10 GB | 19% |
| 10 a 100 GB | 4,1% |
| 100 GB a 1 TB | 1,1% |
| 1 a 10 TB | 0,21% |
| mais de 10 TB | 0,03% |

Três em cada dez mil queries passam de 10 TB. Mesmo em organizações **com tabelas na casa dos terabytes**, a query mediana sobre uma tabela de 10 a 100 TB escaneia **0,045% dela**.

### 4.2 O que "cabe numa máquina" cresceu duas ordens de magnitude

Em 2006 a instância padrão da AWS tinha 1 core e 2 GB de RAM. Hoje uma instância comum tem 64 cores e 256 GB, e a família **U7i** chega a **32 TiB de RAM e 1.920 vCPUs**. O custo de VM na nuvem escala de forma **linear**: uma máquina inteira custa oito vezes um oitavo dela, e não existe desconto por distribuir.

O software acompanhou. Em dezembro de 2025, um teste público mostrou o **DuckDB processando 1 TB de Parquet no S3 em uma máquina de 64 GB de RAM, em 19 minutos**, com spill para disco. O Polars, no mesmo teste, estourou memória em lazy mode, porque o motor do DuckDB é streaming por design e o do Polars historicamente não era.

Se o teto do single-node subiu duas ordens de magnitude desde 2010, o **limiar a partir do qual vale pagar complexidade distribuída** também deveria ter subido. Os livros de 2020 e 2021 não fizeram esse ajuste.

### 4.3 O overhead do Spark em jobs pequenos

O Spark cobra pedágio antes de processar o primeiro byte: inicialização da JVM, alocação de executores, planejamento e escalonamento de tasks. São tipicamente **2 a 5 segundos** só de partida. Em uma query de 500 MB que rodaria em um segundo, isso é 100% de overhead ou mais.

No [benchmark TPC-DS de Miles Cole](https://milescole.dev/data-engineering/2024/12/12/Should-You-Ditch-Spark-DuckDB-Polars.html), para queries ad-hoc, DuckDB e Polars foram de **2 a 6 vezes mais rápidos** que Spark **com Photon ligado**. Em 10 GB com 2 a 4 vCores, custaram cerca de **metade** do preço.

E há o custo que nenhum benchmark mede direito: **tuning e carga cognitiva**. No [benchmark TPC-H do Coiled](https://docs.coiled.io/blog/tpch.html), os autores relatam ter gasto mais tempo ajustando o Spark do que todos os outros sistemas somados. Particionamento, memória de executor, skew, shuffle, serialização. O DuckDB, nas palavras deles, "exige configuração mínima". No contexto de uma operação enxuta isso é dinheiro real: cada hora de tuning de cluster é uma hora que não foi para qualidade de dados.

### 4.4 Warehouses e o argumento de categoria

Se o workload é SQL analítico sobre tabelas, com muitos usuários simultâneos e SLA de segundos, o Spark está na **categoria errada**. Não por ser lento, mas porque é um **motor de jobs**, não um **serviço de queries**. BigQuery e Snowflake entregam concorrência, cache de resultados, governança e otimização adaptativa como produto pronto. O próprio Photon é a admissão disso pela Databricks: para competir em SQL, foi preciso construir um motor de warehouse por baixo do Spark.

### 4.5 Os furos do argumento "small data"

Esta seção é a que separa crítica inteligente de cinismo. Se você levar só a seção 4.1 para a aula, alguém derruba em dois minutos. Leve isto junto:

1. **Bytes escaneados não são o working set.** O RedSet mede **scan**. Um join que lê 50 GB pode materializar shuffle de duas ou três vezes o input. A estatística não captura a pressão de memória do estágio mais largo, que é exatamente onde o single-node quebra.
2. **A evidência é de leitura; a força do Spark é escrita.** Os três estudos medem queries analíticas. O caso mais forte do Spark é ELT **write-heavy**: MERGE, compaction, backfill, reprocessamento de histórico. No benchmark de Cole, a 100 GB, o Spark foi **mais de 2x mais rápido** que o DuckDB em "ler Parquet e escrever Delta" e cerca de 2x mais rápido em MERGE. Os dois lados do debate não estão medindo a mesma coisa.
3. **Conflito de interesse.** Tigani é CEO da MotherDuck, que vende DuckDB gerenciado. O argumento dele é bom apesar disso, e a evidência mais forte (RedSet) vem da AWS, o que sustenta a tese independentemente do vendedor.
4. **Você dimensiona pela cauda, não pela mediana.** Se 0,2% dos seus jobs precisam de cluster e você não tem cluster, esses jobs simplesmente não rodam. A pergunta certa não é "qual o tamanho mediano" e sim "qual o custo de manter dois caminhos de execução".
5. **Concorrência é um eixo separado de volume.** O DuckDB tem modelo de escritor único e execução single-node. Acima de umas 20 queries concorrentes, ele não é opção, mesmo que cada query toque 200 MB.
6. **O Spark não morre.** No benchmark de Cole, a 100 GB, o DuckDB deu OOM com 2 vCores e o Polars deu OOM com 2, 4 e 8 vCores. O Spark **não falhou em nenhuma configuração**. Linhagem, spill e retry são o seguro que você paga o ano inteiro e usa no dia do incidente.
7. **O Spark aproveita hardware adicional; single-node satura.** No mesmo benchmark, a 100 GB o DuckDB é o mais rápido com 4 vCores, mas o Spark vence com 8, 16 e 32. A 32 vCores o Spark ficou 4,5x mais rápido por 2x o custo, enquanto o DuckDB ficou 2,4x mais rápido por 3,5x o custo. A economia de escala é do Spark.

---

## 5. Decisão prática

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

### Aplicando ao seu contexto

Vale fazer esse exercício antes da aula, com números seus:

- Os pipelines de scraping produzem muitos arquivos pequenos. Isso é caso de **compaction**, que é write-heavy, e portanto território de Spark, mesmo com volume total modesto.
- Extração de PDF é carga **CPU-bound e embaraçosamente paralela**, sem shuffle. Aqui o Spark serve como escalonador de tarefas, não como motor de dados, e a comparação honesta é contra um pool de workers simples.
- Checagens de Data Quality sobre recortes recentes são exatamente o perfil que o RedSet descreve: leitura, volume pequeno, alta frequência. Candidatas naturais a DuckDB.

---

## 6. Perguntas para levar à aula

Ancoradas em fonte, ordenadas da mais construtiva para a mais afiada.

1. O paper de RDDs vende até 100x sobre MapReduce, mas o recorde do GraySort em 2014 foi em SSD, não em memória. Que parte do ganho é a linhagem evitando replicação e que parte é apenas "estar na RAM"?
2. O estudo da Microsoft Research de 2013 já mostrava mediana de job abaixo de 14 GB, um ano depois do paper de RDDs. Por que a indústria adotou scale-out em massa com essa evidência disponível desde o início?
3. Se o motor unificado é a tese central do artigo de 2016, por que a Databricks reescreveu a execução do Spark SQL em C++ fora da JVM com o Photon? A unificação sobreviveu na API ou no motor?
4. A documentação do Spark 4.2.0 ainda precisa afirmar que a MLlib não está deprecada. Sem primitivas de GPU e com Ray dominando treino distribuído, a MLlib ainda faz parte da proposta de valor?
5. A evidência de "small data" vem de telemetria de **leitura**, e o caso mais forte do Spark é ELT **write-heavy**. Os dois lados desse debate estão medindo a mesma coisa?
6. O DuckDB processa 1 TB numa máquina de 64 GB em 19 minutos. A AWS vende instâncias de 32 TiB de RAM. Onde está o ponto de inflexão hoje, em números concretos?
7. Se o meu maior job cabe em 256 GB de RAM, qual é o argumento **técnico**, e não organizacional, para eu rodar Spark?

---

## Fontes

**Fundacionais**
- Zaharia et al., *Resilient Distributed Datasets*, NSDI 2012 (Best Paper) - [usenix.org](https://www.usenix.org/conference/nsdi12/technical-sessions/presentation/zaharia)
- Zaharia et al., *Apache Spark: A Unified Engine for Big Data Processing*, CACM 59(11), nov/2016 - [dl.acm.org](https://dl.acm.org/doi/10.1145/2934664)
- Behm et al., *Photon: A Fast Query Engine for Lakehouse Systems*, SIGMOD 2022 - [berkeley.edu](https://people.eecs.berkeley.edu/~matei/papers/2022/sigmod_photon.pdf)

**Evidência sobre tamanho real das cargas**
- Appuswamy et al., *Nobody ever got fired for buying a cluster*, MSR-TR-2013-2 - [microsoft.com](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/msrtr-2013-2.pdf)
- Saxena et al., *Why TPC is Not Enough: An Analysis of the Amazon Redshift Fleet*, PVLDB 17, 2024 - [vldb.org](https://www.vldb.org/pvldb/vol17/p3694-saxena.pdf)
- Tigani, *Big Data is Dead*, MotherDuck, fev/2023 - [motherduck.com](https://motherduck.com/blog/big-data-is-dead/)

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

**Nota de confiabilidade.** Os números fundacionais (NSDI 2012, CACM 2016, MSR 2013, VLDB 2024, benchmarks de Cole e Coiled, documentação oficial do Spark e da AWS) vêm de fonte primária. Métricas de receita da Databricks e latências divulgadas pelo próprio fornecedor devem ser lidas como ordem de grandeza. Não há deprecação formal do GraphX: a afirmação segura é "sem desenvolvimento relevante".
