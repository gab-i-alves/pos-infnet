---
title: "RDD, DataFrame, Dataset, Catalyst e Tungsten"
aula: "Aula 01 - Arquitetura unificada do Spark"
data: 2026-07-24
tags:
  - spark
  - pyspark
  - catalyst
  - tungsten
  - rdd
  - dataframe
  - dataset
  - explain
  - pandas-udf
  - arrow
  - pos-infnet
---

Versão de referência deste documento: **Apache Spark 4.2.0**, lançado em 14/07/2026 ([release notes](https://spark.apache.org/releases/spark-release-4-2-0.html)). Os livros da bibliografia descrevem o Spark 3.0/3.1, então marco explicitamente o que mudou.

---

## 1. A linhagem RDD, DataFrame e Dataset

As três APIs não são alternativas de gosto pessoal. São três gerações de resposta à mesma pergunta: **quanta informação o motor tem sobre o que você quer fazer?** Cada salto entrega mais informação ao otimizador e cobra menos controle manual de você.

### RDD (Spark 1.0, 2010)

Um *Resilient Distributed Dataset* é uma coleção de objetos opacos da JVM, particionada entre os nós do cluster ([RDD Programming Guide](https://spark.apache.org/docs/latest/rdd-programming-guide.html)). Ele traz três ideias que sobrevivem até hoje no núcleo do Spark: tolerância a falha por **linhagem** (recomputar a partição perdida em vez de replicar), **avaliação preguiçosa** (transformações constroem o grafo, ações disparam a execução) e **particionamento explícito**.

O que você ganha é controle total. Você escreve `map`, `mapPartitions`, `reduceByKey` e o Spark executa exatamente aquilo, na ordem em que você escreveu.

O que você perde é tudo o mais. O Spark não faz ideia do que existe dentro da sua closure. Uma lambda `x => x.idade > 21` é uma caixa preta: o otimizador não pode empurrar esse filtro para o arquivo Parquet, não pode podar colunas, não pode reordenar nada. Sem schema não há Catalyst, e sem Catalyst não há Tungsten. Os dados ficam como objetos Java no heap, com todo o custo do modelo de objetos da JVM e toda a pressão de coleta de lixo.

### DataFrame (Spark 1.3, 2015)

Um DataFrame é um Dataset organizado em colunas nomeadas, conceitualmente equivalente a uma tabela relacional ([SQL Programming Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)).

A mudança fundamental não é sintática, é **semântica**. Você para de dizer *como* computar e passa a declarar *o que* quer. Quando você escreve `df.filter(F.col("idade") > 21)`, isso não é uma função que roda. É uma **árvore de expressões** que o Catalyst pode inspecionar, reescrever, empurrar para a fonte de dados e compilar em bytecode.

O ganho é o motor inteiro: Catalyst, Tungsten, representação binária compacta e, o detalhe que mais importa para você, **performance idêntica entre Scala, Java, Python e R**, porque todos produzem exatamente o mesmo plano lógico.

A perda é a tipagem em tempo de compilação. `df.select("nmae")` não quebra na escrita: quebra em tempo de execução, na fase de análise do Catalyst.

### Dataset (Spark 1.6, 2016)

O Dataset tenta unir os dois mundos: tipagem forte e lambdas de um lado, motor otimizado do outro. A peça que torna isso possível é o **Encoder**, um codec gerado em tempo de compilação que mapeia objetos JVM tipados (`case class Pessoa(nome: String, idade: Int)`) para o formato binário interno do Tungsten e de volta, sem passar por serialização Java ou Kryo genérica.

Em Scala, `DataFrame` **é literalmente** um apelido de tipo para `Dataset[Row]`.

O ganho é o compilador pegando erro de coluna e de tipo antes de o job existir. A perda é sutil e importante: no momento em que você usa uma lambda tipada, o Catalyst volta a enxergar uma caixa preta. Existe custo de desserializar o `UnsafeRow` em objeto JVM e re-serializar depois, visível no plano físico como nós `DeserializeToObject`, `MapElements` e `SerializeFromObject`. Dataset tipado é mais rápido que RDD e **mais lento** que DataFrame puro.

### Resumo comparativo

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

## 2. A situação de quem escreve Python

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

## 3. Quando ainda descer para RDD

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

## 4. Catalyst em quatro fases

Catalyst é um framework de otimização construído sobre **árvores** (`TreeNode`) e **regras** (`Rule[LogicalPlan]`), usando casamento de padrões do Scala. As regras são aplicadas em lotes, e cada lote roda até a árvore parar de mudar (ponto fixo) ou até um número máximo de iterações. Referência primária: [Deep Dive into Spark SQL's Catalyst Optimizer](https://www.databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html).

### Fase 1: análise

Entrada: plano lógico **não resolvido**, vindo do parser SQL ou da API de DataFrame.

O que acontece: `UnresolvedRelation("processos")` vira uma relação concreta após consulta ao catálogo; `UnresolvedAttribute("valor")` é ligado a uma coluna específica de uma relação específica, com um `exprId` único; funções são resolvidas; coerção de tipo é aplicada; `*` é expandido; agregações são validadas.

É aqui que nasce a `AnalysisException`. Coluna inexistente, tipo incompatível, função desconhecida: tudo estoura nesta fase, antes de qualquer execução.

Saída: plano lógico analisado, com nome, tipo e nulabilidade conhecidos em cada nó.

### Fase 2: otimização lógica

Regras aplicadas ao plano analisado. As que você precisa reconhecer no plano:

**Predicate pushdown** (`PushDownPredicates`, `PushPredicateThroughJoin`). Move filtros o mais perto possível da fonte. Um `df.join(outro).filter(col("uf") == "PR")` vira o filtro aplicado *antes* do join e, se a fonte suportar, empurrado até o leitor. Em Parquet isso vira `PushedFilters` no plano físico, e o leitor usa as estatísticas de mínimo e máximo de cada row group para **pular blocos inteiros do disco**. Frequentemente é o maior ganho isolado de um plano.

**Column pruning** (`ColumnPruning`). Elimina colunas nunca usadas. Se você lê uma tabela de 200 colunas e usa 4, o Catalyst reescreve a leitura para pedir 4. Em formato colunar isso é redução literal de entrada e saída. Consequência prática: `SELECT *` seguido de um `select` de três colunas custa o mesmo que pedir as três diretamente.

**Constant folding** (`ConstantFolding`, `ConstantPropagation`). `col("x") + (2 * 3)` vira `col("x") + 6`. `WHERE 1 = 1` desaparece. `WHERE 1 = 0` colapsa a subárvore inteira para uma relação vazia. Com propagação, `WHERE a = 5 AND b = a + 1` vira `a = 5 AND b = 6`.

**Reordenação.** `CombineFilters` funde filtros adjacentes, `CollapseProject` funde projeções, `ReorderJoin` reordena joins internos por heurística. Existe reordenação por custo real (`CostBasedJoinReorder`), mas ela exige `spark.sql.cbo.enabled=true`, `spark.sql.cbo.joinReorder.enabled=true` e estatísticas coletadas por `ANALYZE TABLE`. **O CBO vem desligado por padrão** e, na prática, em 2026 o AQE cobre boa parte do que ele prometia, usando estatísticas reais em vez de estimadas.

**`InferFiltersFromConstraints`.** A regra mais subestimada. Se `a.id = b.id` e existe `a.id > 100`, ela deduz `b.id > 100` e empurra para o outro lado do join. O efeito em joins grandes é enorme.

Vale conhecer ainda `BooleanSimplification`, `NullPropagation`, `EliminateOuterJoin` (converte outer em inner quando um filtro posterior torna as linhas nulas irrelevantes) e `ConvertToLocalRelation`. O Spark 4.2 acrescentou fusão de subplanos com condições de filtro diferentes (SPARK-40193), reduzindo varreduras redundantes.

### Fase 3: planejamento físico

Um operador lógico vira vários candidatos físicos. `Join` pode virar `BroadcastHashJoinExec`, `ShuffledHashJoinExec`, `SortMergeJoinExec` ou `BroadcastNestedLoopJoinExec`. `Aggregate` vira `HashAggregateExec`, `ObjectHashAggregateExec` ou `SortAggregateExec`.

O uso de modelo de custo aqui é deliberadamente limitado: na prática ele serve sobretudo para escolher broadcast join quando um lado é pequeno, controlado por `spark.sql.autoBroadcastJoinThreshold` (padrão 10 MB).

Depois vêm as regras de preparação. `EnsureRequirements` insere `Exchange` (shuffle) e `Sort` onde a distribuição exigida não está satisfeita, e **é aqui que os shuffles nascem**. `ReuseExchange` reaproveita shuffles idênticos. `CollapseCodegenStages` cria os `WholeStageCodegenExec`.

**O AQE se encaixa neste ponto**, e isso é o que os livros de 2020 não contam direito: `spark.sql.adaptive.enabled` é `true` por padrão **desde o Spark 3.2.0** ([Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)). O plano físico é fatiado em estágios; ao fim de cada shuffle o Spark tem estatísticas reais e re-otimiza o resto: coalescência de partições pequenas, conversão de sort-merge join em broadcast, e divisão de partições enviesadas. Isso muda como você lê o `explain()`.

### Fase 4: geração de código

O Catalyst transforma expressões em código-fonte Java, compilado em tempo de execução pelo Janino, com cache das classes geradas. É a ponte para o Tungsten, detalhada na seção 6.

---

## 5. Como ler o `explain()` de verdade

Modos disponíveis ([EXPLAIN, SQL Reference](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html)):

| Chamada | Saída |
|---|---|
| `df.explain()` | só o plano físico |
| `df.explain(True)` | os quatro planos: parsed, analyzed, optimized, physical |
| `df.explain("formatted")` | esqueleto numerado mais detalhe por nó |
| `df.explain("cost")` | plano lógico com estatísticas por nó |
| `df.explain("codegen")` | o código Java gerado |

O truque de diagnóstico mais útil é comparar mentalmente o plano **analisado** com o **otimizado**. Se você escreveu um filtro e ele não subiu no plano otimizado, algo bloqueou o pushdown: quase sempre uma UDF Python, uma janela ou um cast inseguro.

### Um plano anotado, linha a linha

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

## 6. Tungsten: UnsafeRow, cache e whole-stage codegen

Se o Catalyst decide **o que** executar, o Tungsten decide **como** executar rápido. A tese do projeto: depois de anos otimizando disco e rede, o gargalo do Spark passou a ser **CPU e memória** ([Project Tungsten](https://www.databricks.com/blog/2015/04/28/project-tungsten-bringing-spark-closer-to-bare-metal.html)).

### UnsafeRow

O modelo de objetos da JVM é caro: uma string de 4 caracteres consome mais de 48 bytes, entre cabeçalho de objeto, cabeçalho de array, codificação UTF-16 e alinhamento. E o coletor de lixo genérico não sabe nada sobre o ciclo de vida dos dados do Spark.

A solução é gerenciar memória explicitamente via `sun.misc.Unsafe`, operando direto sobre bytes. O formato é o `UnsafeRow`: uma linha serializada em um bloco contíguo, com um bitset de nulidade, uma região de valores de tamanho fixo (8 bytes por campo) e uma região de tamanho variável para strings e arrays.

As consequências são as que importam:

- **Zero desserialização para acessar um campo.** Ler o campo 3 é aritmética de ponteiro. Nenhum objeto Java é criado.
- **Comparação e hash direto sobre bytes.**
- **Pressão de GC quase eliminada**: milhões de linhas viram um punhado de arrays grandes, não milhões de objetos.

Há também a computação consciente de cache. O exemplo canônico é a ordenação: em vez de ordenar ponteiros para registros espalhados no heap, o Spark ordena pares de (prefixo de 8 bytes da chave, ponteiro). A maioria das comparações resolve olhando só o prefixo, que está contíguo e cabe na linha de cache. Isso deu 3 vezes de ganho e é o motivo de o `SortMergeJoin` moderno ser bem mais rápido do que a descrição do algoritmo sugere.

### Whole-stage code generation

No Spark 1.x cada operador implementava `next()`, devolvendo uma tupla por vez (modelo Volcano). Elegante e caro: bilhões de chamadas virtuais, tuplas trafegando por memória em vez de registradores da CPU, e um grafo de chamadas que impede o compilador de fazer loop unrolling e SIMD. Um laço Java escrito à mão respondia a mesma consulta 10 vezes mais rápido que o Spark 1.6.

A resposta foi gerar, em tempo de execução, bytecode que **colapsa a consulta inteira em uma única função**. A regra `CollapseCodegenStages` percorre o plano físico, encontra subárvores que suportam codegen e as funde em um `WholeStageCodegenExec`. O resultado é um único laço `while` sobre as linhas, com filtros e projeções embutidos ([Whole-Stage Code Generation](https://books.japila.pl/spark-sql-internals/whole-stage-code-generation/)).

Os números da transição 1.6 para 2.0, em nanossegundos por linha ([Apache Spark as a Compiler](https://www.databricks.com/blog/2016/05/23/apache-spark-as-a-compiler-joining-a-billion-rows-per-second-on-a-laptop.html)): filtro de 15 para 1,1; soma sem agrupamento de 14 para 0,9; hash join de 115 para 4,0; decodificação Parquet de 120 para 13.

### Como identificar codegen no plano

Operadores fundidos aparecem com **asterisco e o id do estágio de codegen**: `*(2) Project`, `*(2) Filter`. Todos os nós com o mesmo `*(N)` viraram uma única função Java. Nós **sem** asterisco são fronteiras: ali o dado precisa ser materializado.

Fronteiras típicas:

- `Exchange` e `BroadcastExchange`, barreiras naturais;
- `BatchEvalPython` e `ArrowEvalPython`, ou seja, **UDFs Python quebram o codegen**;
- operadores com `ObjectType`, das lambdas de Dataset tipado;
- planos com mais campos que `spark.sql.codegen.maxFields` (padrão **100**). Acima disso o Spark cai para execução interpretada. Esse é o sintoma real por trás de "tabelas muito largas ficam misteriosamente lentas".

`spark.sql.codegen.wholeStage` é `true` por padrão. Desligar é ferramenta de diagnóstico, nunca de produção. Quando codegen não é viável, o Spark recorre à **vetorização**: processar lotes em formato colunar, o que reduz despacho virtual sem exigir compilação. O leitor Parquet vetorizado é cerca de 9 vezes mais rápido na decodificação, e o Spark 4.2 trouxe mais uma rodada de otimizações nele (SPARK-55722).

---

## 7. A armadilha das UDFs Python

### Por que a API de DataFrame empata com Scala

Quando você escreve isto:

```python
(df.filter(F.col("valor") > 100)
   .groupBy("uf_destino")
   .agg(F.sum("valor").alias("total")))
```

**nenhuma linha de dado passa pelo Python.** A API Python constrói uma árvore de expressões e a envia para a JVM, via Py4J no PySpark clássico ou como protobuf sobre gRPC no Spark Connect. A partir daí, o Catalyst otimiza o mesmo plano que o Scala geraria, o Tungsten gera bytecode, os executores JVM processam `UnsafeRow`, e só o resultado agregado volta. O interpretador Python fica ocioso.

O corolário é a regra de ouro de quem escreve PySpark: **a briga "Scala contra Python" é uma não-questão enquanto você ficar dentro das expressões de coluna.** Boa parte do ofício é nunca sair delas.

### Por que a UDF Python quebra isso

Uma função Python não pode virar expressão Catalyst. O Spark então precisa inserir um operador `BatchEvalPython` no plano (fronteira de codegen), iniciar um processo worker Python em cada executor, serializar cada linha, mandar pelo socket, executar, serializar de volta e desserializar na JVM.

Os custos empilham: serialização por linha, invocação por linha, troca de contexto entre processos, memória do worker Python **fora** do gerenciador unificado do Spark (causa clássica de OOM e de container morto pelo YARN ou pelo Kubernetes) e, o pior, perda de otimização. O Catalyst enxerga uma caixa preta e **não empurra filtros através dela** nem poda colunas depois dela. Uma UDF no meio da cadeia pode invalidar o pushdown do pipeline inteiro.

### Como Arrow e pandas UDFs mitigam

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

## 8. Perguntas para levar à aula ao vivo

1. Com AQE ligado por padrão desde o 3.2 e reordenação real de junções acontecendo em tempo de execução, o CBO baseado em `ANALYZE TABLE` ainda tem uso prático, ou virou peça de museu?
2. `spark.sql.codegen.maxFields` com padrão 100: em tabelas largas, qual a estratégia recomendada, aumentar o limite ou reestruturar o schema em structs aninhados?
3. Com UDFs Arrow ligadas por padrão no 4.2, qual a diferença de desempenho que ainda resta entre uma `@udf` comum e uma `@pandas_udf` escrita à mão?
4. Photon, Gluten e Comet substituem o runtime JVM por execução nativa vetorizada. Isso torna o whole-stage codegen do Tungsten uma tecnologia de transição?
5. O Spark Connect não expõe `SparkContext` nem RDD. Se a recomendação é Connect para aplicações novas, os casos legítimos de RDD deixam de existir na prática?

---

## Fontes

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
