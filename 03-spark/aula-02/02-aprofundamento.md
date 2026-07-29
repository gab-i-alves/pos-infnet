---
title: "Aula 02 de Spark - Aprofundamento"
aula: "Aula 02 - Transformação e persistência SQL"
data:
tags:
  - pos-infnet
  - aula-02
  - aprofundamento
  - spark
  - spark-sql
  - catalyst
  - aqe
  - schema
  - parquet
  - delta-lake
  - udf
---

# Aula 02 · Aprofundamento

Segunda etapa: o que foi estudado por conta própria, além da bibliografia. A primeira etapa, [01-pre-aula.md](01-pre-aula.md), registra o que as cinco leituras de fato dizem, com 110 dúvidas numeradas e doze divergências entre os três livros; a seção final dela, "O que fica para o aprofundamento", é a lista de compras que este documento paga. As perguntas que sobreviverem vão para [03-aula.md](03-aula.md), e o artefato que fecha o ciclo para [04-pos-aula.md](04-pos-aula.md).

**Versão de referência: Apache Spark 4.2.0**, lançado em 14 de julho de 2026. A bibliografia cobre o Spark 3.0 e 3.1 (Luu e Damji) e o ciclo 3.4 (Chadha). Onde o comportamento mudou, este documento marca a diferença e cita a fonte.

## O que este documento paga

A aula 01 fechou com cinco termos em aberto e uma dívida nomeada: o Catalyst, citado uma linha por capítulo em quatro capítulos. A aula 02 acrescentou cinco capítulos e pagou parte. O saldo depois deste aprofundamento:

| Dívida | Estado |
|---|---|
| **Catalyst, as quatro fases** | paga, e com uma surpresa: a caixa `Cost Model` que o livro desenha **não existe no código** |
| **Whole-stage codegen** | paga: o que colapsa, o que aparece no plano como `*(n)`, e as quatro condições que impedem |
| **`Exchange`** | paga: é o shuffle, e contar `Exchange` é literalmente o avaliador de custo default do AQE |
| **Tungsten** | paga em parte, o suficiente para a aula |
| **Linhagem** | paga: a recomputação é por partição, coisa que nenhum dos nove capítulos diz |
| **Ler um `explain()`** | paga campo por campo, e é o item mais prático do documento |

E abriu três buracos que a bibliografia inteira não cobre e que este documento fecha: **UDF** (zero ocorrências em 141 páginas), **leitura paralela de JDBC** (só o Luu ensina, e ensina serial) e **formato de tabela transacional** (silêncio de cinco capítulos).

## Sumário

| Parte | Do que trata |
|---|---|
| [1. O otimizador de verdade](#parte-1---o-otimizador-de-verdade-catalyst-aqe-e-como-ler-um-plano) | as quatro fases com o detalhe que falta; onde o custo age de fato; o AQE quebrando a cascata; `explain()` campo por campo; o que o AQE não resolve |
| [2. Schema, contrato e dado ruim](#parte-2---schema-contrato-e-dado-ruim) | varrer contra ler rodapé; as duas formas de declarar; `nullable` como promessa; o espectro de registro corrompido; o modo ANSI; VARIANT |
| [3. Formatos, layout e persistência](#parte-3---formatos-layout-e-persistência) | o layout real do Parquet; divisibilidade de codec; escrita e bucketing; descoberta de partição; catálogo contra metastore; formato transacional |
| [4. RDD, DataFrame, Dataset e a fronteira do Python](#parte-4---rdd-dataframe-dataset-e-a-fronteira-do-python) | quantos atributos o RDD tem; encoders; a fronteira JVM contra Python; UDF e Arrow; a Python Data Source API |
| [5. O que mudou desde os livros](#parte-5---o-que-mudou-desde-os-livros-e-o-que-a-bibliografia-não-cobre) | release por release de 3.2 a 4.2 no que toca esta aula, mais as lacunas da bibliografia |

Para **entender o motor**, leia 1 e 4. Para **escrever pipeline que não quebra**, leia 2 e 3. Para **saber o que do livro já não vale**, leia 5.

---
## Parte 1 - O otimizador de verdade: Catalyst, AQE e como ler um plano

Versão de referência: **Apache Spark 4.2.0**, lançado em 14/07/2026 ([release notes](https://spark.apache.org/releases/spark-release-4-2-0.html)). Toda config, default e versão citada aqui vem da documentação oficial do 4.2.0 ou do código-fonte da tag `v4.2.0`, com link. Onde a fonte é o código e não a documentação, eu digo, porque isso muda a confiança que você deve ter na afirmação.

Esta parte paga a dívida que cinco capítulos de bibliografia deixaram aberta. O placar registrado no [pré-aula](01-pre-aula.md): das cinco leituras, **uma** nomeia as quatro fases do Catalyst (Damji 3), e ela mesma se contradiz sobre onde o custo age, não nomeia um único operador físico, imprime uma saída de `explain()` inteira sem explicar nenhum campo dela, e descreve o otimizador como jornada estática em uma versão do Spark que já tinha AQE. O Luu nomeia o Catalyst duas vezes em 41 páginas e nunca usa `explain`. O Chadha não usa a palavra.

A regra desta parte: sempre que eu escrever "o livro diz", a frase está registrada no pré-aula. Tudo o mais é informação de fora dos capítulos lidos, e está marcada.

---

### 1. O que o Catalyst é, antes de qualquer fase

Antes das quatro fases existe uma máquina de três peças, e nenhum dos livros a descreve.

**Árvore.** Todo plano é uma árvore de nós imutáveis. Um `Filter` tem um filho, um `Join` tem dois, um `Relation` é folha. Expressões também são árvores: `a + (2 * 3)` é um nó `Add` com um filho `Attribute` e um filho `Multiply`. Isso importa porque otimizar deixa de ser "reescrever texto de consulta" e passa a ser "casar um padrão numa árvore e devolver outra árvore".

**Regra.** Uma regra é uma função de árvore para árvore. `ConstantFolding` casa um nó de expressão cujos filhos são todos literais e devolve o literal do resultado. Nada mais.

**Lote e ponto fixo.** As regras são agrupadas em lotes, e cada lote roda até a árvore parar de mudar. O texto original do projeto é explícito: "Catalyst groups rules into batches, and executes each batch until it reaches a fixed point, that is, until the tree stops changing after applying its rules", e justifica a escolha: "Running rules to fixed point means that each rule can be simple and self-contained, and yet still eventually have larger global effects on a tree" ([Deep Dive into Spark SQL's Catalyst Optimizer](https://www.databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html)). Existe teto: `spark.sql.optimizer.maxIterations`, default **100**, desde a 2.0.0, marcada como interna no código ([SQLConf.scala, v4.2.0](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala)).

Essa é a diferença entre entender o Catalyst e decorar quatro nomes. **Não existe um algoritmo grande que otimiza a consulta.** Existem dezenas de reescritas pequenas, cada uma burra isoladamente, rodando em loop até nada mais mudar. Quando um plano sai ruim, o diagnóstico útil não é "o otimizador falhou": é "qual regra deixou de casar, e por quê".

As quatro fases que o Damji nomeia são as mesmas da fonte original de 2015: "(1) analyzing a logical plan to resolve references, (2) logical plan optimization, (3) physical planning, and (4) code generation to compile parts of the query to Java bytecode" ([Databricks, 2015](https://www.databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html)). A cascata da Figura 3-4 do livro, de `Unresolved Logical Plan` até `RDDs` com uma caixa `Cost Model` no meio, é o mesmo desenho que esse texto de 2015 publica. Isso explica muito do que está errado no capítulo: o livro reproduz um desenho de 2015 e o descreve com um texto que não corresponde a ele. Volto a isso na seção 6.

---

### 2. Fase 1, análise: de não resolvido a resolvido

**Entrada:** plano lógico **não resolvido**. Ele existe do mesmo jeito venha a consulta de SQL (parser produz a árvore sintática) ou da API de DataFrame (cada chamada de método já constrói um nó). Esse é o fato material por trás da uniformidade que o Damji vende: SQL, DataFrame e Dataset entram no mesmo cano porque os três produzem `LogicalPlan`.

**O que "não resolvido" quer dizer, na prática.** No `explain(extended=True)` o bloco `== Parsed Logical Plan ==` traz nomes com apóstrofo à frente: `'Sort`, `'Total`, `unresolvedalias('sum('v), None)`. O apóstrofo é a marca de nó não resolvido. A saída oficial de exemplo mostra isso: `'Aggregate ['k], ['k, unresolvedalias('sum('v), None)]` e `'UnresolvedInlineTable [k, v], ...` ([EXPLAIN, SQL Reference](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html)). O livro imprime essa marca e não a comenta.

**O que a fase faz.** Resolve nome contra catálogo, e resolver quer dizer trocar um símbolo por uma referência concreta. O lote `Resolution` do analisador roda a ponto fixo e as regras têm nome ([Analyzer.scala, v4.2.0](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/Analyzer.scala)):

| Regra | O que troca por o quê |
|---|---|
| `ResolveCatalogs`, `ResolveRelations` | `UnresolvedRelation("pedidos")` vira a relação concreta, com schema, obtida no catálogo |
| `ResolveReferences` | `UnresolvedAttribute("valor")` vira um `AttributeReference` ligado a **uma** relação, com tipo, nulabilidade e um `exprId` único |
| `DeduplicateRelations` | resolve o auto-join, em que a mesma tabela aparece dos dois lados e as colunas precisam de identidade distinta |
| `ResolveFunctions` | `'sum(...)` vira a expressão `Sum` registrada |
| `ResolveAliases` | `unresolvedalias(...)` ganha o nome de saída |
| `ResolveOrdinalInOrderByAndGroupBy` | `GROUP BY 1` vira `GROUP BY` da primeira coluna projetada |
| `typeCoercionRules()` | insere `Cast` onde os tipos não casam. É por isso que `sum(v)` aparece como `sum(cast(v#48 as bigint))` no plano analisado do exemplo oficial |

O `exprId` é a peça que ninguém explica e que você vai ver em toda linha de plano: o `#12` de `uf#12`. Não é número de coluna nem posição. É identidade única de atributo dentro da consulta, e é o que permite ao otimizador mover um filtro por cima de dez operadores sem perder de vista de qual relação aquela coluna veio.

**Saída:** plano lógico analisado. No `explain` ele vem com uma linha de tipos no topo, antes da árvore: `k: int, sum(v): bigint` ([EXPLAIN](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html)). A documentação define a fase em uma frase: "Analyzed logical plans transforms which translates unresolvedAttribute and unresolvedRelation into fully typed objects".

**É aqui que nasce a `AnalysisException`.** Nome de coluna errado, função inexistente, tipo incompatível, agregação inválida: tudo estoura nesta fase, em milissegundos, antes de existir uma única tarefa. Essa é a resposta honesta a quem diz que PySpark não tem segurança de tipo: não tem em tempo de escrita, tem antes da distribuição.

**O que o livro acerta e o que envelheceu.** Acerta que o `Catalog` é a peça que entra na fase 1. O que ele não pode dizer, porque é posterior: desde o Spark 3.0 existe `CatalogManager` e catálogo plugável (DataSource V2), então "o catálogo" não é mais um objeto único e não é mais sinônimo do metastore do Hive. Informação de fora dos capítulos lidos.

---

### 3. Fase 2, otimização lógica: as regras, uma a uma

O livro nomeia quatro regras e fecha com "etc.": dobra de constante, pushdown de predicado, poda de projeção e simplificação booleana. A lista real do 4.2.0 tem mais de cem regras em pouco mais de vinte lotes, e ela é pública ([Optimizer.scala, v4.2.0](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala)). Não vale decorar todas. Vale conhecer as que aparecem no plano e as que, quando não disparam, custam dinheiro.

#### 3.1 A ordem dos lotes, que é informação em si

A sequência do `defaultBatches` no 4.2.0, resumida ([Optimizer.scala](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala)):

1. `Finish Analysis`, `Inline CTE`, `Union`, `LocalRelation early`
2. `Operator Optimization before Inferring Filters` (ponto fixo, o lote grande)
3. `Infer Filters` (uma passada): `InferFiltersFromGenerate`, `InferFiltersFromConstraints`
4. `Operator Optimization after Inferring Filters` (o mesmo lote grande, **de novo**)
5. `Push extra predicate through join`
6. `Pre CBO Rules`
7. `Early Filter and Projection Push-Down`
8. `Update CTE Relation Stats`
9. `Join Reorder`: `CostBasedJoinReorder`
10. `Eliminate Sorts`, `Decimal Optimizations`, `Distinct Aggregate Rewrite`, `Object Expressions Optimization`, `Check Cartesian Products`, `RewriteSubquery`, `NormalizeFloatingNumbers`

O detalhe de desenho está no item 4: o lote de otimização de operador roda **duas vezes**, antes e depois de inferir filtros. O motivo é o efeito em cascata: inferir um filtro novo cria oportunidade de empurrá-lo, e empurrá-lo cria oportunidade de podar coluna. Um comentário no próprio arquivo justifica a posição do `Early Filter and Projection Push-Down`: antes desse lote o plano pode conter nós que não reportam estatística, e "anything that uses stats must run after this batch". Ou seja, a ordem não é estética, é dependência de informação.

#### 3.2 As regras que você precisa reconhecer

| Regra | O que faz | Como você vê no plano |
|---|---|---|
| `PushDownPredicates` | é composta de três: `CombineFilters` funde filtros vizinhos, `PushPredicateThroughNonJoin` empurra filtro por cima de projeção, agregação e afins, `PushPredicateThroughJoin` empurra filtro para dentro dos ramos do join ([Optimizer.scala L2078](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala#L2078-L2085)) | o nó `Filter` desce na árvore entre o plano analisado e o otimizado |
| `ColumnPruning` | elimina coluna que ninguém consome. `SELECT *` seguido de um `select` de três colunas custa o mesmo que pedir três | `ReadSchema` curto no scan |
| `SchemaPruning` | poda **campo aninhado** dentro de `struct`, não só coluna de topo. Roda no lote de scan pushdown ([SparkOptimizer.scala L37](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkOptimizer.scala#L37-L47)) | `ReadSchema: struct<cliente:struct<id:bigint>>` em vez do struct inteiro |
| `ConstantFolding` e `ConstantPropagation` | `2 * 3` vira `6` na fase de planejamento, não por linha. Com propagação, `a = 5 AND b = a + 1` vira `a = 5 AND b = 6` | expressões já resolvidas no plano otimizado |
| `BooleanSimplification`, `SimplifyConditionals`, `SimplifyBinaryComparison`, `ReplaceNullWithFalseInPredicate` | reescrevem predicado. `NOT (a > 1)` vira `a <= 1`, `CASE` com condição constante colapsa, `NULL` em posição de predicado vira `false` | condição de `Filter` mais curta que a que você escreveu |
| `PruneFilters` | filtro sempre verdadeiro desaparece; sempre falso colapsa a subárvore para relação vazia | ramo inteiro do plano sumiu |
| `PropagateEmptyRelation` e `ConvertToLocalRelation` | relação vazia se propaga para cima (join com vazio é vazio) e cálculo sobre `LocalRelation` pequena é resolvido no driver | `LocalTableScan` no lugar de um scan de verdade |
| `InferFiltersFromConstraints` | a mais subestimada da lista. De `a.id = b.id` e `a.id > 100` ela **deduz** `b.id > 100` e empurra para o outro lado do join | um `Filter` que você não escreveu, no ramo oposto |
| `UnwrapCastInBinaryComparison` | `cast(int_col as bigint) > 10L` vira `int_col > 10`. Sem essa regra o `cast` em volta da coluna bloquearia o pushdown | filtro no `PushedFilters` em vez de só no `Filter` |
| `EliminateOuterJoin` | converte `outer` em `inner` quando um filtro posterior descarta as linhas nulas de qualquer jeito | `Inner` no plano onde você escreveu `left` |
| `ReorderJoin` | reordena join **por heurística**, empurrando para baixo os joins que têm condição. Não é custo | ordem de join diferente da que você escreveu |
| `CostBasedJoinReorder` | reordena por custo estimado, com programação dinâmica. **Depende do CBO ligado**, ver seção 6 | raramente, porque vem desligado |
| `LimitPushDown`, `EliminateLimits`, `LimitPushDownThroughWindow` | empurram e fundem `LIMIT` | `LocalLimit` perto da folha |
| `CollapseProject`, `CollapseRepartition`, `CollapseWindow`, `RemoveNoopOperators`, `RemoveRedundantAliases`, `RemoveRedundantAggregates` | limpeza. Fundem ou removem nós que não mudam nada | plano otimizado com menos nós que o analisado. É isso que o livro observa quando diz que o `Project` desapareceu, absorvido pelo `Relation`, e não explica |
| `RewriteDistinctAggregates`, `RewriteExceptAll`, `RewriteIntersectAll`, `ReplaceIntersectWithSemiJoin` | reescrevem operação de conjunto e `COUNT(DISTINCT)` em algo que o motor sabe executar. Explica por que dois `COUNT(DISTINCT)` na mesma consulta viram um plano bem maior | `Expand` no plano |
| `RewritePredicateSubquery`, `RewriteCorrelatedScalarSubquery` | subconsulta correlacionada vira semi-join ou anti-join | `LeftSemi` ou `LeftAnti` onde você escreveu `EXISTS` ou `IN` |

Duas novidades do 4.2.0 nesse território, e valem porque atacam o mesmo desperdício: `SPARK-40193` funde subplanos com condições de filtro diferentes e `SPARK-44571` estende a fusão para subplanos de agregação sem agrupamento, "reducing redundant scans" ([release notes 4.2.0](https://spark.apache.org/releases/spark-release-4-2-0.html)).

Se você precisar desligar uma regra para provar que ela é a culpada de um plano estranho, existe `spark.sql.optimizer.excludedRules`, que recebe nome de regra separado por vírgula, com a ressalva declarada no código de que regras necessárias para correção não são excluíveis ([SQLConf.scala](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala)). É ferramenta de diagnóstico, não de produção.

#### 3.3 A correção que importa nesta fase

O livro diz que a fase 2 tem dois estágios internos, que o Catalyst "primeiro constrói um conjunto de vários planos" e depois usa o CBO para atribuir custo a cada um. **Isso não descreve a fase 2.** A otimização lógica produz **um** plano, de forma determinística, por aplicação de regra até ponto fixo. Não há conjunto de candidatos e não há ranqueamento nesta fase. A única regra de custo do lote lógico é `CostBasedJoinReorder`, e ela vem desligada. Fonte: a lista de lotes do próprio otimizador, citada acima, e os defaults da seção 6.

E aqui se resolve a divergência 6 do pré-aula, sobre pushdown de predicado ser lógico ou físico. A resposta é **os dois, em dois passos distintos**:

1. **Mover o filtro para baixo na árvore é lógico.** É `PushDownPredicates`, no lote de otimização de operador.
2. **Entregar o filtro à fonte depende da fonte.** Para DataSource V2, quem entrega é `V2ScanRelationPushDown`, ainda na fase lógica, no lote `Early Filter and Projection Push-Down` ([SparkOptimizer.scala L37](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkOptimizer.scala#L37-L47)). Para fonte de arquivo V1, o `PushedFilters` que você lê no plano é calculado na **fase física**, quando `FileSourceStrategy` traduz os `dataFilters` e monta o `FileSourceScanExec` ([DataSourceScanExec.scala L508](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/DataSourceScanExec.scala#L508)).

Ou seja: o texto do livro e a figura do livro estão cada um com metade da razão, e nenhum dos dois diz que são dois passos. O sintoma prático de saber isso: se o seu filtro sumiu do `PushedFilters`, a pergunta certa é "a regra lógica não desceu o filtro, ou a fonte não aceitou o filtro?", e as duas causas têm remédios diferentes.

---

### 4. Fase 3, planejamento físico: os operadores, com nome

Esta é a fase que o livro entrega em uma frase, sem nomear um único operador. Aqui está o que falta.

#### 4.1 Como um operador lógico vira um físico

O `SparkPlanner` aplica uma lista de estratégias ao plano lógico otimizado. Cada estratégia casa um padrão lógico e devolve **zero ou mais** candidatos físicos ([SparkStrategies.scala, v4.2.0](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkStrategies.scala)). O mapa mínimo:

| Nó lógico | Operadores físicos possíveis |
|---|---|
| `Join` com chave de igualdade | `BroadcastHashJoinExec`, `ShuffledHashJoinExec`, `SortMergeJoinExec` |
| `Join` sem chave de igualdade | `BroadcastNestedLoopJoinExec`, `CartesianProductExec` |
| `Aggregate` | `HashAggregateExec`, `ObjectHashAggregateExec`, `SortAggregateExec` |
| `Project` | `ProjectExec` |
| `Filter` | `FilterExec` |
| `Sort` | `SortExec` |
| `Relation` de arquivo V1 | `FileSourceScanExec`, que imprime como `FileScan <formato>` |
| `Relation` de DataSource V2 | `BatchScanExec`, que imprime como `BatchScan` |
| `LocalRelation` | `LocalTableScanExec` |
| `Range` | `RangeExec` |
| redistribuição | `ShuffleExchangeExec` (imprime `Exchange`), `BroadcastExchangeExec` |
| conversão de formato | `ColumnarToRowExec`, `RowToColumnarExec` |
| avaliação de Python | `BatchEvalPythonExec`, `ArrowEvalPythonExec` |
| Dataset tipado | `DeserializeToObjectExec`, `MapElementsExec`, `SerializeFromObjectExec` |

Os nomes de arquivo confirmam a lista: o pacote `execution/joins` do 4.2.0 tem exatamente `BroadcastHashJoinExec.scala`, `BroadcastNestedLoopJoinExec.scala`, `CartesianProductExec.scala`, `ShuffledHashJoinExec.scala`, `SortMergeJoinExec.scala`, e o pacote `execution/aggregate` tem `HashAggregateExec.scala`, `ObjectHashAggregateExec.scala`, `SortAggregateExec.scala` ([árvore do repositório na tag v4.2.0](https://github.com/apache/spark/tree/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/joins)).

#### 4.2 Como o Spark escolhe entre estratégias de join

O livro fala em "recomendar um tipo de join mais eficiente" e nunca lista um. A ordem de decisão está escrita, em comentário, dentro da estratégia `JoinSelection`, e é curta o bastante para ler inteira ([SparkStrategies.scala L219-L237](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/SparkStrategies.scala#L219-L237)):

Havendo chave de igualdade, primeiro olha as dicas (*hints*), nesta ordem: dica de broadcast, dica de sort merge, dica de shuffle hash, dica de replicate NL. Não havendo dica, ou não sendo ela aplicável, as regras são estas, uma a uma:

1. **Broadcast hash join**, se um lado é pequeno o suficiente para ser transmitido e o tipo de join permite. Se os dois são pequenos, transmite o menor.
2. **Shuffled hash join**, se um lado é pequeno o suficiente para construir um mapa de hash local, é **muito** menor que o outro, e `spark.sql.join.preferSortMergeJoin` é `false`.
3. **Sort merge join**, se as chaves são ordenáveis.
4. **Produto cartesiano**, se o tipo de join é da família *inner*.
5. **Broadcast nested loop join**, como última solução. O comentário no código é honesto sobre o custo: "It may OOM but we don't have other choice".

Três coisas caem dessa lista.

Primeira: o **único** número que decide a escolha padrão é o limite de broadcast. `spark.sql.autoBroadcastJoinThreshold`, default `10485760` (10 MB), desde a 1.1.0, e `-1` desliga ([sql-performance-tuning.html](https://spark.apache.org/docs/latest/sql-performance-tuning.html)). Tudo o mais é regra, não custo.

Segunda: o passo 2 quase nunca dispara, porque `spark.sql.join.preferSortMergeJoin` tem default `true`. Ela é config **interna**, com a justificativa escrita no código: sort merge join consome menos memória que shuffled hash join e funciona bem quando as duas tabelas são grandes ([SQLConf.scala](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala)). Consequência prática: se você viu `SortMergeJoin` onde esperava `ShuffledHashJoin`, o motivo é esse default, não uma decisão de custo.

Terceira: `BroadcastNestedLoopJoin` no plano é alarme, não informação. Ele quase sempre quer dizer que a sua condição de join não é de igualdade e você está prestes a comparar cada linha com cada linha.

As dicas, quando você quiser forçar: `BROADCAST`, `MERGE`, `SHUFFLE_HASH`, `SHUFFLE_REPLICATE_NL`, com a precedência documentada ("Spark prioritizes the BROADCAST hint over the MERGE hint over the SHUFFLE_HASH hint over the SHUFFLE_REPLICATE_NL hint") e a ressalva de que não há garantia, porque uma estratégia pode não suportar o tipo de join ([sql-performance-tuning.html](https://spark.apache.org/docs/latest/sql-performance-tuning.html)).

#### 4.3 Onde os shuffles nascem

Depois de escolher os operadores, roda um conjunto de regras de **preparação**. A lista do 4.2.0, na ordem ([QueryExecution.scala L750-L781](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala#L750-L781)): `InsertAdaptiveSparkPlan`, `CoalesceBucketsInJoin`, `PlanDynamicPruningFilters`, `PlanSubqueries`, `RemoveRedundantProjects`, `EnsureRequirements`, `InsertSortForLimitAndOffset`, `ReplaceHashWithSortAgg`, `RemoveRedundantSorts`, `RemoveRedundantWindowGroupLimits`, `DisableUnnecessaryBucketedScan`, `ApplyColumnarRulesAndInsertTransitions`, `CollapseCodegenStages` e, fora de subconsulta, `ReuseExchangeAndSubquery`. A última é a que explica o nó `ReusedExchange` que aparece quando o mesmo shuffle serviria a dois ramos.

O primeiro item merece nota, porque é a costura entre esta fase e o AQE. `InsertAdaptiveSparkPlan` troca o plano por um `AdaptiveSparkPlanExec`, e o comentário no código diz o efeito: "`AdaptiveSparkPlanExec` is a leaf node. If inserted, all the following rules will be no-op as the original plan is hidden behind `AdaptiveSparkPlanExec`". Ou seja, **com AQE ligado as regras de preparação não rodam aqui**: elas rodam dentro do AQE, a cada replanejamento, numa lista quase idêntica que ainda acrescenta `AdjustShuffleExchangePosition`, `ValidateSparkPlan` e `OptimizeSkewedJoin` ([AdaptiveSparkPlanExec.scala L110-L132](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/AdaptiveSparkPlanExec.scala#L110-L132)).

A que importa é `EnsureRequirements`. Cada operador físico declara a distribuição e a ordenação que exige da entrada. `SortMergeJoinExec` exige as duas tabelas distribuídas pela chave e ordenadas por ela. `HashAggregateExec` final exige as linhas da mesma chave na mesma partição. `EnsureRequirements` compara o que o filho entrega com o que o pai exige e, quando não bate, **insere um `Exchange` e um `Sort`**.

Guarde isso, porque é o modelo mental que falta em toda a bibliografia: **você nunca escreve um shuffle. Você escreve uma exigência de distribuição, e uma regra de preparação materializa o shuffle.** É por isso que "contar `Exchange` no plano" é medir custo, e não contar palavra.

#### 4.4 A caixa `Cost Model` da figura não existe no código

O livro desenha `Physical Plans` como pilha, com três flechas descendo para uma caixa `Cost Model` e daí para `Selected Physical Plan`. O texto de 2015 diz a mesma coisa: "Spark SQL takes a logical plan and generates one or more physical plans, using physical operators that match the Spark execution engine. It then selects a plan using a cost model" ([Databricks, 2015](https://www.databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html)).

No código do 4.2.0, essa seleção não aconteceu. O método que transforma plano lógico em plano físico tem três linhas, e uma delas é um comentário:

```scala
// TODO: We use next(), i.e. take the first plan returned by the planner, here for now,
//       but we will implement to choose the best plan.
planner.plan(ReturnAnswer(plan)).next()
```

([QueryExecution.scala L806-L811](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/QueryExecution.scala#L806-L811))

O planejador devolve um **iterador** de candidatos, como a figura promete, e o Spark pega o **primeiro**. Onze anos depois do texto de 2015, o `TODO` está lá, na versão de julho de 2026. Isso não quer dizer que a escolha seja arbitrária: as estratégias já são escritas em ordem de preferência, e a escolha de join da seção 4.2 acontece **dentro** da estratégia, não depois dela. Mas a caixa `Cost Model` entre planos físicos, como etapa separada de ranqueamento, não é uma coisa que exista para ser encontrada. Informação de fora dos capítulos lidos, e verificável em três linhas de código.

---

### 5. Fase 4, geração de código: o que colapsa e o que não

O livro entrega a definição inteira: geração de código de estágio completo colapsa a consulta em uma única função, elimina chamada de função virtual e usa registrador de CPU para dado intermediário. Falta a outra metade, que é a metade prática: **o que impede isso de acontecer**, e como você vê no plano. O pré-aula registra essa lacuna com essas palavras.

#### 5.1 O que a fase faz

A regra `CollapseCodegenStages` percorre o plano físico, encontra subárvores que suportam geração de código e as embrulha em um `WholeStageCodegenExec`. Cada `WholeStageCodegenExec` recebe um id sequencial, e é esse id que aparece como `*(n)` na frente dos operadores. O código Java gerado é compilado em tempo de execução.

`spark.sql.codegen.wholeStage` tem default `true`, desde a 2.0.0, e é config **interna** ([SQLConf.scala](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala)). Desligar é diagnóstico, nunca produção.

#### 5.2 O que impede o colapso

O predicado que decide isso tem dez linhas e é legível ([WholeStageCodegenExec.scala L918-L935](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/WholeStageCodegenExec.scala#L918-L935)). Um operador entra no estágio compilado quando, e só quando:

1. ele implementa `CodegenSupport` **e** o seu `supportCodegen` é verdadeiro;
2. nenhuma das suas expressões é `CodegenFallback`, isto é, nenhuma expressão precisa cair para o caminho interpretado;
3. o schema de **saída** dele não tem campos demais;
4. o schema de **entrada** de nenhum filho tem campos demais.

"Campos demais" é `spark.sql.codegen.maxFields`, default **100**, contando campo aninhado, com a descrição declarada de ser "the maximum number of fields (including nested fields) that will be supported before deactivating whole-stage codegen". É config interna ([SQLConf.scala](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala)).

Esse é o mecanismo real por trás de "tabela larga fica misteriosamente lenta". Acima de cem colunas o Spark **desliga o codegen em silêncio** naquele pedaço do plano, e o único sinal é o asterisco que deixa de aparecer.

Há um segundo teto, por tamanho de bytecode: `spark.sql.codegen.hugeMethodLimit`, default `65535`, também interna, com a descrição de que, quando a função compilada passa desse limite, "the whole-stage codegen is deactivated for this subtree of the current query plan" ([SQLConf.scala](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala)). 65535 é o maior tamanho válido de método na JVM.

Além desses tetos, três fronteiras naturais: `Exchange` e `BroadcastExchange` (ali o dado é materializado e escrito), os nós de avaliação Python (`BatchEvalPythonExec`, `ArrowEvalPythonExec`), e os nós de objeto do Dataset tipado. Quando um operador não suporta codegen, o Spark insere um `InputAdapter` em volta dele, e o `InputAdapter` é justamente a costura entre o mundo compilado e o interpretado.

#### 5.3 Duas ressalvas de leitura, e duas novidades do 4.2.0

O livro chama a geração de código de estágio completo de "fase de otimização física de consulta", o que colide com a própria contagem de quatro fases: ela é parte da fase 4, não uma quinta. E chamar o resultado de "código compacto de RDD" é literal demais para uma coisa que é código Java compilado que roda dentro de uma tarefa.

Do 4.2.0, duas entradas atacam exatamente este ponto: `SPARK-56032`, eliminação de subexpressão dentro do codegen de `FilterExec`, e `SPARK-56482`, fusão de codegen para `UnionExec` ([release notes 4.2.0](https://spark.apache.org/releases/spark-release-4-2-0.html)). A segunda é a mais visível num plano: `Union` era fronteira de estágio e passou a poder ser fundida.

---

### 6. Onde o custo age, de verdade

O pré-aula registra a contradição: o texto do Damji põe o CBO e a escolha entre múltiplos planos na fase 2, e a Figura 3-4 do mesmo capítulo põe a pilha de planos e o `Cost Model` depois do plano lógico otimizado, ou seja, na fase 3. Resolução, com fonte primária.

**Quem está certo sobre a figura.** A figura. Ela reproduz o desenho de 2015, e o texto de 2015 também põe a seleção por custo no planejamento físico, com a ressalva que o livro não copiou: "At the moment, cost-based optimization is only used to select join algorithms: for relations that are known to be small, Spark SQL uses a broadcast join" ([Databricks, 2015](https://www.databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html)). Ou seja, já em 2015 "modelo de custo" queria dizer "limiar de broadcast", e não um otimizador de custo completo.

**Quem está errado sobre o texto.** O livro. A fase 2 não constrói conjunto de planos e não os ranqueia. É rule-based e determinística, como mostrei na seção 3.3.

**E o que nenhum dos dois diz.** Em 2026 o custo age em **três** lugares, nenhum deles sendo a caixa da figura:

| Onde | O que é comparado | Ligado por padrão |
|---|---|---|
| Fase 2, `CostBasedJoinReorder` | ordens de join alternativas, por custo estimado a partir de estatística do catálogo | **não** |
| Fase 3, dentro de `JoinSelection` | tamanho estimado de um lado contra `spark.sql.autoBroadcastJoinThreshold` | sim, e é só isso |
| Execução, AQE | plano atual contra plano reotimizado, por **contagem de shuffles** | **sim**, ver seção 7 |

#### 6.1 O CBO: precisa de `ANALYZE TABLE` e não vem ligado

Respondendo às duas perguntas exatas do pré-aula. Todos os defaults abaixo vêm da tabela de configuração de runtime do SQL da versão 4.2.0 ([configuration.html](https://spark.apache.org/docs/latest/configuration.html)):

| Config | Default | Desde | O que a doc diz |
|---|---|---|---|
| `spark.sql.cbo.enabled` | `false` | 2.2.0 | "Enables CBO for estimation of plan statistics when set true" |
| `spark.sql.cbo.joinReorder.enabled` | `false` | 2.2.0 | "Enables join reorder in CBO" |
| `spark.sql.cbo.joinReorder.dp.threshold` | `12` | 2.2.0 | máximo de nós de join na programação dinâmica |
| `spark.sql.cbo.joinReorder.dp.star.filter` | `false` | 2.2.0 | heurística de star-join na enumeração |
| `spark.sql.cbo.starSchemaDetection` | `false` | 2.2.0 | reordenação por detecção de esquema estrela |
| `spark.sql.cbo.planStats.enabled` | `false` | 3.0.0 | "the logical plan will fetch row counts and column statistics from catalog" |
| `spark.sql.statistics.histogram.enabled` | `false` | 2.3.0 | gera histograma equi-height ao coletar estatística de coluna, com custo de uma varredura extra |

Portanto: **o CBO não vem ligado, e ligá-lo sem estatística coletada não faz nada útil.** Ligar de verdade custa quatro coisas: `spark.sql.cbo.enabled=true`, `spark.sql.cbo.joinReorder.enabled=true`, estatística no catálogo e um catálogo que a guarde.

A coleta é explícita, por comando ([ANALYZE TABLE](https://spark.apache.org/docs/latest/sql-ref-syntax-aux-analyze-table.html)):

```sql
-- só o tamanho em bytes, sem varrer a tabela
ANALYZE TABLE pedidos COMPUTE STATISTICS NOSCAN;

-- contagem de linhas e tamanho: varre a tabela
ANALYZE TABLE pedidos COMPUTE STATISTICS;

-- estatística por coluna (a que o CBO usa para estimar seletividade)
ANALYZE TABLE pedidos COMPUTE STATISTICS FOR ALL COLUMNS;
```

A documentação declara que `NOSCAN` "collects only the table's size in bytes (which does not require scanning the entire table)", que o comportamento padrão "collects both number of rows and size in bytes", e que as estatísticas "are stored in the catalog". Sem catálogo persistente, não há CBO.

#### 6.2 De onde vem estatística, e como olhar

A documentação de tuning lista três origens, e a distinção é a que o Luu apaga ao chamar tudo de "inferido" ([sql-performance-tuning.html, seção *Leveraging Statistics*](https://spark.apache.org/docs/latest/sql-performance-tuning.html)):

- **Fonte de dados**: estatística que o Spark lê direto da fonte, "like the counts and min/max values in the metadata of Parquet files".
- **Catálogo**: estatística lida do catálogo, "collected or updated whenever you run `ANALYZE TABLE`".
- **Runtime**: estatística que o Spark calcula sozinho enquanto a consulta roda. "This is part of the adaptive query execution framework."

E a mesma página diz como inspecionar cada uma: `DESCRIBE EXTENDED` para estatística de tabela e de coluna, `EXPLAIN COST` ou `DataFrame.explain(mode="cost")` para as estimativas que o otimizador fez, e a aba SQL da UI para as de runtime, onde se procura por `Statistics(..., isRuntime=true)` no plano.

A frase que fecha a seção vale copiar, porque é o argumento inteiro em uma linha: "Missing or inaccurate statistics will hinder Spark's ability to select an optimal plan, and may lead to poor query performance".

---

### 7. O AQE quebra a cascata estática

O pré-aula registra a ausência: o capítulo descreve o Catalyst como jornada estática de quatro fases, decidida antes de a execução começar, e não menciona AQE nem Dynamic Partition Pruning, que chegaram no Spark 3.0, a versão que o livro diz cobrir. Aqui está o que aquele desenho perde.

#### 7.1 Está ligado, e há uma pegadinha de versão

"Adaptive Query Execution (AQE) is an optimization technique in Spark SQL that makes use of the runtime statistics to choose the most efficient query execution plan, which is enabled by default since Apache Spark 3.2.0. Spark SQL can turn on and off AQE by `spark.sql.adaptive.enabled` as an umbrella configuration" ([sql-performance-tuning.html](https://spark.apache.org/docs/latest/sql-performance-tuning.html)).

A pegadinha: na tabela de configuração, `spark.sql.adaptive.enabled` aparece com default `true` e coluna "Since Version" igual a **1.6.0** ([sql-performance-tuning.html](https://spark.apache.org/docs/latest/sql-performance-tuning.html), confirmado em [configuration.html](https://spark.apache.org/docs/latest/configuration.html)). Os dois números são verdadeiros e falam de coisas diferentes: a chave existe desde a 1.6.0, o AQE como está hoje é do 3.0.0, e o default virou `true` na 3.2.0. Quem cita só a tabela erra a história.

#### 7.2 Onde ele se encaixa, mecanicamente

O AQE não é uma quinta fase. É um operador que fica na **raiz** do plano físico, o `AdaptiveSparkPlanExec`, e que executa o resto do plano em pedaços. O laço, lido no código ([AdaptiveSparkPlanExec.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/AdaptiveSparkPlanExec.scala)):

1. **Corta o plano em estágios de consulta** nas fronteiras de `Exchange` e de broadcast. Um `ShuffleQueryStage` ou `BroadcastQueryStage` é um pedaço que pode ser materializado sozinho.
2. **Executa os estágios cujas dependências estão prontas** e espera a materialização.
3. Quando um estágio termina, **substitui o nó correspondente no plano lógico** por um `LogicalQueryStage`, que carrega a estatística real do que foi escrito. O comentário no código chama isso de fechar o "semantic gap" entre plano lógico e físico.
4. **Reotimiza**: roda o otimizador lógico sobre esse plano lógico atualizado e replaneja fisicamente, com `reOptimize` ([L805](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/AdaptiveSparkPlanExec.scala#L805)).
5. **Compara o custo** do plano novo com o do atual e adota o novo se o custo for menor, ou igual mas diferente. O comentário no código: "Adopt the new plan if its cost is equal to or less than that of the current plan".
6. Volta ao passo 1 até não haver mais estágio. Aí `isFinalPlan` passa a `true`.

Duas precisões que quase todo material erra.

**A reotimização lógica não roda o otimizador inteiro.** Ela roda o `AQEOptimizer`, que tem quatro lotes e cinco regras: `AQEPropagateEmptyRelation`, `ConvertToLocalRelation` e `UpdateAttributeNullability` no lote de propagação de relação vazia, `DynamicJoinSelection`, `EliminateLimits` e `OptimizeOneRowPlan` ([AQEOptimizer.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/AQEOptimizer.scala)). Ou seja: estatística real **não** volta para o pushdown nem para a reordenação de join. Ela volta para "esse ramo está vazio, corte" e para a escolha de estratégia de join. O replanejamento **físico**, esse sim, roda inteiro.

**O modelo de custo do AQE é contagem de shuffle.** Literalmente. O avaliador padrão coleta os nós `ShuffleExchangeLike` do plano e devolve a quantidade como custo; quando `forceOptimizeSkewedJoin` está ligado, ele também conta os joins marcados como skew e os prioriza ([simpleCosting.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/simpleCosting.scala)):

```scala
val numShuffles = plan.collect { case s: ShuffleExchangeLike => s }.size
...
SimpleCost(numShuffles)
```

Isso é a melhor validação possível da regra prática de leitura de plano: **contar `Exchange` é a forma mais rápida de estimar o custo de um plano, e é exatamente o que o Spark faz.** Não é heurística de blog, é o `CostEvaluator` padrão do motor. Dá para trocar por um seu, via `spark.sql.adaptive.customCostEvaluatorClass`, sem default, desde a 3.2.0 ([sql-performance-tuning.html](https://spark.apache.org/docs/latest/sql-performance-tuning.html)).

#### 7.3 O que ele reotimiza, com que estatística

As regras aplicadas a cada estágio novo, na ordem do código: `PlanAdaptiveDynamicPruningFilters`, `ReuseAdaptiveSubquery`, `OptimizeSkewInRebalancePartitions`, `CoalesceShufflePartitions`, `OptimizeShuffleWithLocalRead` ([AdaptiveSparkPlanExec.scala L137-L145](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/AdaptiveSparkPlanExec.scala#L137-L145)). Em linguagem de documentação, são estas as otimizações, com os defaults do 4.2.0 ([sql-performance-tuning.html](https://spark.apache.org/docs/latest/sql-performance-tuning.html)):

**Coalescer partições pós-shuffle.** A estatística usada é a saída do map, ou seja, o tamanho real de cada partição de shuffle já escrita. O Spark funde partições vizinhas até chegar perto de um tamanho alvo, "to avoid too many small tasks". Configs: `spark.sql.adaptive.coalescePartitions.enabled` (`true`, 3.0.0), `spark.sql.adaptive.advisoryPartitionSizeInBytes` (`64 MB`, 3.0.0), `spark.sql.adaptive.coalescePartitions.minPartitionSize` (`1MB`, 3.2.0), `spark.sql.adaptive.coalescePartitions.initialPartitionNum` (sem default, cai em `spark.sql.shuffle.partitions`, 3.0.0). E uma pegadinha: `spark.sql.adaptive.coalescePartitions.parallelismFirst` tem default `true` e, quando verdadeiro, o Spark **ignora** o alvo de 64 MB e respeita só o mínimo de 1 MB, para maximizar paralelismo (3.2.0). Quem ajusta `advisoryPartitionSizeInBytes` sem desligar essa chave está mexendo em número que não está sendo lido.

**Converter sort-merge join em broadcast hash join.** "AQE converts sort-merge join to broadcast hash join when the runtime statistics of any join side are smaller than the adaptive broadcast hash join threshold." O texto é honesto sobre o limite: "This is not as efficient as planning a broadcast hash join in the first place, but it's better than continuing the sort-merge join". O limiar é `spark.sql.adaptive.autoBroadcastJoinThreshold`, sem default próprio, herdando `spark.sql.autoBroadcastJoinThreshold` (3.2.0).

**Ler shuffle local.** `spark.sql.adaptive.localShuffleReader.enabled`, `true`, 3.0.0. Quando a repartição deixou de ser necessária, cada tarefa lê os arquivos de shuffle da própria máquina, o que poupa rede.

**Converter sort-merge join em shuffled hash join**, quando todas as partições pós-shuffle são menores que `spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold`, cujo default é `0` (3.2.0), isto é, **desligado** até você definir um valor.

**Otimizar join enviesado.** "This feature dynamically handles skew in sort-merge join by splitting (and replicating if needed) skewed tasks into roughly evenly sized tasks." Configs: `spark.sql.adaptive.skewJoin.enabled` (`true`, 3.0.0), `skewedPartitionFactor` (`5.0`, 3.0.0) e `skewedPartitionThresholdInBytes` (`256MB`, 3.0.0). Uma partição é considerada enviesada quando é maior que 5 vezes a mediana **e** maior que 256 MB. E `spark.sql.adaptive.forceOptimizeSkewedJoin` tem default `false` (3.3.0): por padrão o Spark **abre mão** da correção de skew quando ela introduziria um shuffle extra, porque o modelo de custo dele conta shuffle.

**Rebalancear partições enviesadas** em `RebalancePartitions`: `spark.sql.adaptive.optimizeSkewsInRebalancePartitions.enabled` (`true`, 3.2.0) e `rebalancePartitionsSmallPartitionFactor` (`0.2`, 3.3.0).

#### 7.4 O que sobra do desenho do livro

A cascata continua descrevendo a **primeira** passagem. O que ela perde é que a passagem não é única.

| No desenho do livro | No motor de 2026 |
|---|---|
| uma passagem, do topo até `RDDs` | uma passagem inicial, mais um replanejamento por estágio de shuffle concluído |
| plano físico escolhido antes de qualquer execução | plano físico escolhido em pedaços, com o pedaço seguinte decidido depois de o anterior rodar |
| `Cost Model` entre planos físicos | nenhuma seleção por custo entre planos físicos (o `.next()` da seção 4.4); custo real só no AQE, contando shuffle |
| `200` como número de partições de shuffle | `200` como número **inicial**, quase sempre reduzido por coalescência |
| estatística estimada | estatística real de partição, medida no shuffle já escrito |
| plano é artefato de compilação | plano é artefato de execução, e `explain()` antes da ação mostra outra coisa |

O item que mais muda o dia a dia é o penúltimo. **Com AQE ligado, o número que você lê no `Exchange` não é o número de tarefas que vão rodar.** A recomendação da própria documentação inverte a prática antiga de calibrar `spark.sql.shuffle.partitions`: "You do not need to set a proper shuffle partition number to fit your dataset. Spark can pick the proper shuffle partition number at runtime once you set a large enough initial number of shuffle partitions via `spark.sql.adaptive.coalescePartitions.initialPartitionNum`" ([sql-performance-tuning.html](https://spark.apache.org/docs/latest/sql-performance-tuning.html)).

E o vizinho do AQE, que o livro também não tem: **Dynamic Partition Pruning**, `spark.sql.optimizer.dynamicPartitionPruning.enabled`, default `true` desde a 3.0.0, descrita como gerar predicado para a coluna de partição quando ela é usada como chave de join ([SQLConf.scala](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala)). Ela aparece no plano como um `SubqueryBroadcast` dentro de `PartitionFilters`, e é como um filtro numa tabela de dimensão poda partições da tabela de fato.

---

### 8. Como ler um `explain()` campo por campo

Esta é a dívida mais concreta da bibliografia. O pré-aula registra: o Damji imprime a saída completa dos quatro blocos e não explica nada dela, nem o `*(n)`, nem `Exchange`, nem o `200`, nem `partial_sum`, nem `PushedFilters`; e nas 41 páginas do Luu a palavra `explain` tem zero ocorrências.

#### 8.1 Os modos, e qual usar

A sintaxe SQL é `EXPLAIN [ EXTENDED | CODEGEN | COST | FORMATTED ] statement`, e sem parâmetro "this clause provides information about a physical plan only" ([EXPLAIN, SQL Reference](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html)). Na API de DataFrame a assinatura é `DataFrame.explain(extended=None, mode=None)`, com o argumento `mode` adicionado na 3.0.0 ([pyspark.sql.DataFrame.explain](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.explain.html)).

| Chamada | O que sai, na definição da doc | Para que serve na prática |
|---|---|---|
| `df.explain()` ou `mode="simple"` | "Print only a physical plan" | conferência rápida |
| `df.explain(True)` ou `mode="extended"` | "Print both logical and physical plans", isto é, os quatro blocos: parsed, analyzed, optimized, physical | **comparar analisado com otimizado.** É o único modo que mostra o que o otimizador fez |
| `mode="formatted"` | "Split explain output into two sections: a physical plan outline and node details" | ler plano grande. É o modo que eu recomendo por padrão |
| `mode="cost"` | "Print a logical plan and statistics if they are available" | ver a estimativa que o otimizador usou, e descobrir que ela é ruim |
| `mode="codegen"` | "Print a physical plan and generated codes if they are available" | quase nunca. Só quando a pergunta é sobre o Java gerado |

**A recomendação.** Use `formatted` como default de leitura. O motivo está na forma da saída: primeiro um esqueleto numerado, com um nó por linha, depois um bloco de detalhe por número. A doc mostra o formato ([EXPLAIN](https://spark.apache.org/docs/latest/sql-ref-syntax-qry-explain.html)):

```text
== Physical Plan ==
* HashAggregate (4)
+- Exchange (3)
   +- * HashAggregate (2)
      +- * LocalTableScan (1)

(1) LocalTableScan [codegen id : 1]
Output: [k#19, v#20]

(3) Exchange
Input: [k#19, sum#24L]
```

Note o que `formatted` faz de melhor: o `*` fica sozinho e o id do codegen vira texto legível, `[codegen id : 1]`, em vez do enigmático `*(1)`. Em plano de quarenta operadores o modo padrão vira muro de texto e o `formatted` continua legível, porque a árvore e o detalhe estão separados.

E use `extended` quando a pergunta for "por que meu filtro não desceu". Comparar `== Analyzed Logical Plan ==` com `== Optimized Logical Plan ==` é o diagnóstico mais direto que existe: se o filtro está no mesmo lugar nos dois, alguma regra não casou.

#### 8.2 Um plano curto, lido nó por nó

A consulta:

```python
# pedidos está particionado por ano no disco; clientes é uma tabela pequena
resultado = (pedidos
    .filter(F.col("ano") == 2026)
    .join(clientes, "cliente_id")
    .groupBy("uf")
    .agg(F.sum("valor").alias("total"))
    .orderBy(F.desc("total")))

resultado.count()      # força a execução: o AQE precisa rodar para o plano ficar final
resultado.explain()    # agora sim
```

O plano abaixo está na forma que o Spark 4.x imprime para uma consulta desta forma. Os nomes de campo (`Batched`, `PartitionFilters`, `PushedFilters`, `DataFilters`, `ReadSchema`, `Location`, `Format`) são os que o `FileSourceScanExec` emite, verificáveis no código ([DataSourceScanExec.scala L518-L528](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/DataSourceScanExec.scala#L518-L528)). Os `exprId` (`#18`, `#33`) e os `plan_id` variam a cada execução.

```text
AdaptiveSparkPlan isFinalPlan=true
+- == Final Plan ==
   *(4) Sort [total#61L DESC NULLS LAST], true, 0
   +- AQEShuffleRead coalesced
      +- ShuffleQueryStage 2
         +- Exchange rangepartitioning(total#61L DESC NULLS LAST, 200), ENSURE_REQUIREMENTS
            +- *(3) HashAggregate(keys=[uf#33], functions=[sum(valor#21)])
               +- AQEShuffleRead coalesced
                  +- ShuffleQueryStage 1
                     +- Exchange hashpartitioning(uf#33, 200), ENSURE_REQUIREMENTS
                        +- *(2) HashAggregate(keys=[uf#33], functions=[partial_sum(valor#21)])
                           +- *(2) Project [uf#33, valor#21]
                              +- *(2) BroadcastHashJoin [cliente_id#18], [cliente_id#30], Inner, BuildRight
                                 :- *(2) Filter isnotnull(cliente_id#18)
                                 :  +- *(2) ColumnarToRow
                                 :     +- FileScan parquet [cliente_id#18,valor#21,ano#25]
                                 :          Batched: true,
                                 :          PartitionFilters: [isnotnull(ano#25), (ano#25 = 2026)],
                                 :          PushedFilters: [IsNotNull(cliente_id)],
                                 :          ReadSchema: struct<cliente_id:bigint,valor:double>
                                 +- BroadcastQueryStage 0
                                    +- BroadcastExchange HashedRelationBroadcastMode(...), [plan_id=42]
                                       +- *(1) Filter isnotnull(cliente_id#30)
                                          +- *(1) ColumnarToRow
                                             +- FileScan parquet [cliente_id#30,uf#33] ...
+- == Initial Plan ==
   Sort [total#61L DESC NULLS LAST], true, 0
   +- Exchange rangepartitioning(total#61L DESC NULLS LAST, 200), ENSURE_REQUIREMENTS
      +- HashAggregate(keys=[uf#33], functions=[sum(valor#21)])
         +- Exchange hashpartitioning(uf#33, 200), ENSURE_REQUIREMENTS
            +- HashAggregate(keys=[uf#33], functions=[partial_sum(valor#21)])
               +- Project [uf#33, valor#21]
                  +- SortMergeJoin [cliente_id#18], [cliente_id#30], Inner
                     ...
```

**Regra zero: leia de baixo para cima.** A folha é onde o dado entra, a raiz é o resultado. Os `+-` marcam filho e os `:-` marcam o primeiro filho de um nó de dois filhos, que é como se desenha o ramo esquerdo de um join.

Agora, nó por nó:

**`FileScan parquet [...]`.** A folha. Tudo o que você conseguir empurrar para cá é I/O que não acontece.

**`Batched: true`.** No código, este campo é `supportsColumnar.toString` ([DataSourceScanExec.scala L524](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/DataSourceScanExec.scala#L524)). Quer dizer que o scan devolve **lotes colunares**, não linha por linha, ou seja, o leitor vetorizado está em uso. Para Parquet, quem controla é `spark.sql.parquet.enableVectorizedReader`, default `true` desde a 2.0.0 ([configuration.html](https://spark.apache.org/docs/latest/configuration.html)). `Batched: false` custa CPU e é o valor que aparece no exemplo do livro, porque lá a fonte é CSV. O Damji escolheu um exemplo em que quase nenhuma otimização de leitura aparece, e é isso que o pré-aula registra como escolha ruim de exemplo.

**`PartitionFilters: [(ano#25 = 2026)]`.** A poda mais barata que existe: o Spark elimina **diretórios inteiros** pelo nome, sem abrir arquivo. Só aparece se a tabela foi escrita particionada por aquela coluna.

**`PushedFilters: [IsNotNull(cliente_id)]`.** Filtro delegado ao leitor Parquet, que usa mínimo e máximo por grupo de linhas para pular blocos. Controlado por `spark.sql.parquet.filterPushdown`, default `true` desde a 1.2.0 ([configuration.html](https://spark.apache.org/docs/latest/configuration.html)). Se o filtro que você escreveu aparece **só** no nó `Filter` acima e não aqui, você está lendo dado para jogar fora.

**`DataFilters`.** Aparece no modo verboso e é o conjunto de filtros sobre coluna de dado antes da tradução para a API da fonte. A diferença entre `DataFilters` e `PushedFilters` é justamente o que a fonte aceitou.

**`ReadSchema: struct<cliente_id:bigint,valor:double>`.** Prova da poda de coluna. Note duas coisas. Primeira: `ano` está na lista de saída do scan e **não** está no `ReadSchema`, porque coluna de partição vem do caminho do arquivo, não de dentro dele. Segunda: se a sua tabela tem duzentas colunas e aqui aparecem duzentas, a poda falhou.

**`ColumnarToRow`.** Converte o lote colunar do leitor para o formato de linha interno. É normal, e a sua presença confirma que o `Batched: true` está de fato produzindo lote.

**`Filter isnotnull(cliente_id#18)`.** O filtro residual, o que a fonte não garantiu sozinha. É esperado ver a mesma condição no `PushedFilters` e aqui: o Spark reaplica por segurança, porque o pushdown é aproximado (pula bloco, não garante linha).

**`BroadcastExchange` no ramo direito.** O lado pequeno indo inteiro para todos os executores. É um `Exchange`, mas não é shuffle: é difusão.

**`BroadcastHashJoin ... Inner, BuildRight`.** O join bom, sem shuffle do lado grande. `BuildRight` diz de qual lado o mapa de hash foi construído. Se aqui estivesse `SortMergeJoin`, seriam dois `Exchange` mais dois `Sort`. Se estivesse `BroadcastNestedLoopJoin`, seria alarme.

**`HashAggregate(..., functions=[partial_sum(valor#21)])`.** Agregação **parcial**, do lado do map, antes do shuffle. Reduz o volume que atravessa a rede. É o padrão desejável, e é o que faz `groupBy().sum()` ser barato e `groupBy().collect_list()` ser caro: a segunda não tem forma parcial útil.

**`Exchange hashpartitioning(uf#33, 200), ENSURE_REQUIREMENTS`.** O shuffle. Três informações em uma linha:

- **`hashpartitioning`**: as linhas vão para a partição dada pelo hash das expressões. A garantia declarada no código é: "All rows where `expressions` evaluate to the same values are guaranteed to be in the same partition", e o id de partição é literalmente `Pmod(CollationAwareMurmur3Hash(expressions), numPartitions)` ([partitioning.scala L299-L321](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/physical/partitioning.scala#L299-L321)). É o particionamento de join e de agregação: junta igual com igual, e não diz nada sobre ordem entre partições.
- **`rangepartitioning`** (o do `Exchange` de cima, o do `orderBy`): as linhas são divididas por **faixas de uma ordem total**. A garantia declarada: dadas duas partições vizinhas, toda linha da segunda é maior que qualquer linha da primeira, segundo as expressões de ordenação, e "there is no overlap between partitions" ([partitioning.scala L604-L616](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/physical/partitioning.scala#L604-L616)). É o que permite ordenar globalmente sem juntar tudo numa partição, e é caro porque exige amostrar o dado para descobrir os limites das faixas. **Resumo da diferença que a bibliografia deixou aberta: hash agrupa, range ordena.**
- **`200`**: é `spark.sql.shuffle.partitions`, default **200** desde a 1.1.0, descrita como "the default number of partitions to use when shuffling data for joins or aggregations" ([sql-performance-tuning.html](https://spark.apache.org/docs/latest/sql-performance-tuning.html)). Não é mágico, não é calculado, não tem relação com o seu cluster nem com o seu dado. É uma constante escolhida em 2014 que sobreviveu. Com AQE ligado ele é o número **inicial**, e o `AQEShuffleRead coalesced` logo acima é a prova de que o número final foi menor.
- **`ENSURE_REQUIREMENTS`**: a origem do shuffle. O código enumera as origens com comentário ([ShuffleExchangeExec.scala L152-L183](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/exchange/ShuffleExchangeExec.scala#L152-L183)): `ENSURE_REQUIREMENTS` quer dizer que a regra interna inseriu por necessidade e "Spark is free to optimize it as long as the requirements are still ensured"; `REPARTITION_BY_COL` quer dizer que você pediu `repartition(col)` e o Spark ainda pode mudar o número de partições; `REPARTITION_BY_NUM` quer dizer que você pediu um número exato e, nas palavras do comentário, "Spark can't optimize it"; `REBALANCE_PARTITIONS_BY_NONE` e `REBALANCE_PARTITIONS_BY_COL` vêm de `rebalance`; `REQUIRED_BY_STATEFUL_OPERATOR` é de streaming e é estático de propósito. **Consequência prática direta: `repartition(200, "chave")` amarra as mãos do AQE, `repartition("chave")` não.**

**`ShuffleQueryStage 1` e `BroadcastQueryStage 0`.** Os pedaços que o AQE materializou, numerados na ordem em que foram criados. A presença deles é a marca de que você está lendo um plano que já rodou.

**`AQEShuffleRead coalesced`.** Aqui o AQE fundiu partições de shuffle pequenas. Outros valores que aparecem nesse nó: `local` (leitura local de shuffle) e `skewed` (partição enviesada dividida).

**`*(2)`, `*(3)`, `*(4)`.** Geração de código de estágio completo, e o número é o id do estágio de codegen. Todos os nós marcados `*(2)` foram compilados em **uma única função Java**. Nó **sem** asterisco é fronteira: `Exchange`, `BroadcastExchange`, `AQEShuffleRead` e `ShuffleQueryStage` nunca têm. Se um `Project` ou `Filter` seu aparece sem asterisco, vale investigar: cai nas condições da seção 5.2.

**`AdaptiveSparkPlan isFinalPlan=true`.** A raiz. O campo vem de `stringArgs`, que imprime literalmente `isFinalPlan=$isFinalPlan` ([AdaptiveSparkPlanExec.scala L434](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/AdaptiveSparkPlanExec.scala#L434)). E aqui está o detalhe que muda a forma de trabalhar:

- **`isFinalPlan=false`**: você está vendo o plano **antes** de executar. Ele é especulativo. Todo `explain()` chamado antes de uma ação mostra isso.
- **`isFinalPlan=true`**: o plano acabou de rodar.
- **Os cabeçalhos `== Final Plan ==` e `== Initial Plan ==` só aparecem se o plano mudou.** O código é explícito: se `currentPhysicalPlan.fastEquals(initialPlan)`, imprime **uma** árvore, sem cabeçalho; caso contrário imprime a atual, rotulada `Current Plan` ou `Final Plan` conforme `isFinalPlan`, e depois `Initial Plan` ([AdaptiveSparkPlanExec.scala L458-L490](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/AdaptiveSparkPlanExec.scala#L458-L490)). Ou seja: ver os dois cabeçalhos é a prova de que o AQE mudou algo, e não ver não quer dizer que o AQE está desligado.

No exemplo acima, os dois cabeçalhos estão lá e o diff é o que interessa: o plano inicial tinha `SortMergeJoin`, o final tem `BroadcastHashJoin`. O AQE mediu o lado de `clientes` depois do shuffle, viu que caberia num broadcast e trocou a estratégia. É esse diff que você quer ver, e é ele que o `explain()` chamado antes da ação esconde.

**Fluxo recomendado.** Rode a ação, depois chame `explain()`. Ou, melhor, use a aba SQL/DataFrame da Spark UI, que dá o plano final com métrica por nó (linhas de saída, bytes de shuffle, tempo, spill), coisa que `explain()` nunca dá. O 4.2.0 mexeu bastante nessa aba: plano SQL navegável com zoom e busca, painel lateral de detalhe, botão de copiar o texto do plano, e, o mais direto ao ponto desta seção, **comparação lado a lado do plano inicial contra o final para consultas com AQE** (`SPARK-55877`, `SPARK-55760`, `SPARK-55785`, `SPARK-56048`, `SPARK-56792`, `SPARK-56799`, em [release notes 4.2.0](https://spark.apache.org/releases/spark-release-4-2-0.html)).

#### 8.3 Tabela de bolso

| Campo | Onde aparece | O que significa | O que te diz |
|---|---|---|---|
| `*(n)` | prefixo de operador | estágio de codegen `n`; todos os `*(n)` iguais viraram uma função Java | falta de asterisco é fronteira ou fallback |
| `#12`, `#61L` | sufixo de coluna | `exprId`, identidade única de atributo; o `L` é o tipo `long` | permite rastrear a mesma coluna por todo o plano |
| `Exchange` | nó | shuffle: escrita em disco mais tráfego de rede, e fronteira de estágio | contar `Exchange` é medir custo, e o AQE faz exatamente isso |
| `hashpartitioning(cols, n)` | dentro do `Exchange` | mesma chave vai para a mesma partição | join e agregação |
| `rangepartitioning(ord, n)` | dentro do `Exchange` | faixas de uma ordem total, sem sobreposição | ordenação global, e custa amostragem |
| `200` | dentro do `Exchange` | `spark.sql.shuffle.partitions` | número inicial, não final, quando há AQE |
| `ENSURE_REQUIREMENTS` | dentro do `Exchange` | o Spark inseriu o shuffle por exigência de distribuição, e pode otimizá-lo | `REPARTITION_BY_NUM` no lugar dele quer dizer que você travou a otimização |
| `BroadcastExchange` | nó | difusão do lado pequeno, não shuffle | acompanha `BroadcastHashJoin` |
| `PartitionFilters` | metadado do scan | diretórios eliminados pelo nome | a poda mais barata; vazio quer dizer tabela não particionada por essa coluna |
| `PushedFilters` | metadado do scan | filtros entregues ao leitor | vazio com filtro no `Filter` acima é I/O desperdiçado |
| `ReadSchema` | metadado do scan | colunas realmente lidas | prova da poda de coluna |
| `Batched` | metadado do scan | `supportsColumnar`, ou seja, leitura vetorizada em lote | `false` custa CPU |
| `partial_sum`, `partial_count` | dentro de `HashAggregate` | agregação parcial pré-shuffle | ausência quer dizer que tudo atravessa a rede |
| `AQEShuffleRead coalesced` / `local` / `skewed` | nó | o AQE fundiu partições, leu shuffle local ou dividiu partição enviesada | prova de que o AQE agiu |
| `ShuffleQueryStage n` | nó | pedaço já materializado | você está lendo plano pós-execução |
| `isFinalPlan=false` | raiz | plano especulativo | rode a ação e chame de novo |
| `Statistics(..., isRuntime=true)` | modo `cost` e UI | estatística medida, não estimada | ([sql-performance-tuning.html](https://spark.apache.org/docs/latest/sql-performance-tuning.html)) |
| `BatchEvalPython`, `ArrowEvalPython` | nó | UDF Python; fronteira de codegen e caixa preta para o Catalyst | filtro não atravessa esse nó |
| `SubqueryBroadcast` dentro de `PartitionFilters` | metadado do scan | Dynamic Partition Pruning agiu | poda de partição decidida em runtime |

---

### 9. O que o AQE não resolve

Vender o otimizador como mágica é o erro que a bibliografia comete pelo silêncio e que material de fornecedor comete pelo entusiasmo. A lista abaixo é o contrapeso. Tudo aqui é informação de fora dos capítulos lidos, derivada dos mecanismos citados nas seções 7 e 8.

**Não age antes do primeiro shuffle.** A estatística que o AQE usa é a de shuffle materializado. Uma consulta de scan mais filtro mais escrita, **sem nenhum `Exchange`**, não tem ponto de reotimização: o AQE fica na raiz do plano e não faz nada. Todo o problema de leitura (arquivo pequeno demais, arquivo grande demais, partição de leitura mal dimensionada) continua governado por config estática: `spark.sql.files.maxPartitionBytes` (default `134217728`, 128 MB, desde 2.0.0), `spark.sql.files.openCostInBytes` (`4194304`, 4 MB, 2.0.0) e `spark.sql.files.minPartitionNum` (paralelismo default, 3.1.0), todos em [sql-performance-tuning.html](https://spark.apache.org/docs/latest/sql-performance-tuning.html).

**Coalescer é depois, não em vez de.** O shuffle já foi escrito quando o AQE decide fundir partições. Ele conserta o número de tarefas do lado de leitura, não o volume escrito. Se o problema é o tamanho do shuffle, o remédio é reduzir dado antes dele (filtro, poda, agregação parcial), não ajustar coalescência.

**Não conserta layout de dado.** Se a tabela não foi escrita particionada, não há `PartitionFilters` para nenhum otimizador inventar. Se o Parquet foi escrito em milhares de arquivos de 2 MB, o custo de abrir arquivo é pago antes de o AQE existir. Decisão de escrita não é reotimizável em leitura.

**Não reordena join por custo.** O `AQEOptimizer` tem cinco regras e `CostBasedJoinReorder` não está entre elas (seção 7.2). Estatística real melhora a escolha de **estratégia** de join, não a **ordem** dos joins. Em consulta de estrela com sete dimensões, a ordem continua vindo de `ReorderJoin`, que é heurística, a menos que você ligue o CBO e colete estatística.

**Não conserta skew de agregação.** A otimização de skew documentada é de join: "dynamically handles skew in sort-merge join" ([sql-performance-tuning.html](https://spark.apache.org/docs/latest/sql-performance-tuning.html)). `groupBy` sobre chave enviesada continua produzindo uma tarefa gigante, e o remédio continua sendo manual (salting, pré-agregação, chave composta).

**Abre mão do skew quando o conserto custa um shuffle.** `spark.sql.adaptive.forceOptimizeSkewedJoin` é `false` por default (3.3.0). Como o custo do AQE é contagem de shuffle, um plano que conserta skew ao preço de um `Exchange` extra é rejeitado, ainda que fosse melhor no relógio. Em pipeline com skew crônico, essa chave é candidata a virar `true`, com medição.

**O modelo de custo é grosso.** Contagem de `ShuffleExchangeLike`, e nada mais ([simpleCosting.scala](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/adaptive/simpleCosting.scala)). Ele não sabe de spill, de pressão de memória, de custo de ordenação nem de assimetria de rede. Um plano com um shuffle e uma ordenação enorme "custa" menos que um plano com dois shuffles baratos, e o AQE prefere o primeiro.

**Não adota plano que empate para pior.** A adoção exige custo menor, ou igual com plano diferente. Melhoria que não reduz contagem de shuffle tende a não ser adotada.

**Não vê dentro de uma UDF Python.** `BatchEvalPython` e `ArrowEvalPython` são caixa preta para o Catalyst, antes e depois do AQE. Filtro não atravessa esse nó, coluna não é podada depois dele, e o codegen quebra ali. Nenhuma estatística de runtime resolve isso.

**Não conserta cache mal usado.** Depois de `cache()`, o plano é fixado a partir do ponto materializado, e a poda de coluna que aconteceria abaixo dele deixa de acontecer. É a causa clássica de `ReadSchema` com todas as colunas onde você esperava três.

**Não salva de uma tarefa que já está rodando.** A reotimização acontece **entre** estágios. Um estágio que estourou memória ou que está fazendo spill em disco vai até o fim (ou até o erro) do jeito que foi planejado.

**Não conserta o que você não escreveu.** Nenhum otimizador inventa um filtro que não existe na consulta, e nenhum inventa uma condição de join. Cartesiano por condição esquecida continua sendo cartesiano, e ele **não** estoura por padrão: existe a regra `CheckCartesianProducts` no lote lógico, mas ela é governada por `spark.sql.crossJoin.enabled`, config interna, default `true` desde a 2.0.0, cuja descrição é "When false, we will throw an error if a query contains a cartesian product without explicit CROSS JOIN syntax" ([SQLConf.scala](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala)). Ou seja, com o default o Spark aceita em silêncio o produto cartesiano que você escreveu por acidente.

**E, do lado da leitura de plano: `explain()` antes da ação mente por omissão.** `isFinalPlan=false` é o aviso, e ele é fácil de ignorar. Metade das discussões sobre "o Spark escolheu o join errado" morre quando alguém roda a ação e olha o plano final.

---

### 10. O que trocar no desenho do livro

Fechando a parte com o saldo em cinco linhas, porque o resto é detalhe verificável:

1. **A cascata de quatro fases está certa como primeira passagem e errada como retrato do motor.** O motor de 2026 é cascata mais laço.
2. **A caixa `Cost Model` entre planos físicos não corresponde a código.** O planejador devolve um iterador e o Spark pega o primeiro, com `TODO` aberto na versão de julho de 2026.
3. **O CBO não vem ligado, mora na fase lógica e exige `ANALYZE TABLE`.** O texto do livro errou a fase e a figura errou a natureza.
4. **`explain()` só é útil depois de a ação rodar**, e `formatted` é o modo de leitura.
5. **Contar `Exchange` é medir custo**, e não é regra de bolso de blog: é o `CostEvaluator` padrão do AQE.

---

### Perguntas que a parte 1 abriu

**1. Se nenhum modelo de custo escolhe entre planos físicos, por que a figura canônica ainda circula em livro de 2020 e em slide de 2026?**
*Hipótese:* a figura descreve a intenção declarada em 2015, e o `SparkPlanner` de fato devolve um iterador de candidatos, o que mantém a figura "quase" verdadeira. A seleção nunca foi implementada porque o custo migrou de lugar: entrou para dentro das estratégias (limiar de broadcast) e depois para o AQE (custo real). Pergunta concreta ao professor: ele conhece consulta em que o iterador devolve mais de um candidato e a ordem das estratégias muda o resultado?

**2. Vale ligar o CBO em 2026, num ambiente com metastore e `ANALYZE TABLE` agendado, ou o AQE já cobre?**
*Hipótese:* vale num caso só, e é o que o AQE não faz: **ordem** de join em consulta de muitas tabelas, tipicamente esquema estrela. Fora disso, estatística de catálogo envelhece e estimativa errada é pior que nenhuma, enquanto a estatística de runtime do AQE é sempre atual. Custo de ligar: quatro chaves mais uma rotina de coleta que varre tabela.

**3. A reotimização do AQE usa o `AQEOptimizer`, com cinco regras, e não o otimizador inteiro. Isso quer dizer que estatística real nunca melhora pushdown nem poda?**
*Hipótese:* sim, e é uma limitação de desenho, não de implementação: pushdown e poda são decididos por estrutura da consulta, não por volume, então reexecutá-los com estatística nova não mudaria nada. O que estatística nova muda é escolha de estratégia e eliminação de ramo vazio, que é exatamente o que as cinco regras cobrem. Se a hipótese estiver certa, a lista curta é escolha deliberada e não dívida técnica.

**4. Com AQE ligado, a recomendação é subir `spark.sql.shuffle.partitions` (ou `initialPartitionNum`) e deixar coalescer. Mas `coalescePartitions.parallelismFirst` é `true` e faz o coalescing ignorar o alvo de 64 MB. As duas recomendações não se anulam?**
*Hipótese:* não se anulam, mas a combinação default entrega partição pequena, porque prioriza paralelismo e respeita só o mínimo de 1 MB. Em cluster com muitos núcleos isso é bom; em job com muitas partições e escrita no fim, produz arquivo pequeno. Suspeito que a receita correta seja subir o número inicial **e** pôr `parallelismFirst=false` quando o objetivo é tamanho de arquivo de saída, e a pergunta é se o professor viu esse par ser ajustado junto na prática.

**5. Onde termina o pushdown lógico e começa o físico depende de a fonte ser V1 ou V2. Isso tem consequência prática de escolha de fonte?**
*Hipótese:* pouca, hoje, para leitura de arquivo, porque `FileSourceStrategy` faz o trabalho na fase física e o resultado que você lê no `PushedFilters` é equivalente. A consequência real aparece em fonte externa (Iceberg, JDBC, conector próprio): em V2 o pushdown é negociado ainda no plano lógico, então dá para vê-lo em `explain(extended=True)` sem chegar ao plano físico. Se estiver certo, a divergência dos livros é um artefato de eles só terem visto V1.

**6. `spark.sql.codegen.maxFields` é 100 e é config interna. Como se detecta em produção que uma consulta caiu para interpretado por largura de tabela?**
*Hipótese:* o único sinal é a ausência do asterisco nos nós do plano, e ninguém procura por ausência. Suspeito que a rotina certa seja comparar contagem de operadores com asterisco antes e depois de uma mudança de schema, e que subir o valor seja arriscado porque o limite existe para não gerar método além de `hugeMethodLimit`. Pergunta: existe métrica na UI que denuncie fallback de codegen, ou só o plano?

**7. Se o custo do AQE é contagem de shuffle, quando esse modelo escolhe errado?**
*Hipótese:* nos casos em que o gargalo não é rede. Trocar dois shuffles baratos por um shuffle mais uma ordenação gigante reduz o "custo" medido e aumenta o tempo real, e o mesmo vale para plano que evita shuffle ao preço de spill. Se a hipótese se sustenta, `customCostEvaluatorClass` deixa de ser curiosidade e passa a ser ferramenta para quem tem um perfil de carga estável e medido.

**8. Qual é o critério de revisão de plano que cabe num pull request?**
*Hipótese:* três perguntas, na ordem, e todas respondíveis com `explain("formatted")` mais uma execução: quantos `Exchange` existem e cada um é necessário; o `ReadSchema` e o `PushedFilters` estão coerentes com o que a consulta pede; e há algum nó sem asterisco que devia ter. Quero saber se o professor acrescentaria uma quarta, e qual.
---

## Parte 2 - Schema, contrato e dado ruim

Versão de referência: **Apache Spark 4.2.0**, lançado em 14 de julho de 2026 ([release notes](https://spark.apache.org/releases/spark-release-4-2-0.html)). A bibliografia cobre o Spark 3.0 (Luu, Damji) e o ciclo 3.4 (Chadha). Esta parte fecha o bloco "Schema e contrato" de [O que fica para o aprofundamento](01-pre-aula.md#o-que-fica-para-o-aprofundamento) e ataca as divergências 4, 7 e 11 do pré-aula.

O eixo é um só: **schema no Spark não é validação, é declaração**. Quase todo erro operacional desta camada nasce de tratar uma promessa como se fosse uma verificação. As sete subseções abaixo separam, uma por uma, o que o motor de fato faz do que os livros dizem que ele faz.

Duas convenções. Onde escrevo "o livro diz X", a fonte é sempre o [pré-aula](01-pre-aula.md), nunca a memória. Onde a informação vem de fora dos cinco capítulos lidos, escrevo **fora da bibliografia** e ponho o link. Onde o código-fonte contradiz a documentação oficial, digo os dois e marco como dúvida aberta, porque não tive Spark instalado nesta máquina para arbitrar.

| Subseção | Do que trata |
|---|---|
| [1](#1-inferir-contra-ler-metadado-duas-operações-com-um-nome-só) | varrer dado contra ler rodapé, o custo de cada um, `samplingRatio` e como ver o job na UI |
| [2](#2-as-duas-formas-de-declarar-schema-e-por-que-a-ddl-ganhou) | programática e DDL, nas duas linguagens, e o que a DDL expressa de verdade |
| [3](#3-nullable-é-promessa-não-validação) | por que `nullable=false` vira `true` na leitura de arquivo, e o que isso custa |
| [4](#4-o-espectro-completo-de-dado-ruim) | os três modos, `_corrupt_record`, `badRecordsPath` e um desenho de quarentena |
| [5](#5-o-modo-ansi-default-desde-o-spark-40) | o que passou a falhar, como voltar atrás, e o que ANSI não governa |
| [6](#6-o-tipo-variant-o-que-nenhum-dos-três-livros-tem) | VARIANT, shredding e o trade-off honesto |
| [7](#7-os-tipos-que-a-bibliografia-não-lista) | `TimestampNTZType`, `TimeType`, intervalos, `CHAR`/`VARCHAR` e os geoespaciais do 4.2 |

---

### 1. Inferir contra ler metadado: duas operações com um nome só

O pré-aula registra o problema com precisão na leitura 4: a Tabela 3-2 do Luu diz que algumas fontes trazem o schema embutido no arquivo, "especialmente Parquet e ORC", e que nesses casos "the schema is automatically inferred". O mesmo verbo, **inferir**, aparece para CSV, JSON, Parquet e ORC. E a Tabela 3-3 do mesmo capítulo descreve JSON como formato em que "nome e tipo de coluna inferidos automaticamente". O Luu chega perto de fechar o buraco quando diz que "it is quite expensive to load a very large JSON file", mas nunca diz que a inferência **lê o dado**, e não fala uma palavra sobre CSV.

Quem fecha é o Damji 3, e o pré-aula anota isso como o achado da rodada: declarar o schema **impede o Spark de criar um job separado** só para ler uma porção grande do arquivo e determinar o schema. É a frase certa. O que falta nela é a fronteira: para Parquet e ORC não existe job de varredura, existe **leitura de rodapé**, que é outra ordem de grandeza.

#### O que o motor faz, formato por formato

| Formato | O que o Spark toca para descobrir o schema | Bytes lidos | Jobs disparados |
|---|---|---|---|
| **CSV** com `inferSchema=true` | o dado inteiro, linha por linha, e depois lê tudo de novo na leitura real | duas passadas completas | **dois**: um `take(1)` para o cabeçalho, um para os tipos |
| **CSV** com `inferSchema=false` e sem schema | nada. Toda coluna sai `string` | zero na descoberta | zero de inferência |
| **JSON** (não existe `inferSchema` aqui) | o dado inteiro, parseando cada objeto | uma passada completa | **um** |
| **Parquet** com `mergeSchema=false` (default) | o rodapé de **um** arquivo | ordem de KB | **um**, com **uma** task |
| **Parquet** com `mergeSchema=true` | o rodapé de **todos** os arquivos | KB vezes N arquivos | **um**, com até `defaultParallelism` tasks |
| **ORC** | igual ao Parquet, com `spark.sql.orc.mergeSchema` | idem | idem |
| qualquer um com `.schema(...)` | nada | zero | **zero** |

Cada linha dessa tabela tem endereço no código do 4.2.0, e vale ter os endereços porque é o que permite discutir o assunto sem apelar para folclore.

**CSV custa duas passadas, e a documentação diz isso em cinco palavras.** A opção `inferSchema` tem default `false` e a descrição oficial é "Infers the input schema automatically from data. **It requires one extra pass over the data**" ([CSV Data Source Options](https://spark.apache.org/docs/latest/sql-data-sources-csv.html)). "Uma passada extra" quer dizer: a passada de inferência **mais** a leitura de verdade. E são dois jobs, não um: [`CSVDataSource.scala`](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/csv/CSVDataSource.scala#L147-L154) faz primeiro um `.take(1)` para pegar a linha de cabeçalho e só depois chama `inferFromDataset`, que agrega os tipos sobre o conjunto todo. O `take(1)` é barato (lê o começo de um arquivo), o segundo não.

**JSON não tem `inferSchema`.** O pré-aula já marcou o erro na tabela de vocabulário ("E que a opção é de CSV, não de JSON") e a tabela oficial de opções de JSON confirma: `inferSchema` não está lá ([JSON Data Source Options](https://spark.apache.org/docs/latest/sql-data-sources-json.html)). O Listing 3-14 do Luu, que passa `option("inferSchema","true")` junto com `.schema(movieSchema2)` num `read.json`, é duas vezes inútil: a opção não existe para o formato e o schema explícito já desligaria a inferência. Em JSON a inferência é o comportamento padrão quando não há schema, e ela varre o dado: [`JsonDataSource.scala`](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/json/JsonDataSource.scala#L96-L114) monta um `Dataset[String]`, aplica a amostragem e roda `JsonInferSchema.infer` sobre o RDD resultante.

**Parquet e ORC leem rodapé, e ainda assim disparam um job.** Este é o detalhe que o folclore erra nas duas direções. Não é varredura, mas também não é grátis nem invisível. O caminho é [`ParquetUtils.inferSchema`](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/parquet/ParquetUtils.scala#L86-L134): quando `mergeSchema` é `false`, a lista `filesToTouch` recebe **um** arquivo só (`_common_metadata`, senão `_metadata`, senão o primeiro arquivo de dado); quando é `true`, recebe todos. Nos dois casos a chamada seguinte é `mergeSchemasInParallel`, e o comentário do código é literal: "**Issues a Spark job to read Parquet/ORC schema in parallel**" ([`SchemaMergeUtils.scala`](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/SchemaMergeUtils.scala#L71-L80)). Com um arquivo na lista, o `numParallelism` calculado é 1, ou seja um job de **uma task** que lê um rodapé.

Isso corrige o Damji pelos dois lados: declarar schema não elimina "o job", elimina **a varredura**; e ler Parquet sem schema não é ausência de job, é um job minúsculo.

Os defaults de merge, com fonte: `spark.sql.parquet.mergeSchema` é `false` desde 1.5.0, e a descrição oficial explica o que acontece no caso negativo, "the schema is picked from the summary file or a **random data file** if no summary file is available"; `spark.sql.orc.mergeSchema` é `false` desde 3.0.0, "otherwise the schema is picked from a random data file" (as duas na [Runtime SQL Configuration](https://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration)). Guarde a expressão "arquivo de dado aleatório": ela é a resposta para a pergunta de por que um diretório com schemas divergentes às vezes lê certo e às vezes lê errado, sem nada ter mudado.

#### Quanto custa, em números

Não achei benchmark oficial, então o que vem abaixo é **estimativa minha**, por decomposição, não medição. O cenário: 25 GB, 200 arquivos de 128 MB, cinquenta colunas.

| Caminho | Bytes lidos até o primeiro registro sair | Ordem de grandeza |
|---|---|---|
| CSV, `inferSchema=true` | 25 GB (inferência) mais 25 GB (leitura) | 50 GB |
| CSV, schema declarado | 25 GB | 25 GB |
| Parquet, sem schema, `mergeSchema=false` | um rodapé, mais a leitura das colunas pedidas | dezenas de KB mais a projeção |
| Parquet, `mergeSchema=true` | 200 rodapés | poucos MB mais a projeção |

O rodapé de um Parquet cresce com número de colunas e número de row groups, e a faixa realista vai de poucos KB a algumas centenas de KB. Tomando 50 KB, a razão entre inferir CSV e ler rodapé de Parquet é da ordem de **10^5 a 10^6**. É essa razão, e não a compressão, que responde por boa parte da diferença de tempo de `spark.read.csv` contra `spark.read.parquet` em dado grande. Chamar as duas operações de "inferência" apaga um fator de cem mil.

Um efeito colateral que a bibliografia não menciona: **o custo da inferência de CSV e JSON é pago no driver**, no sentido de que ele acontece no planejamento, antes de qualquer ação do usuário. É por isso que um notebook parece travar em `spark.read.csv(...)`, uma linha que "não faz nada". A [Parte 1 do aprofundamento da aula 01](../aula-01/02-aprofundamento.md#parte-1---arquitetura-e-modelo-de-execução) já tinha registrado esse padrão em outra forma: algumas chamadas que não são ações disparam jobs, e inferência de schema é o caso número um.

#### `samplingRatio`: o que faz, e por que quase nunca é a resposta

O pré-aula registra que o Damji 3 oferece `samplingRatio` como meio-termo, com exemplo em `0.001`, e que o Luu declara o default `1.0` e recomenda **baixar** o valor para acelerar o carregamento de JSON grande. Nenhum dos dois diz o que a opção faz por dentro, e é aí que a recomendação desanda.

Defaults, com fonte: `samplingRatio` é `1.0` em CSV, "Defines fraction of rows used for schema inferring", e `1.0` em JSON, "Defines fraction of input JSON objects used for schema inferring" ([CSV](https://spark.apache.org/docs/latest/sql-data-sources-csv.html), [JSON](https://spark.apache.org/docs/latest/sql-data-sources-json.html)).

A implementação é idêntica nos dois formatos e tem três propriedades que mudam a conclusão ([`CSVUtils.scala`](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/csv/CSVUtils.scala#L115-L136), [`JsonUtils.scala`](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/json/JsonUtils.scala#L34-L55)):

1. **Valor acima de 0,99 desliga a amostragem por completo.** O código faz `if (options.samplingRatio > 0.99) csv else csv.sample(...)`. Passar `0.995` é o mesmo que passar `1.0`.
2. **Valor zero ou negativo levanta erro**, por um `require(options.samplingRatio > 0)`.
3. **A amostra é `sample(withReplacement = false, ratio, seed = 1)` sobre as linhas.** Semente fixa em 1, ou seja o resultado é determinístico para o mesmo layout de arquivos, o que é bom para reprodutibilidade e péssimo para quem espera que "rodar de novo" mude a amostra.

A consequência que ninguém escreve: em modo linha única, **`samplingRatio` não reduz I/O**. `Dataset.sample` é um filtro sobre a varredura, então todo byte continua sendo lido do armazenamento e quebrado em linhas; o que a amostra corta é o **parsing** e a **fusão de tipos**, que é CPU. Ou seja, o conselho do Luu de baixar `samplingRatio` para acelerar o carregamento vale para CPU e não vale para o gargalo de object storage, que costuma ser o que dói.

Em modo `multiLine=true` o quadro muda e piora: a amostragem acontece sobre um `RDD[PortableDataStream]`, isto é sobre **arquivos inteiros** ([`JsonDataSource.scala`](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/json/JsonDataSource.scala#L162-L176)). Aí sim há economia de I/O real, e o risco explode: com `0.001` e mil arquivos, o schema sai de **um** arquivo.

O risco, que o pré-aula registrou como pergunta ("o que acontece se a amostra não representar o arquivo"), tem três formas:

- **Coluna rara desaparece.** Campo que aparece em 0,05% dos registros não entra no schema com `samplingRatio=0.001`. Na leitura real, ele é simplesmente ignorado, sem erro e sem aviso.
- **Tipo estreita.** Uma coluna que é `long` no arquivo inteiro mas cabe em `int` na amostra sai `int`, e o valor grande virá nulo ou vai falhar, conforme o modo.
- **A promessa de "detectar erro cedo" morre.** O terceiro benefício que o Damji atribui a declarar schema não sobrevive a uma amostragem: você não declarou nada, apenas pediu ao Spark que adivinhasse com menos dado.

Recomendação prática: `samplingRatio` é ferramenta de exploração, para descobrir o schema **uma vez**, imprimir com `df.schema` e então **congelar** o resultado no código. Como valor de produção, é um contrato sorteado.

#### Como confirmar na Spark UI que houve job de inferência

Três instrumentos, do mais direto ao mais confiável.

**Aba Jobs, em `http://localhost:4040`.** Abra a sessão, não chame ação nenhuma e execute só o `read`. Se aparecer job, foi inferência, listagem de arquivos ou descoberta de partição. Não tive PySpark nesta máquina para conferir o texto exato da descrição do job, então **não afirmo o rótulo**: o que afirmo é a contagem, e a contagem é o que interessa. Referência de porta e de abas: [Monitoring and Instrumentation](https://spark.apache.org/docs/latest/monitoring.html).

**Grupo de job mais `statusTracker`, que é o teste determinístico.** Funciona em script, sem olhar tela:

```python
sc = spark.sparkContext
sc.setJobGroup("probe", "medir jobs de descoberta de schema")
df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
print(len(sc.statusTracker().getJobIdsForGroup("probe")))  # CSV inferido: 2
sc.setJobGroup("probe2", "com schema declarado")
df2 = spark.read.schema(esquema).option("header", "true").csv(path)
print(len(sc.statusTracker().getJobIdsForGroup("probe2")))  # 0
```

**API REST, para automatizar em CI.** O endpoint é `/api/v1/applications/[app-id]/jobs`, documentado em [Monitoring, REST API](https://spark.apache.org/docs/latest/monitoring.html). Contar antes e depois do `read` transforma "não infira schema em produção" em teste executável, que é a diferença entre uma regra de estilo e um guarda-corpo.

Duas ressalvas para não interpretar errado o que aparecer:

- **Job na leitura de Parquet não é sinal de varredura.** Pelo que vimos acima, o job de rodapé com uma task é o comportamento normal. Olhe o número de tasks e os bytes lidos no stage, não a existência do job.
- **Job pode ser de listagem, não de schema.** `spark.sql.sources.parallelPartitionDiscovery.threshold` tem default **32** e a descrição diz que, se o número de caminhos detectados passa desse valor durante a descoberta de partição, o Spark "tries to list the files with another Spark distributed job" ([Runtime SQL Configuration](https://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration)). Declarar schema **não** elimina esse job, porque as colunas de partição continuam saindo do layout de diretórios.

E o fato que fecha a subseção: declarar schema corta a inferência na raiz, não a torna mais barata. Em [`DataSource.getOrInferFileFormatSchema`](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/DataSource.scala#L221-L232) a expressão é `userSpecifiedSchema.map { ... }.orElse { format.inferSchema(...) }`: com schema do usuário, `inferSchema` nunca é chamado.

---

### 2. As duas formas de declarar schema, e por que a DDL ganhou

O pré-aula registra que **só o Damji 3** entrega as duas formas com código nas duas linguagens, e que ele declara preferência explícita pela DDL, chamando-a de "much simpler and easier to read". Os outros dão metades diferentes: Luu 3.2 só a programática (`DDL` tem zero ocorrências nas 41 páginas), Damji 4 só a DDL, Chadha só a programática.

**Forma programática.** Objetos `StructType` e `StructField`, com nulabilidade no terceiro argumento:

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

esquema = StructType([
    StructField("id", IntegerType(), False),
    StructField("titulo", StringType(), True),
    StructField("campanhas", ArrayType(StringType(), containsNull=False), True),
])
```

```scala
import org.apache.spark.sql.types._

val esquema = StructType(Array(
  StructField("id", IntegerType, nullable = false),
  StructField("titulo", StringType, nullable = true),
  StructField("campanhas", ArrayType(StringType, containsNull = false), nullable = true)))
```

A diferença de sintaxe que a Tabela 3-3 do Damji erra, segundo o pré-aula, aparece aqui: em Python o tipo é instanciado (`IntegerType()`), em Scala é objeto (`IntegerType`). E o terceiro argumento de `ArrayType` chama-se `containsNull`, não `nullable`, o que a própria saída de `printSchema` do livro mostra e o rótulo da tabela contradiz.

**Forma DDL.** Uma string, idêntica nas duas linguagens:

```python
esquema = "id INT NOT NULL, titulo STRING, campanhas ARRAY<STRING>"
df = spark.read.schema(esquema).json(caminho)
```

```scala
val esquema = "id INT NOT NULL, titulo STRING, campanhas ARRAY<STRING>"
val df = spark.read.schema(esquema).json(caminho)
```

**Aqui há uma correção a fazer, e ela contraria o que eu tinha suposto no pré-aula.** A anotação da leitura 2 diz que o capítulo "não diz o que a DDL não consegue expressar (nulabilidade por campo, por exemplo, que a forma programática expressa com o terceiro argumento)". A suposição está errada: **a DDL expressa nulabilidade**. A regra da gramática é literal:

```text
colType
    : colName=errorCapturingIdentifier dataType (errorCapturingNot NULL)? commentSpec?
    ;
```

([`SqlBaseParser.g4`, v4.2.0](https://github.com/apache/spark/blob/v4.2.0/sql/api/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBaseParser.g4#L1555-L1557)). E o construtor do `StructField` a partir dessa árvore faz `nullable = NULL == null`, ou seja, casar `NOT NULL` produz `nullable = false` ([`DataTypeAstBuilder.scala`](https://github.com/apache/spark/blob/v4.2.0/sql/api/src/main/scala/org/apache/spark/sql/catalyst/parser/DataTypeAstBuilder.scala#L490-L499)). A mesma regra vale dentro de `STRUCT<...>`, pelo `complexColType`. Além de `NOT NULL`, a DDL aceita `COMMENT`, o que carrega metadado de documentação junto do contrato.

| Critério | Programática | DDL |
|---|---|---|
| Tipos aninhados | verbosa, uma classe por nível | `ARRAY<STRUCT<a INT, b STRING>>`, uma linha |
| Nulabilidade | terceiro argumento de `StructField` | `NOT NULL`, conforme a gramática acima |
| Comentário de coluna | `StructField(...).withComment` ou metadado | `COMMENT 'texto'` |
| Metadado arbitrário | `metadata` do `StructField` | **não expressa** |
| `containsNull` de array e `valueContainsNull` de mapa | argumento explícito | não há sintaxe própria, herda o default `true` |
| Portátil entre Python e Scala | não, o código muda | **sim**, é a mesma string |
| Guardável em YAML, JSON de config, coluna de tabela | ruim | **boa**, é texto |
| Erro de digitação | erro de tipo na linguagem, pego cedo | erro de parse em runtime |
| Verifica se casa com o dado | não | não |

**Por que a DDL ganhou na prática**, em quatro razões que se somam:

1. **Uma linha em vez de dez.** Para um schema de trinta colunas com dois níveis de aninhamento, a forma programática ocupa meia tela e esconde o erro na indentação.
2. **É dado, não código.** Uma string pode viver num arquivo de configuração, num campo de tabela de contrato, num parâmetro de job. `StructType` não sai do processo sem serialização.
3. **É a mesma coisa nas duas linguagens.** Equipe mista deixa de manter duas cópias do mesmo contrato.
4. **Fecha o ciclo com o que o Spark imprime.** `df.schema.toDDL` devolve exatamente a string que `.schema()` aceita de volta ([implementação em `types.py`](https://github.com/apache/spark/blob/v4.2.0/python/pyspark/sql/types.py#L1847-L1857); não há página de API dedicada, então cito o código). O fluxo de trabalho honesto é: infira uma vez na exploração, imprima `toDDL`, cole no código, nunca mais infira.

O que **nenhuma** das duas formas faz, e é o assunto da subseção seguinte: verificar que o dado obedece ao que você declarou.

---

### 3. `nullable` é promessa, não validação

A divergência 11 do pré-aula é a mais bem documentada de todas, porque o próprio livro imprime a prova contra si: o Damji 3 monta o mesmo DataFrame de seis autores duas vezes, com `nullable=false` em cada campo. No exemplo em Python, com dado em memória, todo campo sai `nullable = false` e o array sai `containsNull = false`. No exemplo em Scala, com o mesmo `false` passado em cada `StructField` e leitura de um `blogs.json`, todo campo sai `nullable = true` e `containsNull = true`. E o texto afirma que "the output from the Scala program is no different than that from the Python program". As saídas na página dizem o contrário, e o livro não comenta.

O pré-aula acerta o diagnóstico ("a leitura de JSON força nulabilidade") e não tem o mecanismo. O mecanismo é uma linha.

#### O mecanismo: `forceNullable = true`, embutido no caminho de arquivo

Quando o Spark resolve uma fonte de arquivo, ele chama `DataSource.resolveRelation`, cuja assinatura é:

```scala
def resolveRelation(
    checkFilesExist: Boolean = true,
    readOnly: Boolean = false,
    forceNullable: Boolean = true): BaseRelation = {
```

e mais abaixo, ao montar a relação:

```scala
dataSchema = if (forceNullable) dataSchema.asNullable else dataSchema,
```

([`DataSource.scala`, v4.2.0](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/DataSource.scala#L365-L368) e [linha 446](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/DataSource.scala#L446)). O default do parâmetro é `true` e não há chave de configuração para mudá-lo no caminho de lote. `asNullable` percorre o `StructType` inteiro, incluindo campos aninhados, e põe tudo em `nullable = true`. **Isso acontece depois de o seu schema ser aceito.** Ou seja, o Spark não ignora o seu `false` por bug: ele o descarta por desenho, e por um motivo declarado.

O motivo aparece escrito no caminho de streaming, que usa a mesma ideia com uma chave própria, `spark.sql.streaming.fileSource.schema.forceNullable`, interna, default `true` desde 3.0.0: "When true, force the schema of streaming file source to be nullable (including all the fields). **Otherwise, the schema might not be compatible with actual data, which leads to corruptions**" ([`SQLConf.scala`](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L3880-L3888)). Traduzido: o Spark relaxa a sua promessa porque **não confia nela**, e um plano físico construído sobre uma promessa falsa produz resultado errado, não exceção.

Por que o dado em memória mantém o `false`: `createDataFrame` não passa por `resolveRelation`, então o schema entra como você escreveu. E, em PySpark, ele é de fato verificado: `createDataFrame` tem `verifySchema=True` por padrão ("verify data types of every row against schema. Enabled by default", [API de `createDataFrame`](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.SparkSession.createDataFrame.html)) e um `None` em campo não nulável levanta `PySparkValueError` com a mensagem `FIELD_NOT_NULLABLE_WITH_NAME`, "`<field_name>`: This field is not nullable, but got None" ([`types.py`](https://github.com/apache/spark/blob/v4.2.0/python/pyspark/sql/types.py#L3188-L3205)). Duas ressalvas da própria documentação: a verificação **não é efetiva** para entrada `pandas.DataFrame` com Arrow ligado nem para `pyarrow.Table`, e **não é suportada no Spark Connect**.

O quadro completo, então, é este:

| Origem do dado | `nullable=false` é respeitado na saída de `printSchema`? | É verificado contra o dado? |
|---|---|---|
| `createDataFrame` de coleção local, PySpark clássico | sim | **sim**, `verifySchema=True` levanta erro |
| `createDataFrame` de pandas com Arrow, ou `pyarrow.Table` | sim | não, segue coerção do Arrow |
| `createDataFrame` via Spark Connect | sim | **não**, `verifySchema` não é suportado |
| `spark.read` de CSV, JSON, Parquet, ORC, XML | **não**, vira `true` | não |
| JDBC com coluna `NOT NULL` na origem | sim, vem `false` da própria origem | pelo banco, não pelo Spark |

A última linha explica a única saída `nullable = false` vinda de **fonte externa** em toda a bibliografia, que o pré-aula anotou sem comentário do autor: a tabela `film` do Sakila, no Listing 3-20 do Luu, onde `film_id`, `title`, `language_id`, `rental_duration`, `rental_rate` e `last_update` saem `nullable = false`. Não é o Spark validando nada. É o Spark **copiando** a restrição declarada no catálogo do MySQL.

#### E se eu declarar `false` e vier nulo?

Em leitura de arquivo, a pergunta não chega a ser feita, porque o seu `false` já foi trocado por `true` antes de qualquer linha ser lida. O nulo entra, a coluna aceita, e nada acontece.

O caso perigoso é o outro: quando o `false` **sobrevive** e mente. Isso ocorre em DataFrame construído em memória sem verificação, em tabela cujo catálogo declara `NOT NULL` sem que a camada de armazenamento imponha, e em UDF ou expressão cuja nulabilidade o Catalyst deduz como `false`. Aí o efeito é de otimização, e é aqui que o assunto deixa de ser cosmético:

- **O Catalyst usa nulabilidade para eliminar verificação.** Se um campo é `nullable=false`, `isnotnull(campo)` é constante verdadeira e o filtro desaparece do plano. Filtro que desapareceu não filtra nada.
- **Muda escolha de join e de agregação.** Semântica de nulo em join e em `count` depende de saber se pode haver nulo.
- **O codegen pode omitir o teste de nulo por registro**, que é justamente o ganho que se busca ao declarar `false`.

O resultado de mentir, portanto, não é exceção: é **resultado silenciosamente errado**. É o pior modo de falha possível, e é o argumento mais forte para tratar `nullable` como o que ele é.

Um exemplo recente de como esse terreno é escorregadio, **fora da bibliografia**: "Since Spark 4.1, the Parquet reader no longer assumes all struct values to be null, if all the requested fields are missing in the parquet file. The new default behavior is to read an additional struct field that is present in the file to determine nullness. To restore the previous behavior, set `spark.sql.legacy.parquet.returnNullStructIfAllFieldsMissing` to true" ([Migration Guide](https://spark.apache.org/docs/latest/sql-migration-guide.html)). Até a versão 4.0, portanto, um struct podia ser lido como nulo inteiro só porque as colunas pedidas não estavam no arquivo. Nulabilidade em Spark é área em manutenção ativa, não conceito estável.

**A regra operacional que sai daqui, em três linhas.** Use `nullable=false` como **documentação** do contrato, não como defesa. A defesa é uma verificação explícita que você escreve, e ela é barata:

```python
from pyspark.sql import functions as F

obrigatorias = ["id", "titulo"]
faltando = (df.select([F.count(F.when(F.col(c).isNull(), 1)).alias(c) for c in obrigatorias])
              .collect()[0].asDict())
if any(v > 0 for v in faltando.values()):
    raise ValueError(f"campos obrigatorios com nulo: {faltando}")
```

Ou empurre a restrição para uma camada que a imponha de verdade: formato de tabela transacional com `CHECK` ou `NOT NULL` aplicado na escrita. Parquet num diretório não impõe nada, e os cinco capítulos da bibliografia não têm uma palavra sobre isso (divergência 5 do pré-aula: Delta Lake tem zero ocorrências em Luu 3 e em Chadha 1).

---

### 4. O espectro completo de dado ruim

A divergência 7 do pré-aula resume a situação: três livros, três coberturas parciais, nenhuma utilizável em produção. Luu dá dois pontos do espectro (o default nulifica a linha, `failFast` levanta) e nunca nomeia `PERMISSIVE`. Damji 4 dá os três nomes numa célula de tabela, declara que o default é `PERMISSIVE` e terceiriza a explicação à documentação. Chadha nomeia um modo só, descreve `PERMISSIVE` errado, não diz que é o default, e usa `columnNameOfCorruptRecord` numa receita que não funciona como impressa.

Vamos preencher o espectro inteiro, com o código na mão.

#### Os três modos, pelo que o código faz

O default é `PERMISSIVE`, tanto em [CSV](https://spark.apache.org/docs/latest/sql-data-sources-csv.html) quanto em [JSON](https://spark.apache.org/docs/latest/sql-data-sources-json.html). Os três modos são implementados num arquivo só, de sessenta linhas, que vale ler inteiro: [`FailureSafeParser.scala`](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/FailureSafeParser.scala).

| Modo | O que acontece com o registro ruim | Coluna de corrompido no schema | Sem a coluna no schema |
|---|---|---|---|
| `PERMISSIVE` (default) | emite a linha com o que deu para parsear, nulos no resto, e o texto cru na coluna de corrompido | recebe a string original | o registro **continua saindo**, como resultado parcial ou linha toda nula, e o texto cru é **descartado** |
| `DROPMALFORMED` | `Iterator.empty`, a linha desaparece | irrelevante | irrelevante |
| `FAILFAST` | levanta `MALFORMED_RECORD_IN_PARSING`, com o conteúdo do registro na mensagem | irrelevante | irrelevante |

Três coisas importantes saem daí, e duas contradizem material publicado.

**Primeira: `PERMISSIVE` não nulifica mais a linha inteira, quando dá para salvar parte.** O branch é `val partialResults = e.partialResults(); if (partialResults.nonEmpty) ...`. Em JSON isso é governado por `spark.sql.json.enablePartialResults`, interna, default **`true`** desde 3.4.0, "enables partial results for structs, maps, and arrays in JSON when one or more fields do not match the schema" ([`SQLConf.scala`](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L6175-L6182)). Em CSV, o `convert` do parser popula o que consegue e guarda a exceção para o fim ([`UnivocityParser.scala`](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/csv/UnivocityParser.scala#L392-L447)). O Listing 3-15 do Luu, com cinco linhas **inteiramente** nulas por causa de um `BooleanType` declarado em cima de texto, descreve comportamento de Spark 3.0 e não é o que uma versão atual produz nas mesmas condições, ao menos para os campos que não têm problema.

**Segunda: `DROPMALFORMED` é a opção que perde dado em silêncio.** Uma linha, `Iterator.empty`. Sem contador, sem log, sem coluna. Em pipeline de produção isso é a diferença entre um relatório errado e um alerta. Só use quando o dado descartado for provadamente irrelevante, e mesmo aí prefira `PERMISSIVE` mais filtro, porque assim você **conta** o que jogou fora.

**Terceira: `FAILFAST` levanta na ação, não na leitura.** O pré-aula já tinha notado isso no Luu, sem que o livro chamasse atenção: o erro aparece como `ERROR Executor: Exception in task 0.0 in stage 3.0`, ou seja no executor, durante a ação. É consequência direta da avaliação preguiçosa. A mensagem do 4.2.0 é `MALFORMED_RECORD_IN_PARSING`, "Malformed records are detected in record parsing: `<badRecord>`. Parse Mode: `<failFastMode>`. To process malformed records as null result, try setting the option 'mode' as 'PERMISSIVE'", com subclasses `CANNOT_PARSE_JSON_ARRAYS_AS_STRUCTS` e `CANNOT_PARSE_STRING_AS_DATATYPE` ([`error-conditions.json`](https://github.com/apache/spark/blob/v4.2.0/common/utils/src/main/resources/error/error-conditions.json)).

#### `_corrupt_record` e a regra que quebra a receita do Chadha

O nome default da coluna vem da configuração `spark.sql.columnNameOfCorruptRecord`, cujo valor é **`_corrupt_record`** desde a versão 1.2.0, descrito como "The name of internal column for storing raw/un-parsed JSON and CSV records that fail to parse" ([Runtime SQL Configuration](https://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration)). A opção `columnNameOfCorruptRecord`, por leitura, sobrescreve a configuração de sessão.

E agora a regra que o pré-aula levantou como defeito silencioso da receita 2 do Chadha ([R3-6]). A documentação oficial é explícita: "To keep corrupt records, an user can set a string type field named `columnNameOfCorruptRecord` in an user-defined schema. **If a schema does not have the field, it drops corrupt records during parsing.** When inferring a schema, it implicitly adds a `columnNameOfCorruptRecord` field in an output schema" ([JSON Data Source Options](https://spark.apache.org/docs/latest/sql-data-sources-json.html), texto idêntico em CSV e em XML).

O código mostra o porquê, em duas linhas do `FailureSafeParser`:

```scala
private val corruptFieldIndex = schema.getFieldIndex(columnNameOfCorruptRecord)
private val actualSchema = StructType(schema.filterNot(_.name == columnNameOfCorruptRecord))
```

Se o índice não existe, o ramo usado é `(row, _) => row.getOrElse(nullResult)`: o `badRecord` é **ignorado**. E o `actualSchema` exclui a coluna de corrompido para que o parser não tente ler um campo com esse nome no arquivo. Ou seja, o comportamento não é opcional nem configurável: **a coluna precisa estar no schema declarado**, senão ela não existe para receber nada. A receita do Chadha, que passa `columnNameOfCorruptRecord = "corrupt_record"` sobre um `json_schema` que não tem esse campo, não entrega a coluna que promete, exatamente como o pré-aula registrou.

Duas restrições a mais, que nenhum dos três livros tem:

- **O tipo tem de ser `STRING` nulável.** Erro: `INVALID_CORRUPT_RECORD_TYPE`, "The column `<columnName>` for corrupt records must have the nullable STRING type, but got `<actualType>`" (SQLSTATE 42804).
- **Não dá para consultar só essa coluna direto do arquivo.** O erro é `UNSUPPORTED_FEATURE.QUERY_ONLY_CORRUPT_RECORD_COLUMN`, e a mensagem dá o exemplo e a saída: "Queries from raw JSON/CSV/XML files are disallowed when the referenced columns only include the internal corrupt record column (named `_corrupt_record` by default). For example: `spark.read.schema(schema).json(file).filter($"_corrupt_record".isNotNull).count()` ... Instead, you can cache or save the parsed results and then send the same query." Isso é uma armadilha direta para quem quer medir taxa de rejeição, porque a primeira coisa que se tenta escrever é justamente esse `filter(...).count()`.

Duas armadilhas específicas de CSV, e a segunda é uma contradição entre documentação e código que eu não consegui arbitrar sem rodar:

- **A poda de colunas muda o que conta como corrompido.** A documentação oficial diz: "Note that Spark tries to parse only required columns in CSV under column pruning. **Therefore, corrupt records can be different based on required set of fields.** This behavior can be controlled by `spark.sql.csv.parser.columnPruning.enabled` (enabled by default)" ([CSV Data Source Options](https://spark.apache.org/docs/latest/sql-data-sources-csv.html)). Consequência prática para o desenho da subseção seguinte: **a taxa de rejeição depende da projeção**. Dois jobs sobre o mesmo arquivo, um lendo três colunas e outro lendo trinta, medem taxas diferentes. Isso precisa estar escrito no runbook, ou a métrica engana.
- **Linha com número errado de campos: corrompida ou não?** A documentação diz que não: "A record with less/more tokens than schema is not a corrupted record to CSV. When it meets a record having fewer tokens than the length of the schema, sets `null` to extra fields. When the record has more tokens than the length of the schema, it drops extra tokens." O código do 4.2.0 diz o contrário, com comentário próprio: `if (tokens.length != parsedSchema.length)` produz um `LazyBadRecordCauseWrapper(malformedCSVRecordError(...))`, precedido de "**If the number of tokens doesn't match the schema, we should treat it as a malformed record.** However, we still have chance to parse some of the tokens" ([`UnivocityParser.scala`](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/csv/UnivocityParser.scala#L406-L413)). Se o código estiver certo, `DROPMALFORMED` sobre CSV com uma vírgula extra **apaga a linha**, o que a documentação nega. Marco como dúvida aberta e ponho na lista do fim.

#### `badRecordsPath`: é Databricks, não é Spark aberto

Resposta curta: **não existe no Apache Spark**. Três evidências, em ordem de força:

1. A opção **não aparece** na tabela oficial de opções de [CSV](https://spark.apache.org/docs/latest/sql-data-sources-csv.html) nem na de [JSON](https://spark.apache.org/docs/latest/sql-data-sources-json.html) do 4.2.0.
2. Busca de código no repositório `apache/spark` por `badRecordsPath` retorna **zero resultados**, enquanto o termo de controle `columnNameOfCorruptRecord` retorna resultados em `docs/sql-data-sources-csv.md`, `docs/sql-data-sources-json.md`, `docs/sql-data-sources-xml.md` e no cliente Python. Fiz a busca com `gh search code` nesta sessão.
3. Quem documenta é a Databricks, como recurso de plataforma: "unified interface for both corrupt records and files", com a ressalva de que "using the `badRecordsPath` option in a file-based data source has a few important limitations: **it is non-transactional and can lead to inconsistent results**" ([Handle bad records and files, Azure Databricks](https://learn.microsoft.com/en-us/azure/databricks/ingestion/bad-records)).

Isto entra direto no bloco [Spark contra Databricks](01-pre-aula.md#spark-contra-databricks) do pré-aula. E a ressalva de não transacionalidade é a razão técnica para não construir a quarentena de produção em cima dela mesmo em quem roda Databricks: um caminho de registros ruins escrito fora de transação pode divergir do resultado que o job entregou.

#### O desenho de produção: quarentena e taxa de rejeição

O que o espectro do Spark aberto oferece é matéria-prima, não solução. A solução tem quatro peças e nenhuma delas é uma opção do leitor.

**Peça 1: schema declarado com a coluna de corrompido dentro.** Sem isso, nada do resto funciona.

```python
CONTRATO = (
    "id INT NOT NULL, "
    "ocorrido_em TIMESTAMP, "
    "valor DECIMAL(12,2), "
    "origem STRING, "
    "_corrupt_record STRING"          # obrigatória no schema, senão o texto cru se perde
)

bruto = (spark.read
         .schema(CONTRATO)
         .option("mode", "PERMISSIVE")
         .option("columnNameOfCorruptRecord", "_corrupt_record")
         .json(caminho_entrada))
```

**Peça 2: materializar antes de perguntar.** Por causa de `QUERY_ONLY_CORRUPT_RECORD_COLUMN`, e também para não pagar duas varreduras, materialize uma vez. `cache()` serve para volume que cabe; para volume que não cabe, escreva o bruto anotado em Parquet e leia de volta.

```python
bruto = bruto.withColumn("_arquivo", F.input_file_name()) \
             .withColumn("_lido_em", F.current_timestamp())
bruto.cache()

ruins = bruto.filter(F.col("_corrupt_record").isNotNull())
bons  = bruto.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")
```

`input_file_name()` é a peça que transforma "temos 12 mil registros ruins" em "o arquivo X da origem Y está quebrado", que é a única forma de a quarentena virar conserto e não cemitério.

**Peça 3: a quarentena é uma tabela, não um diretório de sobras.** Particionada por data de processamento e por origem, com o texto cru preservado, o motivo e a versão do contrato que rejeitou. Versão do contrato importa porque metade das rejeições, na prática, é o contrato que envelheceu, não o dado que apodreceu.

```python
(ruins.select("_arquivo", "_lido_em", "_corrupt_record")
      .withColumn("contrato_versao", F.lit("v3"))
      .write.mode("append")
      .partitionBy("_lido_em")
      .parquet(caminho_quarentena))
```

**Peça 4: taxa de rejeição como métrica com limiar, não como log.** Dois números por execução, `total` e `rejeitados`, contados na mesma passada, e uma decisão automática:

```python
c = bruto.select(
        F.count("*").alias("total"),
        F.count("_corrupt_record").alias("rejeitados")).collect()[0]
taxa = c["rejeitados"] / c["total"] if c["total"] else 0.0

if taxa > 0.01:                     # limiar do contrato, não número mágico
    raise ValueError(f"taxa de rejeição {taxa:.4f} acima do limiar; execução abortada")
```

Quatro observações sobre a métrica, e são elas que separam medida de teatro:

- **Meça a taxa, não a contagem.** Contagem absoluta sobe com o volume e não diz nada. Taxa é comparável entre dias.
- **O limiar é parte do contrato.** Fonte que sempre teve 0,3% de sujeira não deve derrubar o pipeline em 0,4%. Fonte que sempre teve zero deve derrubar em qualquer coisa maior que zero.
- **Registre a taxa em série temporal.** A informação útil raramente é o valor de hoje, é a **derivada**: salto de 0,1% para 2% costuma ser mudança de schema na origem, não dado ruim.
- **Fixe a projeção da medição.** Pelo aviso da documentação de CSV sobre poda de colunas, medir com `select` diferente dá número diferente. Meça sempre com o contrato completo.

Quando usar cada modo, em uma tabela:

| Situação | Modo | Por que |
|---|---|---|
| Ingestão de origem externa que você não controla | `PERMISSIVE` com quarentena | preserva evidência e permite medir |
| Contrato interno entre times, qualquer linha ruim é bug | `FAILFAST` | falhar rápido é mais barato que investigar depois |
| Reprocessamento histórico, sujeira conhecida e tolerada | `PERMISSIVE` com limiar folgado | mantém a contagem do que foi tolerado |
| Qualquer situação | `DROPMALFORMED` | quase nunca; perde dado sem contar |

E a fronteira que a aula 01 já tinha levantado, agora com resposta: **o Spark aberto entrega o mecanismo de captura e nada mais**. Não há destino de registro ruim, não há métrica, não há limiar, não há histórico, não há evolução de schema. Onde termina o Spark e começa o framework de qualidade é exatamente aqui: nas quatro peças acima, que são todas código seu, ou de um Great Expectations, Soda, Deequ, ou de uma tabela transacional com constraint.

---

### 5. O modo ANSI, default desde o Spark 4.0

O pré-aula registra a tese "Comportamento de cast e overflow antes do modo ANSI" como confirmada no Luu 3.2, que declara o default de nulificar todas as colunas da linha e oferece `failFast` como única alternativa, e ausente nos outros quatro capítulos. E anota, na tabela de vocabulário, que `spark.sql.ansi.enabled` "virou default no 4.0". É a defasagem mais consequente da bibliografia inteira, porque não é uma lacuna: é uma afirmação que hoje está **errada** para quem roda uma versão atual.

#### O que mudou, e onde está escrito

A frase da fonte primária: "Since Spark 4.0, `spark.sql.ansi.enabled` is on by default. To restore the previous behavior, set `spark.sql.ansi.enabled` to false or `SPARK_ANSI_SQL_MODE` to false" ([Migration Guide, Upgrading from Spark SQL 3.5 to 4.0](https://spark.apache.org/docs/latest/sql-migration-guide.html)). O ticket é [SPARK-44444](https://issues.apache.org/jira/browse/SPARK-44444), "Use ANSI SQL mode by default", listado nos destaques do [Spark 4.0.0](https://spark.apache.org/releases/spark-release-4-0-0.html).

A configuração existe desde 3.0.0, o que confunde quem só olha a coluna "Since Version": o que mudou no 4.0 foi o **default**, não a chave. Na [Runtime SQL Configuration](https://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration), `spark.sql.ansi.enabled` aparece com default `true` e "Since Version 3.0.0". No código, o default é calculado assim: `.createWithDefault(!sys.env.get("SPARK_ANSI_SQL_MODE").contains("false"))` ([`SQLConf.scala`](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L5180-L5190)), o que confirma a variável de ambiente citada no guia de migração.

A página de referência abre com a moldura: "In Spark SQL, there are two options to comply with the SQL standard: `spark.sql.ansi.enabled` and `spark.sql.storeAssignmentPolicy`. By default, `spark.sql.ansi.enabled` is `true` ... By default, `spark.sql.storeAssignmentPolicy` is `ANSI`" ([ANSI Compliance](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html)).

#### O que passou a falhar

| Operação | Spark 3.x, default | Spark 4.x, default (ANSI) | Escape |
|---|---|---|---|
| `2147483647 + 1` | `-2147483648`, wraparound | `SparkArithmeticException [ARITHMETIC_OVERFLOW]` | `try_add` |
| soma de `decimal` que estoura | `null` | exceção | `try_sum` |
| `CAST('a' AS INT)` | `null` | `SparkNumberFormatException [CAST_INVALID_INPUT]` | `try_cast` |
| divisão por zero | `null` | `[DIVIDE_BY_ZERO]` | `try_divide`, `try_mod` |
| `abs(-2147483648)` | `-2147483648` | `[ARITHMETIC_OVERFLOW]` | reescrever com tipo maior |
| `element_at`, `elt`, `array_col[i]` com índice inválido | `null` | `ArrayIndexOutOfBoundsException` | `try_element_at` |
| `to_date`, `to_timestamp`, `unix_timestamp` com string inválida | `null` | exceção | `try_to_date`, `try_to_timestamp` |
| `parse_url` com URL inválida | `null` | `IllegalArgumentException` | `try_parse_url` |
| `make_date`, `make_timestamp`, `make_interval` inválidos | `null` | exceção | `try_make_*` |
| `next_day` com dia da semana inválido | `null` | `IllegalArgumentException` | filtrar antes |
| `INSERT` de `string` em coluna `int` | permitido | recusado pela política `ANSI` de store assignment | `storeAssignmentPolicy=legacy` |
| valor fora da faixa em `INSERT` numérico | truncava ou virava nulo | erro de overflow | `try_cast` explícito |

Fonte de toda a coluna do meio e da lista de funções: [ANSI Compliance](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html), seções "Arithmetic Operations", "Cast", "Functions with different behaviors", "SQL Operators" e "Useful Functions for ANSI Mode". Vale ler a mensagem de erro completa que a página imprime, porque ela ensina o escape na própria exceção: "integer overflow. **Use 'try_add' to tolerate overflow and return NULL instead.** If necessary set spark.sql.ansi.enabled to "false" to bypass this error."

Um detalhe que a página nomeia e vale registrar: o comportamento não ANSI de overflow **não é uniforme**. "If `spark.sql.ansi.enabled` is false, then the decimal type will produce null values and other numeric types will behave in the same way as the corresponding operation in a Java/Scala program", ou seja `decimal` dava nulo e `int` dava número negativo. Quem herdou pipeline de Spark 3 tem os dois padrões de sujeira convivendo no mesmo dado.

#### O que ANSI **não** governa, e é onde o Luu confunde as camadas

Esta é a distinção que fecha a subseção 4 com a 5, e não achei documentação oficial que a enuncie, então declaro como **inferência minha a partir do código**, com endereço.

São **duas camadas independentes**:

| Camada | Quem decide | Governado por |
|---|---|---|
| Parsing de texto para tipo, na fonte CSV, JSON e XML | o parser da fonte | opção **`mode`**, mais `columnNameOfCorruptRecord` |
| Avaliação de expressão, cast, aritmética, função | o Catalyst e o codegen | **`spark.sql.ansi.enabled`** |

A evidência: em [`UnivocityParser.scala`](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/csv/UnivocityParser.scala) não há uma única referência a ANSI; a falha de conversão vira `BadRecordException`, que o `FailureSafeParser` resolve pelo `mode`. Desligar ANSI não faz CSV malformado voltar a virar nulo silencioso, e ligar ANSI não faz `PERMISSIVE` levantar exceção.

O Listing 3-15 do Luu mistura as duas: ele declara `actor_name` como `BooleanType` num **JSON**, o que é falha de **parsing**, e apresenta `mode=failFast` como a alternativa ao "default de nulificar", que é vocabulário de **cast**. As duas coisas parecem uma só naquele exemplo porque em 2021 as duas devolviam nulo. Hoje o exemplo continua sendo governado por `mode`, e o comportamento default mudou por outro motivo (resultado parcial em JSON desde 3.4), não por causa do ANSI.

#### Como voltar atrás, e por que pensar duas vezes

```text
spark.sql.ansi.enabled = false                 # volta ao dialeto Hive-compatível
spark.sql.storeAssignmentPolicy = LEGACY       # volta à coerção solta de INSERT
SPARK_ANSI_SQL_MODE=false                      # variável de ambiente, efeito global
```

Também há chaves adjacentes que só agem com ANSI ligado, todas com default `false`: `spark.sql.ansi.enforceReservedKeywords` (3.3.0), `spark.sql.ansi.doubleQuotedIdentifiers` (3.4.0), `spark.sql.ansi.relationPrecedence` (3.4.0), as três na [Runtime SQL Configuration](https://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration).

O argumento contra desligar: ANSI trocou **erro silencioso** por **falha visível**. Um pipeline que devolvia nulo em cast inválido produzia relatório errado sem ninguém saber; o mesmo pipeline em ANSI para de rodar. Desligar para "não quebrar a migração" é escolher voltar ao erro silencioso. O caminho melhor é cirúrgico: identifique as expressões que falham, troque por `try_*` onde nulo é a semântica desejada de fato, e trate o resto como bug encontrado. Desligar global é dívida que não vence sozinha.

Uma nota **fora da bibliografia** sobre alcance: no 4.1, ANSI passou a valer por padrão também para a API pandas sobre Spark ([SPARK-53295](https://issues.apache.org/jira/browse/SPARK-53295), em [Spark 4.1.0](https://spark.apache.org/releases/spark-release-4.1.0.html)), e [SPARK-53348](https://issues.apache.org/jira/browse/SPARK-53348) passou a persistir o valor de ANSI na criação de view, para que a view não mude de semântica conforme a sessão que a consulta. O segundo é o tipo de detalhe que resolve um bug de "a mesma view dá resultado diferente para dois times".

---

### 6. O tipo VARIANT, o que nenhum dos três livros tem

O pré-aula previu essa lacuna e a confirmou nas cinco leituras, por construção: `VARIANT` não pode estar em livro de 2020, 2021 ou do ciclo 3.4. Tudo nesta subseção é, portanto, **fora da bibliografia**.

#### O que é

`VARIANT` é um tipo que guarda valor **semiestruturado** em uma coluna, como par de binários (valor e metadado de chaves), sem schema fixo e sem virar string. Não é `StringType` com JSON dentro, e não é `MapType<String,String>`: os tipos dos escalares são preservados na codificação, e o acesso a um campo não exige parsear o documento inteiro a cada linha.

Cronologia, com fonte:

| Marco | Versão | Ticket |
|---|---|---|
| tipo adicionado | 4.0.0 | [SPARK-45827](https://issues.apache.org/jira/browse/SPARK-45827), "Add VARIANT data type", nos [destaques do 4.0.0](https://spark.apache.org/releases/spark-release-4-0-0.html) |
| ligado por default, GA | 4.1.0 | [SPARK-54454](https://issues.apache.org/jira/browse/SPARK-54454), "Enable VARIANT type by default (VARIANT type GA)", nos [destaques do 4.1.0](https://spark.apache.org/releases/spark-release-4.1.0.html) |
| suporte em varredura de CSV | 4.1.0 | [SPARK-51298](https://issues.apache.org/jira/browse/SPARK-51298) |
| suporte em varredura de XML | 4.1.0 | [SPARK-51503](https://issues.apache.org/jira/browse/SPARK-51503) |
| tipo lógico Variant em Parquet, leitura e anotação | 4.1.0 | [SPARK-54410](https://issues.apache.org/jira/browse/SPARK-54410), [SPARK-54306](https://issues.apache.org/jira/browse/SPARK-54306) |
| operador de dois-pontos para acessar campo | 4.1.0 | [SPARK-52494](https://issues.apache.org/jira/browse/SPARK-52494) |

Em PySpark, a classe é `VariantType`, com `.. versionadded:: 4.0.0` ([`types.py`](https://github.com/apache/spark/blob/v4.2.0/python/pyspark/sql/types.py#L1860-L1866), [página de API](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.VariantType.html)), e o valor materializado é `VariantVal`.

**Lacuna de documentação que eu achei e vale reportar:** a página [Data Types](https://spark.apache.org/docs/latest/sql-ref-datatypes.html) do 4.2.0 **não lista VARIANT**. Conferi o HTML da página: as duas únicas ocorrências da palavra estão na navegação. Um tipo GA desde o 4.1 fora da referência de tipos é buraco na documentação, não no meu levantamento.

Superfície de uso, com as funções que existem no 4.2.0:

```python
from pyspark.sql import functions as F

# 1. transformar string JSON em VARIANT
df = df.withColumn("payload", F.parse_json("payload_txt"))     # try_parse_json tolera inválido

# 2. ler direto para uma coluna VARIANT, sem declarar schema
bruto = (spark.read.format("json")
         .option("singleVariantColumn", "payload")
         .load(caminho))

# 3. extrair com tipo
df.select(
    F.variant_get("payload", "$.cliente.id", "int").alias("cliente_id"),
    F.try_variant_get("payload", "$.valor", "decimal(12,2)").alias("valor"),
)

# 4. descobrir a forma real do dado
df.select(F.schema_of_variant_agg("payload")).show(truncate=False)
```

Em SQL, o operador de dois-pontos deixa o acesso curto: `SELECT payload:cliente.id FROM eventos`. O catálogo de funções inclui ainda `is_variant_null`, `is_valid_variant`, `to_variant_object`, `schema_of_variant` e a função de tabela `variant_explode` (todas na [referência de funções](https://spark.apache.org/docs/latest/sql-ref-functions-builtin.html)).

Sobre `singleVariantColumn`: a opção **existe no Apache Spark**, não é só Databricks. Ela não está na tabela de opções de JSON da documentação (outra lacuna), mas está no código, com comentário "as a single VARIANT type column in the table with the given column name" ([`JSONOptions.scala`](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/json/JSONOptions.scala)), e a regra de uso está codificada como erro no 4.2.0: `INVALID_SINGLE_VARIANT_COLUMN`, "User specified schema `<schema>` is invalid when the `singleVariantColumn` option is enabled. **The schema must either be a variant field, or a variant field plus a corrupt column field**". Note a costura elegante com a subseção 4: a coluna de corrompido continua valendo em modo VARIANT.

#### Shredding

**Shredding** é a otimização de armazenamento: em vez de guardar todo o documento como um blob binário só, o escritor Parquet **extrai os campos frequentes para colunas tipadas de verdade**, dentro do mesmo arquivo, e mantém o resto no blob. Consulta que toca apenas campos extraídos passa a ler colunas Parquet normais, com estatística de min e max no rodapé, o que habilita **pushdown de filtro e poda de coluna** sobre dado que nominalmente não tem schema. Sem shredding, filtrar por um campo do VARIANT exige ler o blob inteiro de cada linha.

Estado no 4.2.0, pelo código, porque não achei página oficial que descreva o recurso. Todas as chaves abaixo são `.internal()`, o que explica por que não aparecem na [Runtime SQL Configuration](https://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration):

| Chave | Default no v4.2.0 | Desde | O que faz |
|---|---|---|---|
| `spark.sql.variant.allowReadingShredded` | `true` | 4.0.0 | leitor Parquet aceita variant shredded e não shredded |
| `spark.sql.variant.writeShredding.enabled` | `true` | 4.0.0 | escritor Parquet pode escrever variant shredded |
| `spark.sql.variant.pushVariantIntoScan` | `true` | 4.0.0 | troca o variant do schema de varredura por um struct com os campos pedidos |
| `spark.sql.variant.shredding.maxSchemaWidth` | `300` | 4.1.0 | máximo de campos extraídos ao inferir o schema de shredding |
| `spark.sql.variant.shredding.maxSchemaDepth` | `50` | 4.1.0 | profundidade máxima percorrida na inferência |
| `spark.sql.parquet.variant.annotateLogicalType.enabled` | `true` | 4.1.0 | anota os grupos como tipo lógico variant do Parquet |
| `spark.sql.variant.allowDuplicateKeys` | `false` | 4.0.0 | com `false`, chave duplicada no JSON de origem levanta erro |
| `spark.sql.variant.forceShreddingSchemaForTest` | `""` | 4.0.0 | "FOR INTERNAL TESTING ONLY", como diz o próprio doc |

Fonte das oito linhas: [`SQLConf.scala` no tag v4.2.0](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala). O que mudou de 4.0 para 4.1 é a peça que faltava para o recurso ser usável: [SPARK-53659](https://issues.apache.org/jira/browse/SPARK-53659), "Infer Variant shredding schema in parquet writer", ou seja o escritor passou a **decidir sozinho** o que extrair. Antes, a única forma de ligar shredding era passar o schema à mão por configuração de teste, o que não é API de usuário. Duas consequências honestas: você **não escolhe** quais campos são extraídos, e não há chave pública documentada para controlar isso.

Limite duro: valor e metadado de um VARIANT não podem passar de **128 MiB** no Apache Spark. O comentário do código é explícito, "Both variant value and variant metadata need to be no longer than 128MiB", e o erro é `VARIANT_CONSTRUCTOR_SIZE_LIMIT` ([`VariantUtil.java`](https://github.com/apache/spark/blob/v4.2.0/common/variant/src/main/java/org/apache/spark/types/variant/VariantUtil.java#L142-L145)). Atenção a uma confusão fácil: a Databricks documenta 16 MiB, que é o limite da plataforma dela, e no Spark aberto 16 MiB é o valor usado só em teste. Os dois números circulam como se fossem o mesmo.

#### Quando VARIANT ganha, e o trade-off honesto

| Situação | Schema fixo | VARIANT |
|---|---|---|
| Contrato estável, poucas colunas, consumidor de BI | **ganha** | perde, complica sem benefício |
| Payload de webhook ou API que muda sem aviso | quebra a cada mudança | **ganha**, ingestão não para |
| Milhares de chaves possíveis, dezenas por registro | struct largo e esparso, pesado | **ganha** |
| Camada bruta de um lake, "guardar primeiro, decidir depois" | força decisão precoce | **ganha** |
| Camada curada, servida a analista | **ganha** | perde |
| Filtro por campo interno em volume grande | pushdown normal | depende de shredding, e você não controla |
| Detectar mudança de origem no dia em que acontece | o job **falha**, o que é o sinal | o job **passa**, e o sinal se perde |

O trade-off, sem adoçar:

- **VARIANT não elimina o contrato, muda o lugar dele.** Sai da ingestão e entra na consulta. `variant_get(..., "int")` levanta `INVALID_VARIANT_CAST` quando o valor não converte, e a mensagem manda usar `try_variant_get`. Ou seja, você trocou "falhar na leitura, num lugar" por "falhar em cada consulta, em muitos lugares".
- **Você perde a falha útil.** O maior valor de schema declarado é avisar no dia em que a origem mudou. VARIANT engole a mudança em silêncio, e o custo aparece semanas depois, num dashboard.
- **Nulabilidade e tipo por campo deixam de existir** como declaração. Nada para o Catalyst usar, nada para documentar, nada para um contrato de dados apontar.
- **Consumidor a jusante precisa entender VARIANT.** Ferramenta que lê Parquet cru pode ver um grupo binário estranho, ou depender da anotação de tipo lógico do 4.1.
- **Performance depende de shredding, que é opaco.** Ligado por default no 4.2.0, com inferência automática e teto de 300 campos e 50 níveis, e sem API pública para dirigir. Funciona, e você não manda nele.
- **Chave duplicada levanta erro por default** (`allowDuplicateKeys=false`), o que é bom, e é uma diferença de comportamento em relação a guardar a mesma coisa como string.

A recomendação que eu levaria para produção: VARIANT na **camada bruta**, para não perder ingestão por mudança de origem, com projeção explícita e tipada para a camada curada, e um teste que compare `schema_of_variant_agg` de hoje com o de ontem para detectar deriva. Assim você fica com o benefício (não parar) sem perder o sinal (saber que mudou).

---

### 7. Os tipos que a bibliografia não lista

O pré-aula registra que a tese "lista de tipos sem VARIANT e sem os tipos geoespaciais" foi confirmada nas cinco leituras, e que o buraco é maior que o palpite: falta também `TimestampNTZType` (3.4) e os tipos de intervalo (3.2). A tabela abaixo fecha a conta, com a versão de cada um. Tudo aqui é **fora da bibliografia**, e a fonte é a página [Data Types](https://spark.apache.org/docs/latest/sql-ref-datatypes.html) do 4.2.0, salvo onde indico outra.

| Tipo | Desde | O que resolve | Estado na bibliografia |
|---|---|---|---|
| `TimestampType` (`TIMESTAMP_LTZ`) | sempre | instante absoluto, interpretado no fuso da sessão | Damji 3 lista, sem falar de fuso |
| `TimestampNTZType` (`TIMESTAMP_NTZ`) | 3.4 | data e hora **sem fuso**, sem reinterpretação | ausente nas cinco |
| `TimeType(precision)` | **4.1** | hora do dia sem data, precisão de 0 a 6 casas | ausente, é posterior a tudo |
| `YearMonthIntervalType(start, end)` | 3.2 | intervalo de ano e mês, com campos declarados | ausente |
| `DayTimeIntervalType(start, end)` | 3.2 | intervalo de dia a segundo | ausente |
| `CharType(n)` e `VarcharType(n)` | 3.1 | string com limite, só em schema de tabela | ausente; o pré-aula anotou a falta |
| `VariantType` | 4.0, GA 4.1 | semiestruturado sem schema | ausente, ver subseção 6 |
| `GeometryType(srid)` | **4.2** | geometria em plano cartesiano | ausente |
| `GeographyType(srid)` | **4.2** | geometria sobre esfera ou elipsoide terrestre | ausente |

**`TimestampNTZType` é o mais consequente para trabalho de todo dia**, e a razão é operacional. `TimestampType` é *timestamp with local time zone*: o mesmo valor lido em duas sessões com `spark.sql.session.timeZone` diferente **é a mesma coisa** e **imprime diferente**. `TimestampNTZType` não tem fuso, então não é reinterpretado. A chave que decide qual dos dois `TIMESTAMP` significa é `spark.sql.timestampType`, com default `TIMESTAMP_LTZ` desde 3.4.0, e a descrição diz que ela vale para "SQL DDL, Cast clause, type literal **and the schema inference of data sources**" ([Runtime SQL Configuration](https://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration)). Há ainda `spark.sql.parquet.inferTimestampNTZ.enabled`, default `true` desde 3.4.0: coluna de timestamp em Parquet anotada com `isAdjustedToUTC = false` é inferida como `TIMESTAMP_NTZ`. Ou seja, **o mesmo arquivo pode render tipo diferente conforme a versão do Spark que escreveu a anotação**, e isso é exatamente o tipo de coisa que uma ingestão sem schema declarado descobre em produção.

**`TimeType` é a novidade de 4.1 que o 4.2 espalhou pelos formatos.** A classe traz `@since 4.1.0` e valida a precisão entre 0 e 6 ([`TimeType.scala`](https://github.com/apache/spark/blob/v4.2.0/sql/api/src/main/scala/org/apache/spark/sql/types/TimeType.scala)). O 4.2.0 tem uma seção inteira chamada "TIME type across file formats": serde de JSON com `from_json` e `to_json` ([SPARK-54451](https://issues.apache.org/jira/browse/SPARK-54451)), XML ([SPARK-54461](https://issues.apache.org/jira/browse/SPARK-54461)), CSV ([SPARK-54463](https://issues.apache.org/jira/browse/SPARK-54463)), leitura e escrita em ORC ([SPARK-54472](https://issues.apache.org/jira/browse/SPARK-54472)) e em Avro ([SPARK-54473](https://issues.apache.org/jira/browse/SPARK-54473)), tudo nos [release notes do 4.2.0](https://spark.apache.org/releases/spark-release-4-2-0.html). Guardar hora de expediente sem inventar uma data deixou de exigir gambiarra.

**Os tipos de intervalo têm campos declarados**, e é isso que os separa do antigo `CalendarIntervalType`, que existia em 2020 e não era exposto ao usuário, como o pré-aula anotou. `YearMonthIntervalType` aceita `MONTH` (0 a 11) e `YEAR` (0 a 178956970); `DayTimeIntervalType` aceita de `DAY` (0 a 106751991) a `SECOND` (0 a 59.999999). A sintaxe SQL é `INTERVAL YEAR TO MONTH`, `INTERVAL DAY TO SECOND`, `INTERVAL HOUR TO MINUTE`. A consequência prática: subtração de dois timestamps agora tem tipo com semântica, não um objeto opaco.

**`CharType` e `VarcharType` só valem em schema de tabela**, e por default o Spark os troca por `STRING`. A chave é `spark.sql.preserveCharVarcharTypeInfo`, default `false`, desde 4.0.0: "When true, Spark does not replace CHAR/VARCHAR types the STRING type, which is the default behavior of Spark 3.0 and earlier versions. This means the length checks for CHAR/VARCHAR types is enforced and CHAR type is also properly padded" ([Runtime SQL Configuration](https://spark.apache.org/docs/latest/configuration.html#runtime-sql-configuration)). Ou seja: declarar `VARCHAR(10)` **não limita nada** por default. É outro caso de declaração sem validação, e fecha o arco da subseção 3.

**Os geoespaciais do 4.2 são o tipo mais novo, e vêm ligados por default.** O destaque é [SPARK-51658](https://issues.apache.org/jira/browse/SPARK-51658), "SPIP: Geospatial types, new GEOMETRY and GEOGRAPHY data types with ST_* functions, WKB/WKT and Parquet I/O, and a full SRID registry, **enabled by default**" ([release notes 4.2.0](https://spark.apache.org/releases/spark-release-4-2-0.html)). A referência dedicada é [Geospatial (Geometry/Geography) Types](https://spark.apache.org/docs/latest/sql-ref-geospatial-types.html), e o que importa para contrato é isto:

- Os tipos seguem a especificação **OGC Simple Feature Access**. `GEOMETRY` é plano cartesiano, para coordenadas projetadas ou locais; `GEOGRAPHY` é geográfico, latitude e longitude, com interpolação de aresta esférica.
- **SRID é obrigatório em SQL**: "In SQL, `GEOMETRY` and `GEOGRAPHY` columns must always be declared with an explicit SRID (or `ANY`)", e "unparameterized `GEOMETRY` or `GEOGRAPHY` (without `(srid)` or `(ANY)`) is not supported in SQL". Então `GEOMETRY(4326)` ou `GEOMETRY(ANY)`, nunca `GEOMETRY`.
- Em runtime, o valor é **WKB** associado a um SRID. Construção por `ST_GeomFromWKB(wkb[, srid])` e `ST_GeogFromWKB(wkb)`, este último com SRID 4326.
- **Limite de persistência que é uma pegadinha de contrato**: "Parquet, Delta, and Iceberg store geometry/geography with a fixed SRID per column. They do not support persisting `GEOMETRY(ANY)` or `GEOGRAPHY(ANY)`; mixed-SRID types exist for in-memory/query use only." Ou seja, `ANY` é tipo de consulta, não de tabela.
- Erros próprios: `ST_INVALID_SRID_VALUE` para SRID inválido e `GEO_ENCODER_SRID_MISMATCH_ERROR` ao inserir valor com SRID diferente do da coluna. Para `GEOGRAPHY`, longitude em [-180, 180] e latitude em [-90, 90].

Fechando com o que isso muda na leitura da bibliografia: das seis fontes embutidas que a Tabela 3-3 do Luu lista, o 4.2.0 tem XML nativo (desde 4.0, [SPARK-44265](https://issues.apache.org/jira/browse/SPARK-44265), o que aposenta o `spark-xml` que o Chadha receita), Avro, e a **Python Data Source API** ([SPARK-44076](https://issues.apache.org/jira/browse/SPARK-44076)) que responde a pergunta que nenhum dos três livros faz, "e se meu formato não estiver na lista". A lista de tipos cresceu em oito entradas desde a edição mais recente da bibliografia. Nenhum dos três livros está errado sobre o que escreveu; os três estão incompletos de formas que mudam decisão de projeto.

---

### Perguntas que a parte 2 abriu

1. **A documentação de CSV diz que linha com número errado de campos "não é registro corrompido", e o `UnivocityParser` do 4.2.0 diz o oposto, com comentário explícito. Qual das duas vale hoje?** Hipótese: vale o código, e a frase da documentação é resíduo de uma versão antiga. Se estiver certo, `DROPMALFORMED` sobre CSV com uma vírgula extra **apaga a linha**, o que a documentação nega, e isso muda a recomendação de modo para ingestão de CSV de origem externa. Fontes: [CSV Data Source Options](https://spark.apache.org/docs/latest/sql-data-sources-csv.html) contra [`UnivocityParser.scala#L406-L413`](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/csv/UnivocityParser.scala#L406-L413).

2. **Em `PERMISSIVE` sem a coluna de corrompido no schema, o registro ruim é descartado ou sai como linha de nulos?** A documentação diz "it drops corrupt records during parsing"; o `FailureSafeParser` usa `(row, _) => row.getOrElse(nullResult)`, que **emite** o resultado parcial ou uma linha toda nula. Hipótese: o código está certo e o registro sai, o que é pior que descartar, porque entra no dado bom sem marca. Se confirmado, a regra de produção deixa de ser "ponha a coluna para capturar o texto" e passa a ser "ponha a coluna para **conseguir distinguir** o registro ruim".

3. **`spark.read.parquet` sempre dispara um job de uma task para ler o rodapé, ou existe caminho que evita?** Hipótese: sempre dispara, porque `ParquetUtils.inferSchema` passa por `mergeSchemasInParallel` mesmo com um arquivo na lista, e só é evitado quando o schema é declarado ou quando a tabela vem do catálogo com schema persistido. Se for isso, a regra "declarar schema evita o job" precisa ser reescrita como "evita a varredura, e no caso de Parquet evita também o job de rodapé".

4. **`samplingRatio` em modo linha única economiza I/O ou só CPU?** Hipótese: só CPU, porque `Dataset.sample` é filtro sobre a varredura, então todo byte é lido de qualquer forma. Se estiver certo, o conselho do Luu de baixar `samplingRatio` para acelerar carregamento de JSON grande é válido apenas quando o gargalo é parsing, e é enganoso quando o gargalo é object storage, que é o caso comum. E em `multiLine=true` a economia é real porque a amostra é de arquivos inteiros, com risco proporcionalmente maior.

5. **Existe alguma forma de o Spark aberto impor `nullable=false` na leitura de arquivo?** Hipótese: não. `resolveRelation` tem `forceNullable: Boolean = true` como default de parâmetro, sem chave pública para o caminho de lote, então a imposição só pode vir de fora: verificação explícita no código, ou constraint de um formato de tabela transacional. Se a hipótese estiver certa, o benefício "detectar erro cedo" que o Damji atribui a declarar schema **não existe para nulo**, só para tipo e para nome de coluna.

6. **A opção `mode` e `spark.sql.ansi.enabled` são camadas de fato independentes?** Hipótese: sim, são. `mode` governa o parser das fontes de texto e ANSI governa avaliação de expressão; `UnivocityParser` não referencia ANSI em nenhum ponto. Se confirmado, vale como regra de bolso para depuração: falha na leitura investiga-se por `mode`, falha depois de a coluna existir investiga-se por ANSI, e misturar as duas é o erro que o Listing 3-15 do Luu induz.

7. **Há API pública para dirigir o shredding de VARIANT, ou só a inferência automática?** Hipótese: só a inferência, com as chaves internas ligadas por default no 4.2.0 e teto de 300 campos e 50 níveis, mais uma chave declaradamente de teste para forçar o schema. Se for isso, quem depende de pushdown sobre um campo específico do VARIANT não tem garantia contratual de que ele será extraído, e a recomendação passa a ser projetar o campo crítico para coluna tipada de verdade na camada curada.

8. **Onde a taxa de rejeição deve ser calculada para não custar uma varredura extra, dado o `QUERY_ONLY_CORRUPT_RECORD_COLUMN`?** Hipótese: no mesmo `select` de agregação sobre o DataFrame já materializado (`count("*")` e `count("_corrupt_record")` juntos, depois de `cache()` ou de escrever o bruto anotado), porque a alternativa de duas contagens separadas paga duas passadas e a alternativa de acumulador não sobrevive a reexecução de task especulativa. Se houver caminho melhor, é o que eu quero saber: essa métrica é o único guarda-corpo que sobra depois que o Spark entrega a captura e nada mais.
---

## Parte 3 - Formatos, layout e persistência

Esta é a parte que paga a dívida mais concreta da leitura. Três dos cinco capítulos recomendam Parquet (Damji 3, Luu 3.2 e Damji 4), nenhum dos cinco descreve o layout do arquivo, e um deles descreve o layout errado. O pré-aula registra na [divergência 8](01-pre-aula.md#divergências-entre-os-livros) que o Luu afirma que Parquet guarda cada coluna num arquivo separado, e classifica isso como verificável e falso. Também registra, na [divergência 5](01-pre-aula.md#divergências-entre-os-livros), que formato de tabela transacional é um silêncio de cinco capítulos: quem lê a bibliografia conclui que persistir Parquet num diretório resolve, e sai sem uma palavra sobre atomicidade de `overwrite`, escrita concorrente ou consistência de leitura durante escrita.

O eixo desta parte é um só: **layout de arquivo é decisão de custo, e o custo aparece na leitura, meses depois de você escrever**. Toda seção aqui responde a uma pergunta de dinheiro.

Versão de referência: Apache Spark **4.2.0**, de 14/07/2026. Toda página de documentação citada como `docs/latest` foi lida na versão 4.2.0 e o próprio HTML confirma o rótulo.

---

### 1. Como o Parquet realmente se organiza

#### 1.1 O que o livro diz, e o que está no disco

O pré-aula registra a frase do Luu 3.2 e o julgamento: "Parquet stores each column's data in a separate file", e o registro anota que o benefício descrito é real e o mecanismo descrito é falso. Registra também que o Damji 4 e o Chadha chamam o formato de colunar sem dizer o layout, e que o Damji 4 acerta o efeito (ler menos coluna, ler menos byte) sem descrever a causa. O Damji 4 chega mais perto que os outros dois: o pré-aula anota que ele descreve o diretório Parquet e diz que o metadado fica no **footer**, com versão do formato, schema e dado de coluna.

O layout real, de fora para dentro:

| Nível | O que é | Onde vive |
|---|---|---|
| **Arquivo** | começa e termina com o número mágico `PAR1` | um arquivo `.parquet` |
| **Grupo de linhas** (*row group*) | corte horizontal do dataset: um bloco de linhas, com todas as colunas | N por arquivo |
| **Pedaço de coluna** (*column chunk*) | os valores de **uma** coluna dentro de **um** grupo de linhas, contíguos no arquivo | um por coluna por grupo de linhas |
| **Página** (*page*) | unidade de codificação e de compressão dentro do pedaço de coluna | muitas por pedaço de coluna |
| **Rodapé** (*footer*) | `FileMetaData`: schema, lista de grupos de linhas, offset e tamanho de cada pedaço de coluna, e estatística | uma vez, no fim do arquivo |

A documentação do formato é explícita sobre a ordem e o motivo: "File metadata is written after the data to allow for single pass writing", e "The file metadata contains the locations of all the column chunk start locations". O protocolo de leitura vem na frase seguinte: "Readers are expected to first read the file metadata to find all the column chunks they are interested in. The columns chunks should then be read sequentially." Fonte: [Parquet File Format](https://parquet.apache.org/docs/file-format/).

**A concessão honesta, que fortalece a correção em vez de enfraquecê-la.** A mesma página do formato diz: "The format is explicitly designed to separate the metadata from the data. This allows splitting columns into multiple files, as well as having a single metadata file reference multiple parquet files." E o Thrift do formato confirma: o struct `ColumnChunk` tem um campo `file_path`, documentado como "File where column data is stored. If not set, assumed to be same file as metadata" ([parquet.thrift](https://raw.githubusercontent.com/apache/parquet-format/master/src/main/thrift/parquet.thrift)). Ou seja: a **especificação permite** a variante que o Luu descreve. O que não existe é escritor mainstream que faça isso, e o Spark não faz. A frase do livro está errada como descrição do que você tem no disco depois de um `df.write.parquet(...)`, e é essa a descrição que interessa. Vale como pergunta de aula, porque a resposta separa "o livro inventou" de "o livro citou uma possibilidade do padrão como se fosse o comportamento".

**Tamanhos que o próprio projeto recomenda:** grupo de linhas de **512 MB a 1 GB**, com a nota de que o bloco do HDFS deve acompanhar, e a configuração ideal descrita como "1GB row groups, 1GB HDFS block size, 1 HDFS block per HDFS file". Página de dado: "We recommend 8KB for page sizes". O raciocínio declarado é a troca entre I/O sequencial grande (favorece grupo grande) e granularidade de leitura seletiva (favorece página pequena). Fonte: [Parquet Configurations](https://parquet.apache.org/docs/file-format/configurations/). Guarde esses números: eles são maiores que o default de partição de leitura do Spark, e a seção 1.4 volta a isso.

#### 1.2 Por que esse layout, e só ele, dá poda de coluna e pushdown

São três podas distintas, e o vocabulário da bibliografia mistura as três. O pré-aula registra que o Damji 4 usa o termo `columnar pushdown`, que não é termo do Spark, e o próprio registro já separa: o que existe é poda de coluna mais pushdown de predicado, duas coisas.

**a) Poda de coluna (projeção).** O rodapé traz o offset de início de cada pedaço de coluna. Ler três colunas de oitenta é ler três faixas de bytes cujos endereços você já tem. Não é "ler tudo e descartar": o byte da coluna que você não pediu nunca sai do armazenamento. Isso só é possível porque os valores de uma coluna estão contíguos, e é exatamente o que o formato de linha (CSV, JSON, Avro) não consegue oferecer. Note que a versão do Luu, colunas em arquivos separados, produziria o mesmo benefício, e é por isso que o erro passa: o efeito é o mesmo, a implementação é outra, e as consequências de tamanho de arquivo e de layout de diretório são completamente diferentes.

**b) Pushdown de predicado por estatística.** O struct `Statistics` do Parquet carrega `min_value`, `max_value`, `null_count`, `distinct_count`, `nan_count` (só para float e double) e dois booleanos, `is_min_value_exact` e `is_max_value_exact`. Ele aparece em três lugares: `ColumnMetaData` (nível de grupo de linhas), `DataPageHeader` e `DataPageHeaderV2` (nível de página). Fonte: [parquet.thrift](https://raw.githubusercontent.com/apache/parquet-format/master/src/main/thrift/parquet.thrift). Com isso, um filtro `WHERE valor > 1000` permite ao leitor descartar um grupo de linhas inteiro só olhando o rodapé, sem tocar no dado, se o `max_value` daquele grupo for 800.

Há um quarto nível, o **page index**: o `ColumnChunk` tem offsets para `OffsetIndex` e `ColumnIndex`, e o `ColumnIndex` guarda limite inferior e superior por página. Isso permite pular páginas dentro de um pedaço de coluna sem descomprimir as páginas vizinhas. É a estrutura que a bibliografia não menciona e que faz a diferença entre "poda por bloco de 512 MB" e "poda por bloco de 8 KB".

No Spark, as chaves e os defaults, todos da [página de Parquet do Spark 4.2.0](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html):

| Config | Default | Desde | O que faz |
|---|---|---|---|
| `spark.sql.parquet.filterPushdown` | `true` | 1.2.0 | liga o pushdown de filtro para Parquet |
| `spark.sql.parquet.aggregatePushdown` | `false` | 3.3.0 | empurra `MIN`, `MAX` e `COUNT` para o Parquet. Responde a agregação a partir do rodapé, sem ler dado. A doc avisa: "If statistics is missing from any Parquet file footer, exception would be thrown" |
| `spark.sql.parquet.enableVectorizedReader` | `true` | 2.0.0 | decodificação vetorizada |
| `spark.sql.parquet.enableNestedColumnVectorizedReader` | `true` | 3.3.0 | vetorizado para struct, list e map |
| `spark.sql.parquet.columnarReaderBatchSize` | `4096` | 2.4.0 | linhas por lote do leitor vetorizado |
| `spark.sql.parquet.recordLevelFilter.enabled` | `false` | 2.3.0 | filtro nativo linha a linha do Parquet. Só tem efeito com `filterPushdown` ligado **e** o leitor vetorizado desligado |

Três correções à bibliografia saem dessa tabela.

Primeira: o pré-aula registra que o Damji 3 escolhe um exemplo de `explain()` com `PushedFilters: []` num capítulo que vende pushdown. O default de `filterPushdown` é `true` desde a versão 1.2.0, ou seja, o vazio no exemplo dele não é falta de configuração, é característica da consulta escolhida.

Segunda: o pré-aula registra que o Damji 4 define leitor vetorizado como quem lê blocos de linhas, "tipicamente 1.024 por bloco", na seção de ORC. O default documentado do lote de leitura é **4096**, tanto em Parquet (`spark.sql.parquet.columnarReaderBatchSize`, desde 2.4.0) quanto em ORC (`spark.sql.orc.columnarReaderBatchSize`, desde 2.4.0). O número 1.024 existe, mas é o `spark.sql.orc.columnarWriterBatchSize`, default `1024` desde 3.4.0, que é lote de **escrita** e não existia quando o livro saiu. Fontes: [Parquet](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html) e [ORC](https://spark.apache.org/docs/latest/sql-data-sources-orc.html).

Terceira: o pré-aula registra a dúvida R5-15, de que o Damji apresenta `spark.sql.orc.impl = native` e `spark.sql.orc.enableVectorizedReader = true` como pré-requisito a satisfazer sem dizer os defaults. Confirmado na fonte: os defaults são `native` (desde 2.3.0) e `true` (desde 2.3.0). Não há nada a configurar.

**O que pushdown não é.** Pushdown por estatística é **descarte de bloco**, não filtro fino. O leitor descarta grupos de linhas e páginas que provavelmente não têm nada, e depois o Spark ainda aplica o filtro exato sobre o que sobrou. Um filtro que passa em todos os grupos não economiza nada, e é por isso que **ordenar o dado pela coluna de filtro** muda o resultado do pushdown drasticamente: numa tabela ordenada por data, cada grupo de linhas tem um intervalo estreito de datas e o descarte é quase perfeito; numa tabela em ordem aleatória, todo grupo contém quase todo o intervalo e o descarte é zero. Esse é o mesmo raciocínio que sustenta bucketing com `sortBy` (seção 3) e clustering nos formatos de tabela (seção 6). A bibliografia não faz essa ligação em nenhum dos cinco capítulos.

#### 1.3 Por que arquivo pequeno em Parquet é ruim

Quatro custos empilhados, e o primeiro é o que a aula 01 já tinha modelado.

**a) Custo fixo de abertura por arquivo.** Abrir um Parquet não é abrir e ler: é listar, abrir, ler o fim do arquivo para descobrir o tamanho do rodapé, e ler o rodapé. Em armazenamento de objeto, cada um desses passos é uma requisição HTTP com latência de dezenas de milissegundos, e a latência não diminui porque o arquivo é pequeno. Um arquivo de 20 KB paga o mesmo pedágio de um de 1 GB.

O Spark modela isso explicitamente. A [aula 01, Parte 1, seção 4](../aula-01/02-aprofundamento.md) já registrou a fórmula de empacotamento:

```
bytesPerCore  = (soma_bytes + num_arquivos * openCostInBytes) / minPartitionNum
maxSplitBytes = min(spark.sql.files.maxPartitionBytes,
                    max(spark.sql.files.openCostInBytes, bytesPerCore))
```

Os defaults, confirmados em [Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html): `spark.sql.files.maxPartitionBytes` = `134217728` (128 MB), desde 2.0.0; `spark.sql.files.openCostInBytes` = `4194304` (4 MB), desde 2.0.0, descrito como "The estimated cost to open a file, measured by the number of bytes that could be scanned in the same time"; `spark.sql.files.minPartitionNum` herda `spark.sql.leafNodeDefaultParallelism`, desde 3.1.0.

A conta que dói: 10.000 arquivos de 20 KB somam **200 MB** de dado e **40 GB** de custo modelado, porque cada arquivo entra na conta valendo pelo menos 4 MB. O planejador empacota cerca de 32 arquivos por partição (128 MB dividido por 4 MB), o que dá aproximadamente **313 tasks para ler 200 MB**, cada uma lendo 640 KB reais. Você paga escalonamento, serialização de task e abertura de arquivo trezentas vezes para ler o que caberia numa task. E, como a aula 01 registrou, o problema não é só custo de listagem, é forma da partição.

**b) Estatística inútil.** A recomendação do próprio Parquet é grupo de linhas de 512 MB a 1 GB. Num arquivo de 20 KB existe um grupo de linhas, que é o arquivo inteiro. Poda por estatística de grupo de linhas passa a ser poda por arquivo, e o page index perde granularidade porque há poucas páginas. Você mantém a estrutura e perde o mecanismo. Esta é a consequência que o erro do Luu esconde: se cada coluna estivesse num arquivo próprio, "arquivo pequeno" não teria a mesma implicação sobre estatística, porque a unidade de poda não seria o grupo de linhas.

**c) Codificação pior.** Parquet ganha espaço com dicionário e com codificação de repetição, e as duas dependem de ver muitos valores da mesma coluna de uma vez. Num pedaço de coluna com trinta linhas, o dicionário custa mais do que economiza. O resultado é que mil arquivos pequenos somam mais bytes que um arquivo grande com o mesmo dado.

**d) Rodapé proporcionalmente gigante.** O schema, a lista de grupos de linhas e as estatísticas são repetidos em cada arquivo. Num arquivo de 20 KB com oitenta colunas, o rodapé pode ser a maior parte do arquivo. E o planejamento tem de ler todos eles antes de gerar a primeira task, o que é trabalho de driver, serial e invisível na Spark UI até você olhar o tempo entre a submissão e o começo do primeiro stage.

**De onde vêm os arquivos pequenos.** Sempre da escrita, e sempre por um de três motivos: o número de partições do DataFrame na hora do `write` (o pré-aula registra que o Luu diz isso com clareza, que o número de arquivos de saída acompanha o número de partições, verificável com `movies.rdd.getNumPartitions`, e que `coalesce(1)` é o truque para arquivo único); `partitionBy` em coluna de cardinalidade alta, que multiplica arquivos por diretórios (seção 4); ou ingestão frequente em `append`, um micro-lote por arquivo.

**Remédios, em ordem de preferência.** Ajustar o número de partições antes de escrever (`coalesce` para reduzir sem shuffle, `repartition` quando você precisa redistribuir e aceita o shuffle: o pré-aula registra que o Chadha receita os dois lado a lado e não diz que o primeiro é shuffle completo e o segundo é funil). Limitar linhas por arquivo com `maxRecordsPerFile` no writer, para não cair no oposto, um arquivo de 30 GB. E compactação periódica, que é o que os formatos de tabela da seção 6 automatizam. Ressalva de honestidade: **não encontrei** `spark.sql.files.maxRecordsPerFile` nem `spark.sql.sources.partitionOverwriteMode` nas páginas públicas de [configuração](https://spark.apache.org/docs/latest/configuration.html) e de [performance tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html) do 4.2.0 que abri; as duas chaves existem no `SQLConf` e são muito citadas, mas não vou afirmar default sem a linha da fonte.

---

### 2. Codecs e divisibilidade

O pré-aula registra o buraco com precisão, na entrada de vocabulário sobre codecs: os livros dão as listas (JSON e CSV aceitam `bzip2`, `deflate`, `gzip`, `lz4`, `snappy`; Avro tem default `snappy`) e falta "qual escolher e por quê. Nada sobre **divisibilidade**, e gzip não é divisível, o que decide paralelismo de leitura. Nada sobre a troca entre taxa e CPU, nada sobre o default do Parquet".

#### 2.1 A distinção que resolve metade da confusão

Divisibilidade é duas perguntas diferentes, e quem funde as duas erra a decisão.

**Pergunta A, formato de texto comprimido inteiro.** Um `dados.csv.gz` é um fluxo comprimido do primeiro byte ao último. Para ler o byte 5.000.000.000 você tem de descomprimir os 5 bilhões anteriores. Logo, o arquivo não pode ser cortado em pedaços independentes, e o Spark é obrigado a atribuir **uma task por arquivo**. Aqui a divisibilidade é propriedade do codec.

**Pergunta B, codec dentro de um contêiner colunar.** Em Parquet e em ORC, a compressão é aplicada **por página** (ou por fluxo de coluna, no ORC), e o rodapé guarda o offset de cada pedaço de coluna e de cada grupo de linhas. Um leitor pode saltar direto para o grupo de linhas 7 e descomprimir só as páginas dele. **Um Parquet com gzip é divisível**, na granularidade do grupo de linhas, e um Parquet com snappy também. Aqui a divisibilidade vem do contêiner, não do codec.

Essa é a frase que decide: **codec não divisível estraga paralelismo em CSV, JSON e texto, e não estraga em Parquet e ORC**. Se essa distinção fosse dita nos livros, a discussão de codec caberia em cinco linhas.

Há um terceiro caso que o pré-aula pega no Chadha e que pertence a esta seção: `multiLine`. O registro anota que ele usa `multiLine=true` em três receitas e que "o capítulo nunca diz que `multiLine` torna o arquivo indivisível, o que colapsa o paralelismo para uma task por arquivo". É o mesmo efeito do gzip por outra causa: o parser precisa do arquivo inteiro para saber onde um registro acaba, então não existe ponto de corte seguro. Um JSON multilinha de 40 GB é uma task, com codec ou sem.

#### 2.2 A tabela por codec

Taxa e CPU são relativos, não absolutos: os números reais dependem do seu dado, e não vou inventar percentual. A coluna que é fato objetivo é a última.

| Codec | Taxa de compressão | CPU na escrita | CPU na leitura | Divisível em texto? | Onde aparece no Spark |
|---|---|---|---|---|---|
| `none` / `uncompressed` | nenhuma | nenhuma | nenhuma | sim (não há codec) | todos |
| `snappy` | baixa | muito baixa | muito baixa | **não** | **default do Parquet no Spark**; opção de JSON, CSV, Avro, ORC |
| `lz4` / `lz4_raw` | baixa | muito baixa | muito baixa | **não** | Parquet, ORC, JSON, CSV |
| `zstd` | alta, ajustável por nível | moderada | baixa | **não** | Parquet, ORC (**default de escrita do ORC**) |
| `gzip` | alta | alta | média | **não** | Parquet, JSON, CSV |
| `deflate` | alta (mesmo motor do gzip, sem o envelope) | alta | média | **não** | JSON, CSV, Avro |
| `bzip2` | muito alta | muito alta | alta | **sim** | JSON, CSV, Avro |
| `brotli` | alta | alta | média | **não** | Parquet, ORC. A doc do Spark avisa: "Note that `brotli` requires `BrotliCodec` to be installed" |
| `lzo` | baixa a média | baixa | baixa | não, sem índice externo | Parquet, ORC. Precisa de biblioteca nativa |
| `zlib` | alta | alta | média | **não** | ORC (nome do deflate lá) |
| `xz` | muito alta | muito alta | alta | **não** | Avro |

**A fonte de divisibilidade é dura e simples.** No Hadoop, a capacidade de começar a descomprimir do meio é declarada pela interface `SplittableCompressionCodec`, "meant to be implemented by those compression codecs which are capable to compress / de-compress a stream starting at any arbitrary position". A lista de classes que a implementam tem **um** item: `BZip2Codec`. Fonte: [SplittableCompressionCodec (Hadoop)](https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/io/compress/SplittableCompressionCodec.html). Todo o resto, gzip incluído, não é divisível como fluxo.

**Os defaults, com link.**

- Parquet no Spark: `spark.sql.parquet.compression.codec` = **`snappy`**, desde a versão 1.1.1. Valores aceitos: `none`, `uncompressed`, `snappy`, `gzip`, `lzo`, `brotli`, `lz4`, `lz4_raw`, `zstd`. Fonte: [Parquet Files](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html). Isso responde à lacuna que o pré-aula aponta: o Damji 4 passa `snappy` na mão no exemplo e nunca diz que é o default, e o pré-aula registra na dúvida R5-12 que "Parquet não emprega um algoritmo de compressão, ele aceita vários".
- ORC no Spark: a opção de escrita `compression` tem default **`zstd`**, com valores `none`, `snappy`, `zlib`, `lzo`, `zstd`, `lz4`, `brotli`. Fonte: [ORC Files](https://spark.apache.org/docs/latest/sql-data-sources-orc.html). Repare no contraste: os dois formatos colunares embutidos têm defaults **diferentes**, e o ORC já foi movido para zstd enquanto o Parquet segue em snappy.
- Avro: o pré-aula registra que a Tabela 4-5 do Damji dá default `snappy` e remete a `spark.sql.avro.compression.codec`. **Não abri** a página de Avro do Spark 4.2.0, então não confirmo esse default na fonte primária.
- CSV e JSON: o pré-aula registra que o Damji lista os codecs e que o default de `compression` em CSV é não comprimir. **Não abri** as páginas de CSV e de JSON do 4.2.0, então também não confirmo essa linha.

#### 2.3 Por que gzip não divisível estraga paralelismo, com número

Um `eventos.csv.gz` de 8 GB, num cluster de 20 executores com 4 cores cada, 80 slots. O `spark.sql.files.maxPartitionBytes` de 128 MB é irrelevante: o formato não permite cortar. O Spark cria **uma** partição, atribui **uma** task, e 79 slots ficam olhando. Se descomprimir e parsear roda a 50 MB/s, você espera cerca de 160 segundos com 1,25% do cluster ocupado, e nenhuma configuração de Spark conserta isso.

Três saídas, em ordem de qualidade:

1. **Muitos arquivos em vez de um.** Se quem gera o dado escrever 60 arquivos de 130 MB em vez de um de 8 GB, o paralelismo volta, porque a unidade divisível passa a ser o arquivo. Não conserta a divisibilidade, contorna. É a saída certa quando você não controla o formato mas controla o número de arquivos.
2. **Converter na primeira ingestão.** Ler o gzip uma vez, serialmente, e escrever Parquet particionado. Você paga a task única uma vez e nunca mais. É a resposta certa em 95% dos casos, e é literalmente o que o Damji 4 recomenda quando diz para salvar o DataFrame em Parquet depois de transformar e limpar, sem dar o motivo mecânico.
3. **bzip2**, que é divisível. Funciona e custa caro em CPU nas duas pontas. Vale quando você recebe um único arquivo de texto enorme, não pode reescrevê-lo em pedaços e precisa de paralelismo já.

#### 2.4 Como escolher, em três linhas

- **Dado lido muitas vezes, CPU disputada:** `snappy` ou `lz4`. Descompressão quase de graça.
- **Dado frio, armazenamento ou egresso de rede caro:** `zstd`. É a escolha que a indústria fez desde que ele estabilizou, e é por isso que o default do ORC no Spark já é ele.
- **Texto que vai ser lido em paralelo:** ou não comprima, ou comprima em muitos arquivos, ou use `bzip2`. Nunca um gzip único e grande.
- **Em Parquet, a pergunta de divisibilidade não se aplica.** Escolha só por taxa e CPU. Meça no seu dado, com a sua consulta: o ganho de zstd sobre snappy varia enormemente com a cardinalidade das colunas, e qualquer número genérico é propaganda.

---

### 3. Escrita: `mode`, `partitionBy`, `bucketBy`, `sortBy`

#### 3.1 `mode`

Os quatro modos, com o nome em cada dialeto, da [página de load e save do 4.2.0](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html):

| Scala/Java | Qualquer linguagem | O que faz |
|---|---|---|
| `SaveMode.ErrorIfExists` (**default**) | `"error"` ou `"errorifexists"` (**default**) | se o dado já existe, lança exceção |
| `SaveMode.Append` | `"append"` | acrescenta o conteúdo do DataFrame ao dado existente |
| `SaveMode.Overwrite` | `"overwrite"` | o dado existente é sobrescrito pelo conteúdo do DataFrame |
| `SaveMode.Ignore` | `"ignore"` | se o dado já existe, não salva e não altera nada. A doc compara com `CREATE TABLE IF NOT EXISTS` |

O pré-aula registra que a tabela de modos é a única tese inteira do Chadha e a cobertura mais completa dos cinco capítulos, e registra também o defeito: ele diz que `overwrite` "derruba índices e constraints", que é vocabulário de banco relacional colado num writer de arquivo, e nunca diz qual é o default. O Damji 4 dá os modos na tabela e declara o default. As duas fontes conferem com a documentação.

**O que os cinco capítulos não dizem sobre `overwrite`, e é a coisa mais importante desta subseção.** `mode("overwrite")` sobre um diretório não é atômico. O Spark apaga o destino e escreve o novo conteúdo, e existe uma janela entre os dois passos em que o diretório está vazio ou parcial. Se o job morre no meio, você perdeu o dado antigo e não tem o novo. Um leitor que rode nessa janela lê dado incompleto, sem erro e sem aviso, porque `_SUCCESS` é convenção: nada no Spark impede um leitor de ler um diretório que não tem `_SUCCESS`. Essa é a lacuna que o pré-aula aponta na divergência 5, e é o gancho direto da seção 6.

Uma chave relacionada que muda muito o comportamento em tabela particionada é `spark.sql.sources.partitionOverwriteMode`, que decide se `overwrite` apaga a tabela toda (`static`) ou só as partições presentes no DataFrame (`dynamic`). **Não achei** essa chave nas páginas públicas de configuração do 4.2.0 que abri, então não afirmo o default aqui. É pergunta de aula.

#### 3.2 `partitionBy`

O que faz: agrupa as linhas pelos valores das colunas indicadas e escreve **um diretório por combinação de valores**, com o nome no formato `coluna=valor`. A doc é direta: "`partitionBy` creates a directory structure as described in the Partition Discovery section. Thus, it has limited applicability to columns with high cardinality." Fonte: [Generic Load/Save Functions](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html).

Onde funciona: **nos dois**. "While partitioning can be used with both `save` and `saveAsTable` when using the Dataset APIs." Em SQL, `PARTITIONED BY (col, ...)` no `CREATE TABLE`. Fonte: [CREATE DATASOURCE TABLE](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-table-datasource.html).

O detalhe de mecanismo que nenhum dos cinco capítulos dá, e que explica a "coluna que aparece do nada" na leitura: **a coluna de partição é retirada dos arquivos de dado**. Ela não é escrita dentro do Parquet, existe apenas como texto no caminho. É por isso que particionar por uma coluna é, além de tudo, uma economia de bytes, e é por isso que a leitura precisa de um mecanismo próprio para reconstruir a coluna (seção 4).

O pré-aula registra a cobertura: `partitionBy` é a única coisa que o Damji 4 cita e abandona, aparecendo só na linha de assinatura sem definição e sem exemplo; o Chadha dá exemplo completo, particionando o CSV da Netflix por `release_year`; e o Luu, fora do escopo pedido, dá a regra de bolso de que a coluna de partição deve ter **cardinalidade baixa** e mostra o resultado concreto, diretórios `produced_year=1961` até `produced_year=2012`, anotando que esses nomes são o que permite ler menos dado depois.

#### 3.3 `bucketBy` e `sortBy`: as duas linhas do Damji que não rodam

O pré-aula registra, na dúvida R5-16, que o Damji 4 imprime como "recommended usage patterns" do `DataFrameWriter`:

```
DataFrameWriter.format(args).option(args).bucketBy(args).partitionBy(args).save(path)
DataFrameWriter.format(args).option(args).sortBy(args).saveAsTable(table)
```

E registra o veredito: nenhum dos dois roda como impresso, `bucketBy(...).save(path)` não existe porque bucketing só funciona com `saveAsTable()`, e `sortBy` sem `bucketBy` também recusa. O pré-aula chama isso de achado mais forte da leitura, "porque é a assinatura que se copia".

**Confirmado na fonte, com a frase.** A documentação do Spark 4.2.0 abre a seção com: "For file-based data source, it is also possible to bucket and sort or partition the output. **Bucketing and sorting are applicable only to persistent tables**". E o contraste no parágrafo seguinte é explícito: "While partitioning can be used with both `save` and `saveAsTable`". Fonte: [Generic Load/Save Functions](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html).

**Por que a regra é essa, e não é arbitrária.** Particionar produz um layout **autodescritivo**: o nome do diretório diz qual coluna vale qual valor, e qualquer leitor que conheça a convenção `chave=valor` reconstrói a informação sem consultar nada. Bucketizar produz um layout **mudo**: os arquivos são `part-00000` até `part-00041`, e nada no nome ou no conteúdo diz "estes 42 arquivos são o resultado de `hash(name) mod 42`". O número de buckets e as colunas de bucket são um **contrato**, e contrato precisa de um lugar para morar. Esse lugar é o metadado da tabela no catálogo. Sem tabela, você escreveria um layout que nenhum leitor sabe existir e que nenhum otimizador pode usar, ou seja, pagaria o custo de bucketizar e receberia zero. Por isso `save(path)` recusa em vez de aceitar em silêncio.

O mesmo vale para `sortBy`. A ordenação dentro do bucket também é contrato, e a gramática de SQL do Spark torna isso visível: a cláusula é `CLUSTERED BY (col, ...) [ SORTED BY (col [ASC|DESC], ...) ] INTO num_buckets BUCKETS`, com o `SORTED BY` **dentro** do `CLUSTERED BY`. Não existe cláusula de ordenação avulsa na DDL, e a API espelha a DDL. Fonte: [CREATE DATASOURCE TABLE](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-table-datasource.html).

**As formas que rodam**, da própria doc:

```python
people_df.write.bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed")

users_df.write.partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet")

(users_df.write
    .partitionBy("favorite_color")
    .bucketBy(42, "name")
    .saveAsTable("users_partitioned_bucketed"))
```

```sql
CREATE TABLE users_partitioned_bucketed
USING parquet
PARTITIONED BY (favorite_color)
CLUSTERED BY(name) SORTED BY (favorite_numbers) INTO 42 BUCKETS
AS SELECT * FROM parquet.`examples/src/main/resources/users.parquet`;
```

Repare que a combinação que o Damji imprime como recomendada, `bucketBy` mais `partitionBy` mais `save(path)`, é exatamente a combinação certa com o terminador errado. Trocar `save(path)` por `saveAsTable(nome)` faz a primeira linha dele funcionar. É um erro de uma palavra numa assinatura de referência, o que o torna mais perigoso que um erro de conceito.

#### 3.4 Para que serve bucketing de verdade

A definição da doc de DDL é a mais curta e a melhor: "Bucketing is an optimization technique that uses buckets (and bucketing columns) to determine data partitioning and **avoid data shuffle**." Fonte: [CREATE DATASOURCE TABLE](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-table-datasource.html).

O mecanismo, em três passos:

1. Na escrita, `hash(colunas_de_bucket) mod n` decide em qual dos `n` arquivos a linha vai. O `n` e as colunas ficam no metadado da tabela.
2. Duas tabelas bucketizadas pela **mesma** coluna com o **mesmo** `n` têm a garantia de que qualquer chave está no bucket de mesmo número nas duas.
3. No join entre elas por essa coluna, o Spark sabe que o bucket 7 de A só pode casar com o bucket 7 de B. Ele lê os pares diretamente, sem redistribuir nada. O `Exchange`, que é o shuffle, desaparece do plano.

O mesmo raciocínio vale para agregação: um `groupBy` pela coluna de bucket já tem cada chave inteiramente dentro de um arquivo, então a agregação é local. E o `sortBy` acrescenta a ordenação dentro do bucket, o que permite ao sort-merge join dispensar também a fase de ordenação, não só a de shuffle.

Como você **vê** que funcionou: o plano perde os nós `Exchange` antes do join. A doc de performance diz isso sobre o Storage Partition Join, que é a generalização moderna do bucket join: "Storage Partition Join (SPJ) is an optimization technique in Spark SQL that makes use the existing storage layout to avoid the shuffle phase. This is a generalization of the concept of Bucket Joins, which is only applicable for bucketed tables, to tables partitioned by functions registered in FunctionCatalog", e "If Storage Partition Join is performed, the query plan will not contain Exchange nodes prior to the join". Fonte: [Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html). Isso fecha o vocabulário que a aula 01 deixou aberto: contar `Exchange` é contar shuffles, e bucketing é a técnica de layout que remove um deles de graça na leitura.

**Por que quase ninguém usa hoje.** Seis razões, e vale enfileirá-las porque a soma é o que explica o abandono.

1. **Exige tabela em catálogo.** Um data lake em armazenamento de objeto, com Parquet num caminho e leitura por `spark.read.parquet("s3://...")`, não tem tabela. A maior parte do mundo real está nessa situação, e para essa situação bucketing simplesmente não existe.
2. **O contrato é rígido e o volume não é.** O `n` é fixo na criação. A tabela que tinha 50 GB e 200 buckets, com 200 arquivos de 250 MB, vira 500 GB e 200 arquivos de 2,5 GB. Consertar significa reescrever a tabela inteira. E as duas pontas do join precisam combinar em coluna **e** em `n`: uma tabela grande participa de muitos joins com chaves diferentes, e você não pode bucketizar pela mesma coluna para todos.
3. **Multiplica arquivos.** Cada task de escrita produz até um arquivo por bucket. Escrever de 1.000 partições para 200 buckets pode gerar 200.000 arquivos, e a seção 1.3 volta com juros. Evitar isso exige um `repartition` pelas colunas de bucket antes do `write`, ou seja, você paga um shuffle na escrita para economizar um na leitura, o que só compensa se a leitura for muito mais frequente.
4. **O AQE resolveu o caso mais comum por outro caminho.** Como a [aula 01, Parte 5, seção 2](../aula-01/02-aprofundamento.md) registrou, o AQE está ligado por padrão desde o Spark 3.2.0 e converte sort-merge join em broadcast em tempo de execução, além de dividir partição com skew. Para o join fato contra dimensão, que é o caso mais frequente, ele tira o custo do shuffle sem exigir contrato de layout nenhum.
5. **O ecossistema foi para outro lado.** Os formatos de tabela da seção 6 atacam o mesmo custo com poda por estatística de arquivo, clustering e compactação automática, sem congelar o layout. E o próprio Spark generalizou o bucket join em Storage Partition Join sobre funções de particionamento registradas no catálogo V2, que é onde o desenvolvimento está acontecendo.
6. **Informação de fora das fontes primárias que abri:** o bucketing do Spark e o do Hive não são intercambiáveis, porque a função de hash difere, o que significa que uma tabela bucketizada pelo Hive não dá o benefício ao Spark e vice-versa. Não confirmei isso na documentação; trate como coisa a checar. O pré-aula registra que o Damji 4 credita bucketing ao "esquema de bucketing do Hive num filesystem", o que sugere compatibilidade sem afirmá-la.

**Onde ainda vale.** Warehouse estável, tabela fato grande que participa **sempre** do mesmo join pela mesma chave, volume previsível, e leitura muito mais frequente que escrita. Fora disso, o esforço não se paga.

---

### 4. Descoberta de partição na leitura

#### 4.1 A convenção, e quem a entende

O pré-aula registra que o Damji 4 nomeia descoberta de partição três vezes, o Chadha a descreve sem usar o termo, e nenhum dos dois explica "a convenção `chave=valor` no nome do diretório virando coluna", com a observação de que "sem isso, a coluna que aparece do nada é mágica".

A fonte é curta e resolve. Fonte: [Parquet Files, Partition Discovery](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html).

"In a partitioned table, data are usually stored in different directories, with partitioning column values encoded in the path of each partition directory. **All built-in file sources (including Text/CSV/JSON/ORC/Parquet) are able to discover and infer partitioning information automatically.**"

A árvore que a doc desenha:

```
path/to/table
├── gender=male
│   ├── country=US/data.parquet
│   └── country=CN/data.parquet
└── gender=female
    ├── country=US/data.parquet
    └── country=CN/data.parquet
```

Apontando `path/to/table` para `spark.read.parquet` ou `spark.read.load`, "Spark SQL will automatically extract the partitioning information from the paths". O DataFrame resultante tem as colunas dos arquivos **mais** `gender` e `country`, que não estão em nenhum arquivo. Duas notas que a bibliografia não faz: isso vale para **todas** as fontes de arquivo embutidas, não só Parquet, e a ordem de aninhamento dos diretórios é a ordem das colunas em `partitionBy`.

Esse mecanismo fecha um mistério que o pré-aula registra no Damji 4: na seção de imagens a coluna `label` aparece no schema como se fosse propriedade da imagem, e só na seção seguinte, sobre arquivos binários, o leitor descobre por acidente que ela vem de descoberta de partição, porque `recursiveFileLookup=true` a faz desaparecer. Agora está claro: `label` era um diretório `label=0` ou `label=1`, e `recursiveFileLookup` desliga a descoberta.

#### 4.2 O tipo da coluna de partição, e como controlar

"The data types of the partitioning columns are automatically inferred. Currently, **numeric data types, date, timestamp and string type** are supported. Sometimes users may not want to automatically infer the data types of the partitioning columns. For these use cases, the automatic type inference can be configured by `spark.sql.sources.partitionColumnTypeInference.enabled`, which is **default to `true`**." Mesma fonte.

Desligar faz **todas** as colunas de partição virarem `string`. É tudo ou nada, não há controle por coluna.

**Duas armadilhas concretas, e a segunda quebra em silêncio.**

A primeira sai do próprio Chadha. O pré-aula registra que a receita 7 dele particiona o CSV da Netflix por `release_year`, "coluna que, na leitura sem schema desta mesma receita, é string". Siga o tipo: a coluna nasce `string` na leitura sem schema; `partitionBy` a transforma em nome de diretório, `release_year=2019`; a leitura de volta, com inferência ligada, a devolve como **inteiro**. O tipo mudou na ida e na volta, e qualquer código a jusante que compare com `"2019"` para de casar. O livro particiona por essa coluna e não menciona nada disso.

A segunda é pior porque não dá erro. Uma coluna de código com zero à esquerda, digamos `agencia` com valores `"0007"` e `"0042"`, produz diretórios `agencia=0007`. Com inferência ligada, a leitura devolve `7` e `42`, inteiros, e o zero à esquerda desapareceu. Junte dois datasets, um lido do arquivo (string `"0007"`) e outro lido da partição (int `7`), e o join não casa nada. A correção certa não é desligar a inferência global: é declarar o schema da tabela no catálogo, ou não particionar por coluna cujo tipo textual carrega informação.

#### 4.3 `basePath`

"Starting from Spark 1.6.0, partition discovery only finds partitions under the given paths by default. If users need to specify the base path that partition discovery should start with, they can set `basePath` in the data source options. For example, when `path/to/table/gender=male` is the path of the data and users set `basePath` to `path/to/table/`, `gender` will be a partitioning column." Mesma fonte.

O que isso resolve, na prática. Se você aponta a leitura direto para `.../gender=male`, o Spark começa a descoberta ali, e o diretório `gender=male` não é mais visto como partição: **a coluna `gender` desaparece do DataFrame**. Você lê o subconjunto certo e perde a coluna que o identifica. Com `basePath` apontando para a raiz, a coluna volta e vem preenchida com `male`.

Isso é exatamente a situação da receita 3 do Chadha, que o pré-aula registra: ele lê `../data/partitioned_recipes/DatePublished=2020-01*`, um glob que entra dentro do nível de partição. O comportamento da coluna `DatePublished` aí é justamente o que `basePath` controla, e o livro não usa a opção nem menciona o efeito. O pré-aula também registra que a prosa dele diz que as receitas estão particionadas "por categoria de receita" enquanto o glob filtra por `DatePublished`, e que o caminho está escrito com um erro de digitação. É uma receita de três linhas com três problemas.

#### 4.4 A armadilha da coluna de alta cardinalidade

A doc dá o veredito em uma frase: "Thus, it has limited applicability to columns with high cardinality. In contrast `bucketBy` distributes data across a fixed number of buckets and can be used when the number of unique values is unbounded." Fonte: [Generic Load/Save Functions](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html). O pré-aula registra que o Luu dá a mesma regra de bolso, cardinalidade baixa, sem o mecanismo.

O mecanismo são três custos, e nenhum é o que a intuição sugere.

**a) Explosão de arquivo.** Um diretório por valor distinto, e dentro de cada diretório ao menos um arquivo por task de escrita que teve linhas daquele valor. Particionar por `cliente_id` com 2 milhões de valores dá 2 milhões de diretórios com arquivos de poucos KB. É a seção 1.3 elevada a milhão: 2 milhões de arquivos de 10 KB são 20 GB de dado e, pela conta de `openCostInBytes`, 8 TB de custo modelado.

**b) Listagem no planejamento.** Antes de gerar a primeira task, o Spark precisa saber que diretórios existem. Em armazenamento de objeto, listar é uma sequência de requisições paginadas, e milhões de prefixos custam minutos de driver antes de qualquer trabalho útil. É tempo que não aparece como stage na Spark UI e que faz o job parecer travado.

**c) Metadado.** Se a tabela está num metastore, cada partição é uma linha na tabela de partições do metastore. Centenas de milhares de partições transformam o metastore no gargalo, e a consulta a ele passa a demorar mais que a leitura do dado.

**A regra prática.** Particione pela coluna que você **sempre** filtra, com dezenas a poucos milhares de valores distintos, e com volume por partição na casa de centenas de MB. Data é o caso canônico e vale fazer a conta: por dia, cinco anos dão 1.825 diretórios, confortável; por hora, os mesmos cinco anos dão 43.800, o que já pede volume alto por hora para se justificar; por minuto, não faça. Se a partição por dia deixa arquivos de 2 MB, a partição está errada, não o dado.

**E o remédio que a bibliografia não tem.** Nos formatos de tabela da seção 6, a poda deixa de depender de diretório: o metadado guarda estatística por arquivo, e o planejador descarta arquivos sem listar nada. Isso torna partição desnecessária na maioria dos casos em que hoje se particiona por medo. O Iceberg vai além com **particionamento oculto**: a especificação diz que "Reads will be planned using predicates on data values, not partition values" e "Partition filters are derived from column predicates", ou seja, você declara `days(ts)` como transformação de partição, filtra por `ts` na consulta e a poda acontece sem coluna derivada e sem o usuário saber do layout. E, como o layout deixa de estar no nome do diretório, ele pode mudar: "Changing a partition spec produces a new spec identified by a unique spec ID that is added to the table's list of partition specs". Fonte: [Iceberg Table Spec](https://raw.githubusercontent.com/apache/iceberg/main/format/spec.md).

---

### 5. Catálogo, metastore e tabelas

#### 5.1 A confusão, e a distinção em uma linha

O pré-aula registra a pergunta exata, na entrada de vocabulário sobre `Catalog`: "a relação com o metastore: são a mesma coisa vista de dois lados, ou camadas distintas?". E registra que o Damji 4 entrega o catálogo em sete linhas, com três chamadas e nenhuma saída impressa, e que ele descreve o metastore como repositório central do metadado de tabela.

A resposta: **catálogo é API, metastore é armazenamento.** São três camadas, não duas.

| Camada | O que é | Como você toca nela |
|---|---|---|
| **API de catálogo** | a interface que responde "que tabelas existem, que colunas tem esta tabela" | `spark.catalog.listDatabases()`, `listTables()`, `listColumns()`, e o dialeto SQL (`SHOW TABLES`, `DESCRIBE`) |
| **Implementação de catálogo** | quem atende a chamada. Pode ser em memória, pode ser o Hive, pode ser um plugin | configuração |
| **Metastore** | onde o metadado persiste de verdade: um banco relacional, um serviço REST, um arquivo | `spark.sql.hive.metastore.*`, ou a configuração do plugin |

A analogia curta: o catálogo é o balcão, o metastore é o arquivo atrás do balcão. Você faz a mesma pergunta no balcão e recebe respostas diferentes se trocarem o arquivo. Nada garante que o metadado sobreviva ao fim da aplicação: isso depende da implementação, não da API.

Os defaults do lado do Hive, que é o default do Spark: `spark.sql.hive.metastore.version` = **`2.3.10`**, desde 1.4.0, com opções de `2.0.0` a `2.3.10`, `3.0.0` a `3.1.3` e `4.0.0` a `4.1.0`; `spark.sql.hive.metastore.jars` = **`builtin`**, desde 1.4.0, descrito como "Use Hive 2.3.10 bundled with Spark assembly". Fonte: [Hive Tables](https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html). Há também `spark.sql.catalogImplementation`, que escolhe entre `in-memory` e `hive` e decide se o metadado persiste, mas **não achei** essa chave nas páginas públicas de configuração do 4.2.0 que abri; não afirmo default.

#### 5.2 O que a API de catálogo plugável do Spark 3.0 mudou

O pré-aula registra o que o Damji 4 diz e onde ele para: as funcionalidades do `Catalog` foram expandidas no Spark 2.x, e o **Spark 3.0** o estende para usar catálogo externo, "assunto empurrado para o capítulo 12". Aqui está o mecanismo que ele empurrou.

A interface é `CatalogPlugin`, marcada **desde 3.0.0**, e a descrição é: "A marker interface to provide a catalog implementation for Spark. Implementations can provide catalog functions by implementing additional interfaces for tables, views, and functions." O registro é por configuração, e a regra é literal:

```
spark.sql.catalog.catalog-name=com.example.YourCatalogClass
spark.sql.catalog.catalog-name.(key)=(value)
```

A doc explica o segundo par: tudo que compartilha o prefixo do nome do catálogo "will be passed in the case insensitive string map of options in initialization with the prefix removed", e o nome também é passado ao plugin. A carga é feita por `Catalogs.load(String, SQLConf)`, que instancia pelo construtor público sem argumentos e depois chama `initialize`. Fonte: [CatalogPlugin (JavaDoc)](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/connector/catalog/CatalogPlugin.html). A interface que trata de tabela é `TableCatalog`, com os métodos de criar, dropar, listar, carregar, renomear e testar existência. Fonte: [TableCatalog (JavaDoc)](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/connector/catalog/TableCatalog.html). O ticket é o [SPARK-31121](https://issues.apache.org/jira/browse/SPARK-31121).

**O que isso muda de verdade, em três consequências.**

1. **Catálogo deixou de ser singular.** Antes do 3.0, "o catálogo" era um só, amarrado ao metastore do Hive. Depois, catálogos são plurais e nomeados, e o identificador de tabela tem três níveis: `catalogo.namespace.tabela`. Você pode ler de dois catálogos diferentes na mesma consulta.
2. **É por aqui que Delta e Iceberg entram como cidadãos de primeira classe.** Sem essa API, um formato de tabela só podia se plugar como fonte de dados, ou seja, sabia ler e escrever arquivos mas não sabia criar, dropar ou versionar tabelas pelo dialeto SQL do Spark. Com ela, `CREATE TABLE`, `MERGE`, `ALTER TABLE ... ADD COLUMN` e time travel passam a ser SQL do Spark atendido pelo plugin. Toda a seção 6 depende deste parágrafo.
3. **A resposta à pergunta do pré-aula mudou de versão.** Antes do 3.0, "catálogo é a mesma coisa que metastore" era uma aproximação defensável, porque a API tinha uma implementação de fato. Depois do 3.0, é errado: catálogo é ponto de extensão, e o metastore do Hive é uma implementação entre várias.

O nome reservado do catálogo de sessão embutido é `spark_catalog`. **Informação de fora das páginas que abri**, e vale conferir: não achei essa string na descrição do `CatalogPlugin`.

#### 5.3 Tabela gerenciada contra externa, e o que `DROP TABLE` faz

O pré-aula registra que esta é a tese mais bem servida do Damji 4, dita sem ambiguidade em quatro frases, e registra a ressalva de vocabulário: ele diz **unmanaged** em todas as ocorrências e nunca escreve **external**, que é o termo do dialeto SQL do Spark. Registra também que a afirmação é declarada e nunca demonstrada, porque em nenhuma página do capítulo alguém dropa uma tabela.

| | Gerenciada (*managed*) | Externa (*external*, o que o Damji chama de *unmanaged*) |
|---|---|---|
| O Spark controla | metadado **e** dado | somente o metadado |
| Onde o dado fica | sob `spark.sql.warehouse.dir`, num caminho derivado do nome do banco e da tabela | onde você mandou |
| O que a torna assim | você **não** informa localização | você informa: `LOCATION path` em SQL, ou `.option("path", ...)` antes de `saveAsTable()` |
| `DROP TABLE` | apaga metadado **e** dado | apaga **somente** o metadado |

A fonte é explícita nas duas pontas.

Sobre o que torna a tabela externa: "A Data Source table acts like a pointer to the underlying data source. For file sources such as parquet and json, **if you don't specify the LOCATION, Spark will create a default table location for you**." Fonte: [CREATE DATASOURCE TABLE](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-table-datasource.html). O pré-aula registra o equivalente na API: `option("path", ...)` antes de `saveAsTable()` é o que separa não gerenciada de gerenciada.

Sobre o `DROP`: "`DROP TABLE` deletes the table and removes the directory associated with the table from the file system **if the table is not `EXTERNAL` table**. If the table is not present it throws an exception. In case of an external table, only the associated metadata information is removed from the metastore database. If the table is cached, the command uncaches the table and all its dependents." Fonte: [DROP TABLE](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-drop-table.html). O pré-aula anota que o Damji não menciona `PURGE`: a doc traz, "If `PURGE` is specified, completely purge the table skipping trash while dropping table", com a nota de que depende de Hive Metastore 0.14.0 ou posterior.

**Onde o `spark.sql.warehouse.dir` entra, e a correção que ele traz.** É a raiz sob a qual o Spark cria o diretório de toda tabela gerenciada, e é a resposta à pergunta "onde foi parar meu dado quando eu não disse onde escrever". A doc do Spark diz: "use `spark.sql.warehouse.dir` to specify the default location of database in warehouse", e o **default é `spark-warehouse` no diretório corrente de onde a aplicação Spark foi iniciada**. A mesma página nota que "the `hive.metastore.warehouse.dir` property in `hive-site.xml` is deprecated since Spark 2.0.0". Fonte: [Hive Tables](https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html).

Isso corrige o pré-aula, que registra o Damji dizendo que o metastore do Hive fica "por default em `/user/hive/warehouse`", mudável por `spark.sql.warehouse.dir`. `/user/hive/warehouse` é o default **do Hive**, não do Spark. Num Spark local sem Hive configurado, o warehouse aparece como um diretório `spark-warehouse` ao lado de onde você rodou o `spark-submit` ou abriu o notebook, o que explica o `spark-warehouse` que aparece do nada no repositório de quem está aprendendo.

**Duas armadilhas operacionais que decorrem daí.**

Primeira: gerenciada é conveniente e perigosa em armazenamento de objeto. `DROP TABLE` numa tabela gerenciada é uma exclusão recursiva de dado, sem confirmação, e o `PURGE` do Hive pula a lixeira. A prática defensiva em data lake é tornar tudo externo, com `LOCATION` explícito, justamente para que dropar tabela seja uma operação de metadado e nunca de dado.

Segunda: `CREATE TABLE ... LOCATION ... AS SELECT` num diretório não vazio levanta exceção de análise. A doc dá a chave de escape e o custo: "If `spark.sql.legacy.allowNonEmptyLocationInCTAS` is set to true, Spark overwrites the underlying data source with the data of the input query". Fonte: [CREATE DATASOURCE TABLE](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-table-datasource.html). Ligar essa chave para "resolver" um erro é o caminho mais curto para apagar dado alheio.

---

### 6. O silêncio de cinco capítulos: formato de tabela transacional

O pré-aula registra a contagem: Luu cap. 3, zero ocorrências de Delta Lake, apesar de o capítulo 1 do mesmo livro apresentá-lo como a resposta do ecossistema para consistência; Damji cap. 3, zero, e cap. 4, uma, entre parênteses, como consumidor de Parquet, fora da lista de fontes e fora da figura; Chadha, **zero**, num livro cujo título tem Databricks. E registra a consequência: quem lê estes cinco capítulos conclui que persistir Parquet num diretório resolve.

Esta seção existe para dizer o que não resolve.

#### 6.1 Oito coisas que Parquet num diretório não resolve

1. **Commit não é atômico.** Discutido em 3.1: `overwrite` apaga e escreve, e a janela entre os dois passos existe. `_SUCCESS` é convenção, não trava.
2. **Não há isolamento de leitor.** Uma consulta lista os arquivos no começo e os lê ao longo de minutos. Se uma escrita apaga um desses arquivos no meio, a consulta morre com erro de arquivo não encontrado. Se uma escrita **acrescenta** arquivos, a consulta pode ver metade do lote novo. Nenhum dos dois é reportado como erro de consistência.
3. **Não há transação de mais de uma operação.** "Apague estas três partições e insira estas cinco" não existe como unidade. São dois comandos, e existe um estado intermediário visível.
4. **Não há `UPDATE`, `DELETE` nem `MERGE`.** Arquivo é imutável. Corrigir uma linha significa ler a partição, filtrar, reescrever a partição, na mão, e a reescrita cai no problema 1.
5. **Evolução de schema é frágil e cara.** O pré-aula registra o estado: `spark.sql.parquet.mergeSchema` tem default `false` (confirmado na [fonte](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html), desde 1.5.0, e a doc justifica: "Since schema merging is a relatively expensive operation, and is not a necessity in most cases, we turned it off by default starting from 1.5.0"), e o Chadha encerra a seção dizendo que resta tratar a evolução de schema à mão, "como fazer isso à mão nunca é dito". Não existe regra de compatibilidade, não existe histórico de mudança de schema, e não existe garantia de que renomear uma coluna não quebre um leitor antigo.
6. **Não há versão.** Você não lê a tabela de ontem, não descobre o que mudou entre duas execuções e não desfaz uma escrita ruim. O melhor que se faz é copiar o diretório antes de escrever, o que dobra armazenamento e não é atômico.
7. **Não há compactação.** Ninguém junta os arquivinhos por você. A seção 1.3 é um problema que só piora com o tempo em tabela alimentada por ingestão frequente.
8. **A poda é limitada pelo que o diretório sabe.** Partição por diretório mais estatística de rodapé, arquivo a arquivo, com o planejamento tendo de listar diretórios e abrir rodapés. Não existe índice global, nem estatística de tabela consolidada.

#### 6.2 O que Delta, Iceberg e Hudi fazem, e as duas famílias de mecanismo

A ideia central é a mesma nos três e cabe numa frase: **a tabela deixa de ser "os arquivos que estão neste diretório" e passa a ser "a lista exata de arquivos que esta versão do metadado nomeia"**. O arquivo para de ser a fonte da verdade, e o metadado assume. Essa única mudança compra commit atômico, isolamento de leitor e time travel de uma vez, porque as três coisas passam a ser operações sobre um ponteiro.

As implementações se dividem em duas famílias.

**Log de commits, o caminho do Delta.** O diretório da tabela ganha um subdiretório `_delta_log` com arquivos numerados em sequência, cada um descrevendo as ações daquele commit (adicionar arquivo, remover arquivo, mudar metadado). O estado atual é o log inteiro reproduzido em ordem, e checkpoints periódicos evitam reler tudo. O commit é a criação do arquivo `N+1`, e a atomicidade vem de o armazenamento garantir que apenas um escritor consiga criar aquele nome. A documentação do Delta descreve o efeito, não o arquivo: "the table's transaction log at the location is the source of truth", e do lado da leitura, "the DataFrame returned automatically reads the most recent snapshot". Sobre isolamento, a doc de introdução é explícita: "Serializable isolation levels ensure that readers never see inconsistent data". Fontes: [Delta Lake intro](https://docs.delta.io/latest/delta-intro.html) e [Delta batch reads and writes](https://docs.delta.io/latest/delta-batch.html).

O que a API entrega, com a sintaxe: time travel por `VERSION AS OF version` e `TIMESTAMP AS OF timestamp_expression` em SQL, ou `.option("versionAsOf", n)` e `.option("timestampAsOf", "...")` no reader; evolução de schema por `.option("mergeSchema", "true")` na escrita e `overwriteSchema` para trocar o schema junto com o dado; e `replaceWhere`, que sobrescreve atomicamente só as linhas que casam com um predicado, o que é a operação que resolve o problema 3 da lista acima. Fonte: [Delta batch](https://docs.delta.io/latest/delta-batch.html).

**Ponteiro de metadado, o caminho do Iceberg.** Uma árvore: arquivo de metadado da tabela, que aponta para uma lista de manifests, que aponta para manifests, que apontam para os arquivos de dado. A especificação é precisa em cada ponto, e vale citar:

- Commit: "All changes to table state create a new metadata file and replace the old metadata with an atomic swap."
- Concorrência otimista: "Writers create table metadata files optimistically, assuming that the current version will not be changed before the writer's commit. Once a writer has created an update, it commits by swapping the table's metadata file pointer."
- Isolamento: "Reads will be isolated from concurrent writes and always use a committed snapshot of a table's data. Writes will support removing and adding files in a single operation and are never partially visible."
- Leitor: "Readers use the snapshot that was current when they load the table metadata and are not affected by changes until they refresh."
- Snapshot: "A snapshot represents the state of a table at some time and is used to access the complete set of data files in the table."
- Reuso de metadado: "Manifests are reused across snapshots to avoid rewriting metadata that is slow-changing."

Fonte: [Iceberg Table Spec](https://raw.githubusercontent.com/apache/iceberg/main/format/spec.md).

As versões da especificação, da mesma fonte: **v1** "defines how to manage large analytic tables using immutable file formats: Parquet, Avro, and ORC"; **v2** "adds row-level updates and deletes for analytic tables with immutable files"; **v3** "extends data types and existing metadata structures", com timestamp de nanossegundo, variant, geometria e row lineage.

**Hudi é o terceiro caminho, e o problema dele é outro.** Nasceu resolvendo upsert de alta frequência, e é o único dos três que trata o índice por chave como peça central. Oferece dois tipos de tabela: **Copy-on-Write**, que reescreve o arquivo no update e deixa a leitura barata, e **Merge-on-Read**, que grava log delta e funde na leitura ou na compactação, deixando a escrita barata. Mais consulta incremental ("obtain a set of records that changed between a start and end commit time"), CDC, time travel, timeline de commits com `_hoodie_commit_time`, e índices de expressão e secundários. Fonte: [Hudi Quick Start](https://hudi.apache.org/docs/quick-start-guide).

**Resumo por capacidade.** Onde há diferença de mecanismo relevante eu marco; onde os três entregam, digo que entregam.

| Capacidade | Parquet num diretório | Delta | Iceberg | Hudi |
|---|---|---|---|---|
| Commit atômico | não | sim, log incremental | sim, swap de ponteiro | sim, timeline |
| Isolamento de leitor | não | sim, serializável | sim, snapshot ou serializável | sim, snapshot |
| Escrita concorrente | não | otimista | otimista, com retentativa | otimista |
| Evolução de schema com histórico | não | sim | sim, por ID de campo | sim |
| Evolução de **partição** | não | não (clustering em vez) | **sim**, e é o diferencial dele | não |
| Time travel | não | por versão e por timestamp | por snapshot | por instante da timeline |
| `MERGE`, `UPDATE`, `DELETE` | não | sim | sim | sim, e é a origem do projeto |
| Compactação | você faz | `OPTIMIZE` | `rewrite_data_files` | compactação de MOR |
| Poda por estatística global | não | sim | sim, no manifest | sim, com índice |

#### 6.3 Estado da adoção em 2026, e o que eu confirmei

Sou explícito sobre o que verifiquei e o que não.

**Versões e compatibilidade com Spark.**

- **Delta Lake.** A linha 4.x é a atual. O 4.1.0 declara "Full support for the latest Spark version while maintaining compatibility with Spark 4.0.1" e **derruba o suporte a Spark 3.5**, exigindo 4.0.1 ou posterior. Fonte: [Delta Lake 4.1.0 Released](https://delta.io/blog/2026-03-01-delta-lake-4-1-0-released/). A doc do Delta cita sintaxe "in Delta 4.3+" ([Delta batch](https://docs.delta.io/latest/delta-batch.html)), então a linha 4.3 existe. As datas de release que consegui puxar da [página de releases do GitHub](https://github.com/delta-io/delta/releases) saíram com o ano inconsistente na minha leitura, então **não afirmo data exata**: o que está firme é que a linha 4.x é construída contra Spark 4.1.0 e 4.0.1.
- **Iceberg.** A versão documentada como mais recente é a **1.11.0**. A matriz de engines lista Spark **3.5**, **4.0** e **4.1** como *Maintained*, com os runtimes `iceberg-spark-runtime-3.5_2.12/2.13`, `iceberg-spark-runtime-4.0_2.13` (suporte inicial no Iceberg 1.10.0) e `iceberg-spark-runtime-4.1_2.13` (suporte inicial no Iceberg 1.11.0). Spark 3.4 e anteriores estão *End of Life*. Fonte: [Multi-Engine Support](https://raw.githubusercontent.com/apache/iceberg/main/site/docs/multi-engine-support.md).
- **Hudi.** Versão documentada **1.2.0**, com bundles `hudi-spark4.x-bundle_2.13` para Spark 4.1.x e 4.0.x, e Spark 3.5.x como build default. Fonte: [Hudi Quick Start](https://hudi.apache.org/docs/quick-start-guide).

**O achado prático que isso revela, e que quase ninguém diz.** O Spark 4.2.0 saiu em **14/07/2026**, catorze dias antes desta escrita, e **nenhum dos três** tem runtime para ele nas páginas que abri. A conclusão é operacional e vale mais que qualquer comparação de features: **ao adotar um formato de tabela, sua versão de Spark passa a ser limitada pelo formato, não pelo Spark**. Você deixa de poder subir de versão no dia em que o Spark lança. Isso é uma restrição permanente de roadmap, não um detalhe de instalação, e é o primeiro item da lista de custos da subseção 6.4.

**Convergência.** O Iceberg v3 trouxe deletion vectors (informação de delete por linha anexada ao arquivo de dado, em vez de pilha de arquivos de delete posicional), row lineage (identificador estável de linha mais metadado de versão) e os tipos VARIANT, GEOMETRY e GEOGRAPHY. O lado da Databricks declara que o row lineage do Iceberg v3 é compatível com o row tracking do Delta, e que VARIANT e os tipos geoespaciais estão sendo desenvolvidos no Parquet e no Spark upstream, de onde descem para os dois formatos. Fontes secundárias e de fornecedor, marcadas como tal: [Databricks blog sobre Iceberg v3](https://www.databricks.com/blog/apache-icebergtm-v3-moving-ecosystem-towards-unification) e [Google Open Source blog](https://opensource.googleblog.com/2025/08/whats-new-in-iceberg-v3.html). O suporte a deletion vectors e row lineage do v3 já aparece em serviços gerenciados, com [anúncio da AWS](https://aws.amazon.com/about-aws/whats-new/2025/11/aws-apache-iceberg-v3-deletion-vectors-row-lineage) cobrindo EMR, Glue e S3 Tables.

A leitura desse quadro: **Delta e Iceberg estão convergindo em cima das mesmas primitivas do Parquet**, e a diferença técnica entre os dois encolhe a cada release. Isso é bom para quem escolhe, porque reduz o custo de errar, e é ruim para quem quer um critério técnico limpo, porque cada vez menos existe um.

**Qual escolher.** Sem enrolação, e assumindo que a decisão é de arquitetura e dura anos.

- **Delta**, se sua stack é majoritariamente Spark, se você está em Databricks, ou se você já tem Delta. É o caminho de menor atrito, o `MERGE` mais rodado do mercado, e a integração com o Spark é a mais direta porque os dois vêm do mesmo lugar.
- **Iceberg**, se você tem ou vai ter mais de um motor (Trino, Flink, Snowflake, BigQuery, DuckDB), se catálogo REST é requisito, ou se evolução de partição importa. É o formato que virou denominador comum da indústria e o que os catálogos gerenciados adotaram. Se você não tem certeza e a decisão precisa durar, esta é a aposta com menos risco de lock-in.
- **Hudi**, se o problema é upsert de altíssima frequência, CDC de banco com latência de minutos, ou busca por chave primária. É onde ele ainda é o melhor dos três, e é um nicho.
- **Nenhum dos três**, e esta opção é legítima e raramente dita: se a tabela é append-only, escrita por um job só, lida por um motor só, e você tolera uma janela de inconsistência de alguns minutos, Parquet num diretório particionado por data continua sendo a resposta certa e a mais barata. Adotar formato de tabela para uma tabela assim é pagar imposto sem receber serviço.

#### 6.4 O custo de operar mais um componente

Esta subseção existe porque a literatura de fornecedor não a escreve, e ela é a metade da decisão.

1. **Você amarra sua versão de Spark.** Detalhado em 6.3. É o custo mais subestimado, e o que mais dói em dois anos.
2. **Manutenção deixa de ser opcional e passa a ser um job permanente.** Compactação (`OPTIMIZE` no Delta, `rewrite_data_files` no Iceberg, compactação de MOR no Hudi), expurgo de snapshot antigo (`VACUUM`, `expire_snapshots`) e limpeza de arquivo órfão. Se você não roda, o metadado cresce sem parar, a listagem de manifests fica lenta, e a tabela transacional fica **mais lenta** que Parquet cru. Isso é um novo job no seu escalonador, para sempre, com alerta próprio.
3. **Armazenamento aumenta.** Time travel guarda versões. Retenção de 30 dias numa tabela que é reescrita todo dia é 30 cópias parciais. Retenção é dinheiro, e a conversa sobre ela é chata e recorrente.
4. **Você passa a depender de um catálogo.** As capacidades boas (identificador de três níveis, `MERGE` em SQL, commit coordenado entre escritores) dependem de um catálogo registrado pela API da seção 5.2: metastore do Hive, Glue, catálogo REST, Unity, Nessie, Polaris. É mais um serviço a rodar, autenticar, monitorar e recuperar. Se ele cai, sua tabela não é legível como tabela.
5. **Concorrência otimista não é mágica.** A especificação do Iceberg é honesta: escritores commitam otimisticamente, assumindo que a versão corrente não mudou. Quando muda, alguém perde e tem de repetir. Sob escrita concorrente pesada na mesma partição, você trocou corrupção silenciosa por retentativa visível, o que é muito melhor, mas retentativa custa tempo de cluster e complica o SLA.
6. **Depurar exige perícia nova.** Quando quebra, quebra num lugar onde `ls` não ajuda. Ler um `_delta_log` ou percorrer uma árvore de manifests para descobrir por que um arquivo não aparece na consulta é outro conjunto de habilidades, e a equipe leva meses para adquiri-lo.
7. **Leitor ingênuo passa a ler errado.** Quem aponta um script pandas ou um `spark.read.parquet` direto para o diretório da tabela vai ver arquivos que o metadado já removeu, e vai ler linhas apagadas como se existissem. Não dá erro: dá número errado. Todo consumidor precisa passar pelo formato, e "todo consumidor" inclui o analista que descobriu o caminho do S3.
8. **Sair é caro.** Converter para o formato é fácil. Desconverter mantendo histórico, não. É porta de mão única na prática, mesmo sendo tecnicamente reversível.

**A regra de decisão que eu tiraria disto.** Adote quando você tem ao menos um destes: escrita concorrente, necessidade de `UPDATE` ou `DELETE` por conformidade, mais de um motor de leitura, ou requisito de auditoria e reprocessamento. Não adote por moda, e principalmente não adote sem escalar quem vai rodar a manutenção. Formato de tabela sem compactação agendada é pior que Parquet num diretório, e leva de seis a doze meses para ficar óbvio.

---

### Perguntas que a parte 3 abriu

1. **O Luu descreveu errado ou citou uma possibilidade da especificação como se fosse o comportamento?** O campo `file_path` do `ColumnChunk` no Thrift do Parquet permite pedaço de coluna em outro arquivo, e a página do formato diz que o desenho "allows splitting columns into multiple files". *Hipótese:* é erro de leitura, não citação, porque a frase dele explica o benefício de poda de coluna pelo mecanismo errado, e a poda funciona pelo offset no rodapé, não por arquivo separado. Quero saber se o professor conhece algum escritor que produza Parquet multiarquivo por coluna na prática. Aposto que não conhece.

2. **Bucketing entra em algum pipeline que o professor mantém hoje?** *Hipótese:* não, e a razão que ele vai dar será uma de duas: o `n` fixo não sobrevive ao crescimento de volume, ou o data lake não tem tabela em catálogo. Aposto na segunda, e aposto que ele vai acrescentar que o AQE cobriu o caso do broadcast e que o resto foi para clustering em Delta ou Iceberg. Se ele disser que usa, quero o número de buckets e como decidiu.

3. **Sem formato transacional, qual é a prática recomendada para `overwrite` seguro?** *Hipótese:* escrever num diretório novo e trocar o ponteiro no catálogo com `ALTER TABLE ... SET LOCATION`, ou usar `partitionOverwriteMode` dinâmico para tocar só as partições do lote. O que eu quero testar é se ele reconhece que renomear diretório em armazenamento de objeto **não** é atômico (é cópia mais exclusão), o que derruba a receita clássica de "escreve em staging e renomeia" que funcionava em HDFS.

4. **O default `snappy` do Parquet no Spark ainda se justifica em 2026, quando o default do ORC já é `zstd`?** *Hipótese:* ele vai dizer que o default do Parquet é histórico e que zstd é a escolha melhor hoje na maioria dos casos. A pergunta que interessa é a de trás: em que perfil de job a CPU de descompressão aparece como gargalo? Minha hipótese é que quase nunca aparece, porque o gargalo é I/O de rede em armazenamento de objeto, e que a resistência a mudar o default é compatibilidade com leitores antigos, não performance.

5. **Alguém desliga `spark.sql.sources.partitionColumnTypeInference.enabled`?** *Hipótese:* praticamente ninguém, e o resultado são bugs silenciosos com códigos que têm zero à esquerda e com colunas que mudam de tipo entre a escrita e a leitura. Aposto que a resposta dele é que a solução certa não é desligar a inferência (que é global e grosseira) e sim registrar a tabela no catálogo com o schema declarado, ou nunca particionar por coluna cujo texto carrega informação.

6. **O Hive Metastore ainda está de pé nas casas grandes, ou o novo já nasce em catálogo REST?** O Spark 4.2.0 traz `spark.sql.hive.metastore.version` com default `2.3.10` e a API de catálogo plugável desde o 3.0. *Hipótese:* o HMS continua rodando por inércia em quem começou antes de 2022, e todo projeto novo nasce em Glue, Unity ou catálogo REST. Quero saber se ele trata o HMS como dívida técnica com prazo, e qual é o gatilho de migração na prática.

7. **Existe critério para escolher entre Delta e Iceberg que não seja "qual plataforma você paga"?** *Hipótese:* tecnicamente os dois convergiram, o Iceberg v3 e o Delta compartilham primitivas e o desenvolvimento de VARIANT e geo é upstream no Parquet e no Spark. O que sobra de diferença real é evolução de partição (Iceberg) contra maturidade de `MERGE` e integração Spark nativa (Delta). Aposto que ele vai dizer que na prática decide o fornecedor, e que a resposta neutra é Iceberg por causa de multiengine.

8. **Qual é o tamanho alvo de arquivo Parquet que ele usa, e como reconcilia com os defaults?** O Parquet recomenda grupo de linhas de 512 MB a 1 GB, o Spark empacota leitura em 128 MB (`maxPartitionBytes`) e cobra 4 MB fictícios por arquivo aberto (`openCostInBytes`). *Hipótese:* ele vai dizer entre 128 MB e 1 GB por arquivo, e vai alinhar com o `maxPartitionBytes`, não com a recomendação do Parquet. Quero entender a reconciliação: se o grupo de linhas é de 512 MB e a partição de leitura é de 128 MB, o Spark lê um grupo de linhas em quatro tasks ou uma task lê o grupo inteiro? Minha hipótese é que a divisão respeita a fronteira do grupo de linhas, e que na prática um arquivo de 512 MB com um grupo único vira uma task, o que é o caso "não divisível" que a aula 01 registrou.

---

**Fontes primárias desta parte.** Spark 4.2.0: [Parquet Files](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html), [ORC Files](https://spark.apache.org/docs/latest/sql-data-sources-orc.html), [Generic Load/Save Functions](https://spark.apache.org/docs/latest/sql-data-sources-load-save-functions.html), [Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html), [Hive Tables](https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html), [CREATE DATASOURCE TABLE](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-create-table-datasource.html), [DROP TABLE](https://spark.apache.org/docs/latest/sql-ref-syntax-ddl-drop-table.html), [CatalogPlugin](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/connector/catalog/CatalogPlugin.html), [TableCatalog](https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/connector/catalog/TableCatalog.html), [Configuration](https://spark.apache.org/docs/latest/configuration.html). Parquet: [File Format](https://parquet.apache.org/docs/file-format/), [Configurations](https://parquet.apache.org/docs/file-format/configurations/), [Metadata](https://parquet.apache.org/docs/file-format/metadata/), [parquet.thrift](https://raw.githubusercontent.com/apache/parquet-format/master/src/main/thrift/parquet.thrift). Hadoop: [SplittableCompressionCodec](https://hadoop.apache.org/docs/stable/api/org/apache/hadoop/io/compress/SplittableCompressionCodec.html). Formatos de tabela: [Delta Lake intro](https://docs.delta.io/latest/delta-intro.html), [Delta batch](https://docs.delta.io/latest/delta-batch.html), [Delta 4.1.0](https://delta.io/blog/2026-03-01-delta-lake-4-1-0-released/), [Iceberg Table Spec](https://raw.githubusercontent.com/apache/iceberg/main/format/spec.md), [Iceberg Multi-Engine Support](https://raw.githubusercontent.com/apache/iceberg/main/site/docs/multi-engine-support.md), [Hudi Quick Start](https://hudi.apache.org/docs/quick-start-guide). Ticket: [SPARK-31121](https://issues.apache.org/jira/browse/SPARK-31121).

**Tudo que este documento atribui aos livros vem do [pré-aula](01-pre-aula.md)**, das seções [Teses dos capítulos que valem marcação](01-pre-aula.md#teses-dos-capítulos-que-valem-marcação), [Divergências entre os livros](01-pre-aula.md#divergências-entre-os-livros), [Vocabulário novo](01-pre-aula.md#vocabulário-novo) e do registro de leitura. Nada foi citado entre aspas sem estar registrado lá.
---

## Parte 4 - RDD, DataFrame, Dataset e a fronteira do Python

Versão de referência: **Apache Spark 4.2.0**, lançado em 14/07/2026 ([release notes](https://spark.apache.org/releases/spark-release-4-2-0.html)). Os cinco textos da bibliografia descrevem o ciclo 3.0 a 3.4, então marco em linha tudo que é informação de fora dos capítulos lidos.

Esta parte fecha quatro dívidas que a leitura abriu e não pagou. A contagem de atributos do RDD, onde Luu e Damji divergem e a fonte decide ([divergência 2](01-pre-aula.md#divergências-entre-os-livros)). O mecanismo do encoder, que o Damji cita em duas linhas e adia ([R2-21](01-pre-aula.md#dúvidas-damji-capítulo-3)). A UDF, que **não aparece uma única vez nas 141 páginas** e é justo a construção em que a escolha de linguagem custa caro. E a Python Data Source API, do Spark 4.0, que nenhum dos três livros tem e que muda a resposta à pergunta que nenhum deles faz.

Uma advertência de método antes de começar: a documentação oficial de UDF escalar, [`sql-ref-functions-udf-scalar.html`](https://spark.apache.org/docs/latest/sql-ref-functions-udf-scalar.html), cobre **só Scala e Java**. Conferi a página inteira: não há uma linha sobre UDF Python, nem sobre Arrow, nem sobre `useArrow`. Quem procura o assunto pelo lugar óbvio da referência de SQL não acha. O material de Python vive na trilha do PySpark, em [Apache Arrow in PySpark](https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_pandas.html). Isso é lacuna de organização da documentação, não da bibliografia, e vale saber antes de perder meia hora.

---

### 1. Quantos atributos tem um RDD, e a resposta certa

#### O que o código diz

A divergência é checável porque a definição está escrita, em inglês, no comentário de classe do arquivo [`core/src/main/scala/org/apache/spark/rdd/RDD.scala`](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/rdd/RDD.scala). O texto do scaladoc:

> "Internally, each RDD is characterized by five main properties:
> - A list of partitions
> - A function for computing each split
> - A list of dependencies on other RDDs
> - Optionally, a `Partitioner` for key-value RDDs
> - Optionally, a list of preferred locations to compute each split on"

Cinco propriedades, duas marcadas como opcionais. É a mesma contagem e o mesmo rótulo de opcionalidade, com a ordem das três primeiras peças trocada que o **Luu 3.1** apresenta. Ou seja: o Luu não inventou nem inflou nada, ele copiou a definição que o próprio projeto mantém no cabeçalho da classe. Isso já resolve a divergência 2 em favor dele, e resolve por procedência.

#### Os membros que materializam cada propriedade

A lista de cinco não é prosa: cada item corresponde a um membro da classe, e a diferença entre eles é o que explica por que dá para contar de três jeitos.

| Propriedade | Membro na classe `RDD` | Assinatura | Quem implementa |
|---|---|---|---|
| lista de partições | `getPartitions` | `protected def getPartitions: Array[Partition]` | **abstrato**, toda subclasse tem de escrever |
| função que calcula cada split | `compute` | `def compute(split: Partition, context: TaskContext): Iterator[T]` | **abstrato**, toda subclasse tem de escrever |
| dependências de outros RDDs | `getDependencies` | `protected def getDependencies: Seq[Dependency[_]]` | tem implementação padrão (devolve as dependências passadas no construtor) |
| particionador | `partitioner` | `@transient val partitioner: Option[Partitioner] = None` | `None` por padrão, "optionally overridden by subclasses to specify how they are partitioned" |
| localizações preferidas | `getPreferredLocations` | `protected def getPreferredLocations(split: Partition): Seq[String]` | tem implementação padrão vazia, "optionally overridden by subclasses to specify placement preferences" |

Repare no detalhe que a tabela entrega e nenhum dos dois livros entrega: **os dois "opcionais" não são opcionais do mesmo jeito que os outros três são obrigatórios**. Só `compute` e `getPartitions` são abstratos, isto é, só esses dois o compilador cobra. `getDependencies` já vem resolvido pelo construtor. `partitioner` e `getPreferredLocations` vêm com padrão neutro (`None` e sequência vazia) e você sobrescreve se tiver informação melhor.

#### Qual contagem é defensável

Três contagens se sustentam, e cada uma responde a uma pergunta diferente. É por isso que os livros divergem sem que nenhum dos dois esteja mentindo.

| Contagem | Pergunta que ela responde | Onde aparece |
|---|---|---|
| **cinco** | o que compõe a **definição** de um RDD, segundo o projeto | scaladoc de `RDD.scala`; Luu 3.1 |
| **três** | o que compõe a **linhagem**, ou seja, o que o escalonador precisa para ordenar execução e recuperar de falha | Damji 3; e o próprio Luu 3.1, que diz explicitamente que as três primeiras peças **são** a linhagem |
| **dois** | o que você é **obrigado** a escrever para ter um RDD que compila | `RDD.scala`, membros abstratos |

A resposta que eu levo para a prova: **cinco é a definição, e é o número defensável quando a pergunta é "o que é um RDD"**. Cinco é o texto do projeto, e o Luu está citando a fonte. Três é defensável quando a pergunta é "o que o Spark usa para escalonar e recuperar", e aí o Damji e o Luu concordam sem perceber, porque o Luu diz que as três primeiras peças formam a linhagem. Dois é a resposta de quem vai implementar um RDD customizado.

E há uma reconciliação a mais, que fecha a divergência de vez: o pré-aula registra que o Damji descreve as partições **com a informação de localidade dentro delas**. Então o Damji não deixou a localidade de fora, ele a dobrou na peça 2. A conta dele é três para quatro itens, não três para cinco. A única peça que ele realmente omite é o particionador.

#### O que o Luu deixou como rótulo, e que consequência tem

O pré-aula marcou em [R1-7](01-pre-aula.md#dúvidas-luu-capítulo-3-seção-1) que as peças 4 e 5 vêm com "(optional)" e nada mais. As consequências, que o livro não dá:

**Sem `partitioner`, o Spark não sabe que o dado já está no lugar certo.** O particionador é a promessa de que a chave `k` está na partição `p(k)`. Quando dois RDDs declaram o mesmo particionador com o mesmo número de partições, um `join` ou um `reduceByKey` entre eles é dependência **estreita**: cada partição da saída lê uma partição de cada entrada e o shuffle não acontece. Sem a declaração, o Spark tem de assumir o pior e inserir a troca. É o mesmo raciocínio que, na camada estruturada, sustenta o bucketing.

**Sem `getPreferredLocations`, o escalonador perde a localidade.** A propriedade existe para que a task vá até o dado em vez de o dado vir até a task. Um `HadoopRDD` sobrescreve o método com os hosts dos blocos do arquivo, e o escalonador tenta primeiro `PROCESS_LOCAL`, depois `NODE_LOCAL`, e só cai para `ANY` depois de esperar. Com a lista vazia, todo agendamento começa em `ANY` e todo byte trafega pela rede. O tempo de espera por nível é controlado por `spark.locality.wait` e derivados, documentados em [configuration.html](https://spark.apache.org/docs/latest/configuration.html); **não reconferi o default de cada nível nesta rodada**, então não cravo o número aqui.

---

### 2. O que o RDD impede o otimizador de fazer, e por quê

#### A lambda não é uma árvore

O Catalyst opera sobre **árvores**: nós de plano lógico e nós de expressão, que ele casa por padrão e reescreve por regra. Uma regra de pushdown de predicado só funciona porque consegue olhar dentro do nó `Filter`, ver `GreaterThan(AttributeReference("idade"), Literal(21))`, concluir que a expressão só depende de um atributo de uma relação e mover o nó para baixo do `Join`.

Uma closure de RDD não é nada disso. `rdd.filter(x => x.idade > 21)` guarda um **objeto de função serializado**, que será mandado com a task e invocado. Não há nó `GreaterThan`, não há `AttributeReference`, não há tipo de coluna. Do lado do motor existe uma referência opaca que só sabe fazer uma coisa: receber um elemento e devolver um booleano. Não há o que reescrever, porque não há sintaxe para inspecionar. Esse é o mecanismo inteiro, e é por isso que o argumento não é sobre gosto de API: é sobre a diferença entre **descrever** um filtro e **entregar** um filtro já compilado.

O mesmo vale para o tipo. `compute` devolve `Iterator[T]`, e o `T` é apagado em tempo de execução pela JVM. Sem saber que o campo 3 é um `Long` de 8 bytes, o Spark não pode escolher codificação, não pode comprimir por coluna e não pode pular bytes. Sobra serializar o objeto inteiro. É exatamente a quarta acusação que o Damji 3 empilha na abertura do capítulo, e ela é a ponte para os encoders da seção 3.

#### As três otimizações que o Luu nomeia, e por que cada uma morre

O pré-aula registra que o Luu 3.1 lista três otimizações impossíveis sobre RDD. Vale destrinchar cada uma, porque a lista é boa e o livro não explica nenhuma.

| Otimização que o RDD impede | Por que ela precisa de árvore e de schema |
|---|---|
| **pushdown de predicado** | para empurrar um filtro até o leitor Parquet, o Spark tem de traduzir a expressão para o vocabulário do formato (`IsNotNull`, `GreaterThan` sobre uma coluna nomeada). Uma lambda não traduz para nada, então o filtro fica onde está e todo o arquivo é lido |
| **escolher tipo de join melhor** | a escolha entre broadcast, shuffled hash e sort merge depende de saber que a operação **é** um join, sobre quais chaves, e de estimar o tamanho de cada lado. `cogroup` de RDD só diz "junte por chave"; não há estatística nem chave declarada para o planejador olhar |
| **poda de colunas** | poda exige que "coluna" exista como conceito. No RDD o elemento é um objeto único e indivisível para o motor. Não há como pedir três campos de cinco, porque o motor não sabe que há cinco |

#### Onde o Luu exagera, e a correção

O pré-aula marcou em [R1-3](01-pre-aula.md#dúvidas-luu-capítulo-3-seção-1) que o livro escreve que o Spark não pode fazer otimização **nenhuma** sobre RDD, sem condição. A afirmação é forte demais e a correção é simples: o que morre é a otimização **relacional**, aquela que depende de intenção declarada. Sobre RDD o Spark continua fazendo bastante coisa: encadeia dependências estreitas dentro da mesma task sem materializar intermediário, corta o grafo em estágios nas fronteiras de shuffle, reaproveita RDD persistido, agenda por localidade quando a peça 5 existe, e faz combinação no lado do map em `reduceByKey`. Nada disso é Catalyst, e tudo isso é otimização. A frase correta é: **sem estrutura não há álgebra relacional para reescrever**, não "não há otimização".

#### O argumento de estrutura, e a condição dupla que a bibliografia enuncia

O pré-aula registra que o Luu abre o capítulo com uma condição **dupla** para as APIs estruturadas funcionarem: o dado precisa estar em formato estruturado **e** a lógica de cálculo precisa seguir uma certa estrutura. É a formulação mais precisa que a bibliografia oferece sobre o assunto, e ela está na página 1, fora da seção sobre RDD.

Precisa das duas metades, e é fácil provar que uma só não basta:

- **Schema sem lógica estruturada** é um DataFrame com uma UDF Python no meio. O dado tem nome, tipo e nulabilidade; a computação é caixa preta. O otimizador para na fronteira da UDF e o pushdown morre depois dela. Isso é a seção 4 inteira.
- **Lógica estruturada sem schema** é um RDD com um `filter` que casualmente compara um campo. O motor vê a mesma closure de sempre.

Ou seja, o RDD falha nas duas metades e a UDF Python falha em uma. Essa é a razão de a UDF ser o assunto mais prático desta parte: ela é o jeito de reintroduzir, dentro da API estruturada, exatamente o defeito que a API estruturada existe para eliminar.

---

### 3. Dataset, encoder, e por que não existe em Python

#### O que é um encoder

O scaladoc da trait é de uma frase, e é a frase certa ([Encoder, API Scala 4.2.0](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Encoder.html)):

> "Used to convert a JVM object of type `T` to and from the internal Spark SQL representation."

Os dois membros abstratos dizem o resto: `clsTag: ClassTag[T]`, para construir arrays de `T`, e `schema: StructType`, que é "the schema representing how objects are encoded as Rows". A trait existe desde a 1.6.0 e a documentação exige que implementações sejam thread-safe.

Traduzindo para mecanismo: um `Encoder[Pessoa]` é um **par de codecs gerado**, um que escreve um objeto `Pessoa` como bytes no formato binário interno (o `UnsafeRow` do Tungsten) e outro que reconstrói o objeto a partir desses bytes. Ele não é serialização genérica. Serialização Java escreve o grafo de objetos com metadado de classe; Kryo escreve mais compacto mas ainda genérico; um encoder escreve **campo por campo, em posições fixas conhecidas**, porque ele foi gerado sabendo que `Pessoa` tem um `String` em uma posição e um `Int` em outra. É daí que vem a economia de espaço e o acesso a campo sem desserializar o registro inteiro.

Em Scala, o encoder de uma `case class` é derivado pelo compilador, via os implícitos de `spark.implicits._`. Em Java você chama `Encoders.bean(MyClass.class)` ou compõe com `Encoders.tuple(Encoders.INT(), Encoders.STRING())`.

#### Por que ele exige tipo conhecido em tempo de compilação

Três requisitos, e nenhum deles é satisfazível em tempo de execução com tipos dinâmicos:

1. **Precisa do `StructType` antes de o dado existir.** O encoder é o que permite o `.as[T]` transformar `Dataset[Row]` em `Dataset[T]` sem ler uma linha. Isso exige saber os nomes, os tipos e a ordem dos campos de `T` no momento em que a expressão é construída.
2. **Precisa gerar código de acesso por deslocamento.** Escrever um `Long` no campo 3 é aritmética de ponteiro sobre a região de tamanho fixo do `UnsafeRow`. O deslocamento sai do layout, o layout sai do tipo estático.
3. **Precisa de uma classe da JVM para instanciar.** A ponta de volta do codec chama um construtor. Sem classe da JVM não há construtor para chamar.

#### Por que a JVM é condição, e não detalhe

A documentação oficial afirma o resultado sem dar o mecanismo, e a frase sobrevive intacta no 4.2.0 ([SQL Programming Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)):

> "Python does not have the support for the Dataset API. But due to Python's dynamic nature, many of the benefits of the Dataset API are already available (i.e. you can access the field of a row by name naturally `row.columnName`)."

O mecanismo, que a doc não dá, tem três camadas e todas fecham a porta:

- **Não existe o momento da compilação.** Em Python não há build em que uma macro possa derivar o codec. Os tipos aparecem quando o objeto aparece. Anotação de tipo em Python é dica para ferramenta de análise estática, não informação disponível ao Spark no momento de montar o plano.
- **Não existe o objeto da JVM.** `Dataset[Pessoa]` promete que instâncias de `Pessoa` vivem no heap da JVM, onde os executores rodam. Um objeto Python vive em **outro processo**. A promessa é incoerente por construção, não por falta de implementação.
- **A lambda tipada jamais rodaria dentro do codegen.** O ganho central do Dataset é fundir uma lambda de linguagem nativa ao pipeline compilado da JVM. Uma função Python nunca entra nesse pipeline: ela vira o operador de avaliação da seção 4.

A tabela por linguagem que o Damji 3 dá (`Dataset[T]` em Scala, `Dataset<T>` em Java, DataFrame com `Row` genérico em Python e R) descreve o mundo certo, e ela continua certa no 4.2.0. O que o livro não diz é que a ausência **não é perda de performance**, é perda de segurança de tipo em tempo de escrita. A seção 4 explica por quê.

#### O mecanismo que o Damji esconde: por que DataFrame ganha em espaço e velocidade

O pré-aula registra a acusação com precisão em [R2-21](01-pre-aula.md#dúvidas-damji-capítulo-3): o capítulo põe "eficiência de espaço e de velocidade" na coluna do DataFrame, põe "serialização eficiente do Tungsten com encoders" na coluna do Dataset, insinua a conclusão e **nunca dá o mecanismo**. Aqui vai o mecanismo, em duas partes.

**Espaço.** Um DataFrame nunca sai do `UnsafeRow`. Milhões de linhas viram um punhado de blocos de bytes contíguos, com bitset de nulidade e região de tamanho fixo. Um `Dataset[T]` operado por lambda tipada tem de **materializar um objeto da JVM por linha** para entregar à lambda: alocação, cabeçalho de objeto, ponteiros para as strings, alinhamento, e depois lixo para o coletor recolher. O encoder torna a conversão barata e previsível. Não a torna gratuita, e sobretudo não a elimina.

**Velocidade.** O custo aparece no plano físico, e é a evidência que fecha o argumento. Uma lambda tipada faz o Catalyst inserir três nós:

```text
SerializeFromObject [...]
+- MapElements <function1>, obj#12: Pessoa
   +- DeserializeToObject newInstance(class Pessoa), obj#11: Pessoa
      +- FileScan parquet [nome#1,idade#2]
```

Três consequências, e as três importam:

1. **`DeserializeToObject` e `SerializeFromObject` são trabalho puro de conversão**, por linha, que o DataFrame não paga.
2. **O nó do meio é fronteira de codegen.** Ele opera sobre `ObjectType`, não sobre expressões, então não entra no `WholeStageCodegenExec` que fundiria a consulta numa função só. Você perde a fusão nos dois lados dele.
3. **A lambda é caixa preta para o otimizador**, exatamente como a closure de RDD. `ds.filter(d => d.idade > 21)` não vira `PushedFilters` no leitor Parquet. `df.filter($"idade" > 21)` vira.

Ou seja: **Dataset tipado é mais rápido que RDD e mais lento que DataFrame puro**, e o defeito responsável é o mesmo que o livro acusa no RDD nas primeiras cinco páginas e nunca admite quarenta páginas depois, quando apresenta `ds.filter(d => d.temp > 30 && d.humidity > 70)` como virtude de legibilidade.

**A ressalva que salva o Dataset, e o pré-aula tem a peça.** O registro de leitura anota que `filter()` é **sobrecarregado** e que a versão usada no exemplo é a que recebe lambda. A outra versão recebe expressão de coluna. Então:

| Escrita | O que o Catalyst vê | Pushdown | Codegen |
|---|---|---|---|
| `ds.filter(d => d.idade > 21)` | closure opaca | não | quebra |
| `ds.filter($"idade" > 21)` | árvore de expressão | sim | mantém |

As duas linhas devolvem `Dataset[Pessoa]`. A diferença de performance entre elas é grande e o livro não a menciona, porque escolhe a primeira forma para demonstrar a notação de ponto. A regra prática que sai daí: em Scala, use `Dataset[T]` para o **contrato de tipo** e escreva as operações com **expressão de coluna** sempre que der; caia na lambda só quando a lógica for realmente por objeto.

---

### 4. A fronteira JVM contra Python

É o item mais prático desta parte, e cobre um buraco inteiro: o pré-aula confirma que **não há uma única UDF nas 141 páginas** das cinco leituras. Cinco capítulos sobre APIs estruturadas e persistência, e a construção em que a escolha de linguagem custa caro não aparece.

#### 4.1 O caso em que Python não custa nada

```python
(df.filter(F.col("valor") > 100)
   .groupBy("uf_destino")
   .agg(F.sum("valor").alias("total")))
```

Aqui **nenhuma linha de dado passa pelo Python**. A API constrói uma árvore de expressões e manda a árvore para o servidor: via Py4J no PySpark clássico, ou como plano lógico não resolvido em protobuf sobre gRPC no Spark Connect ([Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html)). Dali para frente é Catalyst, Tungsten, `UnsafeRow` e executores da JVM. Só o resultado volta, e volta em lotes Arrow. O interpretador Python fica parado.

Esse é o caso em que a paridade entre linguagens vale, e é a base da seção 5.

#### 4.2 O que acontece quando entra uma UDF Python

O caminho é uma regra de otimização com nome e endereço: [`ExtractPythonUDFs.scala`](https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/execution/python/ExtractPythonUDFs.scala). Ela **arranca** a UDF de dentro da expressão onde você a escreveu e a reescreve como um operador dedicado, avaliado em lote antes do operador que consome o resultado. Sequência completa:

1. A regra extrai a `PythonUDF` da expressão e insere um nó de avaliação no plano.
2. O executor sobe (ou reaproveita) um processo **worker Python**, `pyspark.worker`. O reaproveitamento é controlado por `spark.python.worker.reuse`, default `true` ([configuration.html](https://spark.apache.org/docs/latest/configuration.html)).
3. As colunas de entrada da UDF são serializadas e escritas num socket para o worker.
4. O worker desserializa, chama a função e serializa o retorno.
5. A JVM lê de volta, converte para `UnsafeRow` e o plano segue.

Uma limitação da regra que vale conhecer, e está no código: a entrada de uma UDF Python **não pode incluir atributos de mais de um filho do operador**. Na prática, uma UDF que combina colunas dos dois lados de um join não é avaliada dentro do join; o plano tem de materializar antes. É o tipo de detalhe que explica plano estranho.

#### 4.3 `BatchEvalPython` contra `ArrowEvalPython`, no plano

Os dois nomes que você vai ver no `explain()` são os dois caminhos de transporte, e a escolha é por tipo de avaliação, segundo o mesmo arquivo:

| Nó no plano | Tipo de avaliação | O que trafega |
|---|---|---|
| `BatchEvalPython` | `SQL_BATCHED_UDF` | linhas serializadas com pickle, uma a uma |
| `ArrowEvalPython` | `SQL_ARROW_BATCHED_UDF`, `SQL_SCALAR_PANDAS_UDF`, `SQL_SCALAR_PANDAS_ITER_UDF`, `SQL_SCALAR_ARROW_UDF`, `SQL_SCALAR_ARROW_ITER_UDF` | lotes colunares Arrow |

Duas leituras práticas do plano:

- Todas as UDFs agrupadas num mesmo nó precisam ter o **mesmo** tipo de avaliação; a regra lança erro interno se houver mistura. Então UDF comum e pandas UDF na mesma projeção geram nós separados.
- O código traz um caminho de fallback comentado como "Use `BatchEvalPython` if UDT is detected", com aviso de que a otimização Arrow ficou desligada. Consequência: **em 4.2, ver `BatchEvalPython` num plano é sinal**, não rotina. Ou a config foi desligada, ou há um tipo definido pelo usuário (UDT) na entrada ou na saída.

Por que Arrow muda a natureza do custo: Arrow é formato colunar em memória com o **mesmo layout binário** dos dois lados da fronteira. A transferência deixa de ser "converter objeto Java em pickle e reconstruir objeto Python" e passa a ser "copiar um buffer". Some a conversão por linha; sobra a cópia do lote.

#### 4.4 A mudança do Spark 4.2, que liga Arrow por padrão

**Fora da bibliografia, e é a mudança que invalida todo material anterior a 2026.** O ticket é [SPARK-54555](https://issues.apache.org/jira/browse/SPARK-54555), "Enable Arrow-optimized Python UDFs and Arrow-based PySpark IPC by default", listado entre os destaques do [release 4.2.0](https://spark.apache.org/releases/spark-release-4-2-0.html). A documentação confirma em uma frase ([Apache Arrow in PySpark](https://spark.apache.org/docs/latest/api/python/tutorial/sql/arrow_pandas.html)):

> "Since Spark 4.2, Arrow optimization is enabled by default for regular Python UDFs."

O mecanismo do chaveamento está em [`python/pyspark/sql/udf.py`](https://github.com/apache/spark/blob/master/python/pyspark/sql/udf.py): quando `useArrow` é `None`, o PySpark lê `spark.sql.execution.pythonUDF.arrow.enabled` da sessão e escolhe entre `PythonEvalType.SQL_ARROW_BATCHED_UDF` e `PythonEvalType.SQL_BATCHED_UDF`. Ou seja, o parâmetro do decorador tem três estados: `True` força Arrow, `False` força pickle, e a ausência segue a config da sessão. A config vale para a sessão inteira; o parâmetro vale para aquela UDF.

Configs vizinhas e seus defaults, todos da mesma página oficial:

| Config | Default | O que controla |
|---|---|---|
| `spark.sql.execution.pythonUDF.arrow.enabled` | `true` desde 4.2 | Arrow para UDF Python comum |
| `spark.sql.execution.arrow.pyspark.enabled` | `true` ("This is enabled by default") | Arrow em `toPandas()` e em `createDataFrame()` a partir de pandas |
| `spark.sql.execution.arrow.maxRecordsPerBatch` | `10.000` ("The default value is 10,000 records per batch") | teto de linhas por lote Arrow |

Versões mínimas declaradas: **pandas 2.2.0 e PyArrow 18.0.0** ("the minimum supported versions of Pandas is 2.2.0 and PyArrow is 18.0.0").

**A ressalva que quase todo mundo esquece, e é o coração da seção 4.6.** Arrow por padrão mata a **serialização por linha**. Não mata a **chamada por linha**. Uma `@udf` escalar continua sendo invocada uma vez por linha dentro do worker Python, com todo o custo do interpretador. Só as formas vetorizadas (`pandas_udf`, `mapInPandas`, `applyInArrow`) recebem um lote por chamada e amortizam a invocação. Logo, o 4.2 encurtou a distância entre `@udf` e `@pandas_udf`, e não a zerou.

O 4.2 também trouxe [SPARK-56350](https://issues.apache.org/jira/browse/SPARK-56350), "Skip ColumnarToRow for Arrow-backed input to Python UDFs": quando a entrada já vem colunar do leitor, o plano deixa de converter para linha só para converter de volta. É um nó menos no caminho, e é o tipo de ganho que só existe porque o transporte agora é colunar por padrão.

#### 4.5 Memória: o custo que mata o container e não aparece no heap

O worker Python vive **fora** do heap da JVM e **fora** do gerenciador unificado de memória do Spark. O executor não sabe quanta memória o seu pandas usou. Quem paga é o container.

Os valores oficiais, de [configuration.html](https://spark.apache.org/docs/latest/configuration.html):

| Config | Default | Desde | Observação |
|---|---|---|---|
| `spark.executor.memoryOverhead` | `executorMemory * spark.executor.memoryOverheadFactor`, com mínimo de `spark.executor.minMemoryOverhead` | 2.3.0 | suportado em YARN e Kubernetes |
| `spark.executor.memoryOverheadFactor` | `0.10`, **exceto 0.40 em jobs não-JVM no Kubernetes** | 3.3.0 | ignorado se você setar `memoryOverhead` direto |
| `spark.executor.pyspark.memory` | não setado | 2.4.0 | se setado, limita a memória do PySpark no executor |
| `spark.python.worker.memory` | `512m` | 1.1.0 | memória por worker **durante agregação**; acima disso derrama para disco |
| `spark.executor.memory` | `1g` | 0.7.0 | heap da JVM do executor |

Duas frases da documentação que resolvem a confusão mais comum, citadas literalmente:

> "Additional memory includes PySpark executor memory (when `spark.executor.pyspark.memory` is not configured) and memory used by other non-executor processes running in the same container."

> "The maximum memory size of container to running executor is determined by the sum of `spark.executor.memoryOverhead`, `spark.executor.memory`, `spark.memory.offHeap.size` and `spark.executor.pyspark.memory`."

E sobre `spark.executor.pyspark.memory`:

> "If not set, Spark will not limit Python's memory use and it is up to the application to avoid exceeding the overhead memory space shared with other non-JVM processes."

Traduzindo para diagnóstico. Se você não setou `spark.executor.pyspark.memory`, o Python cabe dentro do `memoryOverhead`, que por padrão é **10% do heap do executor**. Executor de 8 GB dá cerca de 800 MB de folga para o Python, o Arrow, os buffers de shuffle e o resto. Uma UDF que carrega um modelo por partição estoura isso sem esforço. O sintoma **não é** `OutOfMemoryError` da JVM: é o container morto pelo YARN ou pelo Kubernetes por exceder o limite de memória. Aumentar `spark.executor.memory` não resolve, e pode piorar, porque só aumenta o heap. A tecla certa é `memoryOverhead` (ou o fator), e, para transformar o assassinato em erro legível, `spark.executor.pyspark.memory`, que faz o Python falhar dentro do processo Python.

Que o default do fator seja **0.40 em jobs não-JVM no Kubernetes** é a admissão do projeto de que 10% não serve para PySpark. A documentação diz isso com essas palavras, ao explicar que tarefas não-JVM precisam de mais espaço e "commonly fail with Memory Overhead Exceeded errors".

#### 4.6 O otimizador perdendo visibilidade, e como enxergar isso

O nó de avaliação de UDF é opaco. As consequências:

- **O Catalyst não empurra filtro através dele.** Se você filtra por uma coluna calculada pela UDF, o filtro fica acima do nó e o leitor lê tudo.
- **A poda de colunas não atravessa para depois dele** com a mesma folga, porque o motor não sabe o que a função consome nem produz além do schema declarado no retorno.
- **A fusão de codegen quebra** nos dois lados, e o plano mostra isso: nós sem asterisco de `*(n)` em volta do `ArrowEvalPython`.

O procedimento de diagnóstico é curto e vale mais que qualquer regra de bolso: rode `df.explain(True)` e compare o plano **analisado** com o **otimizado**. Se o filtro que você escreveu não subiu, e se `PushedFilters` aparece vazio na folha, procure a UDF no meio do caminho. Uma única UDF pode invalidar o pushdown do pipeline inteiro, e a diferença de I/O é de ordem de magnitude, não de percentual.

O 4.2 acrescentou instrumentação útil aqui: [SPARK-55046](https://issues.apache.org/jira/browse/SPARK-55046), "PySpark: add a UDF processing time metric", que expõe o tempo gasto na UDF como métrica ([release 4.2.0](https://spark.apache.org/releases/spark-release-4-2-0.html)). Antes disso, o custo da fronteira ficava diluído no tempo do estágio.

#### 4.7 `@udf`, `@pandas_udf` e expressão nativa: quando cada uma se justifica

A hierarquia de preferência, em ordem, com o critério de decisão:

| Ordem | Ferramenta | Onde roda | Quando se justifica |
|---|---|---|---|
| 1 | **expressão nativa** (`pyspark.sql.functions`, SQL) | dentro do codegen da JVM | **sempre que existir**. Antes de escrever qualquer UDF, procure a função nativa. O Spark 4.x tem centenas, incluindo `try_*`, colações, `parse_json` com VARIANT e UDF em SQL |
| 2 | **`@pandas_udf`** e as Pandas Function APIs (`mapInPandas`, `applyInPandas`, `applyInArrow`, `cogroup().applyInPandas`) | worker Python, um lote por chamada | quando a lógica exige Python de verdade: modelo de ML, biblioteca científica, parsing que nenhuma função nativa faz. Também quando a operação muda o número de linhas (`mapInPandas`) |
| 3 | **`@udf`** escalar | worker Python, uma chamada por linha | quando a função é irredutivelmente por elemento e o volume é pequeno, ou quando reescrever para lote não paga. Em 4.2 já sai por Arrow, então o custo caiu; a chamada por linha continua |
| 4 | **`df.rdd.map`** | worker Python, sem Catalyst e sem Tungsten | não. Você paga a fronteira **e** perde o motor. Ver seção 7 |

As formas de `pandas_udf` que a documentação oficial lista, e para que serve cada uma:

| Assinatura | Uso |
|---|---|
| `pd.Series -> pd.Series` | transformação escalar vetorizada |
| `Iterator[pd.Series] -> Iterator[pd.Series]` | estado caro de inicializar (carregue o modelo **uma vez por partição**, não por lote) |
| `Iterator[Tuple[pd.Series, ...]] -> Iterator[pd.Series]` | idem, com várias colunas de entrada |
| `pd.Series -> Any` | agregação, usada em `groupBy().agg()` |
| `groupby().applyInPandas()` | grouped map, recebe o **grupo inteiro** |
| `mapInPandas()` | mapeia iterador de DataFrames, pode mudar o número de linhas |
| `groupby().cogroup().applyInPandas()` | join de dois grupos em pandas |

Duas armadilhas de lote que mordem em produção. Primeira: `spark.sql.execution.arrow.maxRecordsPerBatch` tem default 10.000, e em tabela muito larga isso vira pico de memória; reduza. Segunda: historicamente esse teto **não** se aplicava a `applyInPandas`, que carrega o grupo inteiro em memória e com chave enviesada é OOM garantido. **Fora da bibliografia e mudança recente:** o 4.1 trouxe [SPARK-53562](https://issues.apache.org/jira/browse/SPARK-53562), "Limit Arrow batch sizes in applyInArrow and applyInPandas" ([release 4.1.0](https://spark.apache.org/releases/spark-release-4.1.0.html)), o que altera esse conselho. Não reconferi o comportamento exato do limite em `applyInPandas` na 4.2, então mantenho o aviso e marco a mudança.

Vale registrar o que 4.1 e 4.2 acrescentaram nessa camada, porque muda o repertório:

| Ticket | Versão | O que trouxe |
|---|---|---|
| [SPARK-52214](https://issues.apache.org/jira/browse/SPARK-52214) | 4.1.0 | Python Arrow UDF: decorador nativo de Arrow, sem passar por pandas |
| [SPARK-53592](https://issues.apache.org/jira/browse/SPARK-53592) | 4.1.0 | `@udf` passa a suportar UDF vetorizada |
| [SPARK-52979](https://issues.apache.org/jira/browse/SPARK-52979) | 4.1.0 | Python Arrow UDTF |
| [SPARK-54226](https://issues.apache.org/jira/browse/SPARK-54226) | 4.1.0 | compressão Arrow estendida a pandas UDF |
| [SPARK-53615](https://issues.apache.org/jira/browse/SPARK-53615) e [SPARK-53616](https://issues.apache.org/jira/browse/SPARK-53616) | 4.2.0 | API de iterador para UDF de agregação agrupada, em Arrow e em pandas |
| [SPARK-54337](https://issues.apache.org/jira/browse/SPARK-54337) | 4.2.0 | suporte a PyCapsule, a interface C de dados do Arrow |
| [SPARK-54962](https://issues.apache.org/jira/browse/SPARK-54962) | 4.2.0 | correção de inteiro nulável em pandas UDF que perdia precisão em valor grande, silenciosamente |

Esse último merece nota: era perda **silenciosa** de precisão em pandas UDF, corrigida agora. Se você tem pipeline com inteiro grande passando por pandas UDF em 4.0 ou 4.1, isso é motivo para conferir.

---

### 5. A afirmação de bytecode idêntico entre linguagens

O pré-aula registra o problema com precisão em [R2-9](01-pre-aula.md#dúvidas-damji-capítulo-3): o capítulo afirma que os dois blocos, DataFrame em Python e SQL, terminam com bytecode idêntico, e na frase seguinte diz que o bytecode resultante é **provavelmente** o mesmo. Duas forças para a mesma afirmação em duas linhas, sem condição declarada.

A afirmação forte não é errada; é mal delimitada. Vale delimitá-la.

#### Onde vale

**Vale para expressão nativa de DataFrame e para SQL.** O mecanismo é o que a Figura 3-4 do próprio livro desenha: SQL, DataFrame e Dataset entram no **mesmo** cano. A API de qualquer linguagem produz um plano lógico não resolvido; a partir dali só existe Catalyst. Não há caminho de código por linguagem, então não há como o resultado depender da linguagem. É a base material da uniformidade, e é o motivo pelo qual a briga "Scala contra Python" é uma não-questão enquanto você fica dentro das expressões de coluna.

No Spark Connect isso fica ainda mais literal: o protocolo **é** o plano lógico não resolvido, codificado em protobuf ([Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html)). O cliente manda o plano; o servidor faz tudo. Cliente Python, Scala, Swift ou JDBC mandam a mesma coisa.

#### Onde deixa de valer, e por que "provavelmente" é a palavra honesta mesmo no caso bom

Duas famílias de exceção. A primeira é sobre o próprio caso bom: **o bytecode não é função só da consulta**.

- **AQE reotimiza com estatística de execução.** O plano físico é fatiado em estágios e, ao fim de cada shuffle, o Spark decide o resto com o dado real. Duas execuções da mesma consulta podem gerar planos físicos diferentes, logo código gerado diferente. A afirmação "idêntico" é sobre o plano, e o plano não é determinístico entre execuções.
- **Codegen tem teto.** Acima de um número de campos, controlado por `spark.sql.codegen.maxFields`, o Spark cai para execução interpretada em vez de gerar código. **Não consegui abrir a tabela de configuração de SQL da 4.2 nesta rodada** para reconferir o default (a página de configuração trunca antes da seção de SQL); a aula 01 registrou 100. Trate o número como pendente de conferência, e o mecanismo como certo.
- **Config de sessão muda o plano.** ANSI, pushdown por fonte, limiar de broadcast, tudo isso entra na conta.

A segunda família é a que interessa a esta parte: **as construções em que a linguagem volta a importar**.

| Construção | O bytecode é o mesmo? | Por quê |
|---|---|---|
| expressão nativa de DataFrame, SQL | sim, com as ressalvas acima | mesmo plano, mesmo Catalyst, mesmo codegen |
| UDF Python (`@udf`, `@pandas_udf`) | **não** | parte do trabalho roda em CPython, fora do bytecode. O nó `ArrowEvalPython` no plano é a prova visível |
| UDF Scala ou Java registrada | não é a mesma coisa que UDF Python | roda **dentro** da JVM, sem fronteira de processo. Continua opaca ao Catalyst, mas não paga serialização nem worker. É aqui que a paridade morre de forma mais desconfortável: Scala e Python **não** empatam em UDF |
| API de RDD em PySpark | **não** | objetos pickle e um worker Python por task. Não há relação com o bytecode que o Scala geraria |
| `Dataset[T]` com lambda tipada | não se aplica a Python, e difere de DataFrame | insere `DeserializeToObject`, `MapElements`, `SerializeFromObject`. O plano do Scala tipado não é o plano do DataFrame equivalente |
| pandas API on Spark, `toPandas()`, `toArrow()` | não | as duas últimas coletam para o driver |

A formulação que eu levo para a aula, e que corrige o livro sem contradizê-lo: **a paridade é propriedade da API de expressão, não da linguagem.** Quem escreve PySpark inteiro em expressão de coluna tem a performance do Scala. Quem escreve uma UDF sai da propriedade, e o custo de sair é maior em Python do que em Scala, porque em Python a fronteira é de processo e em Scala é só de otimização.

---

### 6. A Python Data Source API, e a pergunta que nenhum livro faz

O pré-aula registra a lacuna duas vezes: na tese de defasagem ("falta a Python Data Source API (4.0), que muda a resposta à pergunta 'e se meu formato não estiver na lista', pergunta que nenhum dos livros faz") e no vocabulário, na linha de **camada de fontes de dados**, onde o Luu 3.2 diz que a camada é extensível e não diz como se estende.

**Fora da bibliografia, por construção: a API é posterior a todos os três livros.**

#### O que é, e desde quando

Entrou no Spark 4.0 pelo SPIP [SPARK-44076](https://issues.apache.org/jira/browse/SPARK-44076), "Python Data Source API", listado entre os destaques de PySpark do [release 4.0.0](https://spark.apache.org/releases/spark-release-4-0-0.html). Serve para ler de fonte customizada e escrever em destino customizado **em Python**, com a mesma interface de `spark.read.format(...)` e `writeStream.format(...)` que as fontes embutidas usam. Documentação: [Python Data Source API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html).

#### O que você implementa

| Classe | Métodos | Para quê |
|---|---|---|
| `DataSource` | `name()` (classmethod), `schema()`, `reader(schema)`, `writer(schema, overwrite)`, `streamReader(schema)` ou `simpleStreamReader(schema)`, `streamWriter(schema, overwrite)` | o ponto de entrada. Você implementa só os métodos das capacidades que quer oferecer |
| `DataSourceReader` | `partitions()`, `read(partition)` | `partitions()` devolve a lista de `InputPartition`; `read()` roda **por partição** e produz as linhas |
| `DataSourceWriter` | `write(rows)`, `commit(messages)`, `abort(messages)` | escrita em duas fases, com mensagem de commit por partição |
| `DataSourceStreamReader` | `initialOffset()`, `latestOffset(start, limit)`, `partitions(start, end)`, `read(partition)`, `commit(end)` | streaming com offset explícito |
| `SimpleDataSourceStreamReader` | `initialOffset()`, `read(start)`, `readBetweenOffsets(start, end)`, `commit(end)` | alternativa mais simples, sem planejar partição |
| `DataSourceStreamWriter` | `write(iterator)`, `commit(messages, batchId)`, `abort(messages, batchId)` | escrita por microlote |

Registro e uso:

```python
spark.dataSource.register(MinhaFonte)

spark.read.format("minha_fonte").option("chave", "valor").load().show()
spark.read.format("minha_fonte").schema("nome string, empresa string").load()
```

#### O detalhe que faz a API valer a pena

`read()` pode fazer `yield` de tupla, de `Row` **ou de `pyarrow.RecordBatch`**. A documentação afirma que o caminho de `RecordBatch` dá ganho "of up to one order of magnitude", especialmente em dado grande, por evitar processamento linha a linha. Isto é a mesma física da seção 4: o transporte colunar mata a conversão por elemento. Aqui ela aparece na ingestão, e não na transformação.

#### Por que isso muda a resposta à pergunta

Antes do 4.0, "meu formato não está na lista" tinha três respostas, todas ruins para quem trabalha em Python:

1. Escrever um conector DSv2 em Scala ou Java, empacotar jar e distribuir com `--jars`. Muda de linguagem, muda de ciclo de build, muda de repositório.
2. Ler como `binaryFile` ou `text` e fazer o parsing numa UDF. Você entra no DataFrame por uma porta que **não** paraleliza pelo conteúdo do dado e paga a fronteira Python por linha, com o pushdown morto de saída.
3. Descer para RDD com `sc.parallelize` da lista de arquivos e `mapPartitions` para ler. Perde o motor inteiro e, como a seção 7 mostra, deixa de funcionar em Spark Connect.

Depois do 4.0, a resposta é uma classe Python no mesmo repositório do pipeline, registrada na sessão, com `partitions()` dando paralelismo de verdade ao escalonador e `read()` executando por partição no executor. O parsing acontece **uma vez na ingestão**, não por linha dentro de uma UDF no meio da consulta. E o resultado é um DataFrame com schema declarado, ou seja, tudo que vier depois volta a ser otimizável.

#### Limites e evolução, que a documentação declara

- Fonte embutida e fonte Scala ou Java **têm precedência** sobre fonte Python com o mesmo nome.
- Duas fontes Python podem registrar o mesmo nome; o registro posterior sobrescreve o anterior. O registro é de sessão ([SPARK-45600](https://issues.apache.org/jira/browse/SPARK-45600)).
- Todas as classes e métodos precisam ser serializáveis por pickle, e as bibliotecas usadas dentro dos métodos devem ser importadas **dentro** deles.
- O leitor roda em worker Python. O custo de fronteira da seção 4 continua existindo; o que muda é que ele acontece uma vez por partição na ingestão, com transporte colunar, em vez de por linha no meio do plano.

Evolução por versão, com ticket:

| Ticket | Versão | O que trouxe |
|---|---|---|
| [SPARK-47367](https://issues.apache.org/jira/browse/SPARK-47367) | 4.0.0 | fontes Python funcionam com **Spark Connect**. É o item que torna a API a substituta legítima das gambiarras com RDD |
| [SPARK-45597](https://issues.apache.org/jira/browse/SPARK-45597) | 4.0.0 | criar tabela usando fonte Python em SQL, via execução DSv2 |
| [SPARK-45525](https://issues.apache.org/jira/browse/SPARK-45525) | 4.0.0 | escrita por DSv2 |
| [SPARK-50471](https://issues.apache.org/jira/browse/SPARK-50471) | 4.0.0 | escritor baseado em Arrow |
| [SPARK-46424](https://issues.apache.org/jira/browse/SPARK-46424) | 4.0.0 | métricas Python na fonte |
| [SPARK-51271](https://issues.apache.org/jira/browse/SPARK-51271) | 4.1.0 | **API de filter pushdown** para fontes Python. Ou seja: a fonte Python pode receber o predicado e reduzir o dado que devolve |
| [SPARK-53030](https://issues.apache.org/jira/browse/SPARK-53030) | 4.1.0 | escritor Arrow para fonte de streaming |
| [SPARK-51919](https://issues.apache.org/jira/browse/SPARK-51919) | 4.1.0 | permite sobrescrever fonte Python registrada estaticamente |

O pushdown do 4.1 é o item mais interessante da lista para o argumento desta parte inteira: uma fonte Python bem escrita **participa** da otimização que a seção 2 disse que a lambda impede. A diferença é que aqui você declara a capacidade numa interface, em vez de esconder a lógica numa closure.

---

### 7. Quando descer para RDD faz sentido em 2026, e o que o Spark Connect fecha

#### Os casos que sobrevivem

Resposta honesta: **quase nunca em código novo, e menos ainda em PySpark.** O RDD não foi depreciado, continua sendo o substrato, e escrever contra ele hoje exige justificativa. O pré-aula registra que o Damji dá três cenários (pacote de terceiros escrito em RDD, poder dispensar as otimizações, e querer instruir o Spark com precisão sobre o **como**) e responde "a resounding no" à depreciação. Os cenários dele envelheceram desigualmente. Minha lista, com a ressalva de 2026 em cada uma:

| Caso | Ainda vale? | Ressalva |
|---|---|---|
| **Spark como escalonador de tarefa genérica** (N conversões de arquivo, N chamadas de API, N simulações) | é o caso mais legítimo que resta | em PySpark, `mapInPandas` sobre um DataFrame de parâmetros costuma dar o mesmo com integração melhor: métricas na UI, AQE, tolerância a falha por task |
| **Particionador customizado** | sim, `partitionBy(new MeuPartitioner)` não tem equivalente exato em DataFrame | `repartition` e `repartitionByRange` cobrem hash e faixa; com AQE, o caso rareia |
| **Formato fora da lista** | **não mais** | é justo o que a Python Data Source API do 4.0 resolve, e resolve melhor. Ver seção 6 |
| **Biblioteca RDD-only** (`org.apache.spark.mllib`, GraphX) | sim, por falta de sucessor | é dívida herdada, não escolha |
| **Diagnóstico** (`rdd.toDebugString`, `df.rdd.getNumPartitions()`) | sim, e é o uso mais comum de todos | não é escrever RDD, é inspecionar |

**O anti-caso a memorizar:** em PySpark, `df.rdd.map(f)` é o pior dos mundos. Você perde Catalyst e Tungsten **e** paga a fronteira Python por elemento, sem Arrow, porque o caminho de Arrow é da API estruturada. Um `df.rdd.map(...).toDF()` inocente custa ordens de magnitude mais que a expressão equivalente.

```python
# RUIM: sai do motor, serializa elemento a elemento para o Python
n = df.rdd.map(lambda r: r.valor * 1.1).sum()

# BOM: expressão de coluna, roda dentro do codegen da JVM
n = df.select(F.sum(F.col("valor") * 1.1)).first()[0]
```

#### O que o Spark Connect fecha, e é o argumento novo

Aqui está a mudança de arquitetura que nenhum dos livros pode ter, e ela é mais consequente que qualquer conselho de estilo. O [Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html) declara, sobre o protocolo:

> "the Spark Connect protocol does not support all the execution APIs of Spark, most importantly RDDs."

E, sobre o `SparkContext`: o cliente **não tem acesso** ao `SparkContext` estático, não acessa campos privados que guardam a implementação da JVM (como `df._jdf` no PySpark) e não usa Py4J. O protocolo é de sessão, então o cliente não alcança propriedade de cluster.

A consequência prática é direta: **se a sua organização roda Spark Connect, "descer para RDD" não é uma decisão de performance, é uma decisão de deployment.** Não existe `sc.parallelize`, não existe `df.rdd`, não existe `rdd.toDebugString`. O erro que o PySpark levanta é explícito quanto ao motivo, e diz que o atributo não é suportado em Spark Connect porque depende da JVM ([SPARK-45674](https://issues.apache.org/jira/browse/SPARK-45674) melhorou justamente essa mensagem). Como conferir antes de migrar: a referência de API do PySpark anota por função se ela funciona em Connect; **não reconferi função por função nesta rodada**, então trate a anotação como o lugar de checar, não como garantia que eu verifiquei.

E o Connect deixou de ser experimento. No 4.0, [SPARK-47540](https://issues.apache.org/jira/browse/SPARK-47540) entregou um pacote Python puro de cerca de 1,5 MB, um tarball com Connect habilitado por padrão, e [SPARK-50605](https://issues.apache.org/jira/browse/SPARK-50605) trouxe `spark.api.mode` para alternar entre Connect e Classic ([release 4.0.0](https://spark.apache.org/releases/spark-release-4-0-0.html)). Escolher RDD hoje é escolher ficar do lado Classic dessa chave.

#### A direção que o 4.2 aponta, e ela responde a pergunta melhor que qualquer opinião

O [release 4.2.0](https://spark.apache.org/releases/spark-release-4-2-0.html) traz um guarda-chuva chamado [SPARK-55227](https://issues.apache.org/jira/browse/SPARK-55227), de compatibilidade com a API de RDD. Note o que ele contém:

| Ticket | O que faz |
|---|---|
| [SPARK-55228](https://issues.apache.org/jira/browse/SPARK-55228) | `Dataset.zipWithIndex` na API Scala |
| [SPARK-55229](https://issues.apache.org/jira/browse/SPARK-55229) | `DataFrame.zipWithIndex` em PySpark |
| [SPARK-55090](https://issues.apache.org/jira/browse/SPARK-55090) | `DataFrame.toJSON` no cliente Python |
| [SPARK-56253](https://issues.apache.org/jira/browse/SPARK-56253), [SPARK-56254](https://issues.apache.org/jira/browse/SPARK-56254), [SPARK-56255](https://issues.apache.org/jira/browse/SPARK-56255) | `spark.read.json`, `.xml` e `.csv` aceitam DataFrame como entrada |
| [SPARK-56256](https://issues.apache.org/jira/browse/SPARK-56256) | `SparkSession.emptyDataFrame` |

Leia a lista pelo que ela **não** faz: ninguém está expondo RDD ao Connect. O projeto está **portando para DataFrame as operações que só existiam em RDD**, uma por uma, para que o motivo de descer desapareça. `zipWithIndex` era o exemplo clássico de "para isso preciso de RDD"; virou método de DataFrame em 4.2. Ler JSON a partir de um DataFrame de strings era `df.rdd.map(...)` seguido de `spark.read.json(rdd)`; virou entrada aceita.

Então a resposta para "quando descer para RDD em 2026" tem uma parte estática e uma dinâmica. Estática: escalonamento de tarefa genérica, particionador customizado, biblioteca legada, diagnóstico. Dinâmica: **a lista encurta a cada release, de propósito**, e o teste que você deve aplicar antes de escrever um RDD é procurar o método equivalente de DataFrame na versão que você roda, porque ele pode ter nascido no ciclo passado.

---

### Perguntas que a parte 4 abriu

**1. Cinco atributos, três ou dois?** O scaladoc de `RDD.scala` diz cinco, com dois marcados como opcionais, e é o texto que o Luu reproduz. Mas só `compute` e `getPartitions` são abstratos, e o Damji conta três porque dobra a localidade dentro das partições. Qual contagem o senhor cobra numa prova, e considera a divergência resolvida pela fonte?
*Hipótese: cinco é a definição (é o texto do projeto), três é a linhagem (e aí Luu e Damji concordam, porque o Luu diz que as três primeiras peças formam a linhagem), dois é o que se implementa. A pergunta "quantos" está mal posta sem dizer para quê.*

**2. `partitioner` ausente custa um shuffle?** O Luu marca as peças 4 e 5 como opcionais e não diz o que se perde. Dá para demonstrar em aula um `join` que ganha um `Exchange` a mais só porque o particionador não foi declarado, com dois RDDs de mesmo número de partições?
*Hipótese: sim, e é a demonstração mais didática do assunto, porque é o mesmo raciocínio que sustenta bucketing na camada estruturada: declarar o layout evita a troca.*

**3. Com Arrow por padrão no 4.2, `@udf` virou aceitável?** [SPARK-54555](https://issues.apache.org/jira/browse/SPARK-54555) ligou Arrow para UDF Python comum, e a documentação confirma o default. A hierarquia de preferência muda?
*Hipótese: não muda a ordem, muda a distância. Arrow mata a serialização por linha e não a chamada por linha, então `@pandas_udf` continua acima de `@udf`, mas a diferença encolheu o suficiente para que reescrever UDF escalar existente deixe de ser prioridade.*

**4. `BatchEvalPython` num plano de 4.2 é sempre sintoma?** O código de `ExtractPythonUDFs` tem fallback comentado como "use `BatchEvalPython` if UDT is detected", com aviso de Arrow desligado.
*Hipótese: sim, é sintoma. Em 4.2, `BatchEvalPython` significa config desligada ou UDT na entrada ou na saída, e nos dois casos vale investigar antes de aceitar o plano.*

**5. Que heurística de `memoryOverhead` para PySpark pesado?** O default é 10% do heap, exceto 0,40 em jobs não-JVM no Kubernetes, e a documentação diz que o Python cabe nesse overhead quando `spark.executor.pyspark.memory` não está setado. O senhor cravaria `pyspark.memory` em produção?
*Hipótese: sim, mesmo sem precisar do limite, porque ele troca "container morto pelo orquestrador sem explicação" por erro de memória do lado do Python, que é legível. O custo é ter de dimensionar o número.*

**6. Python Data Source contra `binaryFile` mais UDF: onde está a fronteira?** A doc promete até uma ordem de magnitude com `yield` de `pyarrow.RecordBatch`, e o 4.1 acrescentou pushdown de filtro para fontes Python.
*Hipótese: a fonte Python ganha por dois motivos que não são linguagem, e sim arquitetura: `partitions()` dá paralelismo declarado ao escalonador, e o pushdown de [SPARK-51271](https://issues.apache.org/jira/browse/SPARK-51271) devolve à fonte a otimização que uma UDF de parsing destrói. Abaixo de alguns arquivos, `binaryFile` mais UDF é mais barato de escrever e igual de rápido.*

**7. Dataset tipado bate DataFrame em algum caso?** O Damji põe "espaço e velocidade" na coluna do DataFrame e "encoders do Tungsten" na do Dataset, sem explicar. O custo real é `DeserializeToObject`, `MapElements`, `SerializeFromObject` e a quebra de codegen.
*Hipótese: não bate em transformação relacional, empata quando a lógica é irredutivelmente por objeto (e aí a alternativa também seria opaca), e a recomendação prática é usar `Dataset[T]` pelo contrato de tipo mas escrever as operações com expressão de coluna, porque `filter` é sobrecarregado e a escolha da sobrecarga decide se há pushdown.*

**8. A disciplina vai usar Spark Connect ou Classic?** Não é pergunta de curiosidade: o Connect não expõe `SparkContext` nem RDD, e o 4.2 está portando para DataFrame as operações que só existiam em RDD ([SPARK-55227](https://issues.apache.org/jira/browse/SPARK-55227)).
*Hipótese: o curso vai usar Classic local, porque é o que os livros pressupõem e é onde `sc.parallelize` roda. Se a resposta for Connect, metade dos exemplos de RDD da bibliografia não executa, e isso vale um aviso no início da aula.*
---

## Parte 5 - O que mudou desde os livros, e o que a bibliografia não cobre

Os três livros desta aula estão desalinhados no tempo por até seis anos. O Damji cobre o Spark 3.0 e saiu em 2020. O Luu cobre 3.0 e 3.1 e saiu em 2021. O Chadha é o mais novo, e o próprio capítulo se data pelos quinze links de *See also* rotulados Spark 3.4.0 e pelo pacote `spark-xml_2.12:0.16.0`. A versão estável de hoje é a **4.2.0, de 14 de julho de 2026**. Entre o texto mais recente da bibliografia e o motor de hoje há três releases de feature na linha 4.x e uma na 3.x.

Esta parte tem duas metades. A primeira é cronológica e propositalmente estreita: só o que afeta **APIs estruturadas, schema e persistência**. AQE, Dynamic Partition Pruning e Spark Connect em geral ficaram na Parte 5 da aula 01 e não voltam aqui, exceto onde tocam leitura ou escrita. A segunda metade é sobre lacunas, ou seja, coisas que os três livros não cobrem e que caem no trabalho de todo dia: leitura paralela de JDBC, UDF, formato de tabela transacional, e como rodar os exemplos do Damji fora da Databricks.

Toda afirmação de versão, default e ticket aqui tem link para fonte primária. Onde eu não achei fonte primária, está dito.

---

### 1. Linha do tempo, release por release, no que toca leitura, schema e escrita

A bibliografia para entre 3.0 e 3.4. O que veio depois, filtrado pelo escopo desta aula:

| Versão | Data | O que muda para quem escreve leitura, transformação e escrita |
|---|---|---|
| 3.2.0 | 13/10/2021 | modo ANSI vira **GA**, ainda desligado ([SPARK-35030](https://issues.apache.org/jira/browse/SPARK-35030)); **tipos de intervalo ANSI** ([SPARK-27790](https://issues.apache.org/jira/browse/SPARK-27790)); novas regras de coerção de tipo em modo ANSI ([SPARK-34246](https://issues.apache.org/jira/browse/SPARK-34246)); opções de rebasing de data e hora na leitura de Parquet ([SPARK-34377](https://issues.apache.org/jira/browse/SPARK-34377)) e de Avro ([SPARK-34404](https://issues.apache.org/jira/browse/SPARK-34404)); leitura de inteiro sem sinal em Parquet ([SPARK-34817](https://issues.apache.org/jira/browse/SPARK-34817), [SPARK-34786](https://issues.apache.org/jira/browse/SPARK-34786)) |
| 3.3.0 | 16/06/2022 | família `try_*` fechada: `try_sum` ([SPARK-38548](https://issues.apache.org/jira/browse/SPARK-38548)), `try_avg` ([SPARK-38589](https://issues.apache.org/jira/browse/SPARK-38589)), `try_element_at` ([SPARK-37533](https://issues.apache.org/jira/browse/SPARK-37533)), `try_to_binary` ([SPARK-38590](https://issues.apache.org/jira/browse/SPARK-38590)), `try_subtract` e `try_multiply` ([SPARK-38164](https://issues.apache.org/jira/browse/SPARK-38164)); pushdown de agregação em DSv2 ([SPARK-37644](https://issues.apache.org/jira/browse/SPARK-37644)), de `LIMIT` ([SPARK-37020](https://issues.apache.org/jira/browse/SPARK-37020)) e de Top-N para JDBC v2 ([SPARK-37483](https://issues.apache.org/jira/browse/SPARK-37483)); pushdown de min, max e count em Parquet ([SPARK-36645](https://issues.apache.org/jira/browse/SPARK-36645)); referência ao registro corrompido de CSV corrigida ([SPARK-38523](https://issues.apache.org/jira/browse/SPARK-38523)); nulo em CSV deixa de sair como `""` ([SPARK-37575](https://issues.apache.org/jira/browse/SPARK-37575)) |
| 3.4.0 | 13/04/2023 | **`TIMESTAMP WITHOUT TIMEZONE`** ([SPARK-35662](https://issues.apache.org/jira/browse/SPARK-35662)); valor `DEFAULT` em coluna ([SPARK-38334](https://issues.apache.org/jira/browse/SPARK-38334)); inferência de `DATE` em CSV ([SPARK-39469](https://issues.apache.org/jira/browse/SPARK-39469)); `ignoreCorruptFiles` e `ignoreMissingFiles` como **opção de fonte**, e não só como config de sessão ([SPARK-38767](https://issues.apache.org/jira/browse/SPARK-38767)); pushdown de `StringEndsWith` e `Contains` para Parquet ([SPARK-39002](https://issues.apache.org/jira/browse/SPARK-39002)); padding de `CharType` na leitura de arquivo externo ([SPARK-40697](https://issues.apache.org/jira/browse/SPARK-40697)); estatística de coluna em DSv2 ([SPARK-41378](https://issues.apache.org/jira/browse/SPARK-41378)) |
| 3.5.0 | 13/09/2023 | UDTF em Python ([SPARK-43797](https://issues.apache.org/jira/browse/SPARK-43797)) e UDTF otimizada por Arrow ([SPARK-43964](https://issues.apache.org/jira/browse/SPARK-43964)); cláusula `IDENTIFIER` ([SPARK-43205](https://issues.apache.org/jira/browse/SPARK-43205)); argumento nomeado em função SQL ([SPARK-43922](https://issues.apache.org/jira/browse/SPARK-43922)); `TimestampNTZType` **exposto em `pyspark.sql.types`** ([SPARK-43759](https://issues.apache.org/jira/browse/SPARK-43759)); `CHAR` e `VARCHAR` no catálogo JDBC ([SPARK-42904](https://issues.apache.org/jira/browse/SPARK-42904)); codec `lz4raw` no Parquet ([SPARK-43273](https://issues.apache.org/jira/browse/SPARK-43273)); DSv2 preserva nulabilidade em `CTAS` e `RTAS` ([SPARK-43390](https://issues.apache.org/jira/browse/SPARK-43390)) |
| **4.0.0** | **23/05/2025** | **modo ANSI por padrão** ([SPARK-44444](https://issues.apache.org/jira/browse/SPARK-44444)); tipo **VARIANT** ([SPARK-45827](https://issues.apache.org/jira/browse/SPARK-45827)); **Python Data Source API** ([SPARK-44076](https://issues.apache.org/jira/browse/SPARK-44076)); **fonte XML embutida** ([SPARK-44265](https://issues.apache.org/jira/browse/SPARK-44265)); **collations de string** ([SPARK-46830](https://issues.apache.org/jira/browse/SPARK-46830)); UDF escrita em SQL ([SPARK-46057](https://issues.apache.org/jira/browse/SPARK-46057)); variável de sessão ([SPARK-42849](https://issues.apache.org/jira/browse/SPARK-42849)); Scala 2.12 removido ([SPARK-45314](https://issues.apache.org/jira/browse/SPARK-45314)); JDK 8 e 11 removidos, JDK 17 vira o padrão ([SPARK-45315](https://issues.apache.org/jira/browse/SPARK-45315)); Python 3.8 removido ([SPARK-47993](https://issues.apache.org/jira/browse/SPARK-47993)) |
| **4.1.0** | **16/12/2025** | **VARIANT ligado por padrão** ([SPARK-54454](https://issues.apache.org/jira/browse/SPARK-54454)) com **shredding** em Parquet: inferência do schema de shredding na escrita ([SPARK-53659](https://issues.apache.org/jira/browse/SPARK-53659)), anotação do tipo ([SPARK-54306](https://issues.apache.org/jira/browse/SPARK-54306)) e leitura do tipo lógico ([SPARK-54410](https://issues.apache.org/jira/browse/SPARK-54410)); operador de dois-pontos para acessar campo de VARIANT ([SPARK-52494](https://issues.apache.org/jira/browse/SPARK-52494)); VARIANT em scan de CSV ([SPARK-51298](https://issues.apache.org/jira/browse/SPARK-51298)) e de XML ([SPARK-51503](https://issues.apache.org/jira/browse/SPARK-51503)); UDF Arrow ([SPARK-52214](https://issues.apache.org/jira/browse/SPARK-52214)) e UDTF Arrow ([SPARK-52979](https://issues.apache.org/jira/browse/SPARK-52979)); collation em nível de schema ([SPARK-52219](https://issues.apache.org/jira/browse/SPARK-52219)) e herdada por view ([SPARK-52338](https://issues.apache.org/jira/browse/SPARK-52338)); `MERGE INTO` com evolução de schema ([SPARK-54274](https://issues.apache.org/jira/browse/SPARK-54274)); constraint de tabela ([SPARK-51207](https://issues.apache.org/jira/browse/SPARK-51207)); pushdown de join em DSv2 ([SPARK-52187](https://issues.apache.org/jira/browse/SPARK-52187)); `NullType` em Parquet ([SPARK-54220](https://issues.apache.org/jira/browse/SPARK-54220)); Python mínimo sobe para 3.10 ([SPARK-52561](https://issues.apache.org/jira/browse/SPARK-52561)) |
| **4.2.0** | **14/07/2026** | tipos **`GEOMETRY` e `GEOGRAPHY`** com funções `ST_*` ([SPARK-51658](https://issues.apache.org/jira/browse/SPARK-51658)), **ligados por padrão** ([SPARK-56771](https://issues.apache.org/jira/browse/SPARK-56771)), com leitura ([SPARK-55261](https://issues.apache.org/jira/browse/SPARK-55261)) e escrita ([SPARK-55260](https://issues.apache.org/jira/browse/SPARK-55260)) em Parquet e registro de SRS montado com dados do PROJ 9.7.1 ([SPARK-55790](https://issues.apache.org/jira/browse/SPARK-55790)); **CDC com cláusula SQL `CHANGES`** ([SPARK-55668](https://issues.apache.org/jira/browse/SPARK-55668), [SPARK-55948](https://issues.apache.org/jira/browse/SPARK-55948)), API de DataFrame e Connect ([SPARK-55949](https://issues.apache.org/jira/browse/SPARK-55949)) e binding de PySpark ([SPARK-55950](https://issues.apache.org/jira/browse/SPARK-55950)); **UDF Python Arrow e IPC Arrow por padrão** ([SPARK-54555](https://issues.apache.org/jira/browse/SPARK-54555)); **transação em DSv2** ([SPARK-55855](https://issues.apache.org/jira/browse/SPARK-55855)); `WITH SCHEMA EVOLUTION` no `INSERT` ([SPARK-54971](https://issues.apache.org/jira/browse/SPARK-54971), [SPARK-55689](https://issues.apache.org/jira/browse/SPARK-55689), [SPARK-55690](https://issues.apache.org/jira/browse/SPARK-55690)); Java 25 ([SPARK-51167](https://issues.apache.org/jira/browse/SPARK-51167)); Scala empacotada sobe de 2.13.17 para 2.13.18 |

Fontes: release notes de [3.2.0](https://spark.apache.org/releases/spark-release-3-2-0.html), [3.3.0](https://spark.apache.org/releases/spark-release-3-3-0.html), [3.4.0](https://spark.apache.org/releases/spark-release-3-4-0.html), [3.5.0](https://spark.apache.org/releases/spark-release-3-5-0.html), [4.0.0](https://spark.apache.org/releases/spark-release-4-0-0.html), [4.1.0](https://spark.apache.org/releases/spark-release-4.1.0.html) e [4.2.0](https://spark.apache.org/releases/spark-release-4-2-0.html).

Um padrão salta da tabela e vale dizer em voz alta: **o eixo do desenvolvimento do Spark nos últimos cinco anos foi tipo e contrato**, não API. Nenhum método novo de DataFrame aparece na coluna da direita. O que aparece é tipo novo (intervalo, NTZ, VARIANT, collation, geoespacial), semântica nova de erro (ANSI, `try_*`) e caminho novo de extensão (Python Data Source API, DSv2 com transação e evolução de schema). Os cinco capítulos lidos ensinam a API certa; ensinam o sistema de tipos e o contrato de erro **errados**, porque os dois mudaram por baixo.

---

### 2. Modo ANSI por padrão: a mudança que reescreve o capítulo de cast

Esta é a alteração de maior impacto para quem lê estes capítulos, e é a única que pode **quebrar código que hoje roda**.

**O que a bibliografia registra.** O Luu 3.2 é o único dos três que trata cast e registro ruim de frente: declara que o comportamento padrão é **nulificar todas as colunas da linha** quando o parse falha e nomeia `failFast` como única alternativa. O Damji 4 dá os três nomes (`PERMISSIVE`, `DROPMALFORMED`, `FAILFAST`) numa célula de tabela e remete à documentação. O Chadha nomeia um modo só e descreve a semântica errado. Nenhum dos três cita `spark.sql.ansi.enabled`.

**O que vale hoje.** A configuração [`spark.sql.ansi.enabled` aparece na tabela de configs do SQL com valor `true`](https://spark.apache.org/docs/latest/configuration.html), e o [guia de migração de SQL](https://spark.apache.org/docs/latest/sql-migration-guide.html) diz, sobre a passagem de 3.5 para 4.0, que a config está ligada por padrão e que para voltar ao comportamento anterior é preciso pôr `spark.sql.ansi.enabled` em `false` ou a variável de ambiente `SPARK_ANSI_SQL_MODE` em `false`. O ticket é [SPARK-44444](https://issues.apache.org/jira/browse/SPARK-44444).

O que muda, com os exemplos da própria [página de conformidade ANSI](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html):

| Operação | ANSI desligado (o mundo dos livros) | ANSI ligado (default desde 4.0) |
|---|---|---|
| `SELECT 2147483647 + 1` | dá a volta e devolve `-2147483648`; em decimal devolve `NULL` | `SparkArithmeticException` com condição `ARITHMETIC_OVERFLOW` |
| `SELECT CAST('a' AS INT)` | `NULL` | `SparkNumberFormatException` |
| divisão por zero | `NULL` | exceção em tempo de execução |

A leitura política disso: **overflow silencioso deixou de ser default**. Em pipeline de dados, `NULL` que aparece por overflow é pior que falha, porque contamina agregação a jusante sem deixar rastro. A `spark.sql.storeAssignmentPolicy` também [já vale `ANSI` por padrão](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html), o que restringe o que o `INSERT` aceita converter em silêncio.

**A saída correta não é desligar a config.** É usar as funções que absorvem o erro onde o erro é esperado: `try_cast`, que a documentação descreve como idêntico a `CAST` mas devolvendo `NULL` em vez de lançar exceção, e a família `try_add`, `try_subtract`, `try_multiply`, `try_divide`, `try_mod`, `try_sum`, `try_avg`. A diferença de desenho em relação ao mundo do Luu é grande: antes o default era tolerante e você optava por rigor com `failFast`; hoje o default é rigoroso e você opta por tolerância, **expressão por expressão**, em vez de arquivo por arquivo. É a granularidade que muda, não só o valor do default.

Um efeito colateral que morde quem usa pandas API on Spark: o [guia de migração do PySpark](https://spark.apache.org/docs/latest/api/python/migration_guide/pyspark_upgrade.html) diz que, no 4.0, a pandas API on Spark **levanta exceção** se o Spark estiver com ANSI ligado, e manda desligar a config ou pôr a opção `compute.fail_on_ansi_mode` em `False`.

---

### 3. VARIANT: o tipo que aposenta `get_json_object`

**O que a bibliografia registra.** O Chadha receita `get_json_object()` e `json_tuple()` sobre coluna de string JSON, e não explica a gramática das expressões de caminho: `$.name` aparece e sai. O Damji 3 lista tipos complexos (`ArrayType`, `MapType`, `StructType`) e nada para dado semiestruturado de schema instável. A tese do pré-aula sobre lista de tipos sem VARIANT foi confirmada nas cinco leituras, por construção: o tipo é posterior aos três livros.

**O que vale hoje.** VARIANT entrou no 4.0 por [SPARK-45827](https://issues.apache.org/jira/browse/SPARK-45827) e foi **ligado por padrão no 4.1** por [SPARK-54454](https://issues.apache.org/jira/browse/SPARK-54454). O 4.1 trouxe o **shredding**, que materializa subcampos frequentes como campos Parquet tipados: inferência do schema de shredding na escrita ([SPARK-53659](https://issues.apache.org/jira/browse/SPARK-53659)), anotação do tipo na escrita ([SPARK-54306](https://issues.apache.org/jira/browse/SPARK-54306)) e leitura do tipo lógico de Variant do Parquet ([SPARK-54410](https://issues.apache.org/jira/browse/SPARK-54410)). O mesmo release adicionou o **operador de dois-pontos** para acessar campo ([SPARK-52494](https://issues.apache.org/jira/browse/SPARK-52494)) e suporte a VARIANT em scan de CSV ([SPARK-51298](https://issues.apache.org/jira/browse/SPARK-51298)) e de XML ([SPARK-51503](https://issues.apache.org/jira/browse/SPARK-51503)).

Por que isso resolve o problema do Chadha: com `get_json_object`, cada acesso a campo **reparseia a string** naquela linha. O texto binário fica na coluna, e o custo de parse é pago por linha e por acesso. VARIANT guarda a representação já parseada em formato binário, então o acesso é navegação, não parse. Com shredding, os campos que você acessa sempre viram coluna física de Parquet, e aí voltam a valer pushdown e poda de coluna, as duas otimizações que o Luu 3.1 lista como impossíveis sobre dado opaco.

Uma nota de honestidade sobre documentação: **`VariantType` não aparece na [página de referência de tipos de dado do 4.2.0](https://spark.apache.org/docs/latest/sql-ref-datatypes.html)**, embora `GeometryType`, `GeographyType`, `TimestampNTZType` e os dois tipos de intervalo apareçam. Conferi a página em busca da palavra e as duas únicas ocorrências de "variant" estão na prosa que descreve `CharType` e `VarcharType` como variantes de outro tipo. O tipo tem [página de API em PySpark](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.types.VariantType.html) e tickets de release, mas não achei página de referência de SQL dedicada a ele nas rotas óbvias (`sql-ref-variant.html` e `sql-data-sources-variant.html` devolvem 404). Ou seja: o tipo é GA e a documentação de referência ainda não o absorveu.

---

### 4. Python Data Source API: a resposta à pergunta que nenhum livro faz

**O que a bibliografia registra.** O Damji 4 lista as fontes embutidas e desenvolve Parquet, JSON, CSV, Avro, ORC, imagens e arquivos binários. O Luu 3.2 diz, na Tabela 3-2, que a camada de fontes de dado é extensível e que fonte customizada exige nome totalmente qualificado, e para aí. **Nenhum dos três faz a pergunta "e se o meu formato não estiver na lista"**, que é a pergunta de todo dia de quem extrai dado de API paginada, de webservice ou de arquivo proprietário.

**O que vale hoje.** A [Python Data Source API](https://spark.apache.org/docs/latest/api/python/tutorial/sql/python_data_source.html) entrou no 4.0 por [SPARK-44076](https://issues.apache.org/jira/browse/SPARK-44076). A documentação abre dizendo que a API "is a new feature introduced in Spark 4.0, enabling developers to read from custom data sources and write to custom data sinks in Python". O molde: você herda de `DataSource` e implementa `reader()` para batch de leitura, `writer()` para batch de escrita, `streamReader()` ou `simpleStreamReader()` para leitura de streaming e `streamWriter()` para escrita de streaming. Registra com `spark.dataSource.register(MinhaFonte)` e depois usa exatamente o padrão de três peças que o Luu 3.2 ensina:

```python
spark.dataSource.register(MinhaFonte)
spark.read.format("minha_fonte").option("numRows", 5).schema("nome string, empresa string").load()
```

O ponto que importa para a aula: isso muda a resposta à pergunta de arquitetura. Antes, "meu formato não está na lista" significava ou escrever conector em Scala sobre DSv2, ou fazer ingestão fora do Spark e aterrissar arquivo. Agora o extractor de API que você já escreveu em Python vira **fonte de primeira classe**, com schema declarado, particionamento e integração com o planejador. O preço é o mesmo de sempre em PySpark, e o 4.2 endureceu a checagem: o [guia de migração](https://spark.apache.org/docs/latest/api/python/migration_guide/pyspark_upgrade.html) diz que uma Python Data Source cujo dado Arrow devolvido tenha tipo de coluna divergente do schema declarado agora falha com `DATA_SOURCE_RETURN_SCHEMA_MISMATCH`, quando antes só divergência de nome e de contagem levantava o erro.

---

### 5. XML nativo, e o que fazer com o `spark-xml` do Chadha

Este é o caso mais bonito da bibliografia, porque o livro erra e o tempo o conserta pela metade.

**O que a bibliografia registra.** O Chadha abre a receita 4 prometendo explorar a fonte de dado XML **embutida** e, na nota logo abaixo, manda instalar `com.databricks:spark-xml_2.12:0.16.0`, apresentado como biblioteca de terceiros lançada pela Databricks. Embutida e de terceiros em duas frases seguidas. O `format` usado é `com.databricks.spark.xml`, e não `"xml"`. Para XML complexo, o texto manda usar `option("rootTag", ...)` na leitura. O *There's more* lista `excludeAttribute`, `inferSchema`, `ignoreSurroundingSpaces` e `mode` e credita as quatro ao Apache Spark.

**O que vale hoje**, item por item:

1. **A fonte XML é embutida de verdade desde o 4.0**, por [SPARK-44265](https://issues.apache.org/jira/browse/SPARK-44265). A [página de fonte XML do 4.2.0](https://spark.apache.org/docs/latest/sql-data-sources-xml.html) mostra `spark.read.option("rowTag", "person").format("xml").load(path)` e o atalho `.xml(path)`. O `format` longo do Chadha não é mais necessário.
2. **`rootTag` é opção de escrita**, com default `ROWS`, e `rowTag` é a que vale na leitura, exatamente como o pré-aula registrou. A documentação oficial confirma o escopo de cada uma. O conselho do livro está trocado.
3. **O repositório está arquivado.** A [página do `databricks/spark-xml`](https://github.com/databricks/spark-xml) traz o aviso "This repository was archived by the owner on Mar 24, 2025. It is now read-only", e o README dizia que o `spark-xml` estava planejado para virar parte do Apache Spark 4.0, com o pacote em modo de manutenção para as linhas 3.x. Conferido pela API do GitHub: `archived` é `true` e a última release é a **v0.18.0, de 10 de abril de 2024**. O Chadha pede a 0.16.0, ou seja, duas releases antes da última de um pacote morto.
4. **O pacote do livro não roda em Spark 4.** É `_2.12`, e o Scala 2.12 foi removido no 4.0 por [SPARK-45314](https://issues.apache.org/jira/browse/SPARK-45314).
5. **As opções ganharam nome oficial.** `mode` com default `PERMISSIVE`, `columnNameOfCorruptRecord` com default herdado de `spark.sql.columnNameOfCorruptRecord`, e a mesma tríade `PERMISSIVE` / `DROPMALFORMED` / `FAILFAST` do CSV e do JSON, agora documentada para XML. A frase do Chadha que creditava as opções ao Apache Spark ficou certa por acidente, com dois anos de atraso.

Migração de uma linha, para quem seguir o livro em Spark 4.2:

```python
# receita do livro, hoje quebrada
df = spark.read.format("com.databricks.spark.xml").option("rowTag", "row").load(caminho)

# equivalente nativo
df = spark.read.format("xml").option("rowTag", "row").load(caminho)
```

---

### 6. Os tipos que os livros não têm

As tabelas de tipos do Damji 3 (3-2 a 3-5) e a Tabela 3-1 do Luu 3.2 são a informação mais consultável dos dois capítulos, e são as mais datadas. O pré-aula já registrou o que falta nelas: nada de `NullType`, nada de tipo de intervalo, nada de `CharType` nem `VarcharType`, nada de VARIANT nem geoespacial. A lista abaixo fecha o buraco com a [referência de tipos do 4.2.0](https://spark.apache.org/docs/latest/sql-ref-datatypes.html).

**`TimestampNTZType`, desde o 3.4.** O release notes do 3.4.0 registra `TIMESTAMP WITHOUT TIMEZONE` por [SPARK-35662](https://issues.apache.org/jira/browse/SPARK-35662); o 3.5.0 expôs o tipo em `pyspark.sql.types` por [SPARK-43759](https://issues.apache.org/jira/browse/SPARK-43759). A referência de tipos hoje descreve dois timestamps distintos: `TimestampType` é "Timestamp with local time zone (TIMESTAMP_LTZ)" e `TimestampNTZType` é "Timestamp without time zone (TIMESTAMP_NTZ)", com todas as operações feitas sem levar fuso em conta. Por que isso importa para o capítulo: o único `timestamp` que a bibliografia mostra é o `last_update` do Sakila, no schema JDBC do Luu 3.2, e o livro não comenta o tipo. Ler timestamp de banco relacional com semântica de LTZ quando a coluna de origem é sem fuso é uma das fontes mais comuns de deslocamento de hora em pipeline, e a distinção que resolve isso não existe em nenhum dos três textos.

**Tipos de intervalo, desde o 3.2.** [SPARK-27790](https://issues.apache.org/jira/browse/SPARK-27790) trouxe os tipos `INTERVAL` do ANSI SQL. A referência documenta `YearMonthIntervalType(startField, endField)`, com campos `MONTH` e `YEAR`, e `DayTimeIntervalType(startField, endField)`, com campos `SECOND`, `MINUTE`, `HOUR` e `DAY`. O pré-aula anotou que a ausência de intervalo nas tabelas do Damji **já era ausência em 2020**, porque o `CalendarIntervalType` existia e não era exposto ao usuário. A diferença de 2026 é que agora há dois tipos separados, expostos e com campos declaráveis, e a diferença de campo decide se subtrair duas datas devolve meses ou dias.

**Collations de string, desde o 4.0.** [SPARK-46830](https://issues.apache.org/jira/browse/SPARK-46830). A [página de `SHOW COLLATIONS` do 4.2.0](https://spark.apache.org/docs/latest/sql-ref-syntax-aux-show-collations.html) lista o catálogo e devolve, por collation, as colunas `NAME`, `LANGUAGE`, `COUNTRY`, `ACCENT_SENSITIVITY`, `CASE_SENSITIVITY`, `PAD_ATTRIBUTE` e `ICU_VERSION`. Exemplos de nome: `UTF8_BINARY`, `UTF8_LCASE`, `UNICODE`, `UNICODE_CI`, `UNICODE_AI`, `en_USA`, `en_USA_CI`. O 4.1 estendeu para collation em nível de schema ([SPARK-52219](https://issues.apache.org/jira/browse/SPARK-52219)) e herança da collation padrão do schema pela view ([SPARK-52338](https://issues.apache.org/jira/browse/SPARK-52338)). O que isso troca no dia a dia: comparação e agrupamento insensíveis a caixa e a acento deixam de exigir `lower()` e `regexp_replace()` na expressão, o que é bom para legibilidade e melhor ainda para o otimizador, porque função em cima de coluna costuma bloquear pushdown. Nenhuma das cinco leituras trata comparação de string sensível a caixa.

**`GEOMETRY` e `GEOGRAPHY`, desde o 4.2.** SPIP em [SPARK-51658](https://issues.apache.org/jira/browse/SPARK-51658), ligados por padrão em [SPARK-56771](https://issues.apache.org/jira/browse/SPARK-56771). A [página de tipos geoespaciais](https://spark.apache.org/docs/latest/sql-ref-geospatial-types.html) diz que os dois seguem a especificação Simple Feature Access do Open Geospatial Consortium, que os valores são representados como WKB em tempo de execução e que cada valor carrega um SRID. `GEOMETRY` é sistema cartesiano; `GEOGRAPHY` é latitude e longitude, com interpolação de aresta sempre esférica. Os dois podem ser fixados num SRID, como `geometry(4326)`, ou aceitar SRIDs mistos com `geometry(any)`, e em SQL a declaração exige SRID explícito ou `ANY`. Defaults que valem decorar: `ST_GeomFromWKB(wkb)` sem SRID devolve SRID **0**, e `ST_GeogFromWKB(wkb)` devolve SRID **4326**. E a ressalva que decide arquitetura de persistência, na letra da documentação: "Parquet, Delta, and Iceberg store geometry/geography with a fixed SRID per column. They do not support persisting `GEOMETRY(ANY)` or `GEOGRAPHY(ANY)`; mixed-SRID types exist for in-memory/query use only." Ou seja, o tipo misto é de consulta, não de tabela. O release notes do 4.2.0 lista também leitura e escrita em Parquet ([SPARK-55261](https://issues.apache.org/jira/browse/SPARK-55261), [SPARK-55260](https://issues.apache.org/jira/browse/SPARK-55260)), escrita de WKT ([SPARK-55339](https://issues.apache.org/jira/browse/SPARK-55339)), WKB para `GEOGRAPHY` ([SPARK-55449](https://issues.apache.org/jira/browse/SPARK-55449)), cast de `GEOGRAPHY` para `GEOMETRY` ([SPARK-55539](https://issues.apache.org/jira/browse/SPARK-55539)) e registro de SRS completo com dados do PROJ 9.7.1 ([SPARK-55790](https://issues.apache.org/jira/browse/SPARK-55790)).

**`CharType` e `VarcharType`.** Não são novidade de 4.x, mas faltam nas tabelas do Damji e valem registro porque a semântica surpreende: a referência diz que `CharType(n)` sempre devolve string de comprimento `n` e que comparação faz padding do mais curto, e que `VarcharType(length)` **falha a escrita** se a string excede o limite, com a ressalva de que o tipo só pode ser usado em schema de tabela, não em função ou operador. O 3.4 adicionou padding de char na leitura de arquivo externo ([SPARK-40697](https://issues.apache.org/jira/browse/SPARK-40697)) e o 3.5 levou `CHAR` e `VARCHAR` ao catálogo JDBC ([SPARK-42904](https://issues.apache.org/jira/browse/SPARK-42904)).

---

### 7. CDC nativo com a cláusula `CHANGES`

**O que a bibliografia registra.** Leitura incremental não aparece nos cinco capítulos. O mais perto é o Chadha, que recorta partição de Parquet por curinga de diretório (`.load(".../partitioned_recipes/DatePublished=2020-01*")`), o que resolve recorte de tempo por convenção de caminho e não sabe nada sobre mudança de linha. O Damji 4 não tem uma palavra sobre ler o que mudou desde a última execução.

**O que vale hoje.** O 4.2.0 trouxe CDC de primeira classe. O SPIP é [SPARK-55668](https://issues.apache.org/jira/browse/SPARK-55668) e as peças estão nomeadas no release notes: API de conector CDC em DSv2, resolução no analisador e a cláusula SQL `CHANGES` ([SPARK-55948](https://issues.apache.org/jira/browse/SPARK-55948)); API de DataFrame e suporte em Spark Connect ([SPARK-55949](https://issues.apache.org/jira/browse/SPARK-55949)); `changes()` em PySpark ([SPARK-55950](https://issues.apache.org/jira/browse/SPARK-55950)); regra `ResolveChangelogTable` para pós-processamento em batch ([SPARK-55952](https://issues.apache.org/jira/browse/SPARK-55952)); pós-processamento em streaming ([SPARK-56686](https://issues.apache.org/jira/browse/SPARK-56686)) e `netChanges` em leitura de streaming ([SPARK-56687](https://issues.apache.org/jira/browse/SPARK-56687)).

O ponto de desenho, e é o que importa: a cláusula é **do motor, não do formato**. Antes, ler mudança dependia da sintaxe do conector, e cada formato tinha a sua. Com `CHANGES`, a mesma consulta vale para qualquer conector DSv2 que implemente a API nova, e o Spark assume o pós-processamento comum: descartar carry-over de copy-on-write, detectar update e calcular mudança líquida por linha.

Ressalva de fonte, e ela é obrigatória aqui: **não achei página de referência de sintaxe da cláusula `CHANGES` na documentação do 4.2.0** pelas rotas de `sql-ref-syntax`. A forma `SELECT * FROM minha_tabela CHANGES FROM VERSION 10 TO VERSION 20` e a variante de streaming com `FROM STREAM ... CHANGES FROM VERSION 0` aparecem no [blog de anúncio do 4.2 da Databricks](https://www.databricks.com/blog/introducing-apache-spark-42), que é fonte secundária. Trate a sintaxe exata como pendente de confirmação; o que está confirmado por fonte primária é a existência da cláusula, o número do SPIP e a lista de tickets.

---

### 8. UDF Arrow por padrão: o 4.2 mexeu no chão de quem escreve PySpark

Esta é a mudança de 4.2 com maior chance de aparecer sem aviso num pipeline existente, e ela tem duas metades.

**O que a bibliografia registra.** Nada. **Não há uma única UDF nas 141 páginas** das cinco leituras. A tese do pré-aula sobre custo de UDF Python descrito sem Arrow ficou impossível de conferir, e o próprio pré-aula classificou isso como achado, não como falha do palpite: cinco capítulos sobre APIs estruturadas e persistência, e a construção em que a escolha de linguagem custa caro de verdade não aparece.

**O que vale hoje.** O [guia de migração do PySpark](https://spark.apache.org/docs/latest/api/python/migration_guide/pyspark_upgrade.html) diz, para a passagem de 4.1 a 4.2, com estas palavras:

- "In Spark 4.2, regular Python UDFs are Arrow-optimized by default. The configuration `spark.sql.execution.pythonUDF.arrow.enabled` now defaults to true."
- "In Spark 4.2, regular Python UDTFs are Arrow-optimized by default. The configuration `spark.sql.execution.pythonUDTF.arrow.enabled` now defaults to true."
- "In Spark 4.2, columnar data exchange between PySpark and the JVM uses Apache Arrow by default. The configuration `spark.sql.execution.arrow.pyspark.enabled` now defaults to true."

O ticket é [SPARK-54555](https://issues.apache.org/jira/browse/SPARK-54555), "Enable Arrow-optimized Python UDFs and Arrow-based PySpark IPC by default". O caminho até aqui: UDF Arrow nasceu opcional no 3.5 ([SPARK-43964](https://issues.apache.org/jira/browse/SPARK-43964) para UDTF), ganhou decorator próprio no 4.1 ([SPARK-52214](https://issues.apache.org/jira/browse/SPARK-52214) e [SPARK-52979](https://issues.apache.org/jira/browse/SPARK-52979)), e virou default no 4.2.

Como o [capítulo de UDF e UDTF do guia do PySpark](https://spark.apache.org/docs/latest/api/python/user_guide/udfandudtf.html) organiza o assunto, há três famílias: UDF escalar de Python, que opera linha a linha e serializa com **cloudpickle**, com o guia dizendo que ela "encounter performance bottlenecks, particularly when dealing with large data inputs and outputs"; pandas UDF, que recebe e devolve `Series` ou `DataFrame` e serializa com Arrow, operando bloco a bloco; e Arrow UDF, que recebe e devolve `pyarrow.Array` direto. O parâmetro `useArrow` liga a otimização por UDF (`@udf(returnType='int', useArrow=True)`), e a config faz o mesmo globalmente.

**Três armadilhas de migração**, todas do mesmo guia:

1. **PyArrow mínimo sobe de 15.0.0 para 18.0.0.** Ambiente que fixa `pyarrow<18` não sobe.
2. **Coluna de inteiro nulável muda de dtype em pandas UDF.** Se o lote traz nulo, a coluna chega como dtype de extensão (`Int8`, `Int16`, `Int32`, `Int64`) em vez de `float64`. Código que assumia `float64` precisa ser revisto. Esta é sutil e silenciosa: não quebra o import, quebra a aritmética.
3. **`createDataFrame` a partir de `ndarray` do NumPy agora exige PyArrow** e converte direto para Arrow, sem passar por pandas, o que pode mudar o schema inferido, porque passa a seguir o mapeamento de tipos do Arrow.

O que fica para a aula: o argumento clássico de que "UDF Python é caro, escreva em Scala" continua verdadeiro, mas o tamanho do custo mudou por default no 4.2, e a orientação prática deixou de ser "evite UDF" para virar "evite UDF onde há função embutida, e onde a UDF é inevitável confira se ela está no caminho Arrow".

---

### 9. Requisitos de runtime: o que simplesmente não sobe mais

A [página de visão geral da documentação do 4.2.0](https://spark.apache.org/docs/latest/index.html) diz, textualmente: "Spark runs on Java 17/21/25, Scala 2.13, Python 3.10+, and R 4.0+ (Deprecated). Java 25 prior to version 25.0.3 support is deprecated as of Spark 4.2.0. When using the Scala API, it is necessary for applications to use the same version of Scala that Spark was compiled for. Since Spark 4.0.0, it's Scala 2.13."

| Peça | O que a bibliografia assume | Hoje | Ticket |
|---|---|---|---|
| Scala | `spark-xml_2.12` no Chadha; Tabela 3-1 do Luu só mapeia tipos de Scala | **só 2.13**, empacotada 2.13.18 no 4.2.0 | [SPARK-45314](https://issues.apache.org/jira/browse/SPARK-45314) |
| Java | nenhum dos três capítulos declara versão de JDK | **17, 21 e 25**; 8 e 11 removidos no 4.0 | [SPARK-45315](https://issues.apache.org/jira/browse/SPARK-45315), [SPARK-51167](https://issues.apache.org/jira/browse/SPARK-51167) |
| Python | o Chadha nunca declara versão de Python nem de Spark | **3.10 ou mais** | [SPARK-47993](https://issues.apache.org/jira/browse/SPARK-47993) (3.8 fora), [SPARK-52561](https://issues.apache.org/jira/browse/SPARK-52561) (mínimo 3.10) |
| PyPy | não tratado | **sem suporte oficial no 4.2**, use CPython ([guia de migração](https://spark.apache.org/docs/latest/api/python/migration_guide/pyspark_upgrade.html)) | |

A consequência prática para o ambiente do Chadha: as imagens do `docker-compose` do livro carregam `spark-xml_2.12`, que não roda em runtime 2.13. E o comando do próprio livro já envelheceu por fora, porque o CLI `docker-compose` com hífen foi descontinuado; hoje é `docker compose`.

---

**Daqui em diante, a segunda metade: o que a bibliografia inteira não cobre.** As quatro seções seguintes são lacunas **da bibliografia como conjunto**, não de um livro. O critério para entrar aqui é duplo: o assunto não está em nenhuma das 141 páginas, e ele aparece na primeira semana de qualquer trabalho real de engenharia de dados.

---

### 10. Lacuna 1: leitura paralela de JDBC

O pré-aula é explícito: **o Luu é o único dos três que ensina JDBC, e ensina leitura serial sem avisar**. A seção declara quatro informações necessárias (driver, URL, autenticação, nome da tabela), a Tabela 3-6 dá três chaves (`url`, `dbtable`, `driver`), e `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions` e `fetchsize` não aparecem em nenhum dos três livros. O Damji 4 cita `jdbc` em tabela e não abre seção. O Chadha não trata.

O agravante é que o livro **contém a prova do próprio problema e não a conecta**: vinte páginas depois da receita de JDBC, `movies.rdd.getNumPartitions` devolve `Int = 1`. O exemplo impresso lê a tabela `film` inteira por uma conexão, numa partição, num executor.

#### As cinco opções, com o texto da documentação

Da [página de fonte JDBC do 4.2.0](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html):

| Opção | Default | O que a documentação diz | Escopo |
|---|---|---|---|
| `partitionColumn`, `lowerBound`, `upperBound` | nenhum | "These options must all be specified if any of them is specified. In addition, `numPartitions` must be specified. They describe how to partition the table when reading in parallel from multiple workers. `partitionColumn` must be a numeric, date, or timestamp column from the table in question. Notice that `lowerBound` and `upperBound` are just used to decide the partition stride, not for filtering the rows in table. So all rows in the table will be partitioned and returned." | leitura |
| `numPartitions` | nenhum | "The maximum number of partitions that can be used for parallelism in table reading and writing. This also determines the maximum number of concurrent JDBC connections." | leitura e escrita |
| `fetchsize` | `0` | "The JDBC fetch size, which determines how many rows to fetch per round trip. This can help performance on JDBC drivers which default to low fetch size (e.g. Oracle with 10 rows)." | leitura |
| `batchsize` | `1000` | "The JDBC batch size, which determines how many rows to insert per round trip." | escrita |
| `isolationLevel` | `READ_UNCOMMITTED` | nível de isolamento da conexão, entre `NONE`, `READ_COMMITTED`, `READ_UNCOMMITTED`, `REPEATABLE_READ` e `SERIALIZABLE` | escrita |

Duas frases dessa tabela merecem ser lidas duas vezes, porque desfazem o mal-entendido mais comum sobre a API:

**`lowerBound` e `upperBound` não filtram.** Eles decidem o **passo** (*stride*) entre as partições. Toda linha da tabela é devolvida. Se você põe `lowerBound=1` e `upperBound=100` numa tabela cujo id vai até 10 milhões, você não leu cem linhas: você leu dez milhões, e a última partição ficou com 9.999.900 delas. Este é o modo de falha número um da leitura paralela de JDBC, e ele se manifesta exatamente como o skew que a aula 01 descreveu, com a diferença de que o AQE **não conserta**, porque o desequilíbrio nasce antes do primeiro shuffle, na fatia que cada conexão pediu ao banco.

**`numPartitions` é o número de conexões concorrentes.** A documentação diz que essa opção determina o máximo de conexões JDBC simultâneas. Isso liga uma decisão de código Spark a uma decisão de capacidade do banco de origem. Pôr `numPartitions=200` num Postgres com `max_connections=100` derruba o banco alheio, não o seu job. É o único parâmetro desta parte cujo erro tem vítima fora do cluster.

#### Como escolher a coluna de particionamento

A documentação diz que `partitionColumn` precisa ser coluna numérica, de data ou de timestamp. Isso é a restrição de tipo. As restrições de qualidade não estão na documentação e são as que decidem se o particionamento serve:

1. **Distribuição próxima de uniforme entre os limites.** O passo é aritmético: o Spark divide o intervalo entre `lowerBound` e `upperBound` em `numPartitions` faixas iguais e emite uma consulta por faixa. Se a coluna se concentra em um trecho, as partições ficam desiguais na mesma proporção. Chave sequencial com buraco grande (por exemplo, migração que deixou ids de 1 a 1.000 e depois de 5.000.000 em diante) produz partições vazias e partições enormes.
2. **Indexada na origem.** Cada partição é uma consulta com `WHERE coluna >= x AND coluna < y`. Sem índice, você acabou de trocar um *full scan* por `numPartitions` *full scans* concorrentes. Este é o erro que parece otimização e é degradação.
3. **Imutável durante a leitura.** As consultas não são simultâneas nem transacionais entre si. Se a coluna de particionamento muda enquanto o job roda, uma linha pode aparecer em duas partições ou em nenhuma. Coluna de data de atualização é péssima escolha por este motivo, mesmo sendo tentadora.
4. **Os limites precisam vir de medição, não de palpite.** O caminho honesto é uma consulta de `MIN` e `MAX` antes, ou estatística do catálogo do banco.

Molde que funciona:

```python
limites = spark.read.format("jdbc").options(**conf).option(
    "query", "SELECT MIN(id) AS lo, MAX(id) AS hi FROM film"
).load().first()

df = (spark.read.format("jdbc")
      .options(**conf)
      .option("dbtable", "film")
      .option("partitionColumn", "film_id")
      .option("lowerBound", limites["lo"])
      .option("upperBound", limites["hi"])
      .option("numPartitions", 8)
      .option("fetchsize", 10000)
      .load())
```

#### A saída para quando não existe coluna boa

E existe, e nenhum dos três livros a menciona: o parâmetro **`predicates`** de `DataFrameReader.jdbc`. O [código-fonte do PySpark na tag v4.2.0](https://raw.githubusercontent.com/apache/spark/v4.2.0/python/pyspark/sql/readwriter.py) documenta assim: "Partitions of the table will be retrieved in parallel if either `column` or `predicates` is specified", `predicates` é "a list of expressions suitable for inclusion in WHERE clauses; each one defines one partition of the `DataFrame`", e "If both `column` and `predicates` are specified, `column` will be used". A assinatura existe desde a 1.4.0.

Isso resolve o caso em que a chave é UUID, ou string, ou o dado é naturalmente desigual: você escreve o predicado de cada partição na mão, e cada um pode ter tamanho diferente do outro.

```python
df = spark.read.jdbc(
    url, "film",
    predicates=[
        "rating = 'G'", "rating = 'PG'", "rating = 'PG-13'",
        "rating = 'R'", "rating = 'NC-17'", "rating IS NULL",
    ],
    properties=props,
)
```

Duas advertências: os predicados precisam ser **mutuamente exclusivos e exaustivos**, porque o Spark não valida isso e vai duplicar ou perder linha em silêncio se você errar; e cada predicado é uma conexão, então a lista é a sua contagem de conexões.

#### O `fetchsize`, que ninguém liga e todos deviam

`fetchsize` com default `0` significa deixar o driver decidir, e vários drivers decidem mal. A própria documentação cita Oracle com 10 linhas por viagem de rede. Dez linhas por *round trip* em uma tabela de milhões é latência de rede multiplicada por centenas de milhares. Subir para alguns milhares costuma dar ganho de ordem de magnitude sem custo nenhum além de memória do executor. Note a assimetria de nomes: na leitura é `fetchsize`, na escrita é `batchsize` com default `1000`.

#### O que o Luu ensinou certo, e o que envelheceu por fora

O predicate pushdown que o Luu 3.2 finalmente define na página 17 continua valendo e está documentado como `pushDownPredicate`, com default `true`, e a documentação inclusive explica quando desligar: "Predicate push-down is usually turned off when the predicate filtering is performed faster by Spark than by the JDBC data source". Só que o pushdown de JDBC cresceu muito depois do livro, e hoje há quatro chaves, todas com default `true`: `pushDownPredicate`, `pushDownAggregate`, `pushDownLimit` (que inclui `LIMIT` com `SORT`, ou seja, Top-N) e `pushDownTableSample`. As de agregação, `LIMIT` e Top-N vieram no 3.3.0 por [SPARK-37644](https://issues.apache.org/jira/browse/SPARK-37644), [SPARK-37020](https://issues.apache.org/jira/browse/SPARK-37020) e [SPARK-37483](https://issues.apache.org/jira/browse/SPARK-37483), e o 4.1 adicionou pushdown de join em DSv2 ([SPARK-52187](https://issues.apache.org/jira/browse/SPARK-52187)). Uma nota de interação que a documentação registra e que é fácil de perder: `pushDownOffset` só empurra o `OFFSET` para o banco quando `numPartitions` é igual a 1. Paralelismo e pushdown de `OFFSET` não coexistem, e faz sentido, porque `OFFSET` por partição não tem significado global.

Duas coisas do exemplo do Luu que envelheceram e não são culpa do particionamento: a classe `com.mysql.jdbc.Driver` foi substituída por `com.mysql.cj.jdbc.Driver`, e a forma corrente de dar o jar ao Spark é `--jars` ou `--packages`, não passar o caminho para `bin/spark-shell`. E o exemplo do livro põe usuário e senha na URL de conexão, o que hoje é problema de segurança óbvio e não recebe comentário no texto.

---

### 11. Lacuna 2: UDF, que não aparece em nenhuma das 141 páginas

Esta é a lacuna mais estranha das quatro, porque UDF não é assunto avançado: é a primeira coisa que alguém escreve quando a função embutida não existe. O pré-aula registra a ausência como achado: cinco leituras sobre APIs estruturadas e persistência, e a construção em que a escolha de linguagem custa caro não é mencionada uma vez. Vale notar a ironia interna: o Chadha usa três transformadores de MLlib (`Tokenizer`, `StopWordsRemover`, `CountVectorizer`) numa receita de ingestão, ou seja, chega a importar biblioteca de ML antes de mencionar UDF.

#### Por que UDF é o assunto que fecha o argumento do capítulo

O Damji 3 constrói o capítulo inteiro sobre uma tese: dar estrutura ao dado dá espaço ao otimizador. A lista de quatro problemas do RDD que ele monta na abertura é, palavra por palavra, a lista de problemas de uma UDF Python:

| O problema que o Damji 3 atribui ao RDD | Vale igual para UDF Python? |
|---|---|
| a função de cômputo é opaca ao Spark, que vê uma expressão lambda | sim, exatamente. O Catalyst não inspeciona o corpo da UDF |
| o `Iterator[T]` é opaco em RDD de Python: só objeto genérico | sim, e o dado atravessa a fronteira JVM para Python |
| sem inspecionar a expressão, não há como otimizar | sim: filtro em UDF **não desce** para a fonte |
| sem conhecer o tipo, não há como comprimir | sim, e o custo de serialização é o mesmo argumento |

Ou seja: **a UDF Python reintroduz, dentro da API estruturada, exatamente a opacidade que o capítulo usa para condenar o RDD.** Quem lê o Damji 3 sai com a tese na mão e sem o nome da construção que a viola. É a pergunta de aula mais direta que esta lacuna produz.

A consequência prática mais cara é o pushdown. Um `df.filter(minha_udf(col("x")) == 1)` não gera `PushedFilters` na fonte, porque o Catalyst não sabe o que `minha_udf` faz; a leitura traz tudo e o filtro roda depois. O `PushedFilters: []` que o Damji imprime no `explain()` do capítulo 3, e não comenta, é o sintoma exato desse tipo de bloqueio, embora naquele exemplo a causa seja outra.

#### O espectro de opções, do mais barato ao mais caro

Do [guia de UDF e UDTF do PySpark 4.2.0](https://spark.apache.org/docs/latest/api/python/user_guide/udfandudtf.html):

1. **Função embutida ou expressão SQL.** Roda dentro da JVM, entra no whole-stage codegen, participa de pushdown. É a única opção sem custo de fronteira. Boa parte do que se escreve como UDF cabe aqui, e a bibliografia dá material: o Chadha lista `explode`, `explode_outer`, `flatten`, `collect_list`, `array_distinct`, `map_keys`, `get_json_object`, `json_tuple`, `regexp_replace`, `regexp_extract`, `split`. As de ordem superior de verdade (`transform`, `filter`, `exists`, `aggregate`) cobrem quase tudo que se quer fazer dentro de array sem sair da JVM, e o Chadha erra ao chamar `array_distinct` de higher-order function.
2. **UDF SQL**, desde o 4.0 por [SPARK-46057](https://issues.apache.org/jira/browse/SPARK-46057). Função nomeada, definida em SQL, guardada no catálogo. É a opção que resolve reuso sem sair do motor.
3. **UDF escalar de Python, otimizada por Arrow.** No 4.2 este é o default, por [SPARK-54555](https://issues.apache.org/jira/browse/SPARK-54555). Continua atravessando a fronteira, mas o transporte é colunar.
4. **pandas UDF e Arrow UDF.** Operam bloco a bloco, sobre `Series` ou `pyarrow.Array`. É onde a vetorização vale mais, porque a lógica trabalha em vetor e não em linha.
5. **UDF escalar de Python com cloudpickle**, o caminho legado. Ainda alcançável com `spark.sql.execution.pythonUDF.arrow.enabled=false`, e é o comportamento que a literatura de 2020 e 2021 descrevia como se fosse o único.
6. **UDTF de Python**, desde o 3.5 ([SPARK-43797](https://issues.apache.org/jira/browse/SPARK-43797)), Arrow-otimizada por padrão no 4.2. Devolve tabela em vez de valor, o que substitui o padrão de devolver array e chamar `explode` depois.

```python
from pyspark.sql.functions import udf, pandas_udf
import pandas as pd

@udf(returnType="int", useArrow=True)          # UDF Arrow explícita
def tamanho(s: str) -> int:
    return len(s) if s is not None else 0

@pandas_udf("double")                           # pandas UDF, bloco a bloco
def normaliza(v: pd.Series) -> pd.Series:
    return (v - v.mean()) / v.std()
```

**A regra de bolso que a bibliografia deveria ter dado:** antes de escrever UDF, procure a função embutida; se não existir, tente compor com funções de ordem superior; se ainda não der, escreva UDF vetorizada (pandas ou Arrow) e não escalar; e confira o `explain()` para ver o que deixou de descer para a fonte. Note que a mesma lógica que a aula 01 aplicou ao AQE se aplica aqui: o que era ajuste manual em 2020 virou default em 2026, e o que se aprende hoje é reconhecer quando o default não vale.

---

### 12. Lacuna 3: formato de tabela transacional, o silêncio de cinco capítulos

O pré-aula chama isso de silêncio de cinco capítulos, e a contagem é dura: o Luu, capítulo 3, tem **zero** ocorrências de Delta Lake, embora o capítulo 1 do mesmo livro apresente o Delta como a resposta do ecossistema para consistência; o Damji 3 tem zero e o Damji 4 tem **uma, entre parênteses**, como consumidor de Parquet, fora da lista de fontes e fora da figura; o Chadha tem **zero**, num livro cujo título traz Databricks.

A consequência que o pré-aula extrai é a certa: **quem lê estes cinco capítulos conclui que persistir Parquet num diretório resolve.** E os três livros reforçam essa conclusão ao recomendar Parquet com convicção. O Damji 3, o Luu 3.2 e o Damji 4 recomendam Parquet e dão motivos que se somam (autodescritivo, compacto, colunar, schema no metadado). Nada disso está errado: `spark.sql.sources.default` continua valendo `parquet` [na configuração do 4.2.0](https://spark.apache.org/docs/latest/configuration.html), desde a 1.3.0. O problema é a pergunta que ninguém faz: **Parquet num diretório é formato de arquivo, não formato de tabela.**

#### O que Parquet num diretório não resolve

Quatro coisas, e todas aparecem no primeiro pipeline que roda mais de uma vez:

1. **Atomicidade do `overwrite`.** Os quatro modos de escrita que o Chadha define e o Damji 4 tabela (`append`, `overwrite`, `ignore`, `error`) atuam sobre diretório. Um `overwrite` que falha no meio deixa o diretório em estado intermediário, com parte do dado novo e parte do velho apagado. Nenhum dos três livros menciona isso, embora o Chadha diga que `overwrite` "derruba índices e constraints", que é vocabulário de banco relacional colado em writer de arquivo.
2. **Leitura durante escrita.** Sem log de commit, o leitor vê arquivos parciais. O Damji 4 ensina `spark.catalog` e views sem uma palavra sobre isolamento de leitura.
3. **Update e delete por linha.** Não existem. O caminho é reescrever a partição inteira, e o Chadha ensina exatamente esse caminho com `partitionBy` e `overwrite`.
4. **Evolução de schema com histórico.** O Chadha entrega `mergeSchema` e uma nota dizendo que, quando a fusão não dá o schema ótimo, resta tratar a evolução "à mão", sem dizer como. `mergeSchema` reconcilia schema **na leitura**; não versiona schema nem registra quando mudou.

#### Os três formatos, e o que cada um resolve

Delta Lake, Apache Iceberg e Apache Hudi resolvem o mesmo problema de base: um **log de commit** ao lado dos arquivos Parquet, que define qual conjunto de arquivos compõe a tabela em cada versão. Disso saem atomicidade, isolamento de snapshot, viagem no tempo, `MERGE`, e evolução de schema registrada. A comparação entre os três é assunto de arquitetura, e a Parte 5 da aula 01 já registrou o estado do ecossistema em 2026, com Iceberg como padrão de fato, Delta com base instalada e UniForm, e Hudi especializado em mutação.

O que interessa **aqui**, na aula de persistência, é uma coisa que a aula 01 não cobriu: o Spark 4.x trouxe para dentro do motor várias das capacidades que antes só existiam no formato.

| Capacidade | Onde estava | No Spark hoje |
|---|---|---|
| commit atômico de várias operações | no log do formato | **transação em DSv2** ([SPARK-55855](https://issues.apache.org/jira/browse/SPARK-55855), 4.2) |
| evolução de schema no `INSERT` | sintaxe própria de cada formato | `WITH SCHEMA EVOLUTION` no `INSERT` SQL ([SPARK-54971](https://issues.apache.org/jira/browse/SPARK-54971), 4.2) e em `AppendData`, `OverwriteByExpression` e `OverwritePartitionsDynamic` ([SPARK-55690](https://issues.apache.org/jira/browse/SPARK-55690)) |
| evolução de schema em `MERGE INTO` | idem | [SPARK-54274](https://issues.apache.org/jira/browse/SPARK-54274) (4.1) |
| leitura de mudança de linha | `table_changes()` no Delta, tabela virtual `.changes` no Iceberg, opção de leitura incremental no Hudi | cláusula **`CHANGES`** de motor ([SPARK-55668](https://issues.apache.org/jira/browse/SPARK-55668), 4.2) |
| constraint de tabela | no formato ou em nenhum lugar | [SPARK-51207](https://issues.apache.org/jira/browse/SPARK-51207) (4.1) |

A leitura disso: **a fronteira entre motor e formato de tabela está se movendo para dentro do motor.** O que o Damji 4 apresenta como catálogo, tabela gerenciada e tabela não gerenciada é hoje a ponta visível de uma pilha DSv2 com transação, evolução de schema e CDC, e a distinção que o livro ensina (gerenciada apaga dado, não gerenciada apaga só metadado) continua verdadeira e virou o menor dos assuntos.

#### O detalhe operacional que ninguém conta, e que vale para hoje

Formato de tabela **anda atrás** da release do Spark. Conferi hoje, 28 de julho de 2026, no índice do Maven Central:

- Iceberg: o artefato mais novo é **`org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`**. Uma busca por `iceberg-spark-runtime-4.2_2.13` devolve **zero resultados**. A [página de início rápido do Iceberg com Spark](https://iceberg.apache.org/docs/latest/spark-getting-started/) documenta a versão 1.11.0 e o runtime para Spark 4.1, com a extensão `org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions` em `spark.sql.extensions`.
- Delta Lake: o mais novo de `io.delta:delta-spark_2.13` é **4.0.0, de 5 de junho de 2025**, ou seja, alinhado ao Spark 4.0.
- Hudi: existe `hudi-spark4.1-bundle_2.13:1.2.0`; `hudi-spark4.2-bundle_2.13` devolve zero resultados.

Isso não é crítica aos projetos, é fato de calendário: o Spark 4.2.0 tem duas semanas. Mas tem consequência direta de decisão, e ela é o oposto da intuição: **adotar a release mais nova do Spark e adotar formato de tabela transacional são objetivos em tensão.** Quem precisa de Iceberg ou Delta hoje fica em 4.1 ou 4.0. Quem quer geoespacial nativo, CDC de motor e UDF Arrow por padrão fica em 4.2 e, por ora, sem esses formatos. É uma escolha real, com data, e nenhuma das 141 páginas dá vocabulário para formulá-la.

---

### 13. Lacuna 4: como rodar os exemplos do Damji fora da Databricks

O pré-aula documenta a inversão: o livro de Databricks não usa Databricks, e **quem depende de plataforma é o Damji**. Todo caminho de dado das 39 páginas do capítulo 4 começa em `/databricks-datasets/`, que só existe dentro do produto, sem aviso e sem instrução de como obter os arquivos fora dele. O capítulo 3 faz o mesmo. As notas remetem a notebooks, e a `SparkSession` pronta é justificada por rodar em shell ou notebook Databricks.

A boa notícia é que a solução é simples, e o próprio material do livro a contém: os arquivos estão no repositório dos autores, sob um caminho que **imita** o da plataforma.

#### O caminho

1. Clone o [repositório LearningSparkV2](https://github.com/databricks/LearningSparkV2). Ele tem, no topo, os diretórios `chapter2`, `chapter3`, `chapter6`, `chapter7`, `mlflow-project-example`, `notebooks` e, o que interessa, **`databricks-datasets/learning-spark-v2`**.
2. Os arquivos citados nos capítulos estão lá. Conferi pela API do GitHub: `databricks-datasets/learning-spark-v2/blogs.json` existe, e `databricks-datasets/learning-spark-v2/flights/` contém `departuredelays.csv`, `airport-codes-na.txt` e `summary-data`.
3. O mapeamento de caminho é mecânico. Onde o livro escreve `/databricks-datasets/learning-spark-v2/<resto>`, aponte para `<clone>/databricks-datasets/learning-spark-v2/<resto>`.

```python
# no livro
caminho = "/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"

# localmente, depois do clone
import os
RAIZ = os.environ["LSV2"]        # ex.: /home/gnix/repos/LearningSparkV2
caminho = f"{RAIZ}/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
```

Duas advertências para não perder tempo. Primeira: **não todo `/databricks-datasets/` do livro tem contraparte no repositório.** O capítulo 4 usa também imagens e arquivos binários, e o pré-aula registra que os caminhos vêm da plataforma sem procedência declarada; onde o arquivo não estiver no clone, o exemplo precisa de dado substituto seu, e aí você está adaptando o exemplo, não reproduzindo. Segunda: os diretórios `chapterN` do repositório trazem aplicações que rodam por `spark-submit`, e o README orienta construir os jars com `python build_jars.py` e pôr `$SPARK_HOME/bin` no `PATH`. Ou seja, o material para rodar fora da plataforma sempre existiu; o livro é que não aponta para ele nos capítulos lidos.

Uma terceira advertência, e ela não é sobre caminho de arquivo: **os exemplos do Damji foram escritos para Spark 3.0, com ANSI desligado.** Rodar o capítulo 4 em 4.2.0 pode produzir exceção onde o livro imprime `null`, porque a leitura de CSV sem schema com cast implícito é justamente o terreno que o modo ANSI endureceu. Se o objetivo é reproduzir a saída impressa, ponha `spark.sql.ansi.enabled=false` **de forma explícita e consciente**, com comentário dizendo que é para reproduzir um livro de 2020, e não como configuração de projeto.

Para o Chadha o problema é outro e menor: o ambiente do livro é `docker-compose` local com JupyterLab e Spark standalone, e roda hoje sem conta e sem custo. O que precisa de emenda são duas coisas de idade, as duas já registradas no pré-aula: `docker compose` sem hífen, e a receita de XML, que precisa virar `format("xml")` nativo em Spark 4, porque `spark-xml_2.12` não sobe em runtime Scala 2.13.

---

### 14. O que a bibliografia manda fazer contra o que vale em julho de 2026

Regra desta tabela, herdada da auditoria da aula 01: **a coluna do meio só contém o que está registrado no pré-aula da aula 02**, atribuído ao capítulo certo, sem citação inventada. Onde a atribuição é de um livro só, está dito qual. Onde a ausência é de todos os cinco textos, está dito assim.

| # | Tema | O que a bibliografia desta aula registra | O que vale em julho de 2026 |
|---|---|---|---|
| 1 | Versão de referência | Damji caps. 3 e 4 cobrem Spark 3.0; Luu cap. 3 cobre 3.0 e 3.1; o Chadha tem os quinze *See also* rotulados Spark 3.4.0 e pede `spark-xml_2.12:0.16.0` | **Spark 4.2.0**, de [14/07/2026](https://spark.apache.org/releases/spark-release-4-2-0.html) |
| 2 | Cast e overflow | Luu 3.2 declara que o default nulifica **todas as colunas da linha** e nomeia `failFast` como única alternativa | ANSI **ligado por padrão** desde o 4.0 ([SPARK-44444](https://issues.apache.org/jira/browse/SPARK-44444)): overflow e cast inválido **falham**; a tolerância virou opt-in por expressão, com `try_cast` e a família `try_*` |
| 3 | Registro ruim na leitura | Damji 4 dá os três nomes numa célula de tabela e remete à documentação; Chadha nomeia um modo e descreve a semântica errado; Luu 3.2 só cita `failFast` | espectro completo documentado por formato, incluindo XML nativo, com `mode` default `PERMISSIVE` e `columnNameOfCorruptRecord` herdando `spark.sql.columnNameOfCorruptRecord` ([docs XML](https://spark.apache.org/docs/latest/sql-data-sources-xml.html)) |
| 4 | Formato default de escrita | Damji 3, Luu 3.2 e Damji 4 recomendam Parquet com motivos; Chadha descreve o formato e nunca o recomenda nem diz que é default | `spark.sql.sources.default` segue **`parquet`** ([config](https://spark.apache.org/docs/latest/configuration.html)); o que mudou é que Parquet hoje mora dentro de um formato de **tabela**, não solto num diretório |
| 5 | Layout do Parquet | Luu 3.2 afirma que o Parquet guarda cada coluna **num arquivo separado** | grupos de linha e pedaços de coluna dentro de **um** arquivo; é o que explica por que arquivo pequeno em Parquet é ruim |
| 6 | Dado semiestruturado | Chadha receita `get_json_object()` e `json_tuple()` sobre coluna de string, sem explicar a gramática de `$.name`; as tabelas de tipos do Damji 3 não têm tipo para isso | tipo **VARIANT** ([SPARK-45827](https://issues.apache.org/jira/browse/SPARK-45827), 4.0), ligado por padrão no 4.1 ([SPARK-54454](https://issues.apache.org/jira/browse/SPARK-54454)) com **shredding** em Parquet |
| 7 | XML | Chadha promete fonte "embutida" e manda instalar `com.databricks:spark-xml_2.12:0.16.0`, usa `format("com.databricks.spark.xml")` e receita `rootTag` na leitura | `format("xml")` **nativo** desde o 4.0 ([SPARK-44265](https://issues.apache.org/jira/browse/SPARK-44265)); `rootTag` é opção de **escrita**; repositório [arquivado em 24/03/2025](https://github.com/databricks/spark-xml), última release v0.18.0 (10/04/2024) |
| 8 | Fonte fora da lista | Damji 4 desenvolve as fontes embutidas; Luu 3.2 diz na Tabela 3-2 que a camada é extensível e que fonte customizada exige nome totalmente qualificado | **Python Data Source API** ([SPARK-44076](https://issues.apache.org/jira/browse/SPARK-44076), 4.0): conector batch e streaming em Python puro, registrado com `spark.dataSource.register` |
| 9 | Leitura de RDBMS | Luu 3.2 é o único que ensina JDBC: quatro peças (driver, URL, autenticação, tabela) e nenhuma palavra sobre particionar; Damji 4 só cita `jdbc` em tabela | `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions` e `fetchsize` ([docs JDBC](https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html)), mais `predicates` quando não há coluna boa ([fonte v4.2.0](https://raw.githubusercontent.com/apache/spark/v4.2.0/python/pyspark/sql/readwriter.py)) |
| 10 | Driver e jar de JDBC | Luu 3.2 usa `com.mysql.jdbc.Driver` e passa o jar direto para `bin/spark-shell`; o exemplo põe usuário e senha na URL | `com.mysql.cj.jdbc.Driver`; jar por `--jars` ou `--packages`; credencial fora da URL |
| 11 | Pushdown em JDBC | Luu 3.2 define predicate pushdown na página 17, só para predicado, e sem meio de verificar, porque `explain` não aparece no capítulo | quatro chaves com default `true`: `pushDownPredicate`, `pushDownAggregate`, `pushDownLimit` (inclui Top-N) e `pushDownTableSample`; `pushDownOffset` só desce com `numPartitions = 1` |
| 12 | UDF | **nenhuma UDF nas 141 páginas**; o Chadha chega a usar três transformadores de MLlib numa receita de ingestão | UDF Python e UDTF **Arrow-otimizadas por padrão** no 4.2 ([SPARK-54555](https://issues.apache.org/jira/browse/SPARK-54555)); PyArrow mínimo sobe para 18.0.0 |
| 13 | Timestamp | Luu 3.2 mostra `last_update: timestamp` no schema do Sakila e não comenta o tipo; a Tabela 3-5 do Damji 3 lista só `TimestampType` | dois tipos: `TimestampType` é LTZ e **`TimestampNTZType`** é sem fuso ([SPARK-35662](https://issues.apache.org/jira/browse/SPARK-35662), 3.4; exposto em PySpark por [SPARK-43759](https://issues.apache.org/jira/browse/SPARK-43759), 3.5) |
| 14 | Intervalo | as tabelas de tipos do Damji 3 não têm tipo de intervalo, e a ausência já era ausência em 2020 | `YearMonthIntervalType` e `DayTimeIntervalType` ([SPARK-27790](https://issues.apache.org/jira/browse/SPARK-27790), 3.2), com campos declaráveis |
| 15 | Comparação de string | nenhuma das cinco leituras trata comparação sensível a caixa ou a acento | **collations** ([SPARK-46830](https://issues.apache.org/jira/browse/SPARK-46830), 4.0), com `UTF8_LCASE`, `UNICODE_CI`, `UNICODE_AI` e locais, listáveis por [`SHOW COLLATIONS`](https://spark.apache.org/docs/latest/sql-ref-syntax-aux-show-collations.html) |
| 16 | Geoespacial | a tese de lista de tipos sem geoespacial foi confirmada nas cinco leituras, por construção | **`GEOMETRY` e `GEOGRAPHY`** com `ST_*` ([SPARK-51658](https://issues.apache.org/jira/browse/SPARK-51658)), ligados por padrão ([SPARK-56771](https://issues.apache.org/jira/browse/SPARK-56771)); tipo de SRID misto é só de consulta, não persiste em Parquet, Delta nem Iceberg |
| 17 | Leitura incremental | não aparece nas cinco; o mais perto é o Chadha recortando partição por curinga de diretório | cláusula **`CHANGES`** de motor ([SPARK-55668](https://issues.apache.org/jira/browse/SPARK-55668), 4.2), com API de DataFrame, PySpark e Connect |
| 18 | Formato de tabela transacional | Luu cap. 3: zero ocorrências de Delta Lake, contra o capítulo 1 do mesmo livro; Damji 4: uma menção entre parênteses; Chadha: zero, num livro com Databricks no título | log de commit é padrão de arquitetura; e o Spark absorveu parte disso: transação em DSv2 ([SPARK-55855](https://issues.apache.org/jira/browse/SPARK-55855)) e `WITH SCHEMA EVOLUTION` ([SPARK-54971](https://issues.apache.org/jira/browse/SPARK-54971)). Detalhe de calendário: em 28/07/2026 não existe `iceberg-spark-runtime-4.2_2.13` nem bundle de Hudi para 4.2 no Maven Central, e `delta-spark_2.13` para no 4.0.0 |
| 19 | Evolução de schema | Chadha entrega `mergeSchema` com default `false` e encerra dizendo que o resto é "à mão", sem dar caminho | `mergeSchema` reconcilia na leitura; evolução com histórico é do formato de tabela, e o `INSERT` do 4.2 aceita `WITH SCHEMA EVOLUTION` |
| 20 | Onde rodar os exemplos | Damji 4 usa `/databricks-datasets/` em **todo** caminho de dado das 39 páginas, sem aviso; Damji 3 faz o mesmo | clonar o [LearningSparkV2](https://github.com/databricks/LearningSparkV2) e apontar para `databricks-datasets/learning-spark-v2/`, onde estão `blogs.json` e `flights/departuredelays.csv` |
| 21 | Ambiente do Chadha | `docker-compose` com hífen, JupyterLab em `127.0.0.1:8888`, Spark standalone, `spark.executor.memory` em `512m` nas sete receitas, nenhuma versão de Spark ou Python declarada | `docker compose` sem hífen; e o runtime de hoje é Java 17, 21 ou 25, Scala 2.13 e Python 3.10 ou mais ([docs 4.2.0](https://spark.apache.org/docs/latest/index.html)) |
| 22 | Scala | o pacote pedido pelo Chadha é `_2.12`; a Tabela 3-1 do Luu mapeia tipos só para Scala | **só 2.13** desde o 4.0 ([SPARK-45314](https://issues.apache.org/jira/browse/SPARK-45314)); 2.13.18 empacotada no 4.2.0 |

---

### Perguntas que a parte 5 abriu

Sete perguntas, cada uma com a hipótese que eu levo para a aula. A hipótese existe para poder estar errada em público, que é o que faz a resposta valer.

1. **Em pipeline legado que sobe para 4.x, a recomendação é ligar ANSI e consertar as expressões, ou desligar a config e migrar aos poucos?**
   *Hipótese:* ligar e consertar, porque desligar cria dívida com prazo indefinido e a config é global, ou seja, protege o código velho e envenena o novo. O caminho seria rodar com ANSI ligado em ambiente de teste, coletar as condições de erro (`ARITHMETIC_OVERFLOW`, `CAST_INVALID_INPUT`) e trocar por `try_*` só onde a tolerância for **decisão de negócio**, não conveniência. O que eu não sei é se existe caminho intermediário oficial, algo como relatório de incompatibilidade, ou se a única ferramenta é rodar e ver quebrar.

2. **VARIANT contra `StructType` declarado: qual é o critério de escolha, e VARIANT com shredding chega perto de coluna tipada em performance de leitura?**
   *Hipótese:* VARIANT ganha quando o schema é instável ou tem cauda longa de campos raros, e struct declarado ganha quando o schema é estável e conhecido, porque o contrato explícito detecta erro cedo, que é o motivo 3 do Damji 3. O shredding deveria fechar boa parte da distância em leitura dos campos frequentes, mas suspeito que o campo raro, o que não foi materializado, continue custando navegação em vez de scan colunar. E há um cheiro de risco na questão de compatibilidade: encontrei registro de discussão sobre a especificação de shredding ter mudado de forma incompatível, e isso sugere que gravar shredded hoje pode ser aposta.

3. **Qual coluna de particionamento usar em JDBC quando a chave é UUID ou string, e `predicates` é solução de produção ou remendo?**
   *Hipótese:* `predicates` é solução de produção, e a mais honesta das duas, porque você declara a fatia em vez de deixar a aritmética inventar. O risco é operacional, não conceitual: a lista precisa ser mutuamente exclusiva e exaustiva e o Spark não valida nada, então uma fatia esquecida é dado perdido em silêncio. Suspeito que a prática de mercado seja derivar os predicados de uma coluna de partição do próprio banco, ou de um hash com módulo, para garantir exaustividade por construção.

4. **UDF Python Arrow por padrão no 4.2 muda **resultado**, e não só performance?**
   *Hipótese:* sim, em pelo menos um caso documentado, o da coluna de inteiro nulável que passa a chegar como dtype de extensão em vez de `float64`, e desconfio que haja mais casos em conversão de tipo na borda, porque o mapeamento de tipos do Arrow não é idêntico ao do pickle mais pandas. Se isso está certo, a orientação de upgrade para 4.2 não é "ganho de graça, sem mudar linha de código", e sim "ganho grande com uma bateria de teste de UDF antes".

5. **Onde termina o motor e começa o formato de tabela, agora que o 4.2 tem transação em DSv2, evolução de schema no `INSERT` e CDC de motor?**
   *Hipótese:* o motor está absorvendo a **interface** e deixando a **implementação** no formato: `CHANGES` e `WITH SCHEMA EVOLUTION` padronizam como se pede, e quem entrega continua sendo o log de commit do Delta, do Iceberg ou do Hudi. Se isso está certo, a decisão de formato perde importância na camada de código e ganha na camada de operação (compactação, expiração de snapshot, catálogo). A pergunta de controle: existe conector DSv2 open source que já implemente a API de CDC do 4.2, ou a cláusula ainda não tem quem a atenda?

6. **Como se estuda Spark 4.2 e formato transacional ao mesmo tempo, se não há build de Iceberg, Delta nem Hudi para 4.2?**
   *Hipótese:* não se estuda ao mesmo tempo, e a escolha certa para o Projeto da Disciplina é ficar em **4.1** se o projeto tocar formato de tabela, e em 4.2 se o projeto tocar geoespacial, CDC ou UDF. Quero saber se o professor considera essa defasagem normal de ciclo, com semanas de atraso, ou se ela costuma durar meses, porque a resposta muda a regra de bolso de qual versão adotar em produção.

7. **Collation resolve comparação insensível a caixa sem custo de otimização, ou troca um bloqueio por outro?**
   *Hipótese:* resolve com vantagem, porque `col = 'x' COLLATE UNICODE_CI` mantém a coluna limpa na expressão, enquanto `lower(col) = 'x'` põe função em cima da coluna e é candidato natural a bloquear pushdown e uso de estatística. Mas suspeito que collation não binária tenha custo próprio de comparação, e que pushdown de filtro com collation para Parquet possa simplesmente não existir, já que o Parquet guarda estatística de min e max em ordem binária. Se for isso, a troca é legibilidade e correção contra pushdown, e vale saber declarada.

---

## Nota de confiabilidade

Escrita em 28/07/2026. Cada parte deste documento cita fonte primária para afirmação de default, versão, nome de configuração e número de ticket. O que **não** foi confirmado, e está marcado como tal no corpo, para ninguém citar em aula como se fosse verificado:

**Defaults que não estão nas páginas públicas de configuração do 4.2.0 que foram abertas:** `spark.sql.files.maxRecordsPerFile`, `spark.sql.sources.partitionOverwriteMode`, `spark.sql.catalogImplementation`, `spark.sql.sources.bucketing.enabled`, `spark.sql.codegen.maxFields`, os defaults de compressão de Avro, CSV e JSON, e os defaults por nível de `spark.locality.wait`. A página de configuração trunca antes da tabela de configuração de runtime de SQL.

**Sintaxe de fonte secundária:** a forma exata da cláusula `CHANGES` do CDC nativo do 4.2 só apareceu em blog da Databricks, porque não foi encontrada página de referência de sintaxe na documentação oficial. Trate como pendente.

**Duas divergências entre documentação e código que não foram arbitradas por execução**, por não haver PySpark instalado na máquina onde a pesquisa rodou: se linha de CSV com número errado de campos conta como registro corrompido (a doc diz que não, o `UnivocityParser` sugere que sim), e o que `PERMISSIVE` faz sem a coluna de corrompido (a doc diz que descarta, o `FailureSafeParser` sugere resultado parcial). As duas viraram pergunta para o professor, e as duas são fáceis de resolver com dez linhas de código: se você rodar antes da aula, resolva.

**Datas de release de terceiros:** as datas exatas das releases do Delta Lake ficaram inconsistentes entre as fontes consultadas. A afirmação que se sustenta, e que foi conferida no Maven Central, é a de **ausência**: não existe runtime de Iceberg, Delta nem Hudi para o Spark 4.2.0.

O que é informação de fora dos capítulos lidos está dito em linha ao longo do documento. Onde este texto afirma "o livro diz X", X está registrado em [01-pre-aula.md](01-pre-aula.md), com o código da dúvida ou o número da divergência.
