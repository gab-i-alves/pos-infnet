# Guia de Leitura — *Learning Spark*, 2ª edição, Capítulo 3: Apache Spark's Structured APIs

**Fonte:** Jules S. Damji, Brooke Wenig, Tathagata Das, Denny Lee. *Learning Spark: Lightning-Fast Data Analytics*, 2ª ed. O'Reilly, 2020. Capítulo 3, "Apache Spark's Structured APIs".

**Escopo:** os Níveis 1 a 4 são respondíveis apenas com este capítulo. O Nível 5 exige verificação externa contra fonte primária. O Nível 6 exige o Capítulo 3 do Luu já lido.

**Sobre as figuras:** abri as páginas 34, 41, 43, 45, 47 e 48 do PDF e li as cinco imagens. Onde uma resposta descreve conteúdo de figura, ela veio da imagem e não do texto extraído.

---

## Como usar este guia

1. Leia o capítulo uma vez de ponta a ponta, sem parar. Ele é longo e alterna prosa curta com blocos grandes de código e de saída. Na primeira passagem, deixe o código passar.
2. Responda o Nível 1 de memória antes de reabrir o capítulo. Depois releia para preencher as lacunas. Este capítulo tem seis tabelas ao todo, quatro delas de tipo, e é aí que a memória falha primeiro.
3. Escreva os Níveis 2 e 3 em frases completas. O Nível 2 mira o argumento central, que é por que estrutura permite otimização. Se você não consegue enunciar esse mecanismo sem copiar os autores, não sabe.
4. O Nível 4 é onde o capítulo se abre. Ele tem mais contradições internas do que os dois anteriores juntos, entre a prosa e o código, entre duas tabelas da mesma seção e entre uma figura e o parágrafo ao lado. Encontrar essas contradições é o exercício.
5. O Nível 5 vai para o backlog de estudo, não para as notas. O Nível 6 vai para uma nota de comparação que fica acima dos dois livros.

---

## Nível 1 — Memorização e definições

Respostas curtas e verificáveis. Uma ou duas frases cada.

**1.** Quais três características vitais o capítulo associa a um RDD? *(Spark: What's Underneath an RDD?)*

R: Dependencies, partitions com alguma informação de locality, e uma compute function com a assinatura `Partition => Iterator[T]`. *No item 1 das Discordâncias do Nível 6 verifiquei que a contagem canônica é outra.*

**2.** Qual das três dá ao RDD a sua resiliência, e por qual mecanismo? *(Spark: What's Underneath an RDD?)*

R: A lista de dependencies. Ela instrui o Spark sobre como o RDD é construído a partir das suas entradas. Quando é preciso reproduzir resultados, o Spark recria o RDD a partir dessas dependencies e replica as operações sobre ele.

**3.** O que as partitions dão ao Spark, e quando a informação de locality é usada? *(Spark: What's Underneath an RDD?)*

R: Partitions dão ao Spark a capacidade de dividir o trabalho e paralelizar a computação entre os executors. O exemplo de locality que o capítulo dá é a leitura de HDFS, quando o Spark manda o trabalho para executors próximos do dado. Assim menos dado trafega pela rede.

**4.** Nomeie os dois problemas que o capítulo identifica no modelo original de RDD. *(Spark: What's Underneath an RDD?)*

R: Primeiro, a compute function é opaca para o Spark. Ele só enxerga uma lambda e não sabe se ela faz join, filter, select ou agregação. Segundo, o tipo `Iterator[T]` também é opaco em RDDs de Python, onde o Spark só sabe que aquilo é um objeto genérico.

**5.** Qual é a única coisa que o Spark consegue fazer com um objeto opaco do tipo `T`? *(Spark: What's Underneath an RDD?)*

R: Serializar o objeto como uma sequência de bytes, sem usar nenhuma técnica de compressão de dados. O Spark não faz ideia se você acessa uma coluna de um tipo específico dentro do objeto.

**6.** Qual consequência essa opacidade tem para o query plan? *(Spark: What's Underneath an RDD?)*

R: O capítulo diz que a opacidade atrapalha a capacidade do Spark de rearranjar a computação em um query plan eficiente. Sem inspecionar a computação ou a expressão, ele não compreende a intenção e não tem como otimizar.

**7.** Quais três esquemas o Spark 2.x introduziu para dar estrutura ao Spark? *(Structuring Spark)*

R: Primeiro, expressar computações com padrões comuns de análise de dados, na forma de operações de alto nível. Segundo, estreitar isso com um conjunto de operadores comuns em uma DSL, disponível como API nas linguagens suportadas. Terceiro, permitir arranjar o dado em formato tabular, como uma tabela SQL ou uma planilha, com os tipos estruturados suportados.

**8.** Quais linguagens o capítulo lista para a DSL, e o que há de errado na lista? *(Structuring Spark)*

R: A lista é "Java, Python, Spark, R, and SQL". Ela traz "Spark" onde deveria estar Scala. É erro de revisão, e o próprio capítulo usa Scala em metade dos exemplos.

**9.** Além de performance e eficiência de espaço, quais quatro vantagens a estrutura traz? *(Key Merits and Benefits)*

R: expressivity, simplicity, composability e uniformity.

**10.** Por que o capítulo diz que uniformity entre componentes e linguagens é possível? *(Key Merits and Benefits)*

R: Por causa do Spark SQL engine sobre o qual as Structured APIs de alto nível são construídas. É esse engine que sustenta todos os componentes do Spark. Uma query contra um DataFrame no Structured Streaming ou na MLlib sempre opera sobre DataFrames como dado estruturado.

**11.** O que inspirou os DataFrames do Spark, e como o capítulo os define? *(The DataFrame API)*

R: Os DataFrames do pandas, em estrutura, formato e algumas operações específicas. A definição é "distributed in-memory tables with named columns and schemas", em que cada coluna tem um tipo de dado específico.

**12.** Quais colunas e tipos a Table 3-1 mostra? *(Table 3-1)*

R: Sete colunas. `Id` (Int), `First` (String), `Last` (String), `Url` (String), `Published` (Date), `Hits` (Int) e `Campaigns` (List[Strings]), com o `s` que o cabeçalho da tabela traz.

**13.** O que o capítulo afirma sobre a relação entre os tipos de Scala e `DataTypes`? *(Table 3-2)*

R: Que todos são subtipos da classe `DataTypes`, com exceção de `DecimalType`. *No item 8 do Nível 5 verifiquei que a frase está errada nas duas metades.*

**14.** Como a coluna "API to instantiate" difere entre a Table 3-3 e a Table 3-5? *(Tables 3-3 and 3-5)*

R: A Table 3-3 escreve `DataTypes.ByteType` para Python. A Table 3-5 escreve `BinaryType()`, `TimestampType()`, `DateType()` e assim por diante, sem prefixo e com parênteses. As duas tabelas descrevem a mesma linguagem e discordam. *No item 8 do Nível 5 verifiquei qual das duas está certa.*

**15.** Dê a definição de schema que o capítulo apresenta. *(Schemas and Creating DataFrames)*

R: Um schema no Spark define os nomes das colunas e os tipos de dado associados de um DataFrame.

**16.** Quais três benefícios definir um schema de antemão oferece em relação a schema-on-read? *(Schemas and Creating DataFrames)*

R: Livra o Spark do ônus de inferir os tipos. Evita que o Spark crie um job separado só para ler uma porção grande do arquivo e determinar o schema, o que é caro e demorado em arquivo grande. E permite detectar erros cedo, quando o dado não bate com o schema declarado.

**17.** Quais são as duas formas de definir um schema, e qual delas o capítulo prefere? *(Two ways to define a schema)*

R: De forma programática, com `StructType` e `StructField`, ou com uma string de Data Definition Language. O capítulo chama a DDL de muito mais simples e fácil de ler, e usa as duas nos exemplos.

**18.** Na saída Python do Example 3-6, qual é a nullability de cada coluna, e o que a entrada aninhada `Campaigns` mostra? *(Example 3-6 output)*

R: Todas as colunas saem com `nullable = false`. O `Campaigns` sai como `array (nullable = false)`, com uma entrada aninhada `element: string (containsNull = false)`.

**19.** O que o Example 3-7 muda, e o que o capítulo afirma sobre a saída dele? *(Example 3-7)*

R: O programa Scala lê de um arquivo JSON com `spark.read.schema(schema).json(jsonFile)` e define o schema de forma programática, em vez de criar dado estático. O capítulo afirma que a saída "is no different than that from the Python program". *Ela é diferente, e esse é o item 3 do Nível 4.*

**20.** Como o capítulo descreve colunas, e qual tipo as representa? *(Columns and Expressions)*

R: Colunas nomeadas em DataFrames são conceitualmente parecidas com colunas nomeadas no pandas, no R ou em uma tabela de RDBMS, e descrevem um tipo de campo. Nas linguagens suportadas, colunas são objetos com métodos públicos, representados pelo tipo `Column`.

**21.** Qual é a diferença entre `col` e `Column`, segundo a NOTE? *(Columns and Expressions, NOTE)*

R: `Column` é o nome do objeto. `col()` é uma função embutida padrão que devolve um `Column`.

**22.** O que `blogsDF.columns` devolve na transcrição, e o que há de estranho no resultado? *(Columns and Expressions)*

R: `Array(Campaigns, First, Hits, Id, Last, Published, Url)`. A ordem é alfabética, e não a ordem do schema declarado no exemplo anterior. *Esse detalhe é o item 4 do Nível 4.*

**23.** O que `$` faz em `blogsDF.sort($"Id".desc)`? *(Columns and Expressions)*

R: O capítulo diz que `$` é uma função no Spark que converte a coluna de nome `Id` em um `Column`. A expressão é idêntica a `sort(col("Id").desc)`, que usa uma função explícita para o mesmo fim.

**24.** Dê a definição de Row que o capítulo apresenta, e diga como os campos são acessados. *(Rows)*

R: Um row no Spark é um objeto `Row` genérico, com uma ou mais colunas. As colunas podem ter o mesmo tipo de dado ou tipos diferentes. Como `Row` é um objeto e uma coleção ordenada de campos, dá para instanciar um em qualquer linguagem suportada e acessar os campos por índice, começando em 0.

**25.** Nomeie as duas interfaces de leitura e escrita, e os formatos listados para o reader. *(Using DataFrameReader and DataFrameWriter)*

R: `DataFrameReader` e `DataFrameWriter`. Os formatos listados são JSON, CSV, Parquet, Text, Avro e ORC. O capítulo acrescenta NoSQL stores, RDBMSs e engines de streaming como Apache Kafka e Kinesis.

**26.** Quantas colunas e quantos registros o SF Fire data set tem, e o que a nota de rodapé 2 diz que foi feito com ele? *(Using DataFrameReader and DataFrameWriter)*

R: 28 colunas e mais de 4.380.660 registros. A nota de rodapé 2 diz que o dataset original tinha mais de 60 colunas, que os autores removeram colunas desnecessárias, descartaram registros com valores nulos ou inválidos, e acrescentaram uma coluna `Delay`.

**27.** Qual formato é o padrão do writer, qual compressão ele usa, e o que isso te poupa depois? *(Using DataFrameReader and DataFrameWriter)*

R: Parquet, com compressão snappy. Quando o DataFrame é escrito em Parquet, o schema é preservado como parte do metadado do arquivo. Leituras posteriores de volta para um DataFrame não exigem fornecer um schema à mão.

**28.** O que `saveAsTable` faz que `save` não faz? *(Saving a DataFrame as a Parquet file or SQL table)*

R: Registra metadado no Hive metastore. O capítulo adia tabelas SQL gerenciadas e não gerenciadas, metastores e DataFrames para o capítulo seguinte.

**29.** Como o capítulo define projection, e quais métodos fazem projection e filtragem? *(Projections and filters)*

R: "A projection in relational parlance is a way to return only the rows matching a certain relational condition by using filters." No Spark, projections são feitas com `select()` e filtros com `filter()` ou `where()`. *A definição de projection que o capítulo dá descreve um filtro, e isso é o item 5 do Nível 4.*

**30.** Quantos `CallType`s distintos estão registrados, e quais duas queries produzem esse número? *(Projections and filters)*

R: 32. A primeira usa `select("CallType")`, `where(col("CallType").isNotNull())` e `agg(countDistinct("CallType").alias("DistinctCallTypes"))`. A segunda troca o `agg` por `distinct()` e mostra 10 linhas.

**31.** Quais quatro coisas a query de conversão de timestamp faz, segundo a lista do próprio capítulo? *(Renaming, adding, and dropping columns)*

R: Converte o tipo da coluna existente de string para um timestamp suportado pelo Spark. Usa o format string `"MM/dd/yyyy"` ou `"MM/dd/yyyy hh:mm:ss a"`, conforme o caso. Depois da conversão, faz `drop()` da coluna velha e anexa a nova, nomeada no primeiro argumento de `withColumn()`. Atribui o DataFrame modificado a `fire_ts_df`.

**32.** Qual é o call type mais comum, e quais são as três maiores contagens? *(Aggregations)*

R: Medical Incident, com 2.843.475. Em seguida vêm Structure Fire com 578.998 e Alarms com 483.518.

**33.** O que a NOTE diz sobre `collect()`, `count()` e `take(n)`? *(Aggregations, NOTE)*

R: `collect()` é caro e perigoso em DataFrames muito grandes, porque pode causar exceções de out-of-memory. Diferente de `count()`, que devolve um único número ao driver, `collect()` devolve uma coleção com todos os objetos `Row`. Para espiar alguns registros, o melhor é `take(n)`, que devolve só os primeiros n.

**34.** O que o Spark 2.0 unificou, e quais duas características os Datasets assumem? *(The Dataset API; Figure 3-1)*

R: Unificou as APIs de DataFrame e Dataset como Structured APIs, com interfaces parecidas, para o desenvolvedor aprender um conjunto só. As duas características são typed APIs e untyped APIs. Abri a Figure 3-1. Ela mostra duas caixas empilhadas à esquerda, DataFrame em amarelo e Dataset em azul, com uma seta para uma caixa verde grande chamada "Structured APIs". Dessa caixa saem duas etiquetas. "Untyped APIs" traz dois itens, "DataFrame = Dataset[Row]" e "Alias in Scala". "Typed APIs" traz "Dataset[T]" e "In Scala & Java".

**35.** Cite a definição de Dataset que o capítulo dá, tirada da documentação de Dataset. *(The Dataset API)*

R: "a strongly typed collection of domain-specific objects that can be transformed in parallel using functional or relational operations. Each Dataset [in Scala] also has an untyped view called a DataFrame, which is a Dataset of `Row`."

**36.** Por que Datasets só fazem sentido em Java e Scala? *(Typed Objects, Untyped Objects, and Generic Rows)*

R: Porque Python e R não têm type safety em tempo de compilação. Neles os tipos são inferidos ou atribuídos dinamicamente durante a execução. Em Scala e Java os tipos ficam ligados a variáveis e objetos em tempo de compilação.

**37.** Reproduza a Table 3-6. *(Table 3-6)*

R:

| Language | Typed and untyped main abstraction | Typed or untyped |
|---|---|---|
| Scala | `Dataset[T]` e DataFrame (alias de `Dataset[Row]`) | Both typed and untyped |
| Java | `Dataset<T>` | Typed |
| Python | DataFrame | Generic `Row` untyped |
| R | DataFrame | Generic `Row` untyped |

**38.** Do que o Spark SQL engine por baixo cuida quando você usa Datasets? *(Dataset Operations)*

R: A criação, a conversão, a serialização e a desserialização dos objetos JVM. E o gerenciamento de memória fora do heap do Java, com ajuda dos Dataset encoders. O detalhamento fica para o Capítulo 6.

**39.** Na lista de DataFrames Versus Datasets, dê os dois casos em que o capítulo diz para usar Datasets e só Datasets. *(DataFrames Versus Datasets)*

R: Quando você quer type safety estrita em tempo de compilação e não se importa de criar várias case classes para um `Dataset[T]` específico. E quando você quer aproveitar a serialização eficiente do Tungsten com Encoders.

**40.** Reproduza a Figure 3-2. *(Figure 3-2)*

R: Abri a figura. O título dela é "Structured APIs In Spark", com uma seta laranja de duas pontas cobrindo SQL, DataFrames e Datasets. Duas linhas de erro:

| | SQL | DataFrames | Datasets |
|---|---|---|---|
| Syntax Errors | Runtime | Compile Time | Compile Time |
| Analysis Errors | Runtime | Runtime | Compile Time |

**41.** Os RDDs estão depreciados, segundo o capítulo, e em quais três cenários você deve considerá-los? *(When to Use RDDs)*

R: Não. O capítulo responde "a resounding no". A API de RDD continua suportada, embora todo o desenvolvimento futuro no Spark 2.x e no 3.0 siga com interface e semântica de DataFrame. Os três cenários são usar um pacote de terceiros escrito com RDDs, poder abrir mão da otimização de código, do uso eficiente de espaço e dos ganhos de performance de DataFrames e Datasets, e querer instruir o Spark com precisão sobre como executar uma query.

**42.** Desde qual release o capítulo data o Spark SQL, e qual padrão ele reivindica? *(Spark SQL and the Underlying Engine)*

R: O capítulo dá duas datas. Na abertura diz que o Spark SQL foi introduzido "in the early Spark 1.x releases". Nesta seção diz "Since its introduction in Spark 1.3". A alegação de padrão é que ele permite emitir queries "ANSI SQL:2003–compatible" sobre dado estruturado com schema. *No item 3 do Nível 5 verifiquei essa frase.*

**43.** Liste as seis coisas que o Spark SQL engine faz. *(Spark SQL and the Underlying Engine)*

R: Unifica os componentes do Spark e permite a abstração para DataFrames e Datasets em Java, Scala, Python e R. Conecta ao Apache Hive metastore e às suas tabelas. Lê e escreve dado estruturado com schema específico a partir de formatos estruturados, como JSON, CSV, Text, Avro, Parquet e ORC, e converte dado em tabelas temporárias. Oferece um shell interativo de Spark SQL para exploração rápida. Fornece uma ponte de e para ferramentas externas por conectores JDBC/ODBC padrão. Gera query plans otimizados e código compacto para a JVM.

**44.** Descreva a Figure 3-3. *(Figure 3-3)*

R: Abri a figura. Ela tem quatro faixas horizontais. No topo, quatro caixas de ferramentas externas: Tableau, Snowflake, Talend e uma caixa com apenas "…..". Na segunda faixa, três caixas: Spark Application à esquerda, JDBC/ODBC Connectors no meio e Spark SQL Shell à direita. As quatro ferramentas do topo se ligam só à caixa de JDBC/ODBC. Na terceira faixa, uma barra azul rotulada Spark SQL, com duas elipses roxas dentro dela, Catalyst Optimizer à esquerda e Tungsten à direita. Na base, cinco caixas de armazenamento: Hive Tables, JSON, Avro, Parquet e ORC. Todas as setas têm duas pontas.

**45.** Nomeie as quatro fases do Catalyst optimizer, em ordem. *(The Catalyst Optimizer)*

R: Analysis, logical optimization, physical planning e code generation.

**46.** O que acontece na fase 1? *(Phase 1: Analysis)*

R: O engine gera uma abstract syntax tree, ou AST, para a query SQL ou DataFrame. Nessa fase, nomes de coluna e de tabela são resolvidos consultando um `Catalog` interno, que é uma interface programática do Spark SQL com a lista de nomes de colunas, tipos de dado, funções, tabelas e bancos. Resolvidos todos, a query segue para a fase seguinte.

**47.** Quais dois estágios internos a fase 2 comporta, e quais otimizações ela nomeia? *(Phase 2: Logical optimization)*

R: Primeiro o Catalyst aplica uma abordagem de otimização baseada em regras padrão e constrói um conjunto de planos. Depois usa o cost-based optimizer, o CBO, para atribuir custos a cada plano. Os planos são dispostos como operator trees. As otimizações nomeadas são constant folding, predicate pushdown, projection pruning e simplificação de expressão booleana.

**48.** O que é whole-stage code generation, segundo o capítulo, e qual engine o executa? *(Phase 4: Code generation)*

R: Uma fase de otimização física de query que colapsa a query inteira em uma única função, elimina chamadas de função virtual e usa registradores de CPU para o dado intermediário. O engine Tungsten de segunda geração, introduzido no Spark 2.0, usa essa abordagem para gerar código RDD compacto para a execução final.

**49.** Quais quatro cabeçalhos de plano aparecem na saída de `explain(True)`, e o que desaparece entre o segundo e o terceiro? *(The Catalyst Optimizer)*

R: `== Parsed Logical Plan ==`, `== Analyzed Logical Plan ==`, `== Optimized Logical Plan ==` e `== Physical Plan ==`. O que desaparece é o nó `Project [State#10, Color#11, Count#12]`. O plano otimizado passa direto de `Aggregate` para `Relation`.

**50.** Quais dois nós `Exchange` aparecem no plano físico, e com qual número de partitions? *(The Catalyst Optimizer)*

R: `Exchange rangepartitioning(Total#24L DESC NULLS LAST, 200)` e `Exchange hashpartitioning(State#10, Color#11, 200)`. Os dois usam 200 partições. *No item 5 do Nível 5 verifiquei de onde vem esse 200 e o que mudou nele.*

**51.** Descreva a Figure 3-4. *(Figure 3-4)*

R: Abri a figura. Ela é uma cascata vertical. No topo, três caixas de entrada: SQL AST, DataFrame e Datasets, que convergem para "Unresolved Logical Plan". A etiqueta "Analysis" marca o passo seguinte, e uma caixa lateral chamada "Catalog" entra com uma seta nesse ponto, produzindo "Logical Plan". "Logical Optimization" leva a "Optimized Logical Plan". "Physical Planning" leva a uma pilha de caixas sobrepostas chamada "Physical Plans", que se ramifica em três setas para uma caixa "Cost Model". Dela sai "Selected Physical Plan". Por fim, "Code Generation" leva a uma caixa "RDDs".

**52.** Descreva a Figure 3-5. *(Figure 3-5)*

R: Abri a figura. Ela tem três painéis empilhados, ligados por setas grossas para baixo. O primeiro, "Logical Plan", mostra `events file` e `users table` alimentando um `join`, e o `join` alimentando um `filter` no topo. O segundo, "Physical Plan", move o `filter` para baixo: `scan (events)` alimenta `filter`, e `filter` mais `scan (users)` alimentam o `join`. O terceiro, "Physical Plan with Predicate Pushdown and Column Pruning", elimina o `filter` como nó separado e deixa só `optimized scan (events)` e `optimized scan (users)` alimentando o `join`.

## Nível 2 — Compreensão

Explique com suas próprias palavras, de três a seis frases. Não reutilize a formulação do capítulo.

**1.** Explique o vínculo exato entre opacidade e a impossibilidade de otimização. Por que uma lambda bloqueia a otimização e `groupBy("name").agg(avg("age"))` não? *(Spark: What's Underneath an RDD?; Key Merits and Benefits)*

R: Uma lambda chega ao Spark como código já compilado, e o motor não consegue abrir esse código para saber o que ele faz. Tudo que ele sabe é que existe uma função que recebe uma partição e devolve um iterador. Sem saber que a função é um filtro por valor de coluna, não dá para empurrar esse filtro para a leitura nem para reordená-lo. A cadeia `groupBy("name").agg(avg("age"))` não é código, é uma descrição. Ela nomeia uma coluna, uma operação de agrupamento e uma função de agregação conhecida. O motor pode inspecionar essa descrição, reconhecer o padrão e escolher o caminho físico. A diferença não é de nível de abstração, é de o Spark receber um objeto que ele consegue ler.

**2.** Explique o que os quatro benefícios da estrutura significam concretamente, usando os dois trechos de média de idade como caso. *(Key Merits and Benefits)*

R: Expressivity é a queda de três operações crípticas para uma linha que nomeia a intenção. Simplicity é o desaparecimento da aritmética manual de soma e contagem, que existia só para simular uma média. Composability é o fato de a query inteira caber em uma única expressão encadeada, montada com operadores prontos em vez de lógica caseira. Uniformity é a versão Scala ao lado ser quase idêntica, com diferença apenas na criação do DataFrame. O capítulo nota que os dois trechos de RDD, em Python e Scala, seriam bem diferentes entre si.

**3.** Explique por que declarar um schema de antemão evita "a separate job", e o que esse job custa. *(Schemas and Creating DataFrames)*

R: Um arquivo CSV ou JSON não guarda tipos. Para descobrir que uma coluna é inteira, o Spark precisa olhar valores reais, o que significa ler o arquivo. Essa leitura não é o trabalho da query, é uma leitura extra que acontece antes, e ela vira um job próprio no plano de execução. Em um arquivo de milhões de linhas isso é uma varredura completa paga duas vezes. Declarar o schema elimina a varredura, porque o tipo passa a ser dado de entrada e não resultado de observação.

**4.** Explique o terceiro benefício de um schema declarado, a detecção precoce de erros, e diga o que "early" significa aqui. *(Schemas and Creating DataFrames)*

R: Com um schema declarado, o Spark tem uma expectativa contra a qual comparar cada registro lido. Quando o dado não bate, o problema aparece na leitura e não vinte transformations depois. "Early" aqui é cedo na execução, não cedo na compilação. O schema é uma string ou um objeto construído em runtime, então nada disso é verificado antes de rodar. É por isso que a Figure 3-2 coloca analysis errors de DataFrame em runtime, mesmo quando o schema foi declarado à mão.

**5.** Explique a diferença entre um objeto `Column` e um nome de coluna dado como string, e quando a diferença importa. *(Columns and Expressions)*

R: Uma string é só um nome, e o Spark a resolve contra o schema para achar a coluna. Um `Column` é um objeto com métodos públicos, então ele carrega comportamento além do nome. A diferença aparece no momento em que você precisa de uma expressão em vez de uma referência. `select("Hits")` funciona porque só nomeia. `select(col("Hits") * 2)` precisa do objeto, porque a multiplicação é um método do `Column`. `expr("Hits * 2")` chega ao mesmo lugar por outro caminho, escrevendo a expressão como texto que o Spark interpreta.

**6.** Explique por que a imutabilidade faz `withColumnRenamed()` devolver um DataFrame novo, e o que isso custa em uma cadeia longa. *(Renaming, adding, and dropping columns, NOTE)*

R: Transformations de DataFrame são imutáveis, então renomear não altera o objeto original. A operação produz um DataFrame novo e o antigo continua existindo, com o nome de coluna velho. Isso garante que a variável anterior siga válida e reutilizável. O custo em cadeia longa não é de memória, porque nada foi materializado. É de plano. Cada passo acrescenta um nó, e a cadeia de conversão de datas do capítulo, com três `withColumn` e três `drop`, empilha seis nós para produzir três colunas.

**7.** Explique por que o capítulo converte três colunas de string para timestamps antes de qualquer análise de data. *(Renaming, adding, and dropping columns)*

R: As colunas `CallDate`, `WatchDate` e `AvailableDtTm` vieram do CSV como strings, porque o schema declarado as tipou como `StringType`. String não tem ordem de calendário nem componentes. Funções como `year()`, `dayofmonth()` e `dayofweek()` esperam um tipo de data ou timestamp e não têm o que fazer com texto. Converter com `to_timestamp()` e um format string dá ao Spark o significado de cada pedaço do texto. Depois disso a coluna passa a suportar comparação, extração e agrupamento por tempo.

**8.** Explique o que o Dataset te dá e o DataFrame não dá, em termos do que o compilador enxerga. *(Typed Objects, Untyped Objects, and Generic Rows; Figure 3-2)*

R: Em um DataFrame, cada linha é um `Row` genérico e as colunas são referenciadas por nome, em string ou em objeto `Column`. O compilador não conhece esses nomes, então um nome errado só falha quando o Spark tenta resolvê-lo contra o schema, em runtime. Em um Dataset, cada linha é uma instância de uma classe, e as colunas são membros dessa classe. Um nome errado deixa de ser string e vira um membro inexistente, o que o compilador rejeita. A Figure 3-2 traduz isso: analysis errors saem de runtime para compile time só na coluna de Datasets.

**9.** Explique por que a mesma proteção não pode existir em Python, indo além de "Python não tem tipos". *(Typed Objects, Untyped Objects, and Generic Rows)*

R: A proteção do Dataset não vem de o programador anotar tipos, vem de existir uma etapa de compilação que verifica os nomes contra uma classe antes de rodar. Em Python não há essa etapa, então não há momento em que um erro de nome pudesse ser detectado sem executar. O capítulo diz isso pelo lado dos tipos, que em Python são inferidos ou atribuídos durante a execução. Acrescento a consequência de projeto: o Dataset precisa de um objeto JVM tipado para mapear, e o Python não tem objetos JVM para mapear.

**10.** Explique o que significa que "a DataFrame is really a `Dataset[Row]` in Scala", e por que o capítulo diz "in Scala". *(Columns and Expressions; The Dataset API)*

R: Em Scala, DataFrame não é uma classe separada, é um apelido de tipo para `Dataset[Row]`. Existe uma abstração só, e o DataFrame é o caso dela em que o parâmetro de tipo é o `Row` genérico. A qualificação "in Scala" existe porque a equivalência é uma propriedade do sistema de tipos de Scala. Em Java não há apelido de tipo, e o programador escreve `Dataset<Row>` de forma explícita. Em Python e R não existe `Dataset` para ser apelidado, então o DataFrame é a única abstração e a frase não se aplica.

**11.** Explique o trade-off que o capítulo apresenta entre operadores da DSL e expressões nativas da linguagem nas operações de Dataset. *(Dataset Operations)*

R: O filtro de DataFrame se escreve com operadores da DSL, que são os mesmos em Python, Scala, Java e R. Ele é agnóstico de linguagem porque o que se escreve é uma expressão que o Spark interpreta, não código executável. O filtro de Dataset se escreve como uma lambda Scala ou Java, com acesso a campos por ponto. Ganha-se legibilidade orientada a objeto e verificação em tempo de compilação. Perde-se a portabilidade entre linguagens e, o que o capítulo não diz, perde-se também a transparência para o otimizador, porque uma lambda volta a ser código opaco.

**12.** Explique por que o capítulo pode dizer que `select()` é semanticamente parecido com `map()` no exemplo `dsTemp`, e onde a analogia para. *(Dataset Operations, NOTE)*

R: As duas versões do exemplo escolhem quatro campos e produzem o mesmo resultado, então o efeito observável é igual. O `map` faz isso construindo uma tupla nova a partir do objeto, e o `select` faz isso projetando colunas nomeadas. A analogia para no que cada um entrega ao motor. O `select` entrega uma projeção que o Catalyst enxerga e pode empurrar para a leitura. O `map` entrega uma função. O capítulo enuncia a equivalência semântica e cala sobre a diferença de custo.

**13.** Explique, a partir do material do próprio capítulo, por que a resposta para "are RDDs deprecated?" é não e ainda assim o capítulo inteiro argumenta contra usá-los. *(When to Use RDDs)*

R: São duas afirmações sobre coisas diferentes. Deprecação é um compromisso de suporte, e o capítulo diz que a API de RDD continua suportada. A recomendação é sobre onde o valor novo aparece, e o capítulo diz que todo desenvolvimento futuro no 2.x e no 3.0 segue com interface e semântica de DataFrame. O RDD também não sai de cena por baixo, já que DataFrames e Datasets são construídos sobre ele e o code generation produz código RDD. O que se depreciou na prática foi escrever RDD à mão, não o RDD.

**14.** Explique para que serve o `Catalog` do Catalyst, usando apenas a fase 1. *(Phase 1: Analysis)*

R: Uma query recém-escrita cita nomes que ainda não significam nada, como `State` ou `MNM_TABLE_NAME`. A AST registra esses nomes como referências não resolvidas. O `Catalog` é a estrutura que sabe quais colunas, tipos, funções, tabelas e bancos existem, e é consultado para ligar cada nome a um objeto real. Sem isso não há como saber se `State` existe, de que tipo é, nem se `sum` é uma função conhecida. É por essa razão que a fase se chama analysis e não parsing. Parsing produz a árvore, analysis dá significado a ela.

**15.** Explique o que o whole-stage code generation remove, e por que remover isso torna a execução mais rápida. *(Phase 4: Code generation)*

R: Um plano físico é uma árvore de operadores, e cada operador é um objeto. Executar a árvore significa, para cada linha, chamar o método de cada operador, uma chamada virtual por operador por linha. Whole-stage code generation colapsa a query inteira em uma função só. Com uma função só não há chamadas virtuais a fazer, e o dado intermediário pode ficar em registradores de CPU em vez de ir e voltar da memória. O ganho é de eficiência de CPU, e não de I/O. Essa é a diferença em relação ao argumento do Capítulo 1, que atribuía a velocidade a manter intermediários em memória.

**16.** Explique por que o capítulo diz que a mesma computação em Python e em SQL termina em "identical bytecode", e o que na Figure 3-4 sustenta essa alegação. *(The Catalyst Optimizer; Figure 3-4)*

R: A Figure 3-4 mostra três entradas diferentes, SQL AST, DataFrame e Datasets, convergindo para uma única caixa, "Unresolved Logical Plan". Depois desse ponto o desenho tem um caminho só. Se as três entradas viram o mesmo plano não resolvido, e o resto do pipeline é comum, então a saída da última caixa é a mesma. A alegação de bytecode idêntico é uma leitura direta do formato do desenho. O que o desenho não cobre é o caso em que a entrada não é traduzível para plano, como uma lambda de Dataset ou uma UDF de Python. O capítulo não trata desse caso.

---

## Nível 3 — Aplicação e transferência

Cenários concretos. O capítulo te equipa para responder, mas não responde por você.

**1.** Você tem 900 GB de logs de acesso de servidor web em JSON, um objeto por linha, 14 campos. Você precisa de percentis de tempo de resposta por endpoint. Argumente a partir do capítulo se deve declarar um schema ou deixar o Spark inferi-lo, e quantifique o que a inferência te custa aqui. *(Schemas and Creating DataFrames; NOTE on samplingRatio)*

R: Declarar. O capítulo dá três razões e as três se aplicam. A inferência criaria um job separado só para ler uma porção grande de 900 GB e determinar tipos, que é exatamente o caso que ele chama de caro e demorado. Declarar também livra o Spark do trabalho de inferir, e faz erros de dado aparecerem na leitura em vez de mais adiante.

Quantificando com o que o capítulo dá: a inferência custa uma varredura extra sobre o arquivo, antes de qualquer trabalho útil. Com 900 GB isso é ler 1,8 TB para produzir o mesmo resultado.

A alternativa intermediária que o capítulo oferece é `samplingRatio`. Com 0,001 a amostra são cerca de 900 MB em vez de 900 GB. O capítulo não avisa do risco, e ele é real. Um campo que é inteiro nas primeiras mil linhas e vem nulo ou textual depois é tipado errado pela amostra. *No item 9 do Nível 5 verifiquei o comportamento exato dessa opção.*

**2.** Escreva o schema em DDL para um catálogo de filmes com `movie_id` inteiro, `title` string, `release_date` date, `genres` como lista de strings e `rating` decimal. Depois diga qual dessas cinco colunas o próprio exemplo de blogs do capítulo teria errado, e por quê. *(Two ways to define a schema; Table 3-1)*

R: A DDL:

```
`movie_id` INT, `title` STRING, `release_date` DATE,
`genres` ARRAY<STRING>, `rating` DECIMAL(3,1)
```

A coluna que o exemplo do capítulo teria errado é `release_date`. A Table 3-1 rotula a coluna `Published` como `(Date)`, e a DDL do Example 3-6 a declara `STRING`. A tabela e o schema discordam, e o schema venceu, porque a saída de `printSchema` traz `Published: string`. O efeito prático é o mesmo da seção de conversão de datas: a coluna não aceita `year()` nem comparação de calendário sem passar por `to_timestamp()` ou `to_date()` antes.

Sobre `rating`, o capítulo lista `DecimalType` nas tabelas de tipo e nunca mostra a sintaxe DDL dele. A forma `DECIMAL(3,1)` vem de fora do capítulo.

**3.** Um pipeline de sensores lê um CSV de 40 milhões de linhas com 12 colunas. Você precisa da leitura média por estação, para as estações cuja leitura ultrapassou um limiar pelo menos uma vez. Escreva a cadeia no estilo da seção do corpo de bombeiros, e marque cada chamada como transformation ou action. *(Projections and filters; Aggregations)*

R:

```python
readings_df = spark.read.csv(path, header=True, schema=readings_schema)

hot_stations_df = (readings_df
  .select("station_id", "reading")
  .where(col("reading") > threshold)
  .select("station_id")
  .distinct())

avg_df = (readings_df
  .join(hot_stations_df, "station_id")
  .groupBy("station_id")
  .agg(avg("reading").alias("avg_reading"))
  .orderBy("avg_reading", ascending=False))

avg_df.show(n=20, truncate=False)
```

Classificação: `read.csv` com schema declarado é leitura e não dispara job de inferência. `select`, `where`, `distinct`, `join`, `groupBy`, `agg` e `orderBy` são transformations. `show` é a action.

O que o capítulo cobre e o que não cobre: ele demonstra `select`, `where`, `distinct`, `groupBy`, `agg` e `orderBy` diretamente na seção do corpo de bombeiros. Ele lista `join` na Table 2-1 do capítulo anterior e não o demonstra aqui, remetendo o assunto para depois. A parte de custo, com dois shuffles no `distinct` mais o `join` e o `groupBy`, não tem tratamento nenhum neste capítulo.

**4.** Seu time escreve em Python e quer a segurança em tempo de compilação que o capítulo anuncia para Datasets. Responda ao pedido usando apenas este capítulo. *(Typed Objects, Untyped Objects, and Generic Rows; Table 3-6; Figure 3-2)*

R: Não dá, e o capítulo é explícito. A seção diz que Datasets fazem sentido apenas em Java e Scala, e que em Python e R apenas DataFrames fazem sentido. A Table 3-6 registra Python com uma única abstração, DataFrame, marcada como "Generic Row untyped".

O motivo que o capítulo dá é que Python não tem type safety em tempo de compilação, porque os tipos são inferidos ou atribuídos durante a execução.

O que sobra pela Figure 3-2 é a coluna do meio. Syntax errors ainda saem em compile time, e analysis errors ficam em runtime. Ou seja, em Python o erro de sintaxe é pego pelo interpretador e o erro de nome de coluna só aparece quando a query roda.

A lista de DataFrames Versus Datasets fecha a resposta com uma linha própria para o caso: usuário de Python usa DataFrames e desce para RDDs quando precisa de mais controle. *No item 1 do Nível 5 verifiquei o estado atual dessa restrição.*

**5.** Você herda um job que termina com `df.collect()` e depois percorre o resultado em Python para escrever um relatório. O DataFrame tem 30 milhões de linhas. Diagnostique o problema e prescreva a correção, os dois a partir do capítulo. *(Aggregations, NOTE)*

R: O problema é o `collect()`. A NOTE do capítulo diz que ele é caro e perigoso em DataFrames muito grandes, porque pode causar exceções de out-of-memory. Ele devolve uma coleção com todos os objetos `Row` do DataFrame inteiro, e essa coleção precisa caber na memória do driver. Trinta milhões de linhas não é um número que se traga para uma máquina só.

A prescrição do capítulo tem duas partes. Se a intenção é espiar registros, `take(n)` devolve só os primeiros n. Se a intenção é produzir um agregado, o caminho é fazer a agregação no cluster e trazer o resultado pequeno, como faz `count()`, que devolve um único número ao driver.

Para o caso do relatório, a leitura correta é que o laço em Python está fazendo no driver um trabalho que a DataFrame API faria distribuído. A seção de Aggregations mostra o padrão substituto: `groupBy`, `agg` e `orderBy`, com `show` no fim.

**6.** Um colega renomeia uma coluna com `withColumnRenamed()` e se surpreende que o nome antigo continua funcionando na variável que ele tinha antes. Explique isso com a NOTE do capítulo, e diga o que teria que mudar para a expectativa dele estar certa. *(Renaming, adding, and dropping columns, NOTE)*

R: A NOTE responde direto. Como transformations de DataFrame são imutáveis, renomear com `withColumnRenamed()` devolve um DataFrame novo e mantém o original com o nome de coluna antigo. A variável antiga aponta para o objeto antigo, que não foi tocado.

Para a expectativa dele estar certa, o DataFrame teria que ser mutável, ou seja, a operação teria que alterar o objeto no lugar em vez de produzir outro. Isso quebraria a lineage descrita no Capítulo 2, porque não haveria mais como refazer um passo a partir da entrada preservada.

O conserto no código dele é uma linha: reatribuir, como o capítulo faz com `new_fire_df = fire_df.withColumnRenamed(...)`.

**7.** Você precisa publicar um data set curado para analistas que usam várias ferramentas. Usando apenas este capítulo, decida entre escrever arquivos Parquet e salvar como tabela SQL, e nomeie o que cada escolha te dá. *(Saving a DataFrame as a Parquet file or SQL table; Spark SQL and the Underlying Engine; Figure 3-3)*

R: Escrever Parquet com `write.format("parquet").save(path)` entrega arquivos com o schema embutido no metadado, comprimidos com snappy. Quem lê depois não precisa fornecer schema. O que não vem junto é descoberta: o consumidor precisa saber o caminho.

Salvar como tabela com `saveAsTable` faz a mesma escrita e ainda registra metadado no Hive metastore. O ganho é o nome. A tabela passa a ser localizável por catálogo e alcançável por SQL.

A Figure 3-3 fecha o argumento para o caso de várias ferramentas. Ela desenha Tableau, Snowflake e Talend chegando ao Spark SQL apenas pela faixa de JDBC/ODBC Connectors, e o Spark SQL alcançando Hive Tables, JSON, Avro, Parquet e ORC na base. Ferramenta externa entra por conector e query, não por caminho de arquivo. Para o cenário da pergunta, a tabela é a escolha.

O capítulo adia tabelas gerenciadas e não gerenciadas para o capítulo seguinte, então a decisão sobre quem é dono dos arquivos fica em aberto aqui.

**8.** Dois times escrevem a mesma agregação, um em PySpark e outro em SQL. Um terceiro pergunta qual das duas vai ser mais rápida. Responda com o capítulo, e nomeie a única condição em que a sua resposta deixa de valer. *(The Catalyst Optimizer; Figure 3-4)*

R: Nenhuma das duas. O capítulo diz que os dois blocos passam pelo mesmo processo, terminam em um query plan parecido e produzem bytecode idêntico para a execução. Ele imprime os dois lado a lado, o `count_mnm_df` em Python e o `SELECT ... GROUP BY ... ORDER BY` em SQL, para sustentar isso.

A Figure 3-4 mostra por quê. SQL AST, DataFrame e Datasets convergem para o mesmo "Unresolved Logical Plan", e dali para a frente existe um caminho só.

A condição em que a resposta para de valer é a entrada deixar de ser traduzível para plano. Uma lambda de Dataset ou uma UDF em Python não vira nó de plano, e o capítulo não menciona esse caso. A pista está na Figure 3-4 mesmo: ela lista três entradas, e as três são declarativas.

**9.** Você precisa detectar dispositivos cuja bateria está abaixo de um limiar em um pipeline Scala em que o JSON tem 15 campos e o código seguinte toca quatro deles. Monte isso pelo caminho de Dataset do capítulo, e diga o que a case class te compra em relação a um DataFrame. *(Scala: Case classes; Dataset Operations)*

R: A montagem segue o exemplo IoT do capítulo:

```scala
case class DeviceReading(device_id: Long, device_name: String,
  battery_level: Long, cca3: String)

val ds = spark.read
  .json(path)
  .select($"device_id", $"device_name", $"battery_level", $"cca3")
  .as[DeviceReading]

val failing = ds.filter(d => d.battery_level < threshold)
```

O que a case class compra: os quatro campos viram membros, então `d.battery_level` é verificado pelo compilador. Um erro de digitação vira erro de compilação em vez de erro de runtime, que é o eixo horizontal da Figure 3-2. E cada elemento do Dataset passa a ser um objeto JVM real, com o qual o resto do código Scala trabalha por notação de ponto.

Uma ressalva que o capítulo dá de passagem e que importa aqui: ao usar Datasets, o Spark SQL engine cuida da criação, conversão, serialização e desserialização desses objetos JVM. Isso é trabalho por linha que o DataFrame não paga.

**10.** O seu reader declara um schema com `nullable = false` para toda coluna e o job depois falha em um nulo. Usando as duas saídas de schema do capítulo, explique o que você deveria ter esperado. *(Example 3-6 output; Example 3-7 output)*

R: O capítulo mostra o problema sem comentá-lo. O Example 3-6 cria dado estático em memória com o mesmo schema e imprime `nullable = false` em todas as colunas. O Example 3-7 declara `false` em todos os `StructField` e lê de um arquivo JSON, e a saída imprime `nullable = true` em todas as colunas, incluindo o `containsNull` do array.

Ou seja, o `false` declarado não sobreviveu à leitura de arquivo. O que se deveria esperar é que declarar não nulo em uma fonte externa é uma intenção, e não uma garantia imposta na leitura. O capítulo afirma que as duas saídas não diferem, e elas diferem exatamente nesse ponto. *Esse é o item 3 do Nível 4.*

**11.** Pedem que você justifique migrar um job de ETL baseado em RDD para DataFrames, a um lead que quer razão técnica e não razão de estilo. Construa o argumento apenas com este capítulo. *(Spark: What's Underneath an RDD?; Key Merits and Benefits; When to Use RDDs)*

R: O argumento técnico tem uma peça só, e é a opacidade. Em RDD, a compute function chega ao Spark como lambda, e o Spark não sabe se ela filtra, junta, seleciona ou agrega. Sem enxergar a expressão, ele não pode rearranjar a computação em um query plan eficiente. Todo ganho de eficiência fica a cargo de quem escreveu.

Em DataFrame, a mesma lógica chega como descrição inspecionável. O capítulo diz que o Spark consegue analisar a query, entender a intenção e otimizar ou rearranjar as operações. A Figure 3-5 mostra o tipo de rearranjo que isso libera: o `filter` desce por baixo do `join` e depois some dentro de um scan otimizado, com predicate pushdown e column pruning.

Há um segundo ponto de tipo. Em RDD o Spark não conhece o tipo dentro de `T`, então só consegue serializar o objeto como bytes, sem compressão. Com schema ele conhece cada coluna e o seu tipo.

O que a migração não resolve, e vale dizer ao lead: o capítulo lista três casos em que RDD continua sendo a escolha, e um deles é depender de um pacote de terceiros escrito com RDDs.

**12.** Uma query sobre uma tabela Parquet larga lê 60 colunas quando o relatório precisa de 4, e aplica um filtro depois de um join. Nomeie as duas otimizações do Catalyst que deveriam resolver isso, e diga qual figura mostra cada uma. *(Phase 2: Logical optimization; Figure 3-5)*

R: As duas são projection pruning e predicate pushdown, e as duas estão na lista de otimizações da fase 2.

A Figure 3-5 mostra as duas no terceiro painel, rotulado "Physical Plan with Predicate Pushdown and Column Pruning". Comparando os painéis: no primeiro o `filter` está no topo, acima do `join`. No segundo ele desceu para baixo, entre `scan (events)` e o `join`. No terceiro ele desapareceu como nó, absorvido por um `optimized scan (events)`. Ou seja, o filtro deixou de ser um passo e virou parte da leitura.

O capítulo nomeia as duas otimizações na fase 2, que é lógica, e ilustra o resultado no plano físico. A pergunta de onde exatamente elas acontecem fica sem resposta clara no texto.

**13.** Você precisa explicar a uma analista por que a query SQL dela e o seu job em PySpark produzem o mesmo plano físico com dois nós `Exchange`. Use o plano impresso no capítulo. *(The Catalyst Optimizer)*

R: Os dois `Exchange` são as duas redistribuições que a query pede, e nenhuma delas foi escolhida por quem escreveu.

O primeiro, de baixo para cima, é `Exchange hashpartitioning(State#10, Color#11, 200)`. Ele existe porque o `GROUP BY State, Color` precisa juntar todas as linhas de um mesmo par estado e cor na mesma máquina. O plano faz uma agregação parcial antes dele, com `partial_sum`, e a agregação final depois.

O segundo é `Exchange rangepartitioning(Total#24L DESC NULLS LAST, 200)`. Ele existe porque o `ORDER BY Total DESC` é global, e ordenar tudo exige reparticionar por faixa de valor.

O ponto para a analista é que a query dela e o job em Python descrevem o mesmo trabalho, então o Catalyst produz o mesmo plano. Os 200 nos dois nós vêm de configuração, não do código. *No item 5 do Nível 5 verifiquei o que aconteceu com esse número.*

---

## Nível 4 — Análise e síntese

Raciocínio que cruza seções. Respostas defensáveis em vez de resposta única, mas todos os ingredientes estão no capítulo.

**1.** O capítulo define um DataFrame como "distributed in-memory tables". Teste essa frase contra o resto do capítulo. Um DataFrame está em memória? *(The DataFrame API; Aggregations, NOTE)*

R: Não necessariamente, e o capítulo derruba a frase em três lugares.

Primeiro, a seção de Aggregations traz uma NOTE dizendo que, para DataFrames grandes com queries frequentes, você poderia se beneficiar de caching, e que estratégias de cache ficam para capítulos posteriores. Se o DataFrame já estivesse em memória por definição, não haveria o que cachear.

Segundo, o capítulo herda a lazy evaluation do Capítulo 2 e a usa o tempo todo. Um DataFrame construído por `select` e `where` não tem dado nenhum até a action chegar. Ele é um plano.

Terceiro, a própria seção de leitura diz que o `spark.read.csv()` devolve um DataFrame de rows e colunas nomeadas com os tipos ditados pelo schema. Devolver um DataFrame de 4,3 milhões de registros na hora da chamada é justamente o que não acontece.

A frase que os autores provavelmente quiseram dizer: "Um DataFrame apresenta o dado distribuído como uma tabela de colunas nomeadas e tipadas, e o Spark pode materializá-la em memória quando a execução exige."

O erro é o mesmo de forma do Capítulo 1, que chamava uma partição de "a DataFrame in memory". Os dois trocam a abstração lógica pela sua realização física.

**2.** A Table 3-3 e a Table 3-5 descrevem ambas o Python e dão convenções de instanciação diferentes. Decida qual está certa usando apenas o capítulo, e diga o que o erro revela sobre como as tabelas foram produzidas. *(Tables 3-3 and 3-5; Example 3-6)*

R: A Table 3-5 está certa e a Table 3-3 está errada, e dá para decidir sem sair do capítulo.

O código Python do capítulo é a prova. O Example 3-6 escreve `StructField("author", StringType(), False)`, com parênteses e sem prefixo. Isso é a convenção da Table 3-5, não a da Table 3-3. Nenhum trecho Python do capítulo escreve `DataTypes.StringType`.

O que o erro revela é a origem das tabelas. A Table 3-2 é a tabela de Scala, e a coluna "API to instantiate" dela traz `DataTypes.ByteType`, que é a forma de Java e Scala. A Table 3-3 é a tabela de Python e repete essa coluna inteira sem tradução. A Table 3-5, que é de tipos complexos em Python, foi traduzida. Ou seja, a Table 3-3 é a Table 3-2 com a coluna do meio trocada e a da direita esquecida.

A consequência prática é ruim: a Table 3-3 é a primeira tabela de tipos que um leitor de Python encontra, e ela ensina uma sintaxe que não existe. *No item 8 do Nível 5 verifiquei o que existe.*

**3.** O capítulo diz que a saída Scala do Example 3-7 "is no different than that from the Python program". Compare os dois schemas impressos e decida se a frase é verdadeira. *(Examples 3-6 and 3-7)*

R: É falsa, e a diferença é substantiva e não cosmética.

| Item | Example 3-6, Python | Example 3-7, Scala |
|---|---|---|
| origem do dado | lista estática em memória | arquivo JSON |
| definição do schema | string DDL | `StructType` programático |
| `nullable` de todas as colunas | `false` | `true` |
| `containsNull` do array | `false` | `true` |
| ordem das colunas na saída | ordem do schema | ordem do schema |

Os dois programas declaram os mesmos nomes e os mesmos tipos, e os dois declaram os campos como não nulos. Só um dos dois imprime não nulos.

A causa é a fonte. Dado criado em memória a partir de uma lista fechada permite ao Spark honrar o `false`. Dado lido de um arquivo externo não permite, porque nada garante que uma chave esteja presente em toda linha do JSON. O Spark relaxa o schema na leitura.

Isso importa mais do que parece. Nullability entra no plano e habilita otimizações. Um leitor que declara `false` esperando que o Spark rejeite nulos vai receber nulos, e o próprio capítulo prometeu, na lista de benefícios do schema, que declarar permite "detect errors early if data doesn't match the schema". Nesta leitura o erro não é detectado, o schema é que cede.

Os autores compararam as duas saídas pelas colunas e não pelas anotações.

**4.** `blogsDF.columns` devolve as colunas em ordem alfabética. Reconcilie isso com o código mostrado imediatamente antes. *(Columns and Expressions; Example 3-7)*

R: Não reconcilia. O código imediatamente anterior é o Example 3-7, que lê o JSON com `spark.read.schema(schema).json(jsonFile)` e um schema em que a ordem é `Id`, `First`, `Last`, `Url`, `Published`, `Hits`, `Campaigns`. Um DataFrame criado com schema explícito tem essa ordem, e `columns` devolve a ordem do schema.

A saída impressa é `Array(Campaigns, First, Hits, Id, Last, Published, Url)`, que é ordem alfabética.

A explicação mais econômica é que a transcrição veio de uma sessão em que o JSON foi lido sem schema. Leitura de JSON com inferência ordena as chaves, e o resultado é alfabético. A saída de `sort(col("Id").desc)`, algumas linhas depois, confirma: ela também imprime as colunas em ordem alfabética, começando por `Campaigns`.

Duas conclusões. A primeira é que os blocos de saída desta seção não foram produzidos pelo código impresso ao lado. A segunda é pedagógica e mais séria: um leitor que rodar o código do capítulo vai ver outra ordem de coluna, e não tem no texto nenhuma pista de por quê.

**5.** A definição de "projection" do capítulo descreve um filtro. Escreva a definição correta, e mostre, pelo código do próprio capítulo, que os autores usam o termo corretamente mesmo depois de defini-lo errado. *(Projections and filters; Phase 2: Logical optimization)*

R: A frase do capítulo é "A projection in relational parlance is a way to return only the rows matching a certain relational condition by using filters". Isso é a definição de seleção, não de projeção. Em álgebra relacional, projeção escolhe colunas e seleção escolhe linhas.

A definição correta: projeção é a operação que devolve um subconjunto das colunas de uma relação, possivelmente transformadas, preservando as linhas.

O código ao lado usa o termo certo. A frase seguinte diz que no Spark projections são feitas com `select()` e filtros com `filter()` ou `where()`, o que separa as duas coisas corretamente. E o exemplo faz `select("IncidentNumber", "AvailableDtTm", "CallType")` seguido de `where(...)`, ou seja, projeta três colunas e depois filtra linhas.

A frase errada é uma colagem das duas metades da frase seguinte. Quem lê só a definição sai achando que `select` filtra linhas.

O efeito colateral aparece no restante do capítulo. A seção se chama "Projections and filters" e a lista de otimizações do Catalyst traz "projection pruning". Um leitor que absorveu a definição errada vai entender projection pruning como eliminação de filtros, que é o contrário do que a Figure 3-5 mostra.

**6.** O capítulo nomeia uma coluna, `AlarmDtTm`, que não existe no schema dele mesmo. Encontre a discrepância e diga o que ela custa a quem está acompanhando. *(Using DataFrameReader and DataFrameWriter; Renaming, adding, and dropping columns)*

R: A prosa diz que "the columns `CallDate`, `WatchDate`, and `AlarmDtTm` are strings rather than either Unix timestamps or SQL dates". O `fire_schema` impresso algumas páginas antes tem 28 `StructField` e nenhum deles se chama `AlarmDtTm`. A coluna existente é `AvailableDtTm`.

O código logo abaixo confirma. Ele converte `CallDate`, `WatchDate` e `AvailableDtTm`, e a saída traz `IncidentDate`, `OnWatchDate` e `AvailableDtTS`.

O custo para quem acompanha é direto. Quem copiar o nome da prosa recebe um erro de análise em runtime, que é exatamente a célula que a Figure 3-2 marca como "Runtime" para DataFrames. O capítulo produz, sem querer, um exemplo do próprio risco que a figura descreve.

O padrão é o mesmo do item anterior: a prosa foi escrita a partir de uma versão do dataset e o código a partir de outra.

**7.** O capítulo mostra dois trechos Scala que chamam `isNotNull` de formas diferentes, e um trecho de Dataset com chaves desbalanceadas. Liste os defeitos de código deste capítulo e ordene-os por quanto custam ao leitor. *(Dataset Operations; Projections and filters; Rows)*

R: Encontrei cinco.

1. **`ds.filter({d => {d.temp > 30 && d.humidity > 70})`.** Chaves desbalanceadas. Abre duas e fecha uma. Não compila. Custo alto, porque é a primeira operação de Dataset do capítulo e o leitor a copia.
2. **`.where($"CallType".isNotNull())` contra `.where(col("CallType").isNotNull)`.** As duas formas aparecem em snippets Scala vizinhos. Custo médio. O leitor não sabe qual é a assinatura, e o capítulo não diz.
3. **`AlarmDtTm`.** Coluna que não existe no schema, tratada no item 6. Custo médio.
4. **`Row(6, "Reynold", "Xin", "https://tinyurl.6", 255568, "3/2/2015", Array(...))`.** O valor de hits é 255568, e a Table 3-1 traz 25568. A ordem também troca `Hits` e `Published` em relação ao schema. Custo baixo, porque o exemplo só serve para mostrar acesso por índice.
5. **`blogsDF.col("Id")` devolvendo `res3: ... = id`.** O nome sai em minúscula. Custo baixo.

O critério de ordenação é se o defeito impede a execução, se ele confunde a sintaxe, ou se ele é só um valor impresso errado. O primeiro impede. O segundo e o terceiro confundem. O quarto e o quinto são ruído.

Um padrão atravessa a lista: os defeitos se concentram nos trechos Scala. Os trechos Python do capítulo rodam.

**8.** A lista de DataFrames Versus Datasets contém dois bullets que apontam em direções opostas quanto a eficiência. Encontre-os e decida se eles de fato se contradizem. *(DataFrames Versus Datasets; Dataset Operations)*

R: São estes dois:

- "If you want to take advantage of and benefit from Tungsten's efficient serialization with Encoders, use Datasets."
- "If you want space and speed efficiency, use DataFrames."

Lidos como estão, eles se contradizem. Um manda usar Dataset por eficiência de serialização e o outro manda usar DataFrame por eficiência de espaço e velocidade.

Eles não se contradizem de fato, e a chave está em o que cada um mede. O encoder torna eficiente a representação binária de um objeto de domínio. Ele existe porque um Dataset precisa converter um objeto JVM tipado para o formato interno e de volta. Um DataFrame não paga essa conversão, porque `Row` já é o formato interno.

Reescrevendo os dois para que parem de brigar: "Entre serializar objetos de domínio com Java serialization e serializá-los com encoders, use encoders, que é o que o Dataset faz. Entre trabalhar com objetos de domínio e trabalhar com `Row`, o `Row` é mais barato, porque não há conversão."

O capítulo tem as duas peças e nunca as junta. Ele nomeia a conversão de objetos JVM como responsabilidade do engine na seção de Dataset Operations e não diz que ela custa. *No item 6 das Discordâncias do Nível 6 mostro que o outro livro fornece exatamente a peça que falta.*

**9.** A prosa do capítulo põe o cost-based optimizer na fase 2, e a Figure 3-4 põe um Cost Model em outro lugar. Reconcilie os dois, e decida qual deles o resto do capítulo sustenta. *(Phase 2: Logical optimization; Phase 3: Physical planning; Figure 3-4)*

R: A prosa diz que na fase 2, logical optimization, o Catalyst constrói um conjunto de planos e depois usa o CBO para atribuir custos a cada um.

A Figure 3-4 desenha outra coisa. Ela põe "Logical Optimization" entre "Logical Plan" e "Optimized Logical Plan", com uma caixa só de cada lado. O "Cost Model" aparece um andar abaixo, na fase de "Physical Planning", recebendo três setas de uma pilha chamada "Physical Plans" e produzindo "Selected Physical Plan".

Ou seja, a figura coloca a escolha por custo entre planos físicos e a prosa a coloca entre planos lógicos.

A figura tem o resto do capítulo do lado dela. A descrição da fase 3 diz que o Spark SQL gera um plano físico ótimo para o plano lógico selecionado, usando operadores físicos que existem no engine de execução. Escolher entre operadores físicos é onde custo faz sentido, porque é onde as alternativas diferem em trabalho real. Duas árvores lógicas equivalentes não têm custo, elas têm forma.

O plano impresso confirma. A saída de `explain(True)` traz um `== Optimized Logical Plan ==` único e um `== Physical Plan ==` único, e a diferença entre eles é a escolha de `HashAggregate` e dos `Exchange`, que são operadores físicos.

Veredito: a prosa da fase 2 mistura as duas fases. A figura está certa.

**10.** Construa o argumento de que a alegação central deste capítulo, a de que estrutura permite otimização, é demonstrada uma única vez no capítulo inteiro. Onde, e o que falta em todas as outras demonstrações? *(The Catalyst Optimizer; Figures 3-4 and 3-5; Key Merits and Benefits)*

R: A demonstração única é o par de planos impressos do exemplo M&M, com o `explain(True)`. É o único lugar do capítulo em que se vê uma otimização acontecer: o nó `Project` presente no plano analisado desaparece no plano otimizado.

Todo o resto é asserção ou ilustração.

A Figure 3-5 é um desenho. Ela mostra um `filter` descendo e depois sumindo dentro de um scan, e é convincente, mas nenhum plano real acompanha. O código Scala do `usersDF`/`eventsDF` que a antecede tem os dois DataFrames como `...`, então não roda.

A comparação Python contra SQL afirma que os dois terminam em bytecode idêntico e não imprime o plano do SQL, só o do Python. A alegação de igualdade nunca é exibida.

O exemplo do corpo de bombeiros ocupa a maior parte do capítulo e não traz plano nenhum, nem tempo, nem contagem de stage.

A comparação RDD contra DataFrame no começo mostra dois códigos e nenhuma medição. O argumento ali é de legibilidade, não de performance, embora o parágrafo fale em performance.

O que falta em cada caso é o mesmo: um antes e um depois observáveis. O capítulo pede que se acredite que o Catalyst melhora a query, e mostra isso uma vez, em uma query de três operadores sobre um CSV pequeno.

**11.** Reconcilie as duas datas que o capítulo dá para o Spark SQL, e diga qual delas é consistente com a linha do tempo do DataFrame que ele também dá. *(Chapter introduction; Spark SQL and the Underlying Engine)*

R: As duas frases são "When Spark SQL was first introduced in the early Spark 1.x releases" e "Since its introduction in Spark 1.3, Spark SQL has evolved into a substantial engine".

Elas não são incompatíveis, porque 1.3 é um release 1.x, mas a segunda é precisa e a primeira é vaga. E a primeira frase continua assim: "followed by DataFrames as a successor to SchemaRDDs in Spark 1.3". Se o Spark SQL veio antes e o DataFrame veio no 1.3, então o Spark SQL é anterior ao 1.3, o que contradiz a segunda frase.

A leitura que salva as duas separa o componente da API. O Spark SQL como componente apareceu antes do 1.3, com o SchemaRDD. O DataFrame, que é a API pela qual o componente ficou conhecido, apareceu no 1.3. A segunda frase data o componente pela API dele.

A pista que resolve está na própria frase: SchemaRDD é o nome anterior, então existia algo estruturado antes do 1.3. *No item 2 das Discordâncias do Nível 6 verifiquei as datas contra fonte primária, porque o outro livro dá uma terceira.*

**12.** O capítulo apresenta a lista de DataFrames Versus Datasets como auxílio de decisão. Transforme-a em uma tabela de decisão e identifique o que a lista de fato decide. *(DataFrames Versus Datasets; Figure 3-2)*

R:

| Critério da lista | DataFrame | Dataset | Decide de fato? |
|---|---|---|---|
| dizer o quê e não como | sim | sim | não |
| semântica rica, DSL, alto nível | sim | sim | não |
| type safety estrita em compile time | não | sim | **sim** |
| expressões, filtros, maps, agregações, SQL | sim | sim | não |
| transformações relacionais tipo SQL | sim | — | **sim** |
| serialização do Tungsten com Encoders | — | sim | **sim** |
| unificação e simplificação entre componentes | sim | — | **sim** |
| usuário de R | sim | — | **sim** |
| usuário de Python | sim | — | **sim** |
| eficiência de espaço e velocidade | sim | — | **sim** |
| erros pegos na compilação | ver Figure 3-2 | ver Figure 3-2 | remete |

Onze critérios, e três deles dizem "DataFrames or Datasets", ou seja, não decidem nada. Um quarto não decide de outro jeito: ele remete à Figure 3-2 em vez de nomear uma API. Eles servem para separar as duas Structured APIs do RDD, e não uma da outra.

Dos sete que decidem, dois são de linguagem e resolvem o caso antes de qualquer preferência técnica. Se você escreve em Python ou R, a lista inteira colapsa em uma linha e as outras dez são decorativas.

Sobram cinco critérios reais para quem escreve Scala ou Java, e dois deles são a contradição do item 8.

O que a lista de fato decide, resumido em duas frases: se a sua linguagem não é Scala nem Java, use DataFrame. Se é, use Dataset quando quiser verificação em tempo de compilação sobre objetos de domínio, e DataFrame no resto.

**13.** A Figure 3-2 mostra syntax errors pegos em compile time para DataFrames. Teste essa célula contra os próprios exemplos em Python do capítulo. *(Figure 3-2; Example 3-6)*

R: A célula não vale para Python, e o capítulo escreve DataFrames em Python o tempo todo.

O que a figura chama de syntax error é erro na forma do código, e ela diz que ele é pego em compile time nas colunas de DataFrames e de Datasets. Isso pressupõe uma etapa de compilação. Python não tem, então um erro de sintaxe em PySpark é pego pelo interpretador ao carregar o módulo, e não por um compilador.

Dá para defender a célula dizendo que o interpretador reporta o erro antes de qualquer execução Spark, o que é o mesmo efeito prático. Mas então a distinção entre a coluna de SQL e a de DataFrames muda de natureza. A query SQL é uma string, e uma string malformada é sintaticamente válida para o Python. É por isso que ela cai em runtime.

Formulação que sobrevive: a diferença entre as colunas não é ter ou não compilador, é a query ser código na linguagem hospedeira ou ser texto passado como dado.

O capítulo não faz essa distinção, e a figura, que veio do mundo Scala, é aplicada sem ressalva a um capítulo majoritariamente Python.

**14.** Comprima o argumento do capítulo em uma frase por seção principal, de modo que remover qualquer uma quebre a cadeia. *(chapter-wide)*

R:

1. *(Spark: What's Underneath an RDD?)* O RDD é dependencies, partitions e uma compute function, e essa compute function é opaca para o Spark, que por isso não consegue nem otimizar a expressão nem comprimir o dado.
2. *(Structuring Spark)* O Spark 2.x responde a essa opacidade impondo estrutura em três frentes: padrões comuns como operações de alto nível, operadores em uma DSL, e dado em formato tabular com tipos suportados.
3. *(Key Merits and Benefits)* Com estrutura, a mesma query cai de três lambdas para uma linha declarativa, e o motor passa a enxergar a intenção, o que compra expressividade, simplicidade, composabilidade e uniformidade entre linguagens e componentes.
4. *(The DataFrame API)* A forma concreta dessa estrutura é o DataFrame, uma tabela distribuída de colunas nomeadas cujos tipos e nomes são declarados em um schema, e é o schema que dispensa a inferência e permite detectar erro na leitura.
5. *(The Dataset API)* Sobre a mesma base existe o Dataset, que troca o `Row` genérico por objetos de domínio tipados, o que move a detecção de erros de nome de runtime para compile time, ao custo de existir só em Scala e Java.
6. *(When to Use RDDs)* O RDD não é depreciado e segue por baixo dos dois, porque DataFrames e Datasets são construídos sobre ele e decompostos em código RDD compacto.
7. *(Spark SQL and the Underlying Engine)* Tudo isso é possível porque um único engine sustenta as três entradas, e é ele que conecta metastore, formatos e conectores externos.
8. *(The Catalyst Optimizer)* Dentro desse engine, o Catalyst leva qualquer das entradas por quatro fases, analysis, logical optimization, physical planning e code generation, e é a última que devolve o resultado ao RDD, agora como código gerado.

Remover qualquer uma quebra a cadeia. Sem a 1 não há problema. Sem a 2 não há resposta. Sem a 3 a resposta não tem benefício declarado. Sem a 4 e a 5 a resposta não tem forma concreta. Sem a 6 o capítulo parece dizer que o RDD sumiu, e a fase 4 fica sem destino. Sem a 7 os componentes não se unificam. Sem a 8 a promessa de otimização fica sem mecanismo, e o capítulo termina em uma lista de APIs.

---

## Nível 5 — Além do capítulo (backlog, não notas)

O capítulo foi escrito em 2020, contra o Spark 3.0 em preview. Ele descreve o motor estrutural do Spark, que é justamente a parte que mais mudou desde então. Verifiquei todos os itens abaixo contra fonte primária em **3 de agosto de 2026**, quando a versão corrente da documentação era a **4.2.0**. As URLs estão ao fim da seção.

**1.** **Dataset em Python.** O capítulo diz que Datasets fazem sentido só em Java e Scala. Verifique se isso continua verdade em 2026 e como a documentação atual formula a restrição.

R: Continua verdade, e a documentação é mais direta que o livro. O Spark SQL, DataFrames and Datasets Guide diz que a Dataset API está disponível em Scala e Java, e que **"Python does not have the support for the Dataset API"**. Ele acrescenta que, pela natureza dinâmica do Python, muitos benefícios já estão disponíveis, porque dá para acessar campos de uma linha por nome com `row.columnName`. Para R a situação é descrita como semelhante.

Sobre a relação entre as duas abstrações, a mesma página diz: "In Scala and Java, a DataFrame is represented by a Dataset of `Row`s. In the Scala API, `DataFrame` is simply a type alias of `Dataset[Row]`. While, in Java API, users need to use `Dataset<Row>` to represent a `DataFrame`." Isso confirma a Table 3-6 do capítulo linha por linha.

A página também data o Dataset: "a new interface added in Spark 1.6". Guarde esse número, porque ele resolve a discordância 2 do Nível 6.

Efeito direto sobre mim: escrevo em Python, então o capítulo inteiro sobre Dataset API, Creating Datasets, Dataset Operations e End-to-End Dataset Example é leitura de contexto, e não de uso. Metade da lista de DataFrames Versus Datasets já vem decidida pela linguagem.

**2.** **Futuro do RDD.** O capítulo responde "a resounding no" à pergunta sobre deprecação. Verifique se essa resposta se sustenta seis anos depois.

R: Sustenta. O RDD Programming Guide continua publicado na documentação 4.2.0 e **não traz aviso de deprecação, de legado ou de modo de manutenção**. Ele define RDD como "a fault-tolerant collection of elements that can be operated on in parallel" e descreve transformations, actions, laziness, persistência e shuffle como conteúdo corrente.

O contraste com o guia de Spark Streaming é o que dá a medida. Aquele abre com aviso explícito de geração anterior e de projeto legado. O do RDD não tem nada disso.

Duas coisas mudaram em volta, e nenhuma é deprecação. A primeira é que o `SparkR` foi depreciado no Spark 4.0.0, via SPARK-49347, o que altera a linha de R da Table 3-6 do capítulo. A segunda é que o esforço de API novo foi todo para o lado estruturado, com Python Data Source API (SPARK-44076) e SQL Pipe syntax (SPARK-49555) no 4.0.0.

Leitura para a nota: o capítulo acertou o prognóstico. RDD não morreu e não recebeu nada.

**3.** **A alegação ANSI SQL:2003.** O capítulo diz que o Spark SQL aceita queries "ANSI SQL:2003–compatible". Verifique o que a documentação diz hoje.

R: A frase não tem equivalente na documentação atual, e eu quase errei o motivo.

O que a documentação atual faz é não repetir a alegação. A página ANSI Compliance **não menciona SQL:2003 em lugar nenhum**, não oferece lista de conformidade por feature, e se resguarda quanto ao rótulo: "Some ANSI dialect features may be not from the ANSI SQL standard directly, but their behaviors align with ANSI SQL's style." A tabela de keywords da mesma página referencia SQL-2016, não SQL:2003. Ou seja, a alegação do capítulo não foi refutada, foi abandonada.

**Aqui eu ia dizer que conformidade é um modo de operação, controlado por `spark.sql.ansi.enabled`, e que em 2020 esse modo estava desligado. Fui conferir e a ligação não existe.** O `ANSI_ENABLED` no `SQLConf.scala` governa semântica de runtime, e o doc string dele é explícito: *"Spark will throw an exception at runtime instead of returning null results when the inputs to a SQL operator/function are invalid."* A página organiza o flag em Arithmetic Operations, Cast, Rounding in cast, Store assignment, Type coercion, SQL Functions e SQL Operators. Todas são semântica de execução. A única chave que toca o parser é `spark.sql.ansi.enforceReservedKeywords`, que é separada e continua com default `false`.

São eixos ortogonais. "Aceitar queries ANSI SQL:2003-compatible" é afirmação sobre **cobertura de gramática**: quais comandos o parser entende. O `spark.sql.ansi.enabled` é afirmação sobre **o que acontece quando um cast falha**. O default de 2020 não diz nada sobre a alegação do capítulo.

O que sobra contra o capítulo é menor. A frase não é falsa, é infalsificável: ele não diz qual subconjunto do padrão está coberto, então ninguém consegue conferir. Imprecisa, não errada.

Sobre o flag, que é assunto separado e vale registrar: o default virou `true` no Spark 4.0.0, via **SPARK-44444, "Use ANSI SQL mode by default"**, e continua `true` na 4.2.0. Existe ainda o `spark.sql.storeAssignmentPolicy`, com default `ANSI`, que governa o casting implícito na inserção em tabela. Consequência prática: código escrito contra Spark 3.x pode passar a lançar exceção no 4.x, onde antes devolvia `null` ou um número circulado.

**4.** **O que mudou no Catalyst desde 2020.** As quatro fases ainda descrevem o que acontece? Verifique o que foi acrescentado.

R: As quatro fases continuam sendo o esqueleto, e ganharam um quinto ator que o capítulo não podia conhecer: a otimização em tempo de execução.

O Performance Tuning guide descreve **Adaptive Query Execution** como "an optimization technique in Spark SQL that makes use of the runtime statistics to choose the most efficient query execution plan", e registra que ela está **habilitada por padrão desde o Apache Spark 3.2.0**. Os dois sub-recursos principais vêm ligados: `spark.sql.adaptive.coalescePartitions.enabled` com default `true`, que junta partições de shuffle contíguas para evitar tasks pequenas demais, e `spark.sql.adaptive.skewJoin.enabled` com default `true`, que trata skew em sort-merge join dividindo e replicando partições enviesadas.

A mesma página reorganiza a questão das estatísticas em três origens, o que é uma resposta melhor que a do capítulo: **data source**, como contagens e min/max no metadado do Parquet, **catalog**, coletadas com `ANALYZE TABLE`, e **runtime**, computadas pelo próprio Spark durante a query, que é a parte do AQE. A página de `ANALYZE TABLE` confirma o papel: as estatísticas são coletadas "to be used by the query optimizer to find a better query execution plan".

Uma peça nova de otimização estática também entrou: o **runtime bloom filter**, cujo doc string diz que, quando um lado de um shuffle join tem um predicado seletivo, o Spark tenta inserir um bloom filter no outro lado para reduzir o dado do shuffle. Ele foi **ligado por padrão no Spark 3.4.0, via SPARK-38841**.

As páginas de documentação que li não trazem o default de `spark.sql.cbo.enabled`, e a página de configuração corrente vem truncada antes da tabela de SQL. Fui ao código-fonte, que é a fonte definitiva: em [`SQLConf.scala`, tag v4.0.0](https://raw.githubusercontent.com/apache/spark/v4.0.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala) a chave é declarada com `.createWithDefault(false)`, desde a versão 2.2.0. **O CBO está desligado por padrão.** Isso confirma o que a prosa da documentação já sugeria: ela não apresenta o CBO como caminho automático, e sim como algo que depende de estatísticas de catálogo coletadas à mão.

**5.** **Os 200 do plano físico.** O plano impresso no capítulo mostra `Exchange rangepartitioning(..., 200)` e `hashpartitioning(..., 200)`. Descubra de onde vem esse número e se um plano impresso hoje seria igual.

R: O número vem de `spark.sql.shuffle.partitions`, cujo default é **200** desde a versão **1.1.0**, segundo a tabela do Performance Tuning guide. Ele não mudou.

O que mudou é o que acontece com ele. Com o AQE ligado por padrão desde o 3.2.0 e `coalescePartitions` ligado, o 200 deixou de ser o número final e virou o ponto de partida. O Spark reduz esse número em runtime, com base no tamanho real dos dados de shuffle e no alvo definido por `spark.sql.adaptive.advisoryPartitionSizeInBytes`.

Efeito prático sobre a leitura do capítulo: um `explain(True)` rodado hoje sobre a mesma query M&M não produz a mesma saída. O plano físico passa a ser embrulhado por um nó de AQE, e os números de partição impressos antes da execução deixam de ser os efetivos. O capítulo ensina a ler um plano estático em um motor que hoje replaneja enquanto roda.

Isso também aposenta parte do conselho de tuning que o capítulo adia para o Capítulo 7.

**6.** **Tungsten e whole-stage code generation.** Verifique se a técnica descrita continua a mesma e onde ela é visível hoje.

R: A técnica continua e o nome do guarda-chuva sumiu.

A origem que o capítulo cita bate. As notas do **Spark 2.0.0** anunciam "substantial (2 - 10X) performance speedups for common operators in SQL and DataFrames via a new technique called whole stage code generation", que é exatamente o Tungsten de segunda geração descrito no capítulo.

O que existe hoje na documentação é o artefato, não o nome do projeto. A página de Web UI descreve a visualização do DAG e diz que os nós são agrupados por escopo de operação e rotulados com nomes como `BatchScan`, `WholeStageCodegen` e `Exchange`, e que **as operações de whole-stage code generation são anotadas com um codegen id**. Ou seja, a forma de ver a técnica funcionando é a UI, e o rótulo é `WholeStageCodegen`.

"Project Tungsten" como nome de iniciativa não aparece nas páginas correntes que li. Ele sobrevive na Figure 3-3 do livro e nas notas de release antigas.

Não achei o default de `spark.sql.codegen.wholeStage` em nenhuma página de documentação que eu tenha lido inteira, então fui ao código-fonte. Em [`SQLConf.scala`, tag v4.0.0](https://raw.githubusercontent.com/apache/spark/v4.0.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala) a chave é declarada com `.createWithDefault(true)`. **A técnica está ativa por padrão.** A presença do rótulo na UI como caso comum era evidência indireta disso, e agora tem prova.

**7.** **Encoders.** O capítulo cita encoders duas vezes e adia o mecanismo para o Capítulo 6. Descubra o que eles fazem de fato.

R: A documentação entrega o mecanismo em duas frases, e ele é mais específico do que "gerenciamento de memória fora do heap".

O Spark SQL Getting Started diz: "Datasets are similar to RDDs, however, instead of using Java serialization or Kryo they use a specialized **Encoder** to serialize the objects for processing or transmitting over the network. While both encoders and standard serialization are responsible for turning an object into bytes, **encoders are code generated dynamically and use a format that allows Spark to perform many operations like filtering, sorting and hashing without deserializing the bytes back into an object**."

A segunda metade é o ponto que o capítulo não faz. O ganho do encoder não é só serializar mais apertado. É produzir um formato sobre o qual o Spark opera direto, sem reconstruir o objeto. Filtrar, ordenar e fazer hash acontecem sobre bytes.

Isso também explica a tensão do item 8 do Nível 4. O encoder é eficiente na representação. A conversão para o objeto de domínio, que acontece quando você escreve uma lambda que acessa `d.temp`, é o que custa. As duas afirmações do capítulo medem lados opostos da mesma fronteira.

**8.** **Os tipos de dado.** O capítulo afirma que os tipos Scala são subtipos de `DataTypes`, exceto `DecimalType`, e escreve `DataTypes.ByteType` como forma de instanciar em Python. Verifique as duas coisas e levante os tipos que apareceram desde 2020.

R: As duas afirmações estão erradas.

Sobre a hierarquia: a JavaDoc de `org.apache.spark.sql.types.DataTypes` declara `public class DataTypes extends Object` e explica que **"To get/create specific data type, users should use singleton objects and factory methods provided by this class"**. É uma classe de fábrica, com campos estáticos como `static final DataType StringType` e métodos como `createArrayType`. Ela não é superclasse de nada. A superclasse real dos tipos é `DataType`, e `DecimalType` também é um `DataType`. A exceção que o capítulo abre para o `DecimalType` existe porque ele é o único que não tem um singleton pronto na fábrica, e não porque ele fique fora da hierarquia.

Sobre Python: a referência de tipos do PySpark 4.2.0 lista as classes em `pyspark.sql.types` e todas são instanciadas com parênteses, `ByteType()`, `StringType()`, `ArrayType()`. **Não existe classe `DataTypes` em PySpark.** A página SQL Data Types confirma o caminho de import: `from pyspark.sql.types import *` em Python e `import org.apache.spark.sql.types._` em Scala. Ou seja, a Table 3-5 do capítulo está certa e a Table 3-3 está errada, como deduzi no item 2 do Nível 4.

Tipos que não existiam quando o capítulo foi escrito e hoje aparecem na referência: `TimestampNTZType`, `TimeType`, `YearMonthIntervalType`, `DayTimeIntervalType`, `CharType`, `VarcharType`, `VariantType`, `GeometryType` e `GeographyType`. O `VARIANT` entrou pelo **SPARK-45827, no Spark 4.0.0**, e serve para dado semiestruturado.

**9.** **`samplingRatio` no CSV.** O capítulo oferece `samplingRatio` como alternativa barata a declarar schema, em uma leitura de CSV. Verifique o que a opção faz e se o exemplo funciona.

R: A opção existe e o exemplo não faz o que promete.

A página CSV Files documenta `samplingRatio` com default **1.0** e a descrição **"Defines fraction of rows used for schema inferring"**. Ela também registra que as funções embutidas de CSV ignoram a opção.

O problema do exemplo é a companhia. O snippet do capítulo passa `.option("samplingRatio", 0.001)` e `.option("header", true)`, e **não passa `inferSchema`**. O default de `inferSchema` no CSV é **false**. Sem inferência ligada, não há inferência a amostrar, e o Spark trata todas as colunas como string. A opção fica inerte.

Os outros defaults do CSV que a página confirma, e que o capítulo nunca dá: `header` é `false`, `sep` é `,`, `escape` é `\` e `mode` é `PERMISSIVE`.

Detalhe que fecha o caso: o próprio código Python do capítulo, algumas linhas depois, escreve `spark.read.csv(sf_fire_file, header=True, schema=fire_schema)`. Ele declara o schema, que é a recomendação correta. A NOTE é que está isolada e incorreta.

**10.** **Parquet como padrão.** O capítulo diz que Parquet é o formato padrão do `DataFrameWriter`. Verifique o escopo real dessa afirmação.

R: O escopo é maior do que o capítulo diz. A página Generic Load/Save Functions afirma: **"In the simplest form, the default data source (`parquet` unless otherwise configured by `spark.sql.sources.default`) will be used for all operations."**

"All operations" cobre leitura e escrita. Então `spark.read.load(path)` sem `format` também lê Parquet, e não só `write.save(path)` escreve Parquet. O capítulo enuncia a metade de escrita e nunca mostra a de leitura, o que é uma perda, porque a leitura sem `format` é a forma mais curta de carregar Parquet.

A afirmação sobre o schema preservado no metadado do Parquet é consistente com o resto da documentação e com o comportamento de um formato autodescritivo.

Sobre a compressão: não achei o default de `spark.sql.parquet.compression.codec` em nenhuma página de documentação que eu tenha lido inteira, então fui ao código-fonte. Em [`SQLConf.scala`, tag v4.0.0](https://raw.githubusercontent.com/apache/spark/v4.0.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala) a chave é declarada com `.createWithDefault("snappy")`. **Snappy continua sendo o padrão.**

**11.** **A sintaxe `'coluna` em Scala.** O capítulo usa `countDistinct('CallType) as 'DistinctCallTypes` sem nunca explicar o apóstrofo. Descubra o que é e se ainda se usa.

R: É um symbol literal de Scala, e o Spark tinha uma conversão implícita de `Symbol` para `Column`, o que fazia `'CallType` funcionar como referência de coluna.

A sintaxe está morrendo. O **symbol literal foi depreciado no Scala 2.13** e **removido no Scala 3**, segundo o guia de migração de features descartadas da linguagem. A classe `scala.Symbol` continua existindo, só a forma com apóstrofo caiu.

O próprio projeto Spark já se moveu. O **SPARK-29392** se chama "Remove use of deprecated symbol literal ' 'name ' syntax in favor Symbol("name")", e o **SPARK-35151** trata de suprimir os avisos de compilação "symbol literal is deprecated" no Scala 2.13.

O que isso significa para o capítulo: desde que o **Spark 4.0.0 tornou o Scala 2.13 o padrão e removeu o 2.12 (SPARK-45314)**, compilar o snippet do corpo de bombeiros gera aviso de deprecação. Em Scala 3 ele não compila.

As substituições, que o capítulo também usa em outros pontos: `col("CallType")`, `$"CallType"` ou a string `"CallType"`.

### Fontes consultadas

Todas acessadas em 3 de agosto de 2026. Todas são fonte primária: documentação oficial, notas de release, o rastreador de issues do projeto ou a documentação da linguagem.

Documentação do Apache Spark, versão 4.2.0:

- [Spark SQL, DataFrames and Datasets Guide, com a ausência da Dataset API em Python](https://spark.apache.org/docs/4.2.0/sql-programming-guide.html)
- [Spark SQL Getting Started, com a descrição dos Encoders](https://spark.apache.org/docs/4.2.0/sql-getting-started.html)
- [ANSI Compliance, com o default de `spark.sql.ansi.enabled` e a ressalva sobre o padrão](https://spark.apache.org/docs/4.2.0/sql-ref-ansi-compliance.html)
- [Performance Tuning, com AQE, as três origens de estatística e o default de `spark.sql.shuffle.partitions`](https://spark.apache.org/docs/4.2.0/sql-performance-tuning.html)
- [ANALYZE TABLE, com o papel das estatísticas para o query optimizer](https://spark.apache.org/docs/4.2.0/sql-ref-syntax-aux-analyze-table.html)
- [Data Types, com a lista corrente de tipos e os imports de Scala e Python](https://spark.apache.org/docs/4.2.0/sql-ref-datatypes.html)
- [Referência de tipos do PySpark, com a instanciação por parênteses](https://spark.apache.org/docs/4.2.0/api/python/reference/pyspark.sql/data_types.html)
- [CSV Files, com `samplingRatio`, `inferSchema`, `header`, `sep`, `escape` e `mode`](https://spark.apache.org/docs/4.2.0/sql-data-sources-csv.html)
- [Generic Load/Save Functions, com o default `parquet` de `spark.sql.sources.default`](https://spark.apache.org/docs/4.2.0/sql-data-sources-load-save-functions.html)
- [RDD Programming Guide, sem aviso de deprecação](https://spark.apache.org/docs/4.2.0/rdd-programming-guide.html)
- [Web UI, com o rótulo `WholeStageCodegen` e o codegen id no DAG](https://spark.apache.org/docs/4.2.0/web-ui.html)
- [JavaDoc de `DataTypes`, declarada como `extends Object` e classe de fábrica](https://spark.apache.org/docs/4.2.0/api/java/org/apache/spark/sql/types/DataTypes.html)
- [ScalaDoc de `RDD`, com as cinco propriedades internas](https://spark.apache.org/docs/4.2.0/api/scala/org/apache/spark/rdd/RDD.html)

Notas de release e tickets:

- [Spark Release 2.0.0, com o anúncio de whole stage code generation](https://spark.apache.org/releases/spark-release-2-0-0.html)
- [Spark Release 3.4.0, com bloom filter joins por padrão](https://spark.apache.org/releases/spark-release-3-4-0.html)
- [Spark Release 4.0.0, com ANSI por padrão, VARIANT, Scala 2.13 e a depreciação do SparkR](https://spark.apache.org/releases/spark-release-4-0-0.html)
- [SPARK-29392, remoção do uso de symbol literal](https://issues.apache.org/jira/browse/SPARK-29392)
- [SPARK-35151, supressão dos avisos de symbol literal no Scala 2.13](https://issues.apache.org/jira/browse/SPARK-35151)

Documentação da linguagem:

- [Scala 3 Migration Guide, Dropped Features, com o symbol literal](https://docs.scala-lang.org/scala3/guides/migration/incompat-dropped-features.html)

Código-fonte:

- [`SQLConf.scala`, tag v4.0.0, com os defaults de `cbo.enabled`, `parquet.compression.codec`, `codegen.wholeStage`, `ansi.enabled` e `ansi.enforceReservedKeywords`](https://raw.githubusercontent.com/apache/spark/v4.0.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala)

Três defaults não estavam em nenhuma página de documentação que eu tenha lido inteira, porque a página de configuração corrente vem truncada antes da tabela de SQL. Resolvi os três no código-fonte, que é a fonte definitiva para valor de configuração: `spark.sql.cbo.enabled` é `false`, `spark.sql.parquet.compression.codec` é `snappy`, e `spark.sql.codegen.wholeStage` é `true`. Todos lidos em [`SQLConf.scala`, tag v4.0.0](https://raw.githubusercontent.com/apache/spark/v4.0.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala), acessada em 3 de agosto de 2026. **A lição de método aqui vale mais que os três valores:** para default de configuração, o `SQLConf.scala` responde onde a documentação renderizada falha.

---

## Nível 6 — Cross-book (contra *Beginning Apache Spark 3*, Capítulo 3)

Dois capítulos sobre a mesma fundação, escritos com um ano de diferença. O Damji chama o assunto de "Apache Spark's Structured APIs" e o Luu chama de "Spark SQL: Foundation". O recorte diferente já é metade da comparação.

**1.** **A anatomia do RDD.** Damji et al. listam três características vitais. O Luu lista cinco. Mapeie uma lista na outra e diga o que a lista curta esconde. *(Spark: What's Underneath an RDD? / Understanding RDD)*

R: O mapeamento:

| Luu, cinco | Damji, três |
|---|---|
| dependencies em RDDs pai | dependencies |
| conjunto de partitions | partitions |
| função para computar todas as linhas | compute function `Partition => Iterator[T]` |
| metadado sobre o partitioning scheme (opcional) | *ausente* |
| localização do dado no cluster (opcional) | dobrado dentro de "partitions (with some locality information)" |

O que a lista curta esconde são duas coisas diferentes. A locality não some, ela vira um parêntese dentro de partitions, o que é compressão e não omissão. O partitioner some inteiro, e essa é a omissão de verdade.

O partitioner importa porque é ele que determina se um shuffle é necessário. Dois RDDs com o mesmo partitioner podem ser juntados sem redistribuir dado. Sem esse conceito, não há como explicar por que algumas operações embaralham o cluster e outras não.

O Luu também organiza melhor o que sobra. Ele diz que as três primeiras informações formam a lineage, e que o Spark a usa para duas coisas, determinar a ordem de execução e recuperar de falha. O Damji atribui resiliência só às dependencies e não usa a palavra lineage nesta seção.

**2.** **A definição de DataFrame.** O Damji diz "distributed in-memory tables with named columns and schemas". O Luu diz "an immutable, distributed collection of data organized into rows". Qual definição sobrevive ao resto de cada capítulo? *(The DataFrame API / Introduction to the DataFrame API)*

R: A do Luu, e por três razões.

A primeira é "in-memory". Já argumentei no item 1 do Nível 4 que o próprio capítulo do Damji desmente isso, com a NOTE sobre caching e com a laziness que ele herda do Capítulo 2. O Luu não faz a afirmação e ainda dedica uma seção inteira, DataFrame Persistence, a explicar que colocar um DataFrame em memória é uma decisão com API própria, `persist` e `unpersist`.

A segunda é "immutable". O Luu põe a imutabilidade na definição. O Damji a menciona duas vezes, uma no corpo e uma em NOTE, sempre como observação lateral.

A terceira é a ordem de composição. O Luu diz que a coleção é organizada em linhas, e que cada linha tem colunas com nome e tipo. Isso constrói a partir da unidade que o motor de fato manipula, o `Row`. O Damji parte da tabela, que é a metáfora.

Uma coisa o Damji tem e o Luu não: a origem. Ele registra que o DataFrame do Spark foi inspirado no do pandas em estrutura, formato e algumas operações. O Luu diz que o conceito veio do pandas e acrescenta a diferença que importa, que o do Spark lida com volume grande espalhado por muitas máquinas.

**3.** **Por que estrutura.** Os dois capítulos abrem com o mesmo argumento, o de que a compute function do RDD é opaca. Compare como cada um o constrói. *(Spark: What's Underneath an RDD? / Understanding RDD)*

R: O Damji constrói o argumento em três degraus e o Luu em um.

O Damji separa duas opacidades. A da computação, em que o Spark só vê uma lambda e não sabe se ela faz join, filter, select ou agregação. E a do tipo, em que o `Iterator[T]` é genérico e o Spark não sabe se você acessa uma coluna de certo tipo dentro do objeto. Da segunda ele tira uma consequência que o Luu não tira: sem conhecer o tipo, tudo que o Spark pode fazer é serializar o objeto como bytes, sem compressão. Isso liga a opacidade à eficiência de espaço, e não só à de plano.

O Luu concentra tudo em uma frase sobre flexibilidade com contrapartida, e depois dá exemplos melhores do que se perde: predicate pushdown para reduzir o dado lido, recomendação de um tipo de join mais eficiente, e pruning de colunas que a saída não usa. Os três são otimizações nomeadas, e o Damji só as nomeia muito depois, na fase 2 do Catalyst.

Saldo: o Damji tem a taxonomia da causa e o Luu tem a lista do efeito. Juntando os dois, o argumento fica completo. Cada um sozinho tem metade.

**4.** **Ler um arquivo.** O Damji mostra uma leitura de CSV com schema declarado e uma leitura de JSON. O Luu cobre text, CSV, JSON, Parquet, ORC e JDBC com tabelas de opções. Qual capítulo de fato te prepara para carregar dado? *(Using DataFrameReader and DataFrameWriter / Creating a DataFrame from Data Sources)*

R: O Luu, sem disputa, e a diferença é de gênero. O capítulo dele é um manual de referência e o do Damji é um argumento com exemplos.

O que o Luu tem e o Damji não: o padrão geral de interação, `spark.read.format(...).option(...).schema(...).load()`. Uma tabela dizendo quais das três peças são opcionais. Uma tabela com as seis fontes embutidas e o formato de cada uma. Defaults de CSV para `sep`, `header`, `escape` e `inferSchema`. Defaults de JSON para `allowComments`, `multiLine` e `samplingRatio`. O modo `failFast` e o que acontece sem ele, que é o Spark colocar `null` em todas as colunas da linha corrompida. E a seção de JDBC inteira, com driver, URL, `dbtable` e o aviso sobre o classpath.

O que o Damji tem e o Luu não é o argumento de por que declarar schema, com as três razões, e a única delas que é mecânica: evitar que o Spark crie um job separado só para ler uma porção grande do arquivo.

Há um ponto em que o Damji falha e o Luu não erraria. A NOTE do `samplingRatio` do Damji passa a opção sem `inferSchema`, e o item 9 do Nível 5 mostra que ela fica inerte. O Luu documenta `inferSchema` como opção separada com default `false`, então quem leu a tabela dele não cai nessa.

Para uso, o Luu. Para entender por que a escolha importa, o Damji.

**5.** **Referenciar uma coluna.** O Damji mostra `col`, `expr`, `$` e a string nua, e usa uma quinta forma sem explicá-la. O Luu tabula cinco. Qual tratamento é completo? *(Columns and Expressions / Working with Columns)*

R: O Luu, e o Damji tem uma dívida concreta com o leitor.

A tabela do Luu, Table 3-8, traz cinco formas: a string `"columnName"`, `col("columnName")`, `column("columnName")`, `$"columnName"` e `'columnName`. Cada uma com uma linha de descrição. Ele marca `$` como açúcar sintático só de Scala e `'` como açúcar que usa symbol literals de Scala. Ele ainda dá recomendação prática: quem alterna entre Scala e Python usa `col`, por consistência, e quem fica só em Scala pode usar o apóstrofo, porque é um caractere só. E menciona a `col` do próprio DataFrame, que desambigua colunas de mesmo nome em um join.

O Damji cobre a string, `col`, `expr` e `$`, e explica o `$` corretamente como função que converte o nome em `Column`. A dívida é o apóstrofo. Ele escreve `countDistinct('CallType) as 'DistinctCallTypes` em um snippet Scala e nunca diz o que aquilo é. Um leitor que só tem o capítulo dele encontra um caractere que não aparece em lugar nenhum da explicação.

O Damji tem uma peça que o Luu não tem, e é boa: a NOTE que separa `Column`, o objeto, de `col()`, a função que devolve um `Column`. Essa distinção é a que dissolve a confusão de nomenclatura, e o Luu não a faz.

Ironia que o Nível 5 fecha: a forma que o Luu recomenda para quem escreve só Scala é justamente a que o Scala depreciou e o Scala 3 removeu.

**6.** **Quando usar Dataset.** O Damji dá uma lista de onze bullets mais a Figure 3-2. O Luu dá duas propriedades e uma frase de orientação. Qual dos dois te deixa em condição de decidir? *(DataFrames Versus Datasets / Introduction to Datasets)*

R: O Luu, e é o caso mais claro de menos texto decidir mais.

A orientação dele cabe em duas frases: as Dataset APIs são boas para jobs de produção que rodam com regularidade e são mantidos por um time de engenheiros de dados, e para a maioria dos casos de análise interativa e exploratória o DataFrame basta. Isso é um critério de contexto de uso, e dá para aplicar sem consultar tabela.

Ele sustenta esse critério com as duas propriedades que criam a diferença. Cada linha do Dataset é um objeto definido pelo usuário, e uma coluna passa a ser uma variável membro desse objeto, o que dá segurança em tempo de compilação. E o Dataset tem encoders, que convertem cada objeto em formato binário compacto, o que reduz memória no cache e bytes no shuffle. Depois ele nomeia o custo, que é a conversão de `Row` para objeto de domínio, e diz que esse custo pesa quando o Dataset tem milhões de linhas.

A lista do Damji tem onze itens e três deles dizem "DataFrames or Datasets", ou seja, não decidem. Um quarto remete à Figure 3-2, o que também não decide. Dois são de linguagem. Dois se contradizem, como mostrei no item 8 do Nível 4. Sobram três critérios úteis, espalhados em uma lista que parece exaustiva e não é.

O que o Damji tem de melhor é a Figure 3-2, que transforma a diferença em uma matriz de duas linhas e três colunas. É a peça mais compacta dos dois livros sobre o assunto, e o Luu tem a mesma matriz na Table 3-12, com um rótulo pior.

**7.** **O lado da escrita e o catálogo.** O Damji mostra duas linhas de escrita e adia tabelas para o capítulo seguinte. O Luu cobre save modes, partitionBy, bucketBy, coalesce, temp views e `spark.catalog`. O que a omissão custa ao leitor do Damji? *(Saving a DataFrame as a Parquet file or SQL table / Writing Data Out to Storage Systems; Using SQL in Spark SQL)*

R: Custa a metade final de qualquer pipeline.

O Damji entrega duas linhas, `write.format("parquet").save(path)` e `write.format("parquet").saveAsTable(name)`, mais a informação de que Parquet é o padrão e que `saveAsTable` registra metadado no Hive metastore.

O Luu entrega o resto. O padrão de interação com o `DataFrameWriter`, com `format`, `mode`, `option`, `partitionBy`, `bucketBy`, `sortBy` e `save`. A tabela de save modes, com `append`, `overwrite`, `errorIfExists` como padrão e `ignore`. O fato de o argumento de `save` ser um diretório e não um arquivo. A relação entre número de partições do DataFrame e número de arquivos de saída, com `movies.rdd.getNumPartitions` para descobrir e `coalesce(1)` para reduzir a um. E o `partitionBy` gerando diretórios no formato `produced_year=1961`, com a regra de escolher coluna de baixa cardinalidade.

Do lado de SQL a lacuna é igual. O Luu mostra `createOrReplaceTempView`, `createOrReplaceGlobalTempView` e a diferença de escopo entre sessão e global, o prefixo `global_temp.` obrigatório, e o `spark.catalog.listTables` e `listColumns` para inspecionar o que está registrado. Ele também mostra misturar SQL e transformações na mesma cadeia, e consultar um arquivo direto com `SELECT * FROM parquet.<caminho>`.

O detalhe que expõe a lacuna do Damji: o `Catalog` aparece no capítulo dele uma vez, dentro da fase 1 do Catalyst, como estrutura interna consultada para resolver nomes. O leitor sai sabendo que existe um catálogo e sem saber que dá para falar com ele. É o mesmo objeto, apresentado só pelo lado de dentro.

**8.** **O Catalyst.** O Damji gasta cinco páginas e duas figuras com o otimizador. O Luu o nomeia duas vezes e o adia para o Capítulo 4 dele. Dado que o capítulo do Luu é mais longo, o que essa alocação diz sobre cada livro? *(Spark SQL and the Underlying Engine / chapter opening)*

R: Diz que os dois capítulos têm público diferente, e o título de cada um já avisa.

"Spark SQL: Foundation" é um capítulo de fundação de uso. O Luu declara na abertura que o módulo Spark SQL tem duas partes, as representações das Structured APIs e o Catalyst optimizer, "responsible for all the complex machinery that works behind the scenes to make your life easier". Ele nomeia o componente, diz que ele existe para você não precisar pensar nele, e vai embora. Toda a extensão do capítulo dele vai para operações, opções e sintaxe.

"Apache Spark's Structured APIs" é um capítulo de tese. O argumento do Damji é que estrutura permite otimização, e um argumento assim precisa mostrar o otimizador ou fica sem prova. Por isso ele gasta as fases, os dois figures e o plano impresso.

A consequência prática é assimétrica de um jeito interessante. Quem lê só o Luu consegue escrever mais código no dia seguinte e não sabe por que o código é rápido. Quem lê só o Damji entende o mecanismo e não sabe escrever um `partitionBy`.

Uma peça que o Luu tem e é melhor que a equivalente do Damji, apesar do adiamento: ele explica predicate pushdown na seção de JDBC, em contexto, dizendo que o Spark empurra as condições de filtro para o RDBMS e que o Parquet tem a mesma capacidade. Isso é o conceito ancorado em uma fonte de dado concreta. O Damji nomeia predicate pushdown em uma lista de quatro otimizações e ilustra em um diagrama abstrato.

### Discordâncias

**1.** **Quantas partes tem um RDD.** Damji diz três, Luu diz cinco.

**O Luu está certo, e a fonte primária dá a lista dele quase palavra por palavra.** A ScalaDoc de `org.apache.spark.rdd.RDD` diz: "Internally, each RDD is characterized by five main properties", e lista uma lista de partitions, uma função para computar cada split, uma lista de dependencies em outros RDDs, opcionalmente um `Partitioner` para RDDs de par chave e valor, e opcionalmente uma lista de localizações preferidas para computar cada split.

O Luu cobre as cinco, incluindo as duas marcadas como opcionais, mas em outra ordem e com outra redação. Ele abre por dependencies, onde a ScalaDoc abre por partitions. O Damji reproduz três e dobra a locality dentro de partitions.

A parte que o Damji perde de fato é o `Partitioner`. Não é detalhe de implementação. A documentação fecha dizendo que todo o escalonamento e a execução no Spark são feitos com base nesses métodos.

Uma ressalva justa ao Damji: ele não erra nada do que diz. A compute function com assinatura `Partition => Iterator[T]` é mais precisa que "a function for computing all the rows in the dataset" do Luu. Ele é mais exato no que cobre e cobre menos.

**2.** **Quando as Structured APIs chegaram.** Damji diz que o Spark SQL veio "in the early Spark 1.x releases" e o DataFrame no 1.3, como sucessor do SchemaRDD. Luu diz que "in Spark version 1.6, a new programming abstraction called Structured APIs was introduced".

**Os dois estão parcialmente certos e o Luu comprime três eventos em um.** A linha do tempo verificada contra a documentação do Spark:

| Release | Evento | Fonte |
|---|---|---|
| 1.3 | DataFrame API, sucedendo o SchemaRDD | Damji |
| 1.6 | Dataset, "a new interface added in Spark 1.6" | Spark SQL Guide |
| 2.0 | DataFrame e Dataset unificados como Structured APIs | os dois livros |

O Damji acerta o 1.3 para o DataFrame. O 1.6 do Luu é a data do Dataset, e não do conjunto chamado Structured APIs, que é nome que só passa a fazer sentido depois da unificação no 2.0. Ele rotulou um marco com o nome de outro.

Curiosidade que ninguém dos dois nota: o Luu, na mesma abertura, diz que o RDD foi a abstração inicial "when Spark was introduced to the world in 2012". O Capítulo 1 do Damji data o início do projeto em 2009 e o open source em 2010. 2012 é o ano do paper de RDD, não da estreia do Spark.

**3.** **A força da alegação ANSI.** Damji diz "ANSI SQL:2003–compatible". Luu diz "Spark implements a subset of ANSI SQL:2003 revision".

**O Luu está certo, e a documentação atual está mais perto dele do que do Damji.** A página ANSI Compliance não reivindica conformidade com SQL:2003 em lugar nenhum e ressalva que algumas features do dialeto ANSI podem não vir do padrão diretamente, ainda que o comportamento se alinhe ao estilo dele.

A diferença entre "compatible" e "a subset of" não é retórica. A primeira é uma afirmação binária sobre um padrão inteiro, e nenhum engine analítico implementa um padrão inteiro. A segunda declara escopo parcial e é falseável só por excesso.

O Luu ainda faz uma coisa que o Damji não faz: ele diz para que a conformidade serve. Estar em conformidade com essa revisão permite comparar o engine em um benchmark padrão da indústria, o TPC-DS. Isso dá à alegação um propósito verificável em vez de um selo.

O que os dois deixam passar é o que o item 3 do Nível 5 encontrou: nenhum dos dois diz qual subconjunto do padrão está coberto, e a documentação atual simplesmente parou de fazer a alegação. Não dá para conferir nem a versão forte nem a fraca. *Ali eu também registro por que a explicação óbvia, a de ligar isso ao `spark.sql.ansi.enabled`, está errada: aquele flag governa semântica de runtime, e não cobertura de gramática.*

**4.** **O rótulo da matriz de erros.** Damji, na Figure 3-2, chama a primeira linha de "Syntax Errors". Luu, na Table 3-12, chama de "System Errors". O conteúdo das seis células é idêntico nos dois.

**O Damji está certo e o Luu tem um erro de rótulo.** A arbitragem é por coerência interna, e não precisa de fonte externa.

A matriz tem duas linhas porque separa dois momentos de falha. Uma linha é sobre a forma do código, e a outra é sobre os nomes que o código cita. "Analysis Errors" nomeia a segunda de forma exata, e é o mesmo vocabulário que o Damji usa na fase 1 do Catalyst, onde a analysis resolve nomes contra o `Catalog`. Se a segunda linha é analysis, a primeira só pode ser sintaxe, porque é o que sobra antes da análise.

"System Errors" não nomeia nada nesse par. Não existe fase de sistema, e um erro de sistema seria uma falha de execução, que estaria na coluna de runtime em todas as três APIs.

O Luu usa o rótulo uma vez, na tabela, e não o repete no texto. É deslize de edição, e não uma tese.

Uma coisa em que o Luu fica melhor mesmo assim: ele coloca a tabela em uma seção chamada "The Trio: DataFrame, Dataset, and SQL" e a fecha com uma frase que explica por que a matriz importa. Quanto mais cedo você pega o erro, mais produtivo você é e mais estável fica a aplicação. O Damji põe a figura no fim de uma lista de onze itens, como o décimo primeiro.

**5.** **O escopo do formato padrão.** Damji diz que Parquet é o formato padrão do `DataFrameWriter`. Luu diz que Parquet é o padrão para leitura e escrita, e mostra `spark.read.load(path)` funcionando sem `format`.

**O Luu está certo e o Damji está incompleto.** A página Generic Load/Save Functions da documentação 4.2.0 diz que o data source padrão, `parquet` a menos que `spark.sql.sources.default` diga outra coisa, "will be used for all operations".

"All operations" fecha a questão. O Damji não afirma nada falso, ele afirma metade.

A metade que falta tem consequência prática. O exemplo do Luu, `spark.read.load("<path>/movies.parquet")`, é a forma mais curta de ler Parquet, e um leitor que só tem o Damji não sabe que ela existe. Ele vai escrever `spark.read.parquet(path)` ou `spark.read.format("parquet").load(path)`, que funcionam e são mais longas.

O Luu também explica por que Parquet ganhou esse lugar, com uma seção sobre formato colunar, compressão, autodescrição e o fato de o `movies.parquet` ocupar cerca de um sexto do `movies.csv`. O Damji cita Parquet como padrão e nunca diz o que ele é.

**6.** **Encoders e eficiência, que não é discordância.** Achei que fosse e não é.

O Damji tem dois itens que parecem brigar com o Luu. Ele diz "If you want to take advantage of and benefit from Tungsten's efficient serialization with Encoders, use Datasets" e, quatro linhas depois, "If you want space and speed efficiency, use DataFrames". O Luu diz que os encoders reduzem uso de memória quando o Dataset é cacheado e reduzem bytes no shuffle, e depois diz que existe um custo de conversão de `Row` para objeto de domínio que pesa em milhões de linhas.

Lido rápido, parece que o Luu contradiz a primeira frase do Damji e confirma a segunda. Não é isso.

As três afirmações falam de duas fronteiras diferentes, e todas são verdadeiras. A primeira fronteira é entre objeto de domínio serializado com Java serialization e objeto de domínio serializado com encoder. Aí o encoder ganha, e é o que os dois livros dizem. A documentação do Spark SQL confirma o mecanismo e acrescenta o que nenhum dos dois diz: encoders são gerados dinamicamente e usam um formato que permite filtrar, ordenar e fazer hash sem desserializar os bytes de volta para objeto.

A segunda fronteira é entre trabalhar com objeto de domínio e trabalhar com `Row`. Aí o `Row` ganha, porque não existe conversão a pagar. É o que a segunda frase do Damji diz e é o custo que o Luu nomeia.

Não há disputa a arbitrar. O que há é uma lacuna no Damji, e o Luu a preenche. O Damji põe as duas pontas em uma lista de bullets sem dizer que elas medem coisas diferentes, o que faz a lista parecer contraditória. O Luu escreve as duas em prosa, na ordem benefício e depois custo, e a relação entre elas fica visível.

O que fica para a nota: quando duas fontes parecem discordar sobre performance, a primeira pergunta é o que cada uma está medindo. Nas três vezes que testei isso neste par de capítulos, duas viraram diferença de escopo e só uma virou erro.

---

## Termos-chave para definir antes de seguir adiante

Escreva uma definição de uma linha para cada, com suas próprias palavras, sem consultar:

RDD · dependencies · compute function · lineage · partitioner · Structured API · DSL · DataFrame · Dataset · `Row` · `Column` · `col` contra `expr` · schema · schema-on-read · DDL string · `StructType` · `StructField` · nullable · `DataType` · `DecimalType` · complex type · `DataFrameReader` · `DataFrameWriter` · `samplingRatio` · `inferSchema` · Parquet · Hive metastore · projection · filter · aggregation · `countDistinct` · `collect` contra `take` · case class · encoder · type safety · compile-time contra runtime · syntax error contra analysis error · Spark SQL engine · Catalyst · Catalog · AST · logical plan · optimized logical plan · physical plan · cost model · predicate pushdown · projection pruning · constant folding · code generation · whole-stage code generation · Tungsten · `Exchange` · `HashAggregate` · ANSI SQL:2003

### Minhas definições

Vinte e quatro dos cinquenta e quatro termos o capítulo usa sem definir, ou define mal. Marquei esses casos em itálico. O padrão deste capítulo é o oposto do Capítulo 1: aqui a terminologia de API é bem tratada e o vocabulário do motor entra quase todo por nome.

**RDD** — Resilient Distributed Dataset. Para este capítulo, a abstração mais básica do Spark, feita de dependencies, partitions com informação de locality e uma compute function `Partition => Iterator[T]`. *A contagem canônica é de cinco propriedades, e o partitioner fica de fora da do capítulo.*

**dependencies** — A lista que diz ao Spark como um RDD é construído a partir das suas entradas. É ela que permite recriar o RDD e repetir as operações, e é dela que vem a resiliência.

**compute function** — A função que produz um `Iterator[T]` com o dado que ficará no RDD. É opaca para o Spark, que só a enxerga como lambda.

**lineage** — *Não aparece neste capítulo.* O registro encadeado das transformations que produziram um DataFrame. O capítulo a menciona de passagem ao dizer que o Spark mantém uma lineage de todas as transformations, e remete ao Capítulo 2.

**partitioner** — *Não aparece.* O objeto que define como as chaves de um RDD de pares são distribuídas entre partitions. É o que permite evitar shuffle quando duas entradas já estão particionadas do mesmo jeito.

**Structured API** — O conjunto formado por DataFrame e Dataset, unificado no Spark 2.0, construído sobre o Spark SQL engine. O capítulo usa o termo no título e nunca o define em uma frase.

**DSL** — *Sigla expandida, conceito não explicado.* Domain-specific language. O conjunto de operadores comuns pelos quais você diz ao Spark o que computar. O capítulo diz que ela está disponível como API nas linguagens suportadas e não diz o que faz de uma coleção de funções uma DSL.

**DataFrame** — Tabela distribuída de colunas nomeadas e tipadas, inspirada no pandas, com schema. Em Scala é um apelido de `Dataset[Row]`. *A definição do capítulo diz "in-memory", e o item 1 do Nível 4 mostra por que essa palavra não se sustenta.*

**Dataset** — Coleção fortemente tipada de objetos específicos de domínio, transformável em paralelo com operações funcionais ou relacionais. Existe só em Scala e Java.

**`Row`** — Objeto genérico do Spark que contém uma ou mais colunas, de tipos iguais ou diferentes. É uma coleção ordenada de campos, acessível por índice começando em 0.

**`Column`** — O tipo que representa uma coluna como objeto com métodos públicos. É o que permite escrever expressões sobre a coluna em vez de apenas nomeá-la.

**`col` contra `expr`** — `col("Hits")` devolve um objeto `Column` a partir do nome. `expr("Hits * 2")` recebe uma string que o Spark interpreta como expressão. As duas chegam ao mesmo lugar, e o capítulo mostra `select(expr("Hits"))` e `select(col("Hits"))` devolvendo o mesmo valor.

**schema** — A definição dos nomes de coluna e dos tipos de dado associados de um DataFrame.

**schema-on-read** — *Nomeado uma vez, nunca explicado.* A abordagem em que o schema não é declarado e o Spark o descobre lendo o dado. É o contraste que o capítulo usa para vender a declaração antecipada, e ele nunca diz que a inferência é o comportamento padrão de JSON e a exceção em CSV.

**DDL string** — A forma de declarar um schema como texto, no estilo `` `Id` INT, `First` STRING ``. O capítulo a chama de muito mais simples que a forma programática.

**`StructType`** — O tipo estruturado que representa uma linha inteira, construído a partir de um array de `StructField`. Em Scala o valor associado é `org.apache.spark.sql.Row`.

**`StructField`** — A declaração de um campo, com nome, tipo de dado e, opcionalmente, se aceita nulo.

**nullable** — *Usado nas saídas e nunca discutido.* A flag do `StructField` que declara se o campo aceita nulo. O capítulo imprime `false` em um exemplo e `true` em outro, com o mesmo schema declarado, e afirma que as duas saídas são iguais.

**`DataType`** — *Não aparece no capítulo.* A superclasse de todos os tipos do Spark SQL. É o termo que a Table 3-2 deveria ter usado no lugar de `DataTypes`. *Verificado no item 8 do Nível 5.*

**`DecimalType`** — O tipo decimal de precisão arbitrária. Mapeia para `java.math.BigDecimal` em Scala e `decimal.Decimal` em Python. O capítulo o apresenta como exceção à hierarquia, e a exceção real é só ele não ter um singleton pronto na classe de fábrica.

**complex type** — Os tipos que contêm outros tipos ou valores compostos: `ArrayType`, `MapType`, `StructType`, mais os temporais e o binário. O capítulo os chama de structured and complex data types e diz que dado real é complexo, aninhado, e vem em muitas formas.

**`DataFrameReader`** — A interface que lê dado para dentro de um DataFrame a partir de fontes em JSON, CSV, Parquet, Text, Avro, ORC e outras.

**`DataFrameWriter`** — A interface que escreve um DataFrame de volta para uma fonte, em um formato à escolha. Tem Parquet como padrão. *O capítulo não mostra `mode`, `partitionBy` nem `bucketBy`.*

**`samplingRatio`** — A opção que define a fração de linhas usada para inferir o schema. *No item 9 do Nível 5 verifiquei que ela só tem efeito quando há inferência ligada, o que o exemplo do capítulo não faz.*

**`inferSchema`** — *Não aparece neste capítulo.* A opção que liga a inferência de tipos na leitura de CSV, com default `false`. A ausência dela é o que torna a NOTE de `samplingRatio` inerte.

**Parquet** — Formato colunar, padrão do Spark, com compressão snappy segundo o capítulo. Preserva o schema no metadado, o que dispensa declará-lo em leituras posteriores. *O capítulo o usa o tempo todo e nunca explica o que é um formato colunar.*

**Hive metastore** — *Nomeado sem definição.* O repositório de metadado onde `saveAsTable` registra a tabela. O capítulo adia metastores para o capítulo seguinte.

**projection** — A operação que devolve um subconjunto das colunas, possivelmente transformadas. No Spark é o `select()`. *A definição do capítulo descreve um filtro, e isso é o item 5 do Nível 4.*

**filter** — A operação que devolve apenas as linhas que satisfazem uma condição. No Spark é `filter()` ou `where()`, que o capítulo trata como equivalentes.

**aggregation** — A operação que reduz muitas linhas a um valor por grupo. O capítulo demonstra com `groupBy()`, `count()`, `agg()`, `sum()`, `avg()`, `min()` e `max()`.

**`countDistinct`** — A função de agregação que conta valores distintos de uma coluna. O capítulo mostra que ela e a combinação de `distinct()` com contagem dão o mesmo 32.

**`collect` contra `take`** — `collect()` traz todos os objetos `Row` do DataFrame para o driver, e pode causar out-of-memory. `take(n)` traz só os primeiros n. `count()` traz um número só.

**case class** — *Nomeada, explicação adiada para o Capítulo 6.* A classe de Scala que define o objeto de domínio de um `Dataset[T]`. O capítulo mostra `DeviceIoTData` e nunca diz o que uma case class tem de diferente de uma classe comum.

**encoder** — *Nomeado duas vezes, mecanismo adiado.* O utilitário que converte um objeto de domínio para o formato binário interno do Spark. *No item 7 do Nível 5 verifiquei que ele também permite filtrar, ordenar e fazer hash sem desserializar de volta.*

**type safety** — A garantia de que uma referência a um campo é verificada contra um tipo conhecido antes da execução. No Spark ela existe só onde há compilação, ou seja, em Scala e Java.

**compile-time contra runtime** — Os dois momentos em que um erro pode aparecer. Compile time é antes de rodar, quando um compilador verifica o código. Runtime é durante a execução da query.

**syntax error contra analysis error** — Erro de sintaxe é erro na forma do código. Erro de análise é referência a um nome que não existe, como uma coluna ausente. A Figure 3-2 cruza os dois com SQL, DataFrames e Datasets. *O capítulo nunca define os dois termos em prosa, só na figura.*

**Spark SQL engine** — O motor sobre o qual as Structured APIs de alto nível são construídas e que sustenta todos os componentes do Spark. Ele unifica os componentes, conecta ao Hive metastore, lê e escreve formatos estruturados, oferece um shell, expõe JDBC/ODBC e gera plano otimizado e código compacto.

**Catalyst** — O otimizador do Spark SQL. Recebe uma query computacional e a converte em um plano de execução, em quatro fases.

**`Catalog`** — A interface programática do Spark SQL que guarda a lista de nomes de colunas, tipos de dado, funções, tabelas e bancos. É consultada na fase de analysis para resolver nomes. *O capítulo só a apresenta pelo lado interno, e nunca menciona que existe um `spark.catalog` para o usuário.*

**AST** — *Sigla expandida, conceito não explicado.* Abstract syntax tree. A árvore que o engine gera a partir da query SQL ou DataFrame, antes de resolver os nomes.

**logical plan** — O plano com os nomes já resolvidos, que descreve o que computar sem dizer como. É a saída da fase de analysis.

**optimized logical plan** — O plano lógico depois das regras de otimização. No exemplo impresso, ele difere do anterior por ter perdido o nó `Project`.

**physical plan** — O plano com operadores físicos que existem no engine de execução, como `HashAggregate` e `Exchange`. É a saída da fase de physical planning.

**cost model** — *Aparece só na Figure 3-4.* O componente que recebe vários planos físicos e escolhe um. *A prosa do capítulo o coloca na fase 2 e a figura o coloca na fase 3, e isso é o item 9 do Nível 4.*

**predicate pushdown** — A otimização que empurra o filtro para o mais perto possível da leitura, para reduzir o dado que entra no plano. A Figure 3-5 mostra o `filter` descendo por baixo do `join` e depois desaparecendo dentro do scan.

**projection pruning** — A otimização que elimina da leitura as colunas que a saída não usa.

**constant folding** — *Nomeada e nunca explicada.* A otimização que avalia em tempo de planejamento as expressões cujos operandos são constantes.

**code generation** — A quarta fase do Catalyst. Gera bytecode Java eficiente para rodar em cada máquina, o que faz o Spark SQL agir como compilador.

**whole-stage code generation** — A otimização física que colapsa a query inteira em uma única função, elimina chamadas de função virtual e usa registradores de CPU para o dado intermediário.

**Tungsten** — O engine que faz o whole-stage code generation. A segunda geração dele foi introduzida no Spark 2.0 e gera código RDD compacto para a execução final. O detalhamento fica para o Capítulo 6.

**`Exchange`** — *Aparece só no plano impresso, sem uma linha de explicação.* O nó do plano físico que redistribui dado entre executors. O plano do capítulo tem dois, um `hashpartitioning` para a agregação e um `rangepartitioning` para a ordenação global.

**`HashAggregate`** — *Aparece só no plano impresso.* O operador físico que agrega por chave usando tabela de hash. No plano do capítulo ele aparece duas vezes, uma com `partial_sum` antes do shuffle e uma com `sum` depois.

**ANSI SQL:2003** — *Citado como selo, nunca qualificado.* A revisão do padrão SQL com a qual o capítulo diz que o Spark SQL é compatível. *No item 3 do Nível 5 verifiquei que a documentação atual não faz essa alegação e não a substitui por nenhuma outra. O flag `spark.sql.ansi.enabled` não tem relação com isso: ele governa semântica de runtime, e não cobertura de gramática.*
