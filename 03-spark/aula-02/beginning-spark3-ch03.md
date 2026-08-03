# Guia de Leitura — *Beginning Apache Spark 3*, Capítulo 3: Spark SQL: Foundation

**Fonte:** Hien Luu, *Beginning Apache Spark 3: With DataFrame, Spark SQL, Structured Streaming, and Spark Machine Learning Library* (Apress, 2021), Capítulo 3, 41 páginas
**Escopo:** as perguntas dos Níveis 1 a 4 são respondíveis apenas com este capítulo. Algumas respostas do Nível 4 fecham o argumento com um fato verificado no Nível 5, e nesses casos elas dizem de onde o fato veio. O Nível 5 não é respondível pelo capítulo, e por isso cada item dele carrega URL e data de acesso. Este guia não tem Nível 6.

**Sobre as figuras:** o capítulo tem duas figuras, a 3-1 e a 3-2. Abri as páginas 1 e 40 do PDF e li as duas imagens. Onde uma resposta descreve conteúdo de figura, ela veio da imagem, e não do texto extraído.

---

## Como usar este guia

1. Leia o capítulo uma vez de ponta a ponta, sem parar. Ele é longo e tem quarenta e nove listings.
2. Faça o Nível 1 de memória primeiro, depois volte ao texto para preencher as lacunas. Marque toda questão que você não conseguiu responder.
3. Faça o Nível 2 por escrito, em frases completas. Se você não consegue escrever, você não sabe.
4. O Nível 3 é para o terminal, não para o papel. Use o dataset que você quiser, contanto que não seja o `movies` do livro.
5. O Nível 4 é o nível mais pesado deste guia, e é onde o capítulo se torna útil. Ele obriga a ler tabelas e listings como evidência, e este capítulo tem muito erro interno para achar.

---

## Nível 1 — Memorização e definições

Respostas curtas e conferíveis.

**1.** What was Spark's initial core programming abstraction, in what year did it arrive, and in which Spark version were the Structured APIs introduced? *(Chapter intro)*

R: O resilient distributed dataset, o RDD, foi a abstração inicial, quando o Spark apareceu em 2012. As Structured APIs foram introduzidas no Spark 1.6.

**2.** What two things do the Structured APIs require so that Spark can optimize? *(Chapter intro)*

R: Que os dados estejam disponíveis em formato estruturado. E que a lógica de computação siga uma certa estrutura. O capítulo diz que, armado com essas duas informações, o Spark consegue fazer as otimizações que aceleram a aplicação.

**3.** What are the two main parts of the Spark SQL module? *(Chapter intro)*

R: A primeira parte são as representações das Structured APIs, chamadas DataFrame e Dataset, que definem as APIs de alto nível para dados estruturados. A segunda é o Catalyst optimizer, responsável pelo maquinário que roda por baixo.

**4.** What inspired the DataFrame concept, and what is the main difference? *(Chapter intro)*

R: O DataFrame do pandas, em Python. A diferença principal é que um DataFrame no Spark lida com um grande volume de dados espalhado por muitas máquinas.

**5.** What concept differentiates structured data from unstructured data, and how does the chapter define it? *(Chapter intro)*

R: O schema. O capítulo o define como aquilo que descreve a estrutura do dado na forma de nomes de coluna e tipos de dado associados. Ele diz que o schema é parte integral das Structured APIs.

**6.** Which text formats and which binary formats does the chapter name as common for structured data, and what unanticipated consequence does the read/write versatility create? *(Chapter intro)*

R: Texto: CSV, XML e JSON. Binário: Avro, Parquet e ORC. A consequência não antecipada é que o Spark pode ser usado como ferramenta de conversão de formato de dados.

**7.** What new group of users does the SQL capability bring to Spark, according to the chapter? *(Chapter intro)*

R: Os business analysts, que já conhecem SQL porque é uma das ferramentas principais do dia a dia deles.

**8.** What does Figure 3-1 show? Name every box and how they stack. *(Figure 3-1)*

R: Abri a página 1 e a figura tem três camadas, com duas caixas aninhadas dentro do Spark SQL. No topo, duas caixas brancas lado a lado, `Spark shell` e `Spark applications`, cada uma com uma seta apontando para baixo. Elas entram numa caixa azul-clara grande chamada `Spark SQL`, que contém duas caixas azuis-escuras empilhadas, `DataFrame API` e `Catalyst Optimizer`. Embaixo de tudo, separada, uma caixa azul-clara chamada `Spark Core`. Não existe caixa de Dataset, nem de SQL, nem de DataFrameReader.

**9.** Give the chapter's one-sentence definition of an RDD, and list its five characteristics. Which two are optional? *(Understanding RDD)*

R: Um RDD representa uma coleção de elementos tolerante a falhas, particionada pelos nós de um cluster, que pode ser operada em paralelo. As cinco características são estas:

1. Um conjunto de dependências sobre RDDs pais.
2. Um conjunto de partitions, que são os pedaços que formam o dataset inteiro.
3. Uma função para computar todas as linhas do dataset.
4. Os metadados sobre o esquema de particionamento, marcado como opcional.
5. A localização dos dados no cluster, também marcada como opcional.

**10.** Which three of those five make up the lineage information, and for what two purposes does Spark use it? *(Understanding RDD)*

R: As três primeiras: dependências, partitions e função de computação. Os dois usos são determinar a ordem de execução dos RDDs e fazer recuperação de falha.

**11.** What does each of the three main pieces contribute? Who supplies the compute function and where is it sent? *(Understanding RDD)*

R: O conjunto de dependências é a entrada de dados do RDD, e é o que permite reproduzi-lo em caso de falha, ou seja, é ele que dá a resiliência. O conjunto de partitions permite executar a lógica em paralelo. A função de computação é fornecida pelo usuário do Spark e enviada a cada executor do cluster, para rodar contra cada linha de cada partition.

**12.** What is the drawback of the RDD's flexibility, and which three optimizations does the chapter name as impossible because of it? *(Understanding RDD)*

R: O Spark não tem visão da intenção do usuário. Ele não sabe se a lógica está filtrando, fazendo join ou agregando. As três otimizações nomeadas são estas. Predicate pushdown, para reduzir o dado lido da origem. Recomendar um tipo de join mais eficiente. E podar as colunas que a saída não usa.

**13.** Give the chapter's definition of a DataFrame. What object represents each row? *(Introduction to the DataFrame API)*

R: Uma coleção imutável e distribuída de dados organizada em linhas. Cada linha tem um conjunto de colunas, e cada coluna tem nome e tipo associado. O capítulo diz que isso equivale à tabela de um RDBMS. Cada linha é representada por um objeto `Row` genérico.

**14.** Into which two types are the DataFrame APIs classified, what are the evaluation semantics of each, and in which four languages is the API available? *(Introduction to the DataFrame API)*

R: Transformation e action. Transformations são avaliadas de forma lazy e actions são avaliadas de forma eager. O capítulo diz que a semântica é idêntica à do RDD. A API está em Scala, Java, Python e R.

**15.** What is the one thing common to every way of creating a DataFrame? *(Creating a DataFrame)*

R: Fornecer um schema, de forma implícita ou explícita.

**16.** Which implicit function converts an RDD into a DataFrame with given column names, and where do the column types come from? *(Creating a DataFrame from RDD; Listing 3-1)*

R: A função implícita `toDF`, chamada como `rdd.toDF("key","value")`. Os tipos das colunas são inferidos a partir dos valores do RDD.

**17.** What do `printSchema` and `show` do, and how many rows does `show` display by default? *(Listings 3-2, 3-3)*

R: `printSchema` imprime os nomes das colunas e os tipos associados no console. `show` imprime os dados em formato tabular. O padrão é 20 linhas, e passar um número muda isso, como em `kvDF.show(5)`.

**18.** Which function builds a DataFrame from an RDD of `Row` objects plus a schema, and what three pieces of information does each `StructField` carry? *(Listing 3-4)*

R: `spark.createDataFrame(peopleRDD, schema)`. Cada `StructField` carrega nome, tipo e se o valor é nullable ou não. O schema é montado como um `StructType` sobre um array de `StructField`.

**19.** What flexibility does the chapter claim for programmatically created schemas? *(after Listing 3-5)*

R: Que a aplicação Spark pode ajustar o schema com base em alguma configuração externa.

**20.** From Table 3-1, give the Scala type for `DecimalType`, `BinaryType`, `TimestampType`, `DateType`, `ArrayType`, `MapType` and `StructType`. *(Table 3-1)*

R:

| Data Type | Scala Type |
|---|---|
| `DecimalType` | `java.math.BigDecial`, escrito assim mesmo, com o erro de digitação |
| `BinaryType` | `Array[Byte]` |
| `TimestampType` | `java.sql.Timestamp` |
| `DateType` | `java.sql.Date` |
| `ArrayType` | `scala.collection.Seq` |
| `MapType` | `scala.collection.Map` |
| `StructType` | `org.apache.spark.sql.Row` |

A tabela ordena os tipos escalares primeiro e os complexos por último.

**21.** What does `spark.range` produce by default, and what do the three arguments of the longest form mean? What limitation does the chapter point out? *(Creating a DataFrame from a Range of Numbers)*

R: Um dataset de coluna única, com nome `id` e tipo `LongType`. Na forma de três argumentos, o primeiro é o valor inicial, o segundo é o valor final e é exclusivo, e o terceiro é o passo. A limitação é que `range` só cria DataFrame de uma coluna.

**22.** What is the option the chapter gives for creating a multicolumn DataFrame without reading a file? *(Listing 3-7)*

R: Usar os implicits do Spark sobre uma coleção de tuplas dentro de um `Seq` do Scala, e chamar `toDF` com os nomes das colunas. O exemplo é o `movies` com actor, title e year.

**23.** What are the two main classes for reading and writing data, and through which variables are they reached? *(Creating a DataFrame from Data Sources; Writing Data Out to Storage Systems)*

R: `DataFrameReader` e `DataFrameWriter`. O reader vem da variável `read` da classe `SparkSession`, como em `spark.read`. O writer vem da variável `write` da classe DataFrame, como em `movies.write`.

**24.** Write the common pattern for interacting with `DataFrameReader`, and say which of the three pieces in Table 3-2 are optional. *(Listing 3-9; Table 3-2)*

R:

```scala
spark.read.format(...).option("key", value").schema(...).load()
```

O `format` é o único obrigatório. `option` e `schema` são opcionais. Sobre o schema, a tabela diz que Parquet e ORC trazem o schema embutido no arquivo e a inferência é automática, e que nos outros casos pode ser necessário fornecê-lo.

**25.** List Spark's six built-in data sources from Table 3-3 with their data format, and say which one is the default. *(Table 3-3)*

R:

| Nome | Formato | Comentário |
|---|---|---|
| Text file | Text | Sem estrutura |
| CSV | Text | Comma-separated values, aceita outro delimitador, nome de coluna pode vir do header |
| JSON | Text | Semiestruturado, nome e tipo de coluna inferidos automaticamente |
| Parquet | Binary | Formato padrão, popular na comunidade Hadoop |
| ORC | Binary | Outro formato binário popular na comunidade Hadoop |
| JDBC | Binary | Formato comum para ler e escrever em RDBMS |

O Parquet é marcado como default format na própria tabela.

**26.** What short names does `format()` accept, and what does a custom data source require? *(Listing 3-10)*

R: Os short names são `json`, `parquet`, `jdbc`, `orc`, `csv` e `text`. Uma origem customizada exige o nome totalmente qualificado, no formato `spark.read.format("org.example.mysource")`.

**27.** When Spark reads a text file, what becomes a row, what is the resulting column called, and which site does the chapter recommend for free plain text books? *(Creating a DataFrame by Reading Text Files; Listing 3-11)*

R: Cada linha do arquivo vira uma linha do DataFrame. A coluna resultante se chama `value` e é do tipo string. O site recomendado é `www.gutenberg.org`. O capítulo também diz que uma forma comum de separar palavras é quebrar cada linha pelo espaço.

**28.** List the four CSV options in Table 3-4 with their values and defaults. *(Table 3-4)*

R:

| Key | Valores | Default | Descrição |
|---|---|---|---|
| `sep` | um caractere | `,` | O caractere usado como delimitador de cada coluna |
| `header` | true, false | `false` | Se true, a primeira linha do arquivo traz os nomes das colunas |
| `escape` | qualquer caractere | `\` | O caractere de escape quando o valor da coluna contém o próprio `sep` |
| `inferSchema` | true, false | `false` | Se o Spark deve tentar inferir o tipo da coluna pelo valor |

**29.** What happens if `inferSchema` is false and no schema is provided? Where does the chapter say the complete option list lives? *(Creating a DataFrame by Reading CSV Files)*

R: O Spark assume o tipo string para todas as colunas. A lista completa, segundo o capítulo, está na classe `CSVOptions`, no repositório `https://github.com/apache/spark`.

**30.** How do you read a tab-separated file with the CSV format? *(Listing 3-13)*

R: Passando `option("sep", "\t")` junto com `option("header","true")` e o schema, e chamando `.csv(<path>)`. O arquivo do exemplo é o `movies.tsv`. O capítulo diz que o parser CSV do Spark aceita qualquer delimitador e que a vírgula só é o padrão.

**31.** List the three JSON options in Table 3-5 with their defaults, and explain why loading a very large JSON file is expensive. *(Table 3-5; Creating a DataFrame by Reading JSON Files)*

R:

| Key | Valores | Default | Descrição |
|---|---|---|---|
| `allowComments` | true, false | `false` | Ignora comentários no arquivo JSON |
| `multiLine` | true, false | `false` | Trata o arquivo inteiro como um objeto JSON grande que atravessa várias linhas |
| `samplingRatio` | a tabela mostra `0.3` na coluna de valores | `1.0` | O tamanho da amostra lida para inferir o schema |

Carregar um JSON grande é caro porque o padrão de `samplingRatio` é 1.0, ou seja, o Spark lê o arquivo inteiro para inferir o schema. Baixar esse valor acelera a carga.

**32.** What is JSON's one disadvantage according to the chapter, and what is the one thing you must tell Spark about a JSON file? *(Creating a DataFrame by Reading JSON Files)*

R: A desvantagem é a verbosidade, porque o nome da coluna se repete em cada linha do arquivo. A coisa que precisa ser informada é se o objeto JSON está numa linha só ou espalhado por várias linhas.

**33.** What does Spark do by default when it meets a corrupted record or a parsing error, and how do you tell it to fail fast? What error appears? *(Listing 3-15)*

R: Segundo o capítulo, o Spark define como null o valor de todas as colunas daquela linha. Para falhar rápido, usa-se `option("mode","failFast")`. O erro mostrado é um `java.lang.RuntimeException` com a mensagem `Failed to parse a value for data type BooleanType`, disparado quando uma action executa, e não na leitura.

**34.** Where was Parquet created, why is it popular, how does its size compare with the CSV in the chapter's example, and what read optimization does Spark apply? *(Creating a DataFrame by Reading Parquet Files)*

R: Foi criado no Twitter. É popular por ser um formato self-describing e por guardar dados de forma muito compacta, com compressão. O `movies.parquet` tem cerca de um sexto do tamanho do `movies.csv`. A otimização é fazer decompression e decoding em lotes de coluna, o que acelera muito a leitura. O capítulo também lembra que Parquet é o formato padrão, então nem `format` nem schema precisam ser informados.

**35.** What does ORC stand for, who created it and why? *(Creating a DataFrame by Reading ORC Files)*

R: Optimized Row Columnar. Foi criado pela Cloudera, como parte de uma iniciativa para acelerar o Hive. O capítulo o descreve como parecido com o Parquet em eficiência e velocidade, também desenhado para workload analítico.

**36.** What four pieces of information does a JDBC data source need, what has to be on the classpath, and how does the chapter test the connection? *(Creating a DataFrame from JDBC)*

R: Um driver JDBC do RDBMS, uma URL de conexão, informação de autenticação e um nome de tabela. O JAR do driver JDBC precisa estar no classpath do Spark, passado na linha de comando ao iniciar o shell. O teste é feito com `java.sql.DriverManager`, chamando `DriverManager.getConnection(connectionURL)` e depois `connection.isClosed()`. Se não vier exception, a conexão funciona.

**37.** Which three JDBC options does Table 3-6 describe? *(Table 3-6)*

R: `url`, que precisa conter pelo menos host, porta e nome do banco. `dbtable`, o nome da tabela de onde ler ou para onde escrever. E `driver`, o nome da classe do driver JDBC, que para o MySQL Connector/J é `com.mysql.jdbc.Driver`.

**38.** What is predicate pushdown, which two data sources does the chapter name as supporting it, and where is the example? *(Creating a DataFrame from JDBC)*

R: É a otimização em que o Spark empurra as condições de filtro para o RDBMS, o máximo que consegue. O dado é filtrado no nível do banco, o que acelera o filtro e reduz muito o volume que o Spark precisa ler. As duas origens nomeadas são JDBC e Parquet. O exemplo fica na seção "Catalyst Optimizer" do Capítulo 4.

**39.** What is a DSL, and what is the application domain in this case? *(Working with Structured Operations)*

R: Domain-specific language, uma linguagem de computador especializada em um domínio de aplicação. Aqui o domínio é a manipulação distribuída de dados. O capítulo diz que as structured operations são desenhadas para ser relacionais, espelhando expressões de SQL como projeção, filtro, transformação e join.

**40.** Name the structured transformations in Table 3-7. *(Table 3-7)*

R: `select`, `selectExpr`, `filter` e `where`, `distinct` e `dropDuplicates`, `sort` e `orderBy`, `limit`, `union`, `withColumn`, `withColumnRenamed`, `drop`, `sample`, `randomSplit`, `join` e `groupBy`. A tabela lembra que o DataFrame é imutável e que toda transformation devolve um DataFrame novo. `join` e `groupBy` são adiados para o próximo capítulo.

**41.** What are the three categories of functionality of the `Column` class, and when is a string not enough? *(Working with Columns)*

R: Operações matemáticas, como soma e multiplicação. Comparações lógicas entre valor de coluna ou literal, como igualdade, maior que e menor que. E casamento de padrão em string, como começa com e termina com. Sempre que houver necessidade de uma column expression, a coluna tem de ser uma instância de `Column`, e não uma string. Passar string nesse caso dá erro de tipo.

**42.** List the five ways to refer to a column in Table 3-8. Which are Scala-only, and what does the author recommend? *(Table 3-8; Listing 3-21)*

R:

| Função | Exemplo | O que é |
|---|---|---|
| `""` | `"columnName"` | Refere a coluna como string |
| `col` | `col("columnName")` | Devolve uma instância de `Column` |
| `column` | `column("columnName")` | Igual ao `col`, devolve uma instância de `Column` |
| `$` | `$"columnName"` | Açúcar sintático para construir um `Column`, só em Scala |
| `'` | `'columnName` | Açúcar sintático que usa symbol literals do Scala, só em Scala |

`col` e `column` são sinônimos e existem em Scala e em Python. A recomendação do autor é usar `col` para quem alterna entre Scala e Python, e usar o apóstrofo para quem fica só em Scala, porque é um caractere só. A classe DataFrame tem o próprio `col`, que desambigua colunas de mesmo nome em um join. Para listar as colunas, o exemplo usa `kvDF.columns`.

**43.** What is the more technical term for `select`, and how does `selectExpr` differ? *(Table 3-7; select; selectExpr)*

R: O termo técnico para `select` é projection. O `selectExpr` faz a mesma projeção, mas aceita uma ou mais expressões SQL em vez de colunas. O Spark converte a string da expressão numa árvore lógica e a avalia na ordem certa. O exemplo é `selectExpr("*","(produced_year - (produced_year % 10)) as decade")`.

**44.** Which operators does `filter` use for equality and for inequality, and what are the two ways of combining conditions? *(filter, where; Listing 3-26)*

R: Igualdade usa três sinais de igual, `===`. Desigualdade usa `=!=`. As duas formas de combinar são o operador `&&` numa expressão só, ou encadear duas chamadas de `filter`. `filter` e `where` têm a mesma semântica, e o capítulo diz que `where` é apenas um pouco mais relacional.

**45.** What is the difference between `distinct` and `dropDuplicates`, and is there a performance difference? *(distinct, dropDuplicates)*

R: O comportamento é idêntico, mas `dropDuplicates` permite escolher quais colunas entram na lógica de deduplicação. Sem coluna especificada, ele usa todas as colunas do DataFrame. Não há diferença de performance, porque o Spark transforma as duas no mesmo logical plan.

**46.** What is the default sort order, how do you reverse it, and what does `limit` do? *(sort, orderBy; limit)*

R: A ordem padrão é ascendente. Para descendente usa-se `.desc` na coluna, como em `orderBy('title_length.desc)`. Com mais de uma coluna dá para ter ordem diferente em cada uma. `limit(n)` devolve um DataFrame novo com as primeiras n linhas, e o uso comum é logo depois de uma ordenação, para pegar o topo ou o fundo.

**47.** What does `union` require of the two DataFrames, and what does `withColumn` do when the column name already exists? *(union; withColumn)*

R: O `union` exige que os dois DataFrames tenham o mesmo schema, com nomes de coluna e ordem batendo exatamente. O `withColumn` recebe nome de coluna e uma column expression. Se o nome dado já existe, aquela coluna é substituída pela expressão.

**48.** In which two situations is `withColumnRenamed` useful, and what happens when the given column does not exist? What about `drop`? *(withColumnRenamed; drop)*

R: Serve para renomear um nome críptico vindo de um schema que você não controla, por exemplo um Parquet produzido por um parceiro. E serve antes de um join entre dois DataFrames com nomes de coluna repetidos. Se o nome informado não existe no schema, o Spark não lança erro e silenciosamente não faz nada. O `drop` se comporta igual: descarta só as colunas que existem e ignora as outras em silêncio.

**49.** What are the three parameters of `sample`, and what does `randomSplit` return? *(sample; randomSplit)*

R: `sample` recebe fraction, um seed opcional e um withReplacement opcional. A fraction é um percentual entre 0 e 1 e o número de linhas devolvidas é aproximadamente igual a ela. O seed alimenta o gerador de números aleatórios, e sem seed um valor aleatório é usado. Com withReplacement true, uma mesma linha pode ser escolhida mais de uma vez. `randomSplit` devolve um ou mais DataFrames, um por peso informado, e normaliza os pesos se eles não somarem 1.

**50.** Which class handles missing data, how is it reached, and what are the three common ways of dealing with the problem? *(Working with Missing or Bad Data)*

R: A classe é `DataFrameNaFunctions`, disponível como member variable da classe DataFrame, alcançada por `df.na`. As três formas são estas. Descartar as linhas com valor faltando em uma ou mais colunas. Preencher os valores faltantes com valores fornecidos pelo usuário. E substituir o dado ruim por algo tratável.

**51.** What is the difference between `na.drop("any")`, `na.drop("all")` and `na.drop(Array("actor_name"))`? *(Listing 3-36)*

R: `na.drop()` e `na.drop("any")` são a mesma coisa e descartam a linha que tiver dado faltando em qualquer coluna. No exemplo do capítulo isso zera o DataFrame inteiro. `na.drop("all")` descarta só as linhas em que todas as colunas estão faltando, e sobram quatro das cinco. `na.drop(Array("actor_name"))` descarta as linhas em que aquela coluna específica está faltando, e sobram duas.

**52.** List the structured actions in Table 3-9 and what each one does. *(Table 3-9)*

R:

| Operação | O que faz |
|---|---|
| `show()`, `show(numRows)`, `show(truncate)`, `show(numRows, truncate)` | Exibe as linhas em formato tabular. Sem `numRows`, mostra as 20 primeiras. O `truncate` controla se uma coluna string é cortada quando passa de 20 caracteres |
| `head()`, `first()`, `head(n)`, `take(n)` | Devolve a primeira linha, ou as primeiras n linhas |
| `takeAsList(n)` | Devolve as primeiras n linhas como uma lista Java |
| `collect`, `collectAsList` | Devolve todas as linhas como array ou como lista Java |
| `count` | Devolve o número de linhas do DataFrame |
| `describe` | Calcula estatísticas comuns sobre colunas numéricas e string |

O aviso da tabela é para `takeAsList` e vale também para `collect`: não pegue linhas demais, porque isso pode causar out-of-memory no processo driver da aplicação.

**53.** Which statistics does `describe` compute, and what does the chapter's example show? *(describe; Listing 3-37)*

R: count, mean, standard deviation, mínimo, máximo e percentis aproximados arbitrários. No exemplo sobre `produced_year`, a saída traz count 31392, mean 2002.7964449541284, stddev 6.377236851493877, min 1961 e max 2012. Os percentis não aparecem na saída.

**54.** In which Spark version were the DataFrame and Dataset APIs unified, why, and what is a DataFrame from the code perspective after that? *(Introduction to Datasets)*

R: No Spark 2.0. A razão foi a confusão da comunidade sobre as diferenças entre as duas, e o objetivo foi ter uma abstração a menos para aprender. Depois da unificação existe uma abstração de alto nível só, chamada Dataset, com dois sabores, uma API strongly-typed e uma API untyped. O termo DataFrame não sumiu, virou alias: do ponto de vista do código, um DataFrame é um type alias para `Dataset[Row]`, onde `Row` é um objeto JVM genérico e untyped.

**55.** Give the Dataset flavors per language in Table 3-10, and explain why Python and R only get one of them. *(Table 3-10)*

R: Scala tem `Dataset[T]` e DataFrame. Java tem `Dataset[T]`. Python tem DataFrame. R tem DataFrame. Python e R não têm type-safety em tempo de compilação, então só as APIs untyped, ou seja, o DataFrame, são suportadas.

**56.** What are the two unique properties of a Dataset, what are encoders for, and what are the limitations? *(Introduction to Datasets)*

R: As duas propriedades são type safety e orientação a objeto. Cada linha é representada por um objeto definido pelo usuário, e a coluna vira uma member variable desse objeto, o que dá compile-time safety. Os encoders são utilitários que convertem o dado de cada objeto num formato binário compacto. Isso reduz o uso de memória quando o Dataset é cacheado. E reduz os bytes trafegados na rede durante o shuffle. São duas as limitações. As APIs de Dataset só existem em linguagens strongly typed como Scala e Java. E existe custo de conversão de um objeto `Row` para o objeto de domínio, que pesa quando o Dataset tem milhões de linhas.

**57.** What guideline does the chapter give for choosing between Dataset and DataFrame? *(Introduction to Datasets; Working with Datasets)*

R: As APIs de Dataset são boas para jobs de produção que rodam regularmente e são escritos e mantidos por um time de Data Engineers. Para a maior parte da análise interativa e exploratória, as APIs de DataFrame bastam. A diretriz geral é o desejo de ter um grau maior de type-safety em tempo de compilação, importante em jobs de ETL complexos mantidos por várias pessoas.

**58.** What is a Scala case class, according to the Note? *(Note, Introduction to Datasets)*

R: É como uma classe JavaBean do Java, com algumas propriedades embutidas. Uma instância de case class é imutável, o que a torna comum para modelar objetos de domínio e fácil de raciocinar sobre o estado interno. Os métodos `toString` e `equals` são gerados automaticamente. E case classes funcionam bem com o pattern matching do Scala.

**59.** What are the three ways to create a Dataset, which is the most popular, and what does Spark validate? *(Creating Datasets; Listing 3-38)*

R: Transformar um DataFrame em Dataset com a função `as` da classe DataFrame, como em `movies.as[Movie]`. Usar `SparkSession.createDataset()` sobre uma coleção de objetos. Ou usar a conversão implícita `toDS`. A primeira é a mais popular. Na conversão com uma case class, o Spark confere se os nomes das member variables batem com os nomes das colunas no schema do DataFrame, e avisa quando não batem.

**60.** What does Listing 3-39 prove about type safety? Give the two errors it triggers. *(Working with Datasets; Listing 3-39)*

R: Prova que erro de nome e erro de tipo aparecem em tempo de compilação no Dataset. `moviesDS.first.movie_tile` devolve `error: value movie_tile is not a member of Movie`. E `moviesDS.map(m => m.movie_title - m.movie_title)` devolve `error: value - is not a member of String`. O contraste é `movies.select('movie_title - 'movie_title)`, o mesmo absurdo no DataFrame, que só quebra em runtime.

**61.** Which use case is SQL in Spark designed for, and which not? Which SQL revision does Spark implement, and which benchmark follows from that? *(Using SQL in Spark SQL)*

R: É desenhado para OLAP, online analytical processing, e não para OLTP, online transaction processing. Ou seja, não serve para casos de baixa latência. O Spark implementa um subconjunto da revisão ANSI SQL:2003. Ser compatível com essa revisão permite que o engine seja medido pelo TPC-DS, um benchmark padrão de indústria para decision support.

**62.** What does the Note say SQL is, and what makes it different from Scala or Python? *(Note, Using SQL in Spark SQL)*

R: É uma domain-specific language para análise e manipulação de dados estruturados organizados em formato de tabela, com conceitos baseados em álgebra relacional. A diferença é que SQL é declarativa: você expressa o que quer fazer com o dado e deixa o engine descobrir como fazer e como otimizar.

**63.** What are the three options for running SQL in Spark, which two integrate Hive, and which one does the chapter cover? *(Running SQL in Spark)*

R: O Spark SQL CLI, em `./bin/spark-sql`. O JDBC/ODBC server. E a forma programática, dentro de aplicações Spark. As duas primeiras integram o Apache Hive para aproveitar o metastore dele, o repositório com metadados e schema das tabelas. O capítulo cobre só a terceira.

**64.** What must you do before issuing SQL against a DataFrame, and what are the two scoping levels? *(Running SQL in Spark)*

R: Registrá-lo como temporary view. Cada view tem um nome, que vira o nome de tabela na cláusula select. Os dois níveis de escopo são o de Spark session e o global. No de sessão, só as queries da mesma sessão enxergam a view, e ela desaparece quando a sessão fecha. No global, a view fica disponível para as SQL statements de todas as Spark sessions.

**65.** Which functions register the two kinds of view, and what do `spark.catalog.listTables` and `spark.catalog.listColumns` return? *(Listing 3-40)*

R: `movies.createOrReplaceTempView("movies")` para a de sessão e `movies.createOrReplaceGlobalTempView("movies_g")` para a global. Todas as views registradas ficam no metadata catalog do Spark, alcançável pela SparkSession. `listTables` devolve as colunas `name`, `database`, `description`, `tableType` e `isTemporary`, e a view aparece com tableType TEMPORARY e isTemporary true. `listColumns("movies")` devolve `name`, `description`, `dataType`, `nullable`, `isPartition` e `isBucket`.

**66.** Which function issues a SQL query, what does it return, and what prefix does a global view need? *(Listing 3-41)*

R: A função `sql` da classe SparkSession. Ela executa a query e devolve um DataFrame, o que permite encadear transformations e actions em cima do resultado. Uma global view precisa do prefixo `global_temp`, como em `select count(*) from global_temp.movies_g`.

**67.** How do you issue a SQL query directly against a data file, without registering a view? *(Listing 3-42)*

R: Nomeando o formato e o caminho em crases dentro do FROM, como em `spark.sql("SELECT * FROM parquet.`<path>/movies.parquet`")`.

**68.** Write the common interaction pattern with `DataFrameWriter`, and say what the input to `save` is. *(Writing Data Out to Storage Systems; Listing 3-44)*

R:

```scala
movies.write.format(...).mode(...).option(...).partitionBy(...).bucketBy(...).sortBy(...).save(...)
```

O formato padrão é Parquet, igual ao do reader, então `format` pode ser omitido quando a saída for Parquet. A entrada de `save` é o nome de um diretório, e não o nome de um arquivo.

*O final da linha acima é reconstruído, não lido.* Neste PDF a Listing 3-44 é cortada na margem em `...partitionBy(...).bucketBy(...).sor`. Completei com `.sortBy(...).save(...)` porque a prosa seguinte fala do argumento de `save`. É o mesmo defeito de produção que anotei no item 10 do Nível 4.

**69.** List the save modes in Table 3-11 and say which is the default. *(Table 3-11)*

R:

| Mode | O que faz |
|---|---|
| `append` | Acrescenta os dados do DataFrame aos arquivos que já existem no destino |
| `overwrite` | Sobrescreve por completo os arquivos que já existem no destino |
| `error`, `errorIfExists`, `default` | Se o destino existe, o `DataFrameWriter` lança erro. É o modo padrão |
| `ignore` | Se o destino existe, não faz nada e não escreve os dados em silêncio |

**70.** What determines the number of output files, how do you find out that number, and how do you force a single file? *(Listings 3-46, 3-47)*

R: O número de arquivos escritos no diretório de saída corresponde ao número de partitions do DataFrame. Para saber quantas são: `movies.rdd.getNumPartitions`, que no exemplo devolve 1. O truque para um arquivo só é reduzir as partitions a uma antes de escrever, com `movies.coalesce(1)`.

**71.** Where does the partitioning and bucketing idea come from, what is the rule of thumb, and what do the resulting directory names look like? *(Writing Data Out to Storage Systems; Listing 3-48)*

R: A ideia vem da comunidade de usuários do Apache Hive. A regra prática é que a coluna de partition tenha baixa cardinalidade, e no exemplo a escolhida é `produced_year`. Os diretórios recebem nomes no formato `produced_year=1961` até `produced_year=2012`, ou seja, nome da coluna mais o valor. Essas duas informações são usadas na leitura para escolher qual diretório ler, o que reduz muito o volume lido.

**72.** Fill in Table 3-12 for SQL, DataFrame and Dataset. *(Table 3-12)*

R:

| | SQL | DataFrame | Dataset |
|---|---|---|---|
| System Errors | Runtime | Compile time | Compile time |
| Analysis Errors | Runtime | Runtime | Compile time |

O comentário do capítulo é que quanto mais cedo o erro aparece, mais produtiva a pessoa é e mais estável fica a aplicação. Sobre o rótulo "System Errors", ver o item 2 do Nível 4.

**73.** Which persistence APIs exist on the DataFrame class, and why does a cached DataFrame take less space than a cached RDD over the same file? *(DataFrame Persistence)*

R: As mesmas do RDD, `persist` e `unpersist`. O DataFrame ocupa menos porque o Spark SQL conhece o schema do dado, então organiza a memória em formato colunar e aplica as compressões aplicáveis para minimizar o espaço.

**74.** How does Listing 3-49 cache a DataFrame with a human-readable name, and what forces the persistence to happen? *(Listing 3-49)*

R: Registrando o DataFrame como view com `numDF.createOrReplaceTempView("num_df")` e depois chamando `spark.catalog.cacheTable("num_df")`. O que força a persistência é uma action, no exemplo `numDF.count`. Sem a action nada é materializado, porque o cache é lazy.

**75.** What exactly does Figure 3-2 show? Give the columns and the row. *(Figure 3-2)*

R: Abri a página 40. É a aba Storage do Spark UI da versão 3.1.1. A barra de navegação traz Jobs, Stages, Storage, Environment, Executors e SQL, com o rótulo `Spark shell application UI` à direita. A seção se chama RDDs. As colunas da tabela são ID, RDD Name, Storage Level, Cached Partitions, Fraction Cached, Size in Memory e Size on Disk. A única linha traz estes valores:

| Coluna | Valor |
|---|---|
| ID | 4 |
| RDD Name | `In-memory table num_df` |
| Storage Level | `Disk Memory Deserialized 1x Replicated` |
| Cached Partitions | 12 |
| Fraction Cached | 100% |
| Size in Memory | 4.1 KiB |
| Size on Disk | 0.0 B |

**76.** What files do the exercises use, what is the delimiter, and what are the four tasks? *(Spark SQL Exercises)*

R: Os arquivos `movies.tsv` e `movie-ratings.tsv`, no diretório `chapter3/data/movies`, com tab como delimitador. Cada linha de `movies.tsv` representa um ator em um filme, então um filme com dez atores gera dez linhas. Os quatro exercícios são: contar filmes por ano, ordenado por count descendente. Contar filmes por ator, ordenado por count descendente. Achar o filme mais bem avaliado por ano com a lista de atores, o que exige um join entre os dois arquivos. E achar o par de atores que mais trabalhou junto, o que exige um self-join.

---

## Nível 2 — Compreensão

Explique com suas próprias palavras. Três a cinco frases cada.

**1.** Explain why the RDD's flexibility blocks optimization, using predicate pushdown as the example. *(Understanding RDD)*

R: No RDD, a lógica chega ao Spark como uma função opaca. O Spark sabe que precisa aplicar aquela função a cada linha de cada partition, mas não sabe o que a função faz. Se a função é um filtro por ano, o Spark não tem como saber disso e não tem como pedir à origem que devolva menos dados. Com Structured APIs a intenção é declarada, `filter('produced_year === 2000)` é uma expressão que o Spark consegue ler. Aí ele pode empurrar a condição para o Parquet ou para o RDBMS, e o dado nem chega a ser lido.

**2.** Explain what the schema buys Spark, beyond documenting the data. *(Chapter intro; DataFrame Persistence)*

R: O schema é a segunda das duas informações que o capítulo diz serem necessárias para otimizar. Ele diz quais colunas existem e de que tipo elas são, antes de qualquer dado ser lido. Com isso o Spark valida expressões, poda colunas que a saída não usa e escolhe representações internas por tipo. O exemplo mais concreto do capítulo é a persistência: como o Spark SQL conhece o schema, ele guarda o DataFrame em formato colunar comprimido, e isso ocupa menos que o mesmo dado como RDD.

**3.** Explain what "lazily evaluated" means in practice for a chain like `movies.filter(...).select(...).show(5)`. *(Introduction to the DataFrame API; Working with Structured Actions)*

R: `filter` e `select` são transformations, então nenhuma das duas lê um byte quando é escrita. Cada uma só devolve um DataFrame novo que descreve a intenção. O `show(5)` é uma action, e é ele que dispara a computação de todas as transformations que levam até ali. A consequência prática é que erros de execução aparecem na linha do `show`, e não na linha que os causou, como acontece no exemplo de `failFast` da Listing 3-15.

**4.** The chapter insists a DataFrame is immutable. Explain what `union` and `withColumn` actually do, given that. *(Table 3-7; union; withColumn)*

R: Nenhuma das duas modifica o DataFrame original. `union` monta um DataFrame novo com as linhas dos dois de entrada, e o original continua com as linhas que tinha. `withColumn` monta um DataFrame novo com a coluna a mais, ou com a coluna substituída quando o nome colide. É por isso que a Listing 3-30 precisa atribuir o resultado a `completeShortNameMovieDF`, em vez de esperar que `shortNameMovieDF` cresça sozinho. Imutabilidade aqui é a mesma ideia de um `val` em Scala, só que aplicada à coleção distribuída.

**5.** Explain when you must pass a `Column` instance instead of a string, using `kvDF.select('key, 'key > 1)` as the case. *(Working with Columns; Listing 3-21)*

R: Uma string só nomeia uma coluna. Ela não sabe fazer nada com ela. `'key > 1` não é o nome de nada, é uma expressão que compara o valor da coluna com um literal e produz uma coluna booleana nova. Comparação é uma das três categorias de funcionalidade da classe `Column`, então só uma instância de `Column` consegue expressá-la. Se a mesma coisa fosse tentada com string, o resultado seria erro de tipo, porque `String` não tem o operador `>` para colunas.

**6.** Explain the trade-off between `inferSchema=true` and a hand-written schema, using the CSV `year` column from Listing 3-12. *(Creating a DataFrame by Reading CSV Files)*

R: Sem `inferSchema`, o Spark lê `year` como string, mesmo o arquivo tendo só números. Com `inferSchema=true`, ele vira integer, e comparações numéricas passam a funcionar. O custo é que a inferência exige uma passada extra sobre o dado, e o tipo depende do conteúdo daquele arquivo específico. Com schema escrito à mão, o tipo é o que você declarou e não muda com o arquivo, e o exemplo mostra que dá até para trocar os nomes de coluna do header. Inferência serve para explorar, schema explícito serve para pipeline que roda todo dia.

**7.** Explain why `samplingRatio` exists for JSON and what you give up by lowering it. *(Creating a DataFrame by Reading JSON Files)*

R: Um arquivo JSON traz nome de coluna, mas não traz tipo. Para descobrir o tipo, o Spark precisa olhar registros de verdade. O `samplingRatio` diz que fração dos registros ele vai olhar, e o padrão 1.0 significa o arquivo inteiro, o que fica caro em arquivos grandes. Baixar o valor acelera a carga porque o Spark lê menos para decidir. O que se perde é garantia: uma coluna que só tem valor não numérico no fim do arquivo pode ser tipada errado, e o erro só aparece na leitura de verdade.

**8.** Explain why Parquet is the default format, comparing it with CSV and JSON on the axes the chapter names. *(Creating a DataFrame by Reading Parquet Files)*

R: O capítulo compara em três eixos. Tamanho: o Parquet do exemplo tem cerca de um sexto do CSV equivalente, porque comprime e guarda de forma compacta. Schema: o Parquet é self-describing, então não precisa de `inferSchema` nem de schema à mão, enquanto CSV precisa dos dois e JSON precisa de amostragem. Velocidade de leitura analítica: sendo colunar, ele evita ler as colunas que a análise não usa, e o Spark ainda descomprime e decodifica em lotes de coluna. CSV e JSON ganham em uma coisa só, que é serem legíveis por humanos, e o capítulo os recomenda apenas para arquivos pequenos.

**9.** Explain what the chapter means by "the metadata catalog", and how a temporary view relates to a DataFrame variable. *(Running SQL in Spark; Listing 3-40)*

R: O catalog é onde o Spark guarda os metadados das tabelas e views que a sessão enxerga, e ele é alcançado por `spark.catalog`. Um DataFrame é uma variável do programa, e SQL não enxerga variáveis do programa. Registrar uma temporary view dá um nome ao DataFrame dentro do catalog, e é esse nome que a cláusula FROM usa. A prova disso é a Listing 3-40: `listTables` devolve vazio antes do registro e devolve a linha `movies` depois. A view não copia dado, ela só nomeia o plano.

**10.** Explain Table 3-12 line by line: why SQL catches nothing at compile time, why DataFrame catches one kind and not the other, and why Dataset catches both. *(The Trio: DataFrame, Dataset, and SQL)*

R: Uma query SQL é uma string. O compilador Scala não olha dentro de string, então nem erro de sintaxe nem coluna inexistente são pegos antes de rodar. Um DataFrame é código Scala, então a sintaxe é conferida pelo compilador. Mas o nome da coluna ainda é string ou symbol, e uma coluna inexistente só aparece na análise, em runtime. Um Dataset representa a linha por um objeto de domínio, então a coluna é uma member variable e o compilador confere o nome e o tipo dela. É exatamente o que a Listing 3-39 demonstra com `movie_tile` e com a subtração de strings.

**11.** Explain the relationship between DataFrame partitions and output files, and why `coalesce(1)` is called a trick. *(Listings 3-46, 3-47)*

R: Cada partition é escrita de forma independente, por uma task, então o diretório de saída recebe um arquivo por partition. Se o DataFrame tem oito partitions, saem oito arquivos, e nenhum deles é o dataset inteiro. `coalesce(1)` reduz tudo a uma partition, e aí sai um arquivo só. O capítulo chama isso de truque com razão: forçar uma partition tira o paralelismo da escrita e concentra o dado em uma task, o que só é aceitável quando o resultado é pequeno. O próprio capítulo condiciona a receita ao caso em que o número de linhas não é grande.

**12.** Explain why the chapter treats `select`, `selectExpr` and `spark.sql` as three ways of doing the same projection, and what actually distinguishes them. *(selectExpr; Running SQL in Spark)*

R: Os três produzem a mesma coisa: um DataFrame com as colunas escolhidas, possivelmente transformadas. A diferença é onde a expressão é escrita. Em `select` ela é código Scala com objetos `Column`. Em `selectExpr` ela é uma string com expressão SQL, que o Spark converte numa árvore lógica. Em `spark.sql` a query inteira é string e exige uma view registrada. A Listing 3-23 e a Listing 3-24 calculam a mesma década de duas dessas formas, e a coluna calculada é a mesma porque tudo cai no mesmo plano. Os DataFrames não são idênticos: a 3-23 projeta duas colunas e a 3-24 projeta quatro.

---

## Nível 3 — Aplicação e transferência

Faça no terminal, com dados seus. Não use o `movies` do livro, para não decorar a resposta.

**1.** You get server access logs as a text file with `|` as the delimiter and no header line. Write the read call with an explicit schema, and say which Table 3-4 options you need and which you must not use.

R: Uso o formato CSV, porque o parser aceita qualquer delimitador:

```scala
import org.apache.spark.sql.types._
val logSchema = StructType(Array(
  StructField("ts", TimestampType, true),
  StructField("ip", StringType, true),
  StructField("path", StringType, true),
  StructField("status", IntegerType, true)
))
val logs = spark.read.option("sep", "|").schema(logSchema).csv("<path>/access")
```

Preciso de `sep` com o valor `|`. Não uso `header`, porque o padrão já é false e não existe linha de cabeçalho. Não uso `inferSchema`, porque forneci o schema, e o capítulo diz que schema explícito prevalece sobre o header. Sem o schema, todas as colunas viriam como string, inclusive `status`.

**2.** You have sensor readings in JSON where each object spans several lines, and a few records are broken. Show the read call and the two behaviors you can choose between.

R:

```scala
val readings = spark.read.option("multiLine", "true").json("<path>/readings")
val strict   = spark.read.option("multiLine", "true").option("mode", "failFast").json("<path>/readings")
```

O `multiLine` é obrigatório aqui, porque o padrão é false e o Spark esperaria um objeto por linha. Os dois comportamentos são: o padrão, em que o capítulo diz que o Spark preenche com null e o job continua, e o `failFast`, em que ele lança `RuntimeException`. A pegadinha é que o erro só aparece quando uma action roda, e não na chamada de leitura. Para pipeline agendado eu escolheria falhar, porque uma tabela cheia de null passa despercebida e um job que quebra não passa. O item 5 do Nível 5 mostra que a descrição do capítulo sobre o modo padrão está incompleta.

**3.** You need the orders of one month from an e-commerce RDBMS. Write the JDBC read and say exactly where the filter runs.

R:

```scala
val orders = spark.read.format("jdbc")
  .option("driver", "<driver class>")
  .option("url", "<jdbc url>")
  .option("dbtable", "orders")
  .option("user", "<user>")
  .option("password", "<password>")
  .load()
  .where('order_date >= "2026-07-01" && 'order_date < "2026-08-01")
```

Pelo que o capítulo diz sobre predicate pushdown, o filtro não roda no Spark. Ele é empurrado para o RDBMS, que devolve só as linhas de julho. Sem isso a tabela inteira atravessaria a rede para ser descartada depois. O que o capítulo não me deu é como conferir se o pushdown de fato aconteceu, e ele adia o exemplo para o Capítulo 4.

**4.** Take an open dataset of city bike trips. Produce trips per station, ordered descending, in three ways: DataFrame operations, `selectExpr`, and SQL.

R:

```scala
// 1. operações de DataFrame
trips.groupBy('start_station).count.orderBy('count.desc).show(10)

// 2. selectExpr sobre o resultado agregado
trips.groupBy('start_station).count
     .selectExpr("start_station as station", "count as trips")
     .orderBy('trips.desc).show(10)

// 3. SQL
trips.createOrReplaceTempView("trips")
spark.sql("""select start_station as station, count(*) as trips
             from trips group by start_station""")
     .orderBy('trips.desc).show(10)
```

O terceiro caminho mostra o ponto da Listing 3-41: `spark.sql` devolve um DataFrame, então dá para continuar com `orderBy` em vez de colocar tudo dentro da string. O `groupBy` é adiado pelo capítulo para o próximo capítulo, então esta questão já usa uma peça que ele só nomeia.

**5.** A partner hands you a Parquet with 120 columns and cryptic names. You need six of them, with readable names. Which transformations do you use and why not the others?

R: Uso `select` para as seis colunas e `withColumnRenamed` para os nomes. Não uso `drop`, porque descartar 114 colunas nomeando cada uma é pior que projetar seis. Não uso `select` sozinho para renomear, embora dê, porque `withColumnRenamed` deixa a intenção explícita e é exatamente o primeiro caso de uso que o capítulo lista para ele, o nome críptico vindo de schema alheio.

```scala
val slim = raw.select("c_001","c_007","c_013","c_042","c_088","c_101")
  .withColumnRenamed("c_001", "order_id")
  .withColumnRenamed("c_007", "customer_id")
  // ... e assim por diante
```

O risco que anoto: se eu errar um nome de origem, `withColumnRenamed` não reclama e a coluna continua com o nome críptico. Preciso conferir o `printSchema` no fim.

**6.** Split a labeled dataset into train, validation and test at 70/15/15, and prove the counts add up.

R:

```scala
val Array(train, valid, test) = labeled.randomSplit(Array(0.7, 0.15, 0.15), 42L)
labeled.count
train.count + valid.count + test.count
```

Os pesos já somam 1, então não há normalização. Passo o seed para o corte ser reproduzível entre execuções, coisa que o capítulo mostra para `sample` e não mostra para `randomSplit`, embora a assinatura aceite. A conferência é a mesma da Listing 3-35: a soma das três contagens tem de bater com a contagem do original.

**7.** Write a sensor DataFrame out so that reading one single day is cheap. Show the call, describe the output directory, and say what breaks if you partition by sensor id instead.

R:

```scala
readings.write.mode("overwrite").partitionBy("day").save("/tmp/output/readings")
```

O diretório de saída ganha um subdiretório por dia, nomeado `day=2026-08-01` e assim por diante. Na leitura, o Spark usa o nome do diretório para pular os dias que a query não pede. Particionar por sensor id quebra a regra prática do capítulo, que pede coluna de baixa cardinalidade. Com milhares de sensores eu criaria milhares de diretórios, cada um com pouquíssimo dado, e o custo de listar e abrir arquivos passa a dominar o ganho.

**8.** Convert a movie catalog DataFrame into a Dataset with a case class, then show one operation that fails at compile time and its DataFrame twin that fails at runtime.

R:

```scala
case class Film(title: String, year: Long, rating: Double)
val filmsDS = films.as[Film]

// falha na compilação
filmsDS.map(f => f.titel)         // error: value titel is not a member of Film

// mesmo erro, só que em runtime
films.select('titel)              // AnalysisException, na hora de executar
```

A conversão com `as[Film]` só passa se os nomes das member variables baterem com os nomes das colunas do schema. É a validação que o capítulo descreve na seção Creating Datasets, e ela é o preço de entrada para ter compile-time safety depois.

**9.** You have a catalog DataFrame where some rows have no identifier at all. Drop only the rows that are entirely empty, then drop the rows without identifier. Write the calls and say what the difference is.

R:

```scala
val notEmpty = catalog.na.drop("all")
val usable   = notEmpty.na.drop(Array("item_id"))
```

`drop("all")` só descarta a linha em que todas as colunas estão nulas, então ele é conservador. `drop(Array("item_id"))` olha uma coluna só e descarta a linha que não tem identificador, mesmo que o resto esteja preenchido. O que eu evito é `drop("any")`, que descartaria qualquer linha com um null em qualquer coluna, e no exemplo do capítulo isso zerou o DataFrame inteiro.

**10.** Write a report out as a single CSV file with `;` as the delimiter, overwriting whatever is there. Write the call and name the three things that could still go wrong.

R:

```scala
report.coalesce(1).write.format("csv").mode("overwrite").option("sep", ";").option("header", "true").save("/tmp/output/report")
```

Os três riscos. Primeiro, `save` recebe um diretório, então o resultado é `/tmp/output/report/part-00000-....csv` e não um arquivo com o nome que eu quero. Segundo, `overwrite` apaga o que estava lá, e se o caminho estiver errado eu destruo outra coisa. Terceiro, `coalesce(1)` só é seguro se o relatório for pequeno, porque uma task passa a carregar tudo. O `header` não está na receita do capítulo, mas sem ele o CSV sai sem nomes de coluna, e a Table 3-4 diz que o padrão é false.

---

## Nível 4 — Análise e síntese

Este é o nível mais longo do guia, e a razão é o próprio capítulo. Ele tem quarenta e nove listings, doze tabelas e um número incomum de erros internos, referências cruzadas quebradas e promessas não cumpridas. As questões aqui são sobre ler os artefatos como evidência.

**1.** **O alias circular.** Table 3-9 says "first is an alias for first. take(n) is an alias for first(n)." Reconstruct the alias graph that the table meant to describe, and explain how a defect like this survives.

R: Abri a página 30 para conferir que não era artefato de extração. Está escrito assim na tabela mesmo. A frase é circular e não informa nada: `first` é alias de `first`.

O grafo que a tabela queria descrever é este. A linha agrupa quatro operações, `head()`, `first()`, `head(n)` e `take(n)`, e a descrição diz que elas devolvem a primeira linha, ou as primeiras n linhas. A leitura que fecha é: `first()` é alias de `head()`, e `take(n)` é alias de `head(n)`. Ou seja, `head` é a operação primária e as outras duas são apelidos dela. Foi trocado um `head` por um `first` nas duas frases.

Sobre a sobrevivência do defeito, a resposta é o formato. A frase é gramaticalmente perfeita e só é absurda para quem lê o conteúdo. Revisão de texto não pega, porque não há erro de língua. Revisão técnica pegaria, mas exigiria ler uma tabela de referência linha a linha, que é justamente o tipo de conteúdo que se revisa por amostragem. É o mesmo motivo pelo qual `java.math.BigDecial` sobreviveu na Table 3-1.

**2.** **A tabela que discorda da própria legenda.** Table 3-12 is captioned "Syntax and Analysis Errors Spectrum", but the first row is labeled "System Errors". Decide which label is correct, and define both rows.

R: Abri a página 39 e a discordância está lá: legenda com "Syntax", linha com "System". O rótulo correto é **Syntax Errors**, por três razões.

A legenda é o texto que o autor escreveu deliberadamente para descrever a tabela. A tabela tem duas linhas e a legenda anuncia dois nomes, "Syntax" e "Analysis", então o pareamento é direto. E "System Errors" não é um conceito que o capítulo defina em lugar nenhum, enquanto a distinção entre erro de sintaxe e erro de análise é padrão em qualquer engine de query.

As duas linhas, definidas: um **syntax error** é código malformado, uma expressão que o parser não consegue ler. Um **analysis error** é código bem formado que se refere a algo que não existe ou não tem o tipo certo, como uma coluna ausente. A tabela diz que o DataFrame pega sintaxe na compilação e análise só em runtime, e é exatamente essa a assimetria que a Listing 3-39 demonstra com `movies.select('movie_title - 'movie_title)`.

**3.** **A referência cruzada apontando para o lugar errado.** The DataFrame Persistence section says all the storage options "described in Table 3-5" apply to persisting a DataFrame. Open Table 3-5. Where should the reference point?

R: A Table 3-5 é "JSON Common Options", com `allowComments`, `multiLine` e `samplingRatio`. Nada ali tem qualquer relação com persistência.

A referência deveria apontar para uma tabela de storage levels, do tipo `MEMORY_ONLY`, `MEMORY_AND_DISK` e `DISK_ONLY`. Essa tabela não existe neste capítulo. Ela existe no RDD programming guide da documentação do Spark, e conferi o conteúdo dela no item 9 do Nível 5.

A conclusão é mais grave que um número trocado. A frase promete ao leitor uma lista de opções de armazenamento e o manda para uma tabela que não a contém, dentro do mesmo capítulo. Quem seguir a referência vai concluir que entendeu errado a frase, e não que o livro errou. E o capítulo nunca lista os storage levels em lugar nenhum. O assunto não é ensinado, embora seja apresentado como se tivesse sido.

**4.** **A união dupla.** Listing 3-30 calls `union` twice but shows five rows. Trace the arithmetic and give the output the code would actually produce.

R: O `shortNameMovieDF` tem quatro linhas, os quatro atores do filme "12". O `forgottenActorDF` tem uma linha, a atriz esquecida.

A primeira linha de código é `val completeShortNameMovieDF = shortNameMovieDF.union(forgottenActorDF)`, que produz cinco linhas. A segunda é `completeShortNameMovieDF.union(forgottenActorDF).show`, que une a atriz de novo ao resultado que já a contém. A saída real teria **seis linhas**, com `Brychta, Edita` aparecendo duas vezes. A saída impressa no livro tem cinco linhas e uma Brychta só.

Ou seja, a saída corresponde a `completeShortNameMovieDF.show`, e o segundo `union` foi digitado por engano na linha do `show`. É um erro que ensina exatamente o oposto do que a seção quer ensinar: a seção existe para mostrar que `union` não muda o original, e a saída publicada esconde que a segunda união aconteceu. Quem rodar o código em casa vai ver seis linhas e achar que errou.

**5.** **A figura que contradiz o sumário.** Figure 3-1 draws the Spark SQL module. The chapter's own Summary says the main programming abstraction in Spark SQL is the Dataset. Reconcile.

R: Abri a página 1. A Figure 3-1 tem, dentro da caixa Spark SQL, exatamente duas caixas: `DataFrame API` e `Catalyst Optimizer`. Não há caixa de Dataset e não há caixa de SQL.

Isso contradiz duas afirmações do próprio capítulo. O Summary diz que a abstração principal do Spark SQL é o Dataset, com dois sabores de API. E o título do capítulo é "Spark SQL: Foundation", com uma seção inteira chamada "Using SQL in Spark SQL", que a figura não representa.

A explicação mais provável é datação. A figura descreve o estado do módulo antes da unificação do Spark 2.0, quando DataFrame e Catalyst eram de fato as duas peças. Depois da unificação o desenho ficou incompleto e não foi refeito. É a mesma causa que aparece nos itens 8 e 9 deste nível.

O que a figura acerta e que vale guardar: a relação de camadas. Spark SQL fica em cima do Spark Core, e o shell e as aplicações entram pelo Spark SQL. O ponto do capítulo é que essa arquitetura em camadas permite herdar qualquer melhoria feita no Core.

**6.** **A afirmação sobre o Parquet.** The chapter states that Parquet "stores each column's data in a separate file". Test that claim against the chapter's own Listing 3-16.

R: A afirmação não se sustenta contra o próprio capítulo. A Listing 3-16 lê `movies.parquet` como um caminho único, com `spark.read.load(...)` e com `spark.read.parquet(...)`. Se cada coluna morasse em um arquivo separado, o caminho apontaria para um diretório com três arquivos, um por coluna, e a leitura precisaria remontá-los. O capítulo trata o Parquet como arquivo único a leitura inteira.

Além disso, a comparação de tamanho do próprio capítulo fala de "o arquivo `movies.parquet`" no singular, com cerca de um sexto do tamanho do `movies.csv`. Um formato que quebrasse por coluna teria três arquivos para comparar, não um.

O que o autor quis dizer é que o Parquet separa os dados **por coluna dentro do arquivo**, de forma que uma leitura pode pular as colunas que não interessam. A conclusão de que colunas não usadas não são lidas está certa. O mecanismo descrito está errado. Confirmei a estrutura real contra a especificação do Parquet no item 10 do Nível 5.

**7.** **Duas contagens do mesmo dataset.** Listing 3-35 reports `movies.count` as 31393. Listing 3-37 reports `describe("produced_year")` with count 31392. Explain the difference.

R: A diferença de um não é erro. `count` conta linhas do DataFrame. O `count` que o `describe` reporta é o count da coluna, e count de coluna ignora null. Logo, exatamente uma linha do dataset tem `produced_year` nulo.

Isso é conferível dentro do próprio capítulo, porque o schema do `movies` declara `produced_year` como `nullable = true` em todas as vezes que aparece.

O que eu levo disso é uma regra de leitura de `describe`: a linha `count` da saída não é o tamanho do dataset, é a completude daquela coluna. A diferença entre as duas é a contagem de nulos, de graça. O capítulo não comenta a discrepância, embora ela esteja a sete listings de distância, e essa é a informação mais útil que o `describe` oferece nesta página.

**8.** **O caminho que denuncia a edição anterior.** Every data path in the chapter says `chapter4/data/movies`, but the exercises say `chapter3/data/movies`. Find the second piece of evidence that dates the listings, and state what happened.

R: A segunda evidência está na Listing 3-41. A primeira query é `select current_date() as today, 1 + 100 as value`, e a saída impressa é **2017-12-27**. O `current_date()` devolve a data da máquina no momento da execução, então esse listing foi capturado em 27 de dezembro de 2017.

O livro é de 2021 e ensina o Spark 3.1.1. Um listing de dezembro de 2017 não pode ter saído dessa versão. O que aconteceu é que este capítulo veio da edição anterior, *Beginning Apache Spark 2*, de 2018, onde o mesmo conteúdo era o Capítulo 4. Daí os caminhos `chapter4/`. Os exercícios foram reescritos para a numeração nova e apontam para `chapter3/`, o texto e os listings não foram.

A consequência prática para quem estuda: os caminhos do livro não correspondem ao repositório de código dele, e as saídas impressas são de uma versão do Spark quatro releases de feature atrás, e uma versão maior de distância. A data 27/12/2017 corresponde ao Spark 2.2.1, e o livro ensina a 3.1.1. Entre as duas passaram 2.3, 2.4, 3.0 e 3.1. É o mesmo defeito que anotei no capítulo 2, na Figure 2-19, onde o dump de configuração era de uma instalação 2.1.1. Lá era uma captura isolada. Aqui é o capítulo inteiro.

**9.** **A variável que não é impressa.** Listings 3-13 and 3-14 create `movies4` and `movies5`, then call `movies.printSchema`. Say what the reader actually sees, and whether the demonstration still works.

R: Em ambos os casos o que é impresso é o schema de `movies`, uma variável criada páginas antes, e não o schema do DataFrame que o listing acabou de construir.

Na Listing 3-13 isso é grave. O ponto do listing é mostrar que um TSV lido com `option("sep", "\t")` produz o schema certo. O que é impresso não prova nada sobre o TSV, prova algo sobre um CSV lido antes. Pior: o schema impresso traz `actor_name`, `movie_title` e `produced_year`, que são os nomes do `movieSchema`, e `movies` na Listing 3-12 foi lido com os nomes do header, `actor`, `title` e `year`. Nem a variável errada bate.

Na Listing 3-14 o efeito é o mesmo, mas o dano é menor, porque a segunda metade do listing usa `movies6.printSchema` corretamente e é ela que carrega o argumento sobre schema explícito.

A demonstração não funciona como publicada. Ela funciona como intenção, porque o leitor entende o que deveria ter sido impresso. O padrão é o mesmo dos itens 4 e 8: código digitado por cima de uma versão anterior e saída colada sem reexecução.

**10.** **O listing cortado.** Listing 3-1 shows `Random.nextInt(100` and the code line is clipped at the page margin. Listing 3-2 shows values like 237, 567 and 360. Deduce the real argument and say what this class of defect costs.

R: Abri a página 4. A linha de código sai da margem direita da caixa de código e termina visualmente em `Random.nextInt(100`, sem o parêntese de fechamento.

`Random.nextInt(n)` devolve um inteiro entre 0 inclusive e n exclusive. A Listing 3-2 mostra 237, 567, 360, 288 e 260, todos acima de 100. Logo o argumento não é 100. O menor valor coerente com todos os números impressos e com o prefixo `100` visível é **1000**. A Listing 3-3, que é uma execução diferente, mostra 280, o que confirma o teto acima de 100.

O custo desse defeito é diferente dos anteriores. Não é um erro do autor, é um erro de produção: o PDF corta a linha longa em vez de quebrá-la. E ele atinge o capítulo inteiro, não só este listing. Todos os listings com linha longa estão truncados do mesmo jeito. Na Listing 3-4 o corte pega a linha do `parallelize`, que termina em `Row(1L, "John Doe", 30L),Ro`, enquanto o `StructType` logo abaixo aparece inteiro em cinco linhas. Os outros dois casos são o `movieSchema` da Listing 3-12 e a chamada JDBC da Listing 3-20. Quem estuda por este PDF não consegue copiar nenhum exemplo multicoluna sem reconstruí-lo. É a razão pela qual o Nível 3 deste guia pede código escrito do zero, e não código copiado.

**11.** **`filler`.** One section heading reads "filler(condition), where(condition)". List the other defects of the same class in this chapter, and say what they have in common.

R: O heading correto é `filter(condition), where(condition)`. A seção inteira fala de `filter`, e a Table 3-7 lista `filter`.

Os outros defeitos da mesma classe que encontrei:

- `java.math.BigDecial` na Table 3-1, por `BigDecimal`.
- "its megastore" na seção Running SQL in Spark, por "metastore".
- "the as function is renames it", na discussão da Listing 3-23.
- "the reach row is represented by a Row object", no Summary, por "each row".
- "Listing 3-20 shows an example of using the limit transformation", na seção `limit`, quando o listing é o 3-29. O 3-20 é a leitura do MySQL.
- "More on these three pieces of information is discussed in later in the chapter".
- "// write data out in CVS format" no comentário da Listing 3-45, por CSV.

O que eles têm em comum é serem invisíveis para corretor ortográfico e para leitura fluente. `filler`, `BigDecial` e `megastore` são palavras plausíveis em contexto técnico. A troca de 3-29 por 3-20 é um número, e número não tem ortografia. Somados aos itens 1, 4 e 9 deste nível, o retrato é de um capítulo que passou por revisão de língua e não por revisão técnica linha a linha.

**12.** **A promessa do writer.** Listing 3-44 shows `partitionBy(...).bucketBy(...).sortBy(...)` and the text says you learn more about this "later in the chapter". Audit what the chapter delivers.

R: Busquei as duas palavras no texto inteiro. `bucketBy` aparece duas vezes: uma na Listing 3-44 e outra na frase que diz que `partitionBy`, `bucketBy` e `sortBy` controlam a estrutura de diretórios. `sortBy` aparece uma vez só, na mesma frase. Nenhuma das duas ganha seção, exemplo ou explicação.

Só `partitionBy` é cumprido, na Listing 3-48. A promessa "later in the chapter" entrega um terço do que anunciou.

O prejuízo é maior do que parece, porque a frase que sobra ensina errado. Ela diz que as três funções controlam a estrutura de diretórios da saída. Isso é verdade para `partitionBy` e é falso para `bucketBy`, que distribui as linhas por um número fixo de buckets e não cria diretório nomeado. Conferi essa diferença na documentação atual, no item 6 do Nível 5, e lá está registrado também que `bucketBy` nem funciona com `save`, só com `saveAsTable`. Ou seja, o padrão de encadeamento da Listing 3-44 não roda como escrito se `bucketBy` for usado. O final desse encadeamento é reconstruído e não lido, porque o PDF corta a linha em `.sor`, conforme registrei na questão 68 do Nível 1.

É a segunda promessa quebrada do capítulo. A primeira é o Catalyst, que a Figure 3-1 desenha, o texto de abertura nomeia como uma das duas metades do módulo, e o capítulo adia inteiro para o Capítulo 4.

**13.** **Schema opcional?** Table 3-2 lists `schema` as optional. Table 3-4 says that with `inferSchema` false and no schema, every column is a string. Reconcile the two, and decide when the schema is genuinely optional.

R: As duas estão certas e a palavra "opcional" está sendo usada em dois sentidos.

Na Table 3-2, opcional significa que a chamada compila e executa sem o `schema`. Isso é verdade sempre.

Na Table 3-4, o que está descrito é o resultado de exercer essa opção: um DataFrame em que tudo é string. Ele existe, mas não é utilizável para nada numérico. Comparar `'year > 2000` sobre uma coluna string não faz o que se espera.

A reconciliação que eu faço é por origem de dado, e o próprio capítulo me dá o critério. O schema é **genuinamente** opcional quando o formato é self-describing, ou seja, Parquet e ORC, e a Table 3-2 diz isso com todas as letras. É **funcionalmente obrigatório** em CSV, porque sem ele ou sem `inferSchema` nada tem tipo. E é **negociável** em JSON, porque o JSON traz nome mas não traz tipo, então o Spark infere por amostragem, e aí a escolha é entre custo de amostragem e controle.

O que fica de fora da tabela: `inferSchema` é uma terceira via, nem schema explícito nem string. O capítulo mostra isso na Listing 3-12 e não o conecta com a Table 3-2, que só oferece duas opções.

**14.** **Duas formas de somar uma coluna.** `withColumn` and `selectExpr` both add a column, and the chapter says they accomplish "pretty much the same goal". Find where they diverge.

R: Divergem em quatro pontos, e a Listing 3-24 e a Listing 3-31 mostram os dois lados.

Colunas preservadas: `withColumn` mantém tudo e acrescenta uma. `selectExpr` só devolve o que foi listado, por isso a Listing 3-24 precisa do `"*"` explícito. Esquecer o asterisco descarta o DataFrame inteiro.

Colisão de nome: `withColumn` com nome existente substitui a coluna, comportamento documentado e usado na segunda metade da Listing 3-31. Em `selectExpr`, duas expressões com o mesmo alias produzem duas colunas homônimas, e não uma substituição.

Forma da expressão: `withColumn` recebe um objeto `Column`, então a expressão é código Scala conferido pelo compilador. `selectExpr` recebe string SQL, que só é analisada em runtime. Pela Table 3-12, isso move o erro de compile time para runtime.

Quantidade: `selectExpr` acrescenta várias colunas de uma vez. `withColumn` acrescenta uma por chamada, e n colunas exigem n chamadas encadeadas.

A frase "pretty much the same goal" é verdadeira sobre o resultado e enganosa sobre o caminho. Para pipeline eu prefiro `withColumn`, porque o compilador confere a expressão.

**15.** **O que o capítulo adia.** List everything this chapter names and postpones. Judge whether the deferral is honest.

R: O que é nomeado e adiado:

- **Catalyst optimizer.** Anunciado na abertura como uma das duas metades do Spark SQL, desenhado na Figure 3-1, e adiado para a seção "Catalyst Optimizer" do Capítulo 4.
- **Predicate pushdown.** Definido em uma frase e o exemplo é adiado para o Capítulo 4.
- **`join` e `groupBy`.** Estão na Table 3-7 e a própria tabela adia os dois para o próximo capítulo. Mas o Exercício 3 e o Exercício 4 do fim deste capítulo **exigem** join, inclusive self-join, e o Exercício 1 e o 2 exigem agregação por grupo.
- **`bucketBy` e `sortBy`.** Adiados para dentro do próprio capítulo e nunca entregues, conforme o item 12.
- **Storage levels.** Referenciados como se estivessem na Table 3-5 e ausentes, conforme o item 3.
- **Spark SQL CLI e JDBC/ODBC server.** Listados como duas das três formas de rodar SQL, e o capítulo declara que cobre só a terceira.

O julgamento: o adiamento é honesto em dois casos e desonesto em três. Catalyst e predicate pushdown são adiados com destino explícito, capítulo e seção nomeados, e isso é aviso suficiente. O CLI e o JDBC server são declarados fora de escopo em uma frase clara.

Os três problemáticos são de naturezas diferentes. `bucketBy` e `sortBy` são promessa quebrada dentro do próprio capítulo. Os storage levels são adiados para uma tabela que não os contém, o que é pior que não mencionar. E `join` e `groupBy` são adiados para o capítulo seguinte, enquanto os exercícios deste capítulo os exigem. Essa última é a mais custosa: quem tentar os quatro exercícios ao terminar o capítulo não consegue fazer nenhum dos quatro com o que aprendeu aqui.

**16.** **A escolha do apóstrofo.** Table 3-8 recommends `'columnName` for daily use, and every example in the chapter uses it. Weigh that choice.

R: A recomendação tem uma justificativa só, e ela está no texto: é um caractere a menos para digitar. Isso é o argumento mais fraco possível para uma escolha que atravessa o capítulo inteiro.

O que a escolha custa. O apóstrofo é symbol literal do Scala, então não existe em Python nem em Java, e a própria Table 3-8 marca isso. Quem lê o livro trabalhando em PySpark tem de traduzir toda linha de exemplo, e o capítulo tem dezenas delas. A alternativa que a própria tabela oferece, `col("columnName")`, é idêntica nas duas linguagens, e a tabela chega a recomendá-la para quem alterna entre as duas. O autor descreve o caminho portável e não o adota.

O que a escolha custa em Scala. Symbol literals foram depreciados no Scala 2.13 e removidos no Scala 3. O Spark 4 é construído com Scala 2.13. Conferi isso no item 8 do Nível 5. Ou seja, a sintaxe que o capítulo escolheu para todos os exemplos é a única das cinco da Table 3-8 que está em rota de remoção da linguagem.

Minha decisão: uso `col()` no meu código e leio `'x` como `col("x")` na cabeça enquanto estudo o livro. O ganho de um caractere não paga nem a perda de portabilidade nem a depreciação.

---

## Nível 5 — Além do capítulo (backlog, não notas)

Estes itens não são respondíveis com o capítulo. Conferi cada um contra fonte primária em **3 de agosto de 2026**, quando a documentação corrente do Spark era a **4.2.0**. As URLs estão ao fim da seção.

**1.** **Versão atual e matriz de suporte.** The chapter targets Spark 3.1.1 with Scala 2.12. Find the current version, the supported Java, Scala, Python and R versions, and whether a new deployment architecture appeared since 2021.

R: A versão corrente é a **Spark 4.2.0**. A página de overview diz que o Spark roda em **Java 17, 21 e 25, Scala 2.13, Python 3.10+ e R 4.0+**, com o R marcado como deprecated. O suporte a Java 25 anterior à 25.0.3 está deprecated a partir da 4.2.0.

A arquitetura nova é o **Spark Connect**, introduzido no Spark 3.4. A documentação o descreve como uma arquitetura cliente-servidor que desacopla a aplicação cliente do cluster e permite conectividade remota. Nada disso existia quando o capítulo foi escrito, e ele descreve o `spark.read` como se só houvesse o modo clássico.

**2.** **Table 3-1 hoje.** The chapter lists fifteen Spark types. Find which ones were added since 2021.

R: A referência de data types da 4.2.0 traz todos os quinze da Table 3-1, e mais estes, que não existiam na tabela do capítulo:

| Data Type | Scala Type |
|---|---|
| `CharType(length)` | `String` |
| `VarcharType(length)` | `String` |
| `TimestampNTZType` | `java.time.LocalDateTime` |
| `TimeType` | `java.time.LocalTime` |
| `YearMonthIntervalType` | `java.time.Period` |
| `DayTimeIntervalType` | `java.time.Duration` |
| `GeometryType` | `org.apache.spark.sql.types.Geometry` |
| `GeographyType` | `org.apache.spark.sql.types.Geography` |

Dois mapeamentos da Table 3-1 também mudaram. `TimestampType` hoje é documentado como `java.time.Instant` ou `java.sql.Timestamp`, e `DateType` como `java.time.LocalDate` ou `java.sql.Date`. O capítulo só traz as formas antigas, de `java.sql`. E `DecimalType` mapeia para `java.math.BigDecimal`, escrito corretamente.

O tipo mais relevante para trabalho de engenharia é o `TimestampNTZType`, timestamp sem timezone, porque a ausência dele é fonte clássica de bug em pipeline.

**3.** **Table 3-3 hoje.** The chapter lists six built-in data sources. Find the current set.

R: A documentação de data sources da 4.2.0 lista **onze**, e não seis. Os seis do capítulo continuam: Parquet, ORC, JSON, CSV, Text e JDBC. Entraram cinco:

- **XML Files**
- **Avro Files**
- **Protobuf data**
- **Whole Binary Files**
- **Hive Tables**

O caso do Avro é notável. O capítulo o nomeia na abertura como um dos três formatos binários comuns, junto com Parquet e ORC, e depois não o inclui na Table 3-3 nem lhe dá seção. Hoje ele é built-in e documentado. O XML também está na abertura, entre os formatos texto, e também ficou de fora da tabela. Ou seja, os dois formatos que o capítulo cita e não cobre são exatamente os que a documentação atual traz como built-in.

A página também documenta a **Data Source V2**, a API sobre a qual as origens customizadas são escritas hoje, e um bloco de Generic File Source Options com coisas que o capítulo não tem, como `recursiveFileLookup` e `pathGlobFilter`.

**4.** **Table 3-4 hoje.** Check the four CSV option defaults, and find out whether `mode` belongs to CSV too.

R: Os quatro defaults do capítulo continuam corretos na 4.2.0: `sep` é `,`, `header` é `false`, `escape` é `\` e `inferSchema` é `false`. A documentação acrescenta o alias `delimiter` para o `sep`.

Sobre o `mode`, sim, ele é opção de CSV também, com default `PERMISSIVE` e os mesmos três valores do JSON, `PERMISSIVE`, `DROPMALFORMED` e `FAILFAST`. O capítulo apresenta o `mode` só na seção de JSON, e a Table 3-4 não o menciona. Um leitor concluiria que `failFast` não existe para CSV, e existe.

A documentação atual também traz `columnNameOfCorruptRecord` para CSV, cujo default vem da configuração `spark.sql.columnNameOfCorruptRecord`, e `quote`, com default `"`, que é a peça que a descrição confusa de `escape` na Table 3-4 tentava explicar.

**5.** **Table 3-5 hoje, e o que o modo padrão realmente faz.** Check the three JSON defaults, and check the chapter's claim that on a corrupted record Spark "sets the value for all the columns in that row to be null".

R: Os três defaults continuam: `allowComments` false, `multiLine` false, `samplingRatio` 1.0. A coluna "Value(s)" da Table 3-5 mostra `0.3` para o `samplingRatio`, o que é um valor de exemplo e não uma faixa, e isso confunde. A documentação atual traz muito mais opções, entre elas `primitivesAsString`, default false, e `allowSingleQuotes`, default true.

A afirmação do capítulo sobre o modo padrão está **errada por generalização**. A documentação diz que o `PERMISSIVE` põe a string malformada num campo configurado por `columnNameOfCorruptRecord` e define como null **os campos malformados**, não a linha inteira. Os campos que foram parseados com sucesso são preservados. Além disso, para guardar o registro corrompido é preciso declarar no schema um campo string com esse nome, e sem ele os registros corrompidos são descartados no parsing.

Por que o capítulo viu a linha inteira nula: no exemplo dele o schema declara `actor_name` como `BooleanType` sobre dado string. Conferi a saída da Listing 3-15 e as três colunas saem nulas. Isso é o comportamento daquele caso específico, não a regra. A regra é por campo.

**6.** **Formato padrão, save modes e o `bucketBy`.** Confirm that Parquet is still the default, list the current save modes, and check whether `bucketBy` works with `save`.

R: O padrão continua Parquet, e a configuração que o define é **`spark.sql.sources.default`**. O capítulo nunca nomeia essa configuração, embora dependa do valor dela em vários listings.

Os save modes continuam quatro e a Table 3-11 continua correta. Os nomes em string são `"error"` ou `"errorifexists"` para o padrão, `"append"`, `"overwrite"` e `"ignore"`. Em Scala existem também as constantes `SaveMode.ErrorIfExists`, `SaveMode.Append`, `SaveMode.Overwrite` e `SaveMode.Ignore`, que o capítulo não menciona.

Sobre o `bucketBy`, a documentação é explícita: `bucketBy` e `sortBy` aplicam-se **apenas a tabelas persistidas**, ou seja, exigem `saveAsTable` e não funcionam com `save`. Só o `partitionBy` funciona com os dois. Isso invalida o padrão de encadeamento da Listing 3-44, que junta as três e termina em `.save(...)`. A documentação também explica a divisão de trabalho que o capítulo não faz. O `partitionBy` cria estrutura de diretório e serve para coluna de baixa cardinalidade. O `bucketBy` distribui por um número fixo de buckets e serve quando o número de valores distintos é ilimitado.

**7.** **ANSI SQL.** The chapter says Spark implements a subset of ANSI SQL:2003. Find what the current documentation says about ANSI compliance.

R: A afirmação de subconjunto continua defensável, mas o quadro mudou de forma importante. A documentação atual tem uma página inteira de ANSI Compliance, com a configuração **`spark.sql.ansi.enabled`**.

O que mudou: no Spark 4 essa configuração passou a ser **`true` por padrão**. O guia de migração diz, na seção de 3.5 para 4.0: *"Since Spark 4.0, `spark.sql.ansi.enabled` is on by default. To restore the previous behavior, set `spark.sql.ansi.enabled` to `false`."* A página de compliance resume: com ela ligada, o Spark SQL usa um dialeto ANSI-compliant em vez de um dialeto Hive-compliant.

Na prática isso muda comportamento silencioso em erro explícito. `SELECT 2147483647 + 1` lança `ARITHMETIC_OVERFLOW` em vez de dar overflow em silêncio, e `CAST('a' AS INT)` lança `CAST_INVALID_INPUT` em vez de devolver null. Há ainda `spark.sql.storeAssignmentPolicy`, com padrão ANSI, que restringe conversões implícitas em insert.

Consequência para quem estuda pelo livro: código escrito contra o comportamento Hive-compliant de 2021 pode passar a lançar exception no Spark 4 sem que uma linha tenha mudado.

**8.** **O apóstrofo.** Every example in the chapter uses `'columnName`. Check the status of Scala symbol literals.

R: Ruim para o capítulo. O guia oficial de migração para Scala 3 diz: *"The Symbol literal syntax is deprecated in Scala 2.13 and dropped in Scala 3."* A substituição indicada é `Symbol("abc")`, e o próprio guia avisa que o `Symbol` também será removido da `scala-library` numa versão futura, e recomenda migrar para string ou para classes dedicadas.

O Spark 4 é construído com Scala 2.13, então a sintaxe do capítulo compila com aviso de depreciação, e não compilaria em Scala 3. O projeto Spark tratou isso internamente, na SPARK-29392, removendo o uso da sintaxe depreciada em favor de `Symbol("name")`, e na SPARK-35151, suprimindo os avisos de compilação em Scala 2.13.

Ou seja, das cinco formas da Table 3-8, a que o autor recomenda para uso diário é a única com data de validade. As formas seguras são `col("x")`, portável entre Scala e Python, `$"x"`, que continua em Scala, e a string simples quando não houver expressão.

**9.** **Storage levels e o nível padrão.** The chapter points at the wrong table for storage options. Find the real list, and reconcile it with what Figure 3-2 shows.

R: A lista real está no RDD programming guide: `MEMORY_ONLY`, `MEMORY_AND_DISK`, `MEMORY_ONLY_SER`, `MEMORY_AND_DISK_SER`, `DISK_ONLY`, as variantes com sufixo `_2` que replicam a partition em dois nós, e o `OFF_HEAP`, marcado como experimental.

O detalhe que fecha a leitura da Figure 3-2 é o padrão, e ele é diferente para RDD e para Dataset. Para RDD, o guide diz que `cache()` usa `MEMORY_ONLY`. Para Dataset e DataFrame, o Scaladoc de `Dataset` diz: *"Persist this Dataset with the default storage level (`MEMORY_AND_DISK`)."*

Isso explica a figura. A Figure 3-2 mostra Storage Level `Disk Memory Deserialized 1x Replicated`, com 4.1 KiB em memória e 0.0 B em disco. Traduzindo: disco e memória habilitados, objetos desserializados, uma réplica, ou seja, `MEMORY_AND_DISK`. O disco está zerado porque o dado coube todo na memória. Se o padrão fosse `MEMORY_ONLY`, como no RDD, a coluna de disco nem apareceria. A figura contradiz a frase do capítulo, que diz que o DataFrame é persistido "just like how it is done with RDDs".

**10.** **A estrutura do Parquet.** The chapter says each column goes to a separate file. Check the specification.

R: A especificação do Parquet contradiz o capítulo. As definições oficiais:

- **Row group:** *"A logical horizontal partitioning of the data into rows. There is no physical structure that is guaranteed for a row group."*
- **Column chunk:** *"A chunk of the data for a particular column. They live in a particular row group and are guaranteed to be contiguous in the file."*
- **Page:** *"Column chunks are divided up into pages. A page is conceptually an indivisible unit (in terms of compression and encoding)."*

A hierarquia é: um arquivo tem um ou mais row groups, cada row group tem exatamente um column chunk por coluna, e cada column chunk é dividido em pages. Tudo dentro do mesmo arquivo, com os metadados no fim.

O que salva parcialmente a intuição do capítulo: os metadados guardam a posição inicial de cada column chunk, e a documentação diz que o leitor lê primeiro os metadados para achar só os chunks que lhe interessam. É daí que vem o ganho de não ler colunas desnecessárias. A separação é física dentro do arquivo, não em arquivos distintos. A especificação registra que o formato *permite* dividir colunas em vários arquivos, mas isso é uma possibilidade do design, não a estrutura padrão.

**11.** **Opções de JDBC hoje.** Table 3-6 lists three options. Find what exists today, especially for parallelism.

R: `url`, `dbtable` e `driver` continuam, e junto vêm `user` e `password`, que o capítulo usa nos listings sem tabelar. As adições relevantes:

| Opção | Default | Para que serve |
|---|---|---|
| `query` | nenhum | Uma query de leitura, embrulhada como subquery no FROM. Não pode ser usada junto com `dbtable` |
| `prepareQuery` | nenhum | Um prefixo que forma a query final junto com `query`, útil para CTE |
| `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions` | nenhum | Leitura paralela. A coluna precisa ser numérica, de data ou timestamp, e os quatro andam juntos |
| `pushDownPredicate` | `true` | Liga ou desliga o predicate pushdown |
| `fetchsize` | 0 | Quantas linhas o driver traz por ida e volta |
| `queryTimeout` | 0 | Segundos que o driver espera pela execução |

O grupo de partitioning é a lacuna mais séria do capítulo. Sem `partitionColumn`, uma leitura JDBC roda em uma única task, com uma conexão só, e todo o volume passa por ela. O capítulo lê uma tabela inteira do MySQL e nunca menciona o assunto. E o `pushDownPredicate` é o botão que desliga a otimização que o capítulo apresenta como automática.

**12.** **O catalog hoje.** Listing 3-40 shows `listTables` returning a `database` column. Check the current shape.

R: O Scaladoc da classe `org.apache.spark.sql.catalog.Table` na 4.2.0 lista sete membros: `name`, `catalog`, `namespace`, `database`, `description`, `tableType` e `isTemporary`.

A saída do capítulo tem cinco colunas e não tem `catalog` nem `namespace`. As duas entraram quando o Spark passou a suportar múltiplos catalogs, e o construtor atual da classe usa `catalog` e `namespace` no lugar de `database`. O `database` continua presente por compatibilidade.

Para quem estuda em 2026 isso importa porque a noção de catalog deixou de ser única. O `spark_catalog` é o catalog de sessão padrão, e produtos de lakehouse plugam catalogs próprios. A saída da Listing 3-40 é a de um mundo com um catalog só.

**13.** **`describe` contra `summary`, `union` contra `unionByName`.** Find the newer siblings of two operations the chapter teaches.

R: **`summary` contra `describe`.** O `describe` continua e computa count, mean, stddev, min e max. O `summary` faz mais: count, mean, stddev, min, max, percentis aproximados arbitrários informados como porcentagem, `count_distinct` e `approx_count_distinct`. Sem argumento, ele devolve count, mean, stddev, min, os quartis 25%, 50% e 75%, e max. O Scaladoc avisa nos dois casos que a função é para análise exploratória, e recomenda `agg` para cálculo programático. A menção do capítulo a "arbitrary approximate percentiles" na descrição de `describe` casa melhor com o `summary`.

**`unionByName` contra `union`.** O Scaladoc confirma a exigência que o capítulo descreve: *"as standard in SQL, this function resolves columns by position (not by name)"*. O `unionByName` resolve **por nome**, e aceita `allowMissingColumns`, que preenche com null as colunas faltantes e as acrescenta no fim do schema. A regra do capítulo, de que os nomes e a ordem precisam bater exatamente, continua verdadeira para `union` e é justamente o problema que `unionByName` resolve. Um `union` entre DataFrames com as mesmas colunas em ordem trocada não dá erro, ele mistura os dados em silêncio.

**14.** **Temporary views hoje.** Check whether the session and global scoping described in the chapter still holds.

R: Continua, e a documentação atual usa quase as mesmas palavras: as temporary views são session-scoped e desaparecem quando a sessão que as criou termina. Para uma view compartilhada entre sessões e viva até a aplicação terminar, existe a global temporary view, amarrada a um database preservado pelo sistema chamado **`global_temp`**, e o nome qualificado é obrigatório, como em `SELECT * FROM global_temp.view1`.

Uma diferença de nomenclatura: a documentação usa `createGlobalTempView`, e o capítulo usa `createOrReplaceGlobalTempView`. As duas existem, e a diferença é a de sempre, a segunda substitui uma view de mesmo nome em vez de dar erro.

A relação DataFrame e Dataset também continua como o capítulo descreve. A página de getting started diz: *"in Spark 2.0, DataFrames are just Dataset of `Row`s in Scala and Java API"*, e chama as operações de DataFrame de untyped transformations, em contraste com as typed transformations dos Datasets tipados. O único acréscimo é que a documentação lembra que `import spark.implicits._` é necessário para a notação `$`.

### Fontes consultadas

Todas acessadas em 3 de agosto de 2026. Todas são fonte primária: documentação oficial do projeto, referência de API, guia de migração ou especificação do formato.

Apache Spark, documentação 4.2.0:

- [Overview, com a matriz de Java, Scala, Python e R e a nota sobre Spark Connect](https://spark.apache.org/docs/4.2.0/index.html)
- [SQL Reference: Data Types](https://spark.apache.org/docs/4.2.0/sql-ref-datatypes.html)
- [Data Sources, com a lista atual das onze origens built-in](https://spark.apache.org/docs/4.2.0/sql-data-sources.html)
- [CSV Files, com os defaults das opções](https://spark.apache.org/docs/4.2.0/sql-data-sources-csv.html)
- [JSON Files, com os defaults e o comportamento do modo PERMISSIVE](https://spark.apache.org/docs/4.2.0/sql-data-sources-json.html)
- [Generic Load/Save Functions, com save modes, formato padrão e a regra de bucketBy](https://spark.apache.org/docs/4.2.0/sql-data-sources-load-save-functions.html)
- [JDBC To Other Databases, com as opções de partitioning e pushDownPredicate](https://spark.apache.org/docs/4.2.0/sql-data-sources-jdbc.html)
- [ANSI Compliance](https://spark.apache.org/docs/4.2.0/sql-ref-ansi-compliance.html)
- [SQL Migration Guide, com a mudança do default de ANSI no Spark 4.0](https://spark.apache.org/docs/4.2.0/sql-migration-guide.html)
- [SQL Getting Started, com DataFrame como Dataset[Row] e as temporary views](https://spark.apache.org/docs/4.2.0/sql-getting-started.html)
- [RDD Programming Guide, com a lista de storage levels](https://spark.apache.org/docs/4.2.0/rdd-programming-guide.html)
- [Scaladoc de `org.apache.spark.sql.Dataset`](https://spark.apache.org/docs/4.2.0/api/scala/org/apache/spark/sql/Dataset.html)
- [Scaladoc de `org.apache.spark.sql.catalog.Table`](https://spark.apache.org/docs/4.2.0/api/scala/org/apache/spark/sql/catalog/Table.html)

Scala:

- [Scala 3 Migration Guide, Dropped Features, com o status dos symbol literals](https://docs.scala-lang.org/scala3/guides/migration/incompat-dropped-features.html)
- [SPARK-29392, remoção do uso da sintaxe depreciada no Spark](https://issues.apache.org/jira/browse/SPARK-29392)
- [SPARK-35151, supressão dos avisos de symbol literal em Scala 2.13](https://issues.apache.org/jira/browse/SPARK-35151)

Apache Parquet:

- [File Format](https://parquet.apache.org/docs/file-format/)
- [Concepts, com as definições de row group, column chunk e page](https://parquet.apache.org/docs/concepts/)

Defaults de opção e nomes de tipo mudam entre releases. Preciso reconferir estes dados antes de usá-los em prova ou em decisão técnica.

---

## Termos-chave para definir antes de seguir adiante

Escreva uma definição de uma linha para cada, com suas próprias palavras, sem consultar:

Spark SQL · Structured APIs · RDD · lineage · partition · compute function · DataFrame · Dataset · `Row` · schema · `StructType` · `StructField` · nullable · Catalyst optimizer · predicate pushdown · transformation · action · lazy evaluation · eager evaluation · `DataFrameReader` · `DataFrameWriter` · format · option · `inferSchema` · `samplingRatio` · `failFast` · Parquet · ORC · Avro · JDBC · columnar storage · classe `Column` · column expression · projection · DSL · `DataFrameNaFunctions` · encoder · case class · type safety · temporary view · global temporary view · catalog · save mode · `partitionBy` · `bucketBy` · `coalesce` · persist · storage level · OLAP · TPC-DS · shuffle

Qualquer termo que você não conseguir definir é alvo de releitura, não item de Nível 5.

### Minhas definições

Treze dos cinquenta e um termos o capítulo usa sem definir, define errado, ou adia para outro capítulo. Marquei esses casos em itálico.

**Spark SQL** — Módulo do Spark para processamento de dados estruturados, construído sobre o Spark Core. Tem duas partes: as representações das Structured APIs e o Catalyst optimizer.

**Structured APIs** — Abstração de programação introduzida no Spark 1.6, que exige dado em formato estruturado e lógica de computação com estrutura, em troca de otimização automática.

**RDD** — Coleção de elementos tolerante a falhas, particionada pelos nós de um cluster, operável em paralelo. Tem cinco características, das quais três formam a lineage.

**lineage** — As três informações que permitem ao Spark determinar a ordem de execução dos RDDs e recuperar de falha: dependências, partitions e função de computação.

**partition** — *Usado sem definição própria.* O capítulo o descreve de passagem como os pedaços que formam o dataset inteiro, e é o que permite executar a lógica em paralelo. Também é a unidade que determina quantos arquivos a escrita produz.

**compute function** — A função fornecida pelo usuário, enviada a cada executor, que roda contra cada linha de cada partition.

**DataFrame** — Coleção imutável e distribuída de dados organizada em linhas, com colunas nomeadas e tipadas, equivalente a uma tabela de RDBMS. Desde o Spark 2.0 é um type alias para `Dataset[Row]`.

**Dataset** — A abstração de alto nível do Spark SQL desde a unificação do 2.0, com dois sabores, um strongly-typed e um untyped. Cada linha do sabor tipado é um objeto de domínio definido pelo usuário.

**`Row`** — Objeto JVM genérico e untyped que representa cada linha de um DataFrame.

**schema** — A estrutura do dado na forma de nomes de coluna e tipos associados. É o que separa dado estruturado de dado não estruturado.

**`StructType`** — O tipo que representa um schema inteiro, montado sobre um array de `StructField`. Na Table 3-1 ele mapeia para `org.apache.spark.sql.Row`.

**`StructField`** — A descrição de uma coluna, com três informações: nome, tipo e se aceita null.

**nullable** — O terceiro campo de um `StructField`, que declara se a coluna aceita valor nulo. *O capítulo o usa em todos os schemas e nunca discute a consequência dele, que aparece no item 7 do Nível 4.*

**Catalyst optimizer** — *Nomeado, desenhado na Figure 3-1 e adiado.* O capítulo o apresenta como a segunda metade do Spark SQL, responsável pelo maquinário que acelera a lógica, e manda para o Capítulo 4 sem explicar nada.

**predicate pushdown** — Otimização em que o Spark empurra as condições de filtro para a origem, que filtra antes de entregar. Funciona em JDBC e em Parquet. *O exemplo é adiado para o Capítulo 4.*

**transformation** — Operação que devolve um DataFrame novo e não executa nada na hora. É avaliada de forma lazy.

**action** — Operação que dispara a computação de todas as transformations que levam até ela. É avaliada de forma eager.

**lazy evaluation** — A semântica das transformations: a operação é registrada e nada roda até uma action pedir resultado.

**eager evaluation** — A semântica das actions: a computação acontece na chamada.

**`DataFrameReader`** — Classe responsável por criar um DataFrame lendo de uma origem de dados. Alcançada por `spark.read`.

**`DataFrameWriter`** — Classe responsável por escrever o conteúdo de um DataFrame em um sistema externo. Alcançada por `df.write`.

**format** — A única informação obrigatória do reader. Aceita short name para os built-in e nome totalmente qualificado para origem customizada.

**option** — Par de chave e valor que sobrescreve um padrão do formato. Cada formato tem seu conjunto.

**`inferSchema`** — Opção de CSV que manda o Spark deduzir o tipo das colunas a partir dos valores. Padrão false, e sem ela todas as colunas viram string.

**`samplingRatio`** — Opção de JSON que define a fração de registros lida para inferir o schema. Padrão 1.0, o que significa o arquivo inteiro.

**`failFast`** — Valor da opção `mode` que faz o Spark lançar exception ao encontrar registro corrompido, em vez de preencher com null. *O capítulo o apresenta só para JSON, e ele vale também para CSV, conforme o item 4 do Nível 5.*

**Parquet** — Formato colunar open source criado no Twitter, self-describing e compacto. É o formato padrão de leitura e escrita do Spark. *O capítulo erra ao dizer que cada coluna vai para um arquivo separado.*

**ORC** — Optimized Row Columnar, formato colunar self-describing criado pela Cloudera para acelerar o Hive. Similar ao Parquet em eficiência.

**Avro** — *Nomeado na abertura e nunca coberto.* O capítulo o cita como um dos três formatos binários comuns e não lhe dá seção nem linha na Table 3-3. Hoje é origem built-in documentada.

**JDBC** — API padrão para ler e escrever em RDBMS. Exige driver no classpath, URL, autenticação e nome de tabela.

**columnar storage** — Organização em que os dados de uma coluna ficam juntos, o que permite ler só as colunas que a análise usa. *O capítulo descreve o benefício corretamente e o mecanismo errado.*

**classe `Column`** — Classe que representa uma coluna e as operações sobre ela: matemática, comparação lógica e casamento de padrão em string.

**column expression** — Uma expressão que produz uma coluna nova a partir de outras, como `'produced_year % 10`. Exige instância de `Column`, não aceita string.

**projection** — O termo técnico para o que o `select` faz: escolher e transformar um subconjunto de colunas.

**DSL** — Domain-specific language, linguagem especializada em um domínio. Aqui, a manipulação distribuída de dados.

**`DataFrameNaFunctions`** — Classe dedicada a dado faltante ou ruim, alcançada por `df.na`. Oferece descartar linhas, preencher valores e substituir dado ruim.

**encoder** — Utilitário do Dataset que converte cada objeto de domínio em formato binário compacto, reduzindo memória no cache e bytes no shuffle.

**case class** — Classe Scala imutável, com `toString` e `equals` gerados, usada para modelar o objeto de domínio de cada linha de um Dataset.

**type safety** — A propriedade de ter nome e tipo de coluna conferidos pelo compilador, e não em runtime. Só existe no Dataset, em Scala e Java.

**temporary view** — Nome dado a um DataFrame dentro do catalog, para que SQL consiga referenciá-lo. Vive enquanto a Spark session viver.

**global temporary view** — View visível a todas as Spark sessions da aplicação, referenciada com o prefixo `global_temp`.

**catalog** — O repositório de metadados das tabelas e views que a sessão enxerga, alcançado por `spark.catalog`. *O capítulo o usa e não o define, e a estrutura mostrada já está desatualizada, conforme o item 12 do Nível 5.*

**save mode** — Opção do writer que decide o que fazer quando o destino já existe: `append`, `overwrite`, `error` ou `ignore`. O padrão é `error`.

**`partitionBy`** — Função do writer que grava um subdiretório por valor da coluna, nomeado `coluna=valor`. Pede coluna de baixa cardinalidade.

**`bucketBy`** — *Nomeado duas vezes, nunca explicado e descrito errado.* O capítulo diz que ele controla a estrutura de diretórios. Ele distribui as linhas por um número fixo de buckets, e só funciona com `saveAsTable`.

**`coalesce`** — Função que reduz o número de partitions de um DataFrame. Usada com argumento 1 para forçar um arquivo de saída só. *O capítulo a usa sem defini-la nem contrastá-la com `repartition`.*

**persist** — API que guarda o DataFrame em memória, com `unpersist` para liberar. Um DataFrame ocupa menos que um RDD equivalente, porque o Spark conhece o schema e usa formato colunar comprimido.

**storage level** — *Referenciado para a tabela errada e nunca listado.* Define onde e como o dado cacheado é guardado. O padrão de um Dataset é `MEMORY_AND_DISK`, o que explica a Figure 3-2 e está no item 9 do Nível 5.

**OLAP** — Online analytical processing, o caso de uso para o qual o SQL do Spark foi desenhado. Não serve para OLTP nem para baixa latência.

**TPC-DS** — Benchmark padrão de indústria para decision support, aplicável ao Spark porque ele implementa um subconjunto do ANSI SQL:2003.

**shuffle** — *A palavra aparece uma vez no capítulo, em "the shuffling process", sem definição.* É a redistribuição de dados entre partitions, e é o momento em que os encoders do Dataset reduzem os bytes trafegados.
