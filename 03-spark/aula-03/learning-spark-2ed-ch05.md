# Guia de Leitura — *Learning Spark*, 2ª edição, Capítulo 5: Spark SQL and DataFrames: Interacting with External Data Sources

**Fonte:** Jules S. Damji, Brooke Wenig, Tathagata Das, Denny Lee. *Learning Spark: Lightning-Fast Data Analytics*, 2ª ed. O'Reilly, 2020. Capítulo 5, "Spark SQL and DataFrames: Interacting with External Data Sources".

**Escopo:** os Níveis 1 a 4 são respondíveis só com este capítulo. O Nível 5 exige verificação contra fonte primária. O Nível 6 exige o Capítulo 2 do *Data Engineering with Databricks Cookbook* já lido.

**Sobre as figuras:** abri as páginas 7, 13, 14, 15, 16 e 17 do PDF e li as imagens da Figure 5-1 e das Figures 5-2 a 5-8. Abri também as páginas 20, 29, 32, 34, 35 e 45 para conferir as Tables 5-2, 5-3, 5-4 e 5-5 e um listing truncado, porque o texto extraído embaralha tabela.

---

## Como usar este guia

1. Leia o capítulo inteiro uma vez, sem parar em questão nenhuma. Ele é de leitura fácil e de armadilha difícil, então a primeira passagem serve só para mapear as seções.
2. Responda o Nível 1 de memória antes de reabrir o texto. O ponteiro de seção em cada questão diz onde releer o que falhou.
3. Escreva os Níveis 2 e 3 em frases completas. Nos cenários do Nível 3 o capítulo dá as peças e não dá a resposta, então a resposta é sua e precisa citar de onde veio cada peça.
4. O Nível 4 é onde este capítulo rende. Ele tem erros de impressão em assinatura de função, uma contradição entre prosa e figura, e uma escada de custo que os autores montam sem nunca medir. Espere gastar mais tempo aqui do que nos três primeiros níveis somados.
5. O Nível 5 vai para o backlog de estudo. O Nível 6 vai para uma nota de comparação que fica acima dos dois livros, não dentro de nenhum deles.

---

## Nível 1 — Memorização e definições

Respostas curtas e verificáveis. Uma ou duas frases cada.

**1.** Quais três coisas a introdução diz que o Spark SQL permite fazer? *(Chapter introduction)*

R: Usar user-defined functions tanto para o Apache Hive quanto para o Apache Spark. Conectar-se a fontes de dados externas como JDBC e bancos SQL, PostgreSQL, MySQL, Tableau, Azure Cosmos DB e MS SQL Server. Trabalhar com tipos simples e complexos, higher-order functions e operadores relacionais comuns.

**2.** Qual foi a gênese do Spark SQL, e o que aquele projeto anterior demonstrou? *(Spark SQL and Apache Hive)*

R: A gênese foi o Shark, construído originalmente sobre a base de código do Hive em cima do Apache Spark. Ele virou um dos primeiros engines de query SQL interativa em sistemas Hadoop. Demonstrou que dava para ter os dois mundos, tão rápido quanto um data warehouse corporativo e escalando tão bem quanto Hive/MapReduce.

**3.** O que a nota de rodapé 1 acrescenta sobre o código do Hive? *(footnote 1)*

R: Que o engine atual do Spark SQL não usa mais o código do Hive na sua implementação.

**4.** O que é uma UDF, segundo o capítulo? *(User-Defined Functions)*

R: São funções que o próprio engenheiro ou cientista de dados define, apesar de o Spark já ter uma grande quantidade de funções embutidas. A sigla vem de user-defined functions.

**5.** Qual exemplo o capítulo dá para justificar a criação de uma UDF? *(Spark SQL UDFs)*

R: Um cientista de dados envolve um modelo de machine learning dentro de uma UDF. Assim um analista consulta as predições em Spark SQL sem precisar entender o interior do modelo.

**6.** Quais duas restrições de escopo o capítulo enuncia sobre UDFs, logo antes do primeiro trecho de código? *(Spark SQL UDFs)*

R: Que as UDFs operam por sessão e que não são persistidas no metastore subjacente.

**7.** Como se registra a UDF `cubed` em Scala e em Python, e qual argumento existe só na versão Python? *(Spark SQL UDFs)*

R: Nas duas o registro é `spark.udf.register("cubed", cubed)`. A versão Python acrescenta um terceiro argumento, `LongType()`, importado de `pyspark.sql.types`, que declara o tipo de retorno.

**8.** O que o Spark SQL não garante, e sobre o quê? *(Evaluation order and null checking in Spark SQL)*

R: Não garante a ordem de avaliação das subexpressões. O capítulo diz que isso vale para SQL, para a DataFrame API e para a Dataset API.

**9.** Qual query o capítulo usa para ilustrar essa falta de garantia? *(Evaluation order and null checking in Spark SQL)*

R: `SELECT s FROM test1 WHERE s IS NOT NULL AND strlen(s) > 1`. Nada garante que a cláusula `s IS NOT NULL` seja executada antes de `strlen(s) > 1`.

**10.** Quais duas recomendações o capítulo dá para fazer null checking direito? *(Evaluation order and null checking in Spark SQL)*

R: Primeira, tornar a própria UDF null-aware e fazer a checagem de nulo dentro dela. Segunda, usar expressões `IF` ou `CASE WHEN` para a checagem e invocar a UDF em um ramo condicional.

**11.** Por que as UDFs de PySpark tinham performance pior que as de Scala? *(Speeding up and distributing PySpark UDFs with Pandas UDFs)*

R: Porque exigiam movimentação de dados entre a JVM e o Python, o que o capítulo descreve como bastante caro.

**12.** Em qual versão do Spark as Pandas UDFs foram introduzidas, e por qual outro nome são conhecidas? *(Speeding up and distributing PySpark UDFs with Pandas UDFs)*

R: No Apache Spark 2.3. Também são chamadas de vectorized UDFs.

**13.** Quais duas tecnologias uma Pandas UDF usa, e para quê cada uma? *(Speeding up and distributing PySpark UDFs with Pandas UDFs)*

R: O Apache Arrow para transferir os dados e o Pandas para trabalhar com eles.

**14.** O que o formato Arrow dispensa, e sobre o que se passa a operar? *(Speeding up and distributing PySpark UDFs with Pandas UDFs)*

R: Dispensa serializar ou fazer pickle dos dados, porque eles já chegam em um formato consumível pelo processo Python. Em vez de operar sobre entradas individuais linha a linha, opera-se sobre uma Series ou um DataFrame do Pandas.

**15.** Como se declara uma Pandas UDF? *(Speeding up and distributing PySpark UDFs with Pandas UDFs)*

R: Com a palavra-chave `pandas_udf`, usada como decorator ou envolvendo a própria função. O exemplo do capítulo usa a segunda forma, `cubed_udf = pandas_udf(cubed, returnType=LongType())`.

**16.** A partir de qual versão do Spark e de qual versão do Python as Pandas UDFs foram divididas, e em quais duas categorias? *(Speeding up and distributing PySpark UDFs with Pandas UDFs)*

R: A partir do Apache Spark 3.0 com Python 3.6 ou superior. As duas categorias são Pandas UDFs e Pandas Function APIs.

**17.** Quais quatro casos de type hint do Python o capítulo diz serem suportados nas Pandas UDFs? *(Speeding up and distributing PySpark UDFs with Pandas UDFs)*

R: Series para Series, Iterator of Series para Iterator of Series, Iterator of Multiple Series para Iterator of Series, e Series para Scalar, ou seja, um único valor.

**18.** O que as Pandas Function APIs permitem, e quais três são suportadas no Spark 3.0? *(Speeding up and distributing PySpark UDFs with Pandas UDFs)*

R: Permitem aplicar diretamente uma função Python local a um DataFrame do PySpark, com entrada e saída sendo instâncias de Pandas. As três são grouped map, map e co-grouped map.

**19.** Qual é a diferença de execução entre chamar `cubed(x)` sobre uma Series local e chamar `cubed_udf` sobre um DataFrame? *(Speeding up and distributing PySpark UDFs with Pandas UDFs)*

R: A função local é uma função Pandas executada apenas no Spark driver. A UDF vetorizada resulta na execução de Spark jobs.

**20.** Quais blocos a Figure 5-1 mostra, e qual deles identifica a execução de uma Pandas UDF? *(Figure 5-1)*

R: Abri a página 7 do PDF. A figura é a tela "Details for Stage 28 (Attempt 0)", com "Total Time Across All Tasks: 73 ms", "Locality Level Summary: Process local: 1" e "Associated Job Ids: 28". O DAG do Stage 28 tem três blocos: um `WholeStageCodegen` com `ParallelCollectionRDD [64]` e dois `MapPartitionsRDD` ([65] e [66]), depois um `ArrowEvalPython` com `MapPartitionsRDD` [67] e [68], depois outro `WholeStageCodegen` com `MapPartitionsRDD [69]`. O bloco que identifica a Pandas UDF é o `ArrowEvalPython`.

**21.** Com o que o job começa, segundo a prosa que acompanha a Figure 5-1, e o que os passos `WholeStageCodegen` representam? *(Speeding up and distributing PySpark UDFs with Pandas UDFs)*

R: Começa com `parallelize()`, que envia os dados locais como Arrow binary batches para os executors, e chama `mapPartitions()` para converter esses batches para o formato interno do Spark. Os passos `WholeStageCodegen` representam um salto de performance, creditado ao whole-stage code generation do Project Tungsten, que melhora a eficiência de CPU.

**22.** O que a nota de rodapé 2 avisa? *(footnote 2)*

R: Que há pequenas diferenças ao trabalhar com Pandas UDFs entre o Spark 2.3, o 2.4 e o 3.0.

**23.** Com o que o `spark-sql` CLI se comunica, e com o que ele não se comunica? *(Using the Spark SQL Shell)*

R: Ele se comunica com o serviço de Hive metastore em modo local. Não conversa com o Thrift JDBC/ODBC server.

**24.** O que significa STS, e o que ele permite? *(Using the Spark SQL Shell)*

R: Spark Thrift Server, outro nome do Thrift JDBC/ODBC server. Ele permite que clientes JDBC/ODBC executem queries SQL sobre o Apache Spark pelos protocolos JDBC e ODBC.

**25.** Qual comando inicia o Spark SQL CLI, e a partir de qual diretório? *(Using the Spark SQL Shell)*

R: `./bin/spark-sql`, executado na pasta `$SPARK_HOME`.

**26.** Que localização de arquivo aparece no aviso ao criar a tabela `people`? *(Create a table)*

R: `file:/user/hive/warehouse/`. A linha completa é um `WARN HiveMetaStore` dizendo que essa localização foi especificada para uma tabela não externa, `people`.

**27.** Quais três linhas o capítulo insere na tabela `people`, e qual delas tem valor nulo? *(Insert data into the table)*

R: `("Michael", NULL)`, `("Andy", 30)` e `("Samantha", 19)`. A de Michael tem a idade nula.

**28.** O que é o Beeline, e contra o que ele roda no mundo Hive? *(Working with Beeline)*

R: É uma ferramenta de linha de comando comum para rodar queries HiveQL contra o HiveServer2. O capítulo o descreve como um cliente JDBC baseado no SQLLine CLI.

**29.** A qual versão do HiveServer2 o capítulo diz que o Thrift JDBC/ODBC server corresponde? *(Working with Beeline)*

R: Ao HiveServer2 do Hive 1.2.1. O capítulo diz que o script de teste do Beeline vem com o Spark ou com o Hive 1.2.1.

**30.** Quais comandos iniciam e param o Thrift server, e qual comando pode ser necessário antes? *(Start the Thrift server, Stop the Thrift server)*

R: `./sbin/start-thriftserver.sh` inicia e `./sbin/stop-thriftserver.sh` para. Se o driver e o worker do Spark ainda não estiverem no ar, é preciso rodar `./sbin/start-all.sh` antes.

**31.** Qual string de conexão o Beeline usa, e qual é o modo de segurança padrão? *(Connect to the Thrift server via Beeline, NOTE)*

R: `!connect jdbc:hive2://localhost:10000`. Por padrão o Beeline está em modo não seguro, então o username é o login da pessoa e a senha fica em branco.

**32.** Qual versão do Tableau Desktop o capítulo usa, e qual driver ODBC ele exige? *(Working with Tableau)*

R: Tableau Desktop versão 2019.2. Exige o Spark ODBC driver do Tableau na versão 1.2.0 ou superior, que já vem pré-instalado em quem tem Tableau 2018.1 ou maior.

**33.** Quais seis parâmetros o capítulo lista para o diálogo Spark SQL do Tableau? *(Start Tableau)*

R: Server `localhost`, Port `10000` (default), Type `SparkThriftServer` (default), Authentication `Username`, Username com o login da pessoa, e Require SSL desmarcado.

**34.** O que a data source API de JDBC devolve, e quais benefícios isso traz? *(JDBC and SQL Databases)*

R: Devolve os resultados como um DataFrame. Isso traz todos os benefícios do Spark SQL, incluindo performance e a capacidade de fazer join com outras fontes de dados.

**35.** Qual comando o capítulo mostra para colocar o driver JDBC no classpath? *(JDBC and SQL Databases)*

R: `./bin/spark-shell --driver-class-path $database.jar --jars $database.jar`, a partir da pasta `$SPARK_HOME`.

**36.** Quais cinco propriedades a Table 5-1 lista, e o que ela diz sobre a sensibilidade a maiúsculas? *(Table 5-1)*

R: `user` e `password`, `url`, `dbtable`, `query` e `driver`. O capítulo diz que essas propriedades de conexão são case-insensitive.

**37.** Quais duas opções da Table 5-1 não podem ser especificadas ao mesmo tempo? *(Table 5-1)*

R: `dbtable` e `query`. A tabela repete a restrição nas duas entradas.

**38.** Por que particionar importa numa transferência JDBC grande, segundo o capítulo? *(The importance of partitioning)*

R: Porque sem particionar todos os dados passam por uma única conexão de driver. Isso pode saturar essa conexão e deixar a extração muito mais lenta, e pode saturar também os recursos do sistema de origem.

**39.** Quais quatro propriedades a Table 5-2 lista, e o que a tabela exige do `partitionColumn`? *(Table 5-2)*

R: `numPartitions`, `partitionColumn`, `lowerBound` e `upperBound`. Abri a página 20 do PDF para conferir. A tabela exige que `partitionColumn` seja uma coluna numérica, de data ou de timestamp.

**40.** No exemplo com `numPartitions: 10`, `lowerBound: 0` e `upperBound: 10000`, qual é o stride e quantas partições saem? *(The importance of partitioning)*

R: O stride é 1.000 e saem 10 partições. O capítulo diz que isso equivale a executar dez queries, uma por partição, a primeira com `partitionColumn BETWEEN 0 and 1000` e a última com `BETWEEN 9000 and 10000`.

**41.** Qual é o bom ponto de partida para `numPartitions`, e qual ressalva o capítulo acrescenta? *(The importance of partitioning)*

R: Um múltiplo do número de Spark workers, por exemplo 4 ou 8 partições para quatro worker nodes. A ressalva é olhar como o sistema de origem aguenta as requisições de leitura. Em sistemas com janela de processamento dá para maximizar as concorrentes. Em sistemas sem janela, como um OLTP em operação contínua, é preciso reduzir.

**42.** Quais outras duas dicas o capítulo dá sobre as propriedades de particionamento? *(The importance of partitioning)*

R: Calcular `lowerBound` e `upperBound` a partir dos valores mínimo e máximo reais do `partitionColumn`. Com `{0, 10000}` e valores só entre 2000 e 4000, apenas duas das dez queries fazem todo o trabalho. E escolher um `partitionColumn` bem distribuído para evitar data skew, gerando uma coluna nova se preciso, como um hash de várias colunas.

**43.** Quais jars o capítulo nomeia para PostgreSQL, MySQL, Azure Cosmos DB e MS SQL Server? *(PostgreSQL, MySQL, Azure Cosmos DB, MS SQL Server)*

R: `postgresql-42.2.6.jar`, `mysql-connector-java_8.0.16-bin.jar`, `azure-cosmosdb-spark_2.4.0_2.11-1.3.5-uber.jar` e `mssql-jdbc-7.2.2.jre8.jar`.

**44.** Quais duas formas de leitura o capítulo mostra para PostgreSQL? *(PostgreSQL)*

R: A Read Option 1 usa `.format("jdbc")` com `.option(...)` e termina em `.load()`. A Read Option 2 usa o método `.jdbc(url, table, properties)`, passando as credenciais num objeto de propriedades.

**45.** Qual configuração o capítulo destaca como comum no Azure Cosmos DB, e para quê? *(Azure Cosmos DB)*

R: A `query_custom`, para aproveitar os vários índices dentro do Cosmos DB.

**46.** Quais duas soluções típicas o capítulo aponta para manipular tipos de dados complexos? *(Higher-Order Functions in DataFrames and Spark SQL)*

R: Explodir a estrutura aninhada em linhas individuais, aplicar alguma função e recriar a estrutura aninhada. Ou construir uma user-defined function.

**47.** Quais cinco funções utilitárias o capítulo nomeia como típicas dessas abordagens? *(Higher-Order Functions in DataFrames and Spark SQL)*

R: `get_json_object()`, `from_json()`, `to_json()`, `explode()` e `selectExpr()`.

**48.** Por que a Option 1 pode sair muito cara? *(Option 1: Explode and Collect)*

R: Porque o `GROUP BY` exige operações de shuffle, então a ordem do array recoletado não é necessariamente a mesma do array original. Como `values` pode ter qualquer número de dimensões e ainda há um `GROUP BY`, o capítulo diz que a abordagem pode ser muito cara.

**49.** Qual é o trade-off da Option 2, e qual vantagem ela tem sobre a Option 1? *(Option 2: User-Defined Function)*

R: O processo de serialização e desserialização em si pode ser caro. A vantagem é que não há problema de ordenação, e o `collect_list()` pode causar out-of-memory nos executors em datasets grandes, o que a UDF alivia.

**50.** A partir de qual versão do Spark existem as funções embutidas para tipos complexos, e o que cobrem as Tables 5-3 e 5-4? *(Built-in Functions for Complex Data Types)*

R: A partir do Apache Spark 2.4. A Table 5-3 cobre funções de tipo array e a Table 5-4 cobre funções de tipo map.

**51.** O que é uma higher-order function, segundo o capítulo, e qual exemplo genérico ele dá? *(Higher-Order Functions)*

R: É uma função que recebe funções lambda anônimas como argumento. O exemplo genérico é `transform(values, value -> lambda expression)`.

**52.** Quais são as quatro assinaturas de higher-order function que o capítulo detalha? *(Higher-Order Functions)*

R: `transform(array<T>, function<T, U>): array<U>`, `filter(array<T>, function<T, Boolean>): array<T>`, `exists(array<T>, function<T, V, Boolean>): Boolean` e `reduce(array<T>, B, function<B, T, B>, function<B, R>)`.

**53.** Qual dataset alimenta os exemplos de higher-order function, e o que ele contém? *(Higher-Order Functions)*

R: A view temporária `tC`, com uma coluna `celsius` do tipo array de inteiros. Tem duas linhas, `[35, 36, 32, 30, 40, 42, 38]` e `[31, 32, 34, 55, 56]`.

**54.** O que a query com `transform` calcula, e qual operador ela usa na divisão? *(Higher-Order Functions, transform())*

R: Converte Celsius em Fahrenheit com `transform(celsius, t -> ((t * 9) div 5) + 32)`. O operador é `div`, de divisão inteira.

**55.** Quais dez categorias de operação de DataFrame o capítulo lista? *(Common DataFrames and Spark SQL Operations)*

R: Aggregate functions, collection functions, datetime functions, math functions, miscellaneous functions, non-aggregate functions, sorting functions, string functions, UDF functions e window functions.

**56.** Quais três operações relacionais o capítulo escolhe cobrir? *(Common DataFrames and Spark SQL Operations)*

R: Unions e joins, windowing e modifications.

**57.** Quantas linhas têm `departureDelays` e `foo`? *(Common DataFrames and Spark SQL Operations)*

R: `departureDelays` tem mais de 1,3 milhão de voos. `foo` tem apenas três linhas.

**58.** Qual é o tipo de join padrão no Spark SQL, e quais opções o capítulo lista? *(Joins)*

R: O padrão é `inner`. As opções são `inner`, `cross`, `outer`, `full`, `full_outer`, `left`, `left_outer`, `right`, `right_outer`, `left_semi` e `left_anti`.

**59.** O que é uma window function, segundo o capítulo? *(Windowing)*

R: É uma função que usa valores das linhas de uma window, uma faixa de linhas de entrada, para devolver um conjunto de valores, tipicamente na forma de outra linha. Com ela dá para operar sobre um grupo de linhas e ainda assim devolver um valor por linha de entrada.

**60.** Quais funções a Table 5-5 lista, e em quais duas categorias? *(Table 5-5)*

R: Abri a página 45 do PDF. Ranking functions traz `rank()`, `dense_rank()`, `percent_rank()`, `ntile()` e `row_number()`. Analytic functions traz `cume_dist()`, `first_value()`, `last_value()`, `lag()` e `lead()`. A tabela tem duas colunas, SQL e DataFrame API, e a segunda usa camelCase em `denseRank()`, `percentRank()`, `rowNumber()`, `cumeDist()`, `firstValue()` e `lastValue()`.

**61.** Qual ressalva o capítulo faz sobre window groupings? *(Windowing)*

R: Que cada window grouping precisa caber em um único executor e vira uma única partição durante a execução. Por isso as queries não podem ser ilimitadas, ou seja, é preciso limitar o tamanho da window.

**62.** Como o capítulo concilia modificações com a imutabilidade dos DataFrames? *(Modifications)*

R: Diz que DataFrames são imutáveis, mas que dá para modificá-los por operações que criam DataFrames novos e diferentes, com colunas diferentes. Lembra também que os RDDs subjacentes são imutáveis para garantir data lineage.

**63.** Quais três métodos o capítulo usa para adicionar, remover e renomear coluna? *(Adding new columns, Dropping columns, Renaming columns)*

R: `withColumn()` para adicionar, `drop()` para remover e `withColumnRenamed()` para renomear.

**64.** O que a query de pivot faz com o mês, e quais duas agregações ela calcula? *(Pivoting)*

R: Troca o número do mês pelo nome, `1 JAN` e `2 FEB`, na cláusula `FOR month IN`. Calcula a média do atraso, com `CAST(AVG(delay) AS DECIMAL(4, 2)) AS AvgDelay`, e o máximo, `MAX(delay) AS MaxDelay`.

## Nível 2 — Compreensão

Explique com suas próprias palavras. Três a seis frases cada.

**1.** Explique por que uma UDF operar por sessão e não ser persistida no metastore muda o jeito de entregá-la a outra pessoa. *(Spark SQL UDFs)*

R: A UDF vive dentro da `SparkSession` que a registrou. Quando a aplicação termina, o registro some, porque nada foi gravado no metastore. Isso significa que o analista do exemplo do capítulo não abre um cliente SQL e encontra `cubed` lá. Alguém precisa rodar o código de registro na mesma sessão que ele vai usar. A consequência prática é que entregar uma UDF é entregar código de inicialização, não um objeto de banco. O capítulo enuncia a restrição em uma frase e nunca discute o que ela implica para o cenário de cientista e analista que ele mesmo montou.

**2.** Explique por que a ausência de garantia de ordem de avaliação quebra o guarda de nulo escrito no `WHERE`. *(Evaluation order and null checking in Spark SQL)*

R: Quem escreve `WHERE s IS NOT NULL AND strlen(s) > 1` está lendo o `AND` como se fosse o de uma linguagem imperativa, com curto-circuito da esquerda para a direita. O Spark SQL não promete isso. O otimizador pode reordenar as subexpressões e chamar `strlen(s)` primeiro. Se `s` for nulo, a UDF recebe nulo e o comportamento depende do que ela faz com nulo. Em Python, uma função que faça `len(s)` levanta exceção e derruba a task. O guarda de nulo não protege nada porque ele não controla quando roda.

**3.** Explique por que as duas recomendações do capítulo funcionam, e o que exatamente cada uma move de lugar. *(Evaluation order and null checking in Spark SQL)*

R: A primeira recomendação move a checagem para dentro da UDF. Ali a ordem é do Python ou do Scala, não do otimizador, então a garantia volta. A segunda move a checagem para uma construção condicional, `IF` ou `CASE WHEN`, que tem semântica de ramo e não de subexpressão. Um ramo só avalia depois que a condição decide, então a chamada da UDF fica dentro de um caminho que só existe quando a condição é verdadeira. As duas fazem a mesma coisa em lugares diferentes. Uma resolve dentro da função e a outra resolve na expressão que a chama. O capítulo dá as duas e não explica por que a segunda é segura enquanto o `AND` não é.

**4.** Explique o problema de movimentação de dados entre JVM e Python, e o que o Arrow muda nele. *(Speeding up and distributing PySpark UDFs with Pandas UDFs)*

R: O dado do Spark vive na JVM. Uma UDF de Python roda em um processo Python separado, então cada linha precisa sair da JVM, atravessar até o Python e voltar. No caminho antigo isso exigia serializar e fazer pickle dos valores nas duas pontas, linha a linha. O Arrow define um formato binário colunar que os dois lados entendem, então o dado atravessa em lotes já consumíveis pelo Python. O capítulo diz que, uma vez em formato Arrow, não é mais preciso serializar ou fazer pickle. O segundo ganho é o tamanho da unidade de trabalho, que passa de uma linha para uma Series inteira.

**5.** Explique a diferença entre Pandas UDFs e Pandas Function APIs. *(Speeding up and distributing PySpark UDFs with Pandas UDFs)*

R: A Pandas UDF é uma função que o Spark chama como se fosse uma coluna, dentro de um `select` ou de uma query SQL. O tipo dela é inferido dos type hints do Python, e o capítulo lista quatro combinações aceitas. A Pandas Function API é outra coisa: ela aplica uma função Python local diretamente a um DataFrame do PySpark, com entrada e saída em objetos Pandas. Ou seja, uma é uma expressão dentro de uma query e a outra é uma operação sobre o DataFrame inteiro ou sobre grupos dele. O capítulo lista grouped map, map e co-grouped map como as três disponíveis no Spark 3.0.

**6.** Explique o que a Figure 5-1 prova e o que ela não prova. *(Figure 5-1)*

R: Abri a figura. Ela prova uma coisa só: que existe um passo chamado `ArrowEvalPython` no meio do plano, entre dois blocos de `WholeStageCodegen`. Isso confirma que o Spark reconhece a Pandas UDF como um operador próprio e a executa com transporte Arrow. O que ela não prova é o resto do título da seção. O cabeçalho fala em acelerar e distribuir, e a figura mostra "Total Time Across All Tasks: 73 ms" e "Locality Level Summary: Process local: 1". É uma única task, em um único stage, sobre um DataFrame de três linhas. Não há comparação com uma UDF comum e não há distribuição para observar.

**7.** Explique as diferenças entre o `spark-sql` CLI, o Beeline e o Tableau como formas de consultar o Spark. *(Querying with the Spark SQL Shell, Beeline, and Tableau)*

R: O `spark-sql` é um processo local. Ele fala com o Hive metastore em modo local e não passa pelo Thrift server, então quem o usa está sozinho na sua própria sessão. O Beeline é um cliente JDBC que se conecta ao Thrift server pela porta 10000, então ele exige que o servidor esteja no ar e compartilha esse servidor com outros clientes. O Tableau usa o mesmo Thrift server, pelo driver ODBC, e é o caso de uso de BI. Os três formam uma escada: ferramenta de linha de comando isolada, cliente de linha de comando remoto e ferramenta gráfica remota. Só o primeiro dispensa servidor.

**8.** Explique para que serve o Thrift server e o que ele acrescenta a um cluster Spark. *(Using the Spark SQL Shell, Working with Beeline)*

R: Ele transforma o Spark em algo que fala o protocolo de um banco de dados. Sem ele, para consultar dados no Spark é preciso escrever uma aplicação Spark ou abrir um shell. Com ele, qualquer cliente que fale JDBC ou ODBC se conecta e manda SQL, sem saber nada de Spark. O capítulo diz que a implementação corresponde ao HiveServer2 do Hive 1.2.1, o que explica por que o Beeline, que é ferramenta do Hive, funciona sem adaptação. O que ele acrescenta é uma superfície de acesso compartilhada e de longa duração, em vez de uma sessão por aplicação.

**9.** Explique os dois riscos de saturação que o capítulo aponta na leitura JDBC, e por que eles puxam para lados opostos. *(The importance of partitioning)*

R: O primeiro risco é a conexão única do driver. Sem particionar, todo o dado passa por um único canal, que satura e desacelera a extração. Aumentar `numPartitions` resolve esse. O segundo risco é o sistema de origem. Cada partição vira uma conexão JDBC concorrente, então mais partições significam mais carga simultânea no banco. Em um OLTP em produção, isso derruba quem está do outro lado. Os dois puxam para lados opostos porque o remédio de um é a doença do outro. O capítulo dá o ponto de partida pelo lado do Spark, múltiplo do número de workers, e depois manda olhar o que a origem aguenta.

**10.** Explique como o stride é calculado e o que acontece quando os bounds não batem com os dados reais. *(The importance of partitioning)*

R: O stride é a faixa de valores que cada partição pega. Ele sai de `(upperBound - lowerBound) / numPartitions`, que no exemplo dá 10000 dividido por 10, ou seja, 1.000. Cada partição vira uma query com um intervalo dessa largura. Quando os bounds não batem com os dados, as faixas continuam iguais mas os dados não estão distribuídos nelas. O exemplo do capítulo é claro: com `{0, 10000}` e valores só entre 2000 e 4000, oito das dez queries voltam vazias e duas fazem todo o trabalho. O paralelismo pedido existe no plano e não existe na prática.

**11.** Explique os dois custos que separam a Option 1 da Option 2, e por que nenhuma delas é a resposta final do capítulo. *(Option 1: Explode and Collect, Option 2: User-Defined Function)*

R: A Option 1 paga shuffle. O `GROUP BY` que recompõe o array força redistribuição, e além do custo isso perde a ordem original dos elementos. A Option 2 paga serialização. A UDF roda fora do plano otimizado, então os dados precisam sair e voltar, e o capítulo diz que esse processo pode ser caro. Ela ganha da primeira em duas frentes, ordem preservada e menos risco de out-of-memory no `collect_list()`. Nenhuma é a resposta final porque o capítulo continua e apresenta as funções embutidas do Spark 2.4 e as higher-order functions como alternativa às duas.

**12.** Explique por que uma higher-order function bate as duas opções anteriores. *(Higher-Order Functions)*

R: A higher-order function recebe a lambda como argumento e constrói o array de saída aplicando a função a cada elemento. O capítulo diz que isso é similar à abordagem de UDF, mas mais eficiente. A razão que o texto não escreve é que a lambda é uma expressão da própria linguagem de query, não código Python ou Scala externo. Ela não sai do processo e não passa por serialização, e o array nunca é desmontado em linhas, então não há shuffle nem perda de ordem. Ou seja, ela evita o custo da Option 1 e o custo da Option 2 ao mesmo tempo. Esse é o argumento, e o capítulo o entrega em meia frase entre parênteses.

**13.** Explique por que cada window grouping precisa caber em um executor, e o que isso implica na hora de escrever a query. *(Windowing)*

R: Uma window function precisa enxergar todas as linhas de um grupo para calcular o valor de cada linha. Um `dense_rank()` com `PARTITION BY origin` precisa de todas as linhas daquele `origin` juntas para ordenar e numerar. O capítulo diz que o grouping é composto em uma única partição durante a execução, e uma partição vive em um executor. A implicação é de dimensionamento: o tamanho do maior grupo precisa caber na memória de uma máquina. Na hora de escrever a query isso vira uma regra sobre a coluna de `PARTITION BY`. Uma coluna com poucos valores distintos gera grupos enormes, e é isso que o capítulo chama de query ilimitada.

**14.** Explique a diferença entre a Option 1 do capítulo e o `explode()` que o próprio capítulo usa nos exemplos anteriores de outras seções. *(Option 1: Explode and Collect)*

R: A operação de explodir é a mesma nos dois casos, transformar cada elemento de um array em uma linha. A diferença está no que vem depois. Quando o objetivo é ficar em formato tabular, o `explode` sozinho resolve e não há custo de recomposição. A Option 1 do capítulo tem outro objetivo: aplicar uma função e devolver o array original com os elementos alterados. Isso obriga o `collect_list()` e o `GROUP BY` de volta, e é aí que entram o shuffle e a perda de ordem. Ou seja, o custo não está no `explode`. Está em desfazê-lo.

---

## Nível 3 — Aplicação e transferência

Cenários concretos. O capítulo te equipa para responder, mas não responde por você.

**1.** Você precisa ler uma tabela de 200 milhões de pedidos de um PostgreSQL para dentro de um cluster com 8 worker nodes. A tabela tem `order_id` inteiro sequencial, de 1 a 200.000.000. Escreva as quatro opções de particionamento e diga o que checaria antes de rodar. *(The importance of partitioning, Table 5-2)*

R: Pelo capítulo, o ponto de partida para `numPartitions` é um múltiplo do número de workers, então 8 ou 16. Com 16, as opções ficam `partitionColumn` igual a `order_id`, `lowerBound` igual a 1, `upperBound` igual a 200000000 e `numPartitions` igual a 16. O stride sai em torno de 12,5 milhões de linhas por partição.

O que eu checaria antes vem das três dicas do capítulo. Primeira, o mínimo e o máximo reais de `order_id`, porque se a tabela tiver sido purgada e começar em 150 milhões, treze das dezesseis queries voltam vazias. Segunda, se esse PostgreSQL é um OLTP em operação. Se for, dezesseis conexões concorrentes podem saturar a origem, e o capítulo manda reduzir a concorrência nesse caso. Terceira, se `order_id` é uniforme. Sendo sequencial e sem buracos grandes, é o caso mais fácil que existe.

O capítulo não me equipa para duas coisas que eu ainda precisaria decidir: quanto cada partição pesa em memória e o que acontece com as linhas fora dos bounds. Anotei a segunda no item 12 do Nível 5.

**2.** A mesma tabela do cenário anterior, mas agora a chave é um UUID em texto. O que a Table 5-2 permite fazer, e qual saída o próprio capítulo sugere? *(Table 5-2, The importance of partitioning)*

R: A Table 5-2 fecha a porta direta. Ela diz que `partitionColumn` precisa ser uma coluna numérica, de data ou de timestamp, e um UUID em texto não é nenhuma das três.

A saída que o capítulo sugere está na terceira dica, escrita para outro problema. Ele diz que, se possível, dá para gerar uma coluna nova, talvez um hash de várias colunas, para distribuir melhor as partições. Aplicando aqui, eu procuraria outra coluna que sirva, tipicamente um timestamp de criação do pedido, que a tabela quase certamente tem. Se não houver nenhuma, a alternativa é materializar uma coluna numérica derivada na origem.

Uma ressalva: o capítulo apresenta o hash como remédio para skew, não como remédio para tipo. Estou transferindo a dica para um problema que ele não trata, e isso precisa ficar registrado como inferência minha.

**3.** Um catálogo de filmes tem uma coluna `genres` do tipo array de strings. Você precisa deixar cada gênero em maiúsculas, preservando a ordem. Percorra as três abordagens do capítulo e escolha uma. *(Higher-Order Functions in DataFrames and Spark SQL, Higher-Order Functions)*

R: A Option 1 é explodir `genres`, aplicar a função de maiúsculas e recoletar com `collect_list()` e `GROUP BY`. O capítulo elimina essa aqui mesmo: ele diz que o `GROUP BY` exige shuffle e que a ordem do array recoletado não é necessariamente a mesma da original. Como o enunciado exige preservar a ordem, essa abordagem está descartada pelo próprio texto.

A Option 2 é uma UDF que percorre o array. Ela resolve a ordem, e o capítulo diz que é melhor que a primeira por isso, mas paga serialização e desserialização.

A terceira é a higher-order function, `transform(genres, g -> upper(g))`. É a que eu escolho. Ela preserva a ordem, porque nunca desmonta o array, e o capítulo a descreve como similar à UDF mas mais eficiente. O capítulo não mede nenhuma das três, então a escolha é pela argumentação dele, não por número.

**4.** Um log de servidor tem uma coluna `user_agent` que às vezes vem nula, e você precisa de uma UDF que devolva o navegador. Escreva as duas formas seguras que o capítulo recomenda e diga o que acontece sem nenhuma delas. *(Evaluation order and null checking in Spark SQL)*

R: A primeira forma é a UDF null-aware:

```python
def browser(ua):
    if ua is None:
        return None
    return ua.split("/")[0]
```

A segunda é deixar a UDF como está e proteger a chamada com um ramo condicional:

```sql
SELECT CASE WHEN user_agent IS NOT NULL THEN browser(user_agent) END AS browser
  FROM logs
```

Sem nenhuma das duas, escrever `WHERE user_agent IS NOT NULL AND browser(user_agent) = 'Mozilla'` não protege. O capítulo diz que o Spark SQL não garante a ordem de avaliação das subexpressões, então `browser` pode ser chamada antes do teste de nulo. A UDF recebe `None`, o `.split` levanta `AttributeError`, e a task falha. O sintoma engana porque o filtro está escrito e parece estar funcionando.

**5.** Uma cientista de dados quer que os analistas usem o modelo de churn dela direto em SQL. Descreva o caminho que o capítulo indica e o problema operacional que ele deixa em aberto. *(Spark SQL UDFs)*

R: O caminho é o do primeiro parágrafo da seção. Ela envolve o modelo em uma UDF e registra com `spark.udf.register("churn_score", ...)`. A partir daí o analista escreve `SELECT customer_id, churn_score(features) FROM clientes` e não precisa entender o interior do modelo.

O problema em aberto está na frase seguinte do capítulo. As UDFs operam por sessão e não são persistidas no metastore. Então essa função não existe para quem abrir um cliente SQL amanhã. Alguém precisa registrar a UDF na sessão que serve os analistas, o que na prática significa registrá-la no processo que hospeda o Thrift server, no boot. O capítulo mostra o Thrift server em outra seção e nunca liga os dois assuntos.

**6.** Você tem atrasos de voo por aeroporto de origem e destino e quer os três piores destinos por origem. Escreva a query e diga o que mudaria se houvesse 50.000 origens. *(Windowing)*

R: A query é a do capítulo, com `dense_rank()`:

```sql
SELECT origin, destination, TotalDelays, rank
  FROM (
    SELECT origin, destination, TotalDelays,
           dense_rank() OVER (PARTITION BY origin ORDER BY TotalDelays DESC) AS rank
      FROM departureDelaysWindow
  ) t
 WHERE rank <= 3
```

Com 50.000 origens a query fica melhor, não pior, e essa é a parte contraintuitiva. A ressalva do capítulo é que cada window grouping precisa caber em um único executor. Mais origens significam mais grupos e grupos menores, então cada partição fica leve. O risco está no caso oposto: poucas origens e muitas linhas cada. Se eu trocasse `PARTITION BY origin` por `PARTITION BY country` com três países, cada grupo carregaria um terço da tabela para um executor só.

O capítulo não me diz qual é o limite em linhas nem em bytes. Ele diz para limitar o tamanho da window e para de falar.

**7.** Uma analista quer plugar o Tableau no seu cluster Spark. Liste o que precisa estar rodando, com quais parâmetros, e o que você conferiria na máquina dela. *(Working with Tableau)*

R: Do lado do cluster, o Thrift server precisa estar no ar, iniciado com `./sbin/start-thriftserver.sh` a partir de `$SPARK_HOME`. Se o driver e o worker ainda não estiverem rodando, o capítulo manda executar `./sbin/start-all.sh` antes.

Do lado dela, os parâmetros do diálogo Spark SQL são Server, Port 10000, Type `SparkThriftServer`, Authentication `Username`, o login dela como Username e Require SSL desmarcado. No capítulo o Server é `localhost`, porque ele conecta a uma instância local, então no meu caso seria o host do Thrift server.

Na máquina dela eu conferiria a versão do Tableau e o driver ODBC. O capítulo pede o Spark ODBC driver 1.2.0 ou superior, e diz que ele já vem pré-instalado a partir do Tableau 2018.1.

Duas coisas que eu não faria como o capítulo. Ele deixa Require SSL desmarcado e usa senha em branco, e diz explicitamente que isso é por ser instância local. Fora de um laptop, isso é uma porta aberta.

**8.** Você precisa rodar uma consulta pontual num terminal e não quer subir serviço nenhum. Qual ferramenta escolhe, e o que muda em relação à outra? *(Using the Spark SQL Shell, Working with Beeline)*

R: Escolho o `spark-sql`. O capítulo diz que ele se comunica com o serviço de Hive metastore em modo local e que não conversa com o Thrift JDBC/ODBC server. Isso é exatamente o que eu quero, porque não preciso subir nada.

O que muda para o Beeline é que ele é um cliente JDBC. Ele não roda Spark, ele se conecta a um Spark que já está rodando, pelo `!connect jdbc:hive2://localhost:10000`. Sem o Thrift server no ar, o Beeline não tem com quem falar.

Uma consequência que o capítulo não escreve, mas que decorre do texto. O `spark-sql` sobe a própria sessão, então a tabela que eu criar nela vai para o metastore e sobrevive. Uma view temporária, não. E qualquer UDF que eu registrar morre com o processo.

**9.** Sua UDF de Python leva horas sobre 500 milhões de linhas de sensores. O que o capítulo oferece, e o que ele não te conta? *(Speeding up and distributing PySpark UDFs with Pandas UDFs)*

R: O capítulo oferece a Pandas UDF. Ele diagnostica o custo como movimentação de dados entre a JVM e o Python. A solução é transportar em Arrow e operar sobre uma Series em vez de linha a linha. Na prática eu reescreveria a função para receber e devolver `pd.Series` e trocaria `udf` por `pandas_udf`.

O que ele não conta é quanto isso melhora. Não há número, não há comparação medida e não há indicação de quando o ganho é pequeno. Também não conta o caminho que ele mesmo apresenta trinta páginas depois: se a minha função for expressável em funções embutidas ou em higher-order functions, eu não deveria ter UDF nenhuma. Essas duas seções nunca se citam.

**10.** Um dataset aberto tem uma coluna `values` com arrays de inteiros e você precisa somar 1 a cada elemento. Compare as três abordagens usando só o capítulo, e diga qual argumento é o decisivo. *(Option 1, Option 2, Higher-Order Functions)*

R: As três são exatamente as do capítulo, porque esse é o exemplo dele. A Option 1 é `SELECT id, collect_list(value + 1) AS values FROM (SELECT id, EXPLODE(values) AS value FROM table) x GROUP BY id`. A Option 2 é a UDF `addOne` registrada como `plusOneInt`. A terceira é `transform(values, v -> v + 1)`.

O argumento decisivo depende do que eu preciso. Se a ordem dos elementos importa, a Option 1 morre na primeira frase, porque o `GROUP BY` embaralha. Se a ordem não importa, o argumento decisivo vira custo, e aí o capítulo diz que `collect_list()` pode estourar a memória dos executors e que a UDF paga serialização.

A `transform` ganha nas duas dimensões e é o que o capítulo recomenda ao final. O que me incomoda é que o capítulo constrói a comparação inteira entre as duas opções ruins e depois joga a terceira em uma frase.

**11.** Você tem leituras de sensor com uma coluna de mês e precisa de uma coluna por mês, com média e máximo. Escreva o esqueleto e diga qual limitação do exemplo do capítulo você carregaria junto. *(Pivoting)*

R: O esqueleto segue o exemplo:

```sql
SELECT * FROM (
  SELECT sensor_id, month, reading FROM leituras
)
PIVOT (
  CAST(AVG(reading) AS DECIMAL(6, 2)) AS AvgLeitura, MAX(reading) AS MaxLeitura
  FOR month IN (1 JAN, 2 FEB)
)
ORDER BY sensor_id
```

A limitação que eu carrego junto é a lista `FOR month IN`. Ela é escrita à mão, então os doze meses precisam estar enumerados no texto da query, e qualquer valor fora da lista some do resultado sem aviso. O exemplo do capítulo enumera dois meses e a saída mostra `null` onde não havia dado no mês listado.

O capítulo também não diz nada sobre custo. Ele apresenta o pivot como uma conveniência de apresentação e não menciona que a operação agrupa.

**12.** Você precisa escrever um DataFrame de 300 GB em um MySQL. O que o capítulo te dá, e onde ele para? *(JDBC and SQL Databases, Table 5-2, MySQL)*

R: Ele dá a mecânica. O jar do conector no classpath, `.write.format("jdbc")` com `url`, `driver`, `dbtable`, `user` e `password`, e `.save()` no fim. A Table 5-2 diz que `numPartitions` é o número máximo de partições usado para paralelismo tanto na leitura quanto na escrita, e que também determina o máximo de conexões JDBC concorrentes.

Onde ele para é logo depois disso. A seção "The importance of partitioning" inteira é escrita para leitura: o `partitionColumn`, o `lowerBound`, o `upperBound` e o stride só fazem sentido lendo. Para escrita sobra `numPartitions` sozinho, sem nenhuma orientação de valor.

E as três perguntas que decidem a escrita não aparecem em lugar nenhum do capítulo. O que acontece se a tabela já existe. Se a escrita é transacional. Quantas linhas vão por lote. Nem `SaveMode` nem `batchsize` são citados aqui.

---

## Nível 4 — Análise e síntese

Raciocínio que cruza seções. Respostas defensáveis em vez de resposta única, mas todos os ingredientes estão no capítulo.

**1.** A introdução lista as fontes externas que o capítulo vai cobrir. Uma das entradas não é uma fonte de dados. Identifique-a e verifique se o erro se repete.

R: A entrada é o Tableau. A frase é "Connect with external data sources such as JDBC and SQL databases, PostgreSQL, MySQL, Tableau, Azure Cosmos DB, and MS SQL Server". O Tableau não é uma fonte de dados. Ele é o consumidor no outro extremo, uma ferramenta de BI que lê do Spark pelo Thrift server, e o próprio capítulo o trata assim na seção dedicada a ele.

O erro se repete no Summary: "such as SQL databases, PostgreSQL, MySQL, Tableau, Azure Cosmos DB, MS SQL Server, and others". Duas vezes, na abertura e no fecho, o que descarta a hipótese de deslize isolado.

A explicação mais provável é que a lista foi montada a partir do sumário das seções, e não a partir de uma classificação. As seções internas do capítulo são, em ordem, o shell e o Tableau, depois JDBC, PostgreSQL, MySQL, Cosmos DB e MS SQL Server. Coladas em sequência, elas produzem exatamente essa lista errada. O que a lista descreve é o índice, não a categoria.

**2.** Abra a Figure 5-2 e confira a frase que a apresenta. O que acontece?

R: Abri a página 13 do PDF. A frase é: "By default, the Spark SQL option will not be included in the 'To a Server' menu on the left (see Figure 5-2)."

A figura mostra o contrário. O menu "To a Server" lista, de cima para baixo, Tableau Server, MySQL, Oracle, Amazon Redshift, **Spark SQL** e More…. A opção que o texto diz não estar ali está ali, no penúltimo lugar da lista, e é a única que a instrução seguinte manda ir procurar em More….

A explicação é que essa lista do Tableau não é fixa, ela mostra conexões usadas recentemente. A captura de tela foi feita depois de o autor já ter conectado ao Spark SQL, então a máquina dele já não estava no estado padrão que a frase descreve. A frase provavelmente está certa sobre uma instalação nova e a figura documenta outra coisa.

O custo pedagógico é direto. Um leitor iniciante abre o Tableau, vê ou não vê Spark SQL, e em nenhum dos casos consegue conciliar o que lê com o que vê. É captura de tela usada como prova de um estado que ela não captura. É o mesmo problema que anotei no Capítulo 1, sobre números de GitHub em imagem.

**3.** Pegue as assinaturas impressas nas Tables 5-3 e 5-4 e confronte cada uma com a query da linha ao lado. Quantos casos não fecham?

R: Abri as páginas 32, 34 e 35 do PDF, porque o texto extraído embaralha essas tabelas. Três casos não fecham, e dois deles são do mesmo tipo.

**Caso 1, `array_zip`.** A coluna de assinatura imprime `array_zip(array<T>, array<U>, ...): array<struct<T, U, ...>>`. A coluna de query ao lado imprime `SELECT arrays_zip(array(1, 2), array(2, 3), array(3, 4));`. Os dois nomes estão na mesma linha da mesma tabela e diferem por um `s`.

**Caso 2, `map_form_arrays`.** A assinatura imprime `map_form_arrays(array<K>, array<V>): map<K, V>` e a query ao lado imprime `SELECT map_from_arrays(array(1.0, 3.0), array('2', '4'));`. Aqui o erro é uma troca de letras, `form` no lugar de `from`.

**Caso 3, `arrays_overlap`.** A assinatura imprime `arrays_overlap(array<T>, array<T>): array<T>`, a descrição diz "Returns true if array1 contains at least one non-null element also present in array2" e a coluna de saída mostra `true`. O tipo de retorno declarado contradiz a descrição e a saída na mesma linha.

O padrão é informativo. Nos dois primeiros casos a coluna de query está certa e a coluna de assinatura está errada, o que sugere que as queries foram executadas e as assinaturas foram digitadas. No terceiro caso, o tipo de retorno foi copiado do padrão `array<T>` das linhas vizinhas.

**4.** Ainda nas mesmas tabelas, encontre duas entradas cujo problema é de outra natureza, e diga qual.

R: São duas, e as duas são de cópia.

A primeira é a `cardinality`. Ela aparece nas duas tabelas com a mesma assinatura, `cardinality(array<T>): Int`, mas na Table 5-4, que é a tabela de maps, a query ao lado é `SELECT cardinality(map(1, 'a', 2, 'b'));`. A assinatura diz array e o exemplo usa map. A descrição salva a linha, porque diz "returns the size of the given array or a map", mas a assinatura foi transplantada da outra tabela sem ajuste.

A segunda é a `element_at` da Table 5-3, impressa como `element_at(array<T>, Int): T /`. A barra no fim não pertence a nada. A Table 5-4 tem a mesma função com a assinatura de map, `element_at(map<K, V>, K): V`, sem a barra. O resíduo é de uma edição em que as duas variantes estavam na mesma célula.

Somando com a questão anterior, são cinco defeitos em duas tabelas de vinte e uma e cinco linhas. Nenhum deles impede o uso, porque a coluna de query é copiável e correta. Todos eles impedem o uso da coluna de assinatura como referência, que é justamente para o que ela existe.

**5.** A assinatura de `exists()` na seção de higher-order functions tem um problema. Encontre-o e diga por que ele é diferente dos erros das tabelas.

R: Abri a página 38 do PDF. Está impresso `exists(array<T>, function<T, V, Boolean>): Boolean`.

O problema é o `V`. A lambda de `exists` recebe um elemento e devolve um booleano, então a assinatura correta tem dois parâmetros de tipo, `function<T, Boolean>`. O próprio exemplo logo abaixo confirma: `exists(celsius, t -> t = 38)` passa uma lambda de um argumento só.

Esse erro é diferente dos das tabelas por dois motivos. Primeiro, ele está na prosa principal da seção, não em célula de tabela, então não dá para atribuí-lo a copiar e colar entre linhas. Segundo, ele é internamente detectável: as três assinaturas vizinhas são coerentes. `transform(array<T>, function<T, U>): array<U>` e `filter(array<T>, function<T, Boolean>): array<T>` seguem a mesma gramática, e só a de `exists` tem um tipo a mais que não aparece em lugar nenhum da linha.

A leitura mais provável é que o `V` veio de uma assinatura de map, onde a lambda recebe chave e valor. `exists` também existe para map em outros dialetos, e a assinatura de array herdou o parâmetro do parente errado.

**6.** Reconstrua a escada de custo que o capítulo monta para manipular arrays, do mais caro ao mais barato, e diga o que ele nunca faz.

R: São quatro degraus, todos com a justificativa do próprio texto.

| Degrau | Custo declarado | Onde o capítulo diz |
|---|---|---|
| Explode e collect | shuffle do `GROUP BY`, perda de ordem, risco de out-of-memory no `collect_list()` | "could be very expensive" |
| UDF | serialização e desserialização | "may be expensive" |
| Funções embutidas para tipos complexos | nenhum declarado | "Instead of using these potentially expensive techniques" |
| Higher-order function | nenhum declarado | "similar to the UDF approach, but more efficiently" |

O que o capítulo nunca faz é medir. Não há um único número em nenhum dos quatro degraus. As palavras que sustentam a escada inteira são "very expensive", "may be expensive", "potentially expensive" e "more efficiently", e nenhuma delas tem unidade.

Há uma segunda ausência, mais grave que a primeira. O capítulo nunca diz que a diferença é de natureza, e não de grau. Os dois primeiros degraus tiram o dado do plano otimizado, um por linha e outro por processo. Os dois últimos ficam dentro dele. Essa é a fronteira que explica a escada, e ela não aparece.

E uma terceira: a Pandas UDF, que é a solução do capítulo para o custo de UDF na primeira seção, não aparece nessa escada. As duas discussões de custo do capítulo acontecem a trinta páginas de distância e nunca se cruzam.

**7.** A UDF `cubed` da primeira seção é testada contra a regra de null checking que a seção seguinte enuncia. Ela passa?

R: Não passa, e a sequência das duas seções é o que torna isso visível.

A versão Python é `def cubed(s): return s * s * s`, registrada com `LongType()`. Se `s` chegar como `None`, `None * None` levanta `TypeError`. A versão Scala, `(s: Long) => s * s * s`, tem outro comportamento, porque `Long` é primitivo e o Spark converte nulo em zero, então ela devolve 0 em silêncio. Duas linguagens, dois modos diferentes de errar, nenhum deles mencionado.

Duas páginas depois o capítulo escreve a regra: "Make the UDF itself null-aware and do null checking inside the UDF". A UDF que ele acabou de mostrar não faz isso.

Isso é defensável como escolha didática, porque a primeira seção é sobre sintaxe e o exemplo usa `spark.range(1, 9)`, que não produz nulo. O que não é defensável é a ordem. A regra vem depois do exemplo e nunca volta para corrigi-lo. Um leitor que copie o padrão da primeira seção leva o defeito para dados reais, e o aviso já ficou para trás quando isso acontecer.

**8.** A tabela `people` do Spark SQL shell tem uma linha com idade nula, e o capítulo faz duas queries sobre ela. Cruze isso com a seção de null checking.

R: As duas queries são `SELECT * FROM people WHERE age < 20`, que devolve só Samantha, e `SELECT name FROM people WHERE age IS NULL`, que devolve Michael.

A primeira é a interessante. Michael tem `age` nula e não aparece no resultado, o que está certo em SQL, porque `NULL < 20` é desconhecido e não verdadeiro. O capítulo não comenta. Ele apenas mostra o resultado e passa adiante.

O cruzamento com a seção de null checking é revelador. Ali o capítulo avisa que o Spark SQL não garante a ordem de avaliação de subexpressões, um risco que só aparece com UDF envolvida. Aqui, com uma função embutida e um operador embutido, o nulo se comporta segundo a regra de três valores do SQL e ninguém precisa proteger nada.

A lição que o capítulo tinha em mãos e não escreveu é a fronteira. Expressões que o engine conhece já tratam nulo. O problema começa quando se injeta código que ele não conhece. Essa frase resumiria a seção de null checking inteira, e as duas queries de `people` seriam a demonstração natural dela.

**9.** As três exibições da tabela `people` no capítulo saem em três ordens diferentes. Liste-as e decida o que isso ensina.

R: Abri a página 17 do PDF para a terceira.

| Onde | Ordem |
|---|---|
| Inserts, no `spark-sql` | Michael, Andy, Samantha |
| `SELECT * FROM people` no Beeline | Samantha, Andy, Michael |
| Figure 5-8, no Tableau | Michael, Samantha, Andy |

Três leituras da mesma tabela de três linhas, três ordens. Nenhuma das queries tem `ORDER BY`.

O que isso ensina é a regra que o capítulo não enuncia: sem `ORDER BY` não existe ordem. As três linhas foram gravadas por três `INSERT` separados, então provavelmente estão em arquivos separados no warehouse, e a ordem de leitura depende de como as tarefas foram escalonadas naquela execução.

Vale registrar que isso não é erro do livro. As três saídas são honestas, cada uma reproduz o que aconteceu na máquina do autor. O que falta é a frase de duas linhas dizendo que a variação é esperada. Sem ela, o leitor cuidadoso, que compara as três, fica sem explicação, e o leitor desatento aprende a confiar em uma ordem que não existe.

**10.** As dez queries que o capítulo escreve para explicar o stride têm um problema aritmético. Encontre-o.

R: As faixas se sobrepõem nas bordas. O capítulo escreve `BETWEEN 0 and 1000`, depois `BETWEEN 1000 and 2000`, e assim por diante até `BETWEEN 9000 and 10000`.

`BETWEEN` em SQL é inclusivo nas duas pontas. Uma linha com `partitionColumn` igual a 1000 satisfaz a primeira query e a segunda. Executadas como estão, as dez queries devolvem em duplicata todas as linhas nos nove valores de fronteira.

Isso não descreve o que o Spark faz. Um particionador que duplicasse linhas nas bordas estaria quebrado, então a implementação real precisa usar predicados exclusivos de um dos lados. O capítulo está simplificando para explicar o stride, e a simplificação introduziu um erro que o próprio texto não sinaliza.

O tamanho do dano depende do dado. Com uma chave contínua, nove linhas duplicadas em milhões. Com uma chave inteira de baixa cardinalidade, muito mais. De qualquer forma, o ponto pedagógico não é a duplicata, é que o capítulo apresenta como equivalência exata algo que ele não conferiu. A frase é "This is the equivalent of executing these 10 queries", e não é.

**11.** O capítulo diz que `lowerBound` e `upperBound` definem o stride, e ilustra o ajuste com o caso `{0, 4000}`. O que um leitor conclui daí que está errado?

R: A conclusão natural é que os bounds filtram. O capítulo diz que com `{numPartitions:10, lowerBound: 0, upperBound: 10000}` e valores reais entre 2000 e 4000 "only 2 of the 10 queries will be doing all of the work", e recomenda trocar para `{0, 4000}`. Lendo isso junto com as dez queries `BETWEEN`, é inevitável entender que os bounds delimitam a faixa lida.

Não delimitam. Eles só calculam o passo. As linhas fora da faixa continuam sendo lidas, empilhadas nas partições das pontas.

O capítulo nunca escreve isso. A Table 5-2 diz que `lowerBound` "sets the minimum value of partitionColumn for the partition stride", e a palavra "stride" está lá, mas nenhuma frase diz que nada é filtrado. Como o exemplo das dez queries `BETWEEN` sugere fortemente o contrário, o leitor sai com o modelo errado.

O erro tem consequência prática de dois tipos. Quem acredita no filtro pode usar os bounds como se fossem um `WHERE` e não entender por que o volume lido não caiu. E quem estreita os bounds achando que está reduzindo trabalho pode, na verdade, estar concentrando todo o excedente em duas partições. *Verifiquei a redação atual da documentação no item 12 do Nível 5.*

**12.** A seção de Azure Cosmos DB fica dentro de "External Data Sources", ao lado de PostgreSQL, MySQL e MS SQL Server. Ela pertence ali?

R: Não, e o texto do próprio capítulo denuncia.

A seção de abertura de "External Data Sources" diz "starting with JDBC and SQL databases", e a subseção seguinte é sobre a data source API de JDBC. PostgreSQL, MySQL e MS SQL Server são três instâncias dessa API: os três usam `.format("jdbc")` ou o método `.jdbc()`, os três recebem `url`, `dbtable`, `user` e `password`.

O Cosmos DB não usa nada disso. O trecho dele importa `com.microsoft.azure.cosmosdb.spark`, monta um `Config(Map(...))` com `Endpoint`, `Masterkey`, `Database`, `Collection` e `PreferredRegions`, e chama `spark.read.cosmosDB(readConfig)`. Não há `jdbc` em lugar nenhum do trecho.

Mesmo assim a frase que apresenta o trecho fala em JDBC: "how to load data from and save it to an Azure Cosmos DB database using the Spark SQL data source API and JDBC". A menção é errada. É a mesma frase de modelo das seções vizinhas, com o nome do banco trocado.

O que o Cosmos DB de fato exemplifica é a outra metade do assunto: um conector de terceiro, distribuído como jar ou por coordenadas Maven, com API própria. Isso merecia enquadramento explícito, porque é o mecanismo pelo qual quase toda fonte moderna entra no Spark. O capítulo o esconde dentro de uma seção sobre JDBC.

**13.** A coluna "DataFrame API" da Table 5-5 nunca aparece em nenhum exemplo do capítulo. Isso importa?

R: Importa muito, e por dois motivos que se somam.

O primeiro é de verificação. Todos os exemplos de windowing do capítulo são SQL: a criação de `departureDelaysWindow`, as três queries por origem e a query com `dense_rank()`. A coluna da esquerda da tabela, portanto, é demonstrada. A coluna da direita não é exercitada em nenhuma linha de código do capítulo, então nada dentro do texto a confere.

O segundo é a forma dos nomes. A coluna imprime `denseRank()`, `percentRank()`, `rowNumber()`, `cumeDist()`, `firstValue()` e `lastValue()`, todos em camelCase. Os quatro primeiros são nomes reais da API de Scala da era 1.x. Os dois últimos eu não consegui encontrar em versão nenhuma. É um conjunto suspeito: nomes de uma geração antiga misturados com nomes que parecem inventados por analogia.

A combinação dos dois motivos é o que torna a tabela perigosa. Ela é o único lugar do capítulo que promete uma tradução SQL para DataFrame, e essa promessa não é testada em lugar nenhum. *No item 7 do Nível 5 verifiquei quando esses nomes deixaram de existir.*

**14.** O título do capítulo é "Interacting with External Data Sources". Meça quanto do capítulo é sobre isso.

R: Pelas páginas do PDF, o capítulo tem 52 páginas. As seções de fontes externas propriamente ditas, de "External Data Sources" na página 18 até o fim de "Other External Sources" na página 28, ocupam onze páginas. As seções de cliente de consulta, do shell ao Tableau, das páginas 8 a 18, ocupam dez. As UDFs ocupam as páginas 1 a 8, e as higher-order functions mais os operadores relacionais ocupam da 28 até a 52, ou seja, vinte e quatro páginas, quase metade.

Ou seja, a maior parte do capítulo é sobre operações internas: manipular arrays e maps, unir, juntar, janelar, adicionar coluna e pivotar. Nada disso tem qualquer relação com fonte externa.

Duas leituras são possíveis. A generosa é que o Capítulo 5 é o depósito do que sobrou do Capítulo 4, que cobriu as fontes embutidas, e o título nomeia a primeira metade. A menos generosa é que o Summary confirma a bagunça, porque ele lista seis assuntos e só dois cabem sob o título.

O efeito prático é sobre a busca. Quem procurar "higher-order functions" ou "window functions" em um índice de livro não vai adivinhar que estão em um capítulo sobre fontes externas. Um leitor que precise de `transform` vai passar direto por aqui.

**15.** Confira a aritmética dos três exemplos de higher-order function. Ela fecha?

R: Fecha, e eu conferi as três.

**`transform`.** A expressão é `((t * 9) div 5) + 32`, com `div` de divisão inteira. Para 35: 315 dividido por 5 é 63, mais 32 dá 95. Para 36: 324 dividido por 5 dá 64, mais 32 dá 96. Para 32: 288 dividido por 5 dá 57, mais 32 dá 89. Para 30: 270 dividido por 5 é 54, mais 32 dá 86. A saída impressa é `[95, 96, 89, 86, ...]`. Bate.

**`filter`.** Com `t > 38`, a primeira linha `[35, 36, 32, 30, 40, 42, 38]` devolve `[40, 42]`, e o 38 fica de fora porque a comparação é estrita. A segunda linha devolve `[55, 56]`. Bate.

**`reduce`.** Primeira linha: soma 253, `size` 7, 253 dividido por 7 dá 36, vezes 9 dá 324, dividido por 5 dá 64, mais 32 dá 96. Segunda linha: soma 208, `size` 5, resultado 41, vezes 9 dá 369, dividido por 5 dá 73, mais 32 dá 105. As saídas impressas são 96 e 105. Bate.

Escrevo a queda porque eu suspeitava de duas coisas e as duas caíram. Suspeitei da precedência entre `div` e `*` na expressão `acc div size(celsius) * 9 div 5`, e ela se comporta como associativa à esquerda, o que produz os números certos. E suspeitei de que a divisão inteira introduzisse erro visível na média, mas o arredondamento para baixo em cada passo dá exatamente os valores impressos. As três saídas do capítulo estão corretas.

**16.** Na função de finalização do `reduce`, a lambda usa `size(celsius)`. O que essa referência tem de incomum?

R: Ela sai de dentro da lambda e alcança uma coluna da linha. A expressão completa é `acc -> (acc div size(celsius) * 9 div 5) + 32`, e `acc` é o parâmetro da lambda enquanto `celsius` é a coluna da tabela `tC`.

Isso é uma captura de escopo externo dentro de uma higher-order function. Ela funciona, porque a saída impressa está correta, e é o que torna a expressão possível: a média exige dividir pela contagem, e a contagem não está no buffer acumulado.

O que é incomum é o capítulo não comentar. Ele acabou de definir a assinatura como `function<B, R>`, uma função de um argumento, e o exemplo imediatamente abaixo usa dois valores, um deles vindo de fora da assinatura. Sem esse comentário, a assinatura parece contradizer o exemplo.

A mesma expressão poderia ter sido escrita acumulando a contagem dentro do buffer. Essa seria a forma canônica de um `reduce`. O capítulo escolhe o atalho e não avisa que é atalho.

**17.** A seção de union diz que a duplicação era esperada. Reconstrua por que, e diga o que o exemplo demonstra mal.

R: A duplicação vem da construção. `foo` foi criado como um filtro sobre `delays`, então as três linhas de `foo` já estão dentro de `delays`. `bar = delays.union(foo)` soma o conjunto inteiro com um subconjunto dele, então essas três linhas passam a existir duas vezes. O filtro aplicado sobre `bar` devolve seis linhas, os três originais e as três cópias.

O que o exemplo demonstra bem é que `union` no Spark não deduplica. Ele é concatenação, não união de conjuntos, e quem espera comportamento de `UNION` do SQL padrão se surpreende.

O que ele demonstra mal é o caso de uso. A frase de abertura da seção diz que um padrão comum é unir dois DataFrames diferentes com o mesmo schema. O exemplo então une um DataFrame com um pedaço de si mesmo, o que não é o padrão descrito e não é algo que alguém faria. Um exemplo com dois arquivos de meses diferentes mostraria o padrão real e ainda deixaria a lição sobre duplicatas disponível, bastando repetir uma linha.

Há ainda uma omissão de contraste. O capítulo não menciona `union` contra `unionByName`, e como os dois DataFrames aqui têm o mesmo schema na mesma ordem, o exemplo esconde o problema clássico de `union` por posição.

**18.** Junte a seção de UDFs e a seção de higher-order functions e reescreva a recomendação do capítulo em uma ordem só.

R: O capítulo dá as peças em duas seções separadas por trinta páginas e nunca as junta. Juntas, elas produzem uma ordem de decisão de quatro passos.

1. Existe função embutida que faça isso? Se sim, use. O capítulo diz "instead of using these potentially expensive techniques, you may be able to use some of the built-in functions for complex data types". A Table 5-3 tem vinte e uma delas.
2. O que falta é aplicar uma expressão a cada elemento de um array ou de um map? Se sim, use higher-order function. O capítulo diz que é similar à UDF, mas mais eficiente.
3. Precisa mesmo de código de linguagem hospedeira, por exemplo para chamar um modelo de machine learning? Aí sim, UDF. É exatamente o exemplo com que o capítulo abre.
4. É UDF em Python sobre volume grande? Então Pandas UDF, com Arrow e operação sobre Series em vez de linha a linha.

Escrita assim, a recomendação é coerente e cada frase vem do texto. O que o capítulo faz é o inverso. Ele abre pelo passo 3 e resolve o passo 4 em seguida. Só chega aos passos 1 e 2 quinze seções depois, num contexto sobre tipo complexo e não sobre custo de UDF. Um leitor que precise de UDF lê a primeira seção, resolve o problema e nunca chega na parte que diz que ele talvez não precisasse de UDF nenhuma.

**19.** O capítulo mostra um único listing truncado. Encontre-o e diga o que falta.

R: Abri a página 29 do PDF. É o registro da UDF `plusOneInt`, na Option 2. A linha impressa termina na margem direita:

```scala
val plusOneInt = spark.udf.register("plusOneInt", addOne(_: Seq[Int]): Seq
```

O texto está cortado, não abreviado com reticências. Pelo contexto, a linha completa termina com `[Int])`, fechando a anotação de tipo de retorno e o parêntese da chamada de `register`. Estou completando a linha por inferência, e marco isso como reconstrução minha, não como leitura.

Um segundo listing tem o mesmo sintoma em outro lugar do capítulo. As duas chamadas de `join` terminam em `.show` sem o parêntese, cortadas no mesmo ponto da margem.

O que isso me ensina sobre o material é operacional. A largura de linha do layout deste livro não comporta as linhas mais longas de Scala, e o corte é silencioso. Antes de copiar qualquer linha de Scala daqui, preciso conferir se ela fecha os parênteses.

**20.** Escolha as três lacunas deste capítulo que mais me custam, e diga onde cada uma deveria estar.

R:

**Primeira, o custo real de uma UDF.** O capítulo diz "may be expensive" e para. A informação que falta é por quê: uma UDF de Python é uma caixa preta para o otimizador, então ela bloqueia predicate pushdown e obriga a materializar os valores. Isso deveria estar na Option 2, ao lado da frase sobre serialização, porque é ali que a comparação com a alternativa é feita.

**Segunda, a fronteira entre o que o engine enxerga e o que ele não enxerga.** É o conceito que unifica quatro seções deste capítulo. Ele explica por que a ordem de avaliação não é garantida e por que a UDF é cara. Explica também por que a Pandas UDF é menos cara e por que a higher-order function é mais barata que as duas. O capítulo dá as quatro consequências e nunca a causa. Ela deveria estar logo na abertura de "User-Defined Functions", em um parágrafo.

**Terceira, o que fazer quando a leitura JDBC não é particionável.** A Table 5-2 exige coluna numérica, de data ou de timestamp. Uma parte grande das tabelas reais tem chave em texto. O capítulo dá a exigência e não dá saída nenhuma. Isso deveria estar em "The importance of partitioning", junto das três dicas, porque é o caso em que as três dicas não se aplicam.

Uma quarta que quase entrou: o capítulo não menciona segurança em nenhum ponto útil. Ele conecta Beeline sem senha e Tableau sem SSL, justifica os dois como instância local, e nunca diz o que muda fora do laptop. Deixei de fora porque o capítulo é honesto sobre o escopo local, mas anotei.

---

## Nível 5 — Além do capítulo (backlog, não notas)

O capítulo é de 2020 e foi escrito contra o Spark 3.0. Os pontos que decaem com o tempo aqui são seis. A API de UDF, o nome das funções, o Thrift server, o metastore, as opções JDBC e as capturas de tela do Tableau. Verifiquei tudo contra fonte primária em **3 de agosto de 2026**, quando a versão corrente da documentação era a **4.2.0**. As URLs estão ao fim da seção.

**1.** **Pandas UDFs hoje.** O capítulo lista quatro casos de type hint e três Pandas Function APIs, no Spark 3.0. Confira se a lista mudou e o que aconteceu com a API antiga de `PandasUDFType`.

R: As duas listas estão **iguais**, seis anos depois. A página de Arrow e Pandas da documentação 4.2.0 traz exatamente os quatro casos que o capítulo nomeia. São Series to Series, Iterator of Series to Iterator of Series, Iterator of Multiple Series to Iterator of Series e Series to Scalar. E as Pandas Function APIs continuam sendo três: Grouped Map, Map e Co-grouped Map. Nada foi acrescentado nem removido nessas duas listas.

O `PandasUDFType` **não foi removido**. A documentação ainda diz que "using `pyspark.sql.functions.PandasUDFType` will be deprecated in the future release". Ou seja, seis anos depois do Spark 3.0 ele nem chegou a ser depreciado, só ameaçado. A classe continua em `python/pyspark/sql/pandas/functions.py` na tag `v4.2.0`.

O que cresceu ao lado, e é o que o capítulo não podia ter, são as **Arrow Function APIs**, em página separada. Elas espelham as três Pandas Function APIs com `mapInArrow()`, `applyInArrow()` e a versão cogroup, e o `applyInArrow` entrou no **Spark 4.0.0**, por SPARK-40559.

Ou seja, a parte que o capítulo cobre envelheceu muito pouco. O que aconteceu foi crescimento em volta.

**2.** **UDF comum de Python hoje.** O capítulo diz que a UDF de PySpark é lenta por causa da movimentação entre JVM e Python. Descubra se isso ainda é o comportamento padrão.

R: **Não é mais, e a mudança é recente.** Desde o Spark 4.2 a UDF comum de Python já é otimizada com Arrow por padrão.

A linha do tempo verificada:

- As Arrow-optimized Python UDFs entraram no **Spark 3.4.0**, por SPARK-40307, atrás da chave `spark.sql.execution.pythonUDF.arrow.enabled`.
- Li o `SQLConf.scala` na tag **v4.0.0**, na linha 3490, e o bloco termina em `.createWithDefault(false)`. Na v4.1.0 também é `false`.
- Na tag **v4.2.0** o mesmo bloco termina em `.createWithDefault(true)`. A virada foi por **SPARK-54555**, "Enable Arrow-optimized Python UDFs and Arrow-based PySpark IPC by default".
- A mesma release virou `spark.sql.execution.arrow.pyspark.enabled` para `true`, e o guia de migração da 4.2.0 registra as duas mudanças.

O diagnóstico do capítulo continua correto como física. A documentação 4.2.0 ainda diz que "Scalar Python UDFs rely on cloudpickle for serialization and deserialization, and encounter performance bottlenecks, particularly when dealing with large data inputs and outputs". O que mudou é que esse caminho virou o legado, e o caminho rápido virou o padrão.

Consequência para mim: o conselho do capítulo, trocar UDF comum por Pandas UDF para ganhar velocidade, perdeu boa parte da força em 2026. O ganho já vem de graça.

**3.** **Tipos de UDF que o capítulo não podia conhecer.** Levante o que apareceu depois de 2020.

R: Quatro coisas, todas posteriores ao livro.

- **Python UDTF**, user-defined table function, no **Spark 3.5**, por SPARK-43798 sob a guarda-chuva SPARK-43797. Diferente da UDF, ela devolve uma tabela e não um valor. Desde a 4.2 ela também é otimizada com Arrow por padrão, com `spark.sql.execution.pythonUDTF.arrow.enabled` em `true`.
- **Python Data Source API**, no **Spark 4.0**, por SPARK-44076. Permite escrever fonte e sink customizados em Python. É a resposta moderna para o problema que o capítulo resolve com conector Java de terceiro.
- **Native Arrow UDFs**, o decorator `arrow_udf`, no **Spark 4.1.0**. Operam direto sobre `pyarrow.Array`, sem converter para Pandas. São seis formas, contra as quatro das Pandas UDFs.
- **Vectorized Python UDTF**, o `@arrow_udtf`, também no **Spark 4.1**, por SPARK-52980.

O padrão que fica: o capítulo apresenta a Pandas UDF como o topo da escada de UDF em Python, e em 2026 ela é o degrau do meio.

**4.** **A função `reduce()`.** Descubra desde quando esse nome existe no Spark SQL. A resposta muda como eu leio o exemplo do capítulo.

R: **Desde o Spark 3.4.0**, e o capítulo é de 2020.

O nome `reduce` é um alias de `ArrayAggregate`, acrescentado por **SPARK-41778**, "Add an alias 'reduce' to ArrayAggregate", commit `ccbd9a7b` de 30 de dezembro de 2022. Conferi a linha no `FunctionRegistry.scala` da tag `v4.2.0`:

```scala
expression[ArrayAggregate]("reduce", setAlias = true, Some("3.4.0")),
```

E conferi a ausência nas tags anteriores. Em `v2.4.0`, `v3.0.0` e `v3.3.0` o registro tem `expression[ArrayAggregate]("aggregate")` e nenhuma linha para `reduce`. A página de collection functions da 4.2.0 confirma, com `Since: 2.4.0` para `aggregate` e `Since: 3.4.0` para `reduce`.

Então, em Spark open source 3.0, a query do capítulo teria falhado com erro de função não resolvida. O nome disponível em 2020 era `aggregate`, com a mesma assinatura.

Isso muda como eu leio o exemplo. O capítulo imprime a saída, 96 e 105, e eu conferi a aritmética no item 15 do Nível 4, então o resultado é real. A hipótese que sobra é que o código rodou em Databricks Runtime, e não em Spark open source. Isso combina com os caminhos `/databricks-datasets/` que o capítulo usa nos exemplos de voos. **Marco isso como hipótese minha, não como fato verificado.** O fato verificado é a data de entrada no projeto Apache.

Nota de método: a instrução original de pesquisa apontava SPARK-41232 como candidato, e esse ticket é outro, "High-order function: array_append". O ticket certo é o SPARK-41778.

**5.** **Higher-order functions embutidas hoje.** O capítulo detalha quatro. Levante a lista completa e as versões.

R: São doze funções que recebem lambda, e o capítulo cobre quatro delas. Todas confirmadas na página de collection functions da 4.2.0 e no `FunctionRegistry.scala` da tag `v4.2.0`.

| Função | Assinatura na doc | Since |
|---|---|---|
| `transform` | `transform(expr, func)` | 2.4.0 |
| `filter` | `filter(expr, func)` | 2.4.0 |
| `exists` | `exists(expr, pred)` | 2.4.0 |
| `aggregate` | `aggregate(expr, start, merge, finish)` | 2.4.0 |
| `zip_with` | `zip_with(left, right, func)` | 2.4.0 |
| `array_sort` com comparador | `array_sort(expr, func)` | 2.4.0 |
| `forall` | `forall(expr, pred)` | 3.0.0 |
| `map_filter` | `map_filter(expr, func)` | 3.0.0 |
| `map_zip_with` | `map_zip_with(map1, map2, function)` | 3.0.0 |
| `transform_keys` | `transform_keys(expr, func)` | 3.0.0 |
| `transform_values` | `transform_values(expr, func)` | 3.0.0 |
| `reduce` | `reduce(expr, start, merge, finish)` | 3.4.0 |

Duas leituras. A primeira é que o capítulo escolheu bem: as quatro que ele detalha cobrem mapear, filtrar, testar e reduzir, que são os quatro verbos básicos.

A segunda é que ele deixou de fora o lado dos maps inteiro. `map_filter`, `map_zip_with`, `transform_keys` e `transform_values` são de 3.0.0, ou seja, já existiam quando o livro foi escrito. O capítulo tem uma tabela dedicada a funções de map, e nenhuma delas aparece lá. Também deixou de fora o `forall`, que é o par natural do `exists` que ele detalha.

A doc também confirma a assinatura de `exists`: `exists(expr, pred)`, com predicado unário. A versão impressa no capítulo, com três parâmetros de tipo, está errada, como levantei no item 5 do Nível 4.

**6.** **Nomes e tipos de retorno das Tables 5-3 e 5-4.** Arbitre os cinco defeitos que encontrei no Nível 4.

R: Os cinco caem contra a documentação 4.2.0.

- **`arrays_zip` é o nome certo.** A página de array functions documenta `arrays_zip(a1, a2, ...)`, `Since: 2.4.0`. Não existe `array_zip`, e a string aparece zero vezes no `sql-expression-schema.md` gerado da tag `v4.2.0`.
- **`map_from_arrays` é o nome certo.** A página de map functions documenta `map_from_arrays(keys, values)`, `Since: 2.4.0`. `map_form_arrays` também aparece zero vezes no mesmo arquivo gerado.
- **`arrays_overlap` devolve boolean.** A prova é gerada por máquina: `sql-expression-schema.md`, linha 37, traz `struct<arrays_overlap(array(1, 2, 3), array(3, 4, 5)):boolean>`. No código, a classe estende `Predicate`, que fixa `dataType = BooleanType`. O `array<T>` impresso no livro é erro.
- **`cardinality` devolve int**, então esse pedaço do livro está certo. O que a doc acrescenta é que ela é um alias de `size`. O comportamento com nulo mudou. Sob ANSI mode, que é o padrão desde o Spark 4.0, `cardinality(NULL)` devolve `null`. Devolver `-1` só acontece com `spark.sql.ansi.enabled` em `false` e `spark.sql.legacy.sizeOfNull` em `true`.
- O `div` que o capítulo usa sem nomear está documentado na página de math functions, `Since: 3.0.0`, e o schema gerado mostra que ele devolve **bigint**, não int.

Ou seja, dos cinco defeitos que apontei lendo o PDF, quatro se confirmam como erro do livro. O quinto, a `cardinality`, era só uma assinatura mal copiada com o tipo certo.

**7.** **A coluna "DataFrame API" da Table 5-5.** Descubra quando esses nomes existiram.

R: Eles não existiam em 2020, e é pior do que eu esperava.

Conferi o `functions.scala` em várias tags. Em **v1.6.0** existem `cumeDist()`, `denseRank()`, `percentRank()` e `rowNumber()`, cada um com a anotação `@deprecated("Use dense_rank. This will be removed in Spark 2.0.", "1.6.0")`. Em **v2.0.0** os quatro sumiram. Ou seja, eles foram depreciados em 2015 e removidos em 2016, quatro anos antes do livro.

Os outros dois são piores. Procurei `firstValue` e `lastValue` nas tags v1.4.0 e v1.6.0 e não encontrei nenhum dos dois. Eles não são nomes antigos, parecem nomes construídos por analogia. Os nomes reais, `first_value` e `last_value`, só entraram no `functions.scala` na tag **v3.5.0**, então em 2020 não havia equivalente de DataFrame API nenhum para essas duas linhas.

Hoje, na referência do PySpark 4.2.0, os nomes disponíveis são `rank`, `dense_rank`, `percent_rank`, `ntile`, `row_number`, `cume_dist`, `first_value`, `last_value`, `lag`, `lead` e `nth_value`. Todos em snake_case, nenhum em camelCase.

Fecha o item 13 do Nível 4. A coluna que o capítulo nunca exercita em nenhum exemplo é a que está errada, e é errada de duas formas ao mesmo tempo.

**8.** **Thrift server hoje.** O capítulo diz que ele corresponde ao HiveServer2 do Hive 1.2.1. Confira o que a documentação diz agora.

R: A frase mudou e ficou sem número. A página de Distributed SQL Engine da 4.2.0 diz: "The Thrift JDBC/ODBC server implemented here corresponds to the `HiveServer2` in built-in Hive." O "in Hive 1.2.1" virou "in built-in Hive".

Qual é a Hive embutida hoje: **2.3.10**. Li o `pom.xml` da tag `v4.2.0`, linha 141, `<hive.version>2.3.10</hive.version>`. A documentação confirma pelo outro lado, ao descrever `spark.sql.hive.metastore.jars=builtin` como "Use Hive 2.3.10, which is bundled with the Spark assembly".

O resto continua igual ao capítulo. O script é `./sbin/start-thriftserver.sh`, a porta é 10000, e a conexão é `!connect jdbc:hive2://localhost:10000`. Não há aviso de deprecação, e o servidor continua recebendo trabalho: as notas do 4.2.0 listam SPARK-55530 e SPARK-53469 sobre ele.

O `spark-sql` CLI também sobreviveu, com a mesma restrição. A doc atual diz "Note that the Spark SQL CLI cannot talk to the Thrift JDBC server". A diferença é de redação. O capítulo diz que o CLI se comunica com o metastore "in local mode". A doc atual diz que o CLI **roda** o serviço de Hive metastore.

**9.** **Spark Connect.** Descubra o que é, quando entrou e se ele muda o quadro de acesso remoto que o capítulo desenha.

R: Entrou no **Spark 3.4**. A documentação o descreve como uma arquitetura cliente-servidor desacoplada. Ela permite conectividade remota a clusters Spark usando a DataFrame API e planos lógicos não resolvidos como protocolo.

Sobre o quadro do capítulo, a resposta honesta é: **ele não substitui o Thrift server, e a documentação não diz que substitui**. Li a página de overview e ela não menciona JDBC, ODBC, Thrift nem ferramenta de BI em lugar nenhum. Os dois mecanismos coexistem na 4.2.0.

O que mexe no quadro é outra coisa. O **Spark 4.1.0 trouxe um driver JDBC para o Spark Connect**, por SPARK-53484, listado nos destaques daquela release. Conferi na tag `v4.2.0`: existe o módulo `sql/connect/client/jdbc`, a classe registrada é `org.apache.spark.sql.connect.client.jdbc.SparkConnectDriver`, e o esquema de URL é `jdbc:sc://`.

O detalhe que fecha o item: esse driver está no código e **não está na documentação**. As buscas por `jdbc:sc`, `SparkConnectDriver` e `connect-client-jdbc` em todos os arquivos de documentação da tag `v4.2.0` devolvem zero. Ou seja, existe hoje um segundo caminho JDBC para o Spark, e quem só ler a documentação não fica sabendo.

**10.** **Hive metastore e warehouse.** O capítulo mostra `/user/hive/warehouse/people`. Isso ainda é o padrão?

R: **Não é, e a string sumiu da documentação inteira.** A busca por `user/hive/warehouse` em todas as páginas da 4.2.0 devolve zero ocorrências.

O padrão atual, pela página de Hive Tables: o Spark "creates a directory configured by `spark.sql.warehouse.dir`, which defaults to the directory `spark-warehouse` in the current directory that the Spark application is started". Ou seja, é relativo ao diretório de onde a aplicação sobe, e não um caminho absoluto no sistema.

A mesma página avisa que `hive.metastore.warehouse.dir`, do `hive-site.xml`, está depreciada **desde o Spark 2.0.0**. Isso já era verdade quando o livro foi escrito.

As versões de metastore suportadas hoje: `spark.sql.hive.metastore.version` tem default **2.3.10**, e as faixas aceitas são 2.0.0 a 2.3.10, 3.0.0 a 3.1.3 e 4.0.0 a 4.1.0. O suporte ao metastore do Hive 4.1 entrou no Spark 4.1.0, por SPARK-53095.

**11.** **Catálogos modernos.** O capítulo assume Hive metastore e não conhece outra coisa. Descubra qual é o mecanismo de hoje.

R: É o `CatalogPlugin` da Data Source V2, e ele ganhou **página própria na documentação da 4.2.0**. Conferi que essa página não existia na 4.1.3, então a documentação é nova.

O mecanismo: "`CatalogPlugin` is a marker interface for catalog implementations. After instantiation, Spark calls `initialize(name, options)` with the catalog name and all configuration properties that share the prefix `spark.sql.catalog.<name>.`". O registro é `spark.sql.catalog.<name>=com.example.MyCatalog`.

As interfaces de mistura documentadas são `TableCatalog`, `StagingTableCatalog`, `SupportsNamespaces`, `ViewCatalog`, `FunctionCatalog` e `ProcedureCatalog`. A `ViewCatalog` vem marcada como trabalho em andamento, ainda não integrada à resolução de query.

A mesma página nomeia **Apache Iceberg, Delta Lake e Lance** como usuários notáveis da API. O Iceberg também aparece na página de performance tuning, com `USING iceberg` e `spark.sql.iceberg.planning.preserve-data-grouping` num exemplo de Storage Partition Join.

O que **não** aparece em lugar nenhum da documentação 4.2.0: Iceberg REST catalog e Unity Catalog, os dois com zero ocorrências. O Spark documenta o mecanismo plugável e não documenta protocolo de catálogo externo específico.

Conclusão para mim: o "metastore" do capítulo é hoje um caso particular. A pergunta moderna não é onde fica o Hive metastore, é qual catálogo está registrado sob qual nome.

**12.** **Opções JDBC hoje.** Confira o quarteto de particionamento e o que apareceu de novo desde 2020.

R: O quarteto continua, e a documentação hoje diz em letras claras o que o capítulo não diz. Sobre `partitionColumn`, `lowerBound` e `upperBound`:

> "These options must all be specified if any of them is specified. In addition, `numPartitions` must be specified. ... Notice that `lowerBound` and `upperBound` are just used to decide the partition stride, **not for filtering the rows in table**. So all rows in the table will be partitioned and returned. This option applies only to reading."

Isso fecha o item 11 do Nível 4. A conclusão errada que o capítulo induz está corrigida explicitamente na documentação, com a frase que ele não tem.

Defaults que o capítulo nunca menciona: `fetchsize` é **0**, `batchsize` é **1000**, `queryTimeout` é **0**, `isolationLevel` é **READ_UNCOMMITTED**, e `pushDownPredicate`, `pushDownAggregate`, `pushDownLimit` e `pushDownOffset` são todos **true**. O `numPartitions` não tem default, e a doc acrescenta que na escrita, se o número de partições exceder o limite, o Spark chama `coalesce(numPartitions)` antes de gravar. Isso responde à pergunta 12 do Nível 3, que o capítulo deixa em aberto.

Opções que apareceram depois de 2020, pela data em que entraram no fonte da documentação. Na v3.2.0, `pushDownAggregate`. Na v3.3.0, `pushDownLimit`, `pushDownTableSample` e `connectionProvider`. Na v3.4.0, `pushDownOffset`, `preferTimestampNTZ` e `prepareQuery`. Na **v4.0.0**, `hint`. Duas ressalvas. A exigência de `partitionColumn` numérico, de data ou de timestamp já estava lá na v2.4.0. E esta é uma história de documentação, não de código.

A restrição entre `dbtable` e `query` continua documentada. E a documentação continua mandando passar `--driver-class-path` e `--jars` juntos, com o mesmo exemplo de `postgresql-9.4.1207.jar` que já estava lá em 2020.

**13.** **Azure Cosmos DB.** O capítulo usa `azure-cosmosdb-spark_2.4.0_2.11-1.3.5-uber.jar`. Confira o estado desse conector.

R: Está **morto, e o repositório diz isso com todas as letras**. O `Azure/azure-cosmosdb-spark` no GitHub está arquivado, e a branch padrão dele se chama literalmente `notsupportedanymore`.

O README, no commit `1c0d95a3`, abre assim: "## NOTE: The connector for Spark 2.4 is not supported anymore. The Azure Cosmos DB Spark Connector 2.4 has been deprecated and is no longer being supported. All users should start new or migrate applications to the v3 Spark Connector."

O substituto vive no `Azure/azure-sdk-for-java`, sob `sdk/cosmos/`, e o group id do Maven mudou de `com.microsoft.azure` para **`com.azure.cosmos.spark`**. Os artefatos publicados vão de `azure-cosmos-spark_3-1_2-12` até `azure-cosmos-spark_3-5_2-13`, e já existem `azure-cosmos-spark_4-0_2-13` e `azure-cosmos-spark_4-1_2-13` para o Spark 4.

Mais um degrau da mesma escada: o próprio conector de Spark 3.1 já foi encerrado. O README dele manda migrar para Spark 3.5, dizendo que Databricks, Synapse e HDInsight não oferecem mais runtime de Spark 3.1.

Isso confirma o que argumentei no item 12 do Nível 4. A seção de Cosmos DB do capítulo não é sobre JDBC, é sobre conector de terceiro, e é justamente a parte do capítulo que apodreceu por inteiro. As três seções de banco relacional continuam válidas porque a API delas é do Spark. A de Cosmos morreu porque a API era de outra pessoa.

**14.** **Tableau hoje.** Sete anos depois do 2019.2, o que sobrou das seis figuras do capítulo?

R: A versão corrente é o **Tableau Desktop 2026.2.1**. O esquema de nomes é o mesmo do livro, ano-ponto-número, então o 2019.2 do capítulo está sete anos atrás.

O conector **Spark SQL continua existindo**. Ele é a entrada 131 do sumário atual da ajuda do Tableau Desktop, entre Snowflake e Splunk. A página diz "Tableau can connect to Spark version 1.2.1 and later". Ela lista como destinos cluster em Azure HDInsight, Azure Data Lake, Databricks ou Apache Spark.

O que mudou nos detalhes que o capítulo fixa:

- Os métodos de autenticação hoje são No Authentication, Kerberos, User Name, User Name and Password e Microsoft Azure HDInsight Service. Os transportes são Binary, SASL e HTTP. Abri a Figure 5-4 e ela mostra Authentication com "Username" e Transport com "SASL" desabilitado, o que ainda existe.
- A exigência de driver mudou de forma. A página atual não nomeia driver, não nomeia fornecedor e **não dá número de versão**. Ela só diz que o conector exige um driver e aponta para a página de download. O "Spark ODBC driver version 1.2.0 or above" do capítulo não tem contraparte na documentação de hoje. *Não consegui verificar o requisito atual de versão, porque a página de drivers do Tableau bloqueia acesso automatizado.*
- A página atual avisa que "the legacy SharkServer and SharkServer2 connections are provided for your use, but are not supported by Tableau". O tipo `SparkThriftServer` que a Figure 5-4 mostra continua, então o menu de Type sobreviveu com os irmãos legados ao lado.

Uma nota de método sobre citar Tableau: `help.tableau.com` não serve mais URL versionada. Testei `/v2026.2/`, `/v2025.1/` e `/v2024.1/` e as três devolvem 404. Só `/current/` resolve. Ou seja, ao contrário do Spark, não dá para fixar a versão na URL, e qualquer citação de documentação do Tableau envelhece em silêncio.

### Fontes consultadas

Todas acessadas em 3 de agosto de 2026.

Documentação do Apache Spark, versão 4.2.0:

- [Apache Arrow in PySpark, com os quatro type hints e as três Pandas Function APIs](https://spark.apache.org/docs/4.2.0/api/python/tutorial/sql/arrow_pandas.html)
- [Arrow-optimized Python UDFs](https://spark.apache.org/docs/4.2.0/api/python/tutorial/sql/arrow_python_udf.html)
- [Python UDTF](https://spark.apache.org/docs/4.2.0/api/python/tutorial/sql/python_udtf.html)
- [Python Data Source API](https://spark.apache.org/docs/4.2.0/api/python/tutorial/sql/python_data_source.html)
- [Guia de UDF e UDTF do PySpark, com o custo de cloudpickle](https://spark.apache.org/docs/4.2.0/api/python/user_guide/udfandudtf.html)
- [Guia de migração do PySpark, com as viradas de default da 4.2](https://spark.apache.org/docs/4.2.0/api/python/migration_guide/pyspark_upgrade.html)
- [Referência de funções do PySpark, com os nomes de window function](https://spark.apache.org/docs/4.2.0/api/python/reference/pyspark.sql/functions.html)
- [Collection functions, com as doze higher-order functions e seus Since](https://spark.apache.org/docs/4.2.0/api/sql/collection-functions/)
- [Array functions, com `arrays_zip` e `arrays_overlap`](https://spark.apache.org/docs/4.2.0/api/sql/array-functions/)
- [Map functions, com `map_from_arrays`](https://spark.apache.org/docs/4.2.0/api/sql/map-functions/)
- [Math functions, com o operador `div`](https://spark.apache.org/docs/4.2.0/api/sql/math-functions/)
- [Distributed SQL Engine, com o Thrift server e a Hive embutida](https://spark.apache.org/docs/4.2.0/sql-distributed-sql-engine.html)
- [Spark SQL CLI](https://spark.apache.org/docs/4.2.0/sql-distributed-sql-engine-spark-sql-cli.html)
- [Spark Connect Overview](https://spark.apache.org/docs/4.2.0/spark-connect-overview.html)
- [Hive Tables, com `spark.sql.warehouse.dir` e as versões de metastore](https://spark.apache.org/docs/4.2.0/sql-data-sources-hive-tables.html)
- [Data Source V2, com o `CatalogPlugin`](https://spark.apache.org/docs/4.2.0/sql-data-sources-v2.html)
- [JDBC To Other Databases, com o quarteto de particionamento e os defaults](https://spark.apache.org/docs/4.2.0/sql-data-sources-jdbc.html)

Código-fonte do Apache Spark, por tag:

- [`SQLConf.scala` na v4.0.0, com o default `false` de `spark.sql.execution.pythonUDF.arrow.enabled`](https://raw.githubusercontent.com/apache/spark/v4.0.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala)
- [`FunctionRegistry.scala` na v4.2.0, com o bloco de higher-order functions](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/FunctionRegistry.scala)
- [`sql-expression-schema.md` na v4.2.0, com os tipos de retorno gerados por máquina](https://github.com/apache/spark/blob/v4.2.0/sql/core/src/test/resources/sql-functions/sql-expression-schema.md)
- [`functions.scala` na v1.6.0, com as depreciações de `denseRank` e companhia](https://raw.githubusercontent.com/apache/spark/v1.6.0/sql/core/src/main/scala/org/apache/spark/sql/functions.scala)
- [`functions.scala` na v4.0.0, sem nenhum nome em camelCase](https://raw.githubusercontent.com/apache/spark/v4.0.0/sql/api/src/main/scala/org/apache/spark/sql/functions.scala)
- [`windowExpressions.scala` na v4.2.0, com `RowNumber` e `RankLike`](https://raw.githubusercontent.com/apache/spark/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/windowExpressions.scala)
- [`builtin.py` na v4.2.0, com o default `StringType()` de `udf` e a nota do `transform`](https://raw.githubusercontent.com/apache/spark/v4.2.0/python/pyspark/sql/functions/builtin.py)
- [`dataframe.py` na v4.2.0, com a regra de tipo do `fillna`](https://raw.githubusercontent.com/apache/spark/v4.2.0/python/pyspark/sql/dataframe.py)
- [`pom.xml` na v4.2.0, com `hive.version` 2.3.10](https://raw.githubusercontent.com/apache/spark/v4.2.0/pom.xml)
- [Commit `ccbd9a7b`, SPARK-41778, que criou o alias `reduce`](https://github.com/apache/spark/commit/ccbd9a7b98d3af5216c6252cad55f3aada278352)

Notas de release:

- [Índice de releases](https://spark.apache.org/releases/)
- [Spark Release 3.4.0, com o cliente Python do Spark Connect](https://spark.apache.org/releases/spark-release-3-4-0.html)
- [Spark Release 4.0.0, com a Python Data Source API](https://spark.apache.org/releases/spark-release-4-0-0.html)
- [Spark Release 4.1.0, com o driver JDBC do Spark Connect](https://spark.apache.org/releases/spark-release-4.1.0.html)
- [Spark Release 4.2.0](https://spark.apache.org/releases/spark-release-4-2-0.html)

Fora do projeto Apache:

- [README do `Azure/azure-cosmosdb-spark`, no commit `1c0d95a3`, com o aviso de fim de suporte](https://github.com/Azure/azure-cosmosdb-spark/blob/1c0d95a32034e00ff414b84b843898fefa240222/README.md)
- [Página do conector Spark SQL na ajuda do Tableau](https://help.tableau.com/current/pro/desktop/en-us/examples_sparksql.htm)

Duas coisas não consegui verificar. A exigência atual de versão do driver ODBC do Tableau, porque `www.tableau.com` bloqueia acesso automatizado. E qualquer página do Jira da Apache, porque `issues.apache.org` estava fora do ar durante a verificação. Os números de ticket vieram de mensagem de commit e de notas de release, não do Jira.

---

## Nível 6 — Cross-book (contra *Data Engineering with Databricks Cookbook*, Capítulo 2)

Li o Capítulo 2 do Chadha inteiro, "Data Transformation and Data Manipulation with Apache Spark". Ele é um livro de receitas de 2024 e o Damji é um livro de conceitos de 2020. O eixo em que os dois se encontram de verdade é a UDF. O Chadha tem duas receitas sobre isso, "Writing custom UDFs in Apache Spark" e a subseção "Handling null values in UDFs".

**1.** **Onde a UDF passa a existir.** O Chadha registra com `udf()` e aplica com `withColumn`. O Damji registra com `spark.udf.register` e chama de dentro do SQL. Compare o que cada caminho torna possível.

R: São dois destinos diferentes para a mesma função.

O `udf()` do Chadha devolve um objeto Python. `concat_udf = udf(concat, StringType())` produz algo que só existe como variável no script dele, e que ele aplica com `df_flattened.withColumn("full_name", concat_udf(...))`. Essa UDF é invisível para SQL.

O `spark.udf.register` do Damji faz a outra coisa: dá um nome à função dentro do catálogo da sessão, e a partir daí `spark.sql("SELECT cubed(id) ...")` funciona. É o que sustenta o cenário de abertura dele, o cientista que embrulha um modelo para o analista consultar em SQL.

O Chadha chega lá, mas em "There's more…" e sem explicar a diferença. Ele apresenta `spark.udf.register` como uma variação de estilo, dizendo que "allows you to make the UDF accessible by name in Spark SQL queries". A frase está correta e não é enquadrada como escolha.

O saldo: o Damji explica por que registrar, o Chadha mostra as duas formas sem hierarquizar. Para quem trabalha em PySpark e não em SQL, a forma do Chadha é a que se usa, e o Damji nem a mostra.

**2.** **Tipo de retorno.** O Chadha primeiro escreve `udf(concat)` e depois `udf(concat, StringType())`. O Damji sempre passa o tipo. Quem está mais seguro?

R: O Damji, e por uma razão que nenhum dos dois escreve.

O Chadha apresenta o tipo como opcional: "You can also specify the return type of the UDF explicitly by passing the return type as the second argument". A palavra "also" sugere que sem ele o Spark infere.

Não infere. Conferi a assinatura em `python/pyspark/sql/functions/builtin.py`, na tag `v4.2.0`: `returnType: "DataTypeOrString" = StringType()`, e a docstring diz "Defaults to `StringType`". Ou seja, omitir o tipo não pede inferência, escolhe string.

O código do Chadha funciona por sorte, porque a função dele concatena dois nomes e devolve string mesmo. A receita seguinte dele já mostra o risco: `square_udf` devolve inteiro e ali ele passa `IntegerType()`. Sem passar, cada quadrado voltaria como texto.

O Damji passa `LongType()` na primeira UDF Python que mostra e nunca comenta por quê. Ele acerta a prática e perde a lição.

**3.** **Null safety, a regra e o exemplo.** Cada livro tem metade. Diga qual metade, e o que só a soma das duas ensina.

R: O Damji tem a regra e não tem o exemplo. Ele escreve as duas recomendações, tornar a UDF null-aware e checar dentro dela, ou usar `IF` e `CASE WHEN` e invocar a UDF em um ramo condicional. Nenhuma UDF do capítulo dele faz isso. A `cubed` de Python quebra com nulo e a de Scala devolve zero em silêncio.

O Chadha tem o exemplo e não tem a regra. A subseção "Handling null values in UDFs" mostra exatamente a recomendação 1 do Damji, em código:

```python
def process_name(name):
    if name is None:
        return "Unknown"
    else:
        return name.upper()
```

O que ele dá como motivo é genérico: "When working with UDFs, we may encounter null values in the input data." Isso vale para qualquer função de qualquer linguagem. Não explica por que o problema é pior no Spark.

Só a soma das duas ensina o que importa. O motivo não é que nulos aparecem, é que o filtro que você escreveu para impedi-los pode não rodar antes da UDF. Sem essa frase, a checagem dentro da UDF parece defensividade opcional. Com ela, é a única defesa que funciona.

**4.** **Ordem de avaliação.** Rode a busca antes de acusar. O Chadha trata do assunto?

R: Não trata, e conferi antes de afirmar. `grep -ci "evaluation order"` no texto extraído do Capítulo 2 dele devolve 0. Também rodei `grep -ci "Catalyst"`, que devolve 0, e `grep -ci "lazy"`, que devolve 0.

Essa última merece uma correção minha. O Chadha menciona avaliação preguiçosa uma vez, numa NOTE sobre `groupBy`: "The groupBy operation in Spark is a transformation, which means it is lazily evaluated. It builds a logical plan but doesn't execute until an action operation (such as collect, show, or write) is triggered." O meu grep por "lazy" não pegou porque a palavra impressa é "lazily". Fica o registro de que grep por radical errado produz acusação errada.

Ainda assim o ponto de fundo se mantém. O Chadha sabe que existe um plano lógico e que a execução é adiada. Ele nunca liga isso à possibilidade de o otimizador reordenar subexpressões. E nunca avisa que um `filter` com `isNotNull` seguido de chamada de UDF não é sequência garantida. É o risco mais específico de UDF em Spark, e é o que o Damji tem e ele não.

**5.** **Custo da UDF.** Compare o que cada livro diz sobre preço.

R: O Damji diz três coisas. Que a UDF paga serialização e desserialização, e que esse processo "may be expensive". Que UDFs de PySpark eram mais lentas que as de Scala porque exigiam movimentação de dados entre JVM e Python. E que as funções embutidas evitam "these potentially expensive techniques".

O Chadha não diz nada. A abertura da receita de UDF dele é só benefício: "Writing UDFs in Apache Spark provides the flexibility and expressiveness necessary to perform custom data transformations and computations. UDFs enable you to extend the capabilities of Spark's built-in functions, integrate with external libraries, and achieve specific data processing requirements in a scalable and distributed manner."

Conferi por busca: `grep -ci "serializ"` no capítulo dele devolve 0, e `grep -ci "Arrow"` devolve 0. A única ocorrência de "expensive" no capítulo inteiro é sobre pivot, não sobre UDF.

O `pandas_udf` aparece uma vez, e só como link na lista "See also", apontando para a documentação do PySpark 3.1.2. Não há uma linha de prosa sobre ele.

O veredito é desconfortável para o livro mais novo. O Chadha ensina a escrever UDF em 2024 sem mencionar que ela é a operação mais cara que se pode colocar em um plano do Spark. O Damji, quatro anos antes, nomeia o custo, nomeia a causa e oferece duas saídas.

**6.** **A alternativa embutida.** O Chadha usa a resposta do problema na primeira receita e faz a pergunta na sexta. Explique.

R: A primeira receita dele, "Applying basic transformations", usa isto:

```python
transform(col("laureates"), lambda x: concat(x.firstname, lit(" "), x.surname))
```

Isso é uma higher-order function, exatamente a coisa que o Damji apresenta como a alternativa eficiente à UDF. A tarefa é a mesma que a sexta receita do Chadha vai resolver com UDF: concatenar nome e sobrenome.

A sexta receita, "Writing custom UDFs", faz assim:

```python
def concat(first_name, last_name):
    return first_name + " " + last_name
concat_udf = udf(concat, StringType())
```

Mesmo resultado, dois mecanismos, custos diferentes, e o livro não faz a ligação. Pior, o Chadha chama a lambda da primeira receita de "user-defined function" na prosa, o que apaga a distinção que o Damji constrói.

O que o Damji tem e ele não é a frase de comparação: `transform` é "similar to the UDF approach, but more efficiently". Meia frase entre parênteses, num livro de conceitos, cobre uma lacuna que o livro de receitas deixa aberta em duas receitas suas.

**7.** **Superfície de API.** O Damji mostra higher-order function como string SQL, o Chadha como chamada de DataFrame. Uma das duas não existia quando o outro livro foi escrito.

R: O Damji escreve `spark.sql("SELECT celsius, transform(celsius, t -> ((t * 9) div 5) + 32) as fahrenheit FROM tC")`. A lambda mora dentro do texto SQL, com a seta `->` da gramática do Spark SQL.

O Chadha escreve `transform(col("laureates"), lambda x: concat(...))`, importando `transform` de `pyspark.sql.functions` e passando uma lambda do Python.

Conferi quando a segunda forma passou a existir. Na tag `v4.2.0`, a docstring de `pyspark.sql.functions.transform` traz `.. versionadded:: 3.1.0`. O livro do Damji foi escrito contra o Spark 3.0-preview2, então a forma do Chadha não estava disponível para ele. A escolha do SQL não é preferência de estilo, é a única opção que ele tinha em PySpark.

Um detalhe que fecha o argumento a favor do Chadha e contra a prosa dele. A mesma docstring diz: "Python `UserDefinedFunctions` are not supported (SPARK-27052)". A lambda passada a `transform` precisa ser feita de expressões de `Column`, o que é o oposto de uma UDF. O código dele obedece a isso, porque usa `concat` e `lit`. A prosa dele diz o contrário. *Isso vira a discordância 3.*

**8.** **Window functions.** Os dois cobrem o assunto. Onde a cobertura difere, e qual das duas envelheceu?

R: O Damji entrega uma tabela de tradução e um exemplo. A Table 5-5 lista dez funções em duas colunas, SQL e DataFrame API, e o único exemplo é `dense_rank()` em SQL, com `OVER (PARTITION BY origin ORDER BY TotalDelays DESC)`. Ele acrescenta a ressalva de dimensionamento, que cada window grouping precisa caber em um executor.

O Chadha entrega mecânica. Ele define a `Window.partitionBy("country").orderBy("date_added")` como objeto, explica partição, ordenação e numeração em três passos, e mostra `row_number`, `lead` e `lag`. Em "There's more…" ele acrescenta window functions aninhadas e window frames, com `rowsBetween(-2, 0)` para média móvel.

A cobertura do Chadha é maior e mais útil para escrever código. Ele tem a `Window`, tem frames e tem o padrão de média móvel, e nada disso existe no Damji.

A que envelheceu é a do Damji, e é a coluna da direita da tabela dele. Conferi na tag `v4.0.0` do `functions.scala` e na referência do PySpark 4.2.0: os nomes `denseRank`, `percentRank`, `rowNumber` e `cumeDist` não existem, e `firstValue` e `lastValue` também não. O Chadha usa `row_number`, `lead` e `lag`, que são os nomes reais. Ele acerta por usar, e o Damji erra por tabelar. *Detalhes no item 7 do Nível 5.*

**9.** **Nulos fora da UDF.** Compare a cobertura dos dois.

R: Não há comparação a fazer, porque um dos lados está vazio.

O Chadha tem uma receita inteira, "Handling null values with Apache Spark", com quatro caminhos. São `dropna()` para descartar linhas, `fillna()` para preencher e `when().otherwise()` para substituir condicionalmente. O quarto é o `Imputer` do `pyspark.ml.feature`, que preenche com média, mediana ou moda.

O Damji tem duas linhas de SQL sobre a tabela `people`, `WHERE age < 20` e `WHERE age IS NULL`, e nenhum comentário sobre nenhuma delas.

O que salva o Damji é que qualidade de dados não é o assunto do capítulo dele. O que não salva é a seção de null checking, que ele tem e o Chadha não. Ali ele levanta o problema mais difícil de nulo em Spark e não mostra uma linha de código que o resolva.

Os dois juntos formam a resposta completa. O Chadha dá o repertório de operações e o Damji dá o risco que nenhuma delas cobre.

**10.** **Qualidade editorial.** Os dois livros têm o mesmo tipo de defeito. Compare a densidade.

R: Têm, e é o mesmo mecanismo: prosa que não foi conferida contra o código ao lado.

No Damji são cinco casos nas tabelas de funções. São `array_zip` contra `arrays_zip`, `map_form_arrays` contra `map_from_arrays` e o tipo de retorno de `arrays_overlap`. Mais a `cardinality` com assinatura de array na tabela de maps e a barra solta em `element_at`. Fora das tabelas há outros três. O `V` a mais na assinatura de `exists`, a frase sobre a Figure 5-2 que a figura desmente, e o Tableau listado como fonte de dados duas vezes.

No Chadha o padrão é diferente e mais denso. A prosa das receitas fala de um dataset e o código usa outro. A receita 1 diz "one row for each unique combination of values in the Name and Age columns" e o código deduplica por `category`, `overallMotivation` e `year`. A mesma receita diz "sort the DataFrame by the Age column in ascending order, then by the Name column in descending order" e o código ordena por `year` e `category`. A receita de agregação diz "we will calculate the total sales amount for each product" e o código conta lançamentos da Netflix por país. Há também `Spark.stop()` com S maiúsculo e `Customer_cards_df` com C maiúsculo, dois nomes que não existem no código anterior.

A diferença de natureza importa. Os defeitos do Damji estão em referência, ou seja, em lugares onde eu ia procurar a verdade. Os do Chadha estão em narrativa, ou seja, em texto de acompanhamento que eu leio uma vez e descarto. Os dele são mais numerosos e menos caros.

### Discordâncias

**1.** **`row_number` é não determinística, e `rank` ou `dense_rank` dão ordem estável?**

O Chadha afirma isso numa NOTE: "row_number is a non-deterministic function, meaning that the order of rows within a partition may vary across different executions of the same query. If you require a stable order, you can use other functions such as rank or dense_rank instead." O Damji não diz nada sobre determinismo.

**Arbitrando: o Chadha está errado nas duas metades.**

Conferi no código-fonte, em `sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/windowExpressions.scala`, na tag `v4.2.0`. A classe é `case class RowNumber() extends RowNumberLike with LeafLike[Expression]`, e não há nenhum `override def deterministic = false` na hierarquia. O Spark não marca `row_number` como não determinística.

Sobre a primeira metade, a instabilidade é real mas a causa está errada. Ela não vem da função, vem do `ORDER BY`. Se a chave de ordenação tem empates, a ordem entre os empatados não é definida, e execuções diferentes podem numerá-los em ordens diferentes. No exemplo do próprio Chadha isso é grave, porque ele ordena por `date_added` num catálogo da Netflix, onde muitos títulos entram no mesmo dia.

Sobre a segunda metade, o remédio não é remédio. Conferi a documentação das duas funções no mesmo arquivo. `rank` "computes the rank of a value in a group of values" e produz buracos na sequência. `dense_rank` faz o mesmo sem buracos. Nenhuma das duas estabiliza a ordem das linhas. As duas dão o **mesmo** valor a linhas empatadas, o que é uma propriedade diferente: elas ficam determinísticas justamente por não distinguirem o que a ordenação não distingue.

O conserto certo é desempatar. `Window.partitionBy("country").orderBy("date_added", "show_id")` resolve o problema que a NOTE descreve, e o Chadha tem a coluna `show_id` disponível no dataset dele.

**2.** **`fillna("N/A")` substitui todos os nulos?**

O Chadha escreve `df_fillna = df_flattened.fillna("N/A")` e diz: "The code will replace all null values with N/A and display the resulting DataFrame on the console." O Damji não trata de `fillna`.

**Arbitrando: o Chadha está errado, e o próprio DataFrame dele prova.**

Conferi a docstring de `DataFrame.fillna` em `python/pyspark/sql/dataframe.py`, na tag `v4.2.0`. O texto é explícito: "Columns specified in subset that do not have matching data types are ignored. For example, if `value` is a string, and subset contains a non-string column, then the non-string column is simply ignored."

O `df_flattened` dele vem de `nobel_prizes.json` e tem `col("year")` e `col("laureates.id")`. O JSON entrega esses campos como número ou texto, conforme o arquivo. A receita seguinte dele confirma que `year` é numérico, porque ali ele preenche com `9999` e não com string. Se `year` é numérico, `fillna("N/A")` não toca nele.

A frase "all null values" é falsa em qualquer DataFrame que misture tipos, que é o caso do dele. O código não está errado, a descrição está.

Vale registrar que a receita imediatamente seguinte, com `when().otherwise()`, é a que de fato alcança colunas de tipos diferentes, porque ali ele escolhe um valor por coluna. O livro tem a resposta certa três parágrafos depois da afirmação errada, e não liga as duas.

**3.** **A lambda passada a `transform` é uma UDF?**

O Chadha diz que sim: "the transform function can be used on array columns to apply a user-defined function to each element of the array". A NOTE dele repete: "The user-defined function passed to the transform function must take a single argument of the same type as the elements in the array column."

O Damji diz que não, por contraste: ele descreve `transform` como "similar to the UDF approach, but more efficiently", e monta a seção inteira apresentando higher-order function como alternativa à UDF.

**Arbitrando: o Damji está certo, e a documentação é categórica.**

Na docstring de `pyspark.sql.functions.transform`, tag `v4.2.0`, o parâmetro `f` pode usar métodos de `Column`, funções de `pyspark.sql.functions` e `UserDefinedFunctions` de Scala. Aí vem a frase decisiva: "Python `UserDefinedFunctions` are not supported (SPARK-27052)".

Ou seja, uma UDF de Python é precisamente o que não pode ser passado ali. A lambda é um construtor de expressão. Ela roda uma vez, no driver, para montar a árvore do Catalyst. O que executa nos dados é a expressão resultante, não código Python.

Essa não é uma disputa de vocabulário. É a diferença que explica por que `transform` é barata e uma UDF é cara. O Chadha a apaga no lugar em que ela seria mais útil, uma receita antes de ensinar UDF.

O código dele obedece à regra e a prosa dele não. Ele passa `concat(x.firstname, lit(" "), x.surname)`, que são expressões de `Column`, e não uma função Python registrada. Se ele tivesse tentado passar uma UDF de Python, a receita não rodaria.

**4.** **Onde a checagem de nulo deve ficar. Achei que fosse discordância e não é.**

O Damji recomenda tornar a UDF null-aware e checar dentro dela. O Chadha, na subseção sobre nulos em UDF, escreve exatamente uma UDF null-aware, com `if name is None`. Li o par duas vezes procurando conflito e não há nenhum.

Dissolvo o caso. Os dois recomendam a mesma ação. O que difere é a justificativa, e essa diferença já está tratada na questão 3 deste nível, onde ela é uma lacuna do Chadha, não uma discordância.

Registro por que quase virei isso em disputa. O Chadha menciona `na.fill` na mesma frase: "we can use the isNull method to check for null values and na.fill to replace null values with default values". O `na.fill` age fora da UDF, e isso parece contradizer a recomendação 1 do Damji. Mas o Damji também oferece um caminho fora da UDF, o `IF` ou `CASE WHEN`. As duas listas têm uma opção dentro e uma opção fora. São a mesma recomendação com nomes diferentes.

**5.** **`approxCountDistinct` contra `approx_count_distinct`. Não é discordância entre os livros, é um defeito de um só.**

O Chadha escreve `approxCountDistinct()` na prosa e `approx_count_distinct` no código, na mesma receita. O Damji não trata de agregação aproximada.

Não há o que arbitrar entre os dois livros, então não é discordância. Mas vale o registro, porque é o mesmo defeito estrutural do `array_zip` no Damji: prosa e código divergindo na mesma página.

Conferi qual nome vale. Em `python/pyspark/sql/functions/builtin.py`, tag `v4.2.0`, `approxCountDistinct` ainda existe e carrega `.. deprecated:: 2.1.0 Use approx_count_distinct instead.`. No Scala, em `functions.scala` da tag `v2.1.0`, as quatro sobrecargas de `approxCountDistinct` já estavam anotadas com `@deprecated("Use approx_count_distinct", "2.1.0")`.

Então o nome da prosa está depreciado desde 2017 e o nome do código está certo. O padrão que fica é o mesmo dos dois livros: quando prosa e código divergem, o código é a fonte confiável, porque ele foi executado e a prosa foi digitada.

**Padrão que fica das cinco.** Três eram discordâncias de verdade e duas se dissolveram. Nas três, o Damji ganhou uma, o Chadha perdeu duas e não ganhou nenhuma, e as três se resolveram no código-fonte do Spark e não na leitura cruzada. Os dois livros erram no mesmo lugar, na prosa que acompanha código correto. A lição operacional é a mesma nos dois. Quando prosa e código discordam dentro de um livro, acredite no código.

---

## Termos-chave para definir antes de seguir adiante

Escreva uma definição de uma linha para cada, com suas próprias palavras, sem consultar:

Shark · Spark SQL · Hive metastore · warehouse · UDF · `spark.udf.register` · sessão · evaluation order · null-aware · `CASE WHEN` · Pandas UDF · vectorized UDF · Apache Arrow · pickle · Python type hint · Pandas Function API · grouped map · co-grouped map · `ArrowEvalPython` · WholeStageCodegen · Project Tungsten · Spark SQL shell · Thrift JDBC/ODBC server · STS · HiveServer2 · Beeline · SQLLine · JDBC · ODBC · classpath · data source API · `dbtable` · `query` · `numPartitions` · `partitionColumn` · `lowerBound` · `upperBound` · stride · data skew · processing window · OLTP · conector · tipo complexo · `explode()` · `collect_list()` · serialização · higher-order function · lambda anônima · `transform()` · `filter()` · `exists()` · `reduce()` · `div` · union · inner join · `left_semi` · `left_anti` · window function · window grouping · `dense_rank()` · `PARTITION BY` · `OVER` · imutabilidade · data lineage · `withColumn()` · `withColumnRenamed()` · pivot

Um termo que o capítulo não define é alvo de releitura, não item de Nível 5.

### Minhas definições

Contei programaticamente: são **67 termos**, e em **28** deles a definição vem de fora do capítulo, porque ele usa o termo sem definir ou nem o menciona. Marquei esses vinte e oito em itálico. O padrão é claro por área. O capítulo define bem o que ele veio ensinar: UDF, higher-order function e as opções de particionamento JDBC. Não define quase nada da infraestrutura em que isso roda, do metastore ao classpath.

**Shark** — Projeto anterior ao Spark SQL, construído sobre a base de código do Hive em cima do Apache Spark. Foi um dos primeiros engines de query SQL interativa em sistemas Hadoop, e demonstrou dar para ter velocidade de data warehouse com escala de Hive/MapReduce.

**Spark SQL** — Componente fundacional do Apache Spark que integra processamento relacional à API de programação funcional do Spark. Permite queries declarativas, armazenamento otimizado e chamada de bibliotecas analíticas complexas.

**Hive metastore** — *Usado sem definição.* Serviço de catálogo com que o `spark-sql` conversa em modo local, e onde as UDFs explicitamente não são persistidas. É o banco de metadados que guarda a definição das tabelas e a localização dos seus arquivos.

**warehouse** — *Aparece só dentro de um caminho de arquivo.* O diretório onde a tabela permanente criada pelo shell é gravada. No capítulo aparece como `/user/hive/warehouse/people`, dentro de um `WARN` do `HiveMetaStore`.

**UDF** — User-defined function. Função escrita pela pessoa que usa o Spark, para além das funções embutidas. Opera por sessão e não é persistida no metastore.

**`spark.udf.register`** — Método que dá um nome à UDF dentro da sessão, tornando-a chamável de dentro de uma query SQL. Em Python aceita um terceiro argumento com o tipo de retorno.

**sessão** — *Usada sem definição neste capítulo.* O escopo em que a UDF vive. Registrar uma UDF em uma sessão não a torna visível em outra, nem depois que a aplicação termina.

**evaluation order** — A ordem em que o engine avalia as subexpressões de uma expressão. O capítulo diz que o Spark SQL não a garante, o que vale para SQL, para a DataFrame API e para a Dataset API.

**null-aware** — Propriedade de uma UDF que trata nulo explicitamente em vez de assumir que o valor chega preenchido. É a primeira das duas recomendações de null checking do capítulo, e nenhuma UDF do capítulo a cumpre.

**`CASE WHEN`** — Expressão condicional do SQL. Na seção de null checking, é a segunda forma recomendada de proteger a chamada da UDF, porque um ramo condicional só avalia depois que a condição decide.

**Pandas UDF** — UDF vetorizada, introduzida no Spark 2.3. Recebe e devolve objetos Pandas em vez de valores individuais, e transporta os dados em Apache Arrow. Declarada com `pandas_udf`, como decorator ou envolvendo a função.

**vectorized UDF** — Outro nome para Pandas UDF. O adjetivo aponta a mudança de unidade de trabalho, de uma linha por chamada para uma Series inteira.

**Apache Arrow** — *Nomeado sem definição.* O formato de transferência usado pelas Pandas UDFs. É um formato binário colunar em memória, entendido pela JVM e pelo Python, que dispensa serializar e fazer pickle na travessia entre os dois.

**pickle** — *Nomeado sem definição.* O mecanismo de serialização nativo do Python, usado no caminho antigo de UDF para transportar valores entre a JVM e o processo Python. O capítulo o cita ao dizer que o Arrow o dispensa.

**Python type hint** — Anotação de tipo na assinatura da função Python. A partir do Spark 3.0, o Spark infere o tipo da Pandas UDF a partir dela, em vez de exigir declaração manual.

**Pandas Function API** — Mecanismo que aplica uma função Python local diretamente a um DataFrame do PySpark, com entrada e saída em objetos Pandas. Diferente da Pandas UDF, que é uma expressão dentro de uma query.

**grouped map** — *Nomeado sem explicação.* Uma das três Pandas Function APIs do Spark 3.0. Aplica a função a cada grupo do DataFrame, recebendo e devolvendo um DataFrame do Pandas por grupo.

**co-grouped map** — *Nomeado sem explicação.* Outra das três. Agrupa dois DataFrames pela mesma chave e entrega os dois grupos correspondentes à função de uma vez.

**`ArrowEvalPython`** — O nó do plano físico que executa uma Pandas UDF. Abri a Figure 5-1 e é o bloco do meio do Stage 28, entre dois blocos de `WholeStageCodegen`. É o que identifica visualmente que uma Pandas UDF está rodando.

**WholeStageCodegen** — *Nomeado sem definição neste capítulo.* Os blocos que aparecem antes e depois do `ArrowEvalPython` na Figure 5-1. Representa o trecho do plano que o Spark compila em código Java compacto em vez de interpretar operador por operador.

**Project Tungsten** — *Nomeado uma vez, de passagem.* O projeto a que o capítulo credita o whole-stage code generation, dizendo que ele melhora bastante a eficiência de CPU.

**Spark SQL shell** — O utilitário `./bin/spark-sql`. Fala com o serviço de Hive metastore em modo local e não conversa com o Thrift JDBC/ODBC server, então roda a própria sessão Spark.

**Thrift JDBC/ODBC server** — Servidor que permite a clientes JDBC e ODBC executar queries SQL sobre o Apache Spark. Iniciado com `./sbin/start-thriftserver.sh` e parado com `./sbin/stop-thriftserver.sh`.

**STS** — Spark Thrift Server, apelido do Thrift JDBC/ODBC server.

**HiveServer2** — *Nomeado sem definição.* O serviço do Hive ao qual o Thrift server do Spark corresponde, na versão do Hive 1.2.1 segundo o capítulo. É o servidor contra o qual o Beeline roda queries HiveQL no mundo Hive.

**Beeline** — Cliente JDBC de linha de comando, baseado no SQLLine CLI, usado para rodar queries contra o HiveServer2 e, no Spark, contra o Thrift server. Conecta com `!connect jdbc:hive2://localhost:10000`.

**SQLLine** — *Nomeado uma vez.* A ferramenta CLI em que o Beeline é baseado. O capítulo não diz mais nada sobre ela.

**JDBC** — *Usado sem definição.* A API padrão de acesso a bancos em Java. No capítulo é o protocolo por onde o Spark lê e escreve em bancos externos e por onde clientes acessam o Thrift server.

**ODBC** — *Usado sem definição.* A contraparte não-Java do JDBC. No capítulo é o protocolo que o Tableau usa para falar com o Thrift server, pelo Spark ODBC driver.

**classpath** — *Usado sem definição.* O conjunto de jars visíveis à JVM. O jar do driver JDBC precisa estar nele, e o capítulo o coloca lá com `--driver-class-path` e `--jars`.

**data source API** — A API do Spark SQL que lê de outros bancos, devolvendo os resultados como DataFrame. É por ela que passam `.format("jdbc")`, `.option(...)` e `.load()`.

**`dbtable`** — Opção JDBC que nomeia a tabela a ler ou escrever. Não pode ser usada junto com `query`.

**`query`** — Opção JDBC que passa uma query a ser executada na origem. Não pode ser usada junto com `dbtable`.

**`numPartitions`** — Número máximo de partições usadas para paralelismo na leitura e na escrita. Determina também o número máximo de conexões JDBC concorrentes.

**`partitionColumn`** — Coluna que decide as partições na leitura de uma fonte externa. Precisa ser numérica, de data ou de timestamp.

**`lowerBound`** — Valor mínimo do `partitionColumn` usado para calcular o stride. *O capítulo não diz que ele não filtra linhas, e o item 12 do Nível 5 mostra que a documentação diz.*

**`upperBound`** — Valor máximo do `partitionColumn` usado para calcular o stride. Mesma ressalva do anterior.

**stride** — *Usado sem definição formal.* O passo de valores que cada partição cobre. Sai da faixa entre os bounds dividida pelo `numPartitions`, e no exemplo do capítulo vale 1.000.

**data skew** — *Nomeado sem definição.* A distribuição desigual dos dados entre partições. O capítulo o usa ao explicar por que escolher um `partitionColumn` uniforme, dando o exemplo do valor 2500 concentrando o trabalho em uma task.

**processing window** — *Nomeada sem definição.* A janela de tempo em que um sistema de origem aceita carga pesada. Sistemas que têm uma permitem maximizar as requisições concorrentes, e sistemas que não têm exigem reduzi-las.

**OLTP** — *Sigla usada sem expansão.* No capítulo, o exemplo de sistema que processa dados continuamente e que por isso não tolera muitas conexões JDBC concorrentes.

**conector** — *Não é termo do capítulo, mas é o que o Cosmos DB exemplifica.* Biblioteca de terceiro, distribuída como jar ou por coordenadas Maven. Ensina o Spark a ler de uma fonte com API própria em vez de JDBC.

**tipo complexo** — Tipo de dado composto por tipos simples, ou seja, array, map e struct. O capítulo diz que a tentação é manipulá-los diretamente e mostra três formas de fazer isso.

**`explode()`** — Função que cria uma linha nova para cada elemento dentro de um array. É a primeira metade da Option 1 do capítulo.

**`collect_list()`** — Função de agregação que devolve uma lista de objetos com duplicatas. É a segunda metade da Option 1, e o capítulo avisa que ela pode causar out-of-memory nos executors em datasets grandes.

**serialização** — *Usada sem definição.* A conversão de valores em uma representação transportável entre processos. É o custo que o capítulo atribui à abordagem de UDF, junto com a desserialização.

**higher-order function** — Função que recebe funções lambda anônimas como argumento. No capítulo, aplica uma expressão a cada elemento de um array e monta o array de saída, sem sair do plano de execução.

**lambda anônima** — *Nomeada sem definição.* A função sem nome passada a uma higher-order function. Em Spark SQL é escrita com a seta, como em `t -> t > 38`.

**`transform()`** — Higher-order function que produz um array aplicando uma função a cada elemento do array de entrada. Assinatura no capítulo: `transform(array<T>, function<T, U>): array<U>`.

**`filter()`** — Higher-order function que produz um array só com os elementos para os quais a função booleana é verdadeira. Assinatura: `filter(array<T>, function<T, Boolean>): array<T>`.

**`exists()`** — Higher-order function que devolve verdadeiro se a função booleana vale para algum elemento do array. *A assinatura impressa no capítulo tem um parâmetro de tipo a mais e está errada, como mostro no item 5 do Nível 4.*

**`reduce()`** — Higher-order function que reduz os elementos do array a um valor único, acumulando em um buffer e aplicando uma função de finalização. *No item 4 do Nível 5 verifiquei quando esse nome passou a existir no Spark, e a data surpreende.*

**`div`** — Operador de divisão inteira do Spark SQL, usado nos exemplos de `transform` e de `reduce`. O capítulo o usa sem nomear e sem explicar.

**union** — Operação que concatena dois DataFrames com o mesmo schema. Não deduplica, e o exemplo do capítulo mostra as três linhas de `foo` aparecendo duas vezes em `bar`.

**inner join** — O tipo de join padrão no Spark SQL. Mantém apenas as linhas com correspondência nos dois lados.

**`left_semi`** — *Listado sem explicação.* Um dos onze tipos de join que o capítulo enumera, e um dos que ele nunca exemplifica. Filtra o lado esquerdo pelas linhas que têm correspondência à direita, sem trazer colunas do lado direito.

**`left_anti`** — *Listado sem explicação.* O complemento do anterior. Devolve as linhas do lado esquerdo que não têm correspondência à direita.

**window function** — Função que usa valores das linhas de uma faixa de linhas de entrada para devolver um conjunto de valores. Permite operar sobre um grupo e ainda devolver um valor por linha de entrada.

**window grouping** — O conjunto de linhas de uma mesma partição da window. Precisa caber em um único executor e vira uma única partição durante a execução.

**`dense_rank()`** — Função de ranking que numera as linhas dentro de cada window grouping, sem deixar buracos na sequência quando há empate. É a única window function que o capítulo exemplifica.

**`PARTITION BY`** — Cláusula que define por qual coluna a window é dividida em groupings. No exemplo do capítulo é `PARTITION BY origin`.

**`OVER`** — *Usada sem definição.* A cláusula que anexa uma especificação de window a uma função. Contém o `PARTITION BY` e o `ORDER BY` que definem o grouping e a ordenação dentro dele.

**imutabilidade** — Propriedade dos DataFrames e dos RDDs subjacentes. Modificar um DataFrame significa criar outro, e o capítulo diz que os RDDs são imutáveis para garantir data lineage.

**data lineage** — *Nomeada sem definição.* A razão que o capítulo dá para a imutabilidade dos RDDs. É o rastro de como cada dado foi derivado, que permite recomputá-lo em caso de falha.

**`withColumn()`** — Método que devolve um DataFrame novo com uma coluna acrescentada. No exemplo, cria a coluna `status` a partir de um `CASE WHEN` sobre `delay`.

**`withColumnRenamed()`** — Método que devolve um DataFrame novo com uma coluna renomeada. No exemplo, `status` vira `flight_status`.

**pivot** — Operação que troca valores de linha por colunas, permitindo nomear os valores e aplicar agregações. No exemplo do capítulo, os meses 1 e 2 viram as colunas `JAN` e `FEB`, com média e máximo de atraso.
