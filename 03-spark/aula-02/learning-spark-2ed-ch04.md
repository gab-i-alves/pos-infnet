# Guia de Leitura — *Learning Spark*, 2ª edição, Capítulo 4: Spark SQL and DataFrames: Introduction to Built-in Data Sources

**Fonte:** Jules S. Damji, Brooke Wenig, Tathagata Das, Denny Lee. *Learning Spark: Lightning-Fast Data Analytics*, 2ª ed. O'Reilly, 2020. Capítulo 4, "Spark SQL and DataFrames: Introduction to Built-in Data Sources".

**Escopo:** os Níveis 1 a 4 são respondíveis apenas com este capítulo. O Nível 5 não é, porque exige verificação externa e pertence a um backlog de estudo.

**Sobre as figuras:** o capítulo tem uma figura só, a Figure 4-1. Abri a página 2 do PDF e li a imagem, em vez de trabalhar com o texto extraído. Onde uma resposta descreve conteúdo de figura, ela veio da imagem.

---

## Como usar este guia

1. Leia o capítulo uma vez de ponta a ponta, sem parar em nenhuma questão. Rode os snippets se tiver Spark instalado, mas não na primeira passagem.
2. Responda o Nível 1 de memória antes de reabrir o capítulo. Depois releia para preencher as lacunas. Toda questão que falhou marca uma seção como alvo de releitura, e o ponteiro diz onde ela está.
3. Escreva os Níveis 2 e 3 em frases completas. Uma resposta que só dá para gesticular oralmente não é resposta. As questões do Nível 2 testam se dá para enunciar um mecanismo sem pegar emprestada a formulação dos autores.
4. O Nível 4 é a seção que conecta o que os autores mantiveram separado: a Figure 4-1 e a lista de data sources que o capítulo de fato cobre, a Table 4-1 e a Table 4-2, as quatro formas de criar tabela e a regra que decide entre managed e unmanaged. Espere que estas levem mais tempo que os Níveis 1 a 3 somados.
5. O Nível 5 vai para o backlog de estudo, não para as notas. Nada ali é fato sobre o estado atual do Spark até ser verificado contra fonte primária.

---

## Nível 1 — Memorização e definições

**1.** Quais capacidades a introdução atribui ao Spark SQL? Liste todas. *(Chapter introduction)*

R: São seis bullets. Fornecer o engine sobre o qual as Structured APIs do Capítulo 3 são construídas. Ler e escrever dados em vários formatos estruturados. Permitir consultar dados por conectores JDBC/ODBC a partir de fontes de BI externas ou de RDBMSs. Fornecer uma interface programática para interagir com dados estruturados guardados como tables ou views num banco, a partir de uma aplicação Spark. Oferecer um shell interativo para emitir queries SQL. Suportar comandos compatíveis com ANSI SQL:2003 e HiveQL.

**2.** Quais formatos estruturados o segundo bullet nomeia? *(Chapter introduction)*

R: JSON, Hive tables, Parquet, Avro, ORC e CSV.

**3.** Quais ferramentas de BI e quais RDBMSs o terceiro bullet nomeia? *(Chapter introduction)*

R: As ferramentas de BI são Tableau, Power BI e Talend. Os RDBMSs são MySQL e PostgreSQL.

**4.** Quais camadas formam a Figure 4-1, de cima para baixo? *(Figure 4-1)*

R: A figura tem quatro faixas. No topo, cinco caixas pequenas: **Tableau**, **Snowflake**, **Talend**, **Power BI** e uma caixa com reticências. Na segunda faixa, três caixas: **Spark Application** à esquerda, **JDBC/ODBC Connectors** ao centro, ocupando quase toda a largura, e **Spark SQL Shell** à direita. Na terceira faixa, uma caixa larga única, **Spark SQL**. Na base, cinco caixas: **Hive Tables**, **JSON**, **Avro**, **Parquet** e **ORC**.

**5.** Como as setas da Figure 4-1 ligam as camadas, e o que isso diz sobre os caminhos de acesso? *(Figure 4-1)*

R: Todas as setas são cinzas e de duas pontas. As cinco caixas do topo ligam-se apenas à caixa **JDBC/ODBC Connectors**. As três caixas da segunda faixa ligam-se cada uma diretamente à caixa **Spark SQL**. A caixa **Spark SQL** liga-se a cada uma das cinco data sources da base. A figura desenha três caminhos de entrada no Spark SQL: a Spark Application, os conectores JDBC/ODBC e o Spark SQL Shell.

**6.** Qual nome aparece na Figure 4-1 e não aparece em nenhuma linha do texto do capítulo? *(Figure 4-1; Chapter introduction)*

R: São dois. **Snowflake** e **Spark SQL Shell**. A prosa cita Tableau, Power BI e Talend, e a figura acrescenta o Snowflake numa caixa cor-de-rosa, ao lado dos três. O nome Spark SQL Shell aparece na segunda faixa da figura. Procurei os dois no texto e nenhum aparece. A palavra "shell" surge duas vezes, em "an interactive shell to issue SQL queries" e em "in a Spark shell (or Databricks notebook)", e nas duas sem o nome.

**7.** Em qual release a SparkSession foi introduzida, e o que ela fornece? *(Using Spark SQL in Spark Applications)*

R: No Spark 2.0. Ela fornece um ponto de entrada unificado para programar o Spark com as Structured APIs. Para usá-la, basta importar a classe e criar uma instância no código.

**8.** Qual método emite uma query SQL, sobre qual objeto ele é chamado, e o que ele devolve? *(Using Spark SQL in Spark Applications)*

R: O método é `sql()`, chamado sobre a instância de SparkSession, que no texto é a variável `spark`. O exemplo dado é `spark.sql("SELECT * FROM myTableName")`. Toda query executada assim devolve um DataFrame, sobre o qual dá para aplicar mais operações Spark.

**9.** Qual data set o capítulo usa nos exemplos de query, em qual formato ele vem, e qual é o tamanho declarado? *(Basic Query Examples)*

R: O Airline On-Time Performance and Causes of Flight Delays data set, com dados de voos dos Estados Unidos. Vem como arquivo CSV. O capítulo diz "over a million records".

**10.** Quais são as cinco colunas do data set, e o que cada uma guarda? *(Basic Query Examples)*

R: `date` guarda uma string como `02190925`, que convertida vira `02-19 09:25 am`. `delay` dá o atraso em minutos entre a partida programada e a real, e valores negativos indicam partida adiantada. `distance` dá a distância em milhas entre o aeroporto de origem e o de destino. `origin` guarda o código IATA do aeroporto de origem. `destination` guarda o código IATA do aeroporto de destino.

**11.** Reproduza a cadeia que lê o CSV e registra a view, e diga o nome da view e o `appName` da sessão. *(Basic Query Examples)*

R: A cadeia é:

```python
df = (spark.read.format("csv")
  .option("inferSchema", "true")
  .option("header", "true")
  .load(csv_file))
df.createOrReplaceTempView("us_delay_flights_tbl")
```

A view chama-se `us_delay_flights_tbl`. O `appName` é `SparkSQLExampleApp`.

**12.** O NOTE mostra como declarar um schema em vez de inferir. Qual é o formato, e qual é a diferença entre a versão Scala e a Python? *(Basic Query Examples, NOTE)*

R: O formato é uma DDL-formatted string. A versão Scala é `"date STRING, delay INT, distance INT, origin STRING, destination STRING"`. A versão Python é a mesma com cada nome de coluna entre crases. O conteúdo declarado é idêntico nas duas.

**13.** Qual é a primeira query do capítulo, e o que o resultado mostra? *(Basic Query Examples)*

R: A query seleciona `distance`, `origin` e `destination` de `us_delay_flights_tbl`, filtra `WHERE distance > 1000` e ordena por `distance DESC`, com `show(10)`. As dez linhas trazem distância 4330, origem HNL e destino JFK. O texto conclui que os voos mais longos ligam Honolulu a Nova York.

**14.** Qual é o predicado da segunda query, e qual é a primeira linha do resultado? *(Basic Query Examples)*

R: O predicado é `WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'`, com ordenação por `delay DESC`. A primeira linha é `02190925`, com atraso de 1638 minutos, de SFO para ORD.

**15.** Reproduza as faixas do `CASE` da terceira query, com os rótulos, e diga o nome da coluna nova. *(Basic Query Examples)*

R: São seis ramos. `delay > 360` vira `'Very Long Delays'`. `delay >= 120 AND delay <= 360` vira `'Long Delays'`. `delay >= 60 AND delay < 120` vira `'Short Delays'`. `delay > 0 and delay < 60` vira `'Tolerable Delays'`. `delay = 0` vira `'No Delays'`. O `ELSE` vira `'Early'`. A coluna nova chama-se `Flight_Delays`.

**16.** Por qual critério a terceira query ordena, e qual origem aparece nas dez primeiras linhas? *(Basic Query Examples)*

R: Ordena por `origin, delay DESC`. As dez linhas mostradas têm origem ABE, com destinos ATL, DTW e ORD, todas rotuladas `Long Delays`.

**17.** Quais duas formas equivalentes de escrever a primeira query na DataFrame API o capítulo dá? *(Basic Query Examples)*

R: A primeira usa `col` e `desc` importados de `pyspark.sql.functions`, com `.where(col("distance") > 1000)` e `.orderBy(desc("distance"))`. A segunda usa strings, com `.where("distance > 1000")` e `.orderBy("distance", ascending=False)`. As duas produzem o mesmo resultado da query SQL.

**18.** O que o capítulo diz sobre o percurso das computações dentro do engine, quando comparadas as duas interfaces? *(Basic Query Examples)*

R: Diz que as computações passam por uma jornada idêntica no Spark SQL engine, e remete a "The Catalyst Optimizer" do Capítulo 3. O resultado é o mesmo nas duas interfaces.

**19.** O que é metadata de uma tabela, quais itens o capítulo lista, e onde tudo isso é guardado? *(SQL Tables and Views)*

R: Metadata é a informação sobre a tabela e seus dados. O capítulo lista schema, description, table name, database name, column names, partitions e a physical location onde os dados moram. Tudo é guardado num central metastore.

**20.** Qual metastore o Spark usa por padrão, em qual caminho, e como se muda esse caminho? *(SQL Tables and Views)*

R: O Spark usa por padrão o Apache Hive metastore, em vez de ter um metastore separado. O caminho declarado é `/user/hive/warehouse`. Para mudar, ajusta-se a variável de configuração `spark.sql.warehouse.dir`, que pode apontar para storage local ou distribuído externo. No item 4 do Nível 5 verifiquei que o default atual não é esse caminho.

**21.** Defina managed table e unmanaged table nos termos do capítulo. *(Managed Versus Unmanaged Tables)*

R: Numa managed table, o Spark gerencia tanto a metadata quanto os dados no file store. Numa unmanaged table, o Spark gerencia só a metadata, e quem gerencia os dados é você, numa data source externa.

**22.** O que `DROP TABLE table_name` faz em cada um dos dois tipos? *(Managed Versus Unmanaged Tables)*

R: Numa managed table, apaga a metadata e os dados. Numa unmanaged table, apaga só a metadata, e não apaga os dados de verdade.

**23.** Quais file stores o capítulo nomeia para managed tables, e qual data source externa ele nomeia para unmanaged? *(Managed Versus Unmanaged Tables)*

R: Para managed, um filesystem local, HDFS, ou um object store como Amazon S3 ou Azure Blob. Para unmanaged, o exemplo dado é o Cassandra.

**24.** Sob qual database o Spark cria tabelas por padrão, e quais dois comandos o capítulo emite para trocar isso? *(Creating SQL Databases and Tables)*

R: Por padrão, sob o database chamado `default`. Os comandos são `spark.sql("CREATE DATABASE learn_spark_db")` e `spark.sql("USE learn_spark_db")`. A partir daí, toda tabela criada pela aplicação nasce nesse database.

**25.** Quais duas formas o capítulo dá para criar uma managed table? *(Creating a managed table)*

R: Em SQL, `CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination STRING)`. Na DataFrame API, ler o CSV com `spark.read.csv(csv_file, schema=schema)` e depois chamar `flights_df.write.saveAsTable("managed_us_delay_flights_tbl")`.

**26.** Quais duas formas o capítulo dá para criar uma unmanaged table, e o que muda em cada uma em relação à managed? *(Creating an unmanaged table)*

R: Em SQL, a diferença é a cláusula `USING csv OPTIONS (PATH '...departuredelays.csv')`. Na DataFrame API, a diferença é `.option("path", "/tmp/data/us_flights_delay")` antes do `saveAsTable("us_delay_flights_tbl")`. Nas duas, o que distingue é apontar um caminho externo. O capítulo mostra a diferença nos exemplos e nunca enuncia a regra.

**27.** Quais dois escopos uma view pode ter, e o que o capítulo diz sobre a duração delas? *(Creating Views)*

R: Uma view pode ser global, visível por todas as SparkSessions num dado cluster, ou session-scoped, visível só por uma SparkSession. As duas são temporárias e desaparecem quando a aplicação Spark termina.

**28.** Qual é a diferença declarada entre uma view e uma table? *(Creating Views)*

R: As views não guardam os dados de fato. As tables persistem depois que a aplicação Spark termina, e as views desaparecem.

**29.** Quais duas views o capítulo cria, e qual filtro cada uma aplica? *(Creating Views)*

R: `us_origin_airport_SFO_global_tmp_view`, criada com `CREATE OR REPLACE GLOBAL TEMP VIEW`, filtra `origin = 'SFO'`. `us_origin_airport_JFK_tmp_view`, criada com `CREATE OR REPLACE TEMP VIEW`, filtra `origin = 'JFK'`. As duas projetam `date`, `delay`, `origin` e `destination`.

**30.** Qual prefixo é obrigatório para acessar uma global temporary view, e por quê? *(Creating Views)*

R: O prefixo é `global_temp.<view_name>`. O motivo dado é que o Spark cria as global temporary views num global temporary database chamado `global_temp`. A view temporária comum é acessada sem prefixo nenhum.

**31.** Quais são os equivalentes na DataFrame API para criar as duas views, e quais são os dois métodos para descartá-las? *(Creating Views)*

R: Para criar, `df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")` e `df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")`. Para descartar, `spark.catalog.dropGlobalTempView(...)` e `spark.catalog.dropTempView(...)`. Em SQL, o equivalente é `DROP VIEW IF EXISTS`.

**32.** Quais duas formas o capítulo dá para ler uma view temporária comum? *(Creating Views)*

R: `spark.read.table("us_origin_airport_JFK_tmp_view")` e `spark.sql("SELECT * FROM us_origin_airport_JFK_tmp_view")`.

**33.** Qual é a diferença entre temporary view e global temporary view, e qual caso de uso o capítulo dá para múltiplas SparkSessions? *(Temporary views versus global temporary views)*

R: Uma temporary view está atrelada a uma única SparkSession dentro de uma aplicação Spark. Uma global temporary view é visível por múltiplas SparkSessions dentro de uma aplicação Spark. O capítulo afirma que dá para criar múltiplas SparkSessions numa mesma aplicação. O caso de uso é acessar e combinar dados de duas SparkSessions que não compartilham a mesma configuração de Hive metastore.

**34.** O que é o Catalog, quando a funcionalidade dele foi expandida, e o que o Spark 3.0 acrescentou? *(Viewing the Metadata)*

R: O Catalog é a abstração de alto nível do Spark SQL para guardar metadata. A funcionalidade foi expandida no Spark 2.x, com novos métodos públicos para examinar a metadata de databases, tables e views. O Spark 3.0 o estende para usar external catalog, assunto do Capítulo 12.

**35.** Quais três métodos do Catalog o capítulo demonstra? *(Viewing the Metadata)*

R: `spark.catalog.listDatabases()`, `spark.catalog.listTables()` e `spark.catalog.listColumns("us_delay_flights_tbl")`.

**36.** Qual é a sintaxe SQL de cache de tabela, o que a palavra `LAZY` faz, e em qual release ela entrou? *(Caching SQL Tables)*

R: A sintaxe é `CACHE [LAZY] TABLE <table-name>` e `UNCACHE TABLE <table-name>`. `LAZY` significa que a tabela só é cacheada quando for usada pela primeira vez, em vez de imediatamente. A opção entrou no Spark 3.0. O capítulo diz que tables e views podem ser cacheadas e descacheadas, como os DataFrames, e adia as estratégias para o próximo capítulo.

**37.** Quais duas formas o capítulo dá para ler uma tabela existente para dentro de um DataFrame? *(Reading Tables into DataFrames)*

R: `spark.sql("SELECT * FROM us_delay_flights_tbl")` e `spark.table("us_delay_flights_tbl")`.

**38.** Qual é o padrão de uso recomendado do DataFrameReader? *(DataFrameReader)*

R: `DataFrameReader.format(args).option("key", "value").schema(args).load()`. O capítulo diz que esse encadeamento de métodos é comum no Spark e fácil de ler.

**39.** Como se obtém uma instância de DataFrameReader, qual é a restrição, e qual é a diferença entre os dois handles? *(DataFrameReader)*

R: Só através de uma instância de SparkSession, com `SparkSession.read` ou `SparkSession.readStream`. Não dá para criar uma instância de DataFrameReader diretamente. O `read` devolve um handle para ler de uma data source estática. O `readStream` devolve uma instância para ler de uma streaming source.

**40.** Na Table 4-1, quais valores o método `format()` aceita, e qual é o default? *(Table 4-1)*

R: Aceita `"parquet"`, `"csv"`, `"txt"`, `"json"`, `"jdbc"`, `"orc"`, `"avro"` e outros. Se o método não for especificado, o default é Parquet, ou o que estiver em `spark.sql.sources.default`.

**41.** Na Table 4-1, quais valores o `mode` de leitura aceita, qual é o default, e a quais formatos as opções `inferSchema` e `mode` pertencem? *(Table 4-1)*

R: Os valores são `PERMISSIVE`, `FAILFAST` e `DROPMALFORMED`. O default é `PERMISSIVE`. As opções `inferSchema` e `mode` são específicas dos formatos JSON e CSV. A tabela remete a documentação do Spark para os detalhes dos modos.

**42.** Na Table 4-1, o que `schema()` aceita e o que a tabela afirma sobre fornecer um schema? *(Table 4-1)*

R: Aceita uma DDL String ou um `StructType`, como `'A INT, B STRING'` ou `StructType(...)`. A tabela diz que para JSON ou CSV dá para inferir pelo `option()`. E diz que, em geral, fornecer um schema para qualquer formato torna o carregamento mais rápido e garante que os dados obedeçam ao schema esperado.

**43.** Na Table 4-1, o que `load()` recebe e quando o argumento pode ficar vazio? *(Table 4-1)*

R: Recebe o caminho da data source, como `"/path/to/data/source"`. Pode ficar vazio se o caminho já tiver sido dado em `option("path", "...")`.

**44.** Quais são os dois padrões de uso recomendados do DataFrameWriter, e de onde vem o handle? *(DataFrameWriter)*

R: Os padrões são:

```
DataFrameWriter.format(args).option(args).bucketBy(args).partitionBy(args).save(path)
DataFrameWriter.format(args).option(args).sortBy(args).saveAsTable(table)
```

O handle não vem da SparkSession. Vem do próprio DataFrame que se quer salvar, com `DataFrame.write` ou `DataFrame.writeStream`.

**45.** Na Table 4-2, quais valores o `mode` de escrita aceita nas duas formas, qual é o default, e o que o default faz? *(Table 4-2)*

R: Na forma de string, aceita `append`, `overwrite`, `ignore`, `error` e `errorifexists`. Na forma de enum, aceita `SaveMode.Overwrite`, `SaveMode.Append`, `SaveMode.Ignore` e `SaveMode.ErrorIfExists`. Os defaults são `error` ou `errorifexists`, e `SaveMode.ErrorIfExists`. Eles lançam exceção em tempo de execução se os dados já existirem. A tabela chama `option()` de método sobrecarregado.

**46.** Na Table 4-2, o que `bucketBy()` recebe e qual esquema ele usa? *(Table 4-2)*

R: Recebe `(numBuckets, col, col..., coln)`, ou seja, o número de buckets e os nomes das colunas por que agrupar. Usa o esquema de bucketing do Hive sobre um filesystem.

**47.** Por que o capítulo começa a exploração das data sources pelo Parquet, e o que ele recomenda? *(Parquet)*

R: Porque o Parquet é a data source default do Spark. É um formato colunar open source, suportado e muito usado por frameworks e plataformas de big data, e oferece otimizações de I/O como compressão. A recomendação é salvar os DataFrames em Parquet depois de transformar e limpar os dados, para consumo downstream. O capítulo acrescenta que o Parquet é também o table open format default do Delta Lake, assunto do Capítulo 9.

**48.** O que um diretório Parquet contém, e o que fica no footer? *(Reading Parquet files into a DataFrame)*

R: Contém os arquivos de dados, metadata, vários arquivos comprimidos e alguns arquivos de status. O exemplo lista `_SUCCESS`, `_committed_<id>`, `_started_<id>` e um `part-00000-tid-<...>-c000.snappy.parquet`. O capítulo diz que pode haver vários arquivos `part-XXXX`. No footer ficam a versão do formato de arquivo, o schema e dados de coluna, como o path.

**49.** Quando é preciso fornecer schema ao ler Parquet, e por quê? *(Reading Parquet files into a DataFrame, NOTE)*

R: Não é preciso ao ler de uma data source Parquet estática, porque a metadata do Parquet normalmente contém o schema e ele é inferido. É preciso ao ler de streaming data sources, e o capítulo remete ao Capítulo 8.

**50.** Como se cria uma tabela SQL a partir de um arquivo Parquet, e que tipo de objeto isso cria? *(Reading Parquet files into a Spark SQL table)*

R: Com `CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl USING parquet OPTIONS (path "...")`. O texto de abertura da subseção diz que dá para criar uma unmanaged table ou uma view do Spark SQL diretamente por SQL. O comando mostrado cria uma temporary view.

**51.** Reproduza a cadeia que escreve um DataFrame em Parquet, e diga o que o NOTE acrescenta. *(Writing DataFrames to Parquet files)*

R: A cadeia é:

```python
(df.write.format("parquet")
  .mode("overwrite")
  .option("compression", "snappy")
  .save("/tmp/data/parquet/df_parquet"))
```

O NOTE lembra que o Parquet é o formato de arquivo default. Se o método `format()` for omitido, o DataFrame ainda será salvo como Parquet.

**52.** O que muda para escrever um DataFrame numa tabela SQL em vez de num arquivo, e que tipo de tabela isso cria? *(Writing DataFrames to Spark SQL tables)*

R: Muda só o método final, que passa a ser `saveAsTable()` em vez de `save()`. O capítulo afirma que isso cria uma managed table chamada `us_delay_flights_tbl`.

**53.** Quais dois formatos de representação o JSON tem, e como se liga o segundo? *(JSON)*

R: Single-line mode e multiline mode. No single-line mode cada linha denota um objeto JSON. No multiline mode o objeto multilinha inteiro constitui um único objeto JSON. Para ler no segundo modo, define-se `multiLine` como `true` no método `option()`. O capítulo diz que os dois modos são suportados.

**54.** Reproduza a Table 4-3 inteira: as quatro opções de JSON, seus valores, significados e escopos. *(Table 4-3)*

R:

| Propriedade | Valores | Significado | Escopo |
|---|---|---|---|
| `compression` | `none`, `uncompressed`, `bzip2`, `deflate`, `gzip`, `lz4`, `snappy` | Codec de compressão para escrita. Na leitura, o Spark só detecta a compressão pela extensão do arquivo | Write |
| `dateFormat` | `yyyy-MM-dd` ou `DateTimeFormatter` | Formato de data, ou qualquer formato do `DateTimeFormatter` do Java | Read/write |
| `multiLine` | `true`, `false` | Liga o multiline mode. Default é `false` | Read |
| `allowUnquotedFieldNames` | `true`, `false` | Permite nomes de campo JSON sem aspas. Default é `false` | Read |

**55.** Como o capítulo descreve o formato CSV, e o que ele diz sobre o separador? *(CSV)*

R: Diz que é um formato de arquivo texto comum, tão usado quanto arquivos texto simples, em que cada dado ou campo é delimitado por vírgula, e cada linha de campos separados por vírgula representa um registro. A vírgula é o separador default, e dá para usar outros delimitadores quando vírgulas fazem parte dos dados. Planilhas populares geram CSV, o que torna o formato popular entre analistas.

**56.** Quais opções o exemplo de leitura de CSV usa, e o que os comentários no código dizem? *(Reading a CSV file into a DataFrame)*

R: Usa `.schema(schema)`, `.option("header", "true")`, `.option("mode", "FAILFAST")` e `.option("nullValue", "")`. O comentário do `mode` diz "Exit if any errors". O comentário do `nullValue` diz que substitui qualquer dado nulo por aspas. O schema declarado é `"DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count INT"`.

**57.** Reproduza a Table 4-4 inteira: as sete opções de CSV, com defaults e escopos. *(Table 4-4)*

R:

| Propriedade | Valores | Significado | Escopo |
|---|---|---|---|
| `compression` | `none`, `bzip2`, `deflate`, `gzip`, `lz4`, `snappy` | Codec de compressão para escrita | Write |
| `dateFormat` | `yyyy-MM-dd` ou `DateTimeFormatter` | Formato de data, ou qualquer formato do `DateTimeFormatter` do Java | Read/write |
| `multiLine` | `true`, `false` | Liga o multiline mode. Default é `false` | Read |
| `inferSchema` | `true`, `false` | Se `true`, o Spark determina os tipos das colunas. Default é `false` | Read |
| `sep` | Qualquer caractere | Caractere que separa valores de coluna numa linha. Default é a vírgula | Read/write |
| `escape` | Qualquer caractere | Caractere que escapa aspas. Default é `\` | Read/write |
| `header` | `true`, `false` | Indica se a primeira linha é cabeçalho com os nomes das colunas. Default é `false` | Read/write |

**58.** Em qual release o Avro entrou como built-in data source, que uso concreto o capítulo cita, e quais benefícios ele lista? *(Avro)*

R: No Spark 2.4. O uso citado é o Apache Kafka, que usa Avro para serializar e desserializar mensagens. Os benefícios listados são o mapeamento direto para JSON, velocidade e eficiência, e bindings disponíveis para muitas linguagens de programação.

**59.** Reproduza a Table 4-5 inteira: as cinco opções de Avro, com defaults e escopos. *(Table 4-5)*

R:

| Propriedade | Default | Significado | Escopo |
|---|---|---|---|
| `avroSchema` | `None` | Schema Avro opcional, fornecido pelo usuário em JSON. Tipos e nomes dos campos precisam bater com o dado Avro de entrada ou com o dado Catalyst, senão a ação de leitura ou escrita falha | Read/write |
| `recordName` | `topLevelRecord` | Nome do record de topo no resultado da escrita, exigido pela spec do Avro | Write |
| `recordNamespace` | `""` | Namespace do record no resultado da escrita | Write |
| `ignoreExtension` | `true` | Se ligada, carrega todos os arquivos, com e sem extensão `.avro`. Caso contrário, ignora os arquivos sem `.avro` | Read |
| `compression` | `snappy` | Codec de compressão para escrita. Os suportados são `uncompressed`, `snappy`, `deflate`, `bzip2` e `xz`. Se não for definida, vale o valor de `spark.sql.avro.compression.codec` | Write |

**60.** Qual nome de view o exemplo de Avro usa em SQL, e o que isso tem de estranho? *(Reading an Avro file into a Spark SQL table)*

R: O nome é `episode_tbl`. É estranho porque o path apontado é o mesmo diretório de sumário de voos usado em todas as outras seções, e o resultado exibido traz `DEST_COUNTRY_NAME`, `ORIGIN_COUNTRY_NAME` e `count`. O nome `episode_tbl` não tem relação com o conteúdo. Ele só é usado nas duas queries logo abaixo, uma em Scala e uma em Python, e nunca mais depois da seção de Avro.

**61.** Quais duas configurações do Spark ditam qual implementação de ORC é usada, e quais valores ligam o vectorized reader? *(ORC)*

R: `spark.sql.orc.impl` precisa estar em `native`, e `spark.sql.orc.enableVectorizedReader` precisa estar em `true`. Com essa combinação, o Spark usa o vectorized ORC reader.

**62.** O que um vectorized reader faz de diferente, com qual tamanho de bloco, e o que ele reduz? *(ORC)*

R: Lê blocos de linhas, muitas vezes 1.024 por bloco, em vez de uma linha por vez. Isso simplifica as operações e reduz o uso de CPU em operações intensivas como scans, filters, aggregations e joins.

**63.** Para tabelas Hive ORC SerDe, o que precisa estar ligado para o vectorized reader ser usado, e como essas tabelas são criadas? *(ORC)*

R: O parâmetro `spark.sql.hive.convertMetastoreOrc` precisa estar em `true`. Essas tabelas são criadas com o comando SQL `USING HIVE OPTIONS (fileFormat 'ORC')`. A sigla SerDe é aberta no texto como serialization and deserialization.

**64.** Como o exemplo Python de leitura de ORC difere do Scala, e onde a Table 4-1 autoriza a diferença? *(Reading an ORC file into a DataFrame; Table 4-1)*

R: O Scala faz `spark.read.format("orc").load(file)`. O Python faz `spark.read.format("orc").option("path", file).load()`. A Table 4-1 autoriza a segunda forma ao dizer que o argumento de `load()` pode ficar vazio quando o caminho é dado em `option("path", "...")`.

**65.** Em qual release a comunidade introduziu a data source de imagens, e por qual motivo? *(Images)*

R: No Spark 2.4. O motivo é dar suporte a frameworks de deep learning e machine learning, como TensorFlow e PyTorch. O capítulo diz que carregar e processar conjuntos de imagens é importante para aplicações de machine learning baseadas em computer vision.

**66.** Qual é o schema produzido pela data source de imagens, e quais valores o exemplo mostra? *(Images)*

R: A raiz tem uma struct chamada `image` e uma coluna `label` do tipo integer. Dentro de `image` ficam `origin` (string), `height` (integer), `width` (integer), `nChannels` (integer), `mode` (integer) e `data` (binary). As cinco linhas exibidas trazem height 288, width 384, nChannels 3 e mode 16. Os labels são 0, 1, 0, 0, 0.

**67.** Quais imports o exemplo de imagens usa em cada linguagem? *(Images)*

R: Em Scala, `import org.apache.spark.ml.source.image`. Em Python, `from pyspark.ml import image`.

**68.** Em qual release o Spark passou a suportar binary files, e quais colunas a data source produz? *(Binary Files)*

R: No Spark 3.0. O DataFrameReader converte cada arquivo binário numa única linha do DataFrame, com o conteúdo bruto e a metadata do arquivo. As colunas são `path` (StringType), `modificationTime` (TimestampType), `length` (LongType) e `content` (BinaryType).

**69.** Qual nome de formato se usa para binary files, e o que fazem as duas opções demonstradas? *(Binary Files)*

R: O formato é `binaryFile`. A opção `pathGlobFilter` carrega arquivos cujos caminhos batem com um padrão glob, preservando o comportamento de partition discovery. O exemplo usa `"*.jpg"`. A opção `recursiveFileLookup` com valor `"true"` ignora o partitioning data discovery no diretório.

**70.** O que acontece com a coluna `label` quando `recursiveFileLookup` é `"true"`, e o que a data source de binary files não suporta? *(Binary Files)*

R: A coluna `label` some da saída. O capítulo diz que a binary file data source, no momento, não suporta escrever um DataFrame de volta no formato original.

**71.** O que o Summary diz que o capítulo cobriu? *(Summary)*

R: Cinco itens. Criar managed e unmanaged tables com Spark SQL e com a DataFrame API. Ler e escrever em várias built-in data sources e formatos de arquivo. Usar a interface programática `spark.sql` para emitir queries SQL sobre dados estruturados guardados como tables ou views do Spark SQL. Percorrer o Spark Catalog para inspecionar a metadata associada a tables e views. Usar as APIs de DataFrameWriter e DataFrameReader.

---

## Nível 2 — Compreensão

Explique cada um com suas próprias palavras, de três a seis frases. Não reutilize a formulação do capítulo.

**1.** Por que registrar um DataFrame como temporary view permite consultá-lo com SQL, e o que a view guarda de fato?

R: A view dá um nome ao plano que produz aquele DataFrame. SQL precisa de um identificador para colocar depois do `FROM`, e o registro fornece esse identificador. A view não guarda linha nenhuma, guarda a associação entre o nome e o plano. Quando a query roda, o parser resolve o nome, encontra o plano e o encaixa no plano maior da query. É por isso que a interface SQL e a DataFrame API produzem o mesmo resultado. As duas terminam no mesmo plano dentro do engine.

**2.** Por que um metastore é necessário, e o que quebraria sem ele?

R: Os arquivos de dados guardam bytes, e às vezes um schema embutido, como no Parquet. Nenhum deles guarda o nome pelo qual você quer chamar aquilo, nem em qual database ele mora, nem onde os arquivos estão. O metastore é o lugar que guarda essa camada de nomes e localização. Sem ele, `SELECT * FROM us_delay_flights_tbl` não teria como resolver o nome para um caminho. Todo acesso viraria caminho literal, e a tabela deixaria de existir como conceito.

**3.** Explique o mecanismo que torna `DROP TABLE` destrutivo para uma managed table e inofensivo para uma unmanaged.

R: O comando é o mesmo nos dois casos, e o que muda é a extensão do que o Spark considera seu. Numa managed table, o Spark é dono da metadata e dos arquivos, porque ele mesmo escolheu onde escrevê-los. Apagar a tabela significa apagar tudo que ele criou. Numa unmanaged table, os arquivos já existiam num caminho que você escolheu, e o Spark só registrou uma referência a eles. Apagar a tabela apaga a referência. Os arquivos ficam onde estavam, porque nunca pertenceram ao Spark.

**4.** Explique por que views desaparecem e tables persistem, em termos de onde cada coisa é registrada.

R: Uma table é registrada no metastore, que é um armazenamento persistente fora do processo. Ele sobrevive ao fim da aplicação, e por isso a tabela ainda está lá na próxima execução. Uma temporary view é registrada dentro da sessão, na memória do processo que a criou. Quando a aplicação termina, esse registro morre com ela. A diferença não é sobre os dados. É sobre onde o nome foi anotado.

**5.** Explique a diferença entre temporary view e global temporary view, e por que o prefixo `global_temp` é obrigatório.

R: A temporary view vive dentro de uma SparkSession e só é visível por ela. A global temporary view vive num escopo acima das sessões e é visível por várias delas na mesma aplicação. Para sustentar isso, o Spark precisa de um lugar de nomes comum, e cria um database chamado `global_temp`. O nome da view passa a morar nesse database, e não no escopo da sessão. Por isso a referência precisa ser qualificada. Sem o prefixo, o resolvedor procura no escopo da sessão, e não acha nada.

**6.** Por que o DataFrameReader é alcançado pela SparkSession e o DataFrameWriter pelo DataFrame?

R: A assimetria segue o que cada lado precisa saber. Ler começa sem dado nenhum, então o único ponto de partida disponível é a sessão, que sabe conversar com o cluster e com as data sources. Escrever começa com um dado já descrito, e esse dado é o DataFrame. O writer precisa do plano e do schema que só o DataFrame tem. Por isso o handle sai de `DataFrame.write`, e o capítulo marca que essa é a diferença em relação ao reader.

**7.** Explique por que ler Parquet dispensa schema e ler CSV ou JSON não.

R: O Parquet guarda o schema dentro do próprio arquivo, no footer. Ler o schema é ler alguns bytes de metadata, o que custa quase nada. CSV é texto sem tipos, e JSON tem tipos por valor mas não declara uma estrutura de tabela. Para os dois, descobrir os tipos exige olhar os dados, que é o que `inferSchema` faz. Essa passagem tem custo, e é por isso que a Table 4-1 diz que fornecer o schema acelera o carregamento. O Parquet não precisa da opção porque já resolveu o problema no momento da escrita.

**8.** Explique o que um vectorized reader faz de diferente e por que isso reduz uso de CPU.

R: O reader comum processa uma linha por vez, e cada linha paga o custo de chamada, de checagem de tipo e de desvio de fluxo. O vectorized reader carrega um bloco de linhas de uma vez, na casa de mil, e aplica a mesma operação sobre o bloco inteiro. O custo fixo por linha se dilui, e o laço interno fica regular. Operações que varrem muito dado, como scan, filter, aggregation e join, ganham mais, porque nelas o custo por linha dominava. O formato colunar ajuda, porque as colunas já estão contíguas na memória.

**9.** Explique por que a binary file data source lê e não escreve.

R: A leitura é bem definida: cada arquivo vira uma linha, com caminho, tamanho, data de modificação e conteúdo bruto. A escrita não tem definição equivalente. Uma linha com um blob de bytes não diz qual nome de arquivo produzir, nem em qual diretório, nem o que fazer com as outras colunas do DataFrame. O formato original também não é recuperável a partir do blob de forma confiável. O capítulo enuncia a limitação e não explica o motivo. A explicação acima é minha leitura.

**10.** Explique o que a Data Sources API entrega, em termos do que muda e do que não muda ao trocar Parquet por Avro.

R: O que muda é a string passada para `format()` e o conjunto de opções específicas daquele formato. O que não muda é todo o resto: o handle vem de `spark.read`, os métodos se encadeiam na mesma ordem, o resultado é um DataFrame, e a escrita espelha a leitura. O capítulo demonstra isso repetindo a mesma estrutura de subseções cinco vezes, com o mesmo data set e a mesma saída. A frase "no different from Parquet, JSON, CSV, or Avro" é o argumento em uma linha. O ganho é que aprender uma data source ensina todas as outras.

**11.** Por que o save mode default é um erro e não um overwrite?

R: Sobrescrever é irreversível e apaga dado que alguém pode precisar. Falhar é reversível, porque nada foi tocado. Um default precisa ser o comportamento cujo engano é mais barato de desfazer. Se o default fosse `overwrite`, um caminho digitado errado destruiria a saída de outro job sem aviso. Com `errorifexists`, o mesmo engano vira uma exceção em tempo de execução, e quem escreveu decide o que fazer. O capítulo dá o comportamento e não dá o argumento.

**12.** Explique para que serve o Catalog, nomeando uma pergunta que ele responde e que `spark.sql` sozinho não responde.

R: O `spark.sql` responde perguntas sobre o conteúdo das tabelas. O Catalog responde perguntas sobre a existência e a forma delas. A pergunta típica é "quais tabelas existem neste database, e quais colunas cada uma tem", feita por quem não criou nenhuma delas. Para responder, é preciso ler a metadata, não os dados. O Catalog expõe isso como chamadas de método que devolvem listas, o que permite programar sobre a resposta. Sem ele, seria preciso conhecer os nomes de antemão.

---

## Nível 3 — Aplicação e transferência

**1.** Você recebeu um catálogo de filmes em CSV, com cabeçalho e uma coluna de nota que precisa ser numérica. Precisa consultá-lo em SQL. Escreva a leitura, o registro e uma query que devolva os dez filmes mais bem avaliados. Diga por que você declara o schema em vez de inferir. *(Basic Query Examples; Table 4-1)*

R:

```python
schema = "title STRING, year INT, genre STRING, rating DOUBLE"

movies_df = (spark.read.format("csv")
  .schema(schema)
  .option("header", "true")
  .option("mode", "FAILFAST")
  .load(csv_file))

movies_df.createOrReplaceTempView("movies_tbl")

spark.sql("""SELECT title, year, rating
FROM movies_tbl
ORDER BY rating DESC""").show(10)
```

Declaro o schema por dois motivos que o capítulo dá. A Table 4-1 diz que fornecer o schema torna o carregamento mais rápido e garante que os dados obedeçam ao esperado. E `inferSchema` custa uma leitura do arquivo antes do trabalho útil. O `mode` em `FAILFAST` faz o job parar num registro malformado, em vez de seguir com nota nula.

**2.** Outro time é dono de um diretório de leituras de sensores em Parquet, num object store, e escreve nele todo dia. Você precisa expor esses dados como tabela consultável em SQL, sem assumir a posse dos arquivos. Escolha entre managed e unmanaged, justifique com o capítulo, e escreva a DDL. *(Managed Versus Unmanaged Tables; Creating an unmanaged table)*

R: Unmanaged. O critério do capítulo é direto: numa unmanaged table o Spark gerencia só a metadata, e quem gerencia os dados é o dono da data source externa. É exatamente a divisão de responsabilidade pedida. O ponto decisivo é o `DROP TABLE`, que numa unmanaged table apaga só a metadata e não toca nos arquivos do outro time.

```sql
CREATE TABLE sensor_readings_tbl (
  sensor_id STRING, ts STRING, temperature DOUBLE, humidity DOUBLE)
  USING parquet
  OPTIONS (PATH 's3a://sensor-lake/readings/')
```

Se preferir a DataFrame API, o equivalente é `.write.option("path", "s3a://sensor-lake/readings/").saveAsTable("sensor_readings_tbl")`. O que torna a tabela unmanaged é apontar o caminho externo, e o capítulo mostra isso nos exemplos sem enunciar a regra.

**3.** Um serviço de logs de servidor grava um arquivo por hora. Metade dos arquivos tem um objeto JSON por linha. A outra metade veio de um exportador que grava um único objeto indentado por arquivo. Explique o que acontece se você ler tudo com a mesma chamada, e o que fazer. *(JSON; Table 4-3)*

R: O capítulo diz que o JSON tem dois formatos de representação, e que o modo de leitura é escolhido por opção. O default de `multiLine` é `false`, ou seja, single-line mode. Lendo tudo de uma vez com o default, os arquivos de uma linha por objeto são lidos corretamente, e os arquivos indentados não, porque cada linha deles não é um objeto JSON completo. Ligar `option("multiLine", "true")` inverte o problema, porque nesse modo o arquivo inteiro conta como um objeto só. Não existe opção que atenda aos dois ao mesmo tempo. A saída é fazer duas leituras, cada uma sobre o subconjunto certo de caminhos, e unir os DataFrames.

**4.** Um pipeline de e-commerce reescreve todo dia uma tabela curada de pedidos do dia anterior. Um segundo pipeline acrescenta eventos de devolução a uma tabela de histórico. Escolha o save mode de cada um a partir da Table 4-2 e diga o que o default faria. *(Table 4-2)*

R: O primeiro pipeline usa `mode("overwrite")`, porque o conteúdo do dia substitui inteiramente o anterior. O segundo usa `mode("append")`, porque cada execução acrescenta linhas ao que já existe. O default não serve para nenhum dos dois. Segundo a Table 4-2, os defaults são `error` ou `errorifexists`, e eles lançam exceção em tempo de execução quando os dados já existem. Na segunda execução de qualquer um dos dois pipelines, o job falharia. O `ignore` também não serve, porque a partir da segunda execução ele não escreveria nada.

**5.** Uma aplicação precisa de duas SparkSessions, cada uma com configuração de metastore diferente, e as duas precisam enxergar a mesma fatia filtrada de um dataset aberto. Diga qual objeto criar, como criá-lo e como lê-lo na segunda sessão. *(Creating Views; Temporary views versus global temporary views)*

R: Uma global temporary view. O capítulo cita esse caso quase literalmente, ao dizer que dá para criar múltiplas SparkSessions numa aplicação, e que isso serve para acessar e combinar dados de duas sessões que não compartilham a mesma configuração de Hive metastore. A criação é `df_slice.createOrReplaceGlobalTempView("open_data_slice_view")`. A leitura na segunda sessão exige o nome qualificado, `SELECT * FROM global_temp.open_data_slice_view`, porque o Spark cria essas views num database global chamado `global_temp`. Uma temporary view comum não serviria, porque fica atrelada à sessão que a criou.

**6.** Você recebeu acesso a um database que outra pessoa montou e não tem documentação nenhuma. Precisa descobrir o que existe ali e quais colunas uma tabela tem. Escreva as chamadas e diga o que cada uma devolve. *(Viewing the Metadata)*

R:

```python
spark.catalog.listDatabases()
spark.catalog.listTables()
spark.catalog.listColumns("us_delay_flights_tbl")
```

A primeira lista os databases disponíveis. A segunda lista as tabelas do database corrente, o que exige um `USE <database>` antes se você quiser olhar outro. A terceira lista as colunas de uma tabela nomeada. O capítulo apresenta as três como acesso à metadata guardada, e nada mais que isso. Ele não mostra o formato do retorno nem menciona os demais métodos do Catalog.

**7.** Um diretório tem alguns milhares de fotos JPEG. Você precisa de duas coisas: a contagem dos arquivos com o tamanho em bytes de cada um, e depois as dimensões em pixels. Diga qual data source serve para cada pedido e escreva as duas leituras. *(Images; Binary Files)*

R: Para tamanho em bytes e contagem, a data source de binary files. Ela produz `path`, `modificationTime`, `length` e `content`, e `length` é o tamanho do arquivo.

```python
binary_df = (spark.read.format("binaryFile")
  .option("pathGlobFilter", "*.jpg")
  .load(path))
```

Para dimensões em pixels, a data source de imagens. Ela decodifica a imagem e expõe `image.height`, `image.width`, `image.nChannels` e `image.mode`.

```python
images_df = spark.read.format("image").load(image_dir)
images_df.select("image.height", "image.width").show(5, truncate=False)
```

A distinção está no capítulo pelo schema de cada uma. A binary file source trata o arquivo como bytes opacos. A image source interpreta o conteúdo. Nenhuma das duas escreve, e para a de binary files o capítulo afirma isso explicitamente.

**8.** Um CSV exportado de uma planilha usa ponto e vírgula como separador, tem cabeçalho, e usa aspas duplas escapadas com aspas. Você quer que o job pare no primeiro registro malformado. Monte a leitura a partir da Table 4-4 e da Table 4-1. *(Table 4-4; Table 4-1)*

R:

```python
df = (spark.read.format("csv")
  .option("sep", ";")
  .option("header", "true")
  .option("escape", "\"")
  .option("mode", "FAILFAST")
  .schema(schema)
  .load(file))
```

A Table 4-4 dá `sep` para trocar o delimitador, com vírgula como default, `header` para declarar a primeira linha como cabeçalho, com `false` como default, e `escape` para o caractere que escapa aspas, com `\` como default. A Table 4-1 dá `mode`, cujo default é `PERMISSIVE`, e `FAILFAST` é o valor que interrompe. O `schema` declarado é o que a Table 4-1 recomenda para qualquer formato.

**9.** Você escreveu saída em Avro num diretório e o consumidor downstream reclama que carregou arquivos que não deveria, porque o diretório também tem arquivos de controle sem extensão. Aponte a opção da Table 4-5 e diga qual valor usar. *(Table 4-5)*

R: A opção é `ignoreExtension`, cujo default é `true`. Com o default, todos os arquivos são carregados, com e sem a extensão `.avro`. Definir `ignoreExtension` como `false` faz o reader ignorar os arquivos sem `.avro`, que é o comportamento pedido. A opção tem escopo de leitura, então quem precisa ajustar é o consumidor, não quem escreveu. No item 9 do Nível 5 verifiquei que essa opção está depreciada hoje.

**10.** Um diretório de CSV bruto precisa virar Parquet todo dia, para consumo downstream, sobrescrevendo a saída anterior. Escreva a conversão e diga o que o capítulo alega que se ganha. *(DataFrameWriter; Parquet)*

R:

```python
raw_df = (spark.read.format("csv")
  .schema(schema)
  .option("header", "true")
  .load(csv_dir))

(raw_df.write
  .mode("overwrite")
  .option("compression", "snappy")
  .save("/data/curated/events_parquet"))
```

O `format("parquet")` pode ser omitido, e o NOTE do capítulo confirma que a saída ainda sai em Parquet. O ganho alegado é que o Parquet é eficiente, colunar e comprimido, e o capítulo recomenda salvar em Parquet depois de transformar e limpar os dados. Ele acrescenta que o formato foi adotado por muitos outros frameworks, o que interessa a quem consome downstream. E a leitura seguinte dispensa schema, porque o Parquet guarda o dele na metadata.

---

## Nível 4 — Análise e síntese

**1.** Compare as data sources desenhadas na Figure 4-1 com as que o capítulo de fato cobre e com as que a introdução lista. Enumere as três listas, aponte as divergências, e diga o que a figura está organizando.

R: As três listas:

| Origem | Data sources |
|---|---|
| Figure 4-1, faixa inferior | Hive Tables, JSON, Avro, Parquet, ORC |
| Bullet da introdução | JSON, Hive tables, Parquet, Avro, ORC, CSV |
| Seções do capítulo | Parquet, JSON, CSV, Avro, ORC, Images, Binary Files |

Três divergências. A figura omite o CSV, que a introdução lista e que o capítulo usa em todos os exemplos de query, desde a primeira página de código. A figura e a introdução omitem Images e Binary Files, que ganham seção própria. E as Hive Tables aparecem na figura e na introdução, mas nunca ganham seção nem exemplo, apesar de o Hive metastore ser o metastore default.

O que a figura organiza não é a lista de data sources do capítulo. Ela organiza as portas de entrada e saída do Spark SQL como componente. Por isso o topo tem ferramentas de BI, a faixa do meio tem os três caminhos de acesso, e a base tem formatos de armazenamento consolidados em 2020. Images e Binary Files ficam de fora porque não são fontes de BI. O CSV fica de fora sem explicação, e essa é a lacuna que a figura não justifica.

**2.** A Figure 4-1 desenha três caminhos até o Spark SQL. Nomeie os três, diga qual deles este capítulo demonstra, e o que os outros dois exigem que o capítulo não fornece.

R: Os três caminhos são a **Spark Application**, os **JDBC/ODBC Connectors** e o **Spark SQL Shell**. Cada um liga-se diretamente à caixa do Spark SQL, com seta de duas pontas.

O capítulo demonstra apenas o primeiro. Todo código é `spark.sql(...)` ou DataFrame API dentro de uma aplicação ou de um notebook, e a seção de abertura chama isso de "the spark.sql programmatic interface".

O caminho JDBC/ODBC é o único que a figura conecta às cinco ferramentas do topo, e o capítulo não mostra uma linha sobre ele. Ele exigiria um servidor que aceite conexões, credenciais e um driver do lado do cliente. Nada disso aparece. O Spark SQL Shell é pior: o nome só existe dentro da figura. A introdução fala em "an interactive shell to issue SQL queries" sem nomear nada, e o shell nunca é lançado nem descrito. A figura promete três portas e o capítulo abre uma.

**3.** A palavra "default" aparece duas vezes sobre o Parquet e significa coisas diferentes. Separe os dois sentidos e diga qual deles é configurável.

R: O primeiro sentido é o de default de configuração. A Table 4-1 e a Table 4-2 dizem que, se `format()` for omitido, o formato é Parquet "ou o que estiver em `spark.sql.sources.default`". Aqui "default" é um valor de configuração, e é trocável por qualquer outro formato.

O segundo sentido é o de recomendação. A seção Parquet diz que ele é a data source "default and preferred", que o capítulo recomenda salvar nele depois de limpar os dados, e que ele deve ser usado em ETL e ingestão. Aqui "default" é preferência editorial dos autores, e nada nele é configurável.

A confusão importa porque as duas afirmações têm status diferente. A primeira é verificável rodando uma linha. A segunda é conselho, e o capítulo não apresenta medida nenhuma para sustentá-la. As justificativas oferecidas, eficiência, armazenamento colunar e compressão rápida, são propriedades do formato, não comparação com alternativas. O ORC também é colunar, e o capítulo o descreve como "additional optimized columnar file format" sem comparar os dois.

**4.** O capítulo mostra quatro comandos que criam tabela e nunca enuncia a regra que decide entre managed e unmanaged. Derive a regra dos quatro exemplos e diga onde ela quase é dita.

R: Os quatro comandos:

| Comando | Tipo | O que decide |
|---|---|---|
| `CREATE TABLE t (cols...)` | managed | não aponta caminho nenhum |
| `flights_df.write.saveAsTable("t")` | managed | não aponta caminho nenhum |
| `CREATE TABLE t (cols...) USING csv OPTIONS (PATH '...')` | unmanaged | aponta um caminho externo |
| `flights_df.write.option("path", "...").saveAsTable("t")` | unmanaged | aponta um caminho externo |

A regra é uma só: a tabela é unmanaged quando o comando aponta uma localização externa, e managed quando não aponta. O mesmo método, `saveAsTable()`, produz os dois resultados, e a única diferença entre as duas linhas é o `option("path", ...)`.

O ponto em que a regra quase é dita é a frase de abertura de "Creating an unmanaged table", que fala em criar tabelas "from your own data sources". A palavra "own" carrega toda a distinção e não é desenvolvida. A seção "Managed Versus Unmanaged Tables" define a consequência, quem gerencia o quê, e promete exemplos na seção seguinte. Os exemplos chegam, a regra não. Quem ler só a definição e depois só o código não tem como saber que a diferença é o `path`.

**5.** Rastreie todos os significados que o nome `us_delay_flights_tbl` assume no capítulo. Diga o que quebra se alguém rodar o capítulo de cima a baixo numa sessão só.

R: O nome aparece com sete papéis distintos:

1. Temporary view sobre o CSV de atrasos, criada na primeira página de código.
2. Unmanaged table criada em SQL com `USING csv OPTIONS (PATH ...)`.
3. Unmanaged table criada com `saveAsTable` mais `option("path", ...)`.
4. Temporary view sobre um arquivo Parquet de sumário, com `USING parquet`.
5. Managed table criada por `df.write.mode("overwrite").saveAsTable(...)`.
6. Temporary view sobre JSON, depois sobre CSV, depois sobre ORC, cada uma com `USING <formato>`.
7. Alvo dos `SELECT * FROM us_delay_flights_tbl` espalhados por cinco seções.

Rodando tudo em sequência numa sessão só, dois problemas aparecem. O primeiro é que o schema muda no meio do caminho. Do papel 1 ao 3 a tabela tem `date`, `delay`, `distance`, `origin` e `destination`. Do papel 4 em diante ela tem `DEST_COUNTRY_NAME`, `ORIGIN_COUNTRY_NAME` e `count`. Nenhum aviso marca a virada, e as saídas impressas mudam de forma sem comentário. O segundo é a colisão entre uma table registrada no metastore e uma temporary view de mesmo nome. O `CREATE OR REPLACE TEMPORARY VIEW` resolve a repetição entre views, e não resolve a convivência com a tabela criada antes.

A conclusão é que o capítulo não foi escrito como sessão contínua. Cada seção é um notebook próprio, e o nome repetido é economia de digitação, não continuidade. O capítulo nunca avisa isso.

**6.** O capítulo dá duas definições de escopo para global view, em páginas diferentes, e elas não coincidem. Enuncie as duas, decida qual delas o resto do capítulo sustenta, e diga o que a outra implicaria.

R: Definição A, em "Creating Views": as views podem ser globais, "visible across all SparkSessions on a given cluster". Definição B, em "Temporary views versus global temporary views": "a global temporary view is visible across multiple SparkSessions within a Spark application".

O escopo de A é o cluster. O de B é a aplicação. São coisas diferentes, porque um cluster hospeda muitas aplicações ao mesmo tempo.

O resto do capítulo sustenta B. A mesma seção "Creating Views" diz que as views são temporárias e desaparecem quando a aplicação Spark termina. Uma view que morre com a aplicação não pode ser visível por outras aplicações do cluster. O caso de uso oferecido também é intra-aplicação: combinar dados de duas SparkSessions criadas por você.

Levar A a sério implicaria um espaço de nomes compartilhado entre aplicações independentes, com colisão de nomes entre times, e tempo de vida atrelado ao cluster e não ao processo. Nada disso aparece no capítulo, e contradiz a frase sobre a duração. A definição A é imprecisa. No item 13 do Nível 5 verifiquei o escopo na documentação oficial.

**7.** Monte uma comparação entre a Table 4-1 e a Table 4-2. Diga o que é simétrico, o que não é, e o que a assimetria revela sobre leitura e escrita.

R:

| Método | Reader (Table 4-1) | Writer (Table 4-2) |
|---|---|---|
| `format()` | sim, mesmo default Parquet | sim, mesmo default Parquet |
| `option("path", ...)` | sim, torna o argumento de `load()` opcional | sim, torna o argumento de `save()` opcional |
| `option("mode", ...)` | sim, mas é parse mode | sim, mas é save mode |
| `schema()` | sim | ausente |
| `bucketBy()` | ausente | sim |
| terminal | `load()` | `save()` e `saveAsTable()` |

O simétrico é a espinha: `format`, `option("path")` e um terminal que aceita caminho vazio. Duas assimetrias contam a história.

A primeira é o `schema()`, que existe só na leitura. Faz sentido, porque escrever parte de um DataFrame que já tem schema. Ler parte de bytes que podem não ter nenhum.

A segunda é o `bucketBy()`, que existe só na escrita, e a Table 4-2 diz que ele usa o esquema de bucketing do Hive sobre um filesystem. Bucketing é decisão de layout físico, e layout só se decide ao gravar.

A assimetria mais perigosa é o `mode`, que aparece nas duas tabelas com o mesmo nome e sentidos que não têm relação. Na leitura ele decide o que fazer com registro malformado. Na escrita ele decide o que fazer com dado que já existe no destino. O padrão `option("mode", ...)` é idêntico nos dois casos, e trocar um pelo outro não dá erro de sintaxe.

**8.** A palavra "mode" carrega quatro sentidos no capítulo. Enumere os quatro e diga o que um leitor precisa saber para desambiguar cada ocorrência.

R: Os quatro:

1. **Parse mode**, na Table 4-1: `PERMISSIVE`, `FAILFAST`, `DROPMALFORMED`, específico de JSON e CSV.
2. **Save mode**, na Table 4-2: `append`, `overwrite`, `ignore`, `error`, `errorifexists`, mais os enums `SaveMode.*`.
3. **Modo de representação do JSON**, na seção JSON: single-line mode e multiline mode, ligado pela opção `multiLine`, não pela opção `mode`.
4. **A coluna `mode` do schema de imagem**, que vale 16 no exemplo e não tem relação alguma com os anteriores. O capítulo nunca diz o que esse 16 significa. *A resposta está fora do capítulo: a documentação de MLlib descreve o campo como um código de tipo compatível com OpenCV.*

Para desambiguar, o leitor precisa olhar de qual lado da cadeia a chamada está. Se veio depois de `spark.read`, é parse mode. Se veio depois de `df.write`, é save mode. Os sentidos 3 e 4 se distinguem pelo contexto, porque um é nome de conceito em prosa e o outro é nome de campo. O capítulo nunca sinaliza a colisão. E as duas tabelas ficam a três páginas uma da outra, em seções consecutivas, o que torna a troca ainda mais fácil de cometer.

**9.** O capítulo afirma que as queries produzem resultados idênticos entre as data sources. Compare as saídas impressas de Parquet, CSV, ORC, Avro e JSON e avalie a afirmação.

R: A afirmação é do fecho da seção de data sources: "Whether you're using the DataFrame API or SQL, the queries produce identical outcomes". Quatro das cinco saídas confirmam. Parquet, CSV, ORC e Avro imprimem exatamente as mesmas dez linhas, com os mesmos valores de `count`: Romania 1, Ireland 264, India 69, Egypt 24, Equatorial Guinea 1, Singapore 25, Grenada 54, Costa Rica 477, Senegal 29 e Marshall Islands 44.

A saída do JSON é outra. Ela traz Romania 15, Croatia 1, Ireland 344, Egypt 15, India 62, Singapore 1, Grenada 62, Costa Rica 588, Senegal 40 e Moldova 1. Dois países aparecem só ali, Croatia e Moldova. Dois somem, Equatorial Guinea e Marshall Islands. Nenhum valor de `count` coincide.

A conclusão é que o diretório JSON não contém o mesmo data set dos outros quatro. Achei que o culpado fosse o caminho, porque o Parquet aponta para `summary-data/parquet/2010-summary.parquet`, com o ano no nome, e o JSON aponta para `summary-data/json/*`, sem ano. Fui conferir e a hipótese não se sustenta. CSV, ORC e Avro também usam glob sem ano, e os três imprimem exatamente as mesmas dez linhas do Parquet. Se glob sem ano bastasse para explicar a divergência, os três divergiriam junto com o JSON. Só o JSON diverge, e o capítulo não dá o que explique isso. A afirmação de resultado idêntico vale para a interface, SQL contra DataFrame API, e não para o dado. O capítulo imprime a evidência contrária na própria página e não comenta.

**10.** O capítulo repete que criar tabela a partir de uma data source "is no different from" as outras. Liste todos os pontos em que a uniformidade quebra.

R: A uniformidade da API vale para a espinha e quebra em seis pontos.

1. **Schema.** Parquet dispensa. CSV precisa de `header` e de schema declarado ou inferido. JSON aceita inferência. A Table 4-1 registra que `inferSchema` só existe para JSON e CSV.
2. **Imports.** Todas as data sources de arquivo funcionam sem import extra. A de imagens exige `import org.apache.spark.ml.source.image` em Scala e `from pyspark.ml import image` em Python.
3. **Escrita.** Parquet, JSON, CSV, Avro e ORC escrevem. Binary files não escrevem, e o capítulo afirma isso. Para imagens, ele não diz nada e não mostra escrita.
4. **Opções específicas.** Cada seção traz uma tabela própria, e as opções não se repetem. `avroSchema` e `recordName` só existem no Avro. `sep` e `escape` só no CSV. `pathGlobFilter` e `recursiveFileLookup` só aparecem em binary files.
5. **Configuração de engine.** Só o ORC exige duas configurações do Spark, `spark.sql.orc.impl` e `spark.sql.orc.enableVectorizedReader`, para escolher a implementação.
6. **Forma do resultado.** As cinco data sources de arquivo devolvem colunas planas. A de imagens devolve uma struct aninhada, e o acesso vira `image.height`. A de binary files devolve uma coluna de bytes crus.

A frase repetida é verdadeira sobre o formato da chamada. Ela é falsa sobre tudo que decide se a chamada vai funcionar.

**11.** Uma unmanaged table guarda metadata no metastore e dados fora dele. Diga exatamente o que o metastore ainda guarda, e o que quebra se o dono dos arquivos mexer neles.

R: Pela lista do próprio capítulo, o metastore guarda schema, description, table name, database name, column names, partitions e a physical location. Numa unmanaged table, tudo isso continua lá. O que sai do controle do Spark é o conteúdo apontado pela physical location.

O que quebra depende do que muda do outro lado. Se os arquivos mudarem de lugar, a physical location fica apontando para o vazio, e a tabela existe no catálogo sem devolver linha nenhuma. Se o schema dos arquivos mudar, o schema registrado passa a mentir, e uma coluna nova fica invisível ou uma leitura falha por tipo. Se uma partição nova for escrita, o metastore não sabe dela, porque quem escreveu não passou pelo Spark.

O capítulo não trata de nenhum desses casos. Ele apresenta o `DROP TABLE` como a única consequência da distinção, e a consequência real é maior: a metadata é uma cópia que pode envelhecer, e nada no capítulo diz como sincronizá-la. No item 3 do Nível 5 verifiquei o comando que a documentação oferece para isso.

**12.** O capítulo apresenta `CACHE [LAZY] TABLE` em cinco linhas e adia o resto. Diga o que ele deixou de fora que a própria seção torna necessário perguntar.

R: Quatro coisas.

Primeiro, o que se cacheia. Uma table tem dados e uma view não tem, então cachear uma view precisa significar materializar o resultado dela. O capítulo diz que dá para cachear as duas e não distingue os casos.

Segundo, onde o cache vive e quanto custa. Nada é dito sobre memória, sobre disco, ou sobre o que acontece quando não cabe.

Terceiro, quanto tempo dura. O cache é da sessão, da aplicação, ou do cluster? A pergunta é a mesma que a seção de views levanta, e nenhuma das duas responde.

Quarto, e mais importante para este capítulo, o que acontece com o cache de uma unmanaged table quando o dono externo reescreve os arquivos. O cache guardaria dado velho, e o capítulo não menciona invalidação.

A opção `LAZY` é o único detalhe entregue, e ela responde só a pergunta de quando o custo é pago. O capítulo remete as estratégias ao próximo capítulo, o que é honesto, mas ele introduz o comando num capítulo cujo tema é justamente a diferença entre dado que o Spark gerencia e dado que ele não gerencia.

**13.** Rastreie as referências para frente do capítulo. Diga quantas são, para onde apontam, e o que um leitor precisa aceitar sem explicação para chegar ao fim.

R: São nove, e elas dividem-se em dois tipos.

Adiamentos internos ao capítulo. `createOrReplaceTempView` é usado na primeira página de código, e views só são definidas dez páginas depois, com um "more on temporary views shortly". A DataFrame API é usada antes de "Data Sources for DataFrames and SQL Tables" apresentar reader e writer.

Adiamentos para outros capítulos. O Catalyst Optimizer vai para o Capítulo 3. O `columnar pushdown` do Parquet vai para o tratamento do Catalyst. O external catalog do Spark 3.0 vai para o Capítulo 12. As estratégias de caching vão para o Capítulo 5. O Delta Lake vai para o Capítulo 9. Streaming data sources vão para o Capítulo 8. A leitura de fontes externas pela Figure 4-1 vai para o Capítulo 5.

São quatro as coisas que o leitor precisa aceitar sem explicação. Que existe um otimizador que faz SQL e DataFrame API convergirem. Que o Parquet tem uma vantagem de leitura chamada pushdown, nunca mostrada. Que o Catalog pode falar com catálogos externos, nunca nomeados. E que o cache tem um custo, nunca dimensionado. Nenhum desses buracos impede rodar o código. Todos impedem julgar as recomendações do capítulo.

**14.** O capítulo dá conselho sobre schema e desobedece a ele no próprio exemplo. Reconstitua o conflito e decida se o exemplo é defensável.

R: O conselho aparece três vezes. O comentário no código diz que, para arquivos maiores, você pode querer especificar o schema. O NOTE logo abaixo mostra como fazer isso com uma DDL string. A Table 4-1 diz que fornecer schema para qualquer formato torna o carregamento mais rápido e garante conformidade.

O exemplo faz o contrário. A leitura do data set de voos usa `option("inferSchema", "true")`, sobre um CSV que o próprio capítulo descreve como tendo mais de um milhão de registros. O NOTE que ensina a alternativa vem depois do código que não a usa.

O exemplo é defensável por um motivo e indefensável por outro. É defensável como didática: mostrar primeiro o caminho de duas opções, e depois a alternativa, é ordem razoável de ensino. É indefensável porque o capítulo mede o arquivo em "over a million records" na mesma página, e depois usa "for larger files" como se o arquivo dele fosse pequeno. Ele nunca diz onde fica a fronteira do "larger". A seção de CSV, mais de vinte páginas adiante, faz a coisa certa e passa `.schema(schema)`. O capítulo corrige a si mesmo sem registrar a correção.

**15.** O exemplo de escrita em JSON passa `option("compression", "snappy")`, e a Table 4-3 diz que a leitura só detecta o codec pela extensão do arquivo. Analise o que isso implica para reler os arquivos que o capítulo acabou de escrever.

R: A Table 4-3 lista `snappy` entre os codecs válidos de escrita para JSON, então a chamada é legítima. A mesma linha da tabela avisa que a leitura só detecta a compressão ou o codec pela extensão do arquivo.

A listagem que o capítulo imprime depois da escrita mostra `part-00000-<...>-c000.json`, com 71 bytes. A extensão é `.json`, sem sufixo de compressão. Comparando com a escrita em Parquet, cuja listagem mostra `c000.snappy.parquet`, a diferença fica visível: o Parquet carrega o codec no nome e o JSON impresso não.

A implicação é que o par escrita mais leitura não fecha por conta própria para JSON comprimido. Quem reler aquele diretório depende do nome do arquivo carregar a informação, e a evidência impressa no capítulo sugere que ele não carrega. O capítulo põe o aviso numa tabela, o exemplo numa página, e a listagem contraditória na página seguinte, sem ligar as três. Não tenho como fechar o caso só com o capítulo, porque a listagem foi truncada para caber na página.

**16.** Compare as sete seções de data source quanto ao que cada uma explica antes de mostrar código. Diga o que o padrão revela sobre as prioridades do capítulo.

R:

| Seção | Explicação antes do código |
|---|---|
| Parquet | por que é o default, estrutura de diretório, footer, recomendação de uso |
| JSON | origem do formato, comparação com XML, dois modos de representação |
| CSV | o que é um CSV, separador, quem produz esse formato |
| Avro | release de entrada, uso pelo Kafka, três benefícios |
| ORC | duas configurações de engine, o que o vectorized reader faz, Hive SerDe |
| Images | release de entrada, motivação de deep learning |
| Binary Files | release de entrada, as quatro colunas produzidas |

O padrão é regressivo. As três primeiras explicam o formato. O Avro explica o ecossistema. O ORC explica a implementação dentro do Spark, e é a única seção que fala em configuração de engine. As duas últimas explicam quase nada, e viram demonstração de API.

O que isso revela é que a prioridade do capítulo é a uniformidade da Data Sources API, não o conhecimento de cada formato. A estrutura de subseções repete-se cinco vezes quase palavra por palavra: ler para DataFrame, ler para tabela SQL, escrever, tabela de opções. Images e Binary Files nem completam a série, porque não escrevem. O capítulo ensina uma API e usa os formatos como instâncias dela.

**17.** O capítulo afirma que SQL e DataFrame API têm a mesma jornada no engine. Se as duas são equivalentes, qual é o critério real de escolha entre elas, usando só material do capítulo?

R: A afirmação está em "Basic Query Examples": as computações passam por uma jornada idêntica no Spark SQL engine, e dão os mesmos resultados. O capítulo demonstra isso imprimindo a mesma saída para a query SQL e para a versão em DataFrame API.

Se o resultado e o caminho de execução são iguais, o critério não pode ser desempenho. O que sobra no capítulo são três coisas.

A primeira é quem escreve. O capítulo diz que emitir queries SQL é parecido com escrever contra uma tabela relacional, o que favorece quem já sabe SQL e não sabe a API.

A segunda é o que a linguagem hospedeira consegue fazer. A DataFrame API compõe em Python ou Scala, aceita variáveis e funções, e permite construir a query em pedaços. A query SQL é uma string, e o capítulo escreve todas elas como literais de três aspas.

A terceira é a superfície de recursos. O `CASE` da terceira query é confortável em SQL, e o capítulo deixa a tradução dela como exercício, sem mostrá-la. Isso é indício de que a equivalência é mais fácil de afirmar do que de exercer.

O critério real, pelo material disponível, é ergonômico e não técnico. O capítulo nunca diz isso.

**18.** Comprima o argumento do capítulo em uma frase por seção, de modo que remover qualquer frase quebre a cadeia de "existe uma SparkSession" a "os built-in data sources são intercambiáveis".

R:

1. *(Chapter introduction)* O Spark SQL é o engine sob as Structured APIs, e ele fala com formatos, com ferramentas de BI e com um shell.
2. *(Figure 4-1)* Esse engine tem três portas de entrada e cinco data sources desenhadas, o que enquadra tudo que vem depois.
3. *(Using Spark SQL in Spark Applications)* A porta que o capítulo usa é a SparkSession, cujo método `sql()` devolve DataFrame.
4. *(Basic Query Examples)* Para que `sql()` tenha o que consultar, o DataFrame precisa de nome, e `createOrReplaceTempView` dá esse nome.
5. *(SQL Tables and Views)* Nomes precisam de um lugar para viver, e esse lugar é o metastore, que por default é o do Hive.
6. *(Managed Versus Unmanaged Tables)* O metastore guarda sempre a metadata, e o que decide se ele também é dono dos dados é o tipo da tabela.
7. *(Creating SQL Databases and Tables)* Os quatro comandos de criação mostram os dois tipos, e o que muda entre eles é apontar ou não um caminho externo.
8. *(Creating Views)* Além de tabelas, dá para nomear recortes, e views são nomes sem dado, que morrem com a aplicação.
9. *(Viewing the Metadata)* Como tudo isso vira registro, o Catalog é a API para ler o registro em vez dos dados.
10. *(Reading Tables into DataFrames)* Um nome registrado volta a ser DataFrame por `spark.sql` ou `spark.table`, o que fecha o ciclo.
11. *(Data Sources for DataFrames and SQL Tables)* Entrar e sair desse ciclo é trabalho de dois objetos, DataFrameReader e DataFrameWriter.
12. *(Table 4-1; Table 4-2)* As duas tabelas fixam a mesma espinha de chamada para toda data source, e é dela que vem a intercambiabilidade.
13. *(Parquet)* O Parquet ocupa o lugar de default porque é colunar e carrega o próprio schema, o que dispensa metade das opções.
14. *(JSON; CSV; Avro; ORC)* Cada uma das outras quatro repete a mesma espinha e acrescenta só uma tabela de opções próprias.
15. *(Images; Binary Files)* Duas data sources mais novas provam o alcance da espinha e mostram seu limite, porque não escrevem.
16. *(Summary)* O resultado é que criar tabelas, ler, escrever, consultar e inspecionar metadata são cinco usos de um mesmo conjunto de APIs.

Remover qualquer uma quebra a cadeia. Sem a 1 e a 2, não há razão para o Spark SQL existir. Sem a 3 e a 4, não há como emitir query nenhuma. Sem a 5 e a 6, os nomes não têm onde morar e a distinção managed não faz sentido. Sem a 7 e a 8, não há como criar nada. Sem a 9 e a 10, o registro é opaco e não há volta para DataFrame. Sem a 11 e a 12, cada formato seria uma API. Sem a 13 a 15, a espinha não teria sido testada. Sem a 16, o capítulo termina numa lista de formatos e não numa API.

---

## Nível 5 — Além do capítulo (backlog, não notas)

O capítulo é de 2020 e descreve o Spark 3.0 recém-saído, o que torna candidatos a defasagem a lista de data sources, os defaults de configuração e o quadro do metastore. Verifiquei os itens abaixo contra fonte primária em **3 de agosto de 2026**, quando a versão corrente da documentação era a **4.2.0**. As URLs estão ao fim da seção.

### Afirmações sujeitas a defasagem

**1.** Verifique quais data sources são built-in hoje. O capítulo cobre sete e a Figure 4-1 desenha cinco.

R: A lista cresceu e mudou de forma. A página de data sources da documentação 4.2.0 documenta onze entradas: **Parquet, ORC, JSON, CSV, Text, XML, Hive Tables, JDBC, Avro, Protobuf** e **Whole Binary Files**. Ela ainda traz Generic Load/Save Functions, Generic File Source Options, Data Source V2 e Troubleshooting.

Quatro diferenças em relação ao capítulo. O **XML** entrou no Spark 4.0.0, por SPARK-44265, e exige a opção `rowTag` para indicar qual elemento vira linha. O **Protobuf** entrou no Spark 3.4.0. O **Text** e o **JDBC** já existiam e o capítulo não os trata, embora a introdução cite JDBC. A **image data source** não aparece nessa lista, e está documentada na seção de MLlib, com o mesmo schema de seis campos que o capítulo mostra.

Uma correção importante sobre a palavra "built-in". A documentação diz que o módulo `spark-avro` "is external and not included in `spark-submit` or `spark-shell` by default", e manda adicioná-lo com `--packages org.apache.spark:spark-avro_2.13:4.2.0`. O mesmo vale para o `spark-protobuf`. A frase do capítulo, "introduced in Spark 2.4 as a built-in data source", vale para o formato estar no projeto, não para o jar estar no classpath.

**2.** Verifique o estado do Parquet como default: `spark.sql.sources.default`, o codec de compressão e o vectorized reader.

R: O quadro do capítulo continua de pé, e ficou mais detalhado. A página de Generic Load/Save Functions confirma que o formato default é `parquet`, controlado por `spark.sql.sources.default`. A página de Parquet dá `spark.sql.parquet.compression.codec` com default **snappy**, e aceita `none`, `uncompressed`, `snappy`, `gzip`, `lzo`, `brotli`, `lz4`, `lz4_raw` e `zstd`. O `spark.sql.parquet.enableVectorizedReader` tem default **true** desde a 2.0.0, o `spark.sql.parquet.filterPushdown` tem default **true** desde a 1.2.0, e o `spark.sql.parquet.mergeSchema` tem default **false** desde a 1.5.0.

Duas novidades que o capítulo não podia ter. O Spark 4.2.0 atualizou o Parquet para a **v1.17.0** e acrescentou suporte a tipos geoespaciais em Parquet, por SPARK-51658, com `GEOMETRY` e `GEOGRAPHY` habilitados por default. E o partition discovery por diretórios `coluna=valor` é documentado na mesma página, com `spark.sql.sources.partitionColumnTypeInference.enabled` em **true**, o que o capítulo nunca menciona.

**3.** Verifique o que mudou em managed contra unmanaged tables, e a regra do `path`.

R: A regra que o capítulo não enuncia está enunciada na documentação, e o vocabulário mudou. A página de Generic Load/Save Functions diz que, com um caminho customizado, `df.write.option("path", "/some/path").saveAsTable("t")` cria uma **external table**, e o caminho não é removido no drop. Sem caminho customizado, os dados vão para o warehouse directory default e são removidos no drop. A documentação usa "external", e o capítulo usa "unmanaged".

Duas coisas que o capítulo deixa de fora. Desde o Spark 2.1, tabelas persistentes de data source guardam a metadata por partição no Hive metastore. E tabelas externas não coletam a informação de partição por default, então é preciso rodar **`MSCK REPAIR TABLE`** para sincronizar o metastore com os arquivos. Esse é o comando que responde à questão 11 do Nível 4.

A documentação também avisa que `bucketBy` e `sortBy` só se aplicam a tabelas persistentes, ou seja, a `saveAsTable`, e não a `save`. O capítulo mostra `bucketBy` no padrão que termina em `save(path)`, na Table 4-2, o que a documentação atual não sustenta.

**4.** Verifique o catálogo e o Hive metastore hoje: o warehouse default, o metastore embutido e as versões suportadas.

R: O caminho `/user/hive/warehouse` do capítulo não é o default. A documentação de Hive Tables diz que `spark.sql.warehouse.dir` aponta por default para **`spark-warehouse`, no diretório corrente onde a aplicação Spark é iniciada**. Ela acrescenta que a propriedade `hive.metastore.warehouse.dir` do `hive-site.xml` está depreciada **desde o Spark 2.0.0**, ou seja, já estava depreciada quando o livro foi escrito. Sem `hive-site.xml`, o Spark cria um `metastore_db` local, com Derby embutido, mais o diretório de warehouse.

As versões de metastore suportadas hoje: `spark.sql.hive.metastore.version` tem default **2.3.10**, e a faixa aceita vai de 2.0.0 a 2.3.10, de 3.0.0 a 3.1.3, e de 4.0.0 a 4.1.0. O `spark.sql.hive.metastore.jars` tem default `builtin`. O Spark 4.0.0 removeu o suporte a Hive anterior à 2.0.0, por SPARK-45328, e acrescentou suporte ao metastore do Hive 4.0, por SPARK-45265.

O Catalog também cresceu muito além dos três métodos do capítulo. A API de Catalog do PySpark documenta 37 métodos hoje, entre eles `listCatalogs`, `currentCatalog` e `setCurrentCatalog`. Esses três são a parte visível do external catalog que o capítulo adia para o Capítulo 12. O mecanismo é o `CatalogPlugin` da Data Source V2, registrado com `spark.sql.catalog.<name>=com.example.MyCatalog`, e inicializado com as propriedades prefixadas por `spark.sql.catalog.<name>.`.

**5.** Verifique JDBC e o particionamento de leitura, que o capítulo cita na introdução e nunca cobre.

R: O capítulo nomeia JDBC/ODBC no primeiro bullet e na Figure 4-1, e não escreve uma linha sobre como usar. A documentação atual cobre o assunto inteiro.

O particionamento de leitura usa quatro opções que andam juntas: **`partitionColumn`**, **`lowerBound`**, **`upperBound`** e **`numPartitions`**. Nenhuma tem valor default. A documentação exige que as quatro sejam especificadas juntas, ou nenhuma. A `partitionColumn` precisa ser numérica, de data ou de timestamp. O detalhe que mais engana está lá em letras claras: `lowerBound` e `upperBound` servem só para calcular o stride das partições, e **não filtram linha nenhuma**, então a tabela inteira é lida e distribuída. A opção vale só para leitura.

Outras três que importam. O `fetchsize` tem default **0**, que significa usar o default do driver JDBC, e a documentação lembra que o Oracle usa 10 linhas por viagem. O `pushDownPredicate` tem default **true**. O `prepareQuery` permite prefixar a query, para casos como `WITH` no SQL Server.

Requisito de infraestrutura que o capítulo também não menciona: o jar do driver precisa estar no classpath, e a documentação manda passar **`--driver-class-path` e `--jars` ao mesmo tempo**, porque o driver precisa estar no processo do driver e nos executors.

**6.** Verifique os formatos de tabela abertos que o capítulo não cobre: Delta Lake, Apache Iceberg e Apache Hudi.

R: O capítulo cita o Delta Lake uma vez, de passagem, dizendo que o Parquet é o table open format default dele, e adia para o Capítulo 9. Iceberg e Hudi não aparecem.

As versões correntes, pelos releases publicados nos repositórios oficiais: **Delta Lake v4.3.1, de 8 de julho de 2026**, **Apache Iceberg 1.11.0, de 20 de maio de 2026**, e **Apache Hudi 1.2.0, de 23 de maio de 2026**.

O ponto que liga os três a este capítulo é o mecanismo de integração. Nenhum deles é built-in. Eles entram pela Data Source V2, e a página de DSv2 da documentação nomeia explicitamente **Apache Iceberg, Delta Lake e Lance** entre os usuários notáveis da API, ao lado da própria data source JDBC. Cada um se registra como `CatalogPlugin` sob `spark.sql.catalog.<name>`, e passa a oferecer namespaces, tabelas, views e funções próprios. Ou seja, a distinção managed contra unmanaged do capítulo é a versão antiga desse problema, e a resposta moderna é um catálogo plugável.

**7.** Verifique o comportamento atual de `saveMode`.

R: Os quatro modos da Table 4-2 continuam iguais, com os mesmos nomes e os mesmos defaults. A documentação lista `SaveMode.ErrorIfExists` com as strings `"error"` e `"errorifexists"` como default, mais `Append`, `Overwrite` e `Ignore`. O `Ignore` é comparado a `CREATE TABLE IF NOT EXISTS`.

O que o capítulo omite e a documentação avisa: **os save modes não usam locking e não são atômicos**. Isso importa para o cenário 4 do Nível 3, porque um `overwrite` que falhe no meio pode deixar o destino num estado parcial. A Table 4-2 fala só de lançar exceção quando o dado já existe.

**8.** Verifique a alegação de compatibilidade com ANSI SQL:2003.

R: A alegação envelheceu de um jeito curioso. Em 2020 o capítulo dizia que o Spark SQL suporta comandos compatíveis com ANSI SQL:2003. Hoje a documentação nem usa essa referência: a página de ANSI Compliance compara palavras-chave contra o **SQL-2016**, e não afirma conformidade total com nenhum padrão. Ela escreve que alguns recursos do dialeto ANSI não vêm direto do padrão, e apenas alinham o comportamento ao estilo dele.

A mudança de comportamento é maior que a de rótulo. **Desde o Spark 4.0, `spark.sql.ansi.enabled` tem default `true`**, por SPARK-44444. Com o default atual, o Spark lança exceção em tempo de execução em vez de devolver `null` quando as entradas de um operador ou função são inválidas. A documentação sugere as funções `try_*`, como `try_cast` e `try_add`, para suprimir isso. Para voltar ao comportamento antigo, define-se `spark.sql.ansi.enabled` como `false`.

Consequência prática para o capítulo: o `CASE` da terceira query continua funcionando, mas qualquer exemplo que dependesse de conversão silenciosa passaria a falhar num Spark 4.

**9.** Verifique o estado de Avro, ORC, image e binary files.

R: Avro. O módulo é externo, como no item 1. A Table 4-5 continua correta nos defaults, com `avroSchema` em `None`, `recordName` em `topLevelRecord`, `recordNamespace` em `""` e `compression` em `snappy`. A exceção é o **`ignoreExtension`, que está depreciado** e será removido em releases futuros. A documentação manda usar a opção genérica `pathGlobFilter` no lugar. Isso corrige a resposta que dei na questão 9 do Nível 3.

ORC. As duas configurações do capítulo continuam iguais: `spark.sql.orc.impl` com default **`native`** e `spark.sql.orc.enableVectorizedReader` com default **`true`**, ambas desde a 2.3.0. O `spark.sql.hive.convertMetastoreOrc` tem default **`true`** desde a 2.0.0. Duas entraram depois do livro: compressão **Zstandard** e **columnar encryption**, as duas desde o Spark 3.2. O Spark 4.2.0 atualizou o ORC para a v2.3.0.

Image. Continua documentada, com o mesmo formato `"image"` e os mesmos seis campos. A página está na seção de MLlib, não na de data sources, e traz uma opção `dropInvalid` que o capítulo não cita. Nenhum aviso de depreciação.

Binary files. Continua com as mesmas quatro colunas, e a documentação repete que **não suporta escrever um DataFrame de volta**. As opções `pathGlobFilter` e `recursiveFileLookup` viraram opções genéricas de file source, válidas para parquet, orc, avro, json, csv e text, com `recursiveFileLookup` em default `false`. A documentação genérica acrescenta `ignoreCorruptFiles`, `ignoreMissingFiles`, `modifiedBefore` e `modifiedAfter`.

**10.** Verifique `CACHE [LAZY] TABLE` e o que o capítulo deixou de fora.

R: A sintaxe continua e ganhou uma cláusula. A referência de SQL dá `CACHE [ LAZY ] TABLE table_identifier [ OPTIONS ( 'storageLevel' [ = ] value ) ] [ [ AS ] query ]`. O `LAZY` mantém o mesmo sentido do capítulo.

O que o capítulo não tem é o `OPTIONS`. O storage level default é **`MEMORY_AND_DISK`**, e os valores aceitos incluem `NONE`, `DISK_ONLY`, `MEMORY_ONLY`, `MEMORY_ONLY_SER`, `MEMORY_AND_DISK_SER`, `OFF_HEAP` e as variantes com réplica. Isso responde em parte a questão 12 do Nível 4: o default cai para disco quando não cabe na memória, e não falha.

### Inconsistências internas do capítulo

**11.** O texto diz que os dois comandos criam "the managed table `us_delay_flights_tbl`", mas os dois comandos escrevem `managed_us_delay_flights_tbl`. Decida qual está errado.

R: A frase está errada e o código está certo. O SQL cria `managed_us_delay_flights_tbl` e o `saveAsTable` recebe `managed_us_delay_flights_tbl`. A frase seguinte deixa cair o prefixo `managed_`. Não é engano inofensivo, porque `us_delay_flights_tbl` é o nome que o capítulo já tinha usado para a temporary view sobre o CSV, e vai usar de novo para a unmanaged table na página seguinte. A frase mistura três objetos diferentes num nome só. A leitura correta é que a seção "Creating a managed table" cria `managed_us_delay_flights_tbl`, e nada mais.

**12.** A saída impressa para JSON difere das saídas de Parquet, CSV, ORC e Avro. Descubra o que isso implica sobre os arquivos de exemplo.

R: Os números não batem em nenhuma linha, e o conjunto de países difere. Croatia e Moldova aparecem só na saída JSON. Equatorial Guinea e Marshall Islands somem dela. Os valores de `count` do JSON são maiores em quase todas as linhas comparáveis, com Ireland em 344 contra 264 e Costa Rica em 588 contra 477.

Tentei explicar pelos caminhos e não deu. Os exemplos de Parquet apontam para `summary-data/parquet/2010-summary.parquet`, com o ano no nome. Os de JSON apontam para `summary-data/json/*`, sem ano, o que carrega o diretório inteiro. A explicação econômica seria que o JSON lê mais de um ano. Ela cai na primeira conferência: CSV, ORC e Avro usam o mesmo tipo de glob sem ano, e os três coincidem com o Parquet. O caminho não é a variável que separa o JSON dos outros. A causa fica indeterminada pelo capítulo. Também não consegui verificar do lado dos dados, porque os arquivos moram em `/databricks-datasets/`, um caminho interno do ambiente Databricks, e não num repositório público. Fica como item aberto, dependente de rodar os notebooks numa Databricks Free Edition.

**13.** As duas definições de escopo de global view não coincidem. Decida qual delas a documentação sustenta.

R: A documentação sustenta a definição da aplicação, não a do cluster. O guia de introdução ao Spark SQL escreve que uma global temporary view serve para ter "a temporary view that is shared among all sessions and keep alive **until the Spark application terminates**". Ele acrescenta que a view está atrelada a um database preservado pelo sistema chamado `global_temp`, e que o nome qualificado é obrigatório, como em `SELECT * FROM global_temp.view1`.

Ou seja, o escopo é cross-session e intra-aplicação. A frase do capítulo sobre "all SparkSessions on a given cluster" está errada. A segunda formulação, em "Temporary views versus global temporary views", está certa. O prefixo obrigatório e a nomenclatura do database batem exatamente com o capítulo.

### Conceitos de que os exemplos dependem e que o capítulo nunca define

**14.** **Metastore** — a peça central de todo o meio do capítulo, usada sem definição.

R: O capítulo diz que a metadata é guardada num "central metastore" e que o Spark usa o do Hive por default. Ele nunca diz o que um metastore é, que processo o serve, nem que ele é um banco de dados. A documentação de Hive Tables fecha a lacuna pela evidência: sem configuração, o Spark cria um diretório `metastore_db` local, que é um banco Derby embutido, e um diretório de warehouse. Ou seja, o metastore é um banco relacional que guarda o catálogo, e o warehouse é o sistema de arquivos que guarda os dados das managed tables. São duas coisas separadas, e o capítulo usa as duas palavras como se fossem uma.

**15.** **PERMISSIVE, FAILFAST e DROPMALFORMED** — nomeados na Table 4-1 e usados num exemplo, sem uma linha de explicação.

R: A Table 4-1 lista os três, marca `PERMISSIVE` como default, e remete a documentação do Spark. O único indício no capítulo é um comentário no código de CSV, "Exit if any errors", ao lado de `FAILFAST`. Os outros dois nunca são explicados. Pelos nomes e pelo contrato da documentação de CSV e JSON: `PERMISSIVE` insere `null` nos campos malformados e guarda o registro cru numa coluna de registro corrompido, `DROPMALFORMED` descarta o registro inteiro, e `FAILFAST` lança exceção. O capítulo escolhe `FAILFAST` no exemplo de CSV sem justificar, e usa `PERMISSIVE` explicitamente num exemplo anterior, também sem justificar.

**16.** **Bucketing** e **`partitionBy`** — um é nomeado sem definição, o outro aparece no padrão de uso e some da tabela.

R: A Table 4-2 diz que `bucketBy()` recebe o número de buckets e as colunas, e que usa "Hive's bucketing scheme on a filesystem". O que é um bucket, para que serve e quando compensa não aparece em lugar nenhum. Nenhum exemplo do capítulo chama o método.

O caso do `partitionBy` é pior. Ele aparece no primeiro padrão de uso do DataFrameWriter, entre `bucketBy` e `save`, e depois não existe: não está na Table 4-2, não é explicado, não é usado. A documentação atual cobre os dois e dá o critério que falta: `partitionBy` cria estrutura de diretórios e serve para colunas de baixa cardinalidade, e `bucketBy` distribui em número fixo de buckets e serve para colunas com muitos valores distintos. A mesma página avisa que `bucketBy` e `sortBy` valem só para tabelas persistentes.

### Fontes consultadas

Todas acessadas em 3 de agosto de 2026. Todas são fonte primária: documentação oficial, notas de release ou a API de releases dos repositórios oficiais.

Documentação do Apache Spark, versão 4.2.0:

- [Data Sources, com a lista de fontes documentadas](https://spark.apache.org/docs/4.2.0/sql-data-sources.html)
- [Generic Load/Save Functions, com `spark.sql.sources.default`, os save modes, managed contra external e `bucketBy`](https://spark.apache.org/docs/4.2.0/sql-data-sources-load-save-functions.html)
- [Generic File Source Options, com `pathGlobFilter` e `recursiveFileLookup`](https://spark.apache.org/docs/4.2.0/sql-data-sources-generic-options.html)
- [Parquet Files, com codecs, vectorized reader e partition discovery](https://spark.apache.org/docs/4.2.0/sql-data-sources-parquet.html)
- [ORC Files, com `spark.sql.orc.impl`, Zstandard e columnar encryption](https://spark.apache.org/docs/4.2.0/sql-data-sources-orc.html)
- [Avro Files, com o aviso de módulo externo e a depreciação de `ignoreExtension`](https://spark.apache.org/docs/4.2.0/sql-data-sources-avro.html)
- [Protobuf data, com o aviso de módulo externo](https://spark.apache.org/docs/4.2.0/sql-data-sources-protobuf.html)
- [XML Files, com a opção `rowTag`](https://spark.apache.org/docs/4.2.0/sql-data-sources-xml.html)
- [Whole Binary Files, com as quatro colunas e a ausência de escrita](https://spark.apache.org/docs/4.2.0/sql-data-sources-binaryFile.html)
- [JDBC To Other Databases, com o particionamento de leitura e o classpath do driver](https://spark.apache.org/docs/4.2.0/sql-data-sources-jdbc.html)
- [Hive Tables, com `spark.sql.warehouse.dir` e as versões de metastore](https://spark.apache.org/docs/4.2.0/sql-data-sources-hive-tables.html)
- [Data Source V2, com `CatalogPlugin` e `spark.sql.catalog.<name>`](https://spark.apache.org/docs/4.2.0/sql-data-sources-v2.html)
- [Getting Started, com o escopo da global temporary view](https://spark.apache.org/docs/4.2.0/sql-getting-started.html)
- [ANSI Compliance, com `spark.sql.ansi.enabled`](https://spark.apache.org/docs/4.2.0/sql-ref-ansi-compliance.html)
- [CACHE TABLE, com `LAZY` e `OPTIONS ('storageLevel')`](https://spark.apache.org/docs/4.2.0/sql-ref-syntax-aux-cache-cache-table.html)
- [Image data source, na documentação de MLlib](https://spark.apache.org/docs/4.2.0/ml-datasource.html)
- [Catalog, na API do PySpark](https://spark.apache.org/docs/4.2.0/api/python/reference/pyspark.sql/catalog.html)

Notas de release:

- [Spark Release 4.0.0, com SPARK-44265, SPARK-44444, SPARK-45328 e SPARK-45265](https://spark.apache.org/releases/spark-release-4-0-0.html)
- [Spark Release 4.2.0, de 14 de julho de 2026, com SPARK-51658 e as atualizações de Parquet e ORC](https://spark.apache.org/releases/spark-release-4-2-0.html)

Releases dos formatos de tabela abertos, lidos pela API de releases do GitHub:

- [delta-io/delta, releases](https://github.com/delta-io/delta/releases)
- [apache/iceberg, releases](https://github.com/apache/iceberg/releases)
- [apache/hudi, releases](https://github.com/apache/hudi/releases)

Um item ficou aberto e depende de execução: a divergência entre a saída JSON e as demais (item 12), que só fecha rodando os notebooks do livro contra os arquivos de `/databricks-datasets/`. Defaults e versões mudam a cada release, então preciso reconferir estes dados antes de usá-los em prova ou em decisão técnica.

---

## Termos-chave para definir antes de seguir adiante

Spark SQL · SparkSession · `spark.sql()` · Structured APIs · DataFrame · temporary view · global temporary view · `global_temp` · table · database · metastore · Hive metastore · warehouse · `spark.sql.warehouse.dir` · metadata · Catalog · external catalog · managed table · unmanaged table · DROP TABLE · CACHE TABLE · storage level · Data Sources API · DataFrameReader · DataFrameWriter · `format()` · `option()` · `schema()` · `load()` · `save()` · `saveAsTable()` · DDL string · schema inference · parse mode · save mode · `bucketBy` · `partitionBy` · Parquet · footer · columnar storage · columnar pushdown · JSON · single-line mode · multiline mode · CSV · Avro · ORC · vectorized reader · SerDe · image data source · binary file data source · `pathGlobFilter` · `recursiveFileLookup` · partition discovery · JDBC/ODBC · ANSI SQL:2003 · HiveQL · Catalyst Optimizer

Um termo não definido é alvo de releitura, não item de Nível 5. Se o capítulo não o define, o Capítulo 3 ou a documentação oficial define. Não vale carregá-lo adiante como lacuna de conhecimento sobre Spark.

### Minhas definições

Dezessete dos termos abaixo o capítulo usa sem definir, ou nem menciona. Marquei esses casos em itálico, porque neles a definição vem de fora do texto.

**Spark SQL** — O engine sobre o qual as Structured APIs são construídas. Lê e escreve formatos estruturados, aceita conectores JDBC/ODBC, oferece interface programática e shell, e suporta comandos ANSI SQL:2003 e HiveQL.

**SparkSession** — Ponto de entrada unificado para programar o Spark com as Structured APIs, introduzido no Spark 2.0. Num shell ou notebook, vem pronta na variável `spark`. Numa aplicação standalone, é criada manualmente com `SparkSession.builder`.

**`spark.sql()`** — Método da SparkSession que emite uma query SQL. Devolve sempre um DataFrame, sobre o qual dá para aplicar mais operações Spark.

**Structured APIs** — *Usado sem definição, remetido ao Capítulo 3.* O conjunto de APIs de alto nível de DataFrame e Dataset, cujo fundamento comum é o Spark SQL engine.

**DataFrame** — *Assumido.* A abstração das Structured APIs. Neste capítulo é o que toda leitura devolve, o que toda query SQL devolve, e a partir do que toda escrita começa.

**temporary view** — Nome dado a um DataFrame dentro de uma SparkSession, para que ele possa ser consultado por SQL. Não guarda dados. Desaparece quando a aplicação termina.

**global temporary view** — View visível por múltiplas SparkSessions dentro de uma mesma aplicação. Vive num database global e exige o nome qualificado. *O capítulo dá duas definições de escopo, uma delas errada, e o item 13 do Nível 5 resolve qual vale.*

**`global_temp`** — O database global em que o Spark cria as global temporary views. É por isso que o acesso exige o prefixo `global_temp.<view_name>`.

**table** — Objeto nomeado que guarda dados e tem metadata associada. Persiste depois que a aplicação Spark termina, ao contrário da view. Reside dentro de um database.

**database** — Espaço de nomes em que as tabelas residem. Por default é o `default`, e cria-se outro com `CREATE DATABASE`, selecionado por `USE`.

**metastore** — *Usado o tempo todo e nunca definido.* O armazenamento central da metadata das tabelas. Sem configuração, o Spark levanta um Derby embutido num diretório `metastore_db`, o que faz dele um banco relacional e não um diretório.

**Hive metastore** — *Nomeado como default e nunca explicado.* O metastore do Apache Hive, que o Spark adota em vez de manter um próprio. O capítulo o localiza em `/user/hive/warehouse`, e o item 4 do Nível 5 mostra que esse não é o default atual.

**warehouse** — *Assumido.* O diretório em que os dados das managed tables são gravados. É coisa distinta do metastore, que guarda só a metadata, e o capítulo trata os dois como um.

**metadata** — A informação sobre a tabela e seus dados: schema, description, table name, database name, column names, partitions e a physical location onde os dados residem.

**Catalog** — A abstração de alto nível do Spark SQL para guardar metadata. Expandida no Spark 2.x com métodos públicos como `listDatabases()`, `listTables()` e `listColumns()`. O Spark 3.0 a estende para catálogos externos.

**external catalog** — *Nomeado e adiado para o Capítulo 12.* Catálogo fornecido por um sistema de fora do Spark, plugado pela Data Source V2 e registrado em `spark.sql.catalog.<name>`. É o mecanismo por onde Iceberg e Delta Lake entram.

**managed table** — Tabela em que o Spark gerencia a metadata e os dados no file store. `DROP TABLE` apaga as duas coisas.

**unmanaged table** — Tabela em que o Spark gerencia só a metadata, e os dados ficam numa data source externa sob responsabilidade de outra pessoa. `DROP TABLE` apaga só a metadata. *A regra que decide o tipo, apontar ou não um caminho externo, é derivável dos exemplos e nunca enunciada.* A documentação atual chama isso de external table.

**Data Sources API** — O conjunto de métodos comuns para ler e escrever nas várias data sources. É o que torna a troca de formato uma troca de string em `format()`.

**DataFrameReader** — Construção central para ler dados de uma data source para um DataFrame. Só é alcançável por `SparkSession.read` ou `SparkSession.readStream`, e não pode ser instanciada.

**DataFrameWriter** — O inverso do reader. Salva ou escreve dados numa built-in data source. O handle vem de `DataFrame.write` ou `DataFrame.writeStream`, e não da SparkSession.

**parse mode** — O `mode` da Table 4-1, que decide o que fazer com registro malformado ao ler JSON ou CSV. Vale `PERMISSIVE`, `FAILFAST` ou `DROPMALFORMED`, com `PERMISSIVE` como default. *Os três são nomeados e nenhum é explicado.*

**save mode** — O `mode` da Table 4-2, que decide o que fazer quando o destino já tem dados. Vale `append`, `overwrite`, `ignore`, `error` ou `errorifexists`, com os dois últimos como default, e eles lançam exceção.

**DDL string** — Forma de declarar schema como texto, no estilo `"date STRING, delay INT"`. Alternativa a `StructType`, e alternativa a inferir.

**schema inference** — Descoberta dos tipos de coluna a partir dos dados, ligada com `option("inferSchema", "true")`. Existe só para JSON e CSV. *O capítulo a usa num arquivo de mais de um milhão de registros logo depois de aconselhar o contrário.*

**`bucketBy`** — Método do writer que recebe o número de buckets e as colunas, e usa o esquema de bucketing do Hive sobre um filesystem. *O capítulo nomeia o método e nunca diz o que é um bucket, nem o usa em exemplo nenhum.*

**`partitionBy`** — *Aparece no padrão de uso do DataFrameWriter e desaparece depois.* Não está na Table 4-2, não é explicado e não é usado. Grava a saída em diretórios por valor de coluna.

**Parquet** — Formato de arquivo colunar open source, e a data source default do Spark. Guarda o schema no footer, o que dispensa declará-lo na leitura estática.

**footer** — A parte final de um arquivo Parquet, com a versão do formato, o schema e dados de coluna como o path.

**columnar pushdown** — *Nomeado uma vez e adiado.* Benefício do Parquet que o capítulo promete tratar junto com o Catalyst optimizer, e que nunca descreve.

**vectorized reader** — Leitor que processa blocos de linhas, muitas vezes 1.024 por bloco, em vez de uma linha por vez. Reduz uso de CPU em scans, filters, aggregations e joins.

**SerDe** — Serialization and deserialization. No capítulo aparece só na expressão "Hive ORC SerDe tables", ligada ao parâmetro `spark.sql.hive.convertMetastoreOrc`.

**single-line mode** e **multiline mode** — Os dois formatos de representação do JSON. No primeiro cada linha é um objeto. No segundo o arquivo inteiro é um objeto. A opção `multiLine` escolhe, com default `false`.

**image data source** — Data source do Spark 2.4 que decodifica arquivos de imagem numa struct com `origin`, `height`, `width`, `nChannels`, `mode` e `data`, mais uma coluna `label`.

**binary file data source** — Data source do Spark 3.0 que converte cada arquivo numa linha, com `path`, `modificationTime`, `length` e `content`. Não escreve.

**`pathGlobFilter`** — Opção que carrega apenas os arquivos cujo caminho bate com um padrão glob, preservando o partition discovery.

**`recursiveFileLookup`** — Opção que, em `"true"`, ignora o partitioning data discovery no diretório. Quando ligada, a coluna `label` some da saída de imagens.

**partition discovery** — *Assumido.* O comportamento pelo qual o Spark deduz colunas a partir da estrutura de diretórios. O capítulo só o menciona ao dizer o que as duas opções acima fazem com ele.

**JDBC/ODBC** — *Nomeado no primeiro bullet, desenhado na Figure 4-1, e nunca usado.* Os conectores por onde ferramentas de BI externas alcançam o Spark SQL. O capítulo não mostra uma linha de código sobre isso, e adia a Figure 4-1 para o Capítulo 5.

**HiveQL** — *Nomeado uma vez e nunca explicado.* O dialeto SQL do Apache Hive, citado ao lado do ANSI SQL:2003 entre os comandos suportados.

**Catalyst Optimizer** — *Nomeado e remetido ao Capítulo 3.* É a peça que sustenta a afirmação de que SQL e DataFrame API percorrem uma jornada idêntica no engine.
