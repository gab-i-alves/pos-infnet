# Guia de Leitura — *Data Engineering with Databricks Cookbook*, Capítulo 1: Data Ingestion and Data Extraction with Apache Spark

**Fonte:** Pulkit Chadha, *Data Engineering with Databricks Cookbook: Build effective data and AI solutions using Apache Spark, Databricks, and Delta Lake*, 1ª ed. (Packt Publishing, 31 de maio de 2024), 438 páginas, ISBN-13 impresso 9781837633357, ISBN-13 do ebook 9781837632060. Capítulo 1, 35 páginas.

**Escopo:** os Níveis 1 a 4 são respondíveis apenas com este capítulo. O Nível 5 exige conferência em fonte externa. O Nível 6 exige o capítulo 4 do *Learning Spark*, 2ª edição, já lido.

**Sobre as figuras:** este capítulo não tem nenhuma. Rodei `pdfimages -list` nas 35 páginas e abri a página 1 renderizada. Existe uma única imagem no PDF inteiro, e ela é a barra de ferramentas do leitor da O'Reilly, não conteúdo do livro. Um cookbook de Databricks sem um único print de notebook ou de workspace é, por si, um dado sobre o capítulo.

---

## Como usar este guia

1. Leia o capítulo uma vez de ponta a ponta, sem parar. São sete receitas com a mesma forma, e a repetição fica evidente só na leitura corrida.
2. **Rode o código. Este item vale mais que todos os outros juntos.** É um cookbook, e uma receita que você não executou é texto, não conhecimento. Suba o ambiente, cole cada trecho e anote toda divergência entre o que o livro promete e o que o shell devolve. Vários trechos do capítulo não rodam como estão impressos.
3. Faça o Nível 1 de memória primeiro, depois volte ao texto para preencher as lacunas. Marque toda questão que você não conseguiu responder.
4. Faça os Níveis 2 e 3 por escrito, em frases completas. O Nível 3 pressupõe que você já executou o Nível 2 no ambiente.
5. O Nível 4 é onde o capítulo se torna útil. Ele obriga a ler as receitas como evidência, e um cookbook copiado de si mesmo deixa muito rastro. O Nível 5 vai para o backlog de estudo. O Nível 6 vai para uma nota de comparação que fica acima dos dois livros.

---

## Nível 1 — Memorização e definições

Respostas curtas e conferíveis. Uma ou duas frases cada.

**1.** Quais são as sete receitas do capítulo, na ordem? *(Chapter intro)*

R: Reading CSV data with Apache Spark. Reading JSON data with Apache Spark. Reading Parquet data with Apache Spark. Parsing XML data with Apache Spark. Working with nested data structures in Apache Spark. Processing text data in Apache Spark. Writing data with Apache Spark.

**2.** Quais quatro formatos de arquivo o capítulo promete cobrir, e quais duas capacidades adicionais ele promete no parágrafo de abertura? *(Chapter intro)*

R: CSV, JSON, Parquet e XML. As duas capacidades adicionais são analisar dado textual com natural language processing (NLP) e otimizar a escrita de dados com buffering, compression e partitioning.

**3.** Em qual linguagem o capítulo inteiro é escrito? *(Chapter intro)*

R: Python. A frase de abertura diz "you will learn how to load and write data files with Apache Spark using Python". Não há um único trecho em Scala, SQL ou R no capítulo.

**4.** Quais são os três requisitos técnicos declarados antes de começar? *(Technical requirements)*

R: Ter as imagens do `docker-compose` no ar. Abrir o servidor JupyterLab em `http://127.0.0.1:8888/lab`. Ter clonado o repositório Git do livro, com acesso ao notebook e ao dado do capítulo.

**5.** Qual comando o capítulo manda rodar ao terminar os exemplos, e onde ficam os notebooks? *(Technical requirements)*

R: `docker-compose stop`. Os notebooks e os dados ficam em `https://github.com/PacktPublishing/Data-Engineering-with-Databricks-Cookbook/tree/main/Chapter01`.

**6.** Reproduza o builder de `SparkSession` da primeira receita. Quais quatro chamadas ele encadeia, e qual linha vem logo depois? *(Reading CSV data, How to do it, step 1)*

R:

```python
spark = (SparkSession.builder
      .appName("read-csv-data")
      .master("spark://spark-master:7077")
      .config("spark.executor.memory", "512m")
      .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
```

As quatro chamadas são `appName`, `master`, `config` e `getOrCreate`. A linha seguinte baixa o nível de log para `ERROR`.

**7.** Qual é o valor de `master` em todas as receitas, e o que ele indica sobre o ambiente? *(todas as receitas, How to do it, step 1)*

R: `spark://spark-master:7077`. É a URL de um Spark standalone cluster manager, com o host `spark-master` resolvido pela rede do Docker Compose. Não é `local[*]` e não é Databricks.

**8.** Escreva a leitura de CSV com schema inferido, exatamente como o capítulo a apresenta. Quais três métodos são encadeados? *(Reading CSV data, How to do it, step 2)*

R:

```python
df = (spark.read.format("csv")
      .option("header", "true")
      .load("../data/netflix_titles.csv"))
```

Os três métodos são `format`, `option` e `load`.

**9.** Quantas linhas `show()` exibe por padrão, segundo o passo 3? Como pedir outra quantidade? *(Reading CSV data, How to do it, step 3)*

R: As primeiras 20 linhas. Para mais ou menos linhas, passa-se um argumento inteiro ao método, como em `df.show(10, truncate=False)`.

**10.** Quais duas classes o capítulo nomeia para definir um schema, e de qual módulo elas vêm? *(Reading CSV data, How to do it, step 4)*

R: `StructType` e `StructField`. Elas são importadas de `pyspark.sql.types`, junto com as classes de tipo como `StringType`, `IntegerType` e `DateType`.

**11.** No schema explícito de `netflix_titles.csv`, quais dois campos não são `StringType`, e qual tipo cada um recebe? *(Reading CSV data, How to do it, step 4)*

R: `date_added` recebe `DateType()` e `release_year` recebe `IntegerType()`. Os outros dez campos são `StringType()`.

**12.** Qual é o terceiro argumento de cada `StructField` no capítulo, e o mesmo valor aparece em todos? *(Reading CSV data, How to do it, step 4)*

R: O terceiro argumento é `True` em todos os campos, nos três schemas do capítulo. Ele indica que a coluna é nullable. O capítulo nunca nomeia nem explica esse argumento.

**13.** Como cada receita termina, e por quê? *(todas as receitas, último passo)*

R: Com `spark.stop()`. A justificativa dada é parar a aplicação Spark e liberar os recursos usados.

**14.** Liste os três problemas de CSV que o capítulo cataloga e a opção que ele oferece para cada um. *(Reading CSV data, Common issues faced while working with CSV data)*

R:

| Problema | Opção proposta |
|---|---|
| O valor do delimitador aparece dentro do dado | `option("escapeQuotes", "true")` |
| Valores null e vazios não são tratados corretamente | `option("nullValue", "null")` e `option("emptyValue", "")` |
| Formatos de data diferentes e não tratados | `option("dateFormat", "LLLL d, y")` |

*A opção do primeiro caso é write-only no Spark, e o código do segundo caso escreve `emptyValues`, no plural. Conferi as duas coisas no item 2 do Nível 5.*

**15.** O que a NOTE sobre lazy evaluation afirma que aconteceu no passo 2, e quais duas vantagens ela atribui à técnica? *(Reading CSV data, NOTE)*

R: Afirma que, ao usar a read API no passo 2, o Apache Spark não executou nenhum job. As duas vantagens são otimizar o plano de execução e recuperar-se de falhas. A mesma NOTE registra a desvantagem: dificuldade em debugging e troubleshooting.

**16.** Quais duas opções o capítulo apresenta para arquivos CSV grandes, e qual método alternativo ele indica? *(Reading CSV data, There's more)*

R: As opções são `maxColumns` e `maxCharsPerColumn`, usadas com `spark.read.csv()`, para "limitar o número de colunas e de caracteres por coluna que o Spark lê por vez". O método alternativo é `spark.readStream.csv()`, para ler o arquivo como stream. *As duas afirmações estão erradas, e conferi isso no item 3 do Nível 5.*

**17.** Contra qual versão da documentação do Spark os links de "See also" são rotulados? *(todas as receitas, See also)*

R: Spark 3.4.0. Quase todos os links de documentação são rotulados "Spark 3.4.0 documentation" ou "PySpark 3.4.0 documentation", e apontam para URLs sob `/docs/latest/`. Quatro fogem do padrão. Dois não trazem versão nenhuma: "pyspark.sql.Column.getItem documentation" e "PySpark Overview – PySpark documentation". Dois saem do domínio da documentação do Spark: o `spark-xml` no GitHub, rotulado "XML Data Source for Apache Spark 3.x", e a documentação do Apache Parquet.

**18.** Qual opção a receita de JSON define em toda leitura, e por quê? *(Reading JSON data, How to do it, step 2)*

R: `option("multiLine", "true")`. A justificativa dada é fazer o parse de registros que se espalham por várias linhas.

**19.** O que a função `explode` faz, segundo o capítulo, e qual método é usado junto com ela para substituir a coluna? *(Reading JSON data, How to do it, step 5)*

R: A `explode` cria uma nova linha para cada elemento de um array ou de um map. O método usado junto é `withColumn`, que recebe dois argumentos, o nome da nova coluna e a expressão que define os valores dela.

**20.** Qual classe de tipo é usada para declarar `laureates` no schema JSON, e o que ela envolve? *(Reading JSON data, How to do it, step 6)*

R: `ArrayType`, envolvendo um `StructType` com os cinco `StructField` do laureado. A ordem dos campos no schema é `firstname`, `id`, `motivation`, `share`, `surname`.

**21.** Quais duas opções o capítulo acrescenta à leitura de JSON com schema, além de `multiLine`? *(Reading JSON data, How to do it, step 6)*

R: `option("mode", "PERMISSIVE")` e `option("columnNameOfCorruptRecord", "corrupt_record")`.

**22.** O que a função `get_json_object()` faz, e o que ela recebe? *(Reading JSON data, There's more)*

R: Extrai um objeto JSON de uma string JSON com base em uma JSON path expression e devolve a representação em string do objeto extraído. No exemplo ela recebe a coluna `json_data` e o caminho `"$.name"`.

**23.** Segundo o capítulo, o que o modo `PERMISSIVE` faz com um registro corrompido em JSON? *(Reading JSON data, There's more, Handling corrupt data)*

R: Faz o Spark definir como null os campos com dado corrompido e guardar o registro corrompido em uma nova coluna chamada `corrupt_record`, que pode ser usada para investigar e tratar os erros.

**24.** O que a função `flatten()` faz, e qual é a restrição declarada na NOTE? *(Reading JSON data, There's more, NOTE)*

R: A `flatten()` transforma um array de arrays em um array único, mesclando todos os elementos dos arrays internos. A restrição é que ela só funciona com colunas de array. Para uma estrutura aninhada com vários níveis de array, é preciso usar `explode()` antes.

**25.** Que papel a `collect_list()` cumpre no exemplo de `flatten()`? *(Reading JSON data, There's more)*

R: Agrupa todos os arrays em uma única coluna de array, antes que a `flatten()` mescle os elementos internos. No segundo exemplo ela aparece dentro de um `groupBy("id").agg(...)`.

**26.** Como o capítulo define o Apache Parquet? *(Reading Parquet data, intro)*

R: Como "a columnar storage format designed to handle large datasets", otimizado para compressão eficiente e encoding de tipos de dado complexos.

**27.** Qual arquivo a receita de Parquet carrega, e quantos passos ela tem? *(Reading Parquet data, How to do it)*

R: `../data/recipes.parquet`. A receita tem cinco passos: importar bibliotecas, carregar o dado, ver o schema, ver o dado, parar a sessão. É a receita mais curta do capítulo.

**28.** O que acontece quando se aponta `spark.read` para um diretório de Parquet particionado? *(Reading Parquet data, Common scenarios, Reading partitioned data)*

R: O Spark reconhece o diretório como um dataset particionado e carrega o dado de acordo, sem configuração adicional. O capítulo também mostra carregar só algumas partições com um caminho curinga, como `../data/partitioned_recipes/DatePublished=2020-01*`.

**29.** Qual é o valor padrão de `spark.sql.parquet.mergeSchema`, e o que muda ao ligá-lo? *(Reading Parquet data, Common scenarios, Schema merging)*

R: O padrão é `false`. Com a opção `mergeSchema` em `true`, o Spark mescla os schemas dos arquivos e tenta criar um schema unificado que sirva para ler todos eles.

**30.** Qual observação concreta o capítulo faz sobre as colunas `ReviewCount` e `Images`? *(Reading Parquet data, Common scenarios, Schema merging)*

R: Ao ler todas as partições, o Spark inferiu `ReviewCount` e não inferiu `Images`. Ao ler só as partições `2020-01*`, inferiu `Images` e não inferiu `ReviewCount`. Com `mergeSchema` em `true`, as duas aparecem no schema inferido.

**31.** Quais três ressalvas a NOTE sobre schema merging levanta? *(Reading Parquet data, NOTE)*

R: A operação pode ser cara, sobretudo com schemas diferentes e complexos. Em alguns casos a mescla não é possível e o Spark lança erro. E o schema resultante nem sempre é o ideal para o caso de uso, o que pode exigir tratar a evolução de schema manualmente.

**32.** Qual pacote a receita de XML exige, de quem ele é, e qual coordenada Maven o capítulo dá? *(Parsing XML data, NOTE)*

R: O pacote `spark-xml`, descrito como biblioteca de terceiros lançada pela Databricks. A coordenada é `com.databricks:spark-xml_2.12:0.16.0`. *Conferi o estado atual desse pacote no item 4 do Nível 5.*

**33.** Qual string de formato a receita de XML passa a `spark.read.format()`, e qual opção é obrigatória? *(Parsing XML data, How to do it, step 2)*

R: A string é `"com.databricks.spark.xml"`. A opção usada é `rowTag`, definida como `"row"`, e ela nomeia o elemento XML que vira uma linha do DataFrame.

**34.** O que o método `getItem` faz, e o que ele recebe? *(Parsing XML data, How to do it, step 4)*

R: Extrai um único elemento ou valor de um tipo complexo, como array, map ou struct. Recebe um argumento, o índice do elemento a extrair. O exemplo é `col("laureates").getItem(0)`.

**35.** Quais quatro opções extras do leitor XML o capítulo lista, e o que cada uma faz? *(Parsing XML data, There's more)*

R:

| Opção | O que faz |
|---|---|
| `excludeAttribute` | Lista de nomes de atributo, separados por vírgula, a excluir do DataFrame |
| `inferSchema` | Define se o schema deve ser inferido a partir do dado |
| `ignoreSurroundingSpaces` | Define se os espaços ao redor devem ser ignorados |
| `mode` | Define o comportamento do leitor diante de registros corrompidos ou erros de parsing |

Nenhum valor padrão é dado para nenhuma das quatro.

**36.** Como o capítulo define `SparkSession` e `SparkContext` na receita de dados aninhados? *(Working with nested data structures, How to do it, step 1)*

R: A `SparkSession` é "a unified entry point for Spark applications", com acesso simplificado a RDDs, DataFrames, datasets, queries SQL, streaming e mais. O `SparkContext` é "the entry point to any Spark functionality", que representa a conexão com um Spark cluster e é responsável por coordenar e distribuir operações nesse cluster.

**37.** Qual dataset a receita de dados aninhados carrega? *(Working with nested data structures, How to do it, step 2)*

R: `../data/Stanford Question Answering Dataset.json`, lido com `format("json")` e `multiLine` em `true`.

**38.** Quantos `explode` a construção de `df_exploded` encadeia, e sobre quais colunas? *(Working with nested data structures, How to do it, step 3)*

R: Dois. O primeiro sobre `paragraphs`, o segundo sobre `paragraphs.qas`, com alias `questions`. Entre eles, `paragraphs.context` é extraído por dot notation com alias `context`.

**39.** O que é uma HOF no vocabulário do capítulo, e qual exemplo é dado? *(Working with nested data structures, How to do it, step 4)*

R: HOF é higher-order function. O capítulo as apresenta como funções que manipulam colunas aninhadas no lugar, sem explodir. O exemplo é `array_distinct`, usada para remover duplicatas do array `answers` dentro da coluna `qas`.

**40.** Quais três problemas comuns o capítulo cataloga para dados aninhados, e a solução de cada um? *(Working with nested data structures, Common issues and considerations)*

R:

| Problema | Solução |
|---|---|
| A `explode()` pode gerar um número grande de linhas, ineficiente de processar | Evitar a `explode` se não for preciso achatar. Usá-la sobre um subconjunto, ou agregar antes de explodir |
| Dot notation é difícil de usar com estruturas muito aninhadas | Usar `getItem` no lugar do ponto, combinada com `getField` para extrair o campo do struct resultante |
| Dado aninhado pode conter valores ausentes ou null, o que causa erro na extração | Usar `isNull` e `isNotNull` para filtrar as linhas |

**41.** Qual é a diferença entre `explode` e `explode_outer`? *(Working with nested data structures, There's more)*

R: Quando a coluna de array ou de map é null, a `explode_outer` ainda devolve uma linha para aquele null. A `explode` não devolve, ela pula o registro.

**42.** O que é tokenização, segundo o capítulo, e quais duas formas de fazê-la ele mostra? *(Processing text data, How to do it, step 5)*

R: Tokenização é o processo de quebrar dado textual em unidades menores, como palavras ou frases. As duas formas são a função `split()` sobre a coluna `Text`, e o `Tokenizer` de `pyspark.ml.feature`, com `inputCol='Text'` e `outputCol='words'`.

**43.** O que são stop words, quais três exemplos o capítulo dá, e qual componente as remove? *(Processing text data, How to do it, step 6)*

R: São palavras comuns que não carregam muito significado. Os exemplos são "the", "and" e "in". O componente é o `StopWordsRemover`, descrito como transformer, com `inputCol="words"` e `outputCol="filtered_words"`.

**44.** Quais duas expressões regulares a limpeza de texto aplica, e com qual função? *(Processing text data, How to do it, step 4)*

R: `[^a-zA-Z ]`, que casa com todo caractere não alfabético, e uma expressão de espaços múltiplos que é substituída por um espaço único. A função é `regexp_replace()`, chamada duas vezes em `withColumn`, sempre sobrescrevendo a coluna `Text`.

**45.** Descreva a sequência de quatro operações que produz a contagem de palavras. *(Processing text data, How to do it, step 7)*

R: `explode()` sobre `filtered_words`, produzindo uma palavra por linha em uma coluna `word`. Depois `groupBy("word")`. Depois `.count()`, que guarda o resultado em uma coluna `count`. Por fim `.orderBy("count", ascending=False)`.

**46.** Como o capítulo define a MLlib, e quais linguagens ele atribui a ela? *(Processing text data, How to do it, step 8)*

R: Como a biblioteca escalável de machine learning do Apache Spark, com APIs em Java, Scala, Python e R. Ela fornece algoritmos e utilitários comuns, entre eles classification, regression, clustering, collaborative filtering, feature extraction e pipelines.

**47.** Qual componente converte texto em features numéricas, e qual representação ele produz? *(Processing text data, How to do it, step 8)*

R: O `CountVectorizer`, da MLlib, com `inputCol='filtered_words'`. Ele produz uma representação bag-of-words (BoW). O uso é `vectorizer.fit(...).transform(...)`.

**48.** Como o dado processado é salvo ao fim da receita de texto? *(Processing text data, How to do it, step 9)*

R:

```python
(vectorized_data.repartition(1)
      .write.mode("overwrite")
      .json("../data/data_lake/reviews_vectorized.json"))
```

O capítulo diz que o dado processado pode ser salvo em qualquer formato desejado, como JSON ou Parquet.

**49.** O que fazem `regexp_extract()` e `rlike()`, segundo o capítulo? *(Processing text data, There's more)*

R: A `regexp_extract()` extrai de uma string as substrings que casam com um padrão de expressão regular. A `rlike()` testa se uma string casa com um padrão e devolve Boolean.

**50.** Quais quatro modos de escrita a NOTE apresenta, e o que cada um faz? *(Writing data, NOTE)*

R:

| Modo | O que faz |
|---|---|
| `overwrite` | Substitui o dado antigo pelo novo, mas derruba índices e constraints |
| `append` | Acrescenta linhas novas ao dado antigo, sem alterar nem apagar o que existe |
| `ignore` | Pula a escrita se o dado ou a tabela existir, evitando duplicatas |
| `error` ou `errorifexists` | Falha a escrita se o dado ou a tabela existir |

A NOTE diz que o parâmetro `mode` controla o que acontece quando o dado ou a tabela já existe. Ela não diz qual é o padrão. *A descrição de `overwrite` é a única do capítulo que menciona índices e constraints, e ela é o alvo do item 9 do Nível 4.*

**51.** O que o método `write` de um DataFrame devolve, segundo o capítulo? *(Writing data, How to do it, step 4)*

R: Um objeto `DataFrameWriter`, que fornece métodos para escrever o dado em vários formatos.

**52.** Quais três formatos a receita de escrita demonstra, e quais opções ela usa em cada um? *(Writing data, How to do it, steps 3 a 5)*

R: CSV, com `header` em `"true"`, `mode("overwrite")` e `delimiter` em `","`. JSON, só com `mode("overwrite")`. Parquet, só com `mode("overwrite")`. Todos usam `.save(...)` com um caminho sob `../data/data_lake/`.

**53.** Como o capítulo escreve dado comprimido, e quais dois codecs ele nomeia? *(Writing data, There's more, Writing compressed data)*

R: Com `option("codec", "org.apache.hadoop.io.compress.GzipCodec")` no writer. O outro codec nomeado é `org.apache.hadoop.io.compress.BZip2Codec`, para BZIP2.

**54.** Como o capítulo define partitioning, e quais três coisas ele diz que o partitioning afeta? *(Writing data, There's more, Specifying the number of partitions)*

R: Partitioning é a forma de dividir o dado em várias partes por um cluster, para que cada parte seja processada em paralelo por nós diferentes. As três coisas afetadas são a quantidade de data shuffling, o balanceamento de carga entre os nós e o nível de fault tolerance.

**55.** Que relação o capítulo estabelece entre número de partitions e arquivos de saída? *(Writing data, There's more, Specifying the number of partitions)*

R: O número de partitions determina o número de arquivos criados na escrita. O exemplo usa `df.repartition(4)` e o capítulo afirma que isso cria quatro arquivos.

**56.** Para que serve `coalesce()` na receita de escrita, e qual valor o exemplo usa? *(Writing data, There's more, Using coalesce())*

R: Para reduzir o número de partitions antes de escrever o dado em arquivo, quando um DataFrame grande com muitas partitions torna a escrita lenta. O exemplo usa `df.coalesce(1)`.

**57.** O que faz `partitionBy()`, e de qual classe ele é? *(Writing data, There's more, Using partitionBy())*

R: Particiona o dado com base em uma coluna durante a escrita. O capítulo o chama de propriedade da classe `DataFrameWriter`. O exemplo particiona o CSV de saída pela coluna `release_year`.

---

## Nível 2 — Compreensão

Explique com suas próprias palavras. Três a seis frases cada.

**1.** Explique por que o capítulo mostra a leitura de CSV duas vezes, primeiro sem schema e depois com schema. O que muda entre as duas? *(Reading CSV data, How to do it, steps 2 e 5)*

R: A primeira leitura declara apenas `header` em `"true"`, então o Spark aprende os nomes das colunas na primeira linha e para por aí. Sem `inferSchema` e sem `schema`, todas as colunas voltam como string. A segunda leitura passa um `StructType` com doze campos, e aí `date_added` vira `DateType` e `release_year` vira `IntegerType`. O que muda é o tipo, não o conteúdo. O capítulo mostra as duas e nunca diz que a primeira devolve tudo em string, que é o efeito prático mais importante da diferença.

**2.** Explique o que a NOTE de lazy evaluation está tentando ensinar, e por que o exemplo escolhido para ilustrá-la é o pior possível. *(Reading CSV data, NOTE + How to do it, step 2)*

R: A NOTE quer ensinar que transformations não executam nada e que só uma action dispara trabalho. A ideia está certa e é o único conceito de arquitetura que o capítulo tenta explicar. O exemplo escolhido é o passo 2, que é uma leitura de CSV com `header` em `"true"` e sem schema. Para saber os nomes das colunas, o Spark precisa abrir o arquivo e ler a primeira linha, e isso é trabalho real. Então a frase "Apache Spark did not execute any jobs" é falsa exatamente no caso que ela usa como demonstração. *Conferi o comportamento no item 7 do Nível 5.*

**3.** Explique a diferença entre `nullValue` e `emptyValue` como o capítulo as apresenta, e por que as duas precisam existir. *(Reading CSV data, Common issues)*

R: `nullValue` diz qual string no arquivo representa um valor nulo. `emptyValue` diz como tratar o valor vazio. As duas existem porque, em CSV, ausência não tem representação única. Um campo pode vir como a palavra literal `null`, como `NA`, ou como nada entre duas vírgulas. Sem as duas opções não há como distinguir "o dado não existe" de "o dado é a string vazia". O capítulo apresenta as duas juntas e nunca explica essa distinção.

**4.** Explique por que definir um schema explícito importa mais em CSV do que em Parquet, usando o que o próprio capítulo diz sobre cada formato. *(Reading CSV data, How to do it, step 4 + Reading Parquet data, intro)*

R: Um CSV é texto puro, sem nenhuma informação de tipo. Todo tipo precisa vir de fora, seja por inferência do Spark, seja por um `StructType` escrito à mão. O Parquet é columnar e o capítulo o descreve como otimizado para compressão e encoding de tipos complexos. O formato carrega o próprio schema, então a leitura de Parquet do capítulo não passa `schema` nem `inferSchema` e mesmo assim `printSchema()` devolve tipos corretos. A assimetria é essa. Em CSV o schema é uma decisão minha, em Parquet ele é um dado do arquivo.

**5.** Explique o mecanismo do `explode` sobre `laureates`, do JSON de entrada até o DataFrame de saída. Quantas linhas entram e quantas saem? *(Reading JSON data, How to do it, step 5)*

R: Cada linha do JSON é um prêmio, com uma coluna `laureates` que é um array de structs. O `explode(col("laureates"))` produz uma linha por elemento do array, e o `withColumn("laureates", ...)` substitui a coluna de array por uma coluna de struct. Um prêmio com três laureados vira três linhas, cada uma com o mesmo `category`, `year` e `overallMotivation`. Depois o `select` desce nos campos do struct por dot notation e os promove a colunas. Entram N prêmios e saem a soma dos laureados de cada prêmio, com os dados do prêmio repetidos.

**6.** Explique o que `mode("PERMISSIVE")` e `columnNameOfCorruptRecord` fazem juntos, e por que uma opção sem a outra é inútil. *(Reading JSON data, There's more, Handling corrupt data)*

R: O `mode` decide o que fazer com um registro que não faz parse. Em `PERMISSIVE`, o Spark não derruba a leitura. Ele anula os campos que não conseguiu ler e guarda o texto original do registro. O `columnNameOfCorruptRecord` diz em qual coluna esse texto original é guardado. Uma sem a outra é inútil nos dois sentidos: sem o modo permissivo não há registro corrompido preservado para guardar, e sem a coluna nomeada não há onde ler o que deu errado. *Falta uma terceira peça que o capítulo não menciona, e ela está no item 5 do Nível 5.*

**7.** Explique a diferença de propósito entre `get_json_object()` e um schema JSON declarado. Quando cada abordagem se aplica? *(Reading JSON data, There's more + How to do it, step 6)*

R: O schema declarado se aplica quando o JSON é o arquivo inteiro, e o Spark faz o parse de tudo de uma vez em colunas tipadas. A `get_json_object()` se aplica quando o JSON está dentro de uma coluna de string, em um DataFrame que já existe. É o caso do exemplo, em que `json_data` é uma coluna de texto criada por `createDataFrame`. A primeira é ingestão, a segunda é extração pontual dentro de dado já ingerido. O capítulo põe as duas na mesma receita e não marca essa fronteira.

**8.** Explique por que `flatten()` precisa de `collect_list()` antes, no exemplo do capítulo. *(Reading JSON data, There's more)*

R: A `flatten()` mescla os arrays internos de um array de arrays em um array único, dentro de uma linha. No primeiro exemplo o DataFrame já tem uma coluna que é array de arrays, mas ela está espalhada em duas linhas. A `collect_list("data")` junta as duas linhas em uma só, produzindo um array de arrays de arrays em uma linha única. Só então a `flatten()` tem sobre o que operar no nível certo. O `collect_list` é uma agregação, e é ele que muda a cardinalidade. A `flatten` só muda a forma dentro da linha.

**9.** Explique o que o experimento de schema merging do capítulo demonstra sobre a leitura de Parquet particionado. *(Reading Parquet data, Common scenarios, Schema merging)*

R: O experimento mostra que o schema de um dataset particionado não é uma propriedade fixa do dataset, é o resultado de quais arquivos o Spark abriu. Lendo tudo, apareceu `ReviewCount` e sumiu `Images`. Lendo só `2020-01*`, aconteceu o inverso. Isso acontece porque, com `mergeSchema` em `false`, o Spark toma o schema de um arquivo e o aplica ao resto. A lição prática é que o mesmo diretório pode devolver colunas diferentes conforme o filtro de caminho. Ligar `mergeSchema` custa uma varredura dos rodapés de todos os arquivos, e é por isso que ele vem desligado.

**10.** Explique por que a instalação de um pacote de terceiros para XML é uma diferença de natureza, e não de grau, em relação às três receitas anteriores. *(Parsing XML data, NOTE)*

R: CSV, JSON e Parquet são data sources embutidos. Ler qualquer um deles é `format("csv")` e nada mais. XML, na versão que o capítulo ensina, exige um JAR externo, uma coordenada Maven com versão de Scala fixada, resolução de dependência no lançamento do cluster e uma string de formato longa, `com.databricks.spark.xml`. A diferença de natureza é que as três primeiras dependem só do Spark, e a quarta depende de um projeto separado, com ciclo de release próprio. É por isso que o capítulo precisa dizer que o pacote já está instalado nas imagens Docker. *No item 4 do Nível 5 conferi que isso mudou.*

**11.** Explique quando usar `getItem` em vez de dot notation, com base no argumento do capítulo. *(Working with nested data structures, Common issues)*

R: O capítulo dá um motivo só: legibilidade em estruturas muito aninhadas, onde a cadeia de pontos fica difícil de acompanhar. O segundo motivo eu deduzi da assinatura de `getItem`, e não está no texto: dot notation não resolve acesso por índice. Para pegar o primeiro elemento de um array, é preciso `getItem(0)`, e depois `getField("text")` para descer no struct resultante. Ou seja, a escolha não é só de estilo. Dot notation navega por nome, `getItem` navega por posição ou por chave, e um array só se acessa por posição.

**12.** Explique por que o capítulo avisa contra o uso liberal de `explode`, e o que ele oferece no lugar. *(Working with nested data structures, Common issues)*

R: A `explode` multiplica linhas. Uma coluna de array com dez elementos transforma uma linha em dez, e o custo se compõe quando há explodes encadeados, como no próprio exemplo da receita. O capítulo oferece três saídas: não explodir se o achatamento não for necessário, explodir só um subconjunto do dado, ou agregar antes de explodir. A quarta saída ele mostra sem nomear como alternativa, que são as higher-order functions como `array_distinct`, capazes de operar dentro do array sem desmontá-lo.

**13.** Explique o pipeline completo de processamento de texto, do CSV bruto à matriz de features. Quantos estágios existem e o que cada um entrega? *(Processing text data, How to do it)*

R: São seis estágios. A leitura carrega `Reviews.csv` com header e `multiLine`. A limpeza com `regexp_replace` remove tudo que não é letra e normaliza espaços. A tokenização quebra a coluna `Text` em uma coluna `words`, que é um array. O `StopWordsRemover` produz `filtered_words`, sem as palavras vazias de significado. A contagem por `explode` mais `groupBy` entrega a frequência, que é diagnóstico e não alimenta o resto. O `CountVectorizer` transforma `filtered_words` em um vetor numérico, e é essa a saída que vai para o disco.

**14.** Explique por que a contagem de palavras existe nessa receita, dado que o resultado dela não é usado em nenhum passo seguinte. *(Processing text data, How to do it, step 7)*

R: Ela é diagnóstico. Depois de remover stop words, olhar as palavras mais frequentes é a forma de saber se a limpeza funcionou. O próprio capítulo entrega a prova disso na seção "There's more", onde a lista de stop words customizada é `["/><br", "-", "/>I", "/>The"]`. Esses tokens só podem ter sido descobertos olhando uma contagem de frequência de um texto com HTML residual. A contagem não alimenta o pipeline, ela alimenta a decisão sobre o pipeline. O capítulo não diz isso e apresenta o passo como se fosse mais uma etapa da linha de montagem.

**15.** Explique a diferença entre `repartition()` e `coalesce()` como o capítulo as usa, e o que ele deixa de fora. *(Writing data, There's more)*

R: O capítulo usa `repartition(4)` para definir quantos arquivos a escrita produz, e `coalesce(1)` para reduzir o número de partitions antes de escrever, quando há muitas. Nos dois casos o efeito descrito é o mesmo, controlar a contagem de arquivos de saída. O que ele deixa de fora é a diferença de mecanismo. A `repartition` redistribui todas as linhas pela rede, o que é um shuffle completo. A `coalesce` apenas junta partitions existentes no mesmo executor, sem shuffle. E `coalesce(1)` tem um efeito colateral grave que o capítulo não menciona: ele reduz o paralelismo de todos os estágios anteriores, e não só o da escrita.

**16.** Explique a diferença entre `repartition(n)` e `partitionBy(coluna)`, dado que o capítulo apresenta os dois sob o mesmo título de partitioning. *(Writing data, There's more)*

R: São operações de camadas diferentes que compartilham a palavra partition. A `repartition(4)` mexe na distribuição em memória, ou seja, em quantas fatias o DataFrame está dividido dentro do cluster. A `partitionBy('release_year')` mexe no layout do disco, criando um subdiretório por valor distinto da coluna, no formato `release_year=1997`. A primeira controla paralelismo e contagem de arquivos, a segunda controla organização física e permite pruning na leitura. O capítulo agrupa as duas em "There's more" e nunca marca que uma é de execução e a outra é de armazenamento.

---

## Nível 3 — Aplicação e transferência

Cenários concretos. O capítulo te equipa para responder, mas não responde por você. Faça no ambiente, não no papel.

**1.** Você recebe um CSV de catálogo de filmes em que os campos de sinopse contêm vírgulas dentro de aspas duplas. Aplicando a receita do capítulo, o que você faria? Depois teste, e diga o que de fato resolve. *(Reading CSV data, Common issues)*

R: Seguindo o capítulo, eu usaria `option("escapeQuotes", "true")`, que é a solução que ele oferece para "delimiter value is present within the data". Testando, isso não faz nada na leitura. A opção `escapeQuotes` é write-only, e uma opção desconhecida na leitura é ignorada em silêncio, sem aviso e sem erro. O que resolve é o comportamento padrão do próprio parser: a opção `quote` já vem como aspa dupla, e um delimitador dentro de um valor entre aspas é tratado como parte do valor. Se o arquivo usar outro caractere de citação, o ajuste é em `quote`, e o caractere de escape se ajusta em `escape`. *Conferi todos esses defaults no item 2 do Nível 5.*

**2.** Você precisa ingerir um diretório com 5.000 arquivos JSON de eventos de servidor, um objeto por linha em cada arquivo. Você copiaria a leitura da receita de JSON como está? *(Reading JSON data, How to do it, step 2)*

R: Não. A receita define `option("multiLine", "true")` em toda leitura de JSON, e o meu caso é single-line mode. Com `multiLine` em `true` o Spark trata cada arquivo como uma unidade indivisível, e o resultado é uma task por arquivo, sem paralelismo dentro do arquivo. Com `multiLine` no padrão `false`, cada arquivo pode ser dividido em blocos e lido em paralelo. O capítulo nunca diz que `multiLine` não é o padrão, nunca diz que ele custa splittability, e nunca menciona que existe single-line mode. *Conferi o custo no item 6 do Nível 5.*

**3.** Você precisa ler um único arquivo JSON de 8 GB, com um array gigante em uma linha só. O que a receita do capítulo entrega, e onde isso vai doer? *(Reading JSON data, How to do it, step 2)*

R: A receita entrega a leitura correta, porque `multiLine` em `true` é exatamente o que esse arquivo exige. Onde dói é no paralelismo. Um arquivo não divisível vira uma task, então 8 GB passam por um único core, e o restante do cluster fica parado. Não adianta aumentar o número de executors. A saída real é anterior ao Spark, e é quebrar o arquivo na origem, ou converter uma vez para um formato divisível e ler a partir dele daí em diante. O capítulo não oferece nada para esse caso, e a receita de Parquet, três páginas adiante, é a resposta que ele não conecta.

**4.** Você lê um diretório Parquet de leituras de sensores particionado por dia, e a coluna `battery_level` aparece em algumas partições e não em outras. Descreva o que acontece com e sem `mergeSchema`, e como você decidiria. *(Reading Parquet data, Common scenarios, Schema merging)*

R: Sem `mergeSchema`, o Spark toma o schema de um arquivo e o aplica ao dataset todo. Se esse arquivo não tiver `battery_level`, a coluna some da leitura inteira, inclusive para as partições que a têm, e nada avisa. É o mesmo efeito que o capítulo observou com `ReviewCount` e `Images`. Com `mergeSchema` em `true`, o Spark lê o rodapé de todos os arquivos e monta o schema unificado, então `battery_level` aparece, com null nas partições antigas.

Como eu decidiria: `mergeSchema` na leitura de exploração, para descobrir a forma real do dataset. Depois, um schema explícito fixo no pipeline de produção, porque ele é barato, é determinístico e não depende de quais arquivos entraram na varredura. A NOTE do capítulo apoia essa escolha ao dizer que a mescla é cara e que às vezes o resultado não é o schema desejado.

**5.** Você precisa carregar um feed XML de catálogo de produtos hoje. Escreva a leitura que você usaria, e diga onde a receita do capítulo te atrapalharia. *(Parsing XML data, How to do it, steps 1 e 2)*

R: A leitura que eu usaria é `spark.read.format("xml").option("rowTag", "product").load(caminho)`, sem nenhum pacote externo e sem nenhuma configuração no builder. A receita atrapalha em três pontos. Ela manda instalar `com.databricks:spark-xml_2.12:0.16.0`, e esse artefato é Scala 2.12, que o Spark 4 não aceita mais. Ela manda passar `spark.jars.packages` no builder, o que atrasa o start do cluster para resolver uma dependência desnecessária. E ela usa a string de formato `com.databricks.spark.xml`, longa e específica do pacote, em vez de `xml`. *Conferi o estado atual no item 4 do Nível 5.*

**6.** Você tem um DataFrame de pedidos de e-commerce em que `items` é um array de structs com `sku`, `qty` e `price`. Você precisa do total de itens por pedido, sem achatar. Como o capítulo te equipa? *(Working with nested data structures, How to do it, step 4 + Common issues)*

R: O capítulo aponta a direção certa e não entrega a ferramenta. A direção é a seção de HOFs, que diz que higher-order functions manipulam colunas aninhadas no lugar, e o aviso de que a `explode` gera muitas linhas e deve ser evitada quando o achatamento não é necessário. A ferramenta que ele mostra é só a `array_distinct`, que não soma nada. Para o meu caso eu preciso de `aggregate` ou `transform` sobre o array, e nenhuma das duas aparece no capítulo. Ele nomeia a categoria HOF, dá um exemplo que não é representativo dela, e lista `array_contains`, `map_keys`, `map_values` e `explode_outer`, que também não são HOFs de fato.

**7.** Você está montando um pipeline de análise de avaliações de produto e precisa saber se a limpeza de texto funcionou. Que passo do capítulo você usaria como controle de qualidade, e o que faria com o resultado? *(Processing text data, How to do it, steps 4 a 7)*

R: A contagem de palavras do passo 7, com `orderBy("count", ascending=False)`. Eu olharia o topo da lista atrás de tokens que não são palavras, resíduo de HTML, ou palavras que passaram pelo filtro de stop words e não deveriam ter passado. O que eu faria com o resultado é alimentar a lista customizada de stop words da seção "There's more". É exatamente o ciclo que produziu o `["/><br", "-", "/>I", "/>The"]` do próprio capítulo. Um detalhe importante: essa lista customizada foi extraída de um texto ainda com HTML, o que significa que ela vem de um pipeline sem o `regexp_replace` do passo 4. Aplicada depois da limpeza, ela não remove nada.

**8.** Você precisa entregar um único arquivo CSV com cabeçalho para um analista, a partir de um DataFrame de 200 partitions. Qual receita do capítulo você usa, e qual armadilha ela esconde? *(Writing data, There's more, Using coalesce())*

R: A receita é `df.coalesce(1).write.format("csv").option("header","true").mode("overwrite").save(caminho)`, que é literalmente o exemplo do capítulo. A armadilha é dupla. Primeira, `coalesce(1)` não é só uma instrução de escrita, ele propaga para trás e força todo o cálculo anterior a rodar em um único core. Segunda, o resultado não é um arquivo chamado como eu pedi. É um diretório com `_SUCCESS` e um `part-00000-...csv` dentro. Se o analista espera um arquivo, alguém precisa renomear fora do Spark. O capítulo não menciona nem uma coisa nem outra.

**9.** Você escreve um dataset de logs particionado por data com `partitionBy('event_date')`, e depois quer ler só uma semana. Trace as duas pontas usando o capítulo. *(Writing data, There's more, Using partitionBy() + Reading Parquet data, Common scenarios)*

R: Na escrita, `partitionBy('event_date')` cria um subdiretório por valor distinto, no formato `event_date=2026-08-01`, e a coluna sai do corpo dos arquivos e passa a viver no nome do diretório. Na leitura, a receita de Parquet cobre as duas formas: apontar para o diretório raiz, e o Spark reconhece a estrutura particionada, ou usar um caminho curinga para carregar só algumas partições, como o `DatePublished=2020-01*` do exemplo. Para uma semana específica, o curinga é a ferramenta mais próxima que o capítulo oferece. O capítulo não menciona a terceira forma, que é ler o diretório raiz e filtrar por `event_date` em um `where`, deixando o Spark podar as partições sozinho.

**10.** Você precisa reduzir o custo de armazenamento de um data lake de arquivos CSV. Quais três alavancas o capítulo te dá, e qual delas ele recomenda de forma mais fraca? *(Writing data, There's more + Reading Parquet data, intro)*

R: As três alavancas são compressão, com `option("codec", ...)` e Gzip ou BZip2. Contagem de partitions, com `repartition` ou `coalesce`, que muda quantos arquivos existem e o overhead de arquivos pequenos. E a troca de formato para Parquet, que o capítulo descreve como columnar e otimizado para compressão eficiente. A mais fraca é justamente a terceira, e ela é a de maior efeito. O capítulo descreve o Parquet na abertura da receita de leitura e nunca volta a esse argumento na receita de escrita, onde ele seria uma recomendação. A escrita em Parquet aparece lá como o passo 5, no mesmo tom dos passos de CSV e JSON, sem preferência declarada.

**11.** Você precisa parar todos os serviços do ambiente do livro em uma máquina instalada hoje. O comando do capítulo funciona? *(Technical requirements)*

R: Provavelmente não. O capítulo manda `docker-compose stop`, com hífen, que é a sintaxe do Compose V1. O Compose V1 parou de receber atualizações em julho de 2023 e saiu das novas versões do Docker Desktop, então instalações recentes não trazem esse binário. O comando equivalente é `docker compose stop`, com espaço. Se a máquina não tiver o alias de compatibilidade, o erro é de comando não encontrado, e não de Docker fora do ar. *Conferi as datas no item 10 do Nível 5.*

**12.** Você quer executar a receita de escrita em Databricks, e não no ambiente Docker do livro. O que precisa mudar no código do capítulo? *(Writing data, How to do it, steps 1 e 3)*

R: Duas coisas mudam e uma terceira precisa de decisão. O builder inteiro sai, porque em um notebook Databricks a `SparkSession` já existe na variável `spark`, e chamar `.master("spark://spark-master:7077")` aponta para um host que não existe. O `spark.stop()` do último passo também sai, porque parar a sessão do notebook derruba o ambiente. A decisão é sobre o caminho: `../data/data_lake/...` é caminho relativo do sistema de arquivos local do container, e em Databricks o destino seria um volume ou um caminho de object storage. O capítulo é o primeiro de um livro de Databricks e não trata de nada disso.

**13.** Você quer processar um CSV de 200 GB de logs de servidor e o capítulo sugere `spark.readStream.csv()` para arquivos grandes. Avalie a sugestão. *(Reading CSV data, There's more)*

R: A sugestão não resolve o problema que ela alega resolver. O capítulo diz que ler como stream "permite processar o dado em tempo real conforme ele é lido do disco", como remédio para problemas de memória e performance com arquivos grandes. A file source do Structured Streaming trata o arquivo como unidade de offset, e não como unidade de execução. Um arquivo inteiro cai num único micro-batch. Ela serve para um diretório em que arquivos novos chegam, e não para partir um arquivo grande em pedaços menores de memória. Além disso, ela exige schema declarado por padrão, o que o exemplo do capítulo não fornece. Para 200 GB em batch, o que funciona é o leitor normal, que já divide o arquivo em blocos. *Conferi o comportamento da file source no item 3 do Nível 5.*

---

## Nível 4 — Análise e síntese

Raciocínio que cruza receitas. Um cookbook copiado de si mesmo deixa muito rastro, e esta seção é sobre ler esse rastro.

**1.** **A receita de XML é a receita de JSON.** Compare as duas receitas linha a linha e liste toda evidência de que uma foi copiada da outra. Quantas evidências você acha, e qual delas é a mais grave?

R: Achei seis.

Primeira, o parágrafo de abertura da receita de XML diz duas vezes "JSON" onde deveria dizer XML: "We will also cover some common issues faced while working with JSON data and how to solve them. Finally, we will cover some common tasks in data engineering with JSON data."

Segunda, o passo 5 da receita de XML começa com "If the JSON data has nested structures, we can use the explode function to simplify the data." É a frase idêntica do passo 5 da receita de JSON.

Terceira, o passo 6 da receita de XML diz "If we want to enforce data types on the JSON data". Mesma troca.

Quarta, o schema do passo 6 é o mesmo `StructType` da receita de JSON, com os mesmos campos `category`, `laureates`, `overallMotivation` e `year`.

Quinta, o dado é o mesmo. As duas receitas carregam prêmios Nobel, uma de `nobel_prizes.json` e a outra de `nobel_prizes.xml`.

Sexta, o bloco de achatamento com `explode` mais `select` é o mesmo código, com diferença apenas de indentação.

A mais grave é a quarta, porque ela não é um deslize de prosa. Um schema `StructType` copiado de um parser para outro é uma afirmação técnica de que os dois formatos produzem a mesma estrutura, e o capítulo não confere isso em nenhum ponto. XML e JSON diferem em atributos, em texto misto e em como um elemento único se distingue de uma lista de um elemento. Um schema colado entre os dois é uma suposição não declarada.

**2.** **Vinte ou cinco.** O passo 3 da receita de CSV e o passo 6 da mesma receita dizem coisas diferentes sobre a mesma chamada. Encontre a contradição, decida quem está certo, e diga o que ela revela.

R: O passo 3 diz que `df.show()` exibe as primeiras 20 linhas do DataFrame. O passo 6, sobre a mesma chamada `df.show()`, diz que ela exibe as primeiras cinco linhas. As duas frases estão na mesma receita, com quatro passos de distância.

O passo 3 está certo. O padrão de `show()` é 20 linhas.

O que a contradição revela é a origem do passo 6. Ele também fala em "the n argument to the show() method", e `n` é o nome do parâmetro na assinatura Python. O passo 3 fala em "an integer argument". São duas vozes descrevendo o mesmo método com vocabulários diferentes e defaults diferentes. A leitura mais provável é que o passo 6 veio de outro texto, com outro exemplo, e foi encaixado sem ajuste. É o mesmo padrão da receita de XML, em escala menor.

**3.** **Uma receita que não roda.** Percorra a receita de Parquet e liste tudo que impede o código impresso de executar. Depois avalie o que isso diz sobre a revisão técnica do capítulo.

R: A receita de Parquet tem cinco passos e três defeitos.

O passo 2 é `df = (spark.read.format("parquet").load("../data/recipes.parquet")` sem o parêntese de fechamento. Erro de sintaxe.

O passo 3 é `df.printSchema(` sem o parêntese de fechamento. Erro de sintaxe.

O passo 3 também descreve o que faz com a frase "can be used to display the schema of the JSON data", em uma receita de Parquet. É a mesma contaminação da receita de XML.

Fora dos cinco passos, a seção de dados particionados escreve o caminho de duas formas, `../data/partioned_recipes` no primeiro exemplo e `../data/partitioned_recipes` no segundo e no terceiro. Uma delas não existe. E a prosa diz que o dado está "partitioned by recipe category", mas o caminho curinga do exemplo é `DatePublished=2020-01*`, ou seja, particionado por data de publicação. As duas afirmações não podem ser verdadeiras ao mesmo tempo.

Sobre a revisão técnica: dois parênteses faltando em cinco passos indicam que esse trecho não foi executado a partir do texto impresso. O notebook do repositório provavelmente roda, e o texto do livro foi transcrito dele à mão. É um problema estrutural de cookbook. O leitor lê o livro, o autor testa o notebook, e nada garante que os dois sejam o mesmo código.

**4.** **A promessa de buffering.** O parágrafo de abertura promete três técnicas de otimização de escrita. Confira quantas o capítulo entrega e o que fazer com a que falta.

R: A abertura promete "how to optimize data writing with buffering, compression, and partitioning". O capítulo entrega duas de três.

Compression está lá, na seção "Writing compressed data", com `codec` e dois codecs Hadoop nomeados.

Partitioning está lá, em três subseções: `repartition`, `coalesce` e `partitionBy`.

Buffering não aparece em lugar nenhum. A palavra não retorna em nenhum ponto do capítulo depois da abertura.

O que fazer com ela: nada, e essa é a resposta certa. "Buffering" não é um conceito estabelecido de escrita no Spark. Não existe uma opção de buffer no `DataFrameWriter`, e o termo mais próximo seria o tamanho de bloco ou de row group no Parquet, que é assunto de outro nível de detalhe. A promessa não foi cortada de uma seção existente, ela foi escrita em um parágrafo de abertura que lista o que soa bem. Isso é pior que uma seção faltando, porque manda o leitor procurar um conceito que não tem endereço.

**5.** **Onde está o Databricks?** Este é o capítulo 1 de um livro chamado *Data Engineering with Databricks Cookbook*. Faça o inventário do que nele é específico de Databricks, e depois julgue a decisão editorial.

R: O inventário é curto e quase todo negativo.

O ambiente é Docker Compose local com JupyterLab, não um workspace Databricks. O cluster é Spark standalone em `spark://spark-master:7077`. Não há notebook Databricks, não há cluster, não há Unity Catalog, não há DBFS nem volumes, não há magic command, não há Delta Lake. Os destinos de escrita são caminhos relativos, `../data/data_lake/...`, ou seja, um data lake que é uma pasta.

O único traço de Databricks é o pacote `spark-xml`, e ele entra como biblioteca de terceiros lançada pela Databricks, não como recurso da plataforma.

Sobre a decisão editorial, o argumento a favor é razoável. Um capítulo 1 que exige conta em nuvem exclui quem quer começar hoje, e o ambiente local é reprodutível e gratuito. Fundar o livro no Apache Spark puro também é honesto sobre o que Databricks é por baixo.

O argumento contra é mais forte para este livro em particular. Um leitor que compra um cookbook de Databricks para ingestão de arquivos vai adotar `spark.read.format(...)` como a resposta, e essa não é a resposta que a plataforma dá para ingestão incremental. O capítulo ensina o mecanismo genérico como se fosse o padrão do produto, sem uma linha dizendo que o produto tem caminho próprio. *Conferi qual é esse caminho no item 8 do Nível 5.*

**6.** **A palavra `format` como declaração de intenção.** As sete receitas sempre escrevem `format("csv")`, `format("json")`, `format("parquet")`. Argumente a favor e contra essa uniformidade.

R: A favor: uniformidade é a virtude central de um cookbook. Sete receitas com a mesma forma tornam a diferença entre elas visível de imediato, e a única coisa que muda entre a leitura de CSV e a de Parquet é a string e as opções. Escrever `format()` sempre também deixa o código independente de qualquer padrão configurado no cluster. Um pipeline que confia no padrão quebra em silêncio se `spark.sql.sources.default` for alterado.

Contra: a uniformidade esconde uma hierarquia real. Parquet é o formato padrão do Spark e `format("parquet")` pode ser omitido. Ao escrever a string sempre, o capítulo apresenta os quatro formatos como quatro escolhas equivalentes, sem preferência. E preferência existe. O próprio capítulo descreve o Parquet como columnar e otimizado, e trata CSV e JSON como formatos com uma seção inteira de problemas comuns cada. A simetria de escrita contradiz a assimetria de conteúdo.

Meu veredito: a uniformidade é a decisão certa para o código e a decisão errada para a prosa. Escrever `format()` sempre é bom estilo. Não dizer em nenhum lugar que existe um padrão, e qual é, é omissão.

**7.** **O corpus de dado como argumento.** Liste os seis datasets que o capítulo usa e o que a escolha de cada um permite demonstrar. Algum deles é escolhido por um motivo que o capítulo não declara?

R:

| Dataset | Receita | O que ele permite demonstrar |
|---|---|---|
| `netflix_titles.csv` | CSV e escrita | Colunas de texto livre com vírgulas, um campo de data em formato por extenso, um ano inteiro. É o caso de schema explícito |
| `nobel_prizes.json` | JSON | Array de structs em `laureates`, ou seja, o caso canônico de `explode` |
| `recipes.parquet` e `partitioned_recipes` | Parquet | Schema já embutido, e um diretório com schemas divergentes entre partições |
| `nobel_prizes.xml` | XML | O mesmo dado do JSON em outro formato, o que isolaria a diferença de parser |
| `Stanford Question Answering Dataset.json` | Dados aninhados | Aninhamento de três níveis, que exige explode encadeado |
| `Reviews.csv` | Texto | Texto longo em linguagem natural, com HTML residual |

O motivo não declarado está no `nobel_prizes.xml`. Usar o mesmo dado do JSON é a decisão de projeto mais inteligente do capítulo, porque ela permitiria mostrar exatamente o que muda de parser para parser com tudo o mais constante. O capítulo escolhe esse dado e desperdiça a oportunidade. Ele não compara os dois schemas inferidos, não menciona atributos XML, e reutiliza o `StructType` do JSON sem conferência. O corpus está montado para uma comparação que o texto nunca faz.

Um segundo motivo não declarado está no `Reviews.csv`. A lista de stop words customizada com `/><br` prova que o texto contém tags HTML. Isso é o que torna a receita realista, e o capítulo nunca diz de onde vem esse dado nem por que ele está sujo.

**8.** **Três níveis de tratamento de erro, nenhuma orientação.** O capítulo oferece `mode("PERMISSIVE")`, `nullValue`, `isNotNull` e o aviso da NOTE de schema merging. Monte a hierarquia de tratamento de dado ruim que o capítulo insinua e diga onde ela tem buraco.

R: A hierarquia que dá para montar tem quatro camadas.

Na camada de parsing, `mode` decide o que acontece quando um registro não faz parse. `PERMISSIVE` anula os campos ruins e preserva o registro original em `columnNameOfCorruptRecord`. O capítulo menciona `PERMISSIVE` três vezes e nunca nomeia as alternativas.

Na camada de representação, `nullValue` e `emptyValue` traduzem convenções do arquivo para o null do Spark. O registro faz parse, mas o conteúdo precisa de interpretação.

Na camada de schema, o schema explícito e o `mergeSchema` decidem quais colunas existem. Um erro aqui não gera registro ruim, ele gera coluna ausente, o que é pior porque é silencioso.

Na camada de consulta, `isNull` e `isNotNull` filtram o que sobrou.

Os buracos são três. O capítulo nunca nomeia `DROPMALFORMED` nem `FAILFAST`, então ele apresenta a política mais permissiva como se fosse a única. Ele nunca diz que `PERMISSIVE` é o padrão, o que faz a opção parecer uma escolha ativa. E ele nunca fecha o ciclo: apresenta a coluna `corrupt_record` como algo "que pode ser usado para investigar e tratar os erros" e não mostra uma única linha que a leia. *Falta ainda uma peça técnica para essa coluna existir, e ela está no item 5 do Nível 5.*

**9.** **Índices e constraints.** A NOTE sobre modos de escrita diz que `overwrite` "drops indexes and constraints". Confronte essa frase com o resto do capítulo.

R: A frase não se sustenta em nenhum lugar do capítulo.

Todas as escritas do capítulo vão para arquivos, em caminhos como `../data/data_lake/netflix_csv_data`. CSV, JSON e Parquet são arquivos em diretório. Nenhum dos três tem índice. Nenhum dos três tem constraint. Não existe nada para derrubar.

O capítulo também nunca escreve em uma tabela. Não há `saveAsTable`, não há `CREATE TABLE`, não há metastore, não há catálogo. Então nem o caso em que a frase poderia fazer algum sentido aparece no capítulo.

A origem provável é uma descrição genérica de modo de escrita, escrita para um destino relacional e colada aqui. Os outros três modos da mesma NOTE são descritos de forma correta e neutra, o que reforça que só essa linha veio de outro contexto.

O custo pedagógico é específico. Um leitor que já trabalhou com banco relacional lê essa frase e conclui que `overwrite` faz algo destrutivo além de substituir os dados, e pode evitar o modo por medo. Um leitor sem esse repertório aprende que arquivos Parquet têm índices, o que vai atrapalhá-lo mais tarde.

**10.** **A palavra partition significa três coisas.** Rastreie os três sentidos no capítulo e diga por que a colisão importa.

R: Primeiro sentido, unidade de paralelismo em memória. É a definição da seção de escrita: dividir o dado em partes por um cluster para processar em paralelo, afetando shuffle, balanceamento e fault tolerance. É o sentido de `repartition(4)` e de `coalesce(1)`.

Segundo sentido, diretório no disco. É o sentido de `partitionBy('release_year')` na escrita, e de `partitioned_recipes` na leitura, com o caminho `DatePublished=2020-01*`. Aqui uma partition é uma pasta com um par chave e valor no nome.

Terceiro sentido, arquivo de saída. Ele aparece na frase "the number of partitions determines the number of files created while writing the data". Esse é um efeito do primeiro sentido, não um sentido novo, mas o capítulo o enuncia como se fosse a definição.

A colisão importa por uma razão prática. Um leitor que faz `repartition(4)` e depois `partitionBy('release_year')` na mesma escrita precisa saber que os dois compõem, e o resultado é até quatro arquivos por diretório de ano. Sem separar os sentidos, o número de arquivos gerado parece arbitrário. O capítulo usa as três acepções em duas páginas e não distingue nenhuma delas.

**11.** **A receita de texto quebra o padrão do capítulo.** Identifique em que ela difere das outras seis e decida se a diferença é uma falha de coerência ou uma escolha correta.

R: Ela difere em quatro pontos.

É a única que importa de `pyspark.ml`, e não só de `pyspark.sql`. `Tokenizer`, `StopWordsRemover` e `CountVectorizer` vêm da MLlib.

É a única que tem dez passos, contra cinco a sete das outras.

É a única que escreve dado, e ela escreve antes da receita que ensina a escrever.

É uma das duas cujo tema não é um formato de arquivo. A outra é a sétima, de escrita, também organizada por tarefa. As cinco restantes são organizadas por formato ou por estrutura.

Sobre o veredito, é uma escolha defensável e mal posicionada. Defensável porque o capítulo promete NLP na abertura e precisa cumprir. Mal posicionada porque ela é a sexta de sete, e a sétima é justamente a receita de escrita. A ordem correta seria escrever antes de precisar escrever. Do jeito que está, o passo 9 da receita de texto usa `repartition(1).write.mode("overwrite").json(...)` sem nenhuma explicação, e a explicação de cada uma dessas três chamadas chega na página seguinte.

Vale registrar também que a promessa de NLP é entregue pela metade. Tokenização, remoção de stop words e bag-of-words são pré-processamento. Nenhuma análise linguística acontece, nenhum modelo é treinado, e a saída é um vetor esparso salvo em JSON.

**12.** **A contradição do `contains_qood`.** Olhe o exemplo de `rlike()` e conte quantas coisas discordam entre si.

R: Três, em três linhas.

O código testa `expr("text rlike 'quick'")`, ou seja, procura a palavra "quick".

A coluna resultante se chama `contains_qood`, que não é palavra nenhuma. Parece "good" com um q, contaminado pelo exemplo anterior, que usava `\bq\w*` para achar palavras começadas em q.

A prosa diz que a coluna "contains a Boolean value indicating whether the text data contains the word good".

Então o código procura "quick", o nome da coluna sugere um híbrido de "q" e "good", e o texto afirma "good". Nenhum dos três concorda com os outros dois.

O que isso mostra é o mecanismo de erro do capítulo em miniatura. O exemplo anterior era sobre palavras com q. Este foi escrito por cima daquele, o nome da coluna carregou o resíduo, e a prosa foi copiada de uma terceira versão. É o mesmo processo que produziu "JSON" na receita de XML, só que compactado em três linhas.

**13.** **O que o capítulo nunca lê nem escreve.** Faça o inventário das ausências e ordene por gravidade para um livro de engenharia de dados em Databricks.

R:

1. **Delta Lake.** Nenhuma menção, em um livro cujo subtítulo o nomeia. É a ausência mais grave, porque toda a receita de escrita entrega arquivos sem transação, sem versionamento e sem schema enforcement.
2. **Tabelas.** Não há `saveAsTable`, não há `CREATE TABLE`, não há catálogo, não há metastore. O capítulo se chama "Data Ingestion" e ingere só para arquivo.
3. **Ingestão incremental.** Nenhuma menção a como carregar apenas o dado novo. Toda leitura é do diretório inteiro, toda escrita é `overwrite`.
4. **Avro e ORC.** Dois formatos columnar e de serialização amplamente usados, ausentes de um capítulo sobre formatos.
5. **JDBC.** Nenhuma leitura de banco relacional, embora ingestão de RDBMS seja uma tarefa central de engenharia de dados.
6. **`bucketBy`.** Citado em nenhum lugar, mesmo com uma seção inteira sobre partitioning na escrita.
7. **Verificação de resultado.** Nenhuma receita conta linhas, compara entrada e saída, ou confere que o que foi escrito pode ser lido de volta.

O padrão das sete: o capítulo cobre bem a mecânica de arquivo e não cobre nada do que transforma um arquivo em dado governado. Isso é coerente com o ambiente Docker que ele escolheu, e incoerente com o título do livro.

**14.** **Um cookbook precisa de "How it works"?** O formato canônico de receita Packt tem "Getting ready", "How to do it", "How it works", "There's more" e "See also". Descubra quais o capítulo usa, e avalie a ausência.

R: O capítulo usa "How to do it" e "See also" nas sete receitas, e "There's more" em seis. A receita de Parquet é a exceção: ela vai de "How to do it" direto para "Common scenarios encountered while working with Parquet data" e daí para "See also". Não usa "Getting ready", que foi substituído pela seção única de "Technical requirements" no início do capítulo. E não usa "How it works" em nenhuma das sete.

Três receitas têm seções de diagnóstico no lugar: "Common issues faced while working with CSV data", "Common scenarios encountered while working with Parquet data" e "Common issues and considerations" na receita de dados aninhados. Elas ficam entre "How to do it" e "There's more", exatamente onde "How it works" ficaria.

A ausência é a decisão que mais define o capítulo. "How it works" é a seção que explica por que o passo funciona, e sem ela o leitor recebe uma sequência de chamadas correta e nenhum modelo mental. É por isso que o capítulo pode dizer que `escapeQuotes` resolve delimitador no dado, que `maxColumns` limita o que o Spark lê por vez, e que `overwrite` derruba índices. Nenhuma dessas frases sobreviveria a uma seção que exigisse explicar o mecanismo.

O único lugar em que o capítulo tenta uma explicação de mecanismo é a NOTE de lazy evaluation, e é justamente ali que ele erra. A conclusão que eu tiro é a inversa da intuitiva: não é que faltou espaço para o "How it works". É que a explicação, quando tentada, mostrou que o autor não a tinha.

---

## Nível 5 — Além do capítulo (backlog, não notas)

Escrito contra o Spark 3.4.0, publicado em maio de 2024. Conferi todos os itens abaixo contra fonte primária em **3 de agosto de 2026**, quando a documentação corrente do Spark era a **4.2.0**. As URLs estão ao fim da seção.

**1.** **A versão do Spark.** O capítulo rotula toda a documentação como 3.4.0 e aponta para `/docs/latest/`. Confira o que esses links servem hoje e quanto o horizonte do livro envelheceu.

R: Quase todos os links de "See also" apontam para `https://spark.apache.org/docs/latest/...`. Hoje o `/latest/` serve a **4.2.0**. Ou seja, os rótulos dizem 3.4.0 e as URLs entregam 4.2.0, e nenhum leitor é avisado da diferença. Quatro links escapam do padrão, dois sem versão no rótulo e dois fora do domínio da documentação do Spark, conforme listei na questão 17 do Nível 1.

O salto é grande. A 4.2.0 exige Java 17, 21 ou 25, Scala 2.13, Python 3.10 ou superior e R 4.0 ou superior, este já depreciado. Entre a 3.4 e a 4.2 aconteceram, entre outras coisas: modo ANSI SQL ligado por padrão no 4.0.0 (SPARK-44444), data source XML embutido no 4.0.0 (SPARK-44265), tipo VARIANT (SPARK-45827), Python Data Source API (SPARK-44076), remoção do Scala 2.12 com o 2.13 como padrão (SPARK-45314) e remoção do JDK 8 e 11 com o JDK 17 como padrão (SPARK-45315).

Duas consequências diretas para este capítulo. O JAR `spark-xml_2.12` não carrega em um Spark 4, porque o Scala 2.12 saiu. E o modo ANSI ligado muda o comportamento de casts e de aritmética inválida, que antes devolviam null e agora lançam exceção.

**2.** **As opções de CSV que o capítulo erra.** Confira `escapeQuotes`, `emptyValue`, `maxColumns` e `maxCharsPerColumn` na documentação de CSV data source. Quantas o capítulo acerta?

R: Nenhuma das quatro está correta como o capítulo a apresenta.

**`escapeQuotes`** é **write-only**, com default `true`. A documentação a descreve como o flag que indica se valores contendo aspas devem sempre ser envolvidos em aspas na escrita. Ela não tem efeito nenhum na leitura, e o capítulo a oferece justamente como solução de um problema de leitura. O que resolve delimitador dentro do dado na leitura é a opção `quote`, cujo default já é a aspa dupla, com a descrição literal "Sets a single character used for escaping quoted values where the separator can be part of the value". O escape dentro do valor citado se ajusta em `escape`, default `\`.

**`emptyValue`** existe e é read/write. O default é vazio na leitura e `""` na escrita. O texto do capítulo escreve o nome certo na prosa e `emptyValues`, no plural, dentro do código. O nome no plural não existe, e o Spark ignora opções desconhecidas sem erro.

**`maxColumns`** é read, com default **20480**. É um limite rígido de segurança do parser, não um controle de quanto o Spark lê por vez.

**`maxCharsPerColumn`** é read, com default **-1**, que significa sem limite. Também é guarda do parser.

A frase do capítulo, "to limit the number of columns and characters per column that Spark reads at a time", descreve um mecanismo de leitura em blocos que não existe. As duas opções só decidem quando o parser desiste de um registro.

**3.** **`spark.readStream.csv()` para arquivos grandes.** Confira o comportamento da file source do Structured Streaming e diga se a sugestão do capítulo é válida.

R: Não é válida, por três motivos documentados.

O arquivo é a unidade de **rastreio de offset**, não a unidade de execução. Dentro de um micro-batch, o `FileStreamSource` monta um `DataSource` comum sobre os caminhos e o arquivo é fatiado em tasks igual à leitura batch. O que a file source garante é outra coisa: um arquivo inteiro cai num único micro-batch. Por isso o streaming não alivia memória para um arquivo grande. Ele só muda quando o arquivo entra, não como ele é lido.

A file source **exige schema por padrão**. A documentação é explícita: "By default, Structured Streaming from file based sources requires you to specify the schema, rather than rely on Spark to infer it automatically." O motivo dado é garantir schema consistente entre execuções, inclusive em caso de falha. Para reabilitar a inferência é preciso ligar `spark.sql.streaming.schemaInference`, cujo default é `false`. O exemplo do capítulo não passa schema.

A file source é para **diretórios em que arquivos novos chegam**, e os arquivos precisam ser colocados atomicamente no diretório. Ela tem opções como `maxFilesPerTrigger`, `maxBytesPerTrigger`, `latestFirst`, `maxFileAge` e `cleanSource`, e todas são sobre gerenciar um fluxo de arquivos, não sobre um arquivo.

Para um CSV único e grande em batch, o leitor normal já divide o arquivo em blocos e paraleliza. A sugestão do capítulo troca uma solução que funciona por uma que não se aplica.

**4.** **O pacote `spark-xml`.** Descubra o que aconteceu com `com.databricks:spark-xml` e como se lê XML no Spark hoje.

R: A receita inteira está obsoleta.

O repositório `databricks/spark-xml` foi **arquivado pelo dono em 24 de março de 2025** e está em modo somente leitura. A última versão é a **0.18.0**, e o próprio README já registrava que a biblioteca "will remain in maintenance mode for Spark 3.x versions" e que estava planejada para virar parte do Apache Spark 4.0.

O **XML virou data source embutido no Spark 4.0.0**, via SPARK-44265. Hoje a documentação tem uma página própria de XML Files, e a leitura é `spark.read.option("rowTag", "person").format("xml").load(path)`. Nada de JAR, nada de `spark.jars.packages`, nada de `com.databricks.spark.xml`.

Os defaults documentados hoje, que o capítulo lista sem valores: `rowTag` é obrigatório, `rootTag` tem default `ROWS`, `excludeAttribute` é `false`, `inferSchema` é `true`, `ignoreSurroundingSpaces` é `true` e `mode` é `PERMISSIVE`. A versão embutida também traz `from_xml()`, `to_xml()`, `schema_of_xml()`, validação por XSD e tratamento de namespace, nada disso mencionado no capítulo.

Detalhe que mata a receita mesmo em quem tentar teimar: o artefato do capítulo é `spark-xml_2.12`, e o Spark 4 removeu o Scala 2.12.

**5.** **A coluna `corrupt_record` que nunca aparece.** Confira o que a documentação de JSON exige para o `columnNameOfCorruptRecord` funcionar com um schema declarado.

R: O capítulo omite a condição que faz a opção funcionar, e por isso o exemplo dele não produz nenhum registro corrompido.

A documentação de JSON diz que, no modo `PERMISSIVE`, para manter os registros corrompidos **o usuário precisa declarar no schema um campo de tipo string com o nome definido em `columnNameOfCorruptRecord`**. Se o schema não tem esse campo, os registros corrompidos são descartados durante o parse. Quando o schema é inferido, o campo é adicionado implicitamente.

O `json_schema` do capítulo tem quatro campos: `category`, `laureates`, `overallMotivation` e `year`. Não há nenhum campo `corrupt_record`. Então a opção `columnNameOfCorruptRecord("corrupt_record")` é passada, aceita, e não faz nada, porque não há coluna de destino no schema declarado.

A correção é uma linha: acrescentar `StructField('corrupt_record', StringType(), True)` ao `StructType`.

O default de `columnNameOfCorruptRecord` é o valor da configuração `spark.sql.columnNameOfCorruptRecord`. O default de `mode` é `PERMISSIVE`, o que significa que a linha `option("mode", "PERMISSIVE")` do capítulo também é redundante.

**6.** **O custo de `multiLine`.** Confira o default e o efeito da opção que o capítulo liga em toda leitura de JSON.

R: O default de `multiLine` é **`false`**, tanto em JSON quanto em CSV, e o capítulo nunca diz isso.

O efeito de ligá-la é perder a divisibilidade do arquivo. A base de conhecimento da Databricks é direta sobre o mecanismo: com `multiLine` em `true`, "file processing cannot be parallelized, so only a single task ends up handling the entire dataset". O Spark precisa tratar o arquivo como unidade para não cortar um registro que atravessa linhas, e o preço é uma task por arquivo.

Isso muda a leitura das três receitas que usam a opção. Em `nobel_prizes.json` e em `Stanford Question Answering Dataset.json` a opção é necessária, porque os arquivos são JSON formatado em múltiplas linhas. Em `Reviews.csv` ela é necessária porque as avaliações contêm quebras de linha dentro dos campos. Em nenhum dos três casos ela é gratuita.

A consequência para o meu uso: `multiLine` é uma decisão sobre a forma do arquivo, não um ajuste que se copia de receita. Antes de ligá-la eu preciso olhar o arquivo, e se ele for grande e single-line, ligá-la é o pior erro de performance possível na ingestão.

**7.** **A NOTE de lazy evaluation.** Confira se a leitura do passo 2 realmente não dispara nenhum job.

R: A afirmação é falsa para o passo 2, e o motivo é documentado em parte.

A documentação de CSV diz que `inferSchema` "requires one extra pass over the data", ou seja, inferir schema executa trabalho. Isso já derruba a ideia de que uma leitura de CSV é sempre gratuita. O passo 2 do capítulo não usa `inferSchema`, mas usa `header` em `"true"` e não declara schema, então o Spark precisa abrir o arquivo e ler a primeira linha para descobrir os nomes das colunas. Sem isso ele não tem como devolver um DataFrame com colunas nomeadas.

Corrijo a atribuição da causa. O job não vem do `header`, vem da ausência de schema. O `CSVDataSource.inferSchema` só roda quando o usuário não declara schema, e o `TextInputCSVDataSource.infer` executa um `take(1)`, que é action e portanto dispara job. Isso acontece com `header` em `false` também, porque o Spark ainda precisa da primeira linha para contar as colunas. Conferi em `CSVDataSource.scala`, então não é inferência.

A parte da NOTE que continua correta é a geral: transformations são lazy e só uma action dispara execução. O erro é de exemplo, não de conceito. Um exemplo correto seria a leitura de Parquet, que pega o schema do rodapé, ou uma leitura de CSV com schema explícito, que é o passo 5 da mesma receita.

**8.** **Ingestão em Databricks hoje.** O capítulo ingere arquivos com `spark.read`. Descubra o que a plataforma recomenda para ingestão de arquivos de object storage.

R: A plataforma tem duas ferramentas próprias, e o capítulo não cita nenhuma.

**Auto Loader** processa incrementalmente arquivos novos que chegam em cloud storage, sem setup adicional. Ele é uma source de Structured Streaming chamada `cloudFiles`, e suporta JSON, CSV, XML, Parquet, Avro, ORC, texto e binário. Tem dois modos de descoberta de arquivo: directory listing, que é o padrão, e file notification.

**COPY INTO** é o comando SQL que carrega dado de um caminho para uma tabela Delta. Ele é idempotente, ou seja, pula arquivos já carregados, e suporta os mesmos formatos mais schema inference e evolution. Ele não está depreciado. A documentação, atualizada em 11 de junho de 2026, registra a recomendação atual: "For a more scalable and robust file ingestion experience, Databricks recommends that SQL users use streaming tables."

A diferença de fundo com o capítulo é o estado. `spark.read` sobre um diretório relê tudo toda vez e não sabe o que já processou. Auto Loader e COPY INTO mantêm registro do que já entrou. Para uma ingestão que roda todo dia, essa é a única diferença que importa, e o capítulo não a menciona.

**9.** **Runtime, catálogo e Delta.** Levante as versões e os padrões atuais da plataforma que dá nome ao livro.

R: **Databricks Runtime.** A versão mais nova é a **DBR 19**, de 15 de junho de 2026, com **Apache Spark 4.2.0**. A LTS mais nova é a **DBR 18**, de 10 de junho de 2026, com **Spark 4.1.0**, com suporte até 10 de junho de 2029. As LTS anteriores em suporte são 17.3 (Spark 4.0.0), 16.4 (Spark 3.5.2), 15.4 e 14.3 (Spark 3.5.0) e 13.3 (Spark 3.4.1). A DBR contemporânea do livro seria da faixa 13.x, que é a única ainda em suporte com Spark 3.4.

**Unity Catalog.** É o padrão. A documentação diz que "Unity Catalog is automatically enabled for all Databricks workspaces created after November 8, 2023". O Hive metastore por workspace é descrito como "a legacy feature", e a recomendação é explícita: "Databricks recommends that you migrate those tables and the workloads that reference them to Unity Catalog". O motivo dado é a ausência de auditoria, lineage e controle de acesso embutidos no metastore legado. Nenhuma data de fim de suporte foi anunciada.

**Delta Lake.** A versão mais recente é a **4.3.1**, de 8 de julho, construída sobre Apache Spark 4.1.0 e 4.0.1.

**Databricks Community Edition.** Foi aposentada e substituída pela **Databricks Free Edition**, anunciada no Data + AI Summit de 2025. O aviso oficial da comunidade diz que a Community Edition se aposenta em 1º de janeiro de 2026. Isso não afeta este capítulo, que roda em Docker, mas afeta qualquer instrução de conta gratuita nos capítulos seguintes.

**10.** **Comandos que mudaram de nome.** Confira o comando de encerramento do ambiente e a linha de instalação de pacote.

R: **`docker-compose stop`** usa a sintaxe do Compose V1. O Compose V1 parou de receber atualizações a partir de **julho de 2023** e saiu das novas versões do Docker Desktop. A forma atual é `docker compose stop`, com espaço, porque o Compose V2 é um plugin do CLI do Docker. Em máquinas instaladas hoje o binário com hífen normalmente não existe, e o erro é de comando não encontrado.

**`$SPARK_HOME/bin/spark-shell --packages ...`** continua válido como sintaxe. O que não é mais válido é o pacote que ela instala, pelo item 4 deste nível. O texto do capítulo também imprime um travessão no lugar do hífen duplo, e quatro asteriscos soltos no fim da linha, o que é defeito de conversão e não de conteúdo.

**11.** **O modo ANSI.** Descubra o que muda no comportamento do Spark entre a versão do livro e a atual, e o que isso significa para o schema explícito da receita de CSV.

R: **`spark.sql.ansi.enabled` tem default `true` na 4.2.0.** A configuração existe desde a 3.0.0, e o default virou `true` no Spark 4.0.0, via SPARK-44444.

Com ANSI ligado, duas famílias de comportamento mudam. Aritmética inválida lança exceção em vez de circular: `SELECT 2147483647 + 1` lança `[ARITHMETIC_OVERFLOW]` e antes devolvia `-2147483648`. Cast inválido lança exceção em vez de devolver null: `CAST('a' AS INT)` lança `SparkNumberFormatException` e antes devolvia null. Funções como `to_date`, `to_timestamp` e `unix_timestamp` também falham em vez de devolver null.

O que isso significa para a receita de CSV: o schema explícito declara `release_year` como `IntegerType` e `date_added` como `DateType`. Um valor não numérico na coluna de ano, ou uma data fora do `dateFormat` declarado, muda de comportamento entre uma versão e outra.

Registro a fronteira aqui também. A falha de parse de um registro CSV é governada pela opção `mode`, cujo default é `PERMISSIVE`, e não pelo flag ANSI. O que o flag ANSI muda de forma documentada são casts e expressões. Não conferi na documentação uma afirmação de que o parser de CSV mude de comportamento com o flag, então não afirmo isso. O que eu levo como alvo de teste é rodar a receita com um valor inválido em `release_year` nas duas versões e comparar.

**12.** **Compressão: `codec` ou `compression`?** O capítulo usa `option("codec", "org.apache.hadoop.io.compress.GzipCodec")` e um nome de classe completo. Confira se isso ainda funciona e qual é a forma documentada.

R: Funciona, e é a forma longa de escrever a forma curta.

No código-fonte do Spark 4.0.0, `CSVOptions.scala` resolve o codec assim: `parameters.get(COMPRESSION).orElse(parameters.get(CODEC))`. Ou seja, `codec` é aceito como alias de `compression`.

O valor também é resolvido nos dois formatos. `CompressionCodecs.getCodecClassName` procura o nome curto em uma tabela e, se não achar, devolve a própria string. O comentário no fonte é literal: "Return the full version of the given codec class. If it is already a class name, just return it."

A forma documentada é `option("compression", "gzip")`, com nomes curtos como `none`, `bzip2`, `gzip`, `lz4`, `snappy` e `deflate`. Ela é mais curta, é a que a documentação lista por formato, e não amarra o código a um nome de classe do Hadoop.

Este é o único caso em que o capítulo escreve algo tecnicamente correto de uma forma desnecessariamente frágil, em vez de escrever algo errado.

### Fontes consultadas

Todas acessadas em 3 de agosto de 2026. Todas são fonte primária: documentação oficial, notas de release, base de conhecimento do fornecedor ou código-fonte do projeto.

Documentação do Apache Spark, versão 4.2.0:

- [Overview, com as versões de Java, Scala, Python e R](https://spark.apache.org/docs/4.2.0/index.html)
- [CSV Files, com escapeQuotes write-only, quote, escape, emptyValue, maxColumns e maxCharsPerColumn](https://spark.apache.org/docs/4.2.0/sql-data-sources-csv.html)
- [JSON Files, com multiLine, mode e a exigência de declarar columnNameOfCorruptRecord no schema](https://spark.apache.org/docs/4.2.0/sql-data-sources-json.html)
- [Parquet Files, com o default de spark.sql.parquet.mergeSchema e partition discovery](https://spark.apache.org/docs/4.2.0/sql-data-sources-parquet.html)
- [XML Files, o data source embutido](https://spark.apache.org/docs/4.2.0/sql-data-sources-xml.html)
- [Generic File Source Options](https://spark.apache.org/docs/4.2.0/sql-data-sources-generic-options.html)
- [Structured Streaming Programming Guide, com a file source e spark.sql.streaming.schemaInference](https://spark.apache.org/docs/4.2.0/streaming/apis-on-dataframes-and-datasets.html)
- [ANSI Compliance, com o default de spark.sql.ansi.enabled](https://spark.apache.org/docs/4.2.0/sql-ref-ansi-compliance.html)

Notas de release e código-fonte:

- [Spark Release 4.0.0, com SPARK-44444, SPARK-44265, SPARK-45314 e SPARK-45315](https://spark.apache.org/releases/spark-release-4-0-0.html)
- [`CSVOptions.scala` na tag v4.0.0, com o alias `codec`](https://raw.githubusercontent.com/apache/spark/v4.0.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/csv/CSVOptions.scala)
- [`CompressionCodecs.scala` na tag v4.0.0, com a resolução de nome curto e nome de classe](https://raw.githubusercontent.com/apache/spark/v4.0.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/CompressionCodecs.scala)
- [databricks/spark-xml, repositório arquivado em 24 de março de 2025](https://github.com/databricks/spark-xml)

Databricks:

- [Databricks Runtime release notes, versões e compatibilidade](https://docs.databricks.com/aws/en/release-notes/runtime/)
- [Auto Loader](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)
- [COPY INTO, com a recomendação de streaming tables](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/copy-into/)
- [Unity Catalog, com a data de habilitação automática](https://docs.databricks.com/aws/en/data-governance/unity-catalog/)
- [Hive metastore como legacy feature](https://docs.databricks.com/aws/en/data-governance/unity-catalog/hive-metastore)
- [Base de conhecimento sobre multiline e paralelismo](https://kb.databricks.com/data-sources/increase-in-processing-time-for-csv-file-with-multiline-option-set-to-true)
- [Aviso de aposentadoria da Community Edition](https://community.databricks.com/t5/announcements/psa-community-edition-retires-on-january-1-2026-move-to-the-free/td-p/141888)

Delta Lake e Docker:

- [Delta Lake, release 4.3.1](https://github.com/delta-io/delta/releases/tag/v4.3.1)
- [Docker, Compose V2 e a depreciação do V1](https://www.docker.com/blog/new-docker-compose-v2-and-v1-deprecation/)

Referência bibliográfica:

- [Registro do livro com data, ISBN e paginação](https://books.google.com.br/books/about/Data_Engineering_with_Databricks_Cookboo.html?id=OMkHEQAAQBAJ)

Defaults de opção mudam entre releases, e este capítulo já perdeu três deles. Preciso reconferir estes dados antes de usá-los em prova ou em decisão técnica.

---

## Nível 6 — Cross-book (contra *Learning Spark*, 2ª edição, Capítulo 4)

Dois capítulos sobre o mesmo assunto, ler e escrever dado em formatos diferentes, escritos com quatro anos de distância e em gêneros opostos. Chadha é receita, Damji é exposição. A comparação mais rica é sobre o que cada gênero entrega e o que cada um deixa cair.

**1.** **Cobertura de formatos.** Liste os formatos que cada capítulo cobre e diga o que a diferença revela sobre o público de cada livro.

R:

| Formato | Chadha, cap. 1 | Damji, cap. 4 |
|---|---|---|
| CSV | receita completa, com problemas comuns | seção com tabela de opções |
| JSON | receita completa, mais funções de extração | seção com tabela de opções |
| Parquet | receita curta, mais schema merging | seção, e é o formato de abertura |
| XML | receita completa, via pacote de terceiros | ausente |
| Avro | ausente | seção com tabela de opções |
| ORC | ausente | seção, com vectorized reader |
| Imagem | ausente | seção |
| Binário | ausente | seção |
| Texto | receita de NLP | ausente como data source |

Damji cobre sete formatos e nenhum problema. Chadha cobre quatro formatos e três seções de problema.

O que a diferença revela: Damji escreve para quem precisa saber que a opção existe, e por isso ele prioriza extensão e tabelas de referência. Chadha escreve para quem já está com o arquivo aberto e o erro na tela, e por isso ele prioriza o caso ruim. Nenhum dos dois cobre JDBC, o que é curioso nos dois casos.

Uma observação sobre a ordem. Damji abre por Parquet e diz por quê, é o padrão do Spark. Chadha abre por CSV e não diz por quê. A ordem de Damji é uma recomendação disfarçada de sumário. A de Chadha é ordem de familiaridade.

**2.** **A tabela contra a receita.** Damji tem as Tabelas 4-1 e 4-2, que enumeram métodos, argumentos e opções de `DataFrameReader` e `DataFrameWriter`. Chadha não tem tabela nenhuma. O que cada forma consegue fazer que a outra não consegue?

R: A tabela consegue declarar defaults. A Tabela 4-1 diz que o padrão de `format()` é Parquet ou o que estiver em `spark.sql.sources.default`, e que o modo padrão é `PERMISSIVE`. A Tabela 4-2 diz que o padrão de escrita é `error` ou `errorifexists`. A Tabela 4-4 diz que `inferSchema` é `false`, que `header` é `false` e que o `escape` padrão é `\`. Chadha declara dois defaults no capítulo inteiro, e nenhum a mais: o `mergeSchema` em `false` e as vinte linhas de `show()`. Fora esses dois, nada. É essa escassez que permite a ele apresentar `option("mode", "PERMISSIVE")` como escolha, quando é o padrão, e omitir que uma leitura sem `inferSchema` devolve tudo em string.

A receita consegue declarar sequência e contexto de falha. A Tabela 4-1 lista `option()` com um exemplo genérico. A receita de Chadha diz qual opção usar quando o delimitador aparece dentro do dado, e qual usar quando as datas vêm em outro formato. Damji nunca liga uma opção a um sintoma.

O padrão que fica: a tabela é boa para saber o que existe e ruim para saber quando usar. A receita é o inverso. E as duas falham do mesmo jeito quando o autor não checa, porque Chadha erra `escapeQuotes` e Damji nem a lista.

**3.** **O caminho para uma tabela.** Damji dedica seções inteiras a tabelas gerenciadas, não gerenciadas, views e catálogo. Chadha não escreve uma tabela em nenhum ponto. Julgue as duas escolhas.

R: Damji trata a tabela como o destino natural. Ele distingue managed de unmanaged pelo que o `DROP TABLE` apaga, mostra `saveAsTable()` como o par de `save()`, mostra `createOrReplaceTempView` e `createOrReplaceGlobalTempView`, e apresenta o `Catalog` com `listDatabases()`, `listTables()` e `listColumns()`. Ele também nomeia o metastore, diz que o padrão é o Apache Hive metastore em `/user/hive/warehouse`, e diz como mudá-lo com `spark.sql.warehouse.dir`.

Chadha escreve só arquivos, em caminhos relativos sob `../data/data_lake/`.

A escolha de Damji está certa e é o achado mais forte desta comparação. Ele é o livro genérico de Spark e cobre o caminho para tabela. Chadha é o livro de Databricks, plataforma cujo produto central é justamente o catálogo de tabelas governadas, e não menciona uma. A assimetria é ao contrário do esperado. Quem lê só o capítulo 1 do Chadha termina achando que "data lake" é uma pasta com arquivos, que é a definição que a plataforma inteira existe para superar.

Um detalhe que envelheceu do lado de Damji: o metastore padrão dele é o Hive, e hoje o padrão em Databricks é o Unity Catalog, com o Hive metastore rotulado como legacy feature. *Conferi no item 9 do Nível 5.*

**4.** **Schema: inferir ou declarar?** Compare o que cada autor recomenda e como cada um justifica.

R: Damji recomenda declarar e diz por quê, em uma frase da Tabela 4-1: "providing a schema for any format makes loading faster and ensures your data conforms to the expected schema". Ele dá dois motivos, velocidade e conformidade. Ele também mostra a forma curta, uma DDL-formatted string como `"date STRING, delay INT, distance INT, origin STRING, destination STRING"`, e diz onde o schema é dispensável, que é em Parquet estático, e onde ele é obrigatório, que é em source de streaming.

Chadha mostra o schema declarado três vezes, sempre como `StructType` completo, e justifica com "if we want to enforce data types". Ele nunca menciona velocidade. Ele nunca mostra a forma DDL. Ele nunca diz que Parquet dispensa schema, embora a receita dele de Parquet não passe schema nenhum.

Ironia dos dois lados. Damji recomenda declarar e depois usa `inferSchema` em quase todos os exemplos, inclusive no principal, com um comentário no código pedindo desculpa: "note that for larger files you may want to specify the schema". Chadha não recomenda nada e é o único dos dois que escreve um `StructType` completo mais de uma vez.

Para o meu uso, a DDL string de Damji é a melhor coisa desta comparação. Doze `StructField` em Chadha viram uma linha de texto.

**5.** **Aninhamento.** Damji lê um struct em uma linha, `imagesDF.select("image.height", "image.width", ...)`. Chadha tem uma receita inteira sobre dados aninhados. O que Chadha entrega que Damji não tem?

R: Chadha entrega o assunto inteiro, e Damji entrega uma linha por acidente.

Chadha tem `explode` e `explode_outer`, dot notation, `getItem` e `getField`, `flatten`, `collect_list`, `array_distinct`, `array_contains`, `map_keys`, `map_values` e uma seção de problemas com aninhamento. Ele também mostra explode encadeado em três níveis, sobre o dataset do SQuAD.

Damji não tem nenhuma dessas funções no capítulo 4. A única aparição de estrutura aninhada é o schema do data source de imagem, com um struct `image` de seis campos, e o único acesso é por dot notation dentro de um `select`. Ele nem comenta que aquilo é dot notation.

Esta é a vantagem mais clara de Chadha na comparação, e ela é grande. Dado real de API vem aninhado, e o capítulo de "built-in data sources" de Damji trata todo formato como se produzisse uma tabela plana. Nos exemplos dele isso é verdade, porque os voos e as somas por país são tabulares. É verdade dos exemplos, não do mundo.

**6.** **A escrita.** Compare os dois tratamentos de escrita: modos, formatos, compressão, particionamento.

R:

| Aspecto | Chadha | Damji |
|---|---|---|
| Modos | os quatro descritos em NOTE, sem o padrão | Tabela 4-2, com o padrão `error`/`errorifexists` |
| Formatos escritos | CSV, JSON, Parquet | Parquet, JSON, CSV, Avro, ORC |
| Compressão | `option("codec", "org.apache...GzipCodec")` | `option("compression", "snappy")` |
| Contagem de arquivos | `repartition(n)` e `coalesce(1)`, com exemplos | não trata |
| Particionamento por coluna | `partitionBy('release_year')`, com exemplo | `partitionBy(args)` na tabela, sem exemplo |
| Bucketing | ausente | `bucketBy` na tabela, sem exemplo |
| Escrita em tabela | ausente | `saveAsTable()`, com exemplo |
| O que sai no disco | não mostra | mostra o `ls`, com `_SUCCESS` e `part-00000-...` |

Chadha ganha em controle do layout de saída. Ele é o único que mostra como decidir quantos arquivos existem, e o único que demonstra `partitionBy`.

Damji ganha em dizer o que acontece. O `ls` que ele imprime, com `_SUCCESS` e `part-00000-<...>-c000.snappy.parquet`, responde a pergunta que todo iniciante faz na primeira escrita, que é por que o caminho que eu pedi virou uma pasta. Chadha escreve em `../data/data_lake/netflix_csv_data` seis vezes e nunca mostra o que aparece lá.

Sobre compressão, os dois estão corretos. `codec` é alias de `compression` e o nome de classe completo é aceito, conforme conferi no item 12 do Nível 5. A forma de Damji é a documentada.

**7.** **Streaming.** Os dois capítulos tocam em streaming de passagem. Compare o que cada um diz e quem se sai melhor.

R: Damji menciona `SparkSession.readStream` ao lado de `SparkSession.read`, e `DataFrame.writeStream` ao lado de `DataFrame.write`, apresentando o par como duas formas de obter o mesmo tipo de handle. Ele também dá a única informação operacional útil dos dois livros sobre o assunto, em duas NOTEs: "for streaming data sources you will have to provide a schema" e "Unless you are reading from a streaming data source there's no need to supply the schema". Ele adia o resto para o capítulo 8.

Chadha menciona `spark.readStream.csv()` uma vez, em "There's more", como remédio para arquivos CSV grandes, e afirma que isso permite processar o dado em tempo real conforme ele é lido do disco.

Damji se sai melhor por larga margem, e a diferença é qualitativa. Ele posiciona o streaming como uma simetria de API e declara a exigência de schema, que é a primeira coisa que quebra na prática. Chadha usa o streaming como resposta para um problema de batch, o que é a única sugestão do capítulo dele que pode piorar a situação de quem a seguir. *Conferi a exigência de schema e o comportamento da file source no item 3 do Nível 5, e Damji está certo.*

**8.** **O que cada livro deixa para o outro.** Feche a comparação.

R: **Damji deixa para Chadha** o dado aninhado inteiro, incluindo `explode`, HOFs e acesso por índice. Deixa o controle do layout de escrita, ou seja, quantos arquivos e particionado por qual coluna. Deixa o tratamento de dado ruim ligado a sintoma, com nome de opção por problema. Deixa o pipeline de texto e a ponte com a MLlib. E deixa a única menção a lazy evaluation dos dois capítulos, ainda que aplicada ao exemplo errado.

**Chadha deixa para Damji** as tabelas de referência com defaults, que é a informação que mais falta no cookbook. Deixa a camada de tabela inteira, com managed contra unmanaged, views temporárias e globais, metastore e catálogo. Deixa a interoperabilidade entre SQL e DataFrame API, mostrando a mesma query nas duas formas. Deixa o `ls` do diretório de saída. Deixa quatro formatos, Avro, ORC, imagem e binário. E deixa a justificativa para preferir Parquet, que Chadha enuncia como descrição e nunca como recomendação.

A divisão não é acidental. Damji cobre a superfície da API e a semântica de armazenamento. Chadha cobre a manipulação do dado depois que ele entrou e o layout depois que ele sai. Lidos juntos, eles cobrem o capítulo que nenhum dos dois escreveu.

### Discordâncias

**1.** **`escapeQuotes` como opção de leitura.** Chadha oferece `option("escapeQuotes", "true")` como solução para delimitador dentro do dado, na leitura. Damji não lista `escapeQuotes` em lugar nenhum, e sua Tabela 4-4 traz `escape`, read/write, como "the character to escape quotes", com default `\`. Quem está certo?

R: **Damji, por omissão, e Chadha está errado.**

A documentação do Spark 4.2.0 é inequívoca: `escapeQuotes` tem escopo **write only**, com default `true`, e controla se valores contendo aspas são sempre envolvidos em aspas na escrita. Passada em uma leitura, ela é ignorada em silêncio.

O que resolve o problema de Chadha na leitura é o comportamento padrão. A opção `quote` já vem como aspa dupla, e a documentação a descreve como o caractere "used for escaping quoted values where the separator can be part of the value". Ou seja, o Spark já trata delimitador dentro de valor citado desde sempre, e não é preciso opção nenhuma.

A discordância é assimétrica porque Damji não afirma nada sobre `escapeQuotes`, ele só não a inclui. Isso não é acerto por conhecimento, é acerto por não ter dito. Mas o `escape` que ele lista é a opção certa para o caso vizinho, que é uma aspa dentro de um valor já citado.

Registro o caso como o erro mais consequente do capítulo do Chadha, porque ele está em uma seção chamada "Common issues" e ensina uma não-solução para um problema real.

**2.** **O padrão do modo de escrita.** Chadha lista os quatro modos e nunca diz qual é o padrão. Damji declara em Tabela 4-2 que "The default mode options are `error` or `errorifexists` and `SaveMode.ErrorIfExists`; they throw an exception at runtime if the data already exists". Isso é discordância?

R: **Não é discordância, e vou dissolver o caso.**

Chadha não afirma um padrão diferente. Ele simplesmente não afirma padrão nenhum. Duas afirmações só entram em conflito se ambas existirem, e aqui existe uma.

O que sobra é uma diferença de completude com consequência prática séria, e ela merece registro por outro motivo. Todas as escritas de Chadha passam `mode("overwrite")` explicitamente, então o código dele funciona. O problema aparece quando o leitor tira a linha ou escreve a própria. Sem saber que o padrão é falhar, ele vai interpretar o primeiro erro de escrita como bug.

Damji também acrescenta uma informação que Chadha não tem: existem duas superfícies para o mesmo conceito, as strings como `"overwrite"` e as constantes `SaveMode.Overwrite`. Chadha só mostra as strings.

Veredito: não há quem arbitrar. Damji é mais completo, Chadha não erra.

**3.** **`overwrite` derruba índices e constraints.** Chadha afirma isso na NOTE de modos de escrita. Damji descreve os mesmos modos e diz apenas que os padrões lançam exceção se o dado já existir. Arbitre.

R: **Chadha está errado, e Damji está certo por não ter inventado nada.**

Nenhum dos formatos que os dois capítulos escrevem tem índice ou constraint. Parquet, CSV, JSON, Avro e ORC são arquivos em diretório. `overwrite` neles substitui os arquivos do caminho.

Damji dá a peça que faltava para julgar: ele mostra o que existe em um diretório de saída. Um `_SUCCESS`, um `_committed_...`, um `_started_...` e arquivos `part-XXXX`. Nenhum índice, nenhuma constraint, nenhum catálogo dentro do diretório.

Damji também é quem mostra o único caso em que a frase de Chadha poderia ter algum sentido, que é `saveAsTable()` sobre uma tabela gerenciada. Mesmo ali, o que existe é metadado no metastore, e não índice nem constraint no sentido relacional.

Origem provável: descrição de modo de escrita copiada de documentação de um conector JDBC ou de uma ferramenta de ETL relacional. Dá para conferir que a frase não pertence ao contexto porque as outras três descrições da mesma NOTE são corretas e neutras.

**4.** **A conformidade com ANSI SQL:2003.** Damji abre o capítulo 4 afirmando que Spark SQL "Supports ANSI SQL:2003-compliant commands and HiveQL", e repete "ANSI:2003–compliant SQL interface" mais adiante. Chadha não faz nenhuma afirmação de conformidade. Há disputa aqui?

R: **Não há disputa, e a acusação que eu ia fazer contra Damji também cai.**

Chadha não fala em SQL no capítulo. Ele nunca escreve `spark.sql`, nunca cria view e nunca cria tabela. Então não existe afirmação dele para confrontar com a de Damji.

Contra Damji eu tinha montado o seguinte: conformidade seria modo de operação, governada por `spark.sql.ansi.enabled`, que em 2020 vinha desligada. Fui conferir e a acusação confunde duas coisas. "Supports ANSI SQL:2003-compliant commands" é afirmação sobre **quais comandos o parser aceita**, ou seja, cobertura de gramática. O `spark.sql.ansi.enabled` governa **semântica de runtime**: o que acontece quando um cast falha ou uma soma estoura. São eixos ortogonais. O próprio SPARK-44444 se chama "Use ANSI SQL mode by default" e não diz nada sobre gramática de comandos. A página ANSI Compliance da 4.2.0 referencia SQL-2016, não SQL:2003, e avisa que parte do dialeto não vem do padrão.

O que sobra contra Damji é menor e de outra natureza. A frase dele ecoa o anúncio do Spark 2.0, que citava o novo parser ANSI SQL e as 99 queries do TPC-DS. Ela não é falsa. Ela é uma alegação de cobertura que ninguém consegue falsificar, porque o capítulo não diz qual subconjunto do padrão está coberto. Imprecisa, não errada.

Sobre o flag, que é assunto separado: ele tem default `true` desde o Spark 4.0.0, via SPARK-44444, e continua `true` na 4.2.0. Na prática, `CAST('a' AS INT)` devolvia null e hoje lança `SparkNumberFormatException`. `SELECT 2147483647 + 1` devolvia `-2147483648` e hoje lança `[ARITHMETIC_OVERFLOW]`.

Dissolvo esta discordância inteira. Achei que tivesse um erro conferível de um lado e não tenho.

**5.** **Precisa de schema para ler Parquet?** Damji afirma duas vezes que não: "In general, no schema is needed when reading from a static Parquet data source" e "Unless you are reading from a streaming data source there's no need to supply the schema, because Parquet saves it as part of its metadata". Chadha lê Parquet sem schema e, na mesma receita, mostra que o schema pode mudar conforme os arquivos lidos. Conflito?

R: **Não é conflito de fato, e o cruzamento das duas afirmações produz a informação que nenhum dos dois dá sozinho.**

Damji fala de **um** data source Parquet consistente. Nesse caso o rodapé traz o schema e nada precisa ser declarado. Está correto.

Chadha fala de um diretório com **schemas divergentes entre partições**. Ele demonstra que ler tudo devolve `ReviewCount` sem `Images`, ler `2020-01*` devolve `Images` sem `ReviewCount`, e ligar `mergeSchema` devolve as duas. Também está correto.

Juntando: o schema do Parquet é uma propriedade do arquivo, não do dataset. Enquanto todos os arquivos concordam, a afirmação de Damji vale e o schema é dispensável. Quando eles divergem, "o schema" deixa de existir como coisa única, e a leitura passa a depender de quais rodapés o Spark abriu.

Damji nunca menciona schema merging, evolução de schema, nem `spark.sql.parquet.mergeSchema`. Essa é a lacuna real, e ela é do lado dele. Chadha demonstra o fenômeno com um caso concreto e não o generaliza, então também não fecha o raciocínio.

A regra que eu levo: em Parquet homogêneo, não declaro schema. Em diretório particionado que evoluiu ao longo do tempo, declaro schema explícito em produção, e uso `mergeSchema` só na exploração. Nenhum dos dois capítulos diz isso. É a síntese das duas metades.

---

## Termos-chave para definir antes de seguir adiante

Escreva uma definição de uma linha para cada, com suas próprias palavras, sem consultar:

data source · `DataFrameReader` · `DataFrameWriter` · schema · `StructType` · `StructField` · nullable · schema inference · schema merging · schema evolution · lazy evaluation · transformation · action · job · partition · repartition · coalesce · shuffle · `partitionBy` · bucketing · save mode · columnar format · Parquet · row group · splittability · multiline mode · corrupt record · parse mode · `PERMISSIVE` · `DROPMALFORMED` · `FAILFAST` · codec · `explode` · `explode_outer` · dot notation · `getItem` · `getField` · higher-order function · `array_distinct` · JSON path expression · tokenization · stop word · bag-of-words · `CountVectorizer` · transformer · estimator · MLlib · NLP · data lake · managed table · `SparkSession` · `SparkContext` · standalone cluster manager

### Minhas definições

Trinta e dois dos cinquenta e três termos o capítulo usa sem definir, ou nem menciona. Marquei esses casos em itálico. O padrão de um cookbook aparece inteiro aqui: ele nomeia opções e funções com precisão e não define quase nenhum conceito por trás delas.

**data source** — *Usado o capítulo inteiro sem definição.* O conector que o Spark usa para ler ou escrever um formato ou sistema, selecionado pela string passada a `format()`.

**`DataFrameReader`** — *Nunca nomeado.* O capítulo usa `spark.read` sete vezes e nunca diz o nome do objeto que isso devolve. É a construção que recebe `format`, `option`, `schema` e `load`.

**`DataFrameWriter`** — Nomeado uma vez, na receita de escrita, como o objeto que o método `write` devolve e que fornece métodos para escrever em vários formatos. O capítulo também o chama de classe ao apresentar `partitionBy`.

**schema** — *Usado sem definição.* A lista de colunas de um DataFrame, com nome, tipo e nullability de cada uma.

**`StructType`** — A classe que representa um schema, construída com uma lista de `StructField`. O capítulo a usa três vezes e nunca a define.

**`StructField`** — A classe que representa uma coluna dentro de um `StructType`. Recebe nome, tipo e um terceiro argumento. O capítulo escreve `True` nesse terceiro argumento em todos os campos dos três schemas e nunca diz o que ele significa.

**nullable** — *Não aparece no capítulo.* O terceiro argumento de `StructField`, que declara se a coluna aceita null.

**schema inference** — *Nomeada só como a opção `inferSchema`, em uma linha de "There's more".* O processo em que o Spark determina os tipos das colunas lendo o dado, o que exige uma passagem extra sobre o arquivo.

**schema merging** — A operação em que o Spark lê o schema de todos os arquivos de um diretório e monta um schema unificado. Controlada por `spark.sql.parquet.mergeSchema` ou pela opção `mergeSchema`, com default `false`.

**schema evolution** — *Nomeada uma vez, na NOTE de schema merging, sem definição.* A mudança do schema de um dataset ao longo do tempo, com colunas entrando ou saindo entre gravações.

**lazy evaluation** — A técnica em que o Spark adia a execução das transformations até que uma action seja chamada, o que permite otimizar o plano e recuperar-se de falhas. É o único conceito de arquitetura que o capítulo tenta explicar, e ele o ilustra com um exemplo em que a afirmação não vale.

**transformation** — *Nomeada uma vez, na NOTE de lazy evaluation, sem definição.* A operação que descreve um novo DataFrame a partir de outro, sem executar nada na hora.

**action** — *Nomeada uma vez, na NOTE de lazy evaluation, sem definição.* A operação que dispara a execução e produz um resultado, como `show()` ou uma escrita.

**job** — *Usado uma vez, na NOTE de lazy evaluation, sem definição.* A unidade de execução que uma action dispara.

**partition** — A fatia do dado em que o Spark divide o dataset para processar em paralelo. O capítulo usa a palavra em três sentidos diferentes, e o item 10 do Nível 4 os separa.

**repartition** — O método que redefine o número de partitions de um DataFrame. O capítulo o usa para controlar quantos arquivos a escrita produz e não menciona que ele provoca um shuffle completo.

**coalesce** — O método que reduz o número de partitions. O capítulo o apresenta como remédio para escrita lenta e não diz que ele evita shuffle nem que ele reduz o paralelismo dos estágios anteriores.

**shuffle** — *Nomeado uma vez, dentro da definição de partitioning, sem explicação.* A redistribuição de dados entre máquinas quando o processamento exige reagrupar as linhas.

**`partitionBy`** — O método do `DataFrameWriter` que cria um subdiretório por valor distinto de uma coluna, no formato `coluna=valor`. O capítulo mostra o uso e não mostra o resultado no disco.

**bucketing** — *Não aparece no capítulo.* O agrupamento do dado em um número fixo de arquivos por hash de coluna, alternativa ao particionamento por valor.

**save mode** — O parâmetro que controla o que acontece quando o destino já existe. O capítulo lista os quatro valores e não diz qual é o padrão. *O padrão é falhar, conforme a tabela do Damji.*

**columnar format** — *Usado na descrição do Parquet, sem definição.* O layout em que os valores de uma coluna ficam juntos no arquivo, em vez de os valores de uma linha.

**Parquet** — Formato de armazenamento columnar para grandes datasets, otimizado para compressão eficiente e encoding de tipos complexos. Carrega o próprio schema, o que dispensa declará-lo na leitura.

**row group** — *Não aparece no capítulo.* O bloco de linhas que é a unidade de leitura e de estatística dentro de um arquivo Parquet.

**splittability** — *Não aparece no capítulo.* A propriedade de um arquivo poder ser dividido em blocos lidos por tasks diferentes. É o que `multiLine` custa, e o capítulo liga `multiLine` em três receitas sem mencionar isso.

**multiline mode** — *Usado como a opção `multiLine` em toda leitura de JSON, sem definição e sem o default.* O modo em que um registro pode atravessar várias linhas do arquivo, o que força o Spark a tratar o arquivo como unidade.

**corrupt record** — O registro que não faz parse. Em modo `PERMISSIVE`, o Spark anula os campos ruins e guarda o texto original na coluna nomeada por `columnNameOfCorruptRecord`. *O capítulo omite que essa coluna precisa estar declarada no schema.*

**parse mode** — *Usado como a opção `mode` e nunca nomeado como conceito.* A política que decide o que fazer com um registro que não faz parse.

**`PERMISSIVE`** — O modo em que o parse continua, com campos ruins em null e o registro original preservado. O capítulo o menciona três vezes e nunca diz que é o padrão.

**`DROPMALFORMED`** — *Não aparece no capítulo.* O modo em que registros corrompidos são descartados em silêncio.

**`FAILFAST`** — *Não aparece no capítulo.* O modo em que o primeiro registro corrompido lança exceção e para a leitura.

**codec** — O algoritmo de compressão usado na escrita. O capítulo o passa como nome de classe Hadoop completo. *A forma documentada é `option("compression", "gzip")`, e `codec` é um alias aceito.*

**`explode`** — A função que cria uma nova linha para cada elemento de um array ou de um map. É a operação central de três receitas do capítulo.

**`explode_outer`** — A variante que devolve uma linha mesmo quando o array ou o map é null, caso em que a `explode` não devolve nada.

**dot notation** — *Usada como técnica, definida só por exemplo.* O acesso a um campo aninhado por nome, com ponto, como em `col("laureates.id")`.

**`getItem`** — O método que extrai um único elemento de um tipo complexo por índice ou por chave. É o que dot notation não faz, porque array se acessa por posição.

**`getField`** — *Nomeado uma vez, sem definição.* O método que extrai um campo nomeado do struct resultante de um `getItem`.

**higher-order function** — *Nomeada como sigla HOF, sem definição.* No capítulo, apresentada como função que manipula colunas aninhadas no lugar. O exemplo dado, `array_distinct`, não recebe função como argumento e portanto não é uma HOF de fato.

**`array_distinct`** — A função que remove elementos duplicados de uma coluna de array, sem alterar a cardinalidade das linhas.

**JSON path expression** — *Nomeada sem definição.* A sintaxe de caminho usada por `get_json_object`, como `"$.name"`, para localizar um valor dentro de uma string JSON.

**tokenization** — O processo de quebrar dado textual em unidades menores, como palavras ou frases. O capítulo mostra duas implementações, `split()` e o `Tokenizer` da MLlib, e não compara as duas.

**stop word** — Palavra comum que não carrega muito significado, como "the", "and" e "in". Removida pelo `StopWordsRemover`, que tem uma lista embutida e aceita lista customizada.

**bag-of-words** — *Nomeada como sigla BoW, sem definição.* A representação em que um texto vira uma contagem de ocorrências por termo, sem ordem. É o que o `CountVectorizer` produz.

**`CountVectorizer`** — O componente da MLlib que converte uma coluna de array de palavras em um vetor numérico de contagens.

**transformer** — *Nomeado uma vez, para descrever o `StopWordsRemover`, sem definição.* O componente que converte um DataFrame em outro pelo método `transform`, sem aprender nada com o dado.

**estimator** — *Não aparece no capítulo.* O componente que aprende com o dado pelo método `fit` e produz um transformer. O `CountVectorizer` do capítulo é um, e o `.fit(...)` do código é a prova, sem que o texto nomeie o conceito.

**MLlib** — A biblioteca escalável de machine learning do Spark, com APIs em Java, Scala, Python e R. Fornece classification, regression, clustering, collaborative filtering, feature extraction e pipelines.

**NLP** — *Sigla expandida na abertura, conceito nunca tratado.* Natural language processing. O capítulo promete análise de texto com NLP e entrega pré-processamento: limpeza, tokenização, remoção de stop words e vetorização.

**data lake** — *Não aparece como conceito, só como nome de pasta.* Os destinos de escrita do capítulo são todos `../data/data_lake/...`, e o termo nunca é definido nem problematizado.

**managed table** — *Não aparece no capítulo.* A tabela cujos metadados e dados o Spark gerencia, de modo que um `DROP TABLE` apaga os dois. *Esta definição vem do capítulo 4 do Damji, não deste.*

**`SparkSession`** — O ponto de entrada unificado para aplicações Spark, com acesso simplificado a RDDs, DataFrames, datasets, queries SQL e streaming. Construída pelo método builder, que configura nome da aplicação, master URL e outras opções.

**`SparkContext`** — O ponto de entrada para qualquer funcionalidade do Spark. Representa a conexão com um cluster e é responsável por coordenar e distribuir as operações nesse cluster. O capítulo diz que vai defini-lo e depois só o usa para baixar o nível de log.

**standalone cluster manager** — *Nunca nomeado.* É o que `spark://spark-master:7077` endereça nas sete receitas. O capítulo escreve a URL sete vezes e nunca diz o que ela é nem por que a porta é 7077.
