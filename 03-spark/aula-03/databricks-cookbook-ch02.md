# Guia de Leitura — *Data Engineering with Databricks Cookbook*, Capítulo 2: Data Transformation and Data Manipulation with Apache Spark

**Fonte:** Pulkit Chadha, *Data Engineering with Databricks Cookbook: Build effective data and AI solutions using Apache Spark, Databricks, and Delta Lake*, 1ª ed. (Packt Publishing, 31 de maio de 2024), 438 páginas, ISBN-13 impresso 9781837633357, ISBN-13 do ebook 9781837632060. Capítulo 2, 32 páginas.

**Escopo:** os Níveis 1 a 4 são respondíveis apenas com este capítulo. O Nível 5 exige conferência em fonte externa. O Nível 6 exige o capítulo 4 do *Beginning Apache Spark 3*, de Hien Luu, já lido.

**Sobre as figuras:** este capítulo tem três, e eu abri as três. Rodei `pdfimages -list` nas 32 páginas e achei quatro imagens, nas páginas 8, 17, 23 e 29. As três primeiras são as Figuras 2.1, 2.2 e 2.3, todas prints de saída de `show()` no console. A quarta é a barra de ferramentas do leitor da O'Reilly, igual à do capítulo 1, e não é conteúdo do livro. As três figuras reais são o material mais revelador do capítulo, e o Nível 4 é quase todo construído sobre elas.

**Aviso sobre os listings:** o PDF corta as linhas de código longas na margem direita. Conferi isso abrindo as páginas 13 e 23 renderizadas. Toda vez que eu completo uma linha cortada, eu digo na resposta que ela estava cortada e marco o que eu completei.

---

## Como usar este guia

1. Leia o capítulo uma vez de ponta a ponta, sem parar. São sete receitas com a mesma forma do capítulo 1, e o padrão de repetição só fica visível na leitura corrida.
2. **Rode o código. Este item vale mais que todos os outros juntos, e vale mais aqui do que no capítulo 1.** No capítulo 1 os defeitos eram de sintaxe e paravam a execução. Aqui vários trechos rodam e devolvem resultado errado em silêncio. As Figuras 2.2 e 2.3 são prova impressa disso. Uma receita que você não executou é texto, não conhecimento.
3. Faça o Nível 1 de memória primeiro, depois volte ao texto para preencher as lacunas. Marque toda questão que você não conseguiu responder.
4. Faça os Níveis 2 e 3 por escrito, em frases completas. O Nível 3 pressupõe que você já executou o Nível 2 no ambiente.
5. O Nível 4 é onde o capítulo se torna útil, e ele depende de você ter olhado as três figuras. O Nível 5 vai para o backlog de estudo. O Nível 6 vai para uma nota de comparação que fica acima dos dois livros.

---

## Nível 1 — Memorização e definições

Respostas curtas e conferíveis. Uma ou duas frases cada.

**1.** Quais são as sete receitas do capítulo, na ordem? *(Chapter intro)*

R: Applying basic transformations to data with Apache Spark. Filtering data with Apache Spark. Performing joins with Apache Spark. Performing aggregations with Apache Spark. Using window functions with Apache Spark. Writing custom UDFs in Apache Spark. Handling null values with Apache Spark.

**2.** O que o parágrafo de abertura promete que você saberá fazer ao fim do capítulo? *(Chapter intro)*

R: Aplicar transformações básicas, filtrar dados e usar window functions para cálculos baseados em tempo ou em rank. Além disso, escrever user-defined functions (UDFs) para aplicar lógica própria ao dado.

**3.** Quais são os três requisitos técnicos declarados antes de começar, e onde ficam os notebooks? *(Technical requirements)*

R: Ter as imagens do `docker-compose` no ar, abrir o JupyterLab em `http://127.0.0.1:8888/lab` e ter clonado o repositório Git do livro. Os notebooks ficam em `https://github.com/PacktPublishing/Data-Engineering-with-Databricks-Cookbook/tree/main/Chapter02`. O comando de encerramento é `docker-compose stop`.

**4.** Quais seções de receita Packt o capítulo usa, e quantas vezes cada uma? *(estrutura do capítulo)*

R: Contei com `grep -c`, usando o apóstrofo tipográfico. "How to do it" aparece 7 vezes, uma por receita. "There's more" aparece 7 vezes, também uma por receita. "See also" aparece 6 vezes. "How it works" aparece zero vez. "Getting ready" aparece zero vez. As NOTE somam 6.

**5.** Qual receita não tem "See also", e para onde ela vai depois de "There's more"? *(estrutura do capítulo)*

R: A receita de window functions. Depois da subseção "Window frames" o texto emenda direto na receita seguinte, "Writing custom UDFs in Apache Spark". É a única das sete sem "See also".

**6.** Como as seis NOTE se distribuem pelas receitas? *(estrutura do capítulo)*

R: Duas na receita de basic transformations, sobre `transform` e sobre `orderBy`. Três na receita de aggregations, sobre `groupBy`, sobre `pivot` e sobre aproximação. Uma na receita de window functions, sobre `row_number`. As receitas de filtering, joins, UDFs e nulls não têm nenhuma.

**7.** Reproduza o builder de `SparkSession` da primeira receita. O que muda entre as sete receitas? *(Applying basic transformations, How to do it, step 1)*

R:

```python
spark = (SparkSession.builder
      .appName("basic-transformations")
      .master("spark://spark-master:7077")
      .config("spark.executor.memory", "512m")
      .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
```

Só o `appName` muda. Os sete valores são `basic-transformations`, `filter-data`, `perform-joins`, `perform-aagregations`, `apply-window-functions`, `write-udfs` e `handle-nulls`. O quarto está escrito com dois "a".

**8.** Quais funções a primeira receita importa de `pyspark.sql.functions`? *(Applying basic transformations, How to do it, step 1)*

R: `transform`, `col`, `concat` e `lit`.

**9.** Qual arquivo a primeira receita lê, e com qual opção? *(Applying basic transformations, How to do it, step 2)*

R: `../data/nobel_prizes.json`, com `format("json")` e `option("multiLine", "true")`. É o mesmo dataset da receita de JSON do capítulo 1.

**10.** O que a função `transform` faz, segundo o capítulo, e quais dois argumentos ela recebe? *(Applying basic transformations, How to do it, step 3)*

R: Ela se aplica a colunas de array e roda uma função definida pelo usuário em cada elemento do array, devolvendo uma nova coluna de array. Os dois argumentos são a coluna de array a transformar e a função a aplicar em cada elemento.

**11.** Escreva a chamada de `transform` da receita e diga qual coluna ela produz. *(Applying basic transformations, How to do it, step 3)*

R:

```python
transform(col("laureates"),
      lambda x: concat(x.firstname, lit(" "), x.surname))
.alias("laureates_full_name")
```

Ela produz `laureates_full_name`, um array com o nome completo de cada laureado, dentro do mesmo `select` que projeta `category`, `overallMotivation`, `year` e `laureates`.

**12.** Quais duas coisas a NOTE sobre `transform` afirma? *(Applying basic transformations, NOTE)*

R: Que a função passada a `transform` precisa receber um único argumento, do mesmo tipo dos elementos do array. E que, havendo várias colunas de array no DataFrame, dá para usar `arrays_zip` para combiná-las em uma coluna de array única antes de aplicar `transform`.

**13.** O que faz `dropDuplicates`, e sobre quais colunas o exemplo a chama? *(Applying basic transformations, How to do it, step 4)*

R: Remove duplicatas de um DataFrame com base em uma ou mais colunas. O exemplo chama `df.dropDuplicates(["category","overallMotivation","year"])`.

**14.** Quais duas funções de ordenação o capítulo apresenta, e como se ordena por várias colunas? *(Applying basic transformations, How to do it, step 5)*

R: `orderBy` e `sort`. Para várias colunas passa-se uma lista de nomes mais uma lista de booleanos em `ascending`, como em `df.orderBy(["year", "category"], ascending=[False, True])`. O capítulo diz que as duas funções alcançam o mesmo resultado.

**15.** O que a NOTE sobre ordenação afirma? *(Applying basic transformations, NOTE)*

R: Que tanto `orderBy` quanto `sort` devolvem um DataFrame novo e ordenado, e que o DataFrame original fica inalterado.

**16.** Quais dois métodos de renomear coluna a receita mostra? *(Applying basic transformations, How to do it, steps 6 e 7)*

R: `withColumnRenamed`, que recebe o nome atual e o nome novo, no exemplo `df.withColumnRenamed("category", "Topic")`. E `selectExpr`, no exemplo com três expressões, `"category as Topic"`, `"year as Year"` e `"overallMotivation as Motivation"`.

**17.** Qual saída o capítulo imprime depois do `selectExpr`, e o que chama atenção nela? *(Applying basic transformations, How to do it, step 7)*

R: Uma tabela de cinco linhas com as colunas `Topic`, `Year` e `Motivation`. Os tópicos são chemistry, economics, literature, peace e physics, todos com `Year` igual a 2022. A coluna `Motivation` é `null` nas cinco linhas.

**18.** Como a primeira receita termina, e qual é o problema com essa linha? *(Applying basic transformations, How to do it, step 8)*

R: Com `Spark.stop()`, escrito com S maiúsculo. É a única das sete receitas em que a linha final está capitalizada. Em Python isso é `NameError`, porque a variável se chama `spark`.

**19.** Quais opções a receita de filtering usa para ler `netflix_titles.csv`? *(Filtering data, How to do it, step 2)*

R: `header` em `"true"`, `nullValue` em `"null"` e `dateFormat` em `"LLLL d, y"`. Não há `inferSchema` e não há `schema`. As três receitas que leem esse arquivo usam exatamente estas opções.

**20.** Como o capítulo filtra por uma condição simples, e como combina várias? *(Filtering data, How to do it, steps 3 e 4)*

R: Simples com `df.filter(col("release_year") > 2020)`. Várias com os operadores `&` para "e" e `|` para "ou", como em `(col("country") == "United States") & (col("release_year") > 2020)`. Cada condição precisa de parênteses próprios.

**21.** Como se filtra por uma lista de valores, e qual exemplo o capítulo dá? *(Filtering data, How to do it, step 5)*

R: Com o método `isin`. O exemplo é `col("country").isin(["United States", "United Kingdom", "India"])`. A saída dele é a Figura 2.1.

**22.** Quais são as quatro subseções de "There's more" na receita de filtering? *(Filtering data, There's more)*

R: Filtering on string, Filtering on data ranges, Filtering on arrays e Filtering on map columns.

**23.** Qual é a diferença entre `like` e `rlike`, e quais exemplos o capítulo dá? *(Filtering data, There's more, Filtering on string)*

R: A `like` filtra por casamento de padrão com caracteres curinga, e a `rlike` filtra por casamento de expressão regular. Os exemplos são `col("listed_in").like("%Crime%")` e `col("listed_in").rlike("(Crime|Thrillers)")`. O texto explica o `%` como curinga e depois se refere a "the app substring", palavra que não aparece em nenhum dos dois exemplos.

**24.** Quais duas formas de filtrar intervalo o capítulo mostra? *(Filtering data, There's more, Filtering on data ranges)*

R: Operadores de comparação, `>`, `>=`, `<` e `<=`, como em `(col("date_added") >= "2021-02-01") & (col("date_added") <= "2021-03-01")`. E o método `between`, como em `col("date_added").between("2021-02-01","2021-03-01")`. As duas linhas de código estão cortadas na margem direita do PDF, e eu completei o fechamento pela linha gêmea seguinte.

**25.** Como o capítulo filtra o que ele chama de coluna de map, e qual é a condição do exemplo? *(Filtering data, There's more, Filtering on map columns)*

R: Com o método `getItem` para acessar chaves. O exemplo explode `laureates` e depois filtra com `(col("laureates").getItem("firstname") == "Albert") & (col("laureates").getItem("surname") == "Einstein")`. O capítulo diz que o resultado tem uma linha só.

**26.** Quais quatro arquivos a receita de joins carrega, e de qual pasta? *(Performing joins, How to do it, step 2)*

R: `CardBase.csv`, `CustomerBase.csv`, `TransactionBase.csv` e `FraudBase.csv`, todos de `../data/Credit Card/`. Os quatro são lidos com `header` em `"true"` e `nullValue` em `"null"`.

**27.** Escreva o inner join do passo 3 e diga qual defeito ele tem. *(Performing joins, How to do it, step 3)*

R: O texto impresso é `cards_df.join(customers_df.` seguido de `on='Cust_ID',` e `how='inner'))`. O separador depois de `customers_df` é um ponto, não uma vírgula. Isso torna a linha um erro de sintaxe. A intenção é `cards_df.join(customers_df, on='Cust_ID', how='inner')`.

**28.** Qual justificativa o capítulo dá para escolher inner join no passo 3 e left outer join no passo 4? *(Performing joins, How to do it, steps 3 e 4)*

R: Para o inner join, o argumento é que todo cartão está associado a um cliente. Para o left outer join, o argumento é que nem toda transação é fraude, então as transações precisam sobreviver ao join com `fraud_df`.

**29.** O que o passo 5 constrói, e quais duas condições entram na expressão? *(Performing joins, How to do it, step 5)*

R: Uma variável `joinExpr` passada em `on=` no lugar de um nome de coluna. As duas condições são a igualdade entre `Card_Number` de um lado e `Credit_Card_ID` do outro, e `isNotNull()` sobre `Fraud_Flag`. A primeira linha está cortada na margem do PDF, e eu completei o nome `Credit_Card_ID` pela prosa da mesma página.

**30.** Qual erro de nome de variável o passo 5 comete? *(Performing joins, How to do it, step 5)*

R: Ele chama `Customer_cards_df.join(...)`, com C maiúsculo. O DataFrame criado no passo 3 se chama `customer_cards_df`, com c minúsculo. A prosa do passo 5 também escreve `Customer_cards_df`, então prosa e código concordam entre si e discordam do passo 3.

**31.** Quais tipos de join a seção "There's more" acrescenta, e qual string cada um usa? *(Performing joins, There's more)*

R:

| Tipo | Como o capítulo o chama |
|---|---|
| Right outer join | `how='right_outer'` |
| Full outer join | `how='outer'` |
| Cross join | método `crossJoin()`, sem string |
| Broadcast join | `df1.join(broadcast(df2), ["Name", "Gender"], "inner")` |
| Multiple join conditions | `on=['Name', 'Gender', 'Age']` |

**32.** Como o capítulo justifica o broadcast join? *(Performing joins, There's more, Broadcast join)*

R: Ele diz que juntar datasets grandes pode exigir que o Spark faça shuffle do dado pela rede, o que é lento e caro. Se um dos datasets couber em memória, o broadcast join o envia a todos os worker nodes, e o join passa a ser feito localmente, sem shuffle.

**33.** Para qual versão da documentação aponta o "See also" da receita de joins? *(Performing joins, See also)*

R: Para a 3.1.2, no link `https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.DataFrame.join.html`. É o único "See also" da receita, e é o único link do capítulo rotulado como documentação do Spark sem dizer a versão no rótulo.

**34.** Como o capítulo define a operação `groupBy`? *(Performing aggregations, How to do it, step 3)*

R: Como a operação que agrupa o dado de uma coleção distribuída com base em uma ou mais chaves, permitindo agregações ou transformações sobre o dado agrupado. As chaves podem ser nomes de coluna do DataFrame.

**35.** O que a NOTE sobre `groupBy` afirma, e qual é a informação mais importante dela? *(Performing aggregations, NOTE)*

R: Que `groupBy` é uma transformation, avaliada de forma lazy, que monta um plano lógico e só executa quando uma action é disparada, como `collect`, `show` ou `write`. A informação mais importante é a segunda: `groupBy` devolve um objeto `GroupedData`, e não um DataFrame.

**36.** Quais duas formas de agregar o capítulo mostra depois do `groupBy`? *(Performing aggregations, How to do it, step 4)*

R: Métodos diretos sobre o grupo, como `grouped_df.count()`. E o método `agg`, como em `grouped_df.agg(max(col("date_added")))`. O capítulo lista `count`, `sum`, `avg`, `min` e `max` como funções aplicáveis.

**37.** Escreva a agregação múltipla do passo 5. *(Performing aggregations, How to do it, step 5)*

R:

```python
release_date_gouped_df = (
      df.groupBy("country")
      .agg(
            count("show_id").alias("NumberOfReleases"),
            max("date_added").alias("LastReleaseDate"),
            min("date_added").alias("FirstReleaseDate")))
release_date_gouped_df.show(3)
```

O nome da variável está escrito `gouped`, sem o "r".

**38.** O que a Figura 2.2 mostra, linha por linha? *(Performing aggregations, How to do it, step 5)*

R: Abri a página 17. A tabela tem quatro colunas e três linhas. A primeira linha tem `country` igual a `null`, `NumberOfReleases` igual a 831, `LastReleaseDate` igual a "September 9, 2021" e `FirstReleaseDate` igual a "December 14, 2018". A segunda tem `country` igual a "Ama K. Abebrese", 1 release, e as duas datas iguais a "Kobina Amissah Sam". A terceira tem `country` igual a "Aziz Ansari", 1 release, e as duas datas iguais a "Carla Gallo".

**39.** Como o capítulo define pivot table, e quantos parâmetros ele atribui à função `pivot`? *(Performing aggregations, There's more, Pivot tables)*

R: Como uma forma de resumir o dado agrupando-o por várias dimensões, convertendo os valores de uma coluna em várias colunas. Ele afirma que `pivot` recebe três parâmetros: a coluna de pivot, a coluna de valores e a lista opcional de valores distintos que viram as novas colunas.

**40.** Escreva o exemplo de pivot do capítulo e diga o que ele produz. *(Performing aggregations, There's more, Pivot tables)*

R: `df.groupBy("country").pivot("type").agg(count("show_id"))`. Ele produz uma linha por valor distinto de `country`, com uma coluna por valor distinto de `type`, e a contagem como valor. O capítulo diz que valores ausentes viram `null`.

**41.** Quais duas ressalvas a NOTE sobre pivot levanta? *(Performing aggregations, NOTE)*

R: Que a operação de pivot exige shuffle e por isso pode ser cara, sobretudo em datasets grandes. E que o número de valores distintos na coluna de pivot deve ser razoavelmente pequeno, porque o resultado ganha uma coluna por valor distinto.

**42.** Quais três motivos o capítulo dá para usar funções aproximadas? *(Performing aggregations, There's more, Approximate aggregations)*

R: Performance optimization, ganho de tempo de execução ao sacrificar um pouco de exatidão. Scalability, capacidade de estimar sobre um subconjunto quando o dataset não cabe em memória. E trade-off between accuracy and cost, controle explícito do erro aceito por meio da relative standard deviation (RSD).

**43.** Quais duas funções aproximadas o capítulo nomeia, e o que ele diz que cada uma recebe? *(Performing aggregations, There's more, Approximate aggregations)*

R: `approxQuantile()`, que segundo o texto recebe o nome da coluna, a lista de quantis desejados e o valor de RSD. E `approxCountDistinct()`, que segundo o texto recebe a coluna e o valor de RSD. O código usa `review_df.approxQuantile("Score", [0.25, 0.5, 0.75], 0.1)` e `approx_count_distinct("ProductId", rsd=0.1)`.

**44.** O que é uma window specification, segundo o capítulo, e qual é a do exemplo? *(Using window functions, How to do it, step 3)*

R: É a especificação de como particionar e ordenar o dado antes de aplicar qualquer window function. A do exemplo é `Window.partitionBy("country").orderBy("date_added")`, importada de `pyspark.sql.window`.

**45.** O que faz `row_number`, e quais três etapas o capítulo descreve para ela? *(Using window functions, How to do it, step 4)*

R: Atribui um inteiro sequencial único a cada linha dentro de uma partition. As três etapas são partitioning, ordering e row numbering. No partitioning o dado é dividido e cada partition é processada de forma independente. No ordering as linhas de cada partition são ordenadas pelo `ORDER BY`. No row numbering a primeira linha recebe 1, a segunda recebe 2, e assim por diante.

**46.** O que a NOTE sobre `row_number` afirma, e o que ela recomenda? *(Using window functions, NOTE)*

R: Que `row_number` é uma função não determinística, e que a ordem das linhas dentro de uma partition pode variar entre execuções da mesma query. A recomendação é usar `rank` ou `dense_rank` no lugar quando se precisa de ordem estável.

**47.** O que fazem `lead` e `lag`, e quantos argumentos o capítulo atribui a cada uma? *(Using window functions, How to do it, step 5)*

R: `lead` pega o valor de uma coluna na próxima linha da partition, e `lag` pega o da linha anterior. O capítulo diz que cada uma recebe dois argumentos, a coluna a recuperar e o número de linhas a avançar ou retroceder. Se o deslocamento passar da fronteira da partition, o retorno é `null`.

**48.** O que a Figura 2.3 mostra? *(Using window functions, How to do it, step 5)*

R: Abri a página 23. A tabela tem cinco colunas e três linhas. A primeira é `title` igual a "Beasts of No Nation", `country` igual a "Ama K. Abebrese" e `date_added` igual a "Kobina Amissah Sam". A segunda é "Get Him to the Greek", "Aziz Ansari" e "Carla Gallo". A terceira é "Rhyme & Reason", "Chuck D." e "Desiree Densiti". As colunas `lead_date_added` e `lag_date_added` são `null` nas três linhas.

**49.** Quais outras window functions o capítulo lista sem demonstrar? *(Using window functions, There's more)*

R: `rank`, `dense_rank`, `percent_rank`, `cume_dist`, `first_value`, `last_value` e `nth_value`. São sete nomes soltos, sem definição e sem exemplo.

**50.** O que o capítulo chama de nested window function, e o que o exemplo faz? *(Using window functions, There's more, Nested window functions)*

R: Usar a saída de uma window function como entrada de outra. O exemplo cria `running_total` com `count("show_id").over(window_spec)`, depois `next_running_total` com `lead("running_total").over(window_spec)`, e por fim `diff` como a subtração das duas. A window spec particiona por `country` e ordena por `release_year`.

**51.** O que o capítulo diz que o exemplo de nested window functions calcula, e o que o código de fato usa? *(Using window functions, There's more, Nested window functions)*

R: A prosa diz que se calcula o running total de uma coluna usando "the sum window function". O código usa `count("show_id")`. O bloco chega a importar `sum` de `pyspark.sql.functions` e nunca a usa.

**52.** O que é um window frame, segundo o capítulo, e qual é o único frame que ele mostra? *(Using window functions, There's more, Window frames)*

R: É a especificação de um intervalo mais restrito de linhas dentro da window, com um ponto de início e um de fim. A função de agregação só é calculada sobre as linhas desse intervalo. O único mostrado é `rowsBetween(-2, 0)`, descrito como a linha atual mais as duas anteriores.

**53.** Escreva o exemplo de window frame e a saída impressa. *(Using window functions, There's more, Window frames)*

R:

```python
data = [(1, 10), (2, 15), (3, 20), (4, 25), (5, 30)]
df = spark.createDataFrame(data, ["id", "value"])
windowSpec = Window.orderBy("id").rowsBetween(-2, 0)
df = df.withColumn("rolling_avg", avg(df["value"]).over(windowSpec))
```

A saída tem `rolling_avg` igual a 10.0, 12.5, 15.0, 20.0 e 25.0. Conferi as cinco médias na mão e as cinco estão corretas.

**54.** Quais são os três passos de UDF na receita, e qual código cada um traz? *(Writing custom UDFs, How to do it, steps 3 a 5)*

R: Definir uma função Python comum, `def concat(first_name, last_name): return first_name + " " + last_name`. Registrá-la com `concat_udf = udf(concat)`, importando `udf` de `pyspark.sql.functions`. E declarar o tipo de retorno como segundo argumento, `concat_udf = udf(concat, StringType())`.

**55.** Como a UDF é aplicada ao DataFrame? *(Writing custom UDFs, How to do it, step 6)*

R: Com `withColumn`, que recebe o nome da coluna nova e o objeto UDF aplicado às colunas de entrada. O código é `df_flattened.withColumn("full_name", concat_udf(df_flattened["firstname"], df_flattened["surname"]))`.

**56.** O que o passo 2 da receita de UDFs faz antes de chegar na UDF? *(Writing custom UDFs, How to do it, step 2)*

R: Lê `nobel_prizes.json`, explode `laureates`, e projeta oito colunas, entre elas `laureates.id`, `laureates.firstname` e `laureates.surname`. Depois aplica um `filter` com `isNotNull` sobre nome e sobrenome. A linha do `filter` está cortada na margem do PDF, e o que se lê dela é `.filter(col("laureates.firstname").isNotNull() & col("laureates.surname`.

**57.** Como se registra uma UDF para uso em SQL, e quais três argumentos a chamada recebe? *(Writing custom UDFs, There's more)*

R: Com `spark.udf.register`. Os três argumentos são o nome a dar à UDF, a função em si e o tipo de retorno. O exemplo é `spark.udf.register("square", square_udf, IntegerType())`, seguido de `createOrReplaceTempView("numbers")` e `spark.sql("SELECT num, square(num) AS square_num FROM numbers")`.

**58.** Quais três links o "See also" da receita de UDFs traz, e de quais versões? *(Writing custom UDFs, See also)*

R: "Functions" e "Scalar UDFs", os dois rotulados Spark 3.4.0 e apontando para `/docs/3.4.0/`. E `pyspark.sql.functions.pandas_udf`, rotulado PySpark 3.1.2 e apontando para `/docs/3.1.2/`. É o único "See also" do capítulo com três links, e o único que não usa `/docs/latest/`.

**59.** Quais três técnicas de tratamento de null a receita demonstra, e com qual função cada uma? *(Handling null values, How to do it, steps 3 a 5)*

R: Descartar, com `dropna()`. Preencher, com `fillna("N/A")`. E substituir por condição, com `when()` e `otherwise()`.

**60.** Quais colunas o passo 5 trata, e com quais valores? *(Handling null values, How to do it, step 5)*

R: Ele encadeia cinco `withColumn`. As colunas `category`, `overallMotivation`, `firstname` e `surname` recebem string vazia no lugar de null. A coluna `year` recebe `9999`.

**61.** O que a prosa do passo 5 diz que o código faz? *(Handling null values, How to do it, step 5)*

R: Que o código substitui os nulos da coluna `age` por 0 e os da coluna `gender` por "Unknown". Nenhuma das duas colunas existe no DataFrame da receita.

**62.** Como o capítulo trata null dentro de uma UDF? *(Handling null values, There's more, Handling null values in UDFs)*

R: A prosa diz para usar o método `isNull` e `na.fill`. O código faz outra coisa: define `def process_name(name)` com um `if name is None: return "Unknown"` em Python puro. Nem `isNull` nem `na.fill` aparecem no código.

**63.** O que faz o `Imputer`, de onde ele vem e qual é o padrão dele? *(Handling null values, There's more, Handling null values in machine learning pipelines)*

R: Substitui valores nulos pela média, pela mediana ou pela moda da coluna. Vem do módulo `ml.feature` do Spark e é chamado de transformer. O padrão declarado é a média. O uso é `imputer.fit(df)` seguido de `imputer_model.transform(df)`.

**64.** Para onde aponta o link de "handling missing data" do último "See also"? *(Handling null values, See also)*

R: Para `https://spark.apache.org/docs/latest/sql-data-sources.html`, que é a página índice de data sources. Os outros dois links do mesmo bloco saem do domínio do Spark e vão para a documentação de missing data do pandas e do NumPy.

**65.** Quantas URLs da documentação do Spark o capítulo cita, e como elas se dividem por versão? *(todas as receitas, See also)*

R: Contei 11 URLs de `spark.apache.org`. Sete usam `/docs/latest/`, duas usam `/docs/3.4.0/` e duas usam `/docs/3.1.2/`. Três receitas repetem `programming-guide.html` ou a raiz de `/docs/latest/`.

**66.** Quais datasets o capítulo usa ao todo? *(todas as receitas)*

R: `nobel_prizes.json` em quatro receitas, `netflix_titles.csv` em três, `recipes.parquet` uma vez, os quatro CSVs da pasta `Credit Card` uma vez, e `Reviews.csv` uma vez. Fora isso, três DataFrames inline criados com `createDataFrame` e dois DataFrames fantasma chamados `df1` e `df2`, que nunca são construídos.

**67.** Onde aparecem `df1` e `df2`, e de onde eles vêm? *(Performing joins, There's more)*

R: Nas cinco subseções de "There's more" da receita de joins, ou seja, right outer, full outer, cross, broadcast e multiple conditions. Eles não vêm de lugar nenhum. Nenhuma linha do capítulo os cria, e as colunas usadas neles são `Name`, `Gender` e `Age`, que não existem em nenhum dataset do capítulo.

---

## Nível 2 — Compreensão

Explique com suas próprias palavras. Três a seis frases cada.

**1.** Explique por que a receita 1 abre com `transform` e não com algo mais simples, e o que essa escolha diz sobre a sequência do capítulo. *(Applying basic transformations, How to do it, step 3)*

R: A receita se chama "basic transformations" e o primeiro exemplo é uma higher-order function sobre uma coluna de array de structs. Isso não é básico. A escolha só faz sentido se o critério for a continuidade com o capítulo 1, que terminou em dados aninhados e usou `nobel_prizes.json`. O capítulo herda o dataset e começa de onde parou, não do mais simples. As operações que realmente são básicas, `dropDuplicates`, `orderBy` e `withColumnRenamed`, vêm depois, nos passos 4 a 7.

**2.** Explique a diferença entre a lambda passada a `transform` na receita 1 e a UDF da receita 6. *(Applying basic transformations, step 3 + Writing custom UDFs, steps 3 e 4)*

R: A lambda da receita 1 recebe uma `Column` e devolve uma `Column`, montada com `concat` e `lit`, que são funções embutidas. Ela é executada na JVM, porque o que ela produz é uma expressão de plano, não código Python de tempo de execução. A UDF da receita 6 é uma função Python de verdade, que recebe duas strings e devolve uma string, e precisa ser registrada com `udf()` para o Spark saber enviá-la aos executors. As duas fazem a mesma concatenação de nome e sobrenome no mesmo dataset. O capítulo chama as duas de "user-defined function" e nunca marca que são coisas diferentes.

**3.** Explique o que a NOTE sobre `groupBy` está tentando ensinar e por que a segunda frase dela é a que importa. *(Performing aggregations, NOTE)*

R: A primeira metade repete lazy evaluation, que já era o assunto da única NOTE conceitual do capítulo 1. A segunda metade dá um fato novo e conferível: `groupBy` devolve um `GroupedData`, não um DataFrame. Isso importa porque explica por que `df.groupBy("country")` sozinho não pode ser mostrado com `show()`. Também explica por que sempre existe uma segunda chamada, `count()` ou `agg()`, que é o que reconverte o grupo em DataFrame. É a informação de tipo que o resto da receita pressupõe sem dizer.

**4.** Explique por que o capítulo precisa de `agg` se `groupBy` já tem `count`, `max` e `min`. *(Performing aggregations, How to do it, steps 4 e 5)*

R: Os métodos diretos do `GroupedData` fazem uma agregação de cada vez e sobre todas as colunas numéricas ou sobre nenhuma coluna nomeada. O `agg` recebe uma lista de expressões, então permite várias agregações diferentes na mesma passada. Ele também permite `alias`, que é a única forma de dar nome legível à coluna de saída. O passo 5 usa as duas coisas ao mesmo tempo, três agregações e três aliases. Sem `agg` não dá para pedir contagem, máximo e mínimo em uma chamada só.

**5.** Explique o que o pivot faz ao formato do DataFrame, usando o exemplo do capítulo. *(Performing aggregations, There's more, Pivot tables)*

R: O `groupBy("country")` decide as linhas, uma por país. O `pivot("type")` decide as colunas, uma por valor distinto de `type`, que no dataset de Netflix são "Movie" e "TV Show". O `agg(count("show_id"))` decide o que vai dentro de cada célula. O resultado é uma matriz de país por tipo, com a contagem no cruzamento. É a mesma informação de um `groupBy("country","type").count()`, só que virada de linhas para colunas.

**6.** Explique por que a NOTE do pivot fala em shuffle e em cardinalidade na mesma frase. *(Performing aggregations, NOTE)*

R: São os dois custos distintos da operação. O shuffle é o custo de execução: para saber quais valores distintos existem em `type`, o Spark precisa varrer a coluna e reagrupar as linhas pela rede. A cardinalidade é o custo de esquema: cada valor distinto vira uma coluna, então mil valores distintos viram mil colunas. O primeiro custo é de tempo e o segundo é de forma do resultado. O capítulo junta os dois em uma NOTE de quatro linhas e não separa que são problemas de natureza diferente.

**7.** Explique o que a receita de joins ganha ao usar quatro tabelas de cartão de crédito em vez de dois DataFrames inline. *(Performing joins, How to do it, steps 2 a 5)*

R: Ela ganha a única coisa que um exemplo de join precisa ter, que é um motivo para escolher o tipo de join. Cartões para clientes é inner porque todo cartão tem dono. Transações para fraudes é left outer porque nem toda transação é fraude. Com dois DataFrames genéricos, a escolha do tipo vira decoração. O passo 5 fecha o argumento ao encadear os dois resultados anteriores em um terceiro join. É o trecho mais bem construído do capítulo, e é justamente o que "There's more" abandona ao voltar para `df1` e `df2`.

**8.** Explique a diferença entre passar `on='Cust_ID'` e passar uma expressão em `on=`. *(Performing joins, How to do it, steps 3 e 5)*

R: Passar o nome da coluna só funciona quando a chave se chama igual dos dois lados, e nesse caso o Spark devolve uma coluna só, não duas. Passar uma expressão funciona quando os nomes divergem, e é o caso do passo 5, em que a chave é `Card_Number` de um lado e `Credit_Card_ID` do outro. A expressão também aceita condições que não são igualdade, e o passo 5 usa isso ao encaixar `isNotNull()` dentro da mesma expressão. O preço é que as duas colunas de chave sobrevivem no resultado. O capítulo mostra as duas formas e não comenta essa diferença de saída.

**9.** Explique o que a definição de window specification do capítulo deixa de fora. *(Using window functions, How to do it, step 3 + There's more, Window frames)*

R: A definição do passo 3 nomeia duas coisas, como particionar e como ordenar. Existe uma terceira, o frame, e o capítulo a apresenta cinco páginas depois, em "There's more", como se fosse um recurso extra. Não é extra. Toda window function tem frame, e quando não se declara um, o Spark usa um padrão. O capítulo nunca diz que existe padrão nem qual é. Isso é o que torna o exemplo de `running_total` opaco, porque o resultado dele depende exatamente do frame que ninguém declarou.

**10.** Explique a diferença entre uma agregação com `groupBy` e a mesma agregação como window function. *(Performing aggregations, step 3 + Using window functions, There's more)*

R: O `groupBy` colapsa. Cem linhas de um país viram uma linha com a contagem. A window function não colapsa. As cem linhas continuam cem, e cada uma ganha uma coluna a mais com o valor calculado sobre a partition. É por isso que o exemplo de nested window functions consegue calcular um running total e depois comparar cada linha com a próxima. Com `groupBy` isso é impossível, porque as linhas individuais já não existem. O capítulo põe as duas receitas lado a lado e nunca enuncia essa diferença.

**11.** Explique por que a receita de UDFs precisa do passo 5 se o passo 4 já registrou a UDF. *(Writing custom UDFs, How to do it, steps 4 e 5)*

R: O passo 4 chama `udf(concat)` sem tipo de retorno. O Spark precisa saber o tipo da coluna de saída para montar o schema antes de executar. Sem declaração, ele assume um padrão. O passo 5 declara `StringType()` de forma explícita e sobrescreve a variável. O capítulo apresenta os dois passos como se fossem etapas sequenciais de um mesmo processo, mas eles são duas versões alternativas da mesma linha. Ele também nunca diz qual é o tipo assumido no passo 4.

**12.** Explique a diferença entre `udf()` e `spark.udf.register()`. *(Writing custom UDFs, How to do it, step 4 + There's more)*

R: `udf()` devolve um objeto que se chama dentro da DataFrame API, dentro de `withColumn` ou `select`. `spark.udf.register()` grava um nome no catálogo da sessão, e esse nome passa a existir dentro de strings SQL. A primeira é uma variável Python, a segunda é uma entrada em um registro do Spark. O exemplo de "There's more" precisa das duas coisas: define a função, registra o nome `square`, cria uma temp view e escreve `SELECT num, square(num)`. É a única aparição de `spark.sql` no capítulo inteiro.

**13.** Explique por que `dropna`, `fillna` e `when/otherwise` são três respostas para o mesmo problema. *(Handling null values, How to do it, steps 3 a 5)*

R: As três decidem o que fazer com a ausência, e diferem no que elas custam. O `dropna` custa linhas, porque elimina o registro inteiro por causa de uma coluna. O `fillna` custa verdade, porque inventa um valor onde não havia. O `when/otherwise` custa código, porque exige uma regra escrita por coluna, mas é o único que permite decidir coluna a coluna. A receita mostra os três na ordem do mais grosso para o mais fino. Ela não diz em nenhum momento que a escolha entre eles é uma decisão de negócio, não de sintaxe.

**14.** Explique o que `fillna("N/A")` faz com uma coluna numérica. *(Handling null values, How to do it, step 4)*

R: O capítulo diz que o código "vai substituir todos os valores nulos por N/A". Isso não pode ser verdade em uma coluna de inteiro, porque "N/A" não é um inteiro. No DataFrame da receita, `year` vem do JSON como número. Então o `fillna("N/A")` não tem como tocar nessa coluna. O comportamento real é preencher só as colunas cujo tipo aceita o valor passado, e ignorar as demais em silêncio. *Conferi o comportamento documentado no item 11 do Nível 5.*

**15.** Explique por que o passo 5 da receita de nulls existe, dado que o passo 4 já preenche nulos. *(Handling null values, How to do it, steps 4 e 5)*

R: Porque `fillna` é um valor só para tudo, e a realidade quase nunca é assim. No passo 5, quatro colunas de texto recebem string vazia e `year` recebe `9999`. Isso é impossível com um `fillna` de argumento único, e é o argumento a favor do `when/otherwise`. O capítulo mostra a diferença no código e não a enuncia no texto. Ele também não menciona a forma intermediária, que é passar um dicionário de coluna para valor ao `fillna`.

**16.** Explique o que a subseção "Handling null values in UDFs" está de fato demonstrando. *(Handling null values, There's more, Handling null values in UDFs)*

R: Ela demonstra que uma UDF Python recebe o null do Spark como `None` no lado Python. É por isso que o `if name is None` funciona. Isso é um fato útil e específico, porque significa que toda UDF que faz `name.upper()` sem checar quebra com `AttributeError` na primeira linha nula. A prosa da subseção descreve outra coisa, `isNull` e `na.fill`, que são métodos do DataFrame e não têm nada a ver com o interior da função. O código ensina a lição certa e o texto ao redor descreve outra.

---

## Nível 3 — Aplicação e transferência

Cenários concretos. O capítulo te equipa para responder, mas não responde por você. Faça no ambiente, não no papel.

**1.** Você tem um catálogo de filmes em CSV e precisa filtrar por data de inclusão entre 1 de fevereiro e 1 de março de 2021. Copiando a receita do capítulo, o que acontece? *(Filtering data, There's more, Filtering on data ranges)*

R: Não retorna o que eu quero, e a Figura 2.1 é a prova. Ela mostra `date_added` com valores como "September 24, 2021", ou seja, texto por extenso. A leitura do passo 2 não passa `schema` nem `inferSchema`, então a coluna é string. Comparar uma string com `"2021-02-01"` é comparação lexicográfica, não cronológica. Como dígito vem antes de letra na ordem de caracteres, toda data por extenso é maior que `"2021-02-01"` e maior que `"2021-03-01"`. O filtro devolve vazio, sem erro e sem aviso. O que resolve é converter a coluna com `to_date(col("date_added"), "LLLL d, y")` antes de comparar, ou declarar um schema com `DateType`.

**2.** No mesmo cenário, o capítulo passa `option("dateFormat", "LLLL d, y")` na leitura. Isso ajuda? *(Filtering data, How to do it, step 2)*

R: Não. `dateFormat` diz ao parser como interpretar um texto quando o destino é uma coluna de data. Sem `schema` e sem `inferSchema`, o destino é string, e não há nada a interpretar. A opção é aceita, não dá erro e não faz nada. Ela aparece nas três receitas que leem `netflix_titles.csv` e em nenhuma delas tem efeito. É o mesmo padrão do capítulo 1, em que opções eram oferecidas como solução sem que ninguém conferisse se elas se aplicavam.

**3.** Você precisa da data mais recente de ingestão por país nesse catálogo. Aplique o passo 5 da receita de aggregations e avalie o resultado. *(Performing aggregations, How to do it, step 5)*

R: O código roda e a resposta está errada. Como `date_added` é string, `max()` devolve o máximo lexicográfico, não o cronológico. Entre os nomes de mês, "September" é o maior em ordem alfabética, então setembro sempre ganha de dezembro. A Figura 2.2 confirma que a coluna é texto de outra forma, e pior: nas linhas 2 e 3 o `LastReleaseDate` é "Kobina Amissah Sam" e "Carla Gallo", que são nomes de pessoas. A correção é a mesma da questão 1, converter para `DateType` antes de agregar.

**4.** Você recebe um dataset de voos e precisa do primeiro e do último voo por aeroporto de origem. Escreva a agregação e diga o que copiar do capítulo e o que não copiar. *(Performing aggregations, How to do it, steps 4 e 5)*

R: A forma que eu copio é a do passo 5, `groupBy` mais `agg` com `count`, `max` e `min`, cada um com `alias`. É boa e é reutilizável. O que eu não copio é a leitura do passo 2. Antes de agregar por tempo, eu declaro um schema com `TimestampType` na coluna de horário, ou converto com `to_timestamp`. Sem isso, `max` e `min` respondem sobre texto. A receita do capítulo entrega a sintaxe certa aplicada ao tipo errado, e a sintaxe é a parte fácil.

**5.** Você precisa saber quantos aeroportos distintos existem em um dataset de 500 milhões de linhas, com tolerância de erro. Como o capítulo te equipa? *(Performing aggregations, There's more, Approximate aggregations)*

R: Ele me dá `approx_count_distinct("origin", rsd=...)` e o argumento correto: trocar exatidão por tempo quando o resultado exato não é necessário. O que ele erra é o vocabulário. Ele chama o terceiro argumento de `approxQuantile` de RSD também, e as duas funções não usam o mesmo parâmetro. Ele também usa `rsd=0.1`, que é o dobro do padrão e um erro grande para uma contagem. Antes de escolher o valor eu preciso saber qual é o padrão, e o capítulo não diz. *Conferi os dois parâmetros no item 8 do Nível 5.*

**6.** Você tem um dataset de leituras de sensores e precisa da média móvel das últimas três leituras por sensor. Monte a window spec com o que o capítulo dá. *(Using window functions, There's more, Window frames)*

R: `Window.partitionBy("sensor_id").orderBy("ts").rowsBetween(-2, 0)`, com `avg("valor").over(windowSpec)`. É a fusão de duas coisas que o capítulo mostra separadas. O `partitionBy` vem do passo 3 e o `rowsBetween(-2, 0)` vem de "Window frames". O exemplo de window frame do capítulo não tem `partitionBy` nenhum, ou seja, ele calcula a média móvel sobre o dataset inteiro em uma partition só. Copiado como está, ele daria uma média móvel misturando sensores diferentes.

**7.** Você quer o ranking dos três produtos mais vendidos por categoria em um catálogo de e-commerce. O capítulo te dá a ferramenta? *(Using window functions, How to do it, step 4 + There's more)*

R: Meio. Ele demonstra `row_number()` com `partitionBy` e `orderBy`, e é isso que eu preciso, mais um `filter` no fim para pegar os três primeiros. Ele nomeia `rank` e `dense_rank` em "There's more" e não explica nenhum dos dois. A escolha entre os três importa quando há empate de vendas, e essa é exatamente a decisão que o cenário exige. O `row_number` quebra empate de forma arbitrária, e as outras duas não. A NOTE do capítulo toca no assunto e o descreve de um jeito que eu não consegui validar. *Voltei nisso no item 6 do Nível 4 e no item 14 do Nível 5.*

**8.** Você precisa juntar uma tabela de fatos de 200 GB com uma tabela de dimensão de 5 MB. Como o capítulo te orienta? *(Performing joins, There's more, Broadcast join)*

R: Ele me dá a ferramenta certa, `df_fatos.join(broadcast(df_dim), "chave", "inner")`, e o motivo certo, evitar shuffle da tabela grande. O que falta é tudo que decide se eu preciso escrever isso. O Spark já promove joins a broadcast sozinho abaixo de um limite de tamanho, e o capítulo não diz que esse limite existe nem qual é. Ele também não diz o que acontece se eu marcar como broadcast uma tabela grande demais, que é estourar a memória do driver. *Conferi o limite no item 3 do Nível 5.*

**9.** Você precisa achar todos os clientes que nunca fizeram uma transação. Que join você usa, e o capítulo te ajuda? *(Performing joins, There's more)*

R: Um left anti join, `clientes.join(transacoes, "cliente_id", "left_anti")`, que devolve só as linhas da esquerda sem correspondência e só as colunas da esquerda. O capítulo não ajuda. Ele lista inner, left outer, right outer, full outer e cross, e nenhum semi ou anti. Com o que ele dá, eu faria um left outer seguido de um filtro por null na chave da direita. Isso funciona, é mais lento e arrasta as colunas da direita sem necessidade. *Conferi os tipos que existem hoje no item 6 do Nível 5.*

**10.** Você tem uma pipeline de logs de servidor e precisa marcar toda requisição sem `user_agent` como "desconhecido", mantendo as linhas. Qual passo do capítulo você usa? *(Handling null values, How to do it, steps 4 e 5)*

R: O passo 5, com `when(col("user_agent").isNull(), "desconhecido").otherwise(col("user_agent"))`. O passo 3 está fora de questão, porque `dropna()` sem argumento derruba a linha inteira por causa de uma coluna nula qualquer, e eu quero manter as linhas. O passo 4 serviria se eu quisesse o mesmo valor em todas as colunas de texto. Como eu quero mexer em uma coluna só, `when/otherwise` é a resposta. A forma mais curta seria `fillna({"user_agent": "desconhecido"})`, e o capítulo não mostra a variante com dicionário.

**11.** Você precisa concatenar dois campos de texto de um dataset de catálogo. Você escreveria uma UDF, seguindo a receita 6? *(Writing custom UDFs, How to do it, steps 3 a 6)*

R: Não. A UDF do capítulo faz `first_name + " " + last_name`, que é exatamente o que a função embutida `concat` faz, e o próprio capítulo usa `concat` na receita 1. Escolher a UDF aqui troca uma expressão executada na JVM por uma chamada que atravessa a fronteira entre JVM e Python linha a linha. O capítulo escolhe o pior exemplo possível para ensinar UDF, porque o caso dele tem substituto embutido. Um bom exemplo de UDF seria algo sem equivalente, como chamar uma biblioteca de parsing de endereço. *Conferi o custo e o estado atual das UDFs Python no item 10 do Nível 5.*

**12.** Você quer somar os valores de uma coluna de array por linha, sem explodir. O capítulo te equipa? *(Applying basic transformations, How to do it, step 3)*

R: Ele aponta o caminho e para na metade. A receita 1 mostra `transform`, que é uma higher-order function de verdade, com lambda e tudo. O que eu preciso é `aggregate`, que é a HOF que reduz um array a um valor, e ela não aparece. O capítulo mostra uma HOF, não usa o nome da categoria em nenhum momento, e não lista nenhuma outra. É o inverso do capítulo 1, que nomeava a sigla HOF e dava um exemplo que não era HOF. *Conferi a lista atual no item 11 do Nível 5.*

**13.** Você quer rodar a receita de joins em Databricks, e não no ambiente Docker do livro. O que muda? *(Performing joins, How to do it, steps 1 e 6)*

R: O builder inteiro sai, porque a `SparkSession` já existe na variável `spark` e `spark://spark-master:7077` aponta para um host inexistente. O `spark.stop()` do passo 6 também sai, porque derruba o ambiente do notebook. Os caminhos `../data/Credit Card/...` viram volumes do Unity Catalog ou object storage. E o `broadcast()` continua válido, porque é API do Spark. É o mesmo trabalho de portabilidade do capítulo 1, e o capítulo 2 tampouco menciona que ele é necessário.

---

## Nível 4 — Análise e síntese

Raciocínio que cruza receitas. Este é o nível em que as três figuras deixam de ser ilustração e viram evidência.

**1.** **A Figura 2.2 é uma confissão.** Olhe a saída impressa da agregação múltipla e liste tudo que ela revela sobre a leitura de `netflix_titles.csv`.

R: Achei quatro coisas.

Primeira, a coluna `country` contém "Ama K. Abebrese" e "Aziz Ansari". São nomes de pessoas, não países. O dado da coluna `cast` ou `director` foi parar na coluna `country` nessas linhas.

Segunda, a coluna `LastReleaseDate`, que é `max("date_added")`, contém "Kobina Amissah Sam" e "Carla Gallo". Uma agregação de máximo devolveu nome de pessoa. Isso só é possível se `date_added` for uma coluna de string e se o conteúdo daquela linha estiver deslocado.

Terceira, o grupo de `country` igual a `null` tem 831 linhas. É o maior grupo da saída e não é comentado.

Quarta, `LastReleaseDate` e `FirstReleaseDate` são idênticos nas linhas 2 e 3, porque cada uma tem uma linha só. O `NumberOfReleases` igual a 1 confirma.

O que isso somado significa: a leitura do passo 2 está desalinhando colunas em parte das linhas, e a agregação está sendo feita sobre texto. A figura é a prova impressa de que o resultado está errado. O capítulo a legenda como "DataFrame output after multiple aggregates" e não escreve uma palavra sobre nada disso.

**2.** **A Figura 2.3 mata a receita que ela ilustra.** Examine a saída de `lead` e `lag` e diga o que ela demonstra.

R: Ela demonstra que a receita de window functions não funcionou.

As colunas `lead_date_added` e `lag_date_added` são `null` nas três linhas mostradas. Pelo próprio texto do capítulo, `lead` devolve null quando o deslocamento ultrapassa a fronteira da partition. Se as duas são null na mesma linha, aquela linha é a única da partition dela.

A causa está na window spec, `partitionBy("country")`, cruzada com o desalinhamento da questão 1. Se `country` contém "Ama K. Abebrese", um valor que aparece uma vez só, então aquela partition tem exatamente uma linha. Sem linha anterior e sem linha seguinte, `lead` e `lag` só podem devolver null.

Ou seja, a figura escolhida para ilustrar `lead` e `lag` é composta inteiramente por linhas em que `lead` e `lag` não têm o que fazer. Um exemplo de window function cuja saída não mostra nenhum valor de window function.

Existe um segundo defeito na mesma figura. A window spec ordena por `date_added`, que é string. Mesmo em uma partition com muitas linhas, a ordem seria alfabética por nome de mês, com abril antes de dezembro. A ordem que `lead` e `lag` percorrem não é cronológica.

**3.** **A Figura 2.1 é a que funciona, e é por isso que ela importa.** Compare-a com as outras duas e diga o que a diferença explica.

R: A Figura 2.1 mostra 20 linhas em que tudo está no lugar: `country` é "United States", `date_added` é "September 24, 2021", `release_year` é 2021 e `rating` é "PG-13" ou "TV-MA". Nenhum desalinhamento.

A explicação é que ela é a saída de um filtro por `isin(["United States", "United Kingdom", "India"])`. Filtrar por valor de país seleciona, por construção, apenas as linhas em que a coluna `country` contém de fato um país. O filtro esconde o problema.

As Figuras 2.2 e 2.3 não filtram nada. A 2.2 agrupa por `country` e a 2.3 ordena por `country`. As duas expõem os valores que a 2.1 tinha excluído.

A conclusão é desconfortável e é a lição prática do capítulo inteiro. O mesmo dataset, lido pelo mesmo código, produz um exemplo que parece certo e dois que estão errados. A diferença não é o código, é se a operação seguinte esconde ou expõe o defeito. Um filtro por lista branca é um mecanismo de ocultação de dado sujo.

**4.** **Onde `date_added` deixa de ser uma data.** Rastreie a coluna pelas três receitas que a usam e mostre o efeito acumulado.

R: A coluna é lida três vezes, sempre com as mesmas opções e sempre como string.

Na receita de filtering, ela é comparada com `"2021-02-01"` por operador e por `between`. As duas comparações são lexicográficas e o resultado não corresponde a nenhum intervalo de datas.

Na receita de aggregations, ela entra em `max()` e `min()`. O resultado é o extremo alfabético, não o cronológico, e a Figura 2.2 imprime "September 9, 2021" como o mais recente.

Na receita de window functions, ela é o `orderBy` da window spec. A ordem que `row_number`, `lead` e `lag` percorrem é a ordem alfabética dos nomes de mês.

O efeito acumulado é que uma coluna de data governa três receitas inteiras sem nunca ter sido convertida em data. O capítulo passa `dateFormat` nas três leituras, o que sinaliza que o autor sabia que a coluna era temporal. Ele nunca declara o schema, nunca liga `inferSchema` e nunca chama `to_date`. A opção que ele passa é a que não faz nada, e as três que resolveriam não aparecem.

**5.** **Nomes fantasma.** Faça o inventário das colunas e variáveis citadas na prosa que não existem no código, e diga o que o padrão revela.

R: Achei sete casos, em cinco receitas.

Na receita 1, passo 4, a prosa diz que o resultado tem uma linha por combinação única "in the Name and Age columns". O código deduplica por `category`, `overallMotivation` e `year`.

Na receita 1, passo 5, a prosa diz três vezes que o resultado fica "sorted by age in ascending order". Não existe coluna `age`.

Na receita 1, passo 6, a prosa diz que se renomeia a coluna `Age` para `Years`. O código renomeia `category` para `Topic`.

Na receita 1, passo 7, o título do passo diz "use the select function" e o corpo diz "we use the selectExpr function". A prosa fala em renomear `Name` para `FirstName` e `Age` para `Years`. O código renomeia três colunas, e nenhuma delas se chama assim.

Na receita 3, passo 2, a prosa diz "the datasets for cars, customers, transactions, and fraud". O dataset é de cartões, não de carros.

Na receita 4, passo 5, a prosa diz "we will calculate the total sales amount for each product". Não há venda nem produto. O código conta lançamentos da Netflix por país.

Na receita 5, passo 5, a prosa fala em "the original category and value columns" e nas colunas `lead_value` e `lag_value`. O código produz `lead_date_added` e `lag_date_added`.

O padrão é único e reconhecível. Todas as prosas fantasma descrevem datasets tabulares genéricos, com colunas `Name`, `Age`, `gender`, `value`, `category`, produto e venda. São os datasets de tutorial padrão de Spark. A prosa foi escrita para outros exemplos e o código foi trocado por baixo dela. É o mesmo mecanismo que produziu "JSON" na receita de XML do capítulo 1, aqui em escala muito maior.

**6.** **A NOTE de `row_number`.** Leia a NOTE com atenção e decida se o conselho que ela dá resolve o problema que ela nomeia.

R: Não resolve, e o diagnóstico também é impreciso.

A NOTE diz que `row_number` é não determinística e que a ordem das linhas dentro de uma partition pode variar entre execuções. O que varia entre execuções não é a função, é o desempate. Quando duas linhas têm o mesmo valor no `ORDER BY`, o Spark não garante qual delas recebe o número menor, porque a ordem em que as linhas chegam ao operador depende do shuffle. A instabilidade vem do empate no `ORDER BY`, não da função.

A recomendação é o erro mais consequente. Trocar `row_number` por `rank` ou `dense_rank` não estabiliza a ordem. Essas funções dão o mesmo valor às linhas empatadas, então a ordem entre elas continua indefinida. O que elas fazem é parar de fingir que existe uma ordem. Isso é diferente de produzir uma ordem estável.

O que resolve de fato é acrescentar colunas ao `ORDER BY` até que a chave de ordenação seja única. O capítulo não menciona isso.

Vale registrar o que sobra de correto. Se o meu objetivo for reproduzir o mesmo resultado a cada execução, `rank` e `dense_rank` são de fato determinísticos, porque o valor deles depende só dos valores da coluna de ordenação. Nesse sentido restrito a NOTE aponta na direção certa e chama a coisa pelo nome errado. *Conferi se o Spark marca `row_number` como não determinística no item 14 do Nível 5.*

**7.** **O bloco de `getItem` sobre um map que não é um map.** Analise a subseção "Filtering on map columns" e diga quantas coisas estão fora do lugar.

R: Três.

Primeira, o título e a prosa. O texto diz "suppose you have a DataFrame with a column called laureates that contains a map". A coluna `laureates` no `nobel_prizes.json` é um array de structs, e o próprio código explode esse array antes de filtrar. Depois do `explode`, o que sobra em `laureates` é um struct, não um map.

Segunda, o método. O capítulo usa `getItem("firstname")` sobre um struct. A documentação descreve `getItem` para posição em lista e para chave em dicionário. O método para campo de struct é `getField`. O capítulo 1 chegou a nomear `getField` e este capítulo o esquece.

Terceira, a coerência interna. A subseção anterior se chama "Filtering on arrays" e trata de array. Esta se chama "Filtering on map columns" e trata do mesmo tipo de estrutura por outro caminho. As duas juntas cobrem array e struct, e a palavra map fica sobrando.

O que isso mostra é a mesma coisa que a questão 5. O capítulo tem uma taxonomia de subseções escrita antes do código, e o código foi encaixado na subseção mais próxima disponível.

**8.** **O parâmetro que não existe.** Confronte a descrição de `pivot` com o exemplo que a acompanha.

R: O capítulo afirma que `pivot` recebe três parâmetros: a coluna de pivot, a coluna de valores e a lista opcional de valores distintos. O exemplo, três linhas abaixo, é `df.groupBy("country").pivot("type").agg(count("show_id"))`.

O `pivot` do exemplo recebe um argumento. A coluna sobre a qual se agrega, `show_id`, não entra em `pivot`, entra em `agg`. Não existe nenhum lugar na chamada para uma segunda coluna dentro de `pivot`.

Ou seja, o próprio exemplo do capítulo refuta a frase que o precede. O parâmetro chamado de "values column" não existe, e o que o capítulo chama de terceiro parâmetro é na verdade o segundo.

O custo disso é específico. O único parâmetro opcional real de `pivot` é justamente o que a NOTE seguinte torna importante, porque é ele que evita a varredura para descobrir os valores distintos. A NOTE avisa que a operação é cara e a descrição erra o nome do parâmetro que a barateia. *Conferi a assinatura no item 7 do Nível 5.*

**9.** **RSD para tudo.** O capítulo usa a mesma sigla para duas funções diferentes. Analise se elas podem compartilhar o mesmo parâmetro.

R: Não podem, e a razão é matemática, não de nomenclatura.

A `approx_count_distinct` implementa uma estimativa de cardinalidade por HyperLogLog. Ela produz um número com incerteza estatística, e o parâmetro `rsd` é o desvio padrão relativo dessa estimativa. Falar em desvio padrão faz sentido aqui porque o resultado é uma variável aleatória.

A `approxQuantile` implementa um algoritmo de sumário de quantis com garantia determinística de posto. O parâmetro dela limita o quanto o posto do valor devolvido pode se afastar do posto pedido. Não é um desvio padrão, é uma tolerância de rank.

O capítulo chama os dois de RSD e escreve, para o `approxQuantile`, que "o RSD determina o erro máximo de aproximação permitido". A frase mistura as duas ideias em uma.

O efeito prático é que um leitor calibra o valor errado. Ele vai pensar em `0.1` como "10% de erro" nos dois casos, e nos dois casos isso significa coisas diferentes. *Conferi as duas assinaturas e as duas descrições no item 8 do Nível 5.*

**10.** **O capítulo fala de shuffle três vezes e nunca o define.** Rastreie as três aparições e diga o que falta entre elas.

R: Rodei `grep -in "shuffl"` e achei exatamente três ocorrências, em duas receitas.

Duas estão na subseção de broadcast join: "Spark may need to shuffle data across the network, which can be slow and resource-intensive" e "avoid shuffling".

Uma está na NOTE de pivot: "the pivot operation requires the shuffling of data, so it can be an expensive operation".

O que falta é tudo. Não há definição de shuffle. Não há explicação de por que um join exige shuffle. Não há a palavra partition ligada a shuffle. Não há menção de que o número de partitions de saída de um shuffle é configurável, nem de qual é o padrão. Não há `explain()` em nenhum ponto do capítulo, então nenhum plano de execução é mostrado.

Isso é grave neste capítulo em particular, e não era no capítulo 1. Um capítulo sobre join, agregação, pivot e window function é um capítulo inteiro sobre operações que fazem shuffle. É o único mecanismo de custo que unifica as sete receitas, e o capítulo o menciona de passagem em duas delas. *Conferi o padrão de partitions de shuffle no item 5 do Nível 5.*

**11.** **A ausência de `explain()`.** Argumente se um cookbook precisa de plano de execução, e decida.

R: O argumento contra é o gênero. Um cookbook entrega receitas testadas, e o leitor que vai ao livro com um problema quer a chamada certa, não o plano físico. Mostrar `explain()` custa páginas e exige explicar Catalyst antes.

O argumento a favor é específico deste capítulo. Duas das sete receitas terminam em uma decisão que só se toma olhando o plano. A escolha entre broadcast join e shuffle join é uma decisão sobre estratégia, e a única forma de saber qual o Spark escolheu é olhar o plano. A recomendação de `broadcast()` que o capítulo dá é, literalmente, um pedido para sobrescrever uma decisão que ninguém verificou. O mesmo vale para o custo do pivot que a NOTE menciona.

Meu veredito: para as cinco primeiras receitas, a ausência é defensável. Para as receitas de join e de agregação, ela é uma falha. Uma linha de `customer_cards_df.explain()` na receita 3 custaria dois centímetros de página e transformaria "broadcast joins are useful" em uma afirmação conferível.

**12.** **Dois DataFrames que nunca nascem.** Avalie a decisão de escrever cinco subseções sobre `df1` e `df2`.

R: Foi a decisão errada, e a receita mesma prova por quê.

O corpo da receita de joins usa quatro CSVs reais de cartão de crédito, com nomes de coluna reais e uma justificativa de negócio para cada tipo de join. É bom material. Ele acaba no passo 6.

A partir de "There's more", os cinco exemplos passam a usar `df1` e `df2`, com colunas `Name`, `Gender` e `Age`. Esses DataFrames não são criados em nenhuma linha do capítulo. Nenhum dos cinco exemplos roda como está impresso.

O custo pedagógico é maior que o de um `NameError`. Um right outer join sobre `df1` e `df2` não ensina nada, porque não há dado para inspecionar. Com as tabelas de cartão, um right outer join de `fraud_df` para `transactions_df` mostraria fraudes sem transação correspondente, o que é um problema de qualidade de dado de verdade. O capítulo tinha o exemplo na mão e trocou por dois nomes vazios.

Existe uma explicação provável. Os cinco exemplos vieram de outra fonte, com outro dataset, e foram colados sem adaptação. É o mesmo mecanismo da questão 5, agora em blocos de código em vez de prosa.

**13.** **O que o capítulo nunca faz.** Faça o inventário das ausências e ordene por gravidade para um capítulo de transformação de dado.

R: Conferi cada uma com `grep -inc` antes de listar.

1. **Nenhuma verificação de resultado.** Nenhuma receita conta linhas antes e depois, nenhuma compara com um valor esperado, nenhuma confere um caso conhecido. Se houvesse uma linha de `assert` ou um `count()` comparativo, os defeitos das Figuras 2.2 e 2.3 apareceriam na hora. É a ausência mais grave, porque é a única que teria pego todas as outras.
2. **Nenhum schema declarado nas receitas principais.** Um `StructType` aparece uma vez no capítulo, dentro de "There's more" da receita de aggregations. As três leituras de `netflix_titles.csv` não declaram nenhum, e é dessa omissão que sai o problema da coluna `date_added`.
3. **Shuffle sem definição e sem número.** Três menções, nenhuma explicação, zero ocorrência de `spark.sql.shuffle.partitions`.
4. **Zero menção a AQE, skew join e join hint.** `grep` devolve zero para "adaptive", "AQE", "skew" e "hint". Um capítulo de join escrito em 2024 que não menciona adaptive query execution está descrevendo o Spark de antes da versão 3.0.
5. **Semi join e anti join ausentes.** Cinco dos sete tipos de join do Spark aparecem. Os dois que faltam são exatamente os que respondem "quem não tem correspondência".
6. **`explain()` ausente.** Nenhum plano de execução no capítulo inteiro.
7. **Delta Lake ausente pela segunda vez.** `grep -inc "delta"` devolve zero, em um livro cujo subtítulo o nomeia. Nenhuma escrita, nenhuma tabela, nenhum `MERGE`.
8. **`cache()` e `persist()` ausentes.** A receita 5 encadeia três `withColumn` sobre o mesmo `df` e a receita 3 encadeia três joins. São os dois casos canônicos de reuso de resultado intermediário.

O padrão das oito: o capítulo ensina a escrever a chamada e nunca ensina a saber se a chamada fez o que se pediu. É coerente com a ausência de "How it works", que o capítulo 1 já tinha, e aqui custa mais caro, porque transformação errada não levanta exceção.

**14.** **Um cookbook sem "How it works", segunda temporada.** O capítulo 1 não tinha a seção e usava seções de diagnóstico no lugar. Compare com o capítulo 2 e diga o que mudou.

R: Piorou, e de um jeito específico.

O capítulo 1 não tinha "How it works" em nenhuma das sete receitas. Três delas compensavam com seções de diagnóstico, chamadas "Common issues faced while working with CSV data", "Common scenarios encountered while working with Parquet data" e "Common issues and considerations".

O capítulo 2 não tem "How it works" e não tem nenhuma seção de diagnóstico. Nenhuma receita se chama "Common issues", "Common scenarios" ou coisa parecida. As sete vão de "How to do it" direto para "There's more".

O substituto que apareceu foi a NOTE. São seis, e três delas fazem trabalho de "How it works": a de `groupBy`, a de pivot e a de aproximação. As três explicam mecanismo, e as três estão entre o melhor conteúdo do capítulo.

A conclusão que eu tiro é que o formato encontrou um lugar para a explicação e não lhe deu espaço. Uma NOTE de quatro linhas cabe um fato, não um mecanismo. É por isso que a NOTE de pivot diz que a operação é cara e não diz o que é shuffle. É pelo mesmo motivo que a NOTE de `row_number` aponta um problema de ordem e erra o remédio.

---

## Nível 5 — Além do capítulo (backlog, não notas)

Escrito contra o Spark 3.4.0, publicado em maio de 2024. Conferi todos os itens abaixo contra fonte primária em **3 de agosto de 2026**, quando a documentação corrente do Spark era a **4.2.0**. As URLs estão ao fim da seção.

**1.** **Os links de "See also".** O capítulo cita 11 URLs da documentação do Spark. Descubra quantas ainda funcionam hoje.

R: Testei as 11 com `curl` e o resultado é pior do que eu esperava.

As **quatro URLs versionadas devolvem 404**. São duas para `/docs/3.4.0/`, na receita de UDFs, e duas para `/docs/3.1.2/`, nas receitas de joins e de UDFs. O motivo é que a documentação das versões antigas saiu de `spark.apache.org`. Hoje `https://spark.apache.org/docs/3.4.0/index.html` é uma página de redirecionamento que manda o navegador para `https://archive.apache.org/dist/spark/docs/3.4.0/`, e os caminhos profundos não são redirecionados, eles simplesmente não existem mais. Conferi que `/docs/3.5.7/`, `/docs/4.0.0/`, `/docs/4.1.0/`, `/docs/4.1.3/` e `/docs/4.2.0/` continuam servidos, e que `/docs/3.4.0/sql-ref-functions.html`, `/docs/3.4.0/sql-programming-guide.html` e `/docs/3.1.2/api/python/index.html` devolvem 404.

As **sete URLs com `/docs/latest/` respondem**, e servem a 4.2.0. Duas delas apontam para `programming-guide.html`, que hoje é um stub com `<meta http-equiv="refresh">` para `rdd-programming-guide.html`. Ou seja, um leitor que segue o "See also" de duas receitas de DataFrame aterrissa no guia de RDD.

O resumo é que o capítulo tem um "See also" cujos links versionados morreram e cujos links não versionados mudaram de versão sem avisar. É o pior dos dois mundos e é o argumento mais forte contra citar `/latest/`.

**2.** **Join hints.** O capítulo mostra `broadcast()` e não menciona hint nenhum. Levante quais existem hoje.

R: A documentação de performance tuning da 4.2.0 lista **quatro join hints**: `BROADCAST`, com os aliases `BROADCASTJOIN` e `MAPJOIN`, `MERGE`, `SHUFFLE_HASH` e `SHUFFLE_REPLICATE_NL`. Eles se usam em SQL com `/*+ BROADCAST(r) */` e na API com `.hint("broadcast")`.

Quando hints diferentes são dados nos dois lados, a prioridade documentada é `BROADCAST > MERGE > SHUFFLE_HASH > SHUFFLE_REPLICATE_NL`.

A função `broadcast()` que o capítulo usa é a forma programática do primeiro hint, e é a única que ele mostra. Não há nada errado nisso. O que falta é dizer que ela é um hint entre quatro, que existe uma sintaxe SQL equivalente e que o Spark decide sozinho na ausência de hint.

**3.** **O limite de broadcast.** Descubra a partir de que tamanho o Spark promove um join a broadcast sem que ninguém peça.

R: A configuração é `spark.sql.autoBroadcastJoinThreshold`, com default de **10 MB**. Confirmei no `SQLConf.scala` da tag `v4.2.0`, que traz `.createWithDefaultString("10MB")`, e a página de performance tuning documenta o mesmo valor como `10485760`. Ela também diz que `-1` desliga o broadcast automático.

Isso muda a leitura da subseção do capítulo. A frase "if one of the datasets is small enough to fit in memory, we can use the broadcast join" descreve uma decisão que o Spark já toma sozinho abaixo de 10 MB. Escrever `broadcast()` só tem efeito acima desse limite, ou quando as estatísticas do planejador estão erradas.

Existe uma segunda configuração para o mesmo conceito dentro do AQE, `spark.sql.adaptive.autoBroadcastJoinThreshold`, que atua depois de o shuffle já ter acontecido, com estatísticas reais em vez de estimadas.

**4.** **AQE e skew join.** O capítulo não menciona nenhuma das duas coisas. Levante o estado atual.

R: Conferi contra o `SQLConf.scala` da tag `v4.2.0` e a página de performance tuning.

`spark.sql.adaptive.enabled` tem default **`true`**, e é `true` por padrão desde o Spark 3.2.0. Isso significa que o Spark reotimiza o plano em tempo de execução, com estatísticas reais de shuffle.

`spark.sql.adaptive.skewJoin.enabled` tem default **`true`**. A descrição no fonte é literal: com AQE ligado, o Spark "handles skew in shuffled join (sort-merge and shuffled hash) by splitting (and replicating if needed) skewed partitions". A página renderizada só cita sort-merge, e o fonte cita as duas estratégias.

Uma partition é considerada skewed quando satisfaz duas condições ao mesmo tempo. Ela precisa ser maior que `spark.sql.adaptive.skewJoin.skewedPartitionFactor` vezes a mediana, com default **5.0**. E precisa ser maior que `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes`, com default **256 MB**.

O que isso significa para o capítulo: o mecanismo que ele mais precisaria mencionar em uma receita de join já vem ligado e já resolve sozinho o caso mais comum. Um leitor que aprenda joins só por este capítulo não sabe que o Spark mexe no plano dele depois de submetido.

**5.** **O número de partitions de um shuffle.** O capítulo menciona shuffle três vezes. Descubra o número que ele nunca dá.

R: `spark.sql.shuffle.partitions` tem default **200**. Conferi na fonte definitiva, o `SQLConf.scala` da tag `v4.2.0`, linhas 1000 a 1005, onde a chave aparece com `.createWithDefault(200)`. A doc da chave é "The default number of partitions to use when shuffling data for joins or aggregations".

Esse é o número que governa todas as sete receitas do capítulo, porque join, `groupBy`, `pivot` e window function com `partitionBy` provocam shuffle. Um `groupBy("country")` sobre um CSV de alguns megabytes produz 200 tasks e 200 arquivos de shuffle, quase todos vazios.

Com AQE ligado, o valor deixou de ser tão fatal, porque `spark.sql.adaptive.coalescePartitions.enabled` junta partitions pequenas depois do shuffle. Mesmo assim, 200 é o ponto de partida, e é o primeiro parâmetro que se ajusta em qualquer job real.

**6.** **Os tipos de join que existem.** O capítulo cobre cinco. Levante a lista completa.

R: Fui ao `joinTypes.scala` da tag `v4.2.0`. O objeto `JoinType` aceita **17 strings**, que resolvem para **7 tipos**:

| Tipo | Strings aceitas |
|---|---|
| Inner | `inner` |
| Full outer | `outer`, `full`, `fullouter`, `full_outer` |
| Left outer | `leftouter`, `left`, `left_outer` |
| Right outer | `rightouter`, `right`, `right_outer` |
| Left semi | `leftsemi`, `left_semi`, `semi` |
| Left anti | `leftanti`, `left_anti`, `anti` |
| Cross | `cross` |

O `apply` normaliza a string com `toLowerCase` e remove os sublinhados antes de casar, o que é por que as três grafias de cada tipo funcionam. Uma string fora da lista lança `AnalysisException` com a classe de erro `UNSUPPORTED_JOIN_TYPE`.

Os dois tipos que o capítulo não cobre são `left_semi` e `left_anti`. O semi devolve as linhas da esquerda que têm correspondência, sem trazer as colunas da direita. O anti devolve as que não têm. Não existe right semi nem right anti, e a saída documentada é inverter os DataFrames.

**7.** **A assinatura de `pivot`.** Confira quantos parâmetros ela tem.

R: **Dois**, e o capítulo diz três.

O fonte de `python/pyspark/sql/group.py` na tag `v4.0.0` traz `def pivot(self, pivot_col: str, values: Optional[List["LiteralType"]] = None) -> "GroupedData":`. Fora o `self`, são `pivot_col` e `values`.

Não existe parâmetro de coluna de valores. A coluna sobre a qual se agrega entra na chamada seguinte, em `agg()` ou em `count()`.

O docstring explica por que o segundo parâmetro importa: "If `values` is not provided, Spark will eagerly compute the distinct values in `pivot_col` so it can determine the resulting schema of the transformation. To avoid any eager computations, provide an explicit list of values."

Ou seja, o parâmetro que o capítulo descreve como o terceiro e opcional é o segundo, e é exatamente o que evita o custo que a NOTE do capítulo avisa existir.

**8.** **RSD contra relativeError.** Confira os parâmetros de `approxQuantile` e de `approx_count_distinct`.

R: São duas funções com parâmetros de nome e de natureza diferentes, e o capítulo chama os dois de RSD.

**`approxQuantile(col, probabilities, relativeError)`.** O parâmetro se chama `relativeError`, e conferi isso tanto no docstring de `python/pyspark/sql/dataframe.py` quanto no código de `python/pyspark/sql/classic/dataframe.py`, os dois na tag `v4.0.0`. A descrição é "The relative target precision to achieve (>= 0). If set to zero, the exact quantiles are computed, which could be very expensive". O docstring também dá a garantia formal do algoritmo, que é de posto: `floor((p - err) * N) <= rank(x) <= ceil((p + err) * N)`. Não há parâmetro chamado RSD, e não há desvio padrão envolvido. A implementação é uma variação de Greenwald-Khanna.

**`approx_count_distinct(col, rsd=None)`.** Aqui o parâmetro se chama `rsd` mesmo, e a descrição é "The maximum allowed relative standard deviation (default = 0.05)". O docstring acrescenta uma ressalva prática: "If rsd < 0.01, it would be more efficient to use `count_distinct`".

O exemplo do capítulo passa `0.1` nas duas funções. Para `approx_count_distinct`, isso é o dobro do padrão, ou seja, um erro maior que o normal. Para `approxQuantile` sobre uma coluna `Score` de 1 a 5, uma tolerância de posto de 10% é grosseira.

Um detalhe extra. O capítulo escreve `approxCountDistinct()` na prosa e `approx_count_distinct` no código. O nome em camelCase ainda existe na 4.2.0, e conferi que ele emite `FutureWarning` com a mensagem "Deprecated in 2.1, use approx_count_distinct instead". Ou seja, a prosa cita um nome depreciado há mais de cinco anos.

**9.** **`row_number` é não determinística?** Confira a afirmação da NOTE contra o código do Spark.

R: O Spark **não marca `row_number` como não determinística**, e o docstring dela não diz nada sobre o assunto.

Fui ao `windowExpressions.scala` da tag `v4.2.0`. A `case class RowNumber` estende `RowNumberLike` e não sobrescreve nada relativo a determinismo. Um `grep` por "nondeterministic" no arquivo inteiro não devolve nada. No lado Python, o docstring de `row_number` em `python/pyspark/sql/functions/builtin.py` da tag `v4.2.0` diz apenas "Window function: returns a sequential number starting at 1 within a window partition", e não traz nenhuma nota de determinismo.

A documentação de sintaxe da 4.2.0 classifica `ROW_NUMBER` como ranking function, ao lado de `RANK`, `DENSE_RANK`, `PERCENT_RANK` e `NTILE`. Nenhuma delas é marcada de forma diferente das outras.

O que sobra de verdadeiro na NOTE: com empate no `ORDER BY`, o número que cada linha empatada recebe não é garantido entre execuções. Isso é uma propriedade da ordenação incompleta, não da função. A correção é tornar a chave de ordenação única, e não trocar de função. Mantenho a conclusão do item 6 do Nível 4.

**10.** **UDF Python contra função embutida.** Levante o que mudou no custo de uma UDF Python e o que a plataforma recomenda.

R: Duas coisas, e a primeira é recente.

**A otimização por Arrow para UDFs Python comuns virou padrão na 4.2.0.** Conferi a chave `spark.sql.execution.pythonUDF.arrow.enabled` em três tags do `SQLConf.scala`. Na `v4.0.0` e na `v4.1.0` ela tem `.createWithDefault(false)`. Na `v4.2.0` ela tem `.createWithDefault(true)`. A chave existe desde a 3.4.0 e a descrição é "Enable Arrow optimization in regular Python UDFs. This optimization can only be enabled when the given function takes at least one argument". A função `udf()` também aceita hoje um parâmetro `useArrow` para decidir caso a caso.

Isso significa que a UDF da receita 6, rodada em um Spark 4.2.0, é mais rápida do que seria em 2024 sem que o código mude. Continua mais lenta que a função embutida.

**A recomendação da plataforma é clara.** A documentação de UDFs do Databricks diz que "Built-in Apache Spark functions are optimized for distributed processing and offer better performance at scale". Ela orienta usar métodos do Spark para datasets grandes e para cargas regulares ou contínuas, incluindo ETL e streaming. As UDFs ficam reservadas para "logic that is difficult to express with built-in Apache Spark functions" e para consultas ad hoc, limpeza manual e análise exploratória. A mesma página registra que Pandas UDFs chegam a ser até 100 vezes mais rápidas que UDFs Python comuns, porque usam Arrow.

A UDF do capítulo faz concatenação de duas strings, o que a função embutida `concat` já faz. Ela cai exatamente do lado errado dessa recomendação.

**11.** **As higher-order functions embutidas.** O capítulo usa `transform` e não nomeia a categoria. Levante a lista atual.

R: A `transform` é a única HOF do capítulo, e ela tem dez irmãs. Levantei a lista em `python/pyspark/sql/functions/builtin.py` na tag `v4.2.0`, procurando as funções cujo segundo parâmetro é um `Callable`:

`transform`, `filter`, `exists`, `forall`, `aggregate`, `reduce`, `zip_with`, `transform_keys`, `transform_values`, `map_filter` e `map_zip_with`. São onze.

Duas coisas que o capítulo erra sobre a `transform`.

A NOTE afirma que a função passada "must take a single argument". O docstring diz o contrário. Ela aceita duas formas, a unária `(x: Column) -> Column` e a binária `(x: Column, i: Column) -> Column`, em que o segundo argumento é o índice do elemento com base zero.

O mesmo docstring traz uma restrição que o capítulo não menciona e que é a mais importante de todas: "Python `UserDefinedFunctions` are not supported". Ou seja, não dá para passar uma UDF Python para dentro de uma HOF. A lambda do capítulo funciona porque ela devolve uma expressão de coluna, não porque ela é uma UDF. Isso amarra este item ao item 2 do Nível 2.

A `transform` foi adicionada ao PySpark na versão **3.1.0**.

**12.** **`fillna` e os tipos de coluna.** Confira o que acontece com colunas de tipo incompatível.

R: Elas são ignoradas em silêncio.

O docstring de `fillna` em `python/pyspark/sql/dataframe.py`, tag `v4.0.0`, diz: "Columns specified in subset that do not have matching data types are ignored. For example, if `value` is a string, and subset contains a non-string column, then the non-string column is simply ignored."

Isso derruba a frase do capítulo, que diz que `df_flattened.fillna("N/A")` "vai substituir todos os valores nulos por N/A". No DataFrame da receita, `year` vem do JSON como número, então ela não é tocada. As colunas de texto são.

O docstring registra também a forma que o capítulo não mostra e que resolve o caso do passo 5 em uma linha. Se `value` for um dicionário, ele mapeia nome de coluna para valor de substituição, e o `subset` é ignorado. Isso permite `fillna({"category": "", "year": 9999})` no lugar dos cinco `withColumn` encadeados.

**13.** **As funções de null da API atual.** O capítulo cobre `dropna`, `fillna`, `when`, `otherwise`, `isNull` e `Imputer`. Levante o que mais existe.

R: Rodei `grep -inc "coalesce"` no capítulo e o resultado é **zero**. Essa é a ausência principal, porque `coalesce` é a forma canônica de tratar null em uma expressão.

A lista que levantei em `python/pyspark/sql/functions/builtin.py`, tag `v4.2.0`:

| Função | O que faz | Desde |
|---|---|---|
| `coalesce(*cols)` | Devolve o primeiro valor não nulo entre as colunas | antiga |
| `ifnull(a, b)` | Devolve `b` se `a` for null | 3.5.0 |
| `nvl(a, b)` | Igual a `ifnull` | 3.5.0 |
| `nvl2(a, b, c)` | Devolve `b` se `a` não for null, senão `c` | 3.5.0 |
| `nullif(a, b)` | Devolve null se `a` for igual a `b` | 3.5.0 |
| `nullifzero(col)` | Devolve null se a coluna for zero | 4.0.0 |
| `zeroifnull(col)` | Devolve zero se a coluna for null | 4.0.0 |
| `equal_null(a, b)` | Comparação null-safe, true se os dois forem null | 3.5.0 |
| `nanvl(a, b)` | Devolve `b` se `a` for NaN | antiga |
| `isnull` e `isnotnull` | Testes de nulidade como função | — |

Existe ainda a família `try_*`, com `try_add`, `try_divide`, `try_to_date`, `try_to_timestamp`, `try_element_at` e outras, que devolvem null em vez de lançar exceção. Ela virou importante com o modo ANSI ligado por padrão, porque é a forma documentada de recuperar o comportamento antigo em uma expressão específica.

O `when/otherwise` do capítulo funciona e é a ferramenta mais verbosa da lista. Cinco `withColumn` encadeados fazem o que `fillna` com dicionário faz em uma linha.

**14.** **`lead` e `lag` têm dois argumentos?** Confira a assinatura.

R: Têm três, e o capítulo diz dois.

O fonte da tag `v4.2.0` traz `def lead(col, offset: int = 1, default: Optional[Any] = None)` e `def lag(col, offset: int = 1, default: Optional[Any] = None)`.

Os dois primeiros são os que o capítulo descreve. O terceiro, `default`, é o valor devolvido quando não há linha suficiente na partition. O docstring é explícito: a função devolve "`default` if there is less than `offset` rows after the current row".

Isso corrige a frase do capítulo de que a função "returns null" quando o deslocamento ultrapassa a fronteira. Ela devolve null porque o padrão de `default` é `None`, e não porque não haja alternativa.

O detalhe importa justamente na Figura 2.3, em que as duas colunas são inteiramente null. Um `lead("date_added", 1, "sem próxima")` teria tornado visível na hora que aquelas partitions têm uma linha só.

**15.** **A plataforma que dá nome ao livro.** Levante as versões correntes do Databricks Runtime.

R: A versão mais nova é a **DBR 19**, de 15 de junho de 2026, com **Apache Spark 4.2.0**. A LTS mais nova é a **DBR 18**, de 10 de junho de 2026, com **Spark 4.1.0**, suportada até 10 de junho de 2029.

As demais LTS em suporte são 17.3 com Spark 4.0.0, até 22 de outubro de 2028. A 16.4 com Spark 3.5.2, até 9 de maio de 2028. A 15.4 com Spark 3.5.0, até 19 de agosto de 2027. A 14.3, até 1 de fevereiro de 2027. E a 13.3 com Spark 3.4.1, até **22 de agosto de 2026**.

A 13.3 é a única faixa em suporte com um Spark 3.4, ou seja, é a última runtime contemporânea deste livro. Ela sai de suporte em menos de três semanas contadas de hoje. A partir daí, nenhum Databricks Runtime suportado roda a versão contra a qual o capítulo foi escrito.

### Fontes consultadas

Todas acessadas em 3 de agosto de 2026. Todas são fonte primária: documentação oficial, código-fonte do projeto em tag fixa ou documentação do fornecedor.

Documentação do Apache Spark, versão 4.2.0:

- [Performance Tuning, com os defaults de shuffle.partitions, autoBroadcastJoinThreshold, AQE e skew join, e a lista de join hints](https://spark.apache.org/docs/4.2.0/sql-performance-tuning.html)
- [Window Functions, com as três cláusulas e as categorias ranking, analytic e aggregate](https://spark.apache.org/docs/4.2.0/sql-ref-syntax-qry-select-window.html)
- [Scalar UDFs](https://spark.apache.org/docs/4.2.0/sql-ref-functions-udf-scalar.html)

Código-fonte, sempre em tag fixa:

- [`SQLConf.scala` na tag v4.2.0, com `spark.sql.shuffle.partitions`, `autoBroadcastJoinThreshold`, `adaptive.skewJoin.*` e `pythonUDF.arrow.enabled`](https://raw.githubusercontent.com/apache/spark/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala)
- [`SQLConf.scala` na tag v4.0.0, para comparar o default de `pythonUDF.arrow.enabled`](https://raw.githubusercontent.com/apache/spark/v4.0.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala)
- [`joinTypes.scala` na tag v4.2.0, com as 17 strings de join aceitas](https://raw.githubusercontent.com/apache/spark/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/joinTypes.scala)
- [`windowExpressions.scala` na tag v4.2.0, com `RowNumber` e `UnspecifiedFrame`](https://raw.githubusercontent.com/apache/spark/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/windowExpressions.scala)
- [`WindowResolution.scala` na tag v4.2.0, com o frame padrão resolvido na análise](https://raw.githubusercontent.com/apache/spark/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/WindowResolution.scala)
- [`builtin.py` na tag v4.2.0, com as HOFs, as funções de null, `lead`, `lag`, `row_number` e `approx_count_distinct`](https://raw.githubusercontent.com/apache/spark/v4.2.0/python/pyspark/sql/functions/builtin.py)
- [`column.py` na tag v4.2.0, com o docstring de `getItem`](https://raw.githubusercontent.com/apache/spark/v4.2.0/python/pyspark/sql/column.py)
- [`dataframe.py` na tag v4.0.0, com os docstrings de `approxQuantile` e `fillna`](https://raw.githubusercontent.com/apache/spark/v4.0.0/python/pyspark/sql/dataframe.py)
- [`classic/dataframe.py` na tag v4.0.0, com o nome do parâmetro `relativeError`](https://raw.githubusercontent.com/apache/spark/v4.0.0/python/pyspark/sql/classic/dataframe.py)
- [`group.py` na tag v4.0.0, com a assinatura de `pivot`](https://raw.githubusercontent.com/apache/spark/v4.0.0/python/pyspark/sql/group.py)

Databricks:

- [User-defined functions, com a recomendação de preferir função embutida](https://docs.databricks.com/aws/en/udf/)
- [Databricks Runtime release notes, com as versões e as datas de fim de suporte](https://docs.databricks.com/aws/en/release-notes/runtime/)

Referência bibliográfica:

- [Registro do livro com data, ISBN e paginação](https://books.google.com.br/books/about/Data_Engineering_with_Databricks_Cookboo.html?id=OMkHEQAAQBAJ)

Defaults mudam entre releases, e um deles mudou entre a 4.1.0 e a 4.2.0 enquanto eu escrevia este guia. Preciso reconferir estes dados antes de usá-los em prova ou em decisão técnica.

---

## Nível 6 — Cross-book (contra *Beginning Apache Spark 3*, Capítulo 4)

Dois capítulos sobre os mesmos três mecanismos, agregação, join e window function. Chadha é receita e Luu é exposição. A comparação mais rica é sobre o que cada gênero consegue dizer sobre o mesmo mecanismo, e o que cada um deixa cair.

**1.** **Tipos de join.** Compare a cobertura dos dois e diga o que a diferença revela.

R:

| Tipo | Chadha, cap. 2 | Luu, cap. 4 |
|---|---|---|
| Inner | passo 3, com justificativa de negócio | Tabela 4-2, Listing 4-21, três formas de expressar a condição |
| Left outer | passo 4, com justificativa de negócio | Tabela 4-2, Listing 4-23, com saída impressa |
| Right outer | "There's more", sobre `df1` e `df2` | Tabela 4-2, Listing 4-24, com saída impressa |
| Full outer | "There's more", sobre `df1` e `df2` | Tabela 4-2, Listing 4-25, com saída impressa |
| Left semi | ausente | Tabela 4-2, Listing 4-27, com saída impressa |
| Left anti | ausente | Tabela 4-2, Listing 4-26, com saída impressa |
| Cross | "There's more", sem saída | Tabela 4-2, Listing 4-28, com as 24 linhas impressas |
| Diagrama de Venn | ausente | Figura 4-3 |

Luu cobre os sete e imprime a saída de todos. Chadha cobre cinco e não imprime saída de nenhum.

O que a diferença revela: Luu monta duas tabelas minúsculas, seis funcionários e quatro departamentos, e as reusa nas sete demonstrações. Isso permite que o leitor confira o resultado linha a linha. O funcionário "Kurt", sem departamento, e o departamento "Marketing", sem funcionário, são plantados de propósito para que cada tipo de join tenha o que mostrar. É desenho experimental.

Chadha usa dados reais e maiores, o que dá realismo às duas primeiras receitas, e depois abandona tudo e vai para `df1` e `df2`, que não existem. Ele tem o melhor motivo para o join e a pior demonstração dele.

**2.** **O custo do shuffle.** Compare o que cada um diz sobre por que um join é caro.

R: Chadha diz três frases, contadas com `grep`. Duas na subseção de broadcast join, uma na NOTE de pivot.

Luu tem uma seção inteira, "Overview of Join Implementation", com duas subseções e duas figuras. Ele nomeia as duas estratégias, shuffle hash join e broadcast hash join, e diz que o critério de escolha é o tamanho dos datasets. Ele explica o mecanismo em dois passos. O primeiro calcula o hash das colunas da expressão de join e move as linhas de mesmo hash para a mesma partition. O destino é decidido pelo módulo do hash pelo número de partitions. Depois combinar as linhas colocadas juntas. Ele compara os dois passos com o modelo MapReduce.

Ele também dá o único número dos dois livros. Um join de dois datasets de 100 GB move aproximadamente 200 GB pela rede, com serialização e desserialização no caminho. E fecha com a orientação prática: não dá para evitar shuffle hash join entre dois datasets grandes, dá para reduzir a frequência com que eles são juntados.

O broadcast hash join ele descreve como dois passos também, distribuir uma cópia do dataset pequeno para cada partition do grande e depois varrer o grande procurando correspondência.

Esta é a maior vantagem de Luu na comparação, e ela é grande. Chadha diz que broadcast join evita shuffle. Luu diz o que shuffle é, quanto custa e por que broadcast o evita. A frase do Chadha é correta e não é aprendível.

**3.** **O plano físico.** Luu imprime um. Chadha não. Avalie o peso disso.

R: Luu fecha a seção de join com `Listing 4-33`, que passa `broadcast(deptDF)` e imprime o plano:

```
== Physical Plan ==
*BroadcastHashJoin [dept_no#30L], [id#41L], Inner, BuildRight
:- LocalTableScan [first_name#29, dept_no#30L]
+- BroadcastExchange HashedRelationBroadcastMode(List(input[0, bigint, false]))
   +- LocalTableScan [id#41L, name#42]
```

Ele mostra também a forma SQL do hint, `/*+ MAPJOIN(departments) */`.

Chadha escreve `df1.join(broadcast(df2), ["Name", "Gender"], "inner")` e passa para a próxima subseção.

O peso é este: o plano é a única forma de saber se o `broadcast()` foi obedecido. O nome `BroadcastHashJoin` na primeira linha e o `BroadcastExchange` no ramo direito são a confirmação. Sem eles, `broadcast()` é uma sugestão cujo efeito ninguém verificou.

Luu também é o único dos dois que diz que o Spark decide sozinho na maior parte dos casos, "based on the statistics it has about datasets while reading them", e que o `broadcast()` é um hint para sobrescrever essa decisão. Chadha apresenta o hint como se fosse a única forma de obter broadcast join. *Conferi o limite automático de 10 MB no item 3 do Nível 5.*

**4.** **Pivoting.** Compare os dois tratamentos.

R: Chadha define pivot em quatro frases, dá um exemplo sem saída, erra o número de parâmetros e fecha com uma NOTE sobre custo.

Luu constrói um dataset de sete estudantes com nome, gênero, peso e ano de formatura, e o usa três vezes.

Na Listing 4-17 ele faz `groupBy("graduation_year").pivot("gender").avg("weight")` e imprime a tabela de duas linhas e duas colunas de gênero.

Na Listing 4-18 ele acrescenta três agregações e imprime a tabela com seis colunas, `F_min`, `F_max`, `F_avg`, `M_min`, `M_max` e `M_avg`. Depois enuncia a regra: "The number of columns added after the group columns in the result table is the product of the number of unique values of the pivot column and the number of aggregations".

Na Listing 4-19 ele passa `pivot("gender", Seq("M"))` e mostra que só as colunas de M aparecem. E dá o motivo, que é o que Chadha erra: "Specifying a list of distinct values for the pivot column speeds up the pivoting process. Otherwise, Spark spends some effort in figuring out a list of distinct values on its own".

Luu ensina o parâmetro que Chadha descreve errado, e ensina por que ele existe. Chadha tem uma coisa que Luu não tem, que é a NOTE ligando pivot a shuffle de forma explícita. Luu diz "spends some effort" e não nomeia o mecanismo.

**5.** **As três partes de uma window function.** Compare as definições.

R: Chadha, passo 3: uma window specification "specifies how to partition and order the data for computation". Duas partes.

Luu, seção "Window Functions": "The window specification defines three important components the window functions use". Ele nomeia partition by, order by e frame, e dedica um parágrafo inteiro ao terceiro, que ele chama de "more complicated" e que "defines the boundary of the window in the current row". Ele também diz que o intervalo pode ser especificado por índice de linha ou pelo valor da expressão de ordenação, e que `rowsBetween` e `rangeBetween` são as duas formas.

Luu é quem separa `rowsBetween` de `rangeBetween`. Chadha usa `rowsBetween` uma vez e nunca menciona `rangeBetween`. Conferi com `grep`: a palavra `rangeBetween` não aparece no capítulo do Chadha, e a palavra `unbounded` também não.

Luu também classifica as window functions em três tipos, ranking, analytic e aggregate, com as Tabelas 4-4 e 4-5. Chadha lista sete nomes em uma frase e não classifica nada.

Esta é a vantagem mais nítida de Luu no capítulo inteiro, e ela explica um defeito concreto do Chadha. O `running_total` da subseção "Nested window functions" depende de um frame que ninguém declarou. Quem leu Luu sabe que existe um frame padrão e vai procurá-lo. Quem leu só Chadha nem sabe que a pergunta existe.

**6.** **Agregações.** Compare o inventário e a forma.

R: Luu tem a Tabela 4-1, com 14 funções e uma linha de descrição cada: `count`, `countDistinct`, `approx_count_distinct`, `min`, `max`, `sum`, `sumDistinct`, `avg`, `skewness`, `kurtosis`, `variance`, `stddev`, `collect_list` e `collect_set`. Depois ele demonstra cada uma sobre o dataset de voos, com a saída impressa.

Chadha nomeia `count`, `sum`, `avg`, `min` e `max` em duas frases, e demonstra `count`, `max` e `min`.

Duas coisas que Luu ensina e Chadha não.

A primeira é a mais útil: `count(col)` não conta null, e `count(*)` conta. Ele prova isso com a Listing 4-3, um DataFrame de quatro filmes com nulos plantados, em que `count(actor_name)` devolve 2, `count(movie_title)` devolve 3, `count(produced_year)` devolve 4 e `count(1)` devolve 4. Isso é exatamente o comportamento que explica o `NumberOfReleases` da Figura 2.2 do Chadha, e Chadha não o menciona.

A segunda é `collect_list` contra `collect_set`, com a diferença de duplicata, e o uso de `size` para contar os elementos coletados.

Chadha tem uma coisa que Luu não tem, e é a NOTE sobre `GroupedData`. Luu diz que `groupBy` devolve `RelationalGroupedDataset`, que é o nome do lado Scala, e não avisa que isso significa que não dá para chamar `show()` direto.

**7.** **Aproximação.** Compare o que cada um explica.

R: Luu explica o problema antes da função. Ele diz que contar valores únicos exatos em um dataset muito grande é caro. Ele dá um caso de uso concreto, a contagem de visitantes únicos por segmento em publicidade online. E nomeia o problema pelo nome acadêmico, cardinality estimation. Depois nomeia o algoritmo, HyperLogLog, com link para a referência, e diz que o Spark implementa uma versão dele em `approx_count_distinct`.

Ele também dá o default. A assinatura que ele imprime é `approx_count_distinct(col, max_estimated_error=0.05)`, e o comentário no código diz "the default estimation error is 0.05 (5%)". Ele demonstra o ganho de tempo, contando a coluna `count` do dataset de voos: `countDistinct` devolve 2033 e `approx_count_distinct` devolve 2252.

Chadha dá três parágrafos de benefício genérico, não nomeia algoritmo nenhum, não dá default nenhum, e chama o parâmetro das duas funções de RSD.

Luu ganha por larga margem, e o número dele é o que falta no Chadha. Sem saber que o padrão é 0.05, o `rsd=0.1` do Chadha parece uma escolha e é o dobro do padrão. *Conferi a descrição atual dos dois parâmetros no item 8 do Nível 5.*

**8.** **UDF.** Compare o que cada um diz sobre o custo.

R: Luu abre a seção de UDF com o custo, antes de mostrar qualquer código. Ele diz que UDFs podem ser escritas em Scala, Java ou Python, e que é preciso conhecer a diferença de performance do Python. O executor é um processo JVM em Scala, então UDFs Scala e Java rodam nativamente no mesmo processo. Uma UDF Python não roda nativamente, o executor precisa lançar um processo Python separado, e há um custo alto de serialização de dado ida e volta para cada linha do dataset.

Ele também estrutura o assunto em três passos: escrever e testar a função, registrá-la com `udf`, e usá-la na DataFrame API ou em SQL. E avisa que o registro é ligeiramente diferente para uso em SQL.

Chadha tem os mesmos três passos, em ordem melhor detalhada, e não diz uma palavra sobre custo. A palavra performance não aparece na receita 6.

Chadha tem duas vantagens de detalhe. Ele mostra o tipo de retorno explícito como um passo próprio, o que Luu não faz porque em Scala o tipo é inferido da assinatura. E ele mostra `spark.udf.register` com os três argumentos e a temp view, o que é mais próximo do que um leitor Python vai escrever.

Uma ironia dos dois lados. Luu explica o custo e depois escolhe um exemplo que não tem substituto embutido, converter nota numérica em conceito por faixa. Chadha não explica o custo e escolhe um exemplo cujo substituto embutido ele mesmo usou trinta páginas antes, a `concat` da receita 1.

### Discordâncias

**1.** **Uma window specification tem duas ou três partes?** Chadha diz que ela especifica como particionar e ordenar. Luu diz que ela define três componentes, partition by, order by e frame. Arbitre.

R: **Luu está certo, e Chadha está incompleto de um jeito que tem consequência.**

A documentação de sintaxe da 4.2.0 traz o bloco de sintaxe com as três cláusulas: `PARTITION BY`, `ORDER BY` e `window_frame`. As três estão no mesmo nível gramatical, dentro do mesmo `OVER (...)`.

O ponto que nenhum dos dois fecha é que o frame nunca está ausente de verdade. Fui ao `WindowResolution.scala` na tag `v4.2.0` e li a regra que resolve um frame não especificado. Com `ORDER BY` presente, o Spark aplica `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`. Sem `ORDER BY`, aplica `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.

Isso arbitra a disputa a favor de Luu e ainda corrige uma imprecisão dele. Ele escreve que "the default frame of a window specification includes all the preceding rows and up to the current row". Isso vale só quando há `ORDER BY`, que é o caso dos exemplos dele. Sem `ORDER BY` o padrão é a partition inteira. Ele afirma a metade do padrão como se fosse o padrão todo.

A consequência prática cai em cima do Chadha. O `running_total` dele usa `count("show_id").over(Window.partitionBy("country").orderBy("release_year"))`. Como há `ORDER BY`, o frame padrão é `RANGE`, não `ROWS`. Em um frame de tipo `RANGE`, todas as linhas com o mesmo `release_year` compartilham o mesmo valor acumulado. O "running total" dele salta por grupo de ano, não linha a linha. Nem o autor nem o leitor têm como saber disso pelo capítulo.

**2.** **`row_number` é não determinística, e a saída é `rank`?** Chadha afirma as duas coisas na NOTE. Luu lista `row_number` na Tabela 4-4 sem nenhuma ressalva. Arbitre.

R: **Chadha está errado nas duas metades, e Luu está certo por não ter dito nada.**

Sobre a primeira metade. O Spark não marca `row_number` como não determinística. Conferi no `windowExpressions.scala` da tag `v4.2.0`, onde a `case class RowNumber` não traz nenhuma marcação nesse sentido, e um `grep` por "nondeterministic" no arquivo inteiro não devolve nada. O docstring da função em `builtin.py`, mesma tag, diz apenas que ela devolve um número sequencial começando em 1 dentro da window partition.

Sobre a segunda metade, que é a mais consequente. Trocar `row_number` por `rank` ou `dense_rank` não produz ordem estável. Essas funções atribuem o mesmo valor a linhas empatadas, então a ordem relativa entre elas continua indefinida. Elas param de inventar um desempate, o que é outra coisa.

Luu descreve as três de forma neutra na Tabela 4-4. "Returns the rank or order of rows within a frame based on some sorting order" para `rank`, "similar to rank, but leaves no gaps in the ranks when there are ties" para `dense_rank`, e "returns a sequential number starting with 1 with a frame" para `row_number`. Ele não afirma nada sobre determinismo, e isso é acerto por omissão, não por conhecimento.

O que sobra de verdadeiro na NOTE do Chadha é o sintoma. Com empate no `ORDER BY`, o resultado de `row_number` pode variar entre execuções. A causa é a ordenação incompleta e a correção é acrescentar colunas ao `ORDER BY` até a chave ficar única. Nenhum dos dois livros diz isso.

**3.** **Quantos parâmetros tem `pivot`?** Chadha diz três. Luu não faz nenhuma afirmação sobre a assinatura, mas usa a função de duas formas. Arbitre.

R: **Chadha está errado, e Luu resolve a disputa demonstrando em vez de afirmando.**

O fonte de `group.py` na tag `v4.0.0` traz `def pivot(self, pivot_col, values=None)`. Dois parâmetros.

Luu escreve `pivot("gender")` na Listing 4-17 e `pivot("gender", Seq("M"))` na Listing 4-19. Uma chamada com um argumento e outra com dois. Ele nunca diz "a função recebe N parâmetros" e mostra as duas assinaturas que existem.

O que Chadha chama de "the values column" não existe. A coluna sobre a qual se agrega entra em `agg()`, e o próprio exemplo dele, `groupBy("country").pivot("type").agg(count("show_id"))`, mostra isso.

Registro que esta discordância é do tipo mais fácil de arbitrar, porque o texto do Chadha é refutado pelo exemplo do Chadha. Não precisei de fonte externa para saber que havia problema, só para saber qual era o número certo.

**4.** **O terceiro argumento de `approxQuantile` é o RSD?** Chadha afirma que sim. Luu não trata de `approxQuantile`, mas trata do parâmetro de `approx_count_distinct` e o chama de estimation error, com default 0.05. Há disputa?

R: **Há, e Chadha está errado, mas a disputa é indireta porque Luu não fala da mesma função.**

Conferi as duas assinaturas. `approxQuantile(col, probabilities, relativeError)`, com o nome do parâmetro confirmado no `classic/dataframe.py` da tag `v4.0.0`, e a descrição "The relative target precision to achieve". `approx_count_distinct(col, rsd=None)`, com a descrição "The maximum allowed relative standard deviation (default = 0.05)".

São conceitos de natureza diferente. O de `approx_count_distinct` é estatístico, porque HyperLogLog produz uma estimativa aleatória e o desvio padrão relativo mede a dispersão dela. O de `approxQuantile` é determinístico, porque Greenwald-Khanna garante um limite de posto, expresso no próprio docstring como `floor((p - err) * N) <= rank(x) <= ceil((p + err) * N)`.

Luu não erra porque só fala da função em que o vocabulário estatístico se aplica. Ele chama o parâmetro de "estimation error" em vez de RSD, o que é menos preciso que o nome oficial e não é falso.

O ponto em que Luu ganha de verdade é outro. Ele dá o default, 0.05, e Chadha não dá nenhum. Sem o default, o `rsd=0.1` do capítulo do Chadha é um número sem escala.

**5.** **A string do full outer join.** Chadha usa `how='outer'`. Luu usa `"outer"` na Listing 4-25 e chama a seção de "Outer Joins (a.k.a. Full Outer Joins)". Eu ia acusar os dois de usar um nome não canônico, porque a forma que eu conhecia era `full_outer`. Há erro?

R: **Não há, e eu dissolvo o caso. A acusação era minha, não deles.**

Fui ao `joinTypes.scala` da tag `v4.2.0`. O objeto `JoinType` aceita quatro strings para o mesmo tipo: `outer`, `full`, `fullouter` e `full_outer`. O método `apply` normaliza com `toLowerCase` e remove os sublinhados antes de casar, então as quatro caem no mesmo `case "outer" | "full" | "fullouter" => FullOuter`.

As quatro são igualmente válidas. Não existe forma canônica privilegiada no código, e a lista `supported` que o Spark imprime em caso de erro traz as quatro juntas.

O mesmo vale para os outros tipos. `left_outer`, `leftouter` e `left` são a mesma coisa, e as três aparecem entre os dois livros. Chadha escreve `left_outer` e `right_outer`. Luu escreve `left_outer` e observa que "the join type can be either `left_outer` or `leftouter`".

Registro que Luu é o único dos dois que menciona a existência de sinônimos. Ele o faz em um comentário de uma linha dentro de um listing, e é a informação que teria evitado a minha acusação.

Uma coisa sobrou desta investigação e não é discordância. Nenhum dos dois livros diz que uma string inválida em `how=` lança `AnalysisException` com a classe `UNSUPPORTED_JOIN_TYPE`, e que a mensagem de erro lista todas as strings aceitas. Essa mensagem é a documentação mais confiável do assunto e ela só aparece quando algo dá errado.

---

## Termos-chave para definir antes de seguir adiante

Escreva uma definição de uma linha para cada, com suas próprias palavras, sem consultar:

transformation · action · lazy evaluation · `GroupedData` · `groupBy` · `agg` · alias · agregação · `count` · `max` · `min` · pivot · cross-tabulation · agregação aproximada · RSD · `relativeError` · HyperLogLog · quantil · join · join expression · join type · inner join · left outer join · right outer join · full outer join · cross join · produto cartesiano · semi join · anti join · broadcast join · shuffle hash join · shuffle · partition · skew · AQE · join hint · window function · window specification · `partitionBy` · `orderBy` · window frame · `rowsBetween` · `rangeBetween` · ranking function · analytic function · `row_number` · `rank` · `dense_rank` · `lead` · `lag` · running total · moving average · determinismo · UDF · `spark.udf.register` · higher-order function · `transform` · lambda · Arrow · null · `dropna` · `fillna` · `when` / `otherwise` · `coalesce` · `Imputer` · `dropDuplicates` · `selectExpr` · `isin` · `like` / `rlike` · `array_contains` · `getItem` / `getField`

### Minhas definições

Marquei em itálico os termos que o capítulo usa sem definir, ou que ele nem menciona. A conta programática está ao fim desta seção. O padrão do cookbook é o mesmo do capítulo 1 e mais severo aqui: ele nomeia funções com precisão e não define quase nenhum conceito de execução.

**transformation** — *Nomeada uma vez, na NOTE de `groupBy`, sem definição.* A operação que descreve um novo DataFrame a partir de outro, sem executar nada na hora.

**action** — *Nomeada uma vez, na mesma NOTE, sem definição.* A operação que dispara a execução. A NOTE dá três exemplos, `collect`, `show` e `write`.

**lazy evaluation** — *Nomeada como "lazily evaluated" na NOTE de `groupBy`, sem definição.* A técnica em que o Spark adia a execução até que uma action seja chamada.

**`GroupedData`** — O objeto que `groupBy` devolve, e que não é um DataFrame. É a única informação de tipo que o capítulo dá em todo o texto.

**`groupBy`** — A operação que agrupa o dado de uma coleção distribuída por uma ou mais chaves, permitindo agregar ou transformar o dado agrupado.

**`agg`** — O método que recebe uma ou mais expressões de agregação e as aplica ao grupo em uma passada só. É o que permite `alias`.

**alias** — *Usado seis vezes sem definição.* O método que dá nome à coluna de saída de uma expressão. Sem ele o nome é a própria expressão.

**agregação** — *Usada como palavra do título da receita e nunca definida.* A operação que reduz um conjunto de linhas a um valor por grupo.

**`count`** — A função que conta linhas do grupo. *O capítulo nunca diz que `count(col)` ignora null e que `count(*)` não ignora, e essa diferença explica a Figura 2.2. A definição vem do capítulo 4 do Luu, não deste.*

**`max` / `min`** — As funções que devolvem o maior e o menor valor do grupo. *O capítulo nunca diz que, sobre uma coluna de string, a comparação é lexicográfica, e a Figura 2.2 é o resultado disso.*

**pivot** — A operação que transforma os valores distintos de uma coluna em colunas do resultado, com uma agregação em cada célula. *O capítulo erra o número de parâmetros da função.*

**cross-tabulation** — *Nomeada uma vez, na prosa do pivot, sem definição.* A tabela de duas entradas em que linhas e colunas são categorias e as células são a agregação do cruzamento.

**agregação aproximada** — A agregação que troca exatidão por tempo, estimando o resultado em vez de calculá-lo. O capítulo dá três motivos para usá-la e nenhum algoritmo.

**RSD** — *Sigla expandida uma vez como relative standard deviation, conceito nunca explicado.* O desvio padrão relativo de uma estimativa. É o parâmetro de `approx_count_distinct`, com padrão 0.05. *O capítulo o atribui também ao `approxQuantile`, onde ele não se aplica.*

**`relativeError`** — *Não aparece no capítulo.* O nome real do terceiro parâmetro de `approxQuantile`. É uma tolerância de posto, não um desvio padrão.

**HyperLogLog** — *Não aparece no capítulo.* O algoritmo de estimativa de cardinalidade que o Spark implementa em `approx_count_distinct`. *Esta definição vem do capítulo 4 do Luu.*

**quantil** — *Usado no plural, na descrição de `approxQuantile`, sem definição.* O valor abaixo do qual cai uma fração dada das observações ordenadas.

**join** — *Usado o capítulo inteiro sem definição.* A operação que combina as colunas de dois DataFrames com base em uma condição.

**join expression** — Nomeada como `joinExpr` no passo 5 e descrita por exemplo. A condição booleana que decide quais pares de linhas entram no resultado.

**join type** — Nomeado como o argumento `how` e descrito por enumeração. A política que decide o que fazer com as linhas sem correspondência.

**inner join** — O join em que só entram as linhas com correspondência dos dois lados.

**left outer join** — O join em que as linhas da esquerda são mantidas, com ou sem correspondência à direita.

**right outer join** — O join em que as linhas da direita são mantidas, e as colunas da esquerda vêm null quando não há correspondência.

**full outer join** — O join em que as linhas dos dois lados são mantidas. A string usada no capítulo é `outer`.

**cross join** — O join que combina cada linha da esquerda com cada linha da direita. Usa o método `crossJoin()`, sem argumento de condição.

**produto cartesiano** — Nomeado uma vez como sinônimo de cross join, sem definição própria.

**semi join** — *Não aparece no capítulo.* O join que devolve as linhas da esquerda que têm correspondência, sem trazer as colunas da direita.

**anti join** — *Não aparece no capítulo.* O join que devolve as linhas da esquerda que não têm correspondência.

**broadcast join** — A estratégia em que o dataset pequeno é enviado a todos os worker nodes e o join é feito localmente, sem shuffle. O capítulo a apresenta como decisão do usuário, via `broadcast()`. *Ele não diz que o Spark a escolhe sozinho abaixo de um limite de tamanho.*

**shuffle hash join** — *Não aparece no capítulo.* A estratégia em que as linhas de mesmo hash de chave são movidas para a mesma partition antes da combinação. *Esta definição vem do capítulo 4 do Luu.*

**shuffle** — *Usado três vezes, sem definição.* A redistribuição de linhas entre máquinas para reagrupá-las por chave.

**partition** — *Usada como nome de argumento e como palavra da prosa, nunca definida.* A fatia do dado que uma task processa. Em window function, o conjunto de linhas com o mesmo valor de `partitionBy`.

**skew** — *Não aparece no capítulo.* A distribuição desigual de linhas entre partitions depois de um shuffle, em que uma partition fica muito maior que a mediana.

**AQE** — *Não aparece no capítulo.* Adaptive query execution, a reotimização do plano em tempo de execução com estatísticas reais de shuffle. Vem ligada por padrão.

**join hint** — *Não aparece no capítulo.* A instrução que força o planejador a usar uma estratégia de join. `broadcast()` é a forma programática de um dos quatro que existem.

**window function** — A função que calcula um valor sobre um conjunto de linhas e devolve um resultado por linha, sem colapsar o conjunto. *O capítulo nunca enuncia essa diferença em relação a `groupBy`.*

**window specification** — A especificação de como particionar e ordenar o dado antes de aplicar uma window function. *O capítulo nomeia duas partes e existem três.*

**`partitionBy`** — Neste capítulo, o método da classe `Window` que define o agrupamento lógico da window. *É o mesmo nome do método do `DataFrameWriter` do capítulo 1, que faz outra coisa, e nenhum dos dois capítulos marca a colisão.*

**`orderBy`** — O método que define a ordem das linhas, tanto no DataFrame quanto dentro da window.

**window frame** — O intervalo de linhas dentro da partition sobre o qual a função é calculada. *O capítulo o apresenta em "There's more" como recurso extra, e ele existe sempre, com um padrão que o capítulo não menciona.*

**`rowsBetween`** — O método que define o frame por índice de linha, contado a partir da linha atual. O único exemplo do capítulo é `rowsBetween(-2, 0)`.

**`rangeBetween`** — *Não aparece no capítulo.* O método que define o frame pelo valor da expressão de ordenação, e não pela contagem de linhas.

**ranking function** — *Não aparece no capítulo como categoria.* A família de window functions que atribui posição, como `row_number`, `rank` e `dense_rank`.

**analytic function** — *Não aparece no capítulo como categoria.* A família que acessa outras linhas do frame, como `lead`, `lag` e `cume_dist`.

**`row_number`** — A função que atribui um inteiro sequencial único a cada linha da partition, começando em 1. *A NOTE do capítulo a chama de não determinística e o Spark não a marca assim.*

**`rank` / `dense_rank`** — *Nomeadas duas vezes, nunca definidas.* Ambas atribuem a mesma posição a linhas empatadas. A primeira deixa buracos na sequência depois de um empate e a segunda não.

**`lead` / `lag`** — As funções que trazem o valor de uma coluna da próxima linha e da linha anterior da partition. *O capítulo diz que elas têm dois parâmetros e elas têm três.*

**running total** — *Nomeado uma vez, na subseção de nested window functions, sem definição.* O acumulado da linha atual para trás dentro da partition.

**moving average** — *Nomeada como "rolling average" na subseção de window frames, sem definição.* A média calculada sobre uma janela deslizante de linhas ao redor da atual.

**determinismo** — *Nomeado só pela negativa, na NOTE de `row_number`.* A propriedade de uma expressão devolver sempre o mesmo resultado para a mesma entrada.

**UDF** — Sigla expandida na abertura do capítulo como user-defined function. A função escrita pelo usuário e registrada no Spark para ser aplicada linha a linha. *O capítulo não menciona o custo dela.*

**`spark.udf.register`** — O método que grava um nome de UDF no catálogo da sessão, tornando-a chamável dentro de strings SQL.

**higher-order function** — *Não aparece no capítulo, nem por extenso nem como sigla.* A função embutida que recebe outra função como argumento e a aplica dentro de uma coluna de array ou de map. O capítulo usa uma, `transform`, e nunca nomeia a categoria.

**`transform`** — A higher-order function que aplica uma função a cada elemento de uma coluna de array e devolve uma nova coluna de array. *A NOTE do capítulo diz que a função recebida precisa ter um argumento, e existe também a forma binária com índice.*

**lambda** — *Usada sem definição, uma vez.* A função anônima Python que o capítulo passa a `transform`. Ela devolve uma expressão de coluna, não é uma UDF.

**Arrow** — *Não aparece no capítulo.* O formato colunar em memória que reduz o custo de serialização entre a JVM e o Python. Desde a 4.2.0 a otimização por Arrow em UDFs Python comuns vem ligada por padrão.

**null** — Definido na abertura da receita 7 como valor ausente ou desconhecido em um dataset, capaz de afetar a análise e a modelagem.

**`dropna`** — O método que remove as linhas que contêm null. *O capítulo o chama sem argumento e não diz que o padrão derruba a linha se qualquer coluna for nula.*

**`fillna`** — O método que substitui null por um valor. *O capítulo diz que ele substitui todos os nulos, e ele só toca nas colunas de tipo compatível.*

**`when` / `otherwise`** — O par que expressa uma condição e o caso padrão, coluna a coluna. É a ferramenta mais fina e mais verbosa das três de null do capítulo.

**`coalesce`** — *Não aparece no capítulo.* A função que devolve o primeiro valor não nulo entre as colunas passadas. É a forma canônica de tratar null dentro de uma expressão. *Esta definição vem do capítulo 4 do Luu, que a demonstra junto com `lit`.*

**`Imputer`** — O transformer de `ml.feature` que substitui nulos pela média, mediana ou moda da coluna. Padrão é a média.

**`dropDuplicates`** — O método que remove linhas duplicadas com base em uma lista de colunas.

**`selectExpr`** — O método que aceita expressões SQL em string, usado no capítulo para renomear três colunas de uma vez com `as`.

**`isin`** — O método que testa se o valor da coluna está em uma lista.

**`like` / `rlike`** — A primeira filtra por padrão com curinga `%`, a segunda por expressão regular.

**`array_contains`** — A função que testa se uma coluna de array contém um valor.

**`getItem` / `getField`** — *`getItem` é usado sem definição e `getField` não aparece.* O primeiro extrai por posição em array ou por chave em map, o segundo extrai campo nomeado de struct. *O capítulo usa `getItem` sobre um struct e chama a coluna de map.*

Conta dos itálicos: rodei os dois comandos de contagem sobre este arquivo. São 68 termos definidos nesta seção, e 45 deles trazem pelo menos uma marcação em itálico. Ou seja, dois terços dos termos-chave deste capítulo são coisas que ele usa sem definir, descreve errado, ou nem menciona.
