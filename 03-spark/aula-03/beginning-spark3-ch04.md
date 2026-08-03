# Guia de Leitura — *Beginning Apache Spark 3*, Capítulo 4: Spark SQL: Advanced

**Fonte:** Hien Luu, *Beginning Apache Spark 3: With DataFrame, Spark SQL, Structured Streaming, and Spark Machine Learning Library* (Apress, 2021), Capítulo 4, 48 páginas

**Escopo:** as perguntas dos Níveis 1 a 4 são respondíveis apenas com este capítulo, que cobre aggregations, joins, funções built-in, UDFs, funções analíticas avançadas, o Catalyst optimizer e o Project Tungsten. Algumas respostas do Nível 4 fecham o argumento com um fato verificado no Nível 5, e nesses casos elas dizem de onde o fato veio. O Nível 5 não é respondível pelo capítulo, e por isso cada item dele carrega URL e data de acesso. Este guia não tem Nível 6.

**Sobre as figuras:** o capítulo tem seis figuras, da 4-1 à 4-6. Abri as páginas 7, 8, 14, 21 e 43 do PDF e li as seis, porque a 4-4 e a 4-5 dividem a página 21. Abri também as páginas 9, 15, 22, 23, 38, 39, 41 e 45 para ler tabelas e listings como imagem, porque a extração de texto embaralha tabela. Onde uma resposta descreve figura ou tabela, ela veio da imagem, e não do texto extraído.

---

## Como usar este guia

1. Leia o capítulo uma vez de ponta a ponta, sem parar. Ele tem cinquenta e nove listings, sete tabelas e seis figuras.
2. Faça o Nível 1 de memória primeiro, depois volte ao texto para preencher as lacunas. Marque toda questão que você não conseguiu responder.
3. Faça o Nível 2 por escrito, em frases completas. Se você não consegue escrever, você não sabe.
4. O Nível 3 é para o terminal, não para o papel. Use o dataset que você quiser, contanto que não seja o `flight_summary` nem o `txData` do livro.
5. O Nível 4 cruza listing, tabela, figura e prosa. Este capítulo tem saída de código que não bate com o código, tabela que não bate com o listing e prosa que não bate com o próprio Summary. É ali que a leitura fica útil.

---

## Nível 1 — Memorização e definições

Respostas curtas e conferíveis.

**1.** O que o capítulo anuncia que cobre, e de quais dois componentes internos ele promete espiar os bastidores? *(Chapter intro)*

R: Ele diz cobrir as capacidades avançadas do módulo Spark SQL. A lista anunciada tem cinco itens. Agregação, join com múltiplos datasets, um conjunto grande de funções built-in, um jeito fácil de escrever função própria e um conjunto de funções analíticas avançadas. Os dois componentes internos são o Catalyst optimizer e o Tungsten engine.

**2.** Em quais três níveis o Spark pode agrupar linhas para agregação? *(Aggregations)*

R: Tratar o DataFrame inteiro como um grupo só. Dividir o DataFrame em vários grupos por uma ou mais colunas e aplicar uma ou mais agregações a cada grupo. Dividir o DataFrame em várias windows e calcular média móvel, soma cumulativa ou ranking. O capítulo acrescenta que, quando a window é baseada em tempo, a agregação pode ser por tumbling ou sliding window.

**3.** Como todas as agregações são feitas no Spark, segundo o capítulo, e para onde ele manda você buscar a lista completa? *(Aggregation Functions)*

R: Todas as agregações são feitas por funções. As funções de agregação são desenhadas para operar sobre um conjunto de linhas, seja o DataFrame inteiro, seja um subgrupo. O capítulo manda para a documentação Scala em `spark.apache.org`. Ele também avisa que, nas APIs Python, às vezes há lacunas na disponibilidade de algumas funções.

**4.** Liste as funções da Table 4-1 e o que cada uma devolve. *(Table 4-1)*

R:

| Operação | O que devolve |
|---|---|
| `count(col)` | O número de itens por grupo |
| `countDistinct(col)` | O número de itens únicos por grupo |
| `approx_count_distinct(col)` | O número aproximado de itens únicos por grupo |
| `min(col)` | O valor mínimo da coluna por grupo |
| `max(col)` | O valor máximo da coluna por grupo |
| `sum(col)` | A soma dos valores da coluna por grupo |
| `sumDistinct(col)` | A soma dos valores distintos da coluna por grupo |
| `avg(col)` | A média dos valores da coluna por grupo |
| `skewness(col)` | A assimetria da distribuição dos valores da coluna por grupo |
| `kurtosis(col)` | A curtose da distribuição dos valores da coluna por grupo |
| `variance(col)` | A variância não enviesada dos valores da coluna por grupo |
| `stddev(col)` | O desvio padrão dos valores da coluna por grupo |
| `collect_list(col)` | Uma coleção dos valores da coluna, que pode conter duplicatas |
| `collect_set(col)` | Uma coleção dos valores únicos da coluna |

São quatorze funções. O Nível 4 volta ao nome errado que a prosa dá às duas últimas.

**5.** Qual dataset os exemplos de agregação usam, de onde ele vem, quantas linhas ele tem, e qual é o schema dele? *(Listing 4-1)*

R: O `flight_summary`, derivado dos arquivos disponíveis no Kaggle, em `www.kaggle.com/usdot/flight-delays/data`. Ele traz os atrasos e cancelamentos de voos domésticos dos Estados Unidos em 2015. `flight_summary.count()` devolve 4693. Cada linha representa os voos de um `origin_airport` para um `dest_airport`.

O schema tem nove colunas: `origin_code`, `origin_airport`, `origin_city`, `origin_state`, `dest_code`, `dest_airport`, `dest_city`, `dest_state` e `count`. As oito primeiras são string. Só `count` é integer, e ela guarda o número de voos. Todas aparecem como `nullable = true`. A leitura usa `format("csv")`, com `header` e `inferSchema` true.

**6.** O que `count(col)` faz com valores null, e como se conta toda linha, inclusive as que têm null? *(count(col); Listing 4-3)*

R: `count(col)` não inclui o valor null na contagem. Para incluir, o capítulo diz que o nome da coluna deve ser trocado por `*`. Na Listing 4-3 o `badMoviesDF` tem quatro linhas, com null em algumas colunas. A saída traz `count(actor_name)` 2, `count(movie_title)` 3, `count(produced_year)` 4 e `count(1)` 4.

**7.** Na Listing 4-2, por que as duas contagens batem, e como o capítulo deixa a coluna de resultado legível? *(Listing 4-2)*

R: `count("origin_airport")` e `count("dest_airport")` devolvem 4693 os dois, porque contam linhas do mesmo DataFrame e nenhuma das duas colunas tem null. Para deixar a coluna legível o capítulo usa a função `as`, como em `count("dest_airport").as("dest_count")`. Ele lembra que é preciso chamar a action `show` para ver o resultado.

**8.** Quantos aeroportos únicos o `countDistinct` encontra em `flight_summary`? *(Listing 4-4)*

R: 322, tanto para `origin_airport` quanto para `dest_airport`. Na mesma linha o `count(1)` devolve 4693.

**9.** Por que `approx_count_distinct` existe, qual algoritmo ela usa, qual é o nome do problema, e qual é o erro de estimativa padrão? *(approx_count_distinct)*

R: Contar o número exato de itens únicos em um dataset muito grande é caro e demorado. O capítulo cita a publicidade online, com centenas de milhões de impressões por hora, como caso em que a contagem aproximada basta. O problema é conhecido como cardinality estimation. O algoritmo é o HyperLogLog. A assinatura mostrada é `approx_count_distinct(col, max_estimated_error=0.05)`, ou seja, o erro padrão é 0.05, cinco por cento.

**10.** Quais números a Listing 4-5 imprime para a coluna `count`, e o que o capítulo diz que acontece quando você reduz o erro? *(Listing 4-5)*

R: `count(count)` 4693, `count(DISTINCT count)` 2033 e `approx_count_distinct(count)` 2252. O capítulo diz que, quanto mais você reduz o erro de estimativa, mais tempo a função leva para terminar. O comentário que compara os tempos no laptop dele está cortado na margem e não dá para ler o número final.

**11.** O que `min`, `max`, `sum` e `sumDistinct` devolvem sobre a coluna `count`, e por que o último é menor? *(min(col), max(col); sum(col); sumDistinct(col))*

R: Mínimo 1 e máximo 13744. O comentário do capítulo é que parece haver um aeroporto muito movimentado, com 13744 voos chegando de um outro aeroporto. `sum("count")` devolve 5332914 e `sumDistinct("count")` devolve 3612257. O segundo é menor porque soma cada valor distinto uma vez só, e o dataset tem 4693 linhas para apenas 2033 valores distintos.

**12.** Qual hipótese a Listing 4-9 valida, e com quais duas expressões? *(avg(col); Listing 4-9)*

R: A hipótese é que `avg` só pega o total e divide pelo número de itens. O listing calcula `avg("count")` e `(sum("count") / count("count"))` lado a lado. Os dois devolvem 1136.3549968037503, o que confirma a hipótese.

**13.** Defina skewness como o capítulo define, e diga o que a Figure 4-1 mostra. *(skewness(col), kurtosis(col); Figure 4-1)*

R: Skewness mede a simetria da distribuição dos valores de um dataset. O valor pode ser positivo, zero, negativo ou indefinido. Numa distribuição normal o skew é 0, e as duas caudas ficam iguais. Skew positivo indica cauda direita mais longa ou mais gorda. Skew negativo indica o oposto.

Abri a página 7. A figura tem duas curvas vermelhas lado a lado, dentro de uma caixa só. Cada uma tem uma curva tracejada cinza sobreposta, como referência simétrica. A da esquerda tem a cauda longa à esquerda, com o rótulo `Negative Skew`. A da direita tem a cauda longa à direita, com o rótulo `Positive Skew`. A legenda credita a Wikipédia. Não há eixo numerado nem valor de skew impresso.

**14.** Defina kurtosis, e interprete os dois valores que a Listing 4-10 imprime. *(skewness(col), kurtosis(col); Listing 4-10)*

R: Kurtosis mede o formato da curva da distribuição, se ela é normal, achatada ou pontuda. Curtose positiva indica curva esbelta e pontuda. Curtose negativa indica curva gorda e achatada. A Listing 4-10 devolve skewness 2.682183800064101 e kurtosis 10.51726963017102. A leitura do capítulo é que a distribuição não é simétrica, com a cauda direita mais longa, e que a curva é pontuda.

**15.** Qual é a relação entre variância e desvio padrão, quantas implementações o Spark oferece, e quais quatro colunas a Listing 4-11 imprime? *(variance(col), stddev(col); Listing 4-11)*

R: As duas medem a dispersão dos dados, ou seja, a distância média dos valores em relação à média. Quando a variância é baixa, os valores estão perto da média. O desvio padrão é a raiz quadrada da variância. O Spark oferece duas implementações de cada uma. Uma usa amostragem e a outra usa a população inteira.

A Listing 4-11 imprime `var_samp(count)` 1879037.7571558713, `var_pop(count)` 1878637.3655604832, `stddev_samp(count)` 1370.779981308405 e `stddev_pop(count)` 1370.633928355957. Ou seja, `variance` é apelido de `var_samp` e `stddev` é apelido de `stddev_samp`.

**16.** O que exatamente a Figure 4-2 mostra, e a legenda concorda com a prosa? *(Figure 4-2)*

R: Abri a página 8. É um histograma de área com duas populações sobrepostas. O eixo x vai de `<0` até `230+` em passos de 10. O eixo y é `Number per bin`, de 0 a 400. Uma linha tracejada vertical marca `Average = 100`. A legenda traz `SD = 10` em vermelho e `SD = 50` em azul. A curva vermelha é estreita e alta, com pico perto de 350. A azul é larga e baixa, com pico perto de 70.

Achei que a legenda contradizia a prosa, porque o texto fala em variância 100 e 2500 e a figura fala em SD 10 e 50. Não contradiz. Desvio padrão 10 é variância 100, e desvio padrão 50 é variância 2500. A figura e o texto dizem a mesma coisa em unidades diferentes.

**17.** Quais são os dois passos da agregação com agrupamento, o que `groupBy` devolve, e por que isso é diferente das outras transformations? *(Aggregation with Grouping)*

R: O primeiro passo agrupa com a transformation `groupBy(col1, col2, ...)`. O segundo aplica uma ou mais funções de agregação a cada subgrupo. O capítulo diz que isso costuma ser feito sobre colunas categóricas, de baixa cardinalidade, como gênero, idade, nome de cidade e nome de país.

`groupBy` devolve uma instância da classe `RelationalGroupedDataset`, e não um DataFrame. O capítulo destaca isso porque as outras transformations devolvem DataFrame. As colunas do `groupBy` entram automaticamente na saída.

**18.** Quais seis funções de agregação padrão o `RelationalGroupedDataset` oferece, e qual delas é a exceção à regra numérica? *(Aggregation with Grouping)*

R: `avg(cols)`, `count()`, `mean(cols)`, `min(cols)`, `max(cols)` e `sum(cols)`. A exceção é `count()`. Todas as outras operam só sobre colunas numéricas.

**19.** O que as Listings 4-12 e 4-13 encontram, e o que a Listing 4-13 faz além de agrupar por duas colunas? *(Listings 4-12, 4-13)*

R: A Listing 4-12 agrupa por `origin_airport` e conta. Melbourne International Airport tem count 1, ou seja, voa para um aeroporto só. Kahului Airport tem 18. A Listing 4-13 agrupa por `origin_state` e `origin_city`. Além do agrupamento duplo, ela filtra as linhas para o estado "CA" e ordena com `orderBy`. San Francisco e Los Angeles aparecem no topo, com 80 cada.

**20.** O que a função `agg` recebe, o que as funções de agregação devolvem, e por que isso importa? *(Multiple Aggregations per Group)*

R: `agg` recebe uma ou mais column expressions, o que permite usar qualquer função de agregação, inclusive as da Table 4-1. As funções de agregação devolvem uma instância da classe `Column`. Isso importa porque, sendo `Column`, dá para aplicar em cima delas qualquer column expression, inclusive a função `as` para renomear. O capítulo diz que o nome padrão da coluna agregada é a própria expressão, o que fica longo e difícil de referenciar.

**21.** Qual é a segunda forma de expressar agregações em `agg`, quais valores ela aceita, e qual é a única vantagem dela? *(Listing 4-15)*

R: Um mapa de chave e valor baseado em string, como `"count" -> "min"`. A chave é o nome da coluna e o valor é o método de agregação, que pode ser `avg`, `max`, `min`, `sum` ou `count`. A vantagem é que o mapa pode ser gerado por programa. A desvantagem é que não há jeito fácil de renomear a coluna de resultado. O capítulo diz que, em job de ETL de produção ou em análise exploratória, a primeira forma é mais usada.

**22.** Qual é a diferença entre `collect_list` e `collect_set`, e o que a Listing 4-16 coleta? *(Collection Group Values)*

R: `collect_list` devolve uma coleção que pode conter valores duplicados. `collect_set` devolve uma coleção de valores únicos. A Listing 4-16 filtra `'count > 5500`, agrupa por `origin_state` e coleta as `dest_city` numa coluna `dest_cities`. Depois ela usa `withColumn` com `size('dest_cities)` para contar quantas cidades caíram em cada estado. AZ tem três, LA tem uma, MN tem duas, VA tem três e NV tem três.

**23.** Defina pivoting como o capítulo define, e dê a ordem dos passos. *(Aggregation with Pivoting)*

R: Pivoting resume os dados escolhendo uma das colunas categóricas e aplicando agregações sobre outras colunas. Com isso os valores categóricos são transpostos de linhas para colunas individuais. A ordem é esta. Agrupar por uma ou mais colunas. Pivotar sobre uma coluna. Aplicar uma ou mais agregações sobre uma ou mais colunas.

**24.** O que a Listing 4-17 calcula, e qual é a regra para o número de colunas na Listing 4-18? *(Listings 4-17, 4-18)*

R: A Listing 4-17 calcula o peso médio por gênero para cada ano de formatura, sobre um `studentsDF` de sete alunos. O código é `studentsDF.groupBy("graduation_year").pivot("gender").avg("weight")`. A saída tem três colunas, `graduation_year`, `F` e `M`. Em 2015, F é 108.0 e M é 190.0. Em 2016, F é 115.0 e M é 195.0.

A regra da Listing 4-18 é que o número de colunas acrescentadas depois das colunas de grupo é o produto do número de valores únicos da coluna pivotada pelo número de agregações. Ali são dois gêneros e três agregações, logo seis colunas, nomeadas `F_min`, `F_max`, `F_avg`, `M_min`, `M_max` e `M_avg`.

**25.** Como se restringe quais valores do pivot são agregados, e por que você faria isso? *(Listing 4-19)*

R: Passando uma sequência de valores como segundo argumento de `pivot`, como em `pivot("gender", Seq("M"))`. A razão é dupla. A coluna pivotada pode ter muitos valores distintos e você quer só alguns. E informar a lista acelera o processo, porque, sem ela, o Spark gasta esforço para descobrir sozinho a lista de valores distintos.

**26.** Por que o capítulo diz que você precisa de joins, e qual é o exemplo de fact e dimension que ele dá? *(Joins)*

R: Porque análise complexa exige trazer junto dados de vários datasets. O join combina as colunas de dois datasets, e o resultado tem colunas dos dois lados. O exemplo é uma empresa de e-commerce. Um dataset é o transacional, com quais clientes compraram quais produtos, chamado de fact table. O outro traz a informação de cada cliente, chamado de dimension table. Juntando os dois dá para saber quais produtos são mais populares em certos segmentos de cliente.

**27.** Quais duas informações um join exige, e o que cada uma determina? *(Join Expression and Join Types)*

R: A join expression e o join type. A join expression diz quais colunas de cada lado determinam quais linhas dos dois datasets entram no resultado. O join type determina o que deve ser incluído no dataset resultante.

**28.** Liste os sete join types da Table 4-2 com suas definições. *(Table 4-2)*

R:

| Tipo | Descrição |
|---|---|
| Inner join, também chamado equi-join | Devolve linhas dos dois datasets quando a join expression avalia como true |
| Left outer join | Devolve linhas do dataset da esquerda mesmo quando a join expression avalia como false |
| Right outer join | Devolve linhas do dataset da direita mesmo quando a join expression avalia como false |
| Outer join | Devolve linhas dos dois datasets mesmo quando a join expression avalia como false |
| Left anti-join | Devolve linhas só do dataset da esquerda quando a join expression avalia como false |
| Left semi-join | Devolve linhas só do dataset da esquerda quando a join expression avalia como true |
| Cross, também chamado Cartesian | Devolve linhas combinando cada linha da esquerda com cada linha da direita. O número de linhas é o produto do tamanho de cada dataset |

**29.** O que exatamente a Figure 4-3 mostra, e quais join types ela deixa de fora? *(Figure 4-3)*

R: Abri a página 14. São quatro diagramas de Venn em uma faixa, cada um com dois círculos rotulados A e B. Os rótulos embaixo são `Inner Join`, `Left Outer Join`, `Right Outer Join` e `Full Outer Join`. No Inner só a interseção está preenchida, em laranja. No Left Outer o círculo A inteiro está amarelo e a interseção laranja. No Right Outer é o espelho, com B amarelo. No Full Outer A é amarelo, B é verde e a interseção é laranja. Ficaram de fora o left anti-join, o left semi-join e o cross join.

**30.** Descreva os dois DataFrames da Listing 4-20, e como o departamento do Kurt é escrito. *(Listing 4-20)*

R: `employeeDF` tem seis linhas, com `first_name` e `dept_no`: John 31, Jeff 33, Mary 33, Mandy 34, Julie 34 e Kurt. O `dept_no` de Kurt é escrito como `null.asInstanceOf[Int]`. `deptDF` tem quatro linhas, com `id` e `name`: 31 Sales, 33 Engineering, 34 Finance e 35 Marketing. Os dois são registrados como views, `employees` e `departments`.

**31.** Por que o inner join é o mais fácil de usar, o que limita o tamanho da saída dele, e o tipo é obrigatório? *(Inner Joins)*

R: É o tipo mais usado, com a join expression comparando igualdade de colunas dos dois lados. Só as linhas em que a expressão é true entram no resultado. Com comparação de igualdade, o capítulo diz que o número de linhas do resultado só pode ser tão grande quanto o tamanho do menor dataset. O inner join é o tipo padrão no Spark SQL, então informá-lo na transformation `join` é opcional.

**32.** Dê as três formas de expressar uma join expression da Listing 4-22, e quando a forma curta falha. *(Listing 4-22)*

R:

```scala
employeeDF.join(deptDF, 'dept_no === 'id).show
employeeDF.join(deptDF, employeeDF.col("dept_no") === deptDF.col("id")).show
employeeDF.join(deptDF).where('dept_no === 'id).show
```

A forma curta só funciona quando os nomes de coluna são únicos. Se não forem, é obrigatório dizer de qual DataFrame cada coluna vem, usando a função `col`. O capítulo também diz que a join expression é um predicado booleano. Ela pode ser tão simples quanto comparar duas colunas, ou encadear várias comparações lógicas de pares de colunas.

**33.** Compare o left outer, o right outer e o full outer join, e dê a saída das Listings 4-23, 4-24 e 4-25. *(Left Outer Joins; Right Outer Joins; Outer Joins)*

R: O left outer acrescenta ao inner join todas as linhas da esquerda em que a expressão é false, preenchendo NULL nas colunas da direita. O right outer aplica o mesmo tratamento ao lado direito. O full outer é o efeito de combinar os dois.

A Listing 4-23 imprime seis linhas: as cinco do inner join mais `Kurt | 0 | null | null`. A Listing 4-24 imprime seis: as cinco do inner join mais `null | null | 35 | Marketing`. A Listing 4-25 imprime sete, com as duas linhas extras juntas. O tipo do left outer aceita tanto `"left_outer"` quanto `"leftouter"`.

A leitura que o capítulo faz do outer join é que ele mostra quatro coisas ao mesmo tempo. Qual departamento cada empregado tem, quais departamentos têm empregado, quais empregados não têm departamento e quais departamentos não têm empregado.

**34.** Como o left anti-join e o left semi-join diferem um do outro e do inner join? *(Left Anti-Joins; Left Semi-Joins)*

R: Os dois devolvem só as colunas do dataset da esquerda. O anti-join guarda as linhas da esquerda sem correspondência à direita, e a Listing 4-26 devolve uma linha só, `Kurt | 0`. O semi-join guarda as linhas com correspondência, e a Listing 4-27 devolve as cinco de John, Jeff, Mary, Mandy e Julie.

O capítulo descreve o semi-join como parecido com o inner join, exceto por não trazer as colunas da direita. E descreve o anti-join como o oposto do semi-join. Ele também diz que o tipo right anti-join não existe, e que basta trocar os datasets de posição.

**35.** Por que o cross join é perigoso, como ele é invocado, e quais dois números o capítulo dá? *(Cross)*

R: Ele junta cada linha da esquerda com cada linha da direita, então o tamanho do resultado é o produto dos dois tamanhos. O capítulo diz que, se cada dataset tem 1024 linhas, o resultado passa de um milhão de linhas. Por isso o caminho de uso é uma transformation dedicada da classe DataFrame, `crossJoin`, e não uma string de join type. Na Listing 4-28, `employeeDF.crossJoin(deptDF).count` devolve 24, ou seja, seis vezes quatro.

**36.** O que acontece quando dois DataFrames unidos por join têm um nome de coluna em comum, e quais são as três formas de lidar com isso? *(Dealing with Duplicate Column Names; Listings 4-29 to 4-32)*

R: O DataFrame resultante fica com várias colunas de mesmo nome. Na Listing 4-29 o `deptDF2` ganha uma coluna `dept_no` por `withColumn("dept_no", 'id)`, e o `dupNameDF` sai com cinco colunas, duas delas chamadas `dept_no`. Projetar essa coluna dispara `org.apache.spark.sql.AnalysisException: Reference 'dept_no' is ambiguous`.

As três saídas são estas. Prefixar a coluna com o DataFrame original, como em `dupNameDF.select(deptDF2.col("dept_no"))`. Renomear antes do join com `withColumnRenamed`, que o capítulo deixa como exercício. E usar a versão de `join` que recebe o nome da coluna de junção, como `employeeDF.join(deptDF2, "dept_no")`, que remove a duplicata sozinha. Essa terceira quebra no self-join, e ali é preciso renomear.

**37.** Quais duas join strategies o capítulo nomeia, qual é o critério, e quais são os dois passos do shuffle hash join? *(Overview of Join Implementation; Shuffle Hash Join)*

R: Shuffle hash join e broadcast join. O critério principal é o tamanho dos dois datasets. Com os dois grandes, o Spark usa shuffle hash join. Quando um deles cabe na memória do executor, ele usa broadcast join. O capítulo abre a seção dizendo que o join é uma das operações mais complexas e caras do Spark.

O primeiro passo do shuffle hash join calcula o hash das colunas da join expression para cada linha dos dois datasets. Depois ele move para a mesma partition as linhas de mesmo hash. Para escolher a partition, o Spark faz o módulo do valor de hash pelo número de partitions. O segundo passo combina as colunas das linhas que têm o mesmo hash. O capítulo compara os dois passos com os do modelo MapReduce.

**38.** O que a Figure 4-4 mostra, e qual número de custo o texto ao redor dá? *(Figure 4-4)*

R: Abri a página 21. A figura tem duas caixas no topo, `Data set #1` em azul e `Data set #2` em verde. Embaixo há quatro caixas, `Partition #1` a `Partition #4`. De cada dataset saem quatro setas que cruzam até as quatro partitions, com a palavra `Shuffle` de cada lado. Ou seja, os dois datasets são embaralhados.

O texto diz que, num join de dois datasets de 100 GB cada, cerca de 200 GB de dados são movidos. Ele lembra que o dado passa por serialização e desserialização ao atravessar a rede.

**39.** Quais são os dois passos do broadcast hash join, e o que a Figure 4-5 mostra? *(Broadcast Hash Join; Figure 4-5)*

R: O primeiro passo transmite uma cópia do dataset menor para cada partition do dataset maior. O segundo passo percorre cada linha do maior e procura as linhas correspondentes no menor.

Abri a página 21. A Figure 4-5 mostra uma caixa amarela `Small Data Set` à esquerda. Dela saem três setas rotuladas `Broadcast`, apontando para dentro de uma caixa azul `Large Data Set`, que contém `Partition #1`, `#2` e `#3`. Cada partition recebe sua própria cópia amarela do small data set. O ponto é que só o menor sofre shuffle.

**40.** Como se força um broadcast hash join, na DataFrame API e em SQL, e o que o physical plan mostra? *(Listing 4-33)*

R: Na API, importando `org.apache.spark.sql.functions.broadcast` e embrulhando o DataFrame menor, como em `employeeDF.join(broadcast(deptDF), ...)`. Em SQL, com o hint `MAPJOIN(departments)` dentro de um comentário, logo depois do `select`. O plano físico traz `*BroadcastHashJoin [dept_no#30L], [id#41L], Inner, BuildRight`, com um `BroadcastExchange` e dois `LocalTableScan`. O capítulo diz que, na maior parte dos casos, o Spark SQL decide sozinho, com base nas estatísticas dos datasets.

**41.** O que é uma função, quantas funções built-in existem, e qual é a única característica comum entre elas? *(Functions; Working with Built-in Functions)*

R: Funções são métodos aplicados a colunas. Elas transformam o valor de coluna de cada linha, e não a linha inteira, que é o que as APIs de DataFrame fazem. O capítulo conta mais de 200 funções built-in e diz que aproximadamente 30 novas chegaram no Spark 3.0.

A característica comum é que todas recebem uma ou mais colunas da mesma linha e devolvem uma única coluna. A razão para preferi-las é que elas geram código otimizado para execução em runtime. O capítulo pede para esgotá-las antes de escrever função própria. Elas podem ser usadas em `select`, `filter` e `groupBy`.

**42.** Liste as oito categorias da Table 4-3 e uma função de cada uma. *(Table 4-3)*

R: Abri a página 23 para ler a tabela como imagem, porque a extração embaralha. As oito categorias são estas.

| Categoria | Exemplo listado |
|---|---|
| Date time | `unix_timestamp` |
| String | `levenshtein` |
| Math | `factorial` |
| Cryptography | `md5` |
| Aggregation | `corr` |
| Collection | `explode` |
| Window | `row_number` |
| Misc. | `monotonically_increasing_id` |

A categoria Window traz `dense_rank`, `lag`, `lead`, `ntile`, `rank` e `row_number`. A Misc. traz `coalesce`, `isNan`, `isnull`, `isNotNull`, `monotonically_increasing_id`, `lit` e `when`. A tabela tem vários nomes de função escritos errado, e eles estão no item 7 do Nível 4.

**43.** Em quais três categorias as funções de data e hora se dividem, qual é o formato de data padrão, e qual sintaxe o Spark diz usar internamente? *(Working with Date Time Functions)*

R: Converter data ou timestamp de um formato para outro. Fazer cálculo de data e hora. E extrair valores específicos de uma data ou timestamp, como ano, mês e dia da semana. O formato padrão é `yyyy-MM-dd HH:mm:ss`. O capítulo diz que internamente o Spark usa a sintaxe de padrão de data do Java, documentada na página do `simpleDateFormat` da Oracle. Se o formato da sua coluna for diferente, o padrão tem de ser passado à função.

**44.** Quais funções de conversão as Listings 4-34 e 4-35 usam, e quais colunas precisam de um padrão customizado? *(Listings 4-34, 4-35)*

R: A Listing 4-34 usa `to_date`, `to_timestamp` e `unix_timestamp`. As duas últimas colunas do `testDF` não seguem o formato padrão. A `date_str` vale `"01-01-2018"` e precisa de `"MM-dd-yyyy"`. A `ts_str` vale `"12-05-2017 45:50"` e precisa de `"MM-dd-yyyy mm:ss"`. O schema resultante traz `date1` como date, `ts1` e `ts2` como timestamp e `unix_ts` como long.

A Listing 4-35 faz o caminho de volta, com `date_format('date1, "dd-MM-YYYY")` e `from_unixtime('unix_ts, "dd-MM-YYYY HH:mm:ss")`. A saída traz `01-01-2018` e `01-01-2018 15:04:58`.

**45.** Quais funções de cálculo e de extração de data e hora as Listings 4-36 e 4-37 mostram? *(Listings 4-36, 4-37)*

R: A Listing 4-36 usa `datediff`, para a diferença em dias, `months_between`, para a diferença em meses com casas decimais, `last_day`, para o último dia do mês, `date_add`, `date_sub` e `next_day('new_year, "Mon")`, que devolve `2018-01-08`.

A Listing 4-37 extrai nove campos de `"2018-02-14 05:35:55"`: `year` 2018, `quarter` 1, `month` 2, `weekofyear` 7, `dayofmonth` 14, `dayofyear` 45, `hour` 5, `minute` 35 e `second` 55. O capítulo justifica esse grupo com o caso de agrupar transações de ações por trimestre, mês ou semana.

**46.** Em quais dois grupos as funções de string se dividem, e quais delas a Listing 4-38 demonstra? *(Working with String Functions; Listing 4-38)*

R: O primeiro grupo transforma uma string. O segundo aplica expressão regular. Trimming remove espaços de um lado ou dos dois. Padding acrescenta caracteres de um lado ou do outro.

A Listing 4-38 usa `trim`, `ltrim`, `rtrim`, `lpad`, `rpad`, `concat_ws`, `lower`, `upper`, `initcap`, `reverse` e `translate`. Os pads levam `"Spark"` a `---Spark` e `Spark===`, os dois com comprimento 8. O `concat_ws(" ", ...)` monta `Spark is awesome`, e o `reverse` devolve `emosewa si krapS`. O `translate('subject, "ar", "oc")` transforma `Spark` em `Spock`.

**47.** Quais são os parâmetros de `regexp_extract` e de `regexp_replace`, e o que `regexp_extract` devolve quando não há correspondência? *(Listings 4-39, 4-40)*

R: `regexp_extract` recebe uma coluna string, um padrão e um índice de grupo. O índice começa em 0 e serve para identificar qual das várias correspondências você quer. Sem correspondência, ela devolve string vazia. `regexp_replace` recebe uma coluna string, um padrão e o valor de substituição. O capítulo diz que o Spark usa a biblioteca de expressões regulares do Java por baixo. No exemplo, o padrão `"[a-z]*o[xw]"` extrai `fox` e substitui `fox` e `crow` por `animal`.

**48.** Que tipo de arredondamento `round` faz, quais são as duas variações dela, e o que a Listing 4-41 imprime? *(Working with Math Functions)*

R: Arredondamento half-up, com base na escala informada, que define o número de casas decimais. A primeira variação recebe a coluna e a escala. A segunda recebe só a coluna, e chama a primeira com escala 0. A saída traz `pie0` 3.0, `pie1` 3.1, `pie2` 3.14, `gpa` 4.0 e `year` 2018. O comentário destaca que, por ser half-up, o `gpa` 3.5 sobe para 4.0.

**49.** Quais dois tipos específicos de collection functions o capítulo cobre, e quais funções cada um usa? *(Working with Collection Functions; Listings 4-42, 4-43)*

R: As de array e as de JSON. A Listing 4-42 usa `size`, para o tamanho, `sort_array`, para ordenar, `array_contains`, para checar existência de valor, e `explode`, que cria uma linha nova para cada elemento do array. O `tasksDF` tem uma linha com três tarefas, e depois do `explode` saem três linhas.

A Listing 4-43 usa `from_json` e `to_json`. `from_json` precisa de um schema declarado, montado no exemplo como `new StructType().add("day", StringType).add("tasks", ArrayType(StringType))`. Depois da conversão a coluna vira struct, e a leitura de campo usa `getItem`, encadeável, como em `'todos.getItem("tasks").getItem(0)`. O capítulo motiva o assunto com o payload de mensagem Kafka em JSON.

**50.** Qual problema `monotonically_increasing_id` resolve, que tipo ela gera, e qual é a parte central do algoritmo dela? *(Working with Miscellaneous Functions)*

R: Gerar IDs únicos e crescentes, mas não consecutivos, para cada linha de um dataset espalhado por muitas partitions. Ele gera inteiros de 64 bits. A parte central do algoritmo é colocar o partition ID nos 31 bits superiores do ID gerado. A Listing 4-44 usa `spark.range(1,11,1,5)`, com cinco partitions. A partition 0 começa em 0, a partition 1 em 8589934592 e a partition 2 em 17179869184.

**51.** O que `when`, `otherwise`, `coalesce` e `lit` fazem? *(Working with Miscellaneous Functions; Listings 4-45, 4-46)*

R: `when` substitui o `switch` que existe na maior parte das linguagens de alto nível, aplicado ao valor de uma coluna. As chamadas são encadeadas, como `when('id === 1, "Mon").when('id === 2, "Tue")`. O caso padrão vai em `otherwise`, também da classe `Column`.

`coalesce` recebe um ou mais valores de coluna e devolve o primeiro que não é null. O capítulo diz que a ideia vem do mundo SQL. Cada argumento precisa ser do tipo `Column`. Para usar um literal existe `lit`, que devolve uma instância de `Column` que embrulha o valor. O exemplo é `coalesce('actor_name, lit("no_name"))`.

**52.** Em quais linguagens uma UDF pode ser escrita, quais são os três passos, e qual é o problema de performance com Python? *(Working with User-Defined Functions (UDFs); Listing 4-47)*

R: Em Python, Java ou Scala. Os três passos são escrever e testar a função, registrá-la no Spark passando nome e assinatura para a função `udf`, e usá-la no código DataFrame ou em SQL. O registro para SQL é diferente: `spark.sqlContext.udf.register("letterGrade", letterGrade(_: Int): String)`. A UDF precisa ser registrada para que o Spark saiba enviá-la aos executors.

O problema do Python é que os executors são processos JVM. Eles executam UDF de Scala ou Java no mesmo processo. Uma UDF em Python obriga o executor a lançar um processo Python separado. Além disso há um custo alto de serializar dado de ida e volta para cada linha do dataset.

**53.** Quais são as três capacidades de análise avançada que o capítulo anuncia? *(Advanced Analytics Functions)*

R: Agregações multidimensionais, úteis para análise hierárquica, com subtotais e totais gerais. Agregações baseadas em janelas de tempo, úteis para série temporal, como transações ou valores de sensor IoT. E agregações dentro de um agrupamento lógico de linhas chamado window, que permite média móvel, soma cumulativa e ranking de cada linha.

**54.** Como um rollup trata a ordem das colunas dele, como o total aparece, e qual é o truque de ordenação da Listing 4-48? *(Rollups; Listing 4-48)*

R: A ordem das colunas é tratada como hierarquia de agrupamento. O rollup a respeita e sempre começa pela primeira coluna. O total aparece na linha em que todos os valores de coluna são null.

O truque é ordenar com `'origin_state.asc_nulls_last` e `'origin_city.asc_nulls_last`, para o Spark SQL colocar os nulls por último. No `twoStatesSummary` de treze linhas os subtotais são CA 43 e NY 52, e o total geral é 95. As linhas de detalhe são San Diego 22, San Francisco 21, Albany 17, Elmira 15 e New York 20.

**55.** Como um cube difere de um rollup, e o que a Listing 4-49 acrescenta? *(Cubes)*

R: O cube faz as agregações sobre todas as combinações das colunas de agrupamento. O resultado inclui o que o rollup dá mais outras combinações. A Listing 4-49 acrescenta cinco linhas com `origin_state` null e `origin_city` preenchido, uma por cidade, com os mesmos totais das linhas de detalhe. A forma de uso é igual à do `rollup`.

**56.** Em qual versão do Spark as agregações por time window foram introduzidas, o que elas exigem, e quais dois conceitos elas implementam? *(Aggregation with Time Windows)*

R: No Spark 2.0. Todas as versões exigem uma coluna do tipo timestamp e um comprimento de window, em segundos, minutos, horas, dias ou semanas. O comprimento representa uma janela com hora de início e de fim, e determina em qual bucket cada ponto cai. Outra versão recebe também o tamanho do deslizamento.

Os dois conceitos implementados são tumbling window e sliding window. O capítulo diz que eles vêm do processamento de eventos e que são descritos em detalhe no Capítulo 6.

**57.** O que a Listing 4-50 calcula, qual é o schema do resultado, e em que a Listing 4-51 difere? *(Listings 4-50, 4-51)*

R: A 4-50 calcula a média semanal de fechamento da ação da Apple, sobre um ano de dados do Yahoo! Finance. O schema resultante tem uma coluna `window`, struct com `start` e `end`, ambos timestamp, mais `weekly_avg`, double. O comentário diz que o start é inclusivo e o end é exclusivo. As cinco primeiras semanas vão de 116.08 a 123.12.

A 4-50 usa `window('Date, "1 week")`, uma tumbling window sem sobreposição, então cada transação é usada uma vez só. A 4-51 usa `window('Date, "4 week", "1 week")`, uma sliding window de quatro semanas que desliza uma semana por vez. Entre duas linhas consecutivas há cerca de três semanas de sobreposição.

**58.** Quais são os dois passos principais de trabalhar com window functions, e quais são os três componentes de uma window specification? *(Window Functions)*

R: Definir uma window specification, que define um agrupamento lógico de linhas chamado frame, e depois aplicar a window function apropriada.

Os três componentes são estes. `partition by`, que diz por quais colunas agrupar as linhas. `order by`, que diz como ordenar e em que sentido. E o frame, que define a fronteira da window na linha corrente, ou seja, quais linhas entram no cálculo daquela linha. A spec é construída com a classe `org.apache.spark.sql.expressions.Window`. `rowsBetween` define o intervalo por índice de linha e `rangeBetween` por valor real da expressão de ordenação. O capítulo avisa que o frame não é necessário em todos os cenários.

**59.** Quais são os três tipos de window function, e quais delas as Tables 4-4 e 4-5 listam? *(Window Functions; Tables 4-4, 4-5)*

R: Ranking functions, analytic functions e aggregate functions. Para as de agregação, qualquer função de agregação serve como window function.

A Table 4-4 lista `rank`, que devolve o rank das linhas dentro de um frame segundo uma ordenação, `dense_rank`, parecido com rank mas sem deixar buracos quando há empate, `percen_rank`, que devolve o rank relativo, `ntile(n)`, que devolve o ID do grupo ntile, e `row_number`, que devolve um número sequencial começando em 1.

A Table 4-5 lista `cume_dist`, que devolve a fração de linhas abaixo da linha corrente, `lag(col, offset)`, que devolve o valor `offset` linhas antes, e `lead(col, offset)`, que devolve o valor `offset` linhas depois.

**60.** O que há na Table 4-6, e quais quatro perguntas o capítulo se propõe a responder com ela? *(Table 4-6)*

R: Abri a página 39. A tabela tem três colunas, `Name`, `Date` e `Amount`, e seis linhas. John aparece com 2017-07-02 13.35, 2016-07-06 27.33 e 2016-07-04 21.72. Mary aparece com 2017-07-07 69.74, 2017-07-01 59.44 e 2017-07-05 80.14. As duas datas de 2016 são um defeito que o item 2 do Nível 4 trata.

As quatro perguntas são estas. Quais são as duas maiores transações de cada usuário. Qual a diferença entre o valor de cada transação e a maior transação do usuário. Qual a média móvel do valor de transação de cada usuário. E qual a soma cumulativa do valor de transação de cada usuário.

**61.** Como a Listing 4-52 responde à primeira pergunta? *(Listing 4-52)*

R: Ela define `Window.partitionBy("name").orderBy(desc("amount"))`, aplica `rank().over(forRankingWindow)` dentro de um `withColumn`, e filtra `'rank < 3`. A saída tem quatro linhas: Mary com 80.14 rank 1 e 69.74 rank 2, John com 27.33 rank 1 e 21.72 rank 2.

**62.** Como a Listing 4-53 define um frame que cobre a partition inteira, e o que ela calcula? *(Listing 4-53)*

R: Com `.rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)` em cima do `partitionBy("name").orderBy(desc("amount"))`. Sobre esse frame ela aplica `max(txDataDF("amount"))` e subtrai o `amount` da linha. O resultado é a coluna `amount_diff`, com 0.0 para a maior transação de cada usuário, 10.4 e 20.7 para Mary e 5.61 e 13.98 para John.

**63.** Como a Listing 4-54 define o frame da média móvel, e qual prática o comentário recomenda? *(Listing 4-54)*

R: Com `.rowsBetween(Window.currentRow-1, Window.currentRow+1)`, ou seja, três linhas, a corrente mais uma antes e uma depois. A ordenação é por `tx_date`. O comentário recomenda como boa prática especificar o deslocamento relativo a `Window.currentRow`, em vez de números soltos. A saída de Mary é 69.79, 69.77 e 74.94, e a de John é 17.54, 20.8 e 24.53.

**64.** Como a Listing 4-55 calcula uma soma cumulativa, e o que o capítulo diz sobre o frame padrão? *(Listing 4-55)*

R: Com `.rowsBetween(Window.unboundedPreceding, Window.currentRow)`, aplicando `sum("amount")` sobre esse frame. A saída de Mary é 59.44, 139.58 e 209.32, e a de John é 13.35, 35.07 e 62.4. O capítulo diz que o frame padrão já inclui todas as linhas anteriores até a linha corrente. Logo, declarar o frame na Listing 4-55 seria desnecessário e o resultado seria o mesmo.

**65.** Quais palavras-chave de SQL expressam uma window function, e qual restrição o capítulo enuncia? *(Listing 4-56)*

R: `PARTITION BY`, `ORDER BY`, `ROWS BETWEEN` e `RANGE BETWEEN`. As fronteiras do frame usam `UNBOUNDED PRECEDING`, `UNBOUNDED FOLLOWING`, `CURRENT ROW`, `<value> PRECEDING` e `<value> FOLLOWING`. A restrição é que, em SQL, o partition by, o order by e o frame precisam ser especificados em uma única statement.

**66.** O que é o Catalyst, e quais dois objetivos de design o capítulo enuncia para ele? *(Exploring Catalyst Optimizer)*

R: É o query optimizer e o segundo componente principal do módulo Spark SQL. Ele garante que a lógica escrita em DataFrame APIs ou em SQL rode de forma eficiente. Os dois objetivos de design são minimizar o tempo de resposta ponta a ponta das queries e ser extensível. Extensível aqui quer dizer que o usuário pode injetar código próprio no optimizer para fazer otimização customizada.

**67.** O que a Figure 4-6 mostra? Nomeie cada caixa e cada seta. *(Figure 4-6)*

R: Abri a página 43. A figura tem cinco caixas. À esquerda, duas caixas azuis empilhadas, `SQL` em cima e `Data Frames` embaixo, cada uma com uma seta apontando para a direita. As duas setas convergem na terceira caixa, `Logical Plan`. Dela sai uma seta para `Physical Plan`, e dessa sai uma seta para `RDD`, a única caixa amarela. Não há caixa de optimized logical plan nem de geração de código.

**68.** Quais são os quatro passos de alto nível do Catalyst, e quais otimizações rule-based o capítulo nomeia? *(Exploring Catalyst Optimizer; Logical Plan)*

R: Traduzir a lógica do usuário em um logical plan, otimizá-lo com heurísticas, converter em physical plan e gerar código a partir dele. O logical plan vem de um objeto DataFrame ou da árvore sintática abstrata da query SQL parseada. Ele é uma árvore de operadores e expressões.

Depois de criá-lo, o Catalyst o analisa para resolver as referências e garantir que são válidas. As otimizações rule-based nomeadas são constant folding, project pruning e predicate pushdown. O exemplo dado é mover a condição de filtro para antes de um join. A lista fica na classe `org.apache.spark.sql.catalyst.optimizer.Optimizer`. O princípio é podar dado desnecessário cedo e minimizar o custo por operador.

**69.** Em qual versão a cost-based optimization foi introduzida, o que ela decide, e de quais estatísticas ela depende? *(Logical Plan)*

R: No Spark 2.2. Ela permite que o Catalyst escolha de forma mais inteligente o tipo de join, com base nas estatísticas do dado processado. Ela depende das estatísticas detalhadas das colunas que participam do filtro ou do join, e por isso o framework de coleta de estatísticas foi introduzido. Os exemplos de estatística são cardinalidade, número de valores distintos, máximo e mínimo, e comprimento médio e máximo.

**70.** O que a fase do physical plan acrescenta, e qual é o passo final? *(Physical Plan)*

R: Ela gera physical plans com os operadores físicos que casam com o engine de execução do Spark. Ela também faz otimizações rule-based próprias. São duas: combinar projeções e filtragem em uma operação só, e empurrar as projeções ou os predicados de filtro para as origens que suportam isso, como o Parquet. O passo final é gerar o bytecode Java do physical plan mais barato.

**71.** Quais quatro seções o `explain(true)` imprime, e o que o capítulo afirma que os planos provam? *(Catalyst in Action; Listing 4-57)*

R: Parsed logical plan, analyzed logical plan, optimized logical plan e physical plan. A afirmação é que o optimized logical plan combina as duas condições de filtro em um filtro só. E que o physical plan mostra o Catalyst empurrando o filtro de `produced_year` e fazendo projection pruning no passo `FileScan`. Sem o argumento, `explain` mostra só o physical plan.

**72.** Liste os cinco modos da Table 4-7, e diga como é a saída do `formatted`. *(Table 4-7; Listing 4-58)*

R:

| Modo | O que imprime |
|---|---|
| `simple` | Só o physical plan |
| `extended` | O logical plan e o physical plan |
| `codegen` | O physical plan e o código gerado, se estiver disponível |
| `cost` | O logical plan e as estatísticas, se estiverem disponíveis |
| `formatted` | Divide a saída em duas seções, um esboço do physical plan e os detalhes |

A variação com string foi introduzida no Spark 3.0. Na Listing 4-58 o esboço é `Project (4)`, `Filter (3)`, `ColumnarToRow (2)` e `Scan parquet (1)`. Os detalhes trazem `PushedFilters: [IsNotNull(produced_year), GreaterThan(produced_year,1970)]`.

**73.** Qual observação criou o Project Tungsten, em que ano, e quais são as três iniciativas dele? *(Project Tungsten)*

R: A partir de 2015 os designers do Spark observaram que as cargas estavam cada vez mais limitadas por CPU e memória, e não mais por I/O e rede. O capítulo credita isso ao avanço do hardware, com links de 10 Gbps e SSD de alta velocidade. As três iniciativas são estas. Gerenciar memória de forma explícita, com técnicas off-heap, para eliminar o overhead do modelo de objeto da JVM e minimizar garbage collection. Usar algoritmos e estruturas de dados cache-aware para explorar a hierarquia de memória. E usar whole-stage code generation para minimizar chamadas de função virtual.

**74.** Como se identifica whole-stage code generation em um physical plan, e o que a Listing 4-59 combina? *(Project Tungsten; Listing 4-59)*

R: Sempre que um asterisco aparece antes de um operador, isso significa que a whole-stage code generation está ligada para ele. Na Listing 4-59, `spark.range(1000).filter("id > 100").selectExpr("sum(id)")` produz cinco operadores, e o `Exchange (4)` é o único sem asterisco. A whole-stage code generation combina a lógica de filtrar e somar inteiros numa função só.

**75.** Resuma o Summary em seis linhas. *(Summary)*

R: Agregação é uma das funcionalidades mais usadas em análise de big data, e o Spark SQL traz as funções comuns como `sum`, `count` e `avg`. Análise complexa costuma exigir join de dois ou mais datasets, e o Spark SQL suporta muitos dos tipos padrão do mundo SQL. O Spark SQL vem com um conjunto rico de funções built-in, e escrever uma UDF é fácil quando nenhuma delas serve. Window functions computam um valor para cada linha do grupo de entrada, o que serve para média móvel, soma cumulativa e rank. O Catalyst permite escrever aplicações eficientes, e o cost-based optimizer chegou no Spark 2.2. O Project Tungsten é o motor de bastidores que acelera a execução melhorando o uso de memória e CPU.

## Nível 2 — Compreensão

Explique com suas próprias palavras. Três a cinco frases cada.

**1.** Explique por que `count(col)` e `count("*")` podem divergir, e o que essa divergência te diz de graça. *(count(col); Listing 4-3)*

R: `count(col)` conta valores presentes naquela coluna e ignora null. `count("*")` conta linhas, independentemente do conteúdo. Na Listing 4-3 as quatro linhas do `badMoviesDF` produzem 2, 3, 4 e 4 para `actor_name`, `movie_title`, `produced_year` e `*`. A diferença entre `count("*")` e `count(col)` é a contagem de nulos daquela coluna, de graça. Dá para escrever um perfil de completude do dataset só com essas duas contagens.

**2.** Explique o trade-off que `approx_count_distinct` oferece, e quando você o recusaria. *(approx_count_distinct)*

R: Ela troca exatidão por tempo. O HyperLogLog mantém uma estrutura pequena em vez de guardar cada valor visto, então o custo de memória e de shuffle cai muito. O preço é um erro de estimativa, que por padrão é 0.05 e pode ser apertado por argumento. O capítulo avisa que apertar o erro faz a função demorar mais, então o botão anda nos dois sentidos. Eu recusaria a aproximação quando o número precisa fechar com outro sistema, como contagem de itens faturados, e aceitaria em métrica de produto, como visitantes únicos.

**3.** Explique por que `groupBy` devolver `RelationalGroupedDataset` em vez de um DataFrame é o design certo. *(Aggregation with Grouping; Multiple Aggregations per Group)*

R: Um DataFrame agrupado ainda não é uma tabela. Ele é um estado intermediário, em que as linhas foram associadas a chaves mas nenhum valor foi reduzido. Se `groupBy` devolvesse um DataFrame, não haveria onde pendurar as funções que só fazem sentido depois do agrupamento, como `agg` e `pivot`. O tipo separado força a próxima chamada a ser uma agregação, e é só depois dela que volta um DataFrame. É o tipo funcionando como documentação.

**4.** Explique o que `agg` te dá a mais do que chamar `count()` direto no dataset agrupado. *(Multiple Aggregations per Group; Listings 4-12, 4-14)*

R: `count()` direto devolve uma agregação só, e o nome da coluna é fixo. `agg` recebe várias column expressions de uma vez, então uma passada sobre os grupos produz count, min, max e sum juntos, como na Listing 4-14. Como cada função de agregação devolve um `Column`, dá para encadear `as` e nomear cada resultado. Sem isso o nome da coluna fica sendo a expressão inteira, como `min(count)`, que é ruim de referenciar depois.

**5.** Explique como o pivoting se relaciona com `groupBy` mais `agg`, e o que o pivoting acrescenta. *(Aggregation with Pivoting)*

R: Pivotar é agrupar duas vezes, uma pelas colunas de linha e outra pela coluna que vira coluna. Se eu fizesse `groupBy("graduation_year", "gender").avg("weight")`, eu teria os mesmos quatro números da Listing 4-17, só que em quatro linhas. O `pivot` pega os valores distintos de `gender` e os transforma em nomes de coluna, então o mesmo dado sai em duas linhas com duas colunas. O que ele acrescenta é forma, não informação. Por isso a regra de contagem de colunas é um produto: valores distintos vezes agregações.

**6.** Explique por que o capítulo diz que um left anti-join e um left semi-join são opostos, e o que eles têm em comum. *(Left Anti-Joins; Left Semi-Joins)*

R: Os dois filtram o dataset da esquerda usando o direito como critério, e nenhum dos dois traz colunas da direita. O semi-join guarda as linhas que casam, o anti-join guarda as que não casam. Juntos eles particionam o dataset da esquerda em duas partes disjuntas, e a soma das duas contagens é o total. Na Listing 4-26 e na Listing 4-27 isso aparece direto, uma linha e cinco linhas, para os seis empregados. A diferença deles para o inner join é que nenhum dos dois duplica linha da esquerda quando há várias correspondências à direita.

**7.** Explique por que o broadcast hash join é mais barato que o shuffle hash join, usando as duas figuras. *(Shuffle Hash Join; Broadcast Hash Join; Figures 4-4, 4-5)*

R: Na Figure 4-4 os dois datasets atravessam a rede. Cada linha dos dois lados é hasheada e enviada para uma partition escolhida por módulo, então o tráfego é a soma dos dois tamanhos. Na Figure 4-5 só o dataset pequeno atravessa a rede, e o grande fica onde está. O tráfego passa a ser o tamanho do pequeno vezes o número de partitions do grande. Com um dataset de 100 GB e outro de alguns megabytes, isso troca 200 GB de shuffle por alguns megabytes replicados. A condição para pagar esse preço é o menor caber na memória do executor.

**8.** Explique por que uma UDF em Python custa mais que uma UDF em Scala, e o que isso significa para o jeito de escrever pipelines. *(Working with User-Defined Functions (UDFs))*

R: O executor é um processo JVM. Uma UDF em Scala ou Java vira código que ele executa no mesmo processo, sem sair dele. Uma UDF em Python obriga o executor a lançar um processo Python separado e a serializar cada linha para lá e de volta. O custo não é o lançamento do processo, que acontece uma vez, é a serialização por linha, que acontece milhões de vezes. A conclusão prática é preferir funções built-in sempre, porque elas geram código otimizado, e só cair para UDF quando não houver built-in.

**9.** Explique a diferença entre as funções de agregação das seções anteriores e as window functions. *(Advanced Analytics Functions; Window Functions)*

R: Uma função de agregação recebe um grupo de linhas e devolve um valor para o grupo, então o número de linhas de saída cai. Uma window function também olha um grupo de linhas, chamado frame, mas devolve um valor para cada linha de entrada, então o número de linhas não muda. É por isso que a Listing 4-53 consegue colocar a diferença para o máximo do usuário na mesma linha de cada transação. Com `groupBy` eu teria o máximo por usuário em uma tabela separada e precisaria de um join para voltar.

**10.** Explique a diferença entre `rowsBetween` e `rangeBetween`, e por que a Listing 4-53 usa uma e a Listing 4-54 usa a outra. *(Window Functions; Listings 4-53, 4-54)*

R: `rowsBetween` conta posições de linha dentro da partition. `rangeBetween` compara o valor real da expressão do `order by`. A Listing 4-53 quer o frame inteiro da partition, do primeiro ao último, e para isso o tipo de fronteira não importa, então `rangeBetween` com unbounded dos dois lados serve. A Listing 4-54 quer exatamente três linhas, a anterior, a corrente e a seguinte, e isso é uma noção posicional, então precisa de `rowsBetween`. Se ela usasse `rangeBetween` com menos um e mais um, a fronteira passaria a ser um dia de diferença na data, e não uma linha.

**11.** Explique o que um rollup e um cube têm a ver com a hierarquia das colunas de agrupamento. *(Rollups; Cubes)*

R: O rollup lê as colunas como uma hierarquia. Com estado e cidade, ele produz o detalhe por cidade, o subtotal por estado e o total geral, e nunca produz um total por cidade ignorando o estado. O cube ignora a hierarquia e faz todas as combinações, então ele acrescenta as linhas em que o estado é null e a cidade está preenchida. A escolha entre os dois é semântica: use rollup quando as colunas realmente formam uma hierarquia, como país, estado e cidade, e cube quando as dimensões são independentes.

**12.** Explique o que o asterisco em um physical plan te diz, e por que isso importa para ler performance. *(Project Tungsten; Listing 4-59)*

R: O asterisco marca os operadores em que a whole-stage code generation está ligada. Esses operadores foram fundidos em uma única função Java compilada, sem chamada de função virtual entre eles. Na Listing 4-59 o `Exchange (4)` não tem asterisco, porque um shuffle quebra a cadeia e força uma fronteira de stage. Lendo o plano assim eu consigo ver onde estão as fronteiras de stage e quantos shuffles a query paga, sem abrir a UI.

---

## Nível 3 — Aplicação e transferência

Faça no terminal, com dados seus. Não use os datasets do livro.

**1.** Você tem uma tabela de logs de acesso de servidor com `status`, `path` e `bytes`. Produza, em uma passada só, a contagem de requisições, a contagem de paths distintos, a soma de bytes e a média de bytes por código de status. Depois renomeie cada coluna de resultado. *(Multiple Aggregations per Group)*

R:

```scala
import org.apache.spark.sql.functions._
logs.groupBy("status")
    .agg(
      count("*").as("requests"),
      countDistinct("path").as("paths"),
      sum("bytes").as("total_bytes"),
      round(avg("bytes"), 2).as("avg_bytes")
    )
    .orderBy('requests.desc)
    .show
```

Uso `count("*")` e não `count("path")`, porque quero contar linhas mesmo quando o path é null. O `as` em cada expressão é o padrão que a Listing 4-14 recomenda, senão a coluna sai chamada `count(DISTINCT path)`. Não uso a forma de mapa da Listing 4-15, porque ela não deixa renomear e eu preciso de nomes curtos aqui.

**2.** Os mesmos logs, mas a contagem de paths distintos está lenta demais. Mostre as duas versões e diga como você decidiria entre elas. *(approx_count_distinct)*

R:

```scala
logs.groupBy("status").agg(countDistinct("path").as("paths")).show
logs.groupBy("status").agg(approx_count_distinct("path", 0.02).as("paths")).show
```

A primeira é exata e cara. A segunda usa HyperLogLog com dois por cento de erro. Eu rodaria as duas uma vez sobre um recorte de um dia, compararia os dois números e mediria o tempo. Se o número aproximado ficar dentro do que a decisão tolera, eu fico com ele. O capítulo avisa que apertar o erro devolve parte do custo, então dois por cento já é mais caro que o padrão de cinco.

**3.** Você precisa do preço médio de passagem por mês e por classe de passagem, com os meses nas linhas e as classes nas colunas. Escreva isso, e diga o que acontece quando uma classe nova aparece. *(Aggregation with Pivoting)*

R:

```scala
val classes = Seq("economy", "premium", "business")
sales.groupBy("month")
     .pivot("class", classes)
     .agg(round(avg("price"), 2).as("avg"), count("*").as("n"))
     .orderBy("month")
     .show
```

Passo a lista de classes porque, sem ela, o Spark gasta uma passada extra para descobrir os valores distintos. O preço disso é que uma classe nova não aparece no relatório e some em silêncio. A saída tem seis colunas depois de `month`, porque são três valores vezes duas agregações. Se eu quiser resiliência a valor novo, tiro a lista e aceito a passada extra.

**4.** Você tem um DataFrame `movies` e um DataFrame `ratings`, os dois com uma coluna `movie_id`. Escreva um join que não produza um `movie_id` duplicado, e dois outros joins que respondam "quais filmes não têm rating" e "quais filmes têm pelo menos um rating". *(Using Joined Column Name; Left Anti-Joins; Left Semi-Joins)*

R:

```scala
val joined  = movies.join(ratings, "movie_id")
val noRate  = movies.join(ratings, movies("movie_id") === ratings("movie_id"), "left_anti")
val rated   = movies.join(ratings, movies("movie_id") === ratings("movie_id"), "left_semi")
```

A primeira usa a versão de `join` que recebe o nome da coluna de junção, e por isso o `movie_id` aparece uma vez só, como na Listing 4-32. As outras duas usam `left_anti` e `left_semi`, e as duas devolvem só as colunas de `movies`. O `left_semi` é melhor que um inner join aqui, porque um filme com dez avaliações apareceria dez vezes no inner join e aparece uma vez no semi-join.

**5.** Você precisa juntar uma tabela de clickstream de 200 GB com uma tabela de lookup de países de 3 MB. Escreva o join, e diga o que você conferiria depois. *(Broadcast Hash Join)*

R:

```scala
import org.apache.spark.sql.functions.broadcast
clicks.join(broadcast(countries), clicks("country_code") === countries("code"))
      .explain("formatted")
```

Pelo que o capítulo diz, o Spark decide sozinho na maior parte dos casos, com base nas estatísticas. Eu dou o hint mesmo assim, porque a tabela pequena vem de uma leitura em que a estatística pode não existir. Depois eu confiro no plano físico se aparece `BroadcastHashJoin` e não outra coisa. Sem broadcast eu estaria pagando o shuffle de 200 GB descrito na Figure 4-4 para casar com 3 MB.

**6.** Você precisa juntar dois DataFrames de leituras de sensor que têm os dois uma coluna `ts` e uma coluna `value`. Mostre o que quebra e as duas soluções que o capítulo dá. *(Dealing with Duplicate Column Names)*

R:

```scala
val bad = indoor.join(outdoor, indoor("sensor_id") === outdoor("sensor_id"))
bad.select("ts")   // AnalysisException: Reference 'ts' is ambiguous
```

A primeira correção é qualificar pela origem, com `bad.select(indoor("ts"), outdoor("ts"))`. A segunda é renomear antes do join:

```scala
val out2 = outdoor.withColumnRenamed("ts", "ts_out").withColumnRenamed("value", "value_out")
val good = indoor.join(out2, indoor("sensor_id") === out2("sensor_id"))
```

Prefiro a segunda em pipeline, porque o DataFrame resultante fica utilizável por quem não escreveu o join. A qualificação por DataFrame original só funciona enquanto as duas variáveis estiverem no escopo.

**7.** Você tem uma coluna string de JSON com o payload de um pedido. Transforme-a em colunas tipadas, e diga o que acontece se um campo faltar no seu schema. *(Working with Collection Functions)*

R:

```scala
import org.apache.spark.sql.types._
val orderSchema = new StructType()
  .add("order_id", StringType)
  .add("total", DoubleType)
  .add("items", ArrayType(StringType))

val parsed = raw.select(from_json('payload, orderSchema).as("o"))
parsed.select('o.getItem("order_id"), 'o.getItem("total"),
              size('o.getItem("items")).as("n_items")).show(false)
```

`from_json` exige o schema declarado. Campo que eu não declarar não aparece na struct. Isso é bom e é ruim. É bom porque poda o payload cedo. É ruim porque um campo novo entra no arquivo e ninguém percebe. O caminho de volta é `to_json('o)`, que a Listing 4-43 mostra.

**8.** Para cada produto de um catálogo, ranqueie os três maiores vendedores por receita, e acrescente a diferença para o melhor vendedor daquele produto. Escreva as duas window specifications. *(Window Functions)*

R:

```scala
import org.apache.spark.sql.expressions.Window

val byRevenue = Window.partitionBy("product_id").orderBy(desc("revenue"))
val wholeGroup = byRevenue.rangeBetween(Window.unboundedPreceding, Window.unboundedFollowing)

sales.withColumn("rk", rank().over(byRevenue))
     .withColumn("gap", round(max('revenue).over(wholeGroup) - 'revenue, 2))
     .where('rk <= 3)
     .orderBy('product_id, 'rk)
     .show
```

O `rank` usa a spec sem frame, porque ranking não precisa de fronteira. O `max` precisa enxergar a partition inteira, e por isso a segunda spec estende o frame de unbounded preceding a unbounded following, exatamente como a Listing 4-53. Uso `rank` e não `row_number` porque quero que empate de receita fique empatado.

**9.** Calcule uma média móvel de sete dias de usuários ativos diários, e o total acumulado do ano. Diga de qual frame cada um precisa. *(Window Functions)*

R:

```scala
val w = Window.partitionBy("year").orderBy("day")
val movingSeven = w.rowsBetween(Window.currentRow - 6, Window.currentRow)
val running     = w.rowsBetween(Window.unboundedPreceding, Window.currentRow)

dau.withColumn("avg_7d", round(avg('users).over(movingSeven), 1))
   .withColumn("ytd", sum('users).over(running))
   .show
```

A média de sete dias precisa de um frame posicional fechado, seis linhas antes mais a corrente. O acumulado precisa de um frame que começa no início da partition. O capítulo diz que o segundo é o frame padrão, então eu poderia omitir. Escrevo mesmo assim, porque escrito é conferível por quem lê depois, e o item 8 do Nível 5 mostra que o padrão não é exatamente o que o capítulo descreve.

**10.** Você tem uma tabela de receita com `region`, `country` e `amount`. Produza subtotais por região e um total geral, depois diga quando você trocaria por um cube. *(Rollups; Cubes)*

R:

```scala
revenue.rollup('region, 'country)
       .agg(sum("amount").as("total"))
       .orderBy('region.asc_nulls_last, 'country.asc_nulls_last)
       .show
```

O `rollup` serve aqui porque região e país formam hierarquia, e um total por país ignorando a região não significa nada. Eu trocaria para `cube` se as duas colunas fossem independentes, como canal de venda e forma de pagamento, onde o total por forma de pagamento sozinho é uma pergunta legítima. O `asc_nulls_last` é obrigatório para os subtotais caírem depois do detalhe, como o capítulo mostra na Listing 4-48.

**11.** Agregue leituras de sensor IoT em buckets de quinze minutos, depois em uma window de uma hora que desliza a cada quinze minutos. Mostre as duas, e leia o schema de saída. *(Aggregation with Time Windows)*

R:

```scala
val tumbling = readings.groupBy(window('ts, "15 minutes")).agg(avg("value").as("v"))
val sliding  = readings.groupBy(window('ts, "1 hour", "15 minutes")).agg(avg("value").as("v"))

tumbling.selectExpr("window.start", "window.end", "round(v,2) as v").orderBy("start").show(5)
```

O schema resultante traz uma coluna `window`, struct com `start` e `end`, mais a agregação. Por isso a projeção usa `window.start` e `window.end`. Na versão tumbling cada leitura entra em um bucket só. Na deslizante cada leitura entra em quatro buckets, porque a janela de uma hora desliza de quinze em quinze minutos. O start é inclusivo e o end é exclusivo.

---

## Nível 4 — Análise e síntese

Este nível cruza listing, tabela, figura e prosa. Várias questões pedem que você abra a página do PDF, porque a extração de texto não traz o conteúdo de figura e embaralha tabela.

**1.** **A variável que não existe.** A Listing 4-21 define `deptJoinExpression` e depois chama `join` duas vezes. Leia o código e diga se ele roda.

R: Abri a página 15 para conferir que não era artefato de extração. Está escrito assim mesmo. A linha de definição é `val deptJoinExpression = employeeDF.col("dept_no") === deptDF.col("id")`. As duas linhas seguintes são `employeeDF.join(deptDF, joinExpression, "inner").show` e `employeeDF.join(deptDF, joinExpression).show`.

O código não roda como publicado. `joinExpression` nunca foi definido e o compilador Scala devolve `not found: value joinExpression`. Ou o `val` deveria chamar-se `joinExpression`, ou as duas chamadas deveriam usar `deptJoinExpression`.

O que me chama atenção é a assimetria. O erro está no primeiro listing de join do capítulo, aquele que todo mundo vai copiar primeiro. E a Listing 4-22, logo abaixo, não usa variável nenhuma, escreve a expressão inline nas três formas. Ou seja, o único listing do capítulo que nomeia uma join expression é o que erra o nome dela.

**2.** **A tabela que não bate com o listing.** A Table 4-6 traz seis transações de compra. A Listing 4-52 monta as mesmas seis em código. Compare as duas, depois diga a qual delas as saídas impressas obedecem.

R: Abri a página 39 e li a Table 4-6 como imagem. As duas ficam a uma página de distância, a tabela na 39 e o `txDataDF` na 40.

A tabela traz John com `2017-07-02`, `2016-07-06` e `2016-07-04`. O código da Listing 4-52 traz John com `2017-07-02`, `2017-07-06` e `2017-07-04`. Duas datas divergem no ano.

As saídas obedecem ao código. A Listing 4-54 imprime John com `2017-07-02`, `2017-07-04` e `2017-07-06` nessa ordem, e o mesmo aparece na 4-53 e na 4-55. Logo o defeito está na tabela, e não no código.

O dano é maior do que trocar dois dígitos. As três listings de window function ordenam por `tx_date` dentro da partition. Se as datas de John fossem mesmo de 2016, a ordem dele viraria `2016-07-04`, `2016-07-06`, `2017-07-02`, e a média móvel e a soma cumulativa sairiam diferentes das que estão impressas. Quem estudar pela tabela e refizer as contas à mão vai achar que errou.

**3.** **O nome que a prosa inventa.** O capítulo usa `collect_list` e `collect_set` no código, mas os chama de outra coisa na prosa. Conte as ocorrências e diga o que esse padrão revela.

R: Contei no texto extraído. `collect_list` aparece 3 vezes e `collect_set` 2 vezes. As formas erradas `collection_list` e `collection_set` aparecem 3 e 1 vez.

A distribuição é o achado. Os nomes certos estão na Table 4-1, na primeira frase da seção e dentro do código da Listing 4-16. Os nomes errados estão só na prosa explicativa e na legenda da Listing 4-16. As quatro ocorrências erradas ficam todas na página 11, coladas umas nas outras.

A leitura que faço é que o autor escreveu a seção em uma sentada, digitando `collection_` de memória, e que o código veio de um shell onde o nome errado teria falhado. O compilador corrigiu o código e ninguém corrigiu o texto. É o mesmo mecanismo do `filler` que anotei no capítulo anterior: erro que só existe onde nada executa.

**4.** **A conclusão colada no lugar errado.** A Listing 4-23 faz um left outer join. Leia o parágrafo logo depois dela contra a saída, e decida se ele pertence ali.

R: Não pertence. O parágrafo diz que o departamento de marketing não tem nenhuma linha correspondente no dataset de empregados, e que o resultado mostra quais departamentos não têm empregado.

A saída da Listing 4-23 não tem Marketing em lugar nenhum. Um left outer join preserva as linhas do lado esquerdo, que é `employeeDF`. Marketing só existe no `deptDF`, do lado direito, e um left outer join descarta a linha da direita que não casa. As seis linhas impressas são os cinco empregados com departamento mais o Kurt.

Quem revela o Marketing é a Listing 4-24, o right outer join, e o parágrafo depois dela é quase idêntico ao da 4-23. Ou seja, o texto do right outer join foi copiado para cima do left outer join. O que a Listing 4-23 de fato mostra é o oposto: quais empregados não têm departamento.

Isso ensina errado justamente o par mais confundido do capítulo. Um leitor que decore o parágrafo vai sair achando que left e right outer join respondem a mesma pergunta.

**5.** **O empregado que não é null.** O capítulo escreve o departamento do Kurt como `null.asInstanceOf[Int]` e depois usa o left anti-join para achar "which employees are not assigned to a department". Audite essa afirmação.

R: A claim está certa por acidente, e o mecanismo não é o que parece.

`null.asInstanceOf[Int]` em Scala não produz null. Ele produz o valor padrão do tipo primitivo, que é 0. A prova está impressa três vezes no próprio capítulo. Na Listing 4-23 o Kurt sai com `dept_no` igual a 0, não null. Na Listing 4-26 o left anti-join devolve `Kurt | 0`. Na Listing 4-28 o cross join lista Kurt quatro vezes, sempre com 0.

Ou seja, o Kurt não é um empregado sem departamento. Ele é um empregado do departamento 0, que não existe no `deptDF`. O anti-join o encontra porque 0 não casa com 31, 33, 34 nem 35, e não porque o valor esteja ausente.

A diferença importa. Se o `dept_no` fosse de fato null, o comportamento seria outro: null nunca é igual a nada em uma comparação de igualdade, nem a outro null. O anti-join também o devolveria, mas por um motivo diferente, e um `where('dept_no.isNull)` só funcionaria no segundo caso. O capítulo escolheu o dado que ilustra o ponto e escreveu o código que produz outro dado.

**6.** **Duas contagens do mesmo aeroporto.** A Listing 4-12 mostra Melbourne com count 1. A Listing 4-14 mostra Melbourne com count 1, min 1332, max 1332 e sum 1332. Concilie as duas, e diga o que a coluna chamada `count` está fazendo.

R: As duas são consistentes, e a confusão está no nome.

O `flight_summary` tem uma coluna de dado chamada `count`. A Listing 4-12 chama `groupBy("origin_airport").count()`, e esse `count()` é a agregação que conta linhas do grupo. Melbourne tem uma linha só, então dá 1.

A Listing 4-14 chama `agg(count("count").as("count"), min("count"), max("count"), sum("count"))`. Aqui `count("count")` conta os valores não nulos da coluna de dado, o que também dá 1. As outras três operam sobre o valor da coluna de dado, que naquela única linha é 1332. Por isso min, max e sum são iguais.

O que aprendo é uma armadilha de leitura. Nessa página a palavra `count` significa três coisas: o nome de uma coluna do dataset, o nome de uma função de agregação e o nome da coluna de saída dela. O capítulo escolheu um dataset em que os três colidem, e depois usa `.as("count")` para fazer o terceiro colidir de propósito com o primeiro. Renomear a coluna de dado para `n_flights` na leitura tornaria a seção inteira legível.

**7.** **A tabela de referência com sete erros.** Leia a Table 4-3 como imagem e liste todo nome de função que não existe. Diga o que eles têm em comum.

R: Abri as páginas 22 e 23 e li a tabela como imagem, porque a extração embaralha e eu não quero acusar defeito de extração como defeito do autor. Os nomes errados estão lá mesmo.

- `current_timesatmp`, na categoria Date time, por `current_timestamp`.
- `radian` e `degree`, na Math, por `radians` e `degrees`. Conferi no código-fonte no item 5 do Nível 5, e as formas singulares nunca existiram.
- `cr32`, na Cryptography, por `crc32`.
- `approx._count_distinct`, na Aggregation, com um ponto sobrando entre `approx` e `_count`.
- `array_contain`, na Collection, por `array_contains`.

Além dos nomes, a tabela tem dois defeitos de conteúdo. A categoria Collection lista `size` duas vezes, na segunda e na sétima posição. E a linha Aggregation termina em `sum,` com vírgula e sem item seguinte, o que sugere uma lista truncada na edição.

O que eles têm em comum é que nenhum deles é conferível por leitura. `cr32`, `radian` e `current_timesatmp` são plausíveis à vista. O ponto sobrando em `approx._count_distinct` é invisível numa fonte pequena, e ele se repete no corpo do texto, na frase que apresenta a Listing 4-5. E `array_contain` fica a cinco páginas do `array_contains` correto, que aparece dentro do código da Listing 4-42, na página 28.

A consequência é específica desta tabela. Ela é o índice do capítulo para 200 funções, e é o único lugar onde a maioria delas é mencionada. Um nome errado ali não é erro de digitação, é uma entrada de índice que não leva a lugar nenhum.

**8.** **`percen_rank`.** A Table 4-4 lista uma ranking function com esse nome. Ache o outro defeito da mesma classe na mesma página e diga por que esse par é pior que os da Table 4-3.

R: Abri a página 38. `percen_rank` está na tabela, por `percent_rank`. Na mesma página, na prosa acima da tabela, está escrito `rangeBetweeen`, com três letras `e`, por `rangeBetween`.

Este par é pior que os da Table 4-3 por uma razão. Na Table 4-3 o erro é um nome em uma lista de referência, e o leitor que digitar errado leva um erro de compilação imediato. Aqui o `rangeBetweeen` aparece na frase que ensina a diferença entre os dois métodos de definir frame, que é o conceito mais difícil da seção. E `rangeBetween` está escrito corretamente três listings depois, na 4-53. Ou seja, o leitor que voltar à explicação vai ver um nome, e o que copiar o código vai ver outro, sem nenhum aviso de qual é o certo.

O que salva o leitor é o compilador. O que não salva é a leitura de estudo, que é exatamente o modo em que este capítulo é consumido.

**9.** **A referência cruzada que aponta para outro listing.** A seção de `regexp_extract` diz "Listing 4-30 is an example of working with the regexp_extract function". Abra a Listing 4-30 e diga o que aconteceu.

R: A Listing 4-30 existe e é outra coisa. Ela está na página 19 e mostra `dupNameDF.select("dept_no")` disparando a `AnalysisException` de coluna ambígua. Nada a ver com expressão regular.

O listing que a frase queria citar é o 4-39, que está logo abaixo dela, na mesma página 27. A legenda impressa é `Listing 4-39 Using regexp_extract string Function to Extract "fox" Out Using a Pattern`.

O que aconteceu é um dígito trocado, 4-30 por 4-39. É o mesmo defeito de classe que anotei no capítulo anterior, onde a seção `limit` citava a Listing 3-20 em vez da 3-29. E é o mesmo motivo de sobrevivência: número não tem ortografia, então nenhuma revisão de língua pega. A diferença é que aqui o alvo errado existe e é plausível, então quem seguir a referência vai encontrar um listing de verdade e concluir que entendeu errado a frase.

**10.** **O capítulo que discorda de si mesmo sobre pivoting.** A seção de pivoting define a operação de um jeito e o Summary define de outro. Decida qual está certo.

R: A seção, na página 11, diz que os valores categóricos são transpostos de linhas para colunas individuais, e reforça logo abaixo que pivoting é traduzir linhas em colunas. O Summary, na página 48, diz que a agregação com pivoting transpõe colunas em linhas. As duas frases ficam a 37 páginas de distância.

A seção está certa e o Summary está errado. A prova é a própria Listing 4-17. A entrada tem uma coluna `gender` com os valores `M` e `F` espalhados em sete linhas. A saída tem duas colunas chamadas `M` e `F`. Valor de linha virou nome de coluna. A operação inversa, transformar colunas em linhas, se chama unpivot e o capítulo não a cobre.

O que me interessa aqui não é o erro em si, é onde ele está. O Summary é o pedaço que a pessoa relê antes da prova, quando não tem mais tempo de conferir. É o pior lugar possível para inverter a definição de uma operação. Somado ao item 4, o padrão do capítulo é que a prosa resumida erra mais que a prosa de exemplo.

**11.** **A década que não é década.** A Listing 4-57 acrescenta uma coluna chamada `produced_decade` e filtra por ela. Calcule o que a expressão de fato produz.

R: Abri a página 45 para ler o listing como imagem. A expressão é `'produced_year + 'produced_year % 10`.

Isso não é uma década. Para 1975 ele devolve 1975 mais 5, ou seja, 1980. Para 1971 ele devolve 1972. Para 1980 ele devolve 1980. A função não é monotônica em relação à década e não mapeia um ano na década dele.

A expressão correta, que o Capítulo 3 usa na Listing 3-23, é `produced_year - (produced_year % 10)`. Ali 1975 vira 1970 e 1971 vira 1970, que é o que a palavra década quer dizer. Trocaram o menos por um mais.

O filtro seguinte é `'produced_decade > 2010`. Com a expressão errada, ele deixa passar filmes de 2005 em diante, porque 2005 mais 5 é 2010 e 2006 mais 6 é 2012. Com a expressão certa, ele deixaria passar só de 2020 em diante, e o dataset do livro vai até 2012, então a saída seria vazia.

A ironia é que o erro salva o listing. Se a fórmula estivesse certa, o exemplo do Catalyst não teria linha nenhuma para mostrar. O ponto pedagógico do listing, que é ver o predicate pushdown no plano, não depende da aritmética, então o defeito passa despercebido. Mas o nome da coluna mente.

**12.** **A figura que para antes do fim.** O texto do Catalyst descreve quatro passos e termina com a geração de bytecode. A Figure 4-6 tem quatro caixas em sequência. Compare os dois.

R: Abri a página 43. A figura tem `SQL` e `Data Frames` convergindo em `Logical Plan`, depois `Physical Plan`, depois `RDD`. A última caixa é amarela, diferente das outras, o que sugere que ela representa a saída e não uma etapa.

O texto descreve outra sequência: criar o logical plan, otimizá-lo com heurísticas, converter em physical plan e gerar código a partir dele. A frase da seção Physical Plan é explícita, o passo final do Catalyst é gerar o bytecode Java do physical plan mais barato.

Duas coisas da figura não casam com o texto. Ela não tem caixa para a otimização, embora a Listing 4-57 imprima três logical plans distintos, parsed, analyzed e optimized. E ela termina em `RDD`, não em bytecode.

A terminação em RDD não é erro, é uma escolha de nível. O physical plan de fato vira um DAG de RDDs, e o bytecode gerado pela whole-stage code generation roda dentro das tasks desses RDDs. Achei que fosse contradição e não é. A figura descreve a estrutura de execução e o texto descreve a geração de código, e as duas coisas convivem.

O que sobra como defeito real é a ausência da etapa de otimização, que é o assunto inteiro da seção que a figura ilustra. Uma figura de um optimizer que não desenha a otimização é uma figura incompleta.

**13.** **A estratégia que o capítulo ensina e nunca mostra.** O capítulo nomeia duas join strategies. Conte quantas vezes cada uma aparece em um physical plan impresso.

R: Nenhum plano físico impresso no capítulo mostra um shuffle hash join. O único plano de join impresso é o da Listing 4-33, e ele mostra `*BroadcastHashJoin` com `BroadcastExchange` e dois `LocalTableScan`.

Isso é esperado, porque os dois DataFrames do exemplo têm seis e quatro linhas. Nenhum join entre eles jamais escolheria uma estratégia de shuffle. O capítulo ensina a estratégia cara com uma figura e a estratégia barata com um plano físico de verdade.

A consequência é que o leitor sai sem saber reconhecer um shuffle hash join no plano, que é justamente o que ele precisa reconhecer quando um job está lento. Ele sabe reconhecer o broadcast, que é o caso que não dói.

Há um problema maior por trás disso, e ele vem de fora do capítulo. Busquei a palavra `merge` no texto inteiro e ela aparece zero vezes. O Spark não escolhe shuffle hash join por padrão para dois datasets grandes, ele escolhe sort-merge join. Conferi a configuração `spark.sql.join.preferSortMergeJoin` no código-fonte, no item 2 do Nível 5, e o padrão dela é `true`. Ou seja, a estratégia que o capítulo apresenta como o caso grande contra grande não é a que o Spark usa nesse caso.

**14.** **`always`.** A seção Cubes diz que o resultado de um cube sempre tem mais linhas que o resultado de um rollup. Teste a palavra "always".

R: A palavra é forte demais. Com duas colunas de agrupamento, como no exemplo, o cube produz treze linhas e o rollup produz oito, então a afirmação vale ali.

Com uma coluna de agrupamento só, os dois produzem exatamente a mesma coisa. `rollup(a)` gera o detalhe por `a` mais o total geral. `cube(a)` gera a mesma coisa, porque só existe uma combinação além do conjunto vazio. Confirmei a regra na documentação de GROUP BY, no item 7 do Nível 5, onde `ROLLUP` de N elementos dá N mais 1 grouping sets e `CUBE` de N elementos dá 2 elevado a N. Para N igual a 1, os dois dão 2.

A frase correta seria "nunca tem menos linhas". Para N maior que 1 o cube tem estritamente mais. Para N igual a 1 são iguais.

Isso não é um erro grave, é uma imprecisão. Anoto porque ela aparece exatamente onde o leitor está tentando decidir entre as duas operações, e porque o critério útil não é o número de linhas e sim a semântica, que discuti na questão 11 do Nível 2.

**15.** **O tipo da coluna de tempo.** A seção Aggregation with Time Windows diz que a window function exige uma coluna timestamp. Confira a Listing 4-50 contra essa regra.

R: A Listing 4-50 não cumpre a regra. O `printSchema` impresso mostra `|-- Date: string (nullable = true)`. O arquivo é lido com `inferSchema` true e a inferência devolveu string, não timestamp. Mesmo assim o listing chama `window('Date, "1 week")` e imprime resultado.

Isso funciona porque o Spark faz cast implícito de string para timestamp na função. O capítulo não menciona esse cast. Ele enuncia a regra em uma página e a viola na página seguinte sem comentar.

O custo prático é alto. Um cast implícito que falha não dá erro, dá null, e uma linha com timestamp null desaparece silenciosamente da agregação. Um leitor que copie o padrão sobre um arquivo com formato de data diferente vai perder linhas sem aviso. A leitura honesta do listing é que ele deveria ter um `to_timestamp` explícito, ou um schema declarado, entre a leitura e o `groupBy`.

**16.** **As bordas de janela às quatro da tarde.** A Listing 4-50 agrupa a ação da Apple em windows de uma semana e as fronteiras saem impressas como `16:00:00`. Explique de onde vem esse horário.

R: A explicação não está no capítulo, e por isso a busquei fora. A documentação da função `window`, no Scaladoc da 4.2.0, diz que o `startTime` é um deslocamento em relação a `1970-01-01 00:00:00 UTC`. Sem `startTime`, as janelas ficam alinhadas com essa origem.

Com isso as contas fecham. As janelas de uma semana começam em múltiplos de sete dias a partir de 1º de janeiro de 1970, que foi uma quinta-feira. Logo toda fronteira cai numa quinta-feira à meia-noite UTC. A primeira linha impressa começa em `2016-12-28 16:00:00`, e 28 de dezembro de 2016 foi uma quarta-feira. Somando oito horas chega-se a `2016-12-29 00:00:00`, quinta-feira. Oito horas é exatamente o deslocamento do horário do Pacífico em relação ao UTC no inverno.

Ou seja, as fronteiras estão certas em UTC e são impressas no fuso da máquina do autor. O `16:00:00` não é dado, é fuso horário.

Isso não é defeito do livro, é uma armadilha do Spark que o livro não avisa. Duas pessoas rodando o mesmo listing em fusos diferentes veem rótulos de semana diferentes para os mesmos dados. Guardo como regra: em agregação por janela de tempo, fixe o fuso da sessão antes de olhar a saída.

**17.** **O listing que não compila.** A Listing 4-34 cria um DataFrame, imprime o schema dele e o exibe. Leia os três nomes de variável e diga se a sequência roda.

R: Não roda. O listing usa três nomes para o que deveria ser uma coisa só.

Ele começa com `val testResultDF = testDF.select(...)` e a cadeia termina em `.show(false)`. Em Scala, `show` devolve `Unit`, então `testResultDF` é `Unit` e não um DataFrame. A linha seguinte, `testResultDF.printSchema`, não compila.

Depois vem `testDateResultDF.show`, com um terceiro nome que não é definido em lugar nenhum do capítulo. Conferi com busca no texto extraído e ele aparece uma vez só, nessa linha.

Por fim, a Listing 4-35, na página seguinte, volta a usar `testResultDF` como se fosse um DataFrame, chamando `.select(...)` nele.

A reconstrução que faz o conjunto funcionar é tirar o `.show(false)` da atribuição e chamar o show separado. Isso torna `testResultDF` um DataFrame, faz o `printSchema` compilar, e o `testDateResultDF` vira só um nome errado para `testResultDF`. O que aconteceu foi um `.show` colado dentro da atribuição durante a edição, e nenhuma reexecução depois.

**18.** **A legenda repetida.** As Listings 4-4 e 4-5 carregam a mesma legenda. Diga o que cada uma de fato demonstra e quais seriam as legendas certas.

R: As duas se chamam "Counting Unique Items in a Group". A Listing 4-4 compara `countDistinct` com `count` e revela os 322 aeroportos únicos. A Listing 4-5 compara `countDistinct` com `approx_count_distinct` sobre a coluna `count`, e é sobre custo, não sobre contagem.

Legendas honestas seriam algo como "Counting Unique Items with countDistinct" para a 4-4 e "Comparing countDistinct with approx_count_distinct" para a 4-5.

Há um segundo problema na 4-5. A linha `approx_count_distinct (col, max_estimated_error=0.05)` aparece dentro do bloco de código do listing, entre a saída da tabela e a legenda. Ela não é código, é o título da subseção. A extração de texto a colocou ali e eu confirmei na página que a diagramação do PDF de fato a coloca colada ao bloco. É defeito de produção, não do autor.

**19.** **O erro que passou de dez por cento.** A Listing 4-5 diz que o erro de estimativa padrão é 0.05 e imprime 2033 exato contra 2252 aproximado. Calcule o erro observado e decida se o capítulo está errado.

R: A diferença é 219 sobre 2033, ou seja, 10,8 por cento. Isso é mais que o dobro dos 5 por cento anunciados, e minha primeira leitura foi que o capítulo ou a saída estavam errados.

A suspeita não se sustenta. O parâmetro do HyperLogLog não é um teto de erro, é um desvio padrão relativo. Conferi a documentação da função no item 5 do Nível 5, e ela chama o argumento de "maximum relative standard deviation allowed", com padrão 0,05. Um desvio padrão de 5 por cento significa que erros de 10,8 por cento, ou seja, cerca de dois desvios, acontecem com frequência baixa mas normal. Uma execução única cair ali não prova nada.

O que fica é uma crítica de redação, não de número. O capítulo escreve `max_estimated_error=0.05` e chama o parâmetro de "acceptable estimation error". Isso soa como garantia dura. A saída impressa, na linha de baixo, mostra um erro de mais do dobro. O capítulo não comenta a diferença. Quem ler as duas coisas juntas conclui ou que a função está quebrada ou que ele leu errado.

**20.** **A numeração dos caminhos.** Todo caminho de dado do capítulo nomeia um diretório de capítulo. Ache todos eles, e diga o que eles datam.

R: Busquei página por página. `chapter5` aparece nas páginas 3 e 36. `chapter4` aparece nas páginas 34, 45 e 46.

Os dois `chapter5` são o `flight_summary` da Listing 4-1 e o `aapl-2017.csv` da Listing 4-50. Os três `chapter4` são a releitura do `flight_summary` na Listing 4-48 e o `movies.parquet` nas Listings 4-57 e 4-58.

O que isso data é a origem do capítulo. No Capítulo 3 deste mesmo livro os caminhos diziam `chapter4`, e a conclusão que registrei lá foi que o conteúdo vinha do Capítulo 4 da edição anterior, *Beginning Apache Spark 2*. Aqui a evidência é a mesma um degrau adiante. Este capítulo era o Capítulo 5 da edição anterior, e o `chapter5` dos caminhos é o resíduo.

O detalhe que fecha o caso é a ordem. O `chapter4` da página 34 fica entre os dois `chapter5`, das páginas 3 e 36. Ou seja, a correção não foi feita de uma vez, do começo ao fim. Alguém corrigiu listings específicos, os que foram reexecutados para esta edição, e deixou os outros. A Listing 4-48 recarrega o mesmo arquivo que a Listing 4-1 já tinha carregado, com um caminho diferente, na mesma variável `flight_summary`.

Para quem estuda, a consequência prática é que os caminhos do livro não correspondem ao repositório de código dele, e é preciso conferir os dois nomes de diretório.

**21.** **O que este capítulo cumpre do que o anterior prometeu.** O Capítulo 3 adiou Catalyst, predicate pushdown, `join` e `groupBy` para este capítulo. Audite a entrega.

R: Todos os quatro chegaram, e três chegaram bem.

`groupBy` ganha a seção Aggregation with Grouping, com `agg`, `pivot`, `rollup` e `cube`. `join` ganha uma seção inteira, com sete tipos, sete listings e a discussão de nome duplicado. Predicate pushdown aparece nomeado entre as otimizações rule-based e é demonstrado de verdade, no `PushedFilters` da Listing 4-58. Essa é a entrega mais limpa das quatro, porque o Capítulo 3 prometeu um exemplo e este dá um exemplo executável.

O Catalyst é o caso mais fraco. Ele ganha quatro páginas, contra as duas metades do módulo que a abertura do Capítulo 3 anunciava. A seção descreve o pipeline em prosa, mostra `explain` funcionando e para. Ela não explica como uma regra é escrita, embora o Capítulo 3 tenha vendido o Catalyst como extensível, e este capítulo repita que o usuário pode injetar código próprio no optimizer. A promessa de extensibilidade é feita duas vezes e cumprida zero.

O que este capítulo abre e não fecha é o assunto de tumbling e sliding window, adiado explicitamente para o Capítulo 6. Esse adiamento é honesto, porque nomeia o destino e porque as duas listings de janela funcionam sem ele.

---

## Nível 5 — Além do capítulo (backlog, não notas)

Estes itens não são respondíveis com o capítulo. Conferi cada um contra fonte primária em **3 de agosto de 2026**, quando a documentação corrente do Spark era a **4.2.0**. As URLs estão ao fim da seção.

**1.** **Os join types hoje.** A Table 4-2 lista sete join types e diz que o right anti-join não existe. Confira a gramática atual.

R: A afirmação continua correta, e a gramática atual explica por quê. A página de sintaxe de JOIN da 4.2.0 define o `join_type` assim:

`[ INNER ] | CROSS | LEFT [ OUTER ] | [ LEFT ] SEMI | RIGHT [ OUTER ] | FULL [ OUTER ] | [ LEFT ] ANTI`

O `LEFT` é opcional em `SEMI` e em `ANTI`, mas não existe forma `RIGHT SEMI` nem `RIGHT ANTI`. Ou seja, escrever `SEMI JOIN` é o mesmo que escrever `LEFT SEMI JOIN`, e o lado nunca é negociável.

O que a tabela do capítulo não tem e a gramática tem: `NATURAL JOIN` e a cláusula `USING`, que é o equivalente SQL da versão de `join` por nome de coluna que a Listing 4-32 usa na API.

**2.** **As estratégias de join hoje.** O capítulo nomeia duas estratégias. Ache o conjunto atual e a padrão.

R: São mais que duas, e a que o capítulo trata como padrão para dois datasets grandes não é a padrão.

A página de performance tuning lista quatro hints de estratégia, `BROADCAST`, `MERGE`, `SHUFFLE_HASH` e `SHUFFLE_REPLICATE_NL`, que correspondem a broadcast hash join, sort-merge join, shuffle hash join e cartesian product join. O texto acrescenta que o `BROADCAST` pode virar broadcast nested loop join quando não há chave de equi-join.

O `MERGE`, ou sort-merge join, é o que falta no capítulo por completo. Busquei a palavra `merge` no texto do capítulo e ela não aparece nenhuma vez.

O padrão está no código-fonte. A configuração `spark.sql.join.preferSortMergeJoin` existe desde a versão 2.0.0 e o `.createWithDefault(true)` está lá. A documentação dela diz "When true, prefer sort merge join over shuffled hash join". Ou seja, o Spark prefere sort-merge ao shuffle hash desde antes do livro ser escrito.

**3.** **O limite do broadcast.** O capítulo diz que o broadcast é usado quando um dataset "is small enough to fit into the memory of the executor". Ache o critério de verdade.

R: O critério é uma configuração, não uma avaliação de memória. `spark.sql.autoBroadcastJoinThreshold` existe desde a 1.1.0 e o padrão é `10MB`, escrito no código como `.createWithDefaultString("10MB")`. A documentação dela fala em "maximum size in bytes for a table that will be broadcast to all worker nodes".

Duas coisas que o capítulo não diz e que mudam a prática. Primeiro, o hint `BROADCAST` ignora o limite. A documentação de performance tuning é explícita: com o hint, o broadcast é priorizado "even if the size of table 't1' suggested by the statistics is above the configuration `spark.sql.autoBroadcastJoinThreshold`". Ou seja, o hint da Listing 4-33 pode causar um estouro de memória no driver que a escolha automática teria evitado. Segundo, existe `spark.sql.shuffledHashJoinFactor`, introduzida na 3.3.0 com padrão 3, que decide quando o shuffle hash join é preferível ao sort-merge.

**4.** **AQE.** A escolha de join strategy do capítulo é descrita como uma decisão tomada a partir de estatísticas coletadas durante a leitura. Confira o que acontece em runtime hoje.

R: Existe uma camada inteira que o capítulo não tem, o Adaptive Query Execution. A documentação diz que o AQE "makes use of the runtime statistics to choose the most efficient query execution plan, which is enabled by default since Apache Spark 3.2.0". Conferi `spark.sql.adaptive.enabled` no código-fonte e o padrão é `true`.

Isso muda a natureza da escolha. No modelo do capítulo, a estratégia de join é decidida no planejamento e fica. Com AQE, o Spark re-otimiza depois de cada stage, com o tamanho real dos dados em mãos, e pode trocar um sort-merge por um broadcast em tempo de execução. Existe também `spark.sql.adaptive.skewJoin.enabled`, que quebra tasks enviesadas em tasks de tamanho parecido dentro de um sort-merge join.

Para quem estuda pelo livro, a consequência é que o plano impresso por `explain` antes da execução pode não ser o plano executado. O `explain` estático mostra a intenção, não o resultado.

**5.** **As funções que mudaram de nome.** A Table 4-1 lista `sumDistinct` e a Table 4-3 lista `radian`, `degree`, `cr32`, `array_contain` e `current_timesatmp`. Confira os nomes reais e as deprecações.

R: Baixei o `functions.scala` da tag `v4.0.0` do repositório do Spark e busquei cada um.

`sumDistinct` existe e está depreciado. A anotação no código é `@deprecated("Use sum_distinct", "3.2.0")`. O substituto é `sum_distinct`. O mesmo aconteceu com `countDistinct`, que ganhou o irmão `count_distinct` na 3.2.0, embora sem depreciação.

`radian` e `degree` no singular nunca existiram. As funções são `radians` e `degrees`. O que existe depreciado são `toRadians` e `toDegrees`, com `@deprecated("Use radians", "2.1.0")` e `@deprecated("Use degrees", "2.1.0")`. Ou seja, o autor escreveu uma terceira forma, que não é nem a atual nem a antiga.

`cr32`, `array_contain` e `current_timesatmp` também não existem, em nenhuma forma. Os nomes são `crc32`, `array_contains` e `current_timestamp`.

Sobre o `approx_count_distinct`, o padrão de 0.05 do capítulo continua correto. O Scaladoc da 4.2.0 descreve o parâmetro como "rsd: maximum relative standard deviation allowed (default = 0.05)".

**6.** **As funções de agregação que chegaram depois.** O capítulo lista quatorze. Ache as adições notáveis.

R: A lista cresceu muito. As adições que mais mudam trabalho de engenharia:

| Função | Desde | Para que serve |
|---|---|---|
| `count_distinct` | 3.2.0 | Substituto de `countDistinct`, com nome em snake case |
| `sum_distinct` | 3.2.0 | Substituto de `sumDistinct` |
| `array_agg` | 3.5.0 | Apelido de `collect_list`, com o nome que o SQL padrão usa |
| `grouping` | 2.0.0 | Devolve 1 se a coluna está agregada naquela linha e 0 se não |
| `grouping_id` | 2.0.0 | Identifica qual grouping set gerou a linha |

`grouping` e `grouping_id` existem desde o Spark 2.0, ou seja, cinco anos antes do livro. Isso importa porque as seções Rollups e Cubes dependem de null para marcar subtotal, e o capítulo não avisa que o null de subtotal é indistinguível de um null de dado. Com `grouping('origin_city)` valendo 1 na linha de subtotal e 0 na linha de detalhe, a ambiguidade some. O capítulo passa por isso sem mencionar.

**7.** **Rollup e cube em SQL.** Confira a regra de equivalência entre rollup, cube e grouping sets.

R: A documentação de GROUP BY da 4.2.0 define as duas como atalhos de `GROUPING SETS`, com regras exatas.

Para o `ROLLUP`: *"`GROUP BY warehouse, product WITH ROLLUP` or `GROUP BY ROLLUP(warehouse, product)` is equivalent to `GROUP BY GROUPING SETS((warehouse, product), (warehouse), ())`. The N elements of a `ROLLUP` specification results in N+1 `GROUPING SETS`."*

Para o `CUBE`: *"`GROUP BY CUBE(warehouse, product)` is equivalent to `GROUP BY GROUPING SETS((warehouse, product), (warehouse), (product), ())`. The N elements of a `CUBE` specification results in 2^N `GROUPING SETS`."*

Isso dá o critério numérico exato que a seção Cubes não dá, e é a fonte que usei para derrubar a palavra "always" no item 14 do Nível 4. Para N igual a 1, N mais 1 e 2 elevado a 1 dão o mesmo número.

**8.** **O frame padrão.** O capítulo diz que o frame padrão inclui todas as linhas anteriores até a linha corrente. Confira a definição exata.

R: A descrição está quase certa e o tipo do frame está omitido, e o tipo é o que importa.

O Scaladoc da classe `Window` na 4.2.0 diz: *"When ordering is not defined, an unbounded window frame (rowFrame, unboundedPreceding, unboundedFollowing) is used by default. When ordering is defined, a growing window frame (rangeFrame, unboundedPreceding, currentRow) is used by default."*

Ou seja, com `order by` o frame padrão é `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, e não `ROWS BETWEEN`. A Listing 4-55 escreve `rowsBetween`, e o capítulo afirma que omitir daria o mesmo resultado.

No dataset dele daria mesmo, porque as três datas de cada usuário são distintas. Com datas repetidas os dois divergem. Um frame `ROWS` para na linha corrente. Um frame `RANGE` inclui todas as linhas com o mesmo valor de ordenação, então duas transações no mesmo dia teriam a mesma soma acumulada, já contando as duas. Essa é uma diferença clássica de resultado em relatório financeiro.

O segundo achado é o caso sem `order by`, que o capítulo não menciona. Ali o frame padrão é a partition inteira, dos dois lados, o que explica por que a Listing 4-53 conseguiria o mesmo `max` sem declarar `rangeBetween`.

**9.** **A coluna de tempo da função `window`.** O capítulo diz que a coluna precisa ser timestamp. Confira o contrato atual e o que foi acrescentado desde então.

R: O contrato é mais estrito e mais amplo ao mesmo tempo. O Scaladoc de `window` diz: *"The time column must be of TimestampType or TimestampNTZType."* O `TimestampNTZType`, timestamp sem fuso, não existia quando o livro foi escrito.

A mesma documentação define o quarto parâmetro que o capítulo não mostra: *"startTime: The offset with respect to 1970-01-01 00:00:00 UTC with which to start window intervals."* É essa frase que explica as fronteiras às 16 horas do item 16 do Nível 4.

Ela também avisa sobre a duração: *"the duration is a fixed length of time, and does not vary over time according to a calendar. For example, `1 day` always means 86,400,000 milliseconds, not a calendar day."* O capítulo diz que o comprimento pode ser em dias e não avisa que um dia não é um dia de calendário. Em fuso com horário de verão, isso quebra a agregação diária.

Duas funções novas: `session_window`, desde a 3.2.0, que agrupa por lacuna de inatividade em vez de janela fixa, e `window_time`, desde a 3.4.0, que extrai um timestamp representativo da janela.

**10.** **Os padrões de data.** O capítulo aponta para o tutorial do `simpleDateFormat` da Oracle. Confira o que o Spark usa hoje.

R: A referência está desatualizada desde o Spark 3.0, ou seja, desde antes do livro sair.

O guia de migração diz que, no Spark 3.0, os padrões de data e hora passaram a ser definidos pelo próprio Spark e implementados por `DateTimeFormatter`, do pacote `java.time`. A frase relevante: *"New implementation performs strict checking of its input."* Os exemplos que ela dá são de padrões que passavam antes e falham agora, como `dd/MM/yyyy hh:mm` sobre `31/01/2015 00:00`, porque `hh` cobre horas de 1 a 12.

A configuração que controla isso é `spark.sql.legacy.timeParserPolicy`. Conferi no código-fonte: ela existe desde a 3.0.0 e o padrão é `CORRECTED`. A documentação interna dela diz: *"When LEGACY, java.text.SimpleDateFormat is used ... which is the approach before Spark 3.0. When set to CORRECTED, classes from java.time.* packages are used."*

O impacto direto no capítulo é a Listing 4-35, que usa o padrão `"dd-MM-YYYY"`. A página de padrões de data e hora da 4.2.0 lista `y` como letra de ano e não documenta `Y` em lugar nenhum. Em `java.time`, `Y` é week-based year, que difere de `y` na virada de ano. Ou seja, o listing escolhe uma letra que o Spark não documenta e que não significa o que parece.

**11.** **O custo da UDF em Python hoje.** O capítulo diz que uma UDF em Python lança um processo separado e serializa linha a linha. Confira o que mudou.

R: A arquitetura continua a mesma e as saídas ficaram melhores. Conferi no código-fonte a configuração `spark.sql.execution.pythonUDF.arrow.enabled`, introduzida na 3.4.0. A documentação dela é *"Enable Arrow optimization in regular Python UDFs"* e o padrão é `false`.

Ou seja, existe hoje uma otimização com Apache Arrow para UDF Python comum, que troca a serialização linha a linha por lotes colunares. Ela não vem ligada. Existe também `spark.sql.execution.pythonUDF.arrow.concurrency.level`, da 4.0.0.

A conclusão para 2026 é que o conselho do capítulo continua válido na direção, e a magnitude mudou. Built-in primeiro, sempre. Depois UDF em Scala, se o projeto for JVM. Depois UDF Python com Arrow ligado ou pandas UDF. E UDF Python sem Arrow por último.

**12.** **O CBO.** O capítulo diz que a cost-based optimization foi introduzida no Spark 2.2 para deixar o Catalyst mais inteligente na escolha de join. Confira se ela está ligada.

R: Está desligada. Conferi `spark.sql.cbo.enabled` no código-fonte da tag v4.0.0. A versão é `2.2.0`, o que confirma a data do capítulo, e a linha final é `.createWithDefault(false)`. A documentação é curta: *"Enables CBO for estimation of plan statistics when set true."*

O mesmo vale para o reordenamento de join, `spark.sql.cbo.joinReorder.enabled`, também da 2.2.0 e também com padrão `false`.

Isso muda a leitura da seção Logical Plan. O capítulo apresenta as otimizações rule-based e as cost-based como dois conjuntos que rodam. As rule-based rodam. As cost-based não rodam a menos que alguém ligue a configuração e rode `ANALYZE TABLE` para coletar as estatísticas de coluna que o próprio capítulo lista. Quatro anos depois do livro, a escolha inteligente de join no Spark vem do AQE, do item 4, e não do CBO.

**13.** **O pivot sem lista de valores.** O capítulo diz que o Spark gasta esforço para descobrir os valores distintos. Ache o limite.

R: Existe um teto e o capítulo não o menciona. A configuração é `spark.sql.pivotMaxValues`, presente desde a 1.6.0, com `.createWithDefault(10000)`. A documentação diz que ela é o número máximo de valores distintos coletados quando o pivot é feito sem lista.

O comportamento acima do teto é falha, não truncamento. Ou seja, um pivot sobre uma coluna com mais de dez mil valores distintos não devolve uma tabela gigante, ele quebra. Isso reforça o conselho da Listing 4-19 por um motivo diferente do que o capítulo dá: não é só velocidade, é a diferença entre rodar e não rodar.

Na mesma família apareceu `spark.sql.transposeMaxValues`, novidade da 4.0.0 com padrão 500, o que indica que o Spark ganhou uma operação de transpose desde o livro.

**14.** **Os hints hoje.** A Listing 4-33 usa `/*+ MAPJOIN(departments) */`. Confira se isso ainda funciona.

R: Continua valendo. A página de hints da 4.2.0 diz: *"The aliases for `BROADCAST` are `BROADCASTJOIN` and `MAPJOIN`."* O `MAPJOIN` aparece inclusive nos exemplos oficiais.

O que mudou é que o `BROADCAST` deixou de ser o único hint de join. Hoje existem `MERGE`, com os apelidos `SHUFFLE_MERGE` e `MERGEJOIN`, além de `SHUFFLE_HASH` e `SHUFFLE_REPLICATE_NL`.

Um aviso que o capítulo não dá e a documentação dá: *"Note that there is no guarantee that Spark will choose the join strategy specified in the hint since a specific strategy may not support all join types."* Ou seja, o hint é um pedido, não uma ordem.

**15.** **Os modos de `explain`.** A Table 4-7 lista cinco modos. Confira se o conjunto mudou.

R: Não mudou. O Scaladoc de `Dataset` na 4.2.0 lista exatamente os cinco, com as mesmas descrições da Table 4-7: `simple` imprime só o physical plan, `extended` imprime os dois, `codegen` imprime o physical plan e o código gerado se disponível, `cost` imprime o logical plan e as estatísticas se disponíveis, e `formatted` divide a saída em esboço e detalhes.

O `explain(mode: String)` é marcado como introduzido na 3.0.0, o que confirma a afirmação do capítulo.

Existe uma dependência escondida. O modo `cost` só é útil se houver estatística, e estatística de coluna só existe se o CBO estiver ligado e o `ANALYZE TABLE` tiver rodado, conforme o item 12. Ou seja, um dos cinco modos vem vazio na configuração padrão.

**16.** **O Tungsten hoje.** O capítulo diz que a whole-stage code generation é visível pelo asterisco. Confira se ela ainda vem ligada por padrão.

R: Continua ligado. Conferi `spark.sql.codegen.wholeStage` no código-fonte, versão 2.0.0, com `.createWithDefault(true)`. A documentação diz que, quando true, o stage inteiro, com vários operadores, é compilado em um único método Java.

O que mudou desde o livro não é o Tungsten, é o que veio depois dele. A leitura do plano continua igual, com asterisco marcando os operadores fundidos e o `Exchange` quebrando a cadeia. Essa parte do capítulo envelheceu bem.

### Fontes consultadas

Todas acessadas em 3 de agosto de 2026. Todas são fonte primária: documentação oficial do projeto, referência de API, guia de migração ou código-fonte com tag fixa.

Apache Spark, documentação 4.2.0:

- [SQL Syntax: JOIN, com a gramática de join_type](https://spark.apache.org/docs/4.2.0/sql-ref-syntax-qry-select-join.html)
- [SQL Syntax: Join Hints, com os apelidos de BROADCAST e o aviso de que o hint não é garantia](https://spark.apache.org/docs/4.2.0/sql-ref-syntax-qry-select-hints.html)
- [SQL Syntax: GROUP BY, com a equivalência de ROLLUP e CUBE para GROUPING SETS](https://spark.apache.org/docs/4.2.0/sql-ref-syntax-qry-select-groupby.html)
- [Performance Tuning, com as estratégias de join, o AQE e o skew join](https://spark.apache.org/docs/4.2.0/sql-performance-tuning.html)
- [SQL Migration Guide, com a troca de SimpleDateFormat por DateTimeFormatter no Spark 3.0](https://spark.apache.org/docs/4.2.0/sql-migration-guide.html)
- [Datetime Patterns for Formatting and Parsing](https://spark.apache.org/docs/4.2.0/sql-ref-datetime-pattern.html)
- [Built-in Functions](https://spark.apache.org/docs/4.2.0/sql-ref-functions-builtin.html)
- [Scaladoc de `org.apache.spark.sql.functions`, com o rsd padrão de approx_count_distinct e o contrato de window](https://spark.apache.org/docs/4.2.0/api/scala/org/apache/spark/sql/functions$.html)
- [Scaladoc de `org.apache.spark.sql.expressions.Window`, com o frame padrão](https://spark.apache.org/docs/4.2.0/api/scala/org/apache/spark/sql/expressions/Window$.html)
- [Scaladoc de `org.apache.spark.sql.Dataset`, com os cinco modos de explain](https://spark.apache.org/docs/4.2.0/api/scala/org/apache/spark/sql/Dataset.html)

Código-fonte do Apache Spark, tag `v4.0.0`:

- [`SQLConf.scala`, com os defaults de autoBroadcastJoinThreshold, preferSortMergeJoin, adaptive.enabled, cbo.enabled, pivotMaxValues, codegen.wholeStage, legacy.timeParserPolicy e pythonUDF.arrow.enabled](https://raw.githubusercontent.com/apache/spark/v4.0.0/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala)
- [`functions.scala`, com as anotações de depreciação de sumDistinct, toRadians e toDegrees, e as versões de session_window, window_time, array_agg, grouping e grouping_id](https://raw.githubusercontent.com/apache/spark/v4.0.0/sql/api/src/main/scala/org/apache/spark/sql/functions.scala)

Defaults de configuração e nomes de função mudam entre releases. Preciso reconferir estes dados antes de usá-los em prova ou em decisão técnica.

---

## Termos-chave para definir antes de seguir adiante

Escreva uma definição de uma linha para cada, com suas próprias palavras, sem consultar:

aggregation function · `count` · `countDistinct` · `approx_count_distinct` · HyperLogLog · cardinality estimation · skewness · kurtosis · variance · standard deviation · `groupBy` · `RelationalGroupedDataset` · coluna categórica · cardinalidade · `agg` · column expression · `collect_list` · `collect_set` · pivot · join expression · join type · inner join · left outer join · right outer join · full outer join · left anti-join · left semi-join · cross join · fact table · dimension table · self-join · coluna ambígua · shuffle · shuffle hash join · broadcast hash join · join hint · função built-in · UDF · rollup · cube · grouping set · tumbling window · sliding window · window specification · frame · `partitionBy` · `orderBy` · `rowsBetween` · `rangeBetween` · `unboundedPreceding` · `currentRow` · ranking function · analytic function · Catalyst optimizer · logical plan · physical plan · rule-based optimization · cost-based optimization · constant folding · project pruning · predicate pushdown · `explain` · Project Tungsten · off-heap memory · whole-stage code generation

Qualquer termo que você não conseguir definir é alvo de releitura, não item de Nível 5.

### Minhas definições

Vinte e um dos sessenta e cinco termos o capítulo usa sem definir, define errado, ou adia para outro capítulo. Marquei esses casos em itálico.

**aggregation function** — Função que opera sobre um conjunto de linhas e devolve um valor. No Spark, toda agregação é feita por função, seja sobre o DataFrame inteiro, seja sobre um subgrupo.

**`count`** — Agregação que devolve o número de itens por grupo. Com nome de coluna, ignora null. Com `*`, conta todas as linhas.

**`countDistinct`** — Agregação que conta apenas os itens únicos por grupo. Exata e cara.

**`approx_count_distinct`** — Agregação que estima o número de itens únicos com um erro configurável, padrão 0.05. *O capítulo apresenta o parâmetro como erro máximo aceitável, e a documentação o define como desvio padrão relativo, conforme o item 19 do Nível 4.*

**HyperLogLog** — *Nomeado e não explicado.* O capítulo diz que é um algoritmo conhecido para o problema de cardinality estimation, dá um link da Wikipédia e não descreve o mecanismo.

**cardinality estimation** — O problema de aproximar a contagem de itens distintos. É o nome formal do que o `approx_count_distinct` resolve.

**skewness** — Medida da simetria da distribuição dos valores. Zero em distribuição normal, positiva quando a cauda direita é mais longa e negativa no caso oposto.

**kurtosis** — Medida do formato da curva da distribuição. Positiva quando a curva é esbelta e pontuda, negativa quando é gorda e achatada.

**variance** — Medida da dispersão dos valores em relação à média. O Spark oferece a versão amostral, `var_samp`, e a populacional, `var_pop`.

**standard deviation** — A raiz quadrada da variância, na mesma unidade dos dados. Também vem em versão amostral e populacional.

**`groupBy`** — Transformation que agrupa as linhas por uma ou mais colunas. Diferente das outras, ela devolve um `RelationalGroupedDataset` e não um DataFrame.

**`RelationalGroupedDataset`** — A classe que representa um DataFrame agrupado e ainda não reduzido. Oferece `avg`, `count`, `mean`, `min`, `max`, `sum`, `agg`, `pivot`, `rollup` e `cube`.

**coluna categórica** — Coluna de baixa cardinalidade, cujos valores nomeiam categorias. Os exemplos do capítulo são gênero, idade, cidade e país.

**cardinalidade** — *Usada sem definição.* O capítulo diz que colunas categóricas têm baixa cardinalidade e nunca explica o termo. É o número de valores distintos de uma coluna.

**`agg`** — Função do `RelationalGroupedDataset` que recebe uma ou mais column expressions e aplica várias agregações de uma vez. Aceita também um mapa de string para string.

**column expression** — Expressão que produz uma coluna a partir de outras. Toda função de agregação devolve uma instância de `Column`, o que permite encadear `as` para renomear.

**`collect_list`** — Agregação que junta os valores da coluna do grupo em uma coleção, com duplicatas. *O capítulo o chama de `collection_list` três vezes na prosa, conforme o item 3 do Nível 4.*

**`collect_set`** — Agregação que junta os valores únicos da coluna do grupo em uma coleção. *O capítulo o chama de `collection_set` uma vez na prosa.*

**pivot** — Operação que transforma os valores distintos de uma coluna categórica em nomes de coluna, aplicando agregações. *O Summary do capítulo inverte a definição, conforme o item 10 do Nível 4.*

**join expression** — Predicado booleano que diz quais colunas de cada lado determinam quais linhas entram no resultado. Pode ser passado ao `join` ou expresso com `where`.

**join type** — O que determina quais linhas entram no resultado além das que casam. O padrão no Spark SQL é `inner`.

**inner join** — Devolve as linhas dos dois datasets em que a join expression é true. Também chamado equi-join quando a expressão é uma igualdade.

**left outer join** — Devolve o inner join mais as linhas da esquerda sem correspondência, com null nas colunas da direita.

**right outer join** — O espelho do anterior, preservando as linhas da direita sem correspondência.

**full outer join** — Combina o resultado do left outer join com o do right outer join. O capítulo o chama de outer join na Table 4-2 e de full outer join no título da seção.

**left anti-join** — Devolve só as linhas da esquerda que não casam, e só as colunas da esquerda. Não existe versão right.

**left semi-join** — Devolve só as linhas da esquerda que casam, e só as colunas da esquerda. É o oposto do anti-join.

**cross join** — Combina cada linha da esquerda com cada linha da direita. Invocado pela transformation dedicada `crossJoin`, e não por string de tipo.

**fact table** — *Nomeada de passagem, sem definição.* O capítulo a apresenta como o dataset transacional do exemplo de e-commerce, com quais clientes compraram quais produtos.

**dimension table** — *Nomeada de passagem, sem definição.* No exemplo, o dataset com a informação de cada cliente.

**self-join** — *Nomeado uma vez e não exemplificado.* Juntar um DataFrame com ele mesmo. O capítulo o cita só para dizer que ali a técnica de nome de coluna de junção não funciona.

**coluna ambígua** — Situação em que o DataFrame resultante tem duas colunas de mesmo nome. Projetar uma delas dispara `AnalysisException`.

**shuffle** — Redistribuição de linhas entre partitions pela rede, com serialização e desserialização. *O capítulo o usa como verbo e substantivo sem nunca defini-lo, herdando a lacuna do Capítulo 3.*

**shuffle hash join** — Estratégia que hasheia a coluna de junção dos dois lados e move as linhas de mesmo hash para a mesma partition. *O capítulo a apresenta como a escolha para dois datasets grandes, e o padrão do Spark é o sort-merge join, conforme o item 2 do Nível 5.*

**broadcast hash join** — Estratégia que envia uma cópia do dataset menor para cada partition do maior, evitando o shuffle do maior. *O critério real é a configuração `spark.sql.autoBroadcastJoinThreshold`, de 10 MB, e não caber na memória do executor, conforme o item 3 do Nível 5.*

**join hint** — Instrução passada ao optimizer para forçar uma estratégia. Na API é a função `broadcast`. Em SQL é um comentário de hint, como o `MAPJOIN(departments)` da Listing 4-33.

**função built-in** — Função que recebe uma ou mais colunas da mesma linha e devolve uma coluna. O capítulo conta mais de 200 e as agrupa em oito categorias.

**UDF** — User-defined function, função escrita por você, registrada no Spark e enviada aos executors. Em Scala e Java roda nativamente, em Python exige um processo separado.

**rollup** — Agregação multidimensional que respeita a hierarquia das colunas, produzindo subtotais e total geral. O total aparece com null em todas as colunas de grupo.

**cube** — Agregação sobre todas as combinações das colunas de grupo. Nunca tem menos linhas que o rollup equivalente.

**grouping set** — *Ausente do capítulo.* É o conceito de que rollup e cube são atalhos, e é o que permite escrever combinações arbitrárias. Verifiquei a equivalência no item 7 do Nível 5.

**tumbling window** — Janela de tempo fixa, sem sobreposição, em que cada registro entra em um bucket só. *Nomeada e adiada para o Capítulo 6.*

**sliding window** — Janela de tempo que desliza por um intervalo menor que o comprimento dela, então um registro entra em vários buckets. *Também adiada para o Capítulo 6.*

**window specification** — A definição do agrupamento lógico sobre o qual uma window function é avaliada. Tem três componentes: partition by, order by e frame.

**frame** — A fronteira da window na linha corrente, ou seja, quais linhas entram no cálculo daquela linha. *O capítulo descreve o frame padrão de forma incompleta, omitindo que ele é do tipo RANGE, conforme o item 8 do Nível 5.*

**`partitionBy`** — Componente da window specification que diz por quais colunas agrupar as linhas. Não confundir com a função homônima do writer, do Capítulo 3.

**`orderBy`** — Componente da window specification que diz como ordenar as linhas dentro de cada partition. É o que dá sentido a rank, lag e lead.

**`rowsBetween`** — Define o frame por índice de linha, contando posições relativas à linha corrente.

**`rangeBetween`** — Define o frame pelo valor real da expressão do order by, e não por posição. *A prosa da página 38 escreve o nome com uma letra a mais.*

**`unboundedPreceding`** — Fronteira que representa a primeira linha da partition, equivalente a `UNBOUNDED PRECEDING` em SQL.

**`currentRow`** — Fronteira que representa a linha corrente. O capítulo recomenda escrever deslocamentos relativos a ela em vez de números soltos.

**ranking function** — Window function que atribui uma posição a cada linha do frame. São `rank`, `dense_rank`, `percent_rank`, `ntile` e `row_number`.

**analytic function** — Window function que olha outras linhas do frame para compor o valor da linha corrente. São `cume_dist`, `lag` e `lead`.

**Catalyst optimizer** — O query optimizer do Spark SQL, que traduz a lógica do usuário em logical plan, otimiza, converte em physical plan e gera código.

**logical plan** — Representação interna da lógica do usuário, numa árvore de operadores e expressões. Existe em três versões: parsed, analyzed e optimized.

**physical plan** — O plano com os operadores que casam com o engine de execução. É dele que sai o bytecode Java.

**rule-based optimization** — Otimização aplicada por regra fixa, sem olhar estatística. Inclui constant folding, project pruning e predicate pushdown.

**cost-based optimization** — Otimização que usa estatísticas de coluna para escolher, entre planos equivalentes, o mais barato. *O capítulo a apresenta como ativa, e `spark.sql.cbo.enabled` tem padrão false, conforme o item 12 do Nível 5.*

**constant folding** — *Nomeado e não explicado.* É avaliar em tempo de planejamento as expressões cujos operandos são todos constantes.

**project pruning** — *Nomeado e não explicado no texto, mas demonstrado.* É descartar do plano as colunas que a saída não usa. Aparece no `ReadSchema` da Listing 4-58, com duas das três colunas do arquivo.

**predicate pushdown** — Empurrar a condição de filtro para a origem, que filtra antes de entregar. Aparece como `PushedFilters` na saída de `explain("formatted")`.

**`explain`** — Função da classe DataFrame que imprime os planos. Com `true` imprime as quatro seções, e desde o Spark 3.0 aceita uma string com cinco modos.

**Project Tungsten** — Iniciativa nascida em 2015 para aproximar o Spark dos limites do hardware moderno, melhorando o uso de memória e CPU.

**off-heap memory** — *Nomeada como uma das três iniciativas do Tungsten e não explicada.* É gerenciar memória fora do heap da JVM, para escapar do modelo de objeto e do garbage collection.

**whole-stage code generation** — Combinar vários operadores em uma única função compilada, para eliminar chamadas de função virtual. O asterisco no plano físico marca os operadores em que ela está ligada.
