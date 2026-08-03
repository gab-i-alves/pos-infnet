# Guia de Leitura — *Learning Spark*, 2ª edição, Capítulo 2: Downloading Apache Spark and Getting Started

**Fonte:** Jules S. Damji, Brooke Wenig, Tathagata Das, Denny Lee. *Learning Spark: Lightning-Fast Data Analytics*, 2ª ed. O'Reilly, 2020. Capítulo 2, "Downloading Apache Spark and Getting Started".

**Escopo:** os Níveis 1 a 4 são respondíveis apenas com este capítulo. O Nível 5 não é, porque exige verificação externa e pertence a um backlog de estudo.

**Sobre as figuras:** abri as páginas do PDF e li as imagens, em vez de trabalhar só com o texto extraído. Onde uma resposta descreve conteúdo de figura, ela veio da imagem.

---

## Como usar este guia

1. Leia o capítulo uma vez de ponta a ponta, sem parar em nenhuma questão. Rode as transcrições de shell se tiver Spark instalado, mas não na primeira passagem.
2. Responda o Nível 1 de memória antes de reabrir o capítulo. Depois releia para preencher as lacunas. Toda questão que falhou marca uma seção como alvo de releitura, e o ponteiro diz onde ela está.
3. Escreva os Níveis 2 e 3 em frases completas. Uma resposta que só dá para gesticular oralmente não é resposta. As questões do Nível 2 testam especificamente se dá para enunciar um mecanismo sem pegar emprestada a formulação dos autores.
4. O Nível 4 é a seção que conecta o que os autores mantiveram separado: a listagem de diretórios e a hierarquia de execução, as figuras e a prosa, as definições do Step 3 e as saídas do Spark UI. Espere que estas levem mais tempo que os Níveis 1 a 3 somados.
5. O Nível 5 vai para o backlog de estudo, não para as notas. Nada ali é fato sobre o estado atual do Spark até ser verificado contra fonte primária.

---

## Nível 1 — Memorização e definições

**1.** Qual execution mode o capítulo usa do início ao fim, qual propriedade declarada o torna bom para aprender, e quais dois deployment modes ele nomeia para trabalho real com dados grandes? *(Chapter introduction)*

R: O local mode, em que todo o processamento acontece numa única máquina, dentro de um Spark shell. A propriedade é o quick feedback loop para executar operações Spark de forma iterativa. Para dados grandes ou trabalho real, o capítulo indica os deployment modes YARN ou Kubernetes.

**2.** Quais linguagens o capítulo diz que o Spark shell suporta, e qual linguagem suportada ele diz que serve para escrever aplicações mas não para usar no shell? *(Chapter introduction)*

R: O shell suporta Scala, Python e R. A linguagem de fora é Java: dá para escrever uma aplicação Spark nela, mas não usá-la no shell.

**3.** Qual package type a página de download manda selecionar no passo 2, e qual é o nome exato do tarball resultante? *(Step 1; Figure 2-1)*

R: "Pre-built for Apache Hadoop 2.7". O tarball é `spark-3.0.0-preview2-bin-hadoop2.7.tgz`.

**4.** Qual release do Spark está selecionado no drop-down da Figura 2-1, e qual é a data dele? *(Figure 2-1)*

R: O release selecionado é **3.0.0-preview2**, com data de **23 de dezembro de 2019**. A nota logo abaixo da figura confirma que o Spark 3.0 ainda estava em preview quando o livro foi para a gráfica.

**5.** Quais quatro itens aparecem no painel "Latest News" da Figura 2-1, com suas datas? *(Figure 2-1)*

R: O painel "Latest News" traz quatro entradas: duas preview releases do Spark 3.0, de dezembro e de novembro de 2019, o Spark 2.3.4 de setembro de 2019, e o Spark 2.4.4, também de setembro de 2019. As quatro datam a captura no fim de 2019, o que ajuda a calibrar a defasagem do capítulo.

**6.** Segundo a nota no rodapé da página de download, com qual versão de Scala o Spark vem pré-construído, e qual release é a exceção, e com o quê? *(Figure 2-1)*

R: A nota diz que o Spark vem pré-construído com **Scala 2.11**, e que a exceção é a versão **2.4.2**, pré-construída com Scala 2.12. O Luu, no capítulo 2 do outro livro, traz a mesma nota já atualizada, acrescentando que o Spark 3.0+ usa Scala 2.12. No item 2 do Nível 5 verifiquei a nota equivalente na página de download atual, que hoje fala de Scala 2.13 e do fim do suporte ao 2.12.

**7.** Em que estado estava o Apache Spark 3.0 quando o livro foi para a gráfica, e o que o NOTE diz para fazer no lugar? *(Step 1, NOTE)*

R: O Spark 3.0 ainda estava em preview mode. O NOTE diz que dá para baixar o Spark 3.0 mais recente pelo mesmo método e pelas mesmas instruções.

**8.** Desde qual release do Spark o PySpark pode ser instalado do PyPI, qual é o comando, e qual benefício concreto o capítulo alega para quem só programa em Python? *(Step 1)*

R: Desde o Apache Spark 2.2. O comando é `pip install pyspark`. O benefício é não precisar instalar todas as outras bibliotecas necessárias para rodar Scala, Java ou R, o que deixa o binário menor.

**9.** Quais três grupos opcionais de dependência podem ser instalados junto com o PySpark, e quais são os dois comandos de exemplo que o capítulo dá? *(Step 1)*

R: SQL, ML e MLlib. Os comandos são `pip install pyspark[sql,ml,mllib]` e `pip install pyspark[sql]`.

**10.** Qual versão de Java o capítulo exige, e qual variável de ambiente precisa estar definida? *(Step 1, NOTE)*

R: Java 8 ou superior, com a variável `JAVA_HOME` definida.

**11.** O que é preciso instalar e rodar para usar R de forma interativa, e qual projeto open source o capítulo nomeia para computação distribuída com R, e quem o criou? *(Step 1)*

R: É preciso instalar o R e depois rodar `sparkR`. O projeto nomeado é o `sparklyr`, criado pela comunidade R.

**12.** Quais sistemas operacionais o capítulo assume, e qual é o comando exato dado para extrair o tarball? *(Spark's Directories and Files)*

R: Linux ou macOS. O comando é:

```
tar -xf spark-3.0.0-preview2-bin-hadoop2.7.tgz
```

**13.** Liste todas as entradas devolvidas pelo `ls` no diretório da distribuição extraída. *(Spark's Directories and Files)*

R: São quinze entradas: `LICENSE`, `NOTICE`, `R`, `README.md`, `RELEASE`, `bin`, `conf`, `data`, `examples`, `jars`, `kubernetes`, `licenses`, `python`, `sbin`, `yarn`.

**14.** Para que o capítulo diz que o `README.md` traz instruções? Nomeie as quatro coisas. *(Spark's Directories and Files → README.md)*

R: O capítulo lista cinco, não quatro: usar os Spark shells, construir o Spark a partir do fonte, rodar exemplos standalone do Spark, consultar links para a documentação e para os guias de configuração, e contribuir com o Spark. A pergunta pede quatro e o texto entrega cinco.

**15.** Quais quatro executáveis de shell moram em `bin`, e para quais duas outras coisas o capítulo diz que `bin` será usado mais adiante no capítulo? *(Spark's Directories and Files → bin)*

R: Os shells são `spark-sql`, `pyspark`, `spark-shell` e `sparkR`. As duas outras utilidades são submeter uma aplicação Spark standalone com `spark-submit`, e escrever um script que constrói e publica imagens Docker ao rodar Spark com suporte a Kubernetes.

**16.** Qual é o propósito declarado de `sbin`, e qual tabela de qual capítulo ele referencia para detalhes de deployment mode? *(Spark's Directories and Files → sbin)*

R: A maioria dos scripts ali é administrativa, para iniciar e parar componentes do Spark no cluster nos vários deployment modes. A referência cruzada é a Table 1-1 do Capítulo 1.

**17.** Desde qual release existe o diretório `kubernetes`, e que dois tipos de conteúdo ele guarda? *(Spark's Directories and Files → kubernetes)*

R: Desde o Spark 2.4. Guarda Dockerfiles para criar imagens Docker da distribuição num cluster Kubernetes, e um arquivo com instruções de como construir a distribuição Spark antes de construir as imagens.

**18.** Que tipo de arquivo popula o diretório `data`, e quais três componentes do Spark consomem esses arquivos? *(Spark's Directories and Files → data)*

R: Arquivos `*.txt`. Os componentes são MLlib, Structured Streaming e GraphX.

**19.** Quais quatro linguagens têm exemplos de código no diretório `examples`, e quais dois "imperativos" o capítulo alega que facilitam aprender uma plataforma nova? *(Spark's Directories and Files → examples)*

R: Java, Python, R e Scala. Os dois imperativos são muitos exemplos de código no formato "how-to" e documentação abrangente.

**20.** Nomeie os quatro interpretadores que o capítulo chama de "widely used", e a quais shells familiares ele compara a interatividade deles. *(Step 2)*

R: `pyspark`, `spark-shell`, `spark-sql` e `sparkR`. A comparação é com os shells de Python, Scala, R, SQL, ou shells Unix como o bash e o Bourne shell.

**21.** Com quais duas capacidades os shells foram aumentados, além do que um interpretador comum faz? *(Step 2)*

R: Conectar ao cluster, e carregar dados distribuídos na memória dos Spark workers.

**22.** Como se inicia o PySpark a partir da distribuição extraída, e o que muda se o PySpark tiver vindo do PyPI? *(Step 2)*

R: Da distribuição, é preciso ir ao diretório `bin` e digitar `pyspark`. Se a instalação veio do PyPI, digitar `pyspark` de qualquer lugar já basta.

**23.** Nas transcrições de shell, qual versão de Python, de Scala e de Spark aparecem, e o que `spark.version` devolve em cada shell? *(Step 2)*

R: Python 3.7.3, Scala 2.12.10 e Spark 3.0.0-preview2. No `pyspark`, `spark.version` devolve `'3.0.0-preview2'`. No `spark-shell`, devolve `res0: String = 3.0.0-preview2`.

**24.** Na transcrição do `spark-shell`: qual variável guarda o Spark context, qual é o valor de master, e em que endereço a web UI é reportada? *(Step 2)*

R: A variável é `sc`. O master é `local[*]`. A web UI é reportada em `http://10.0.1.7:4040`.

**25.** Como as computações Spark são expressas, em que elas são convertidas, e para quem essas coisas são distribuídas? *(Using the Local Machine)*

R: São expressas como operações. As operações são convertidas em bytecode de baixo nível baseado em RDD, na forma de tasks. As tasks são distribuídas aos executors do Spark.

**26.** O que `show(10, false)` faz, e qual é o valor padrão da flag `truncate`? *(Using the Local Machine)*

R: Mostra as dez primeiras linhas sem truncar. O padrão da flag booleana `truncate` é `true`.

**27.** Qual arquivo é lido no primeiro exemplo, e que valor `strings.count()` devolve? *(Using the Local Machine)*

R: O arquivo é `../README.md`. `strings.count()` devolve 109.

**28.** Como se sai de um Spark shell? *(Using the Local Machine)*

R: Com Ctrl-D.

**29.** Desde qual release do Spark os RDDs estão "consigned to low-level APIs", e o que o NOTE diz que acontece com toda computação expressa nas Structured APIs de alto nível? *(Using the Local Machine, final NOTE)*

R: Desde o Spark 2.x. O NOTE diz que toda computação expressa nas Structured APIs é decomposta em operações RDD otimizadas e geradas, de baixo nível, e depois convertida em bytecode Scala para as JVMs dos executors. Esse código RDD gerado não é acessível aos usuários e não é a mesma coisa que a API de RDD voltada ao usuário.

**30.** Dê as definições do capítulo para Application, SparkSession, Job, Stage e Task, e nomeie as duas actions oferecidas como exemplo do que gera um job. *(Step 3, opening definitions)*

R: **Application** é um programa de usuário construído sobre o Spark com suas APIs, composto de um driver program e de executors no cluster. **SparkSession** é um objeto que fornece um ponto de entrada para interagir com a funcionalidade do Spark e permite programar com as APIs. **Job** é uma computação paralela composta de múltiplas tasks, gerada em resposta a uma Spark action. **Stage** é cada um dos conjuntos menores de tasks em que um job se divide, e que dependem uns dos outros. **Task** é uma unidade única de trabalho ou de execução, enviada a um Spark executor. As duas actions citadas como exemplo são `save()` e `collect()`.

**31.** Quem cria a SparkSession num shell interativo e quem a cria numa aplicação, e por qual variável ela fica acessível? *(Spark Application and SparkSession)*

R: No shell interativo, o driver faz parte do shell e instancia a SparkSession automaticamente. Numa aplicação, quem cria o objeto é o programador. A variável é `spark`.

**32.** Quais dois comandos o capítulo diz que mostram como conectar ao cluster manager? *(Spark Application and SparkSession)*

R: `spark-shell --help` e `pyspark --help`.

**33.** Na Figura 2-2, quais três caixas empilhadas formam o bloco da esquerda, o que aparece dentro de cada Cluster Node, e o que a letra T denota? *(Figure 2-2)*

R: O bloco da esquerda tem três caixas empilhadas, de cima para baixo: **Spark Application**, **Spark Driver** e **SparkSession**. O aninhamento é o argumento da figura, porque a SparkSession fica dentro do driver, que fica dentro da aplicação. Cada **Cluster Node** contém uma caixa rotulada **Executors**, e dentro dela quatro círculos. A letra **T** marca alguns desses círculos, e a legenda ao lado define: **Task per Core**. Ou seja, cada círculo é um core e a task ocupa um deles. Setas verdes ligam o bloco da esquerda aos dois cluster nodes nos dois sentidos, e uma seta vertical liga os dois nodes entre si. A figura mostra dois cluster nodes, o que é o mínimo para representar comunicação entre executors.

**34.** Em que o driver converte a aplicação, em que ele transforma cada uma dessas coisas depois, e o que cada nó dessa estrutura pode ser? *(Spark Jobs; Figure 2-3)*

R: O driver converte a aplicação em um ou mais Spark jobs. Depois transforma cada job em um DAG, que é o plano de execução do Spark. Cada nó dentro de um DAG pode ser um Spark stage ou vários.

**35.** Com base em quê os stages são criados, e em que fronteiras o capítulo diz que eles costumam ser delineados? *(Spark Stages; Figure 2-4)*

R: São criados com base em quais operações podem ser executadas em série ou em paralelo. Costumam ser delineados nas computation boundaries do operador, onde ditam a transferência de dados entre os executors.

**36.** A que cada task corresponde, sobre o que ela trabalha, e que número de tasks e de partições o capítulo diz que um executor de 16 cores pode ter em paralelo? *(Spark Tasks; Figure 2-5)*

R: Cada task corresponde a um único core e trabalha sobre uma única partição de dados. Um executor com 16 cores pode ter 16 ou mais tasks trabalhando sobre 16 ou mais partições em paralelo.

**37.** Quais são os dois tipos de operação do Spark, qual é a definição de transformation no capítulo, quais duas operações são os exemplos dela, e que propriedade isso dá aos DataFrames? *(Transformations, Actions, and Lazy Evaluation)*

R: Os dois tipos são transformations e actions. Uma transformation transforma um DataFrame num novo DataFrame sem alterar os dados originais. Os exemplos são `select()` e `filter()`. A propriedade resultante é a imutabilidade.

**38.** O que é uma lineage, quais três coisas uma lineage registrada permite ao Spark fazer mais adiante no plano de execução, e quais dois eventos encerram o adiamento? *(Transformations, Actions, and Lazy Evaluation)*

R: Lineage é o registro das transformations, guardadas em vez de computadas na hora. Ela permite ao Spark rearranjar certas transformations, fundi-las, ou otimizá-las em stages para execução mais eficiente. O adiamento termina quando uma action é invocada, ou quando os dados são "tocados", ou seja, lidos do disco ou escritos nele.

**39.** Quais duas propriedades juntas fornecem tolerância a falhas, e por qual mecanismo o estado original é reproduzido depois de uma falha? *(Transformations, Actions, and Lazy Evaluation)*

R: A lineage e a imutabilidade dos dados. O estado original é reproduzido replaying a lineage registrada, ou seja, repetindo as transformations gravadas.

**40.** Reproduza a Table 2-1 inteira: as cinco transformations e as cinco actions listadas. *(Table 2-1)*

R:

| Transformations | Actions |
|---|---|
| `orderBy()` | `show()` |
| `groupBy()` | `take()` |
| `filter()` | `count()` |
| `select()` | `collect()` |
| `join()` | `save()` |

**41.** No exemplo trabalhado, quais duas operações são as transformations, qual é a action, e que contagem é devolvida nas versões Python e Scala? *(Transformations, Actions, and Lazy Evaluation)*

R: As transformations são `read()` e `filter()`. A action é `count()`. As duas versões devolvem 20.

**42.** Defina narrow e wide dependency nos termos do capítulo. Quais duas operações ele nomeia como narrow, quais duas como wide, e o que ele diz que acontece por partição e no cluster quando `.orderBy()` é chamado? *(Narrow and Wide Transformations; Figure 2-7)*

R: Narrow é qualquer transformation em que uma única partição de saída pode ser computada a partir de uma única partição de entrada, sem troca de dados. Wide é quando dados de outras partições são lidos, combinados e escritos em disco, porque a agregação final depende da saída de outras partições. O capítulo nomeia `filter()` e `contains()` como narrow, e `groupBy()` e `orderBy()` como wide. Com `.orderBy()`, cada partição é ordenada localmente, mas é preciso forçar um shuffle dos dados das partições de cada executor pelo cluster para ordenar todos os registros.

**43.** Em qual porta a web UI do driver roda por padrão, quais cinco tipos de métrica e detalhe o capítulo lista, e em qual URL ela é alcançada em local mode? *(The Spark UI)*

R: Na porta 4040. Os cinco itens são: a lista de scheduler stages e tasks, um resumo dos tamanhos de RDD e do uso de memória, informação sobre o ambiente, informação sobre os executors em execução, e todas as queries Spark SQL. Em local mode, o endereço é `http://<localhost>:4040`.

**44.** Para o DAG do exemplo Python: quantos jobs e stages o driver criou, quais três operações aparecem nas caixas azuis, qual é o status reportado e a contagem de stages concluídos, e por que nenhum `Exchange` é necessário? *(The Spark UI; Figure 2-8)*

R: Um job e um stage. A Figura 2-8 mostra a tela "Details for Job 0", com status `SUCCEEDED`, Associated SQL Query 0 e Completed Stages 1. As três caixas azuis dentro do Stage 0 são `BatchScan`, `WholeStageCodegen` e `mapPartitionsInternal`, nessa ordem, ligadas por setas verticais. Nenhum `Exchange` é necessário porque existe apenas um stage, e o próprio texto abaixo da figura diz isso: sem troca de dados entre executors, não há nó de Exchange. O Stage 0 é composto de uma única task. Um detalhe que a figura entrega e o texto não menciona: a versão no canto é `3.0.0-preview2`, ou seja, a captura é de uma prévia.


**45.** Na visão de detalhe do stage: qual é o tempo total somado das tasks, o resumo de locality level, o id do job associado, e quantos RDDs aparecem no DAG? *(Figure 2-9)*

R: A Figura 2-9 mostra a tela "Details for Stage 0 (Attempt 0)". O tempo total somado das tasks é **0,2 s**. O resumo de locality level é **Process local: 1**. O job associado é o **0**. No DAG aparecem **quatro RDDs**: um `DataSourceRDD [0]` e três `MapPartitionsRDD`, numerados 1, 2 e 3, todos rotulados com a mesma origem de chamada. Eles ficam distribuídos nas três mesmas caixas da Figura 2-8: `BatchScan` contém os dois primeiros, `WholeStageCodegen` contém o terceiro e `mapPartitionsInternal` contém o quarto. Comparar as duas figuras mostra o que muda de nível: a visão de job dá as operações, e a de stage abre cada operação nos RDDs que a implementam.

**46.** Qual capítulo é apontado como o que cobre o Spark UI em detalhe, e como este capítulo caracteriza o papel da UI? *(The Spark UI)*

R: O Capítulo 7. A UI é caracterizada como uma lente microscópica sobre o funcionamento interno do Spark, como ferramenta de debugging e de inspeção.

**47.** O que é a Databricks, em quais quatro linguagens dá para escrever notebooks na Community Edition, que outro formato de notebook pode ser importado, e onde ficam os notebooks do próprio livro? *(Databricks Community Edition sidebar; Figure 2-10)*

R: A Databricks é uma empresa que oferece uma plataforma Apache Spark gerenciada na nuvem. Na Community Edition dá para escrever notebooks em Python, R, Scala ou SQL. Também dá para importar outros notebooks, entre eles Jupyter notebooks. Os notebooks do livro ficam no repositório GitHub dele, importáveis depois do registro em `https://www.databricks.com/try-databricks`.

**48.** Qual é a forma geral do comando para rodar os programas de exemplo que vêm na distribuição, qual é a invocação exata de `JavaWordCount` dada, e qual tarefa o capítulo chama de "Hello, World" da computação distribuída? *(Your First Standalone Application)*

R: A forma geral é `bin/run-example <class> [params]`. A invocação exata é:

```
$ ./bin/run-example JavaWordCount README.md
```

A tarefa chamada de "Hello, World" da computação distribuída é contar palavras.

**49.** Quantas entradas tem o arquivo de M&Ms, quais três campos cada linha guarda, e o que o programa computa? *(Counting M&Ms for the Cookie Monster)*

R: Mais de 100.000 entradas. Cada linha guarda `<state, mnm_color, count>`. O programa computa e agrega as contagens para cada cor e cada estado.

**50.** No Example 2-1, nomeie em ordem as chamadas encadeadas que constroem `count_mnm_df`, os dois argumentos passados a `show()` para o resultado de todos os estados, e como a query da Califórnia difere. *(Example 2-1)*

R: A cadeia é `.select("State", "Color", "Count")`, `.groupBy("State", "Color")`, `.sum("Count")`, `.orderBy("sum(Count)", ascending=False)`. Os argumentos são `n=60` e `truncate=False`. A query da Califórnia acrescenta `.where(mnm_df.State == "CA")` depois do `select`, e exibe com `n=10`.

**51.** Quais dois ajustes de `.option()` são passados ao leitor de CSV, e que restrição o comentário do código enuncia sobre instâncias de SparkSession por JVM? *(Example 2-1)*

R: `.option("header", "true")` e `.option("inferSchema", "true")`. O comentário diz que só pode existir uma SparkSession por JVM.

**52.** Na saída Python: qual é a contagem total de linhas reportada, o estado, a cor e a contagem da primeira linha, e qual é a cor preferida da Califórnia e sua contagem? *(Example 2-1 output)*

R: A contagem reportada é `Total Rows = 60`, que é o número de grupos estado mais cor, não o número de linhas do arquivo. A primeira linha é CA, Yellow, 100.956. A cor preferida da Califórnia é o amarelo, com 100.956.

**53.** O que precisa ser copiado para onde, e qual propriedade precisa ser ajustada para qual valor, para suprimir as mensagens INFO verbosas? *(Counting M&Ms for the Cookie Monster)*

R: Copiar `log4j.properties.template` para `log4j.properties` e ajustar `log4j.rootCategory=WARN` no arquivo `conf/log4j.properties`. No item 5 do Nível 5 verifiquei que tanto o nome do arquivo quanto a sintaxe da propriedade mudaram.

**54.** Qual variável de ambiente precisa apontar para a raiz da instalação, e quais são as invocações exatas de `spark-submit` para o script Python e para o jar Scala? *(Counting M&Ms; Building Standalone Applications in Scala)*

R: A variável é `SPARK_HOME`. As invocações são:

```
$SPARK_HOME/bin/spark-submit mnmcount.py data/mnm_dataset.csv
```

```
$SPARK_HOME/bin/spark-submit --class main.scala.chapter2.MnMcount \
  jars/main-scala-chapter2_2.12-1.0.jar data/mnm_dataset.csv
```

**55.** No Example 2-3: o nome do pacote, a versão, a versão de Scala, e as duas dependências de biblioteca com sua versão. *(Example 2-3)*

R: `name := "main/scala/chapter2"`, `version := "1.0"`, `scalaVersion := "2.12.10"`. As dependências são `"org.apache.spark" %% "spark-core" % "3.0.0-preview2"` e `"org.apache.spark" %% "spark-sql" % "3.0.0-preview2"`.

**56.** Qual comando único constrói a aplicação, que tempo total o log reporta, e qual é o nome do jar produzido? *(Building Standalone Applications in Scala)*

R: O comando é `sbt clean package`. O log reporta `Total time: 6 s`. O jar é `main-scala-chapter2_2.12-1.0.jar`.

**57.** Por que o capítulo pula a etapa de build no caso do Python, e para onde ele remete quem precisa construir programas Java com Spark? *(Building Standalone Applications in Scala, NOTE)*

R: Porque Python é interpretado e não tem etapa de compilação, ainda que dê para compilar em bytecode `.pyc`. Para construir programas Java com Maven, o capítulo remete ao guia no site do Apache Spark.

**58.** No Example 2-2: os nomes de pacote e de objeto, os dois imports, e as funções usadas para ordem descendente e para referência de coluna. *(Example 2-2)*

R: Pacote `main.scala.chapter2`, objeto `MnMcount`. Os imports são `org.apache.spark.sql.SparkSession` e `org.apache.spark.sql.functions._`. A ordem descendente usa `desc(...)` e a referência de coluna usa `col(...)`.

**59.** Quais três passos o Summary diz que o capítulo cobriu? *(Summary)*

R: Baixar o framework, familiarizar-se com o shell interativo Scala ou PySpark, e dominar os conceitos e termos de alto nível de uma aplicação Spark.

---

## Nível 2 — Compreensão

Explique cada um com suas próprias palavras, de três a seis frases. Não reutilize a formulação do capítulo.

**1.** Por que o local mode dá um "quick feedback loop", e por que as mesmas propriedades que produzem esse loop o tornam inadequado para conjuntos de dados grandes?

R: No local mode tudo roda numa máquina só, dentro de um único processo. Não há negociação de recursos com um cluster manager, nem lançamento de executors remotos, nem tráfego de dados pela rede. Por isso o intervalo entre digitar uma expressão e ver o resultado é de segundos. As mesmas condições impõem o teto: a capacidade da máquina é toda a capacidade disponível, e acrescentar máquinas não muda nada, porque nada é distribuído. O capítulo traça a linha ele mesmo, entre prototipar com conjuntos pequenos e trabalho real com dados grandes.

**2.** Por que instalar o PySpark do PyPI produz um binário menor, e o que o capítulo diz que se abre mão ao seguir esse caminho?

R: O pacote do PyPI carrega só o que é preciso para programar em Python. As bibliotecas necessárias para rodar Scala, Java e R ficam de fora, e é daí que vem a diferença de tamanho. O capítulo não diz o que se perde. Ele apresenta a escolha só como ganho. O que se perde é dedutível do restante do texto: sem a distribuição extraída não existe diretório `bin` para entrar, nem `run-example`, nem `sbin`, nem os shells das outras linguagens.

**3.** Qual é a divisão de trabalho entre o driver program e a SparkSession, e por que o capítulo faz questão de dizer que o shell cria uma para o usuário?

R: O driver é o processo que roda no núcleo da aplicação e cria o objeto SparkSession. A SparkSession é o ponto de entrada pelo qual o código alcança a funcionalidade do Spark. Um é processo, o outro é objeto. A observação sobre o shell explica por que as transcrições chamam `spark.read` sem preparação nenhuma, e marca a diferença para uma aplicação, em que o Example 2-1 precisa construir a sessão com `SparkSession.builder`. Sem essa distinção, as quatro linhas extras do script pareceriam arbitrárias.

**4.** Explique o mecanismo pelo qual a lazy evaluation habilita otimização. O que exatamente o Spark faz com uma lineage registrada que não conseguiria fazer se cada transformation executasse na hora?

R: Cada transformation apenas grava um passo na lineage em vez de produzir dados. Quando a action chega, o Spark tem a cadeia inteira diante de si e pode rearranjar passos, fundi-los e agrupá-los em stages. Execução imediata destruiria essa possibilidade, porque cada chamada teria de entregar um resultado concreto e não haveria mais nada a reordenar. Além disso, cada intermediário seria materializado, mesmo os que a otimização eliminaria. O adiamento é o que dá ao otimizador acesso ao futuro da query.

**5.** Explique como a imutabilidade e a lineage produzem tolerância a falhas em conjunto, e por que a imutabilidade é pré-condição e não benefício colateral.

R: Toda transformation devolve um DataFrame novo e deixa a entrada intacta. Como as entradas permanecem, um passo perdido pode ser refeito a partir do que veio antes, e o resultado é o mesmo de antes. A lineage guarda a sequência desses passos, então recuperar é repetir a gravação. Se as transformations alterassem os dados no lugar, a entrada do passo perdido já teria sido consumida, e repetir a gravação produziria outra coisa. A imutabilidade é a condição que torna o registro replayable, não um bônus.

**6.** Explique por que transformations narrow não exigem troca de dados e as wide exigem, em termos da relação entre partição de entrada e partição de saída.

R: Numa transformation narrow, cada partição de saída é computada a partir de uma única partição de entrada. Tudo que a task precisa já está na máquina onde ela roda, então nada trafega. Numa transformation wide, uma partição de saída depende de dados que estão espalhados por várias partições de entrada, que vivem em outros executors. Produzir esse resultado obriga a ler de outras partições, combinar e escrever em disco. O critério inteiro é a cardinalidade dessa relação.

**7.** Por que as fronteiras de stage ficam onde ficam? Ligue a expressão do capítulo sobre computation boundaries ao que ele diz que os stages ditam sobre transferência de dados.

R: Um stage é um trecho de trabalho que avança sem depender de outras máquinas. Enquanto cada task só precisa da própria partição, o trabalho segue sem parar. Quando um operador precisa de dados de outras partições, o avanço trava até que o cluster mova os bytes. É exatamente nesse ponto que o capítulo põe o corte, e por isso diz que os stages ditam a transferência de dados entre executors. A fronteira é o instante de sincronização, e o custo dela é o shuffle.

**8.** Dados os mapeamentos task-para-core e task-para-partição, explique o que de fato determina quanto paralelismo um job Spark alcança.

R: Cada task ocupa um core e trabalha sobre uma partição. Logo, o número de tasks que podem rodar ao mesmo tempo é o total de cores disponíveis, e o número de tasks que existem é o número de partições. O paralelismo efetivo é o menor dos dois. Com mais cores que partições, cores ficam ociosos. Com mais partições que cores, as tasks rodam em ondas. Nenhum dos dois números sozinho determina o resultado.

**9.** Por que nada executa até uma action ser invocada, e que papel o query plan cumpre entre as chamadas de transformation e a execução?

R: As transformations só descrevem um dataset novo em função de um existente. Descrição não exige dados. A action pede um valor que não é um DataFrame, e esse valor não pode ser produzido sem ler dados de verdade. O query plan é o artefato entre as duas coisas: ele acumula as operações à medida que as chamadas acontecem, e nada nele executa antes da action. Quando ela chega, o plano é otimizado e executado de uma vez.

**10.** Explique a cadeia completa de decomposição, de aplicação a jobs, DAG, stages e tasks, e identifique qual delas é a unidade de escalonamento e qual é a unidade de execução.

R: O driver converte a aplicação em um ou mais jobs, um por action. Cada job vira um DAG, que é o plano de execução, e os nós do DAG são stages. Cada stage é composto de tasks, distribuídas aos executors. A unidade de execução é a task, e essa é a palavra do próprio capítulo. A unidade de escalonamento é o stage, porque é ele que precisa terminar para o seguinte começar, e a UI lista "scheduler stages and tasks" nessa ordem. O termo unidade de escalonamento é minha leitura, o capítulo não o usa.

**11.** Explique a relação que o NOTE final de "Using the Local Machine" descreve entre as Structured APIs e os RDDs, e por que o código RDD gerado não é a mesma coisa que a API de RDD voltada ao usuário.

R: Toda computação escrita nas Structured APIs é decomposta em operações RDD otimizadas e geradas, e depois convertida em bytecode Scala para as JVMs dos executors. Os RDDs continuam sendo o substrato de execução, mesmo quando ninguém os escreve. O código gerado não é a API de RDD porque tem outra origem e outro propósito: sai do otimizador, é moldado ao plano específico daquela query, e muda quando o plano muda. A API voltada ao usuário é escrita por uma pessoa e executada como foi escrita. O NOTE ainda acrescenta que o código gerado não é acessível.

**12.** O que distingue uma transformation de uma action em princípio, em vez de listar qual é qual? Use `filter()` e `count()` como caso de teste.

R: A distinção é o que a operação devolve e o que isso obriga. Uma transformation devolve um DataFrame e por isso pode ser descrita sem tocar em dado nenhum, acrescentando um passo ao plano. Uma action devolve algo que não é um DataFrame, e esse valor só existe se os dados forem lidos. `filter()` devolve um DataFrame novo, encadeia-se e não computa nada. `count()` devolve um número, e um número exige percorrer os dados, então dispara tudo que estava registrado.

**13.** Explique a alegação do capítulo de que a DataFrame API permite dizer ao Spark *o quê* fazer em vez de *como*. Que decisões ela tira das mãos do programador em relação à API de RDD?

R: No Example 2-1 o programa nomeia colunas, agrupamentos, agregação e ordem. Ele não escreve laço, não decide particionamento, não escolhe estratégia de join nem ordem física dos passos. Essas decisões passam para o otimizador. Em relação à API de RDD, o que se entrega é a possibilidade de mandar código arbitrário para rodar sobre as partições e de controlar os passos físicos. O capítulo destaca a clareza que se ganha em troca, e o NOTE final de Step 2 mostra o preço: o código que de fato roda não é acessível.

**14.** Por que o capítulo descreve o Spark UI como uma "microscopic lens"? Que tipo de problema a decomposição em jobs, stages e tasks permite diagnosticar que um stack trace não permitiria?

R: A UI mostra a aplicação já decomposta nos níveis em que o Spark realmente trabalha, com detalhe por stage. Um stack trace só serve quando algo lança exceção, e aponta a linha de código, não o trabalho. Muito problema de Spark não é erro, é lentidão ou desequilíbrio: um stage que domina o tempo, um shuffle que ninguém pediu, tasks de uma mesma etapa com durações muito diferentes. Nada disso aparece no código, porque a lazy evaluation separa a linha escrita do momento da execução. A decomposição é o que devolve essa correspondência.

**15.** Por que a paridade de API entre Scala e Python é apresentada como conquista da evolução do Spark desde a 1.x, e não como requisito óbvio de projeto?

R: O Spark nasceu em Scala, e as demais linguagens vieram depois, cada uma como camada sobre um núcleo que não foi desenhado para elas. Manter as assinaturas equivalentes é trabalho recorrente, feito de novo a cada release e a cada API nova. O capítulo situa a paridade entre as melhorias duradouras desde a 1.x, o que registra que ela não existia no começo. Chamar de conquista é honesto quanto a essa história. Os próprios exemplos do capítulo mostram que a paridade é direção e não estado, já que a versão Scala e a Python diferem em vários pontos de sintaxe.

**16.** Por que o exemplo Scala precisa de arquivo de build e de etapa de compilação e o Python não, e o que `sbt clean package` realiza conceitualmente?

R: Scala compila para bytecode da JVM, e o `spark-submit` recebe um jar mais o nome de uma classe com `--class`. Sem compilar não existe jar nem classe para apontar. Python é interpretado, então o `spark-submit` aceita o arquivo `.py` diretamente. `sbt clean package` faz quatro coisas: descarta a saída de build anterior, resolve as dependências declaradas no `build.sbt`, compila os fontes, e empacota as classes num jar cujo nome segue o `name` e a `version` declarados.

**17.** Por que a inferência de schema é uma opção e não o comportamento padrão, e que custo `inferSchema` implica para um arquivo de mais de 100.000 linhas?

R: Um CSV não carrega tipos. Descobrir que uma coluna é inteira exige olhar os valores, ou seja, ler o arquivo. Por isso a inferência não pode ser gratuita nem automática: ela custa uma passagem sobre os dados antes de qualquer trabalho útil. Num arquivo de mais de 100.000 linhas isso significa ler o arquivo duas vezes, uma para descobrir os tipos e outra para o job. A alternativa é declarar o schema, que custa escrita mas não leitura. O capítulo liga a opção e não menciona nenhum dos dois custos.

**18.** O resultado da Califórnia é computado por uma segunda cadeia separada sobre `mnm_df`, em vez de reaproveitar `count_mnm_df`. Explique o que o próprio modelo de laziness e lineage do capítulo implica sobre o trabalho feito duas vezes aqui.

R: `mnm_df` não é dado guardado, é um plano registrado. A segunda cadeia parte desse mesmo plano, então tudo que ele contém acontece de novo: a leitura do CSV, a passagem de inferência de schema e a projeção. Depois disso a segunda cadeia ainda repete o `groupBy` e a ordenação, que são wide e obrigam a dois shuffles. O resultado da Califórnia é um subconjunto de seis linhas de algo já computado no primeiro `show()`. Pelo modelo do próprio capítulo, o programa paga o arquivo inteiro duas vezes para obtê-las. O capítulo tem os conceitos para enxergar isso e não comenta, e nunca menciona `cache()` ou `persist()`.

---

## Nível 3 — Aplicação e transferência

**1.** Você tem 30 dias de logs de scraper em JSONL, 4,2 GB, chegando em 96 partições. Precisa, por spider, da contagem de respostas HTTP 403 ordenada da pior para a melhor. Escreva a cadeia seguindo a forma do Example 2-1, depois classifique cada operação como narrow ou wide, preveja onde cai uma fronteira de stage, e diga quantos jobs a execução produz. *(Narrow and Wide Transformations; Spark Stages; Example 2-1)*

R: A cadeia, na forma do Example 2-1:

```python
logs_df = (spark.read.format("json")
  .option("inferSchema", "true")
  .load(logs_path))

count_403_df = (logs_df
  .select("spider", "status")
  .where(logs_df.status == 403)
  .groupBy("spider")
  .count()
  .orderBy("count", ascending=False))

count_403_df.show(n=50, truncate=False)
```

Classificação: `select()` é narrow e `where()` é narrow, porque cada partição de saída sai de uma única partição de entrada. `groupBy("spider").count()` é wide, porque todas as ocorrências de um spider precisam terminar juntas. `orderBy()` é wide, pelo mesmo motivo que o capítulo dá: cada partição é ordenada localmente e ainda assim é preciso um shuffle pelo cluster. `show()` é a action.

Fronteiras de stage: duas, uma em cada transformation wide. Pelo modelo do capítulo, o corte fica onde o operador exige dados de outras partições, então a execução tem três stages. O primeiro lê, projeta, filtra e agrega parcialmente as 96 partições de entrada. O segundo fecha a agregação depois do shuffle. O terceiro produz a ordem global.

Jobs: um, porque só existe uma action. Aqui o modelo do capítulo é frágil. O próprio log do run Scala mostra `Job 4 finished: show at MnMcount.scala`, o que revela que numa execução real o Spark gera mais jobs por action do que a regra "um job por action" sugere.

**2.** Um cluster tem 6 executors com 8 cores cada, e o DataFrame de entrada tem 32 partições. Usando apenas a regra task/core/partição do capítulo, quantas tasks rodam ao mesmo tempo, quantas ondas o stage leva, e o que muda se a entrada for reparticionada para 96? *(Spark Tasks)*

R: A capacidade é de 6 × 8 = 48 tasks simultâneas. Com 32 partições existem apenas 32 tasks, então 32 rodam ao mesmo tempo e o stage leva uma onda só. Dezesseis cores ficam ociosos, e a capacidade extra não acelera nada, porque não há trabalho para eles. Com 96 partições existem 96 tasks, 48 rodam por vez e o stage leva duas ondas exatas. O paralelismo passa a ser limitado pelos cores e não pelas partições, e o cluster deixa de desperdiçar capacidade.

**3.** Um colega diz que o notebook dele "trava no `count()`" depois de vinte chamadas encadeadas de `select()` e `filter()` que retornaram instantaneamente. Usando apenas o relato de laziness do capítulo, explique o que está de fato acontecendo e qual parte da UI abrir primeiro. *(Transformations, Actions, and Lazy Evaluation; The Spark UI)*

R: As vinte chamadas retornaram rápido porque não computaram nada. Cada uma gravou um passo na lineage e devolveu um DataFrame novo. O `count()` é a action, e é ele que dispara a cadeia inteira, incluindo a leitura dos dados. O tempo que aparece nele é o custo de todo o pipeline, não da contagem. Nada está travado, o trabalho está começando ali. A primeira parada é a UI na porta 4040, no DAG Visualization do job, para ver em quantos stages a cadeia se decompôs e onde há troca de dados. Depois, a aba Stages, para ver qual stage domina o tempo.

**4.** Você encadeia `filter()` → `select()` → `orderBy()` → `count()` sobre um DataFrame de 500 partições. Preveja o número de stages, e diga em que ponto um nó `Exchange` apareceria na visualização do DAG e por quê. *(Narrow and Wide Transformations; Figure 2-8)*

R: Dois stages. `filter()` e `select()` são narrow e cabem no mesmo stage, porque cada partição de saída sai de uma partição de entrada e nada precisa atravessar a rede. `orderBy()` é wide: ordenar todos os registros exige forçar um shuffle das partições de cada executor pelo cluster. O `Exchange` aparece exatamente ali, entre o fim do trabalho local sobre as 500 partições e a etapa que produz a ordem global. É o contraste com a Figura 2-8, onde não havia `Exchange` porque havia um stage só.

**5.** Alguém roda os programas M&M em Python e em Scala sem passar o argumento de arquivo. Trace o que acontece em cada um, dada a posição da checagem de argumento em relação à construção da SparkSession. Qual ordenação é mais segura, e por quê? *(Examples 2-1 and 2-2)*

R: No Python, a checagem `if len(sys.argv) != 2` vem antes de qualquer coisa relacionada ao Spark. O programa imprime `Usage: mnmcount <file>` em stderr e sai com código -1. Nenhuma SparkSession é criada e nenhum recurso é pedido. No Scala, a `SparkSession` é construída primeiro e só depois vem o `if (args.length < 1)`. A sessão sobe, a mensagem é impressa e o programa chama `sys.exit(1)` sem passar pelo `spark.stop()`. A ordenação do Python é mais segura por duas razões: falha antes de alocar qualquer coisa, e não deixa sessão pendurada num caminho de saída. A versão Scala paga a inicialização inteira para descobrir que a entrada estava faltando.

**6.** Num laptop de 4 cores em local mode, você lê um arquivo que produz 12 partições. Dado o valor de master mostrado na transcrição do `spark-shell` e a regra task/core do capítulo, descreva como as 12 partições são processadas. *(Step 2; Spark Tasks)*

R: A transcrição mostra `master = local[*]`, ou seja, o Spark usa todos os cores disponíveis, que são 4. O stage tem 12 tasks, uma por partição, e cada task ocupa um core. Quatro tasks rodam ao mesmo tempo. Assim que uma termina, o core recebe a próxima da fila. São três ondas de quatro, supondo partições de custo parecido. Se uma partição for muito maior que as outras, a última onda fica esperando por ela, e o stage inteiro dura o que durar a task mais lenta.

**7.** Pedem que você mova o job de M&M para um cluster Kubernetes. Nomeie todo diretório e script da listagem da distribuição que se torna relevante, e diga o que o capítulo afirma que cada um fornece. *(Spark's Directories and Files)*

R: O capítulo descreve três dos relevantes. O diretório `kubernetes` contém os Dockerfiles para criar imagens Docker da distribuição num cluster Kubernetes, mais um arquivo com instruções de como construir a distribuição antes de construir as imagens. O diretório `bin` fornece o `spark-submit`, usado para submeter a aplicação, e o próprio capítulo diz que ali também se escreve o script que constrói e publica imagens Docker ao rodar Spark com suporte a Kubernetes. O diretório `sbin` fornece os scripts administrativos para iniciar e parar componentes do Spark nos vários deployment modes, e remete à Table 1-1 do Capítulo 1.

Três outros entram por caminhos que o capítulo cita sem descrever. O `conf` aparece na instrução de logging, como lugar do `log4j.properties` que a imagem precisa carregar. O `jars` aparece no caminho do `spark-submit` da versão Scala. O `data` aparece como origem do `mnm_dataset.csv`, e num cluster ele deixa de servir, porque o arquivo precisa estar num storage que todos os executors alcancem. O capítulo não trata desse último ponto.

**8.** Você já tem uma sessão `spark-shell` aberta e inicia uma segunda. A porta 4040 está ocupada. O que o capítulo diz sobre como descobrir o endereço da UI da segunda sessão? *(The Spark UI, NOTE; Step 2)*

R: O NOTE diz que, ao lançar o `spark-shell`, parte da saída mostra a URL de localhost a acessar na porta 4040. A transcrição do Step 2 confirma, com a linha `Spark context Web UI available at http://10.0.1.7:4040`. Então a resposta do capítulo é ler a saída de inicialização da segunda sessão, que traz o endereço que ela de fato conseguiu. O capítulo não diz o que acontece quando a porta está ocupada, nem menciona incremento de porta. No item 6 do Nível 5 verifiquei que o Spark tenta portas seguintes até `spark.port.maxRetries`.

**9.** Um colega instalou o PySpark com `pip` e não encontra um diretório `bin` para entrar. Responda à dúvida dele usando só o Step 2. *(Step 2)*

R: Não existe diretório `bin` a encontrar, porque não houve distribuição extraída. O Step 2 diz que entrar em `bin` e digitar `pyspark` é o caminho de quem baixou o tarball, e que, se o PySpark veio do PyPI, digitar `pyspark` já basta. O comando fica no PATH do ambiente Python. É só rodar `pyspark` de qualquer diretório.

**10.** Um executor morre no meio de um stage e o Spark reproduz o DataFrame perdido a partir da lineage. Enuncie a suposição sobre os dados de origem que essa recuperação exige em silêncio, e diga se o capítulo a enuncia. *(Transformations, Actions, and Lazy Evaluation)*

R: A suposição é que a fonte continua lá e continua igual. Replaying a lineage significa refazer os passos a partir da origem, então a origem precisa ser legível de novo e devolver os mesmos registros. Se o arquivo tiver sido sobrescrito, movido, ou se a fonte for um stream não rebobinável, o replay produz outro resultado ou nenhum. O capítulo não enuncia isso. Ele fala em imutabilidade dos DataFrames entre transformations, que cobre os intermediários, e nada diz sobre o dado no disco, que está fora da lineage.

**11.** Seus logs de CI estão inundados de saída INFO do `spark-submit`. Prescreva a correção do capítulo, e nomeie o arquivo que precisa existir para ela ter efeito e onde ele fica. *(Counting M&Ms for the Cookie Monster)*

R: A correção do capítulo é copiar `log4j.properties.template` para `log4j.properties` e ajustar `log4j.rootCategory=WARN`. O arquivo que precisa existir é `conf/log4j.properties`, no diretório `conf` da raiz da instalação apontada por `SPARK_HOME`. O template já vem na distribuição, e o arquivo efetivo não, então sem a cópia nada muda. No item 5 do Nível 5 verifiquei que hoje o arquivo é `conf/log4j2.properties` e a propriedade é outra.

**12.** Você precisa prototipar um parser para um corpus de 200 GB. Argumente a partir das afirmações do próprio capítulo se o Spark shell em local mode é lugar aceitável para desenvolvê-lo, e identifique exatamente onde o capítulo traça a fronteira. *(Chapter introduction; Step 2)*

R: É lugar aceitável para desenvolver a lógica, e não para rodar o corpus. A introdução diz que o shell serve para prototipar operações Spark com conjuntos pequenos antes de escrever uma aplicação complexa. O Step 2 reforça que os shells são propícios ao aprendizado rápido e à prototipação rápida. A fronteira está na mesma frase da introdução: para conjuntos grandes ou trabalho real, em que se quer colher os benefícios da execução distribuída, o local mode não é adequado, e o caminho é YARN ou Kubernetes. O critério é o tamanho do dado sobre o qual se roda, não a complexidade do parser. Desenvolver contra uma amostra no shell e submeter o corpus com `spark-submit` num dos dois deployment modes respeita exatamente essa linha.

**13.** Você precisa mudar o exemplo Scala para outra versão do Spark e renomear o artefato. Diga quais linhas do Example 2-3 mudam, e preveja o nome do jar resultante a partir do padrão de nomenclatura do log de build. *(Example 2-3; Building Standalone Applications in Scala)*

R: Mudam três coisas. A linha `name := "main/scala/chapter2"` recebe o novo nome do artefato. As duas linhas de `libraryDependencies` recebem a nova versão do Spark, no lugar de `"3.0.0-preview2"`. A linha `scalaVersion := "2.12.10"` muda junto, porque cada versão do Spark publica artefatos para versões específicas de Scala, e é o `%%` que anexa a versão binária ao nome do artefato.

O padrão do log é nome do pacote com as barras viradas em hífens, mais `_` e a versão binária de Scala, mais `-` e a versão do projeto: `main/scala/chapter2` com Scala 2.12 e versão 1.0 produziu `main-scala-chapter2_2.12-1.0.jar`. Com `name := "mnmcount"`, `scalaVersion := "2.13.18"` e `version := "1.0"`, o jar sai como `mnmcount_2.13-1.0.jar`. No item 11 do Nível 5 verifiquei quais versões de Scala o Spark publica hoje.

---

## Nível 4 — Análise e síntese

**1.** O capítulo descreve o layout de diretórios da distribuição e a hierarquia de execução da aplicação em seções separadas que nunca se referenciam. Desenhe as duas, depois mapeie cada entrada de diretório no ponto do ciclo de vida da aplicação em que ela é usada. Quais entradas não têm lugar nenhum na hierarquia, e o que isso diz sobre por que a listagem está de fato organizada?

R: As duas estruturas:

```
LAYOUT DA DISTRIBUICAO (estatico, no disco)
    LICENSE  NOTICE  licenses  RELEASE  README.md
    bin  sbin  conf  jars
    data  examples  python  R
    kubernetes  yarn

HIERARQUIA DE EXECUÇÃO (dinâmica, por aplicação)
    application
        `-- job          (um por action)
              `-- DAG    (plano de execução)
                    `-- stage   (nó do DAG)
                          `-- task   (um core, uma partição)
```

O mapeamento por momento do ciclo de vida:

| Entrada | Momento em que atua |
|---|---|
| `sbin` | antes da aplicação, sobe e derruba componentes do cluster |
| `kubernetes` | antes da aplicação, na construção das imagens |
| `bin` | submissão, lança shell ou `spark-submit` |
| `conf` | inicialização, define logging e configuração |
| `jars` | inicialização, forma o classpath de driver e executors |
| `python`, `R` | inicialização, bindings de linguagem carregados pelo driver |
| `yarn` | tempo de execução, quando o deployment mode é YARN |
| `data` | tempo de execução, só quando um exemplo é a entrada |
| `examples` | fora da hierarquia, material didático |
| `README.md` | fora da hierarquia, documentação |
| `LICENSE`, `NOTICE`, `licenses`, `RELEASE` | fora da hierarquia, metadado da distribuição |

Seis entradas não têm lugar nenhum na hierarquia. Quatro são metadado legal e de release, uma é documentação e uma é material de exemplo. O que isso revela é que a listagem não está organizada por papel em tempo de execução. Ela está organizada pelo que um release da Apache precisa embarcar: obrigações legais, documentação, bindings por linguagem, ativos por alvo de deployment e amostras de aprendizado. As duas seções não se referenciam porque descrevem eixos diferentes, e o capítulo nunca diz isso. Ele também descreve apenas seis das quinze entradas, e as seis escolhidas são justamente as que o leitor vai tocar no capítulo.

**2.** Compare as Figuras 2-5 e 2-8. Diga o que cada uma mostra que a outra omite, depois nomeie a única coisa ausente das duas que a prosa ao redor trata como central.

R: A Figura 2-5 é um diagrama de decomposição, todo em caixas e setas: Driver aponta para vários Job, um Job aponta para Stage, reticências indicam mais stages, e o último Stage aponta para vários Task. Ela mostra a hierarquia como conceito e a cardinalidade de um para muitos em cada degrau. A Figura 2-8 é uma captura da Spark UI para um caso concreto, com Job 0, status `SUCCEEDED` e um único Stage 0 contendo três operações reais.

O que cada uma omite é o oposto da outra. A 2-5 não tem números, nomes de operação nem status, porque é um esquema e não uma execução. A 2-8 não tem a hierarquia, porque com um job e um stage só a estrutura de um para muitos fica invisível: quem olha só ela não descobre que um job pode ter vários stages.

A coisa ausente das duas que a prosa trata como central é o **executor**. A 2-5 vai de Stage direto a Task e a legenda fala em tasks "to be distributed to executors", mas nenhum executor é desenhado. A 2-8 mostra operações, não onde elas rodam. Quem desenha executor é a Figura 2-2, e ela está a cinco páginas de distância.

A Figura 2-5 mostra o eixo físico: um stage criando uma ou mais tasks para serem distribuídas aos executors. Ela omite quais operações essas tasks executam, então o trabalho aparece como quantidade sem conteúdo. A Figura 2-8 mostra o eixo lógico: o DAG do exemplo Python, com as operações do stage em caixas azuis. Ela omite os executors, os cores e a multiplicidade de tasks, então o plano aparece como conteúdo sem custo. Uma tem a forma do trabalho sem o que ele faz, a outra tem o que ele faz sem a forma.

O que falta nas duas é a partição. A prosa a trata como central em toda parte: a task trabalha sobre uma única partição, narrow e wide são definidas pela relação entre partição de entrada e de saída, e o shuffle é movimento entre partições. Mesmo assim ela não é a unidade de nenhuma das duas figuras. A Figura 2-8 é ainda mais pobre nesse aspecto, porque o exemplo tem um stage e uma task só, e por isso não podia ilustrar nem a distribuição nem o particionamento.

**3.** Concilie "each task maps to a single core" com "an executor with 16 cores can have 16 or more tasks working on 16 or more partitions in parallel". As duas afirmações estão no mesmo parágrafo. Qual leitura torna o par consistente, e o que precisa ser acrescentado ao modelo para chegar lá?

R: A leitura que concilia separa duas contagens que o parágrafo mistura. Uma é quantas tasks ocupam cores num dado instante: dezesseis, uma por core, e nunca mais que isso. A outra é quantas tasks o executor processa ao longo do stage: dezesseis ou mais, porque o stage tem tantas tasks quantas forem as partições, e elas passam pelos cores em ondas. O "16 ou mais" é a segunda contagem, e a palavra "in parallel" na mesma frase pertence à primeira. Escritas juntas, sugerem simultaneidade onde há apenas sequência sobre recursos fixos.

O que falta no modelo do capítulo para sustentar essa leitura é uma fila. Ele nunca diz que o número de tasks de um stage é dado pelo número de partições, nem que tasks esperam core livre, nem que um stage pode levar várias ondas. Sem esses três elementos, a única leitura literal disponível é a de que um core roda mais de uma task ao mesmo tempo, o que contradiz a primeira frase. Acrescentando a fila, o par vira uma descrição correta e o "ou mais" ganha sentido.

**4.** A seção do Spark UI explica que nenhum `Exchange` foi necessário *porque* havia apenas um stage. A seção de narrow e wide implica a direção inversa de causalidade. Enuncie as duas direções, decida qual delas a teoria do próprio capítulo exige, e diga o que a outra direção implicaria se levada a sério.

R: Direção A, da seção do UI: existe um stage só, logo não há `Exchange`. A contagem de stages explica a ausência da troca de dados. Direção B, da seção de narrow e wide: o Spark quebra a computação em stages ao determinar quais operações exigem shuffle ou exchange, logo a necessidade de troca explica a contagem de stages.

A teoria do capítulo exige a direção B. Os stages são criados a partir do que as operações precisam, e o corte cai na computation boundary onde dados atravessam a rede. A necessidade de shuffle é propriedade do operador e das partições, não consequência de uma contagem de stages que alguém definiu antes.

Levar a direção A a sério inverteria causa e efeito, e produziria a ideia de que é possível eliminar um shuffle mantendo tudo num stage só. Isso é falso pela definição de wide dependency: se a partição de saída depende de várias de entrada, os dados têm que se mover, e nenhuma escolha de plano faz isso desaparecer. Na Figura 2-8 as duas direções coincidem, porque a query só tem operações narrow. A frase do capítulo é uma observação correta sobre aquele caso específico, escrita como se fosse regra geral.

**5.** O Step 3 define job como "a parallel computation consisting of multiple tasks". As Figuras 2-8 e 2-9 mostram um job com um stage e uma task. Concilie a definição com a evidência, e diga se a definição precisa de conserto ou se o exemplo é atípico.

R: A definição precisa de conserto e o exemplo é representativo, não atípico. O que a definição erra é a palavra "multiple", que descreve o caso comum como se fosse condição necessária. Pela mecânica do próprio capítulo, o número de tasks de um stage vem do número de partições, e nada impede que ele seja um. O arquivo lido é o `README.md` da distribuição, um texto pequeno, que produz uma partição só num único JVM local. O job resultante tem um stage e uma task porque a mecânica manda, não porque o exemplo fugiu à regra.

Um enunciado que sobrevive à evidência seria: um job é a computação disparada por uma action, decomposta em um ou mais stages, cada um com uma ou mais tasks. Ele preserva o que importa na definição original, que é a origem na action e a decomposição em tasks, e para de prometer pluralidade. O exemplo é atípico apenas em escala, e essa escala é a que o capítulo escolheu para o capítulo inteiro, já que o local mode com arquivos pequenos é a premissa declarada.

**6.** Trace a escada de abstração do RDD ao DataFrame e ao DSL parecido com SQL do Example 2-1. Em cada degrau, nomeie o que se ganha e de que controle se abre mão. Use o NOTE que diz que o código RDD gerado é inacessível como evidência do custo no degrau mais alto.

R: O padrão é o mesmo em toda a escada: cada degrau troca controle sobre o *como* por concisão sobre o *quê*, e transfere decisão de execução para o otimizador.

No **RDD** o controle é máximo. É a camada em que o capítulo diz que o Spark de fato executa, e a única em que o programador escolhe a forma da computação. O preço é que nada entre o código e a execução melhora o que foi escrito, e o capítulo assume isso ao contrastar a clareza do DataFrame com a API de RDD, onde é preciso dizer como fazer.

No **DataFrame** ganha-se estrutura, colunas nomeadas e a possibilidade de encadear porque as funções devolvem o mesmo objeto. Entrega-se o controle físico. O plano físico, a ordem dos passos e o particionamento passam a ser decisão do otimizador, e a lazy evaluation existe justamente para lhe dar espaço.

No **DSL do Example 2-1** ganha-se legibilidade quase de query: `select`, `groupBy`, `sum`, `orderBy`, `where`. O programa declara a intenção inteira em cinco chamadas e nenhuma linha diz como agregar ou como ordenar. Entrega-se a visibilidade. O NOTE final do Step 2 fecha o argumento: toda computação escrita nas Structured APIs é decomposta em operações RDD geradas e otimizadas, e esse código não é acessível ao usuário nem é igual à API de RDD pública. No topo da escada, o que roda não pode ser lido nem ajustado à mão. A única janela sobre ele é a UI, e é por isso que o capítulo a chama de lente microscópica.

**7.** A lazy evaluation é apresentada puramente como benefício. Reformule-a como custo, usando apenas material da seção do Spark UI e do enquadramento de debugging ao fim dela.

R: Como custo, a lazy evaluation quebra a correspondência entre a linha escrita e o trabalho executado, e transfere o diagnóstico do código para uma ferramenta externa.

Três efeitos saem do material da seção. Primeiro, o tempo mente. Vinte transformations retornam num piscar e a action carrega o custo de todas, então a medição feita no código aponta sempre para o lugar errado. Segundo, a decomposição deixa de ser legível no programa. Para saber em quantos jobs e stages a cadeia virou, e se houve `Exchange`, é preciso abrir o DAG Visualization, porque o texto do programa não diz. Terceiro, a inspeção passa a exigir infraestrutura: uma UI servida na porta 4040 pelo driver, viva só enquanto a aplicação viver.

O enquadramento final da seção confirma o custo sem nomeá-lo. A UI é apresentada como lente microscópica para debugging e inspeção. Uma lente microscópica só é necessária quando o objeto não é visível a olho nu. A lazy evaluation é o que torna o objeto invisível, e o capítulo apresenta a lente como recurso, e não como compensação.

**8.** Monte uma tabela comparando os quatro shells, `pyspark`, `spark-shell`, `spark-sql` e `sparkR`, com colunas para linguagem, como é lançado, quais variáveis de sessão e de contexto ele expõe, e se o capítulo o demonstra. Depois identifique quais células o capítulo deixa em branco, e se as lacunas são acidentais ou refletem as prioridades do capítulo.

R:

| Shell | Linguagem | Como é lançado | Variáveis expostas | Demonstrado |
|---|---|---|---|---|
| `pyspark` | Python | `pyspark` no `bin`, ou de qualquer lugar se veio do PyPI | `spark` (SparkSession) | sim, transcrição completa mais dois exemplos |
| `spark-shell` | Scala | `spark-shell` no `bin` | `sc` (Spark context, `master = local[*]`) e `spark` (SparkSession) | sim, transcrição completa mais dois exemplos |
| `spark-sql` | SQL | *não informado* | *não informado* | não |
| `sparkR` | R | instalar o R e rodar `sparkR` | *não informado* | não |

As lacunas se concentram em duas linhas inteiras. Do `spark-sql` o capítulo não diz nada além do nome, nem como lançar, nem o que ele expõe. Do `sparkR` diz apenas a pré-condição de instalar o R. Nenhum dos dois aparece em transcrição.

As lacunas refletem prioridade declarada, não descuido. O capítulo avisa que cobre exemplos principalmente em Python e Scala, e a introdução diz que espera familiaridade com a linguagem escolhida. Dentro dessa escolha, deixar R e SQL de fora é coerente.

Uma delas cobra caro mesmo assim. O `spark-sql` é o único dos quatro que não é REPL de linguagem de programação, e é ele que produz a contradição do capítulo: a introdução afirma que o shell só suporta Scala, Python e R, e o Step 2 lista quatro shells. Uma frase sobre o que o `spark-sql` é resolveria a lacuna e a contradição de uma vez.

**9.** Ordene as formas de evidência do capítulo, transcrições de shell, figuras esquemáticas, capturas do UI, tabelas de saída do M&M e log de build do sbt, por quanto peso cada uma merece como evidência de que o seu próprio ambiente vai se comportar igual. Para cada uma, diga o que ela deixa de fora.

R: Da mais confiável para a menos:

**1. Transcrições de shell.** Merecem mais peso porque são saída literal de máquina, e porque o insumo delas vem na própria distribuição: `../README.md` é o arquivo do release, e por isso `strings.count() == 109` é reproduzível exatamente por quem baixar aquela versão. Deixam de fora o hardware, o sistema (a transcrição diz `darwin`), a versão de Python e a de Spark, que era um preview. Com outra versão, o `README.md` muda e a contagem muda com ele.

**2. Capturas do Spark UI.** São observação real de uma execução real. Deixam de fora a configuração da máquina e, principalmente, a versão do planner. Os rótulos do DAG são nomes de operadores físicos, que mudam entre releases e entre fontes de dados. O que aparece para um `spark.read.text` não é o que aparece para um CSV com inferência de schema.

**3. Log de build do sbt.** Também é saída literal, e mostra o nome exato do jar produzido, o que sustenta a previsão de nomenclatura. Deixa de fora que tudo nele está fixado numa combinação morta: sbt 1.2.8, Scala 2.12.10, Spark 3.0.0-preview2. Reproduzir o log hoje falha na resolução de dependências, antes de qualquer compilação.

**4. Tabelas de saída do M&M.** São saída de máquina, mas as duas versões impressas se contradizem, com magnitudes e nome de coluna diferentes para a mesma linha. Pelo menos uma delas não foi produzida pelo código impresso ao lado. Isso rebaixa as duas, porque não dá para saber qual. Deixam de fora qual versão do `mnm_dataset.csv` gerou cada uma.

**5. Figuras esquemáticas.** Não são evidência sobre ambiente nenhum. São modelos desenhados para ensinar, e nada nelas é mensurável nem falsificável por uma execução. Deixam de fora tudo que teria peso probatório, e é por isso que ficam por último, apesar de serem o material mais útil para entender.

**10.** Construa o contra-argumento à tese implícita do capítulo de que três passos bastam para começar, usando apenas material do próprio capítulo: a ressalva do local mode, as duas referências cruzadas à Table 1-1 do Capítulo 1, e todo adiamento para um capítulo posterior.

R: O capítulo reúne cinco peças que desmontam a própria tese.

A primeira é a ressalva do local mode, e é a mais forte. A introdução diz que para conjuntos grandes ou trabalho real o local mode não serve, e que o caminho é YARN ou Kubernetes. Os três passos entregam um laptop rodando queries sobre um arquivo pequeno. O que eles não entregam é a única configuração que o capítulo define como trabalho real.

A segunda são as duas referências cruzadas à Table 1-1. Uma aparece em `sbin`, para detalhes dos deployment modes. A outra aparece em "Using the Local Machine", para lembrar quais componentes rodam onde no local mode. Os dois pontos são exatamente os que sustentam o passo 3, e nos dois o capítulo remete para fora de si.

A terceira são os adiamentos internos. As Structured APIs ficam para o Capítulo 3. O query plan fica para o próximo capítulo, e é usado como explicação no mesmo parágrafo em que é adiado. O Spark UI fica para o Capítulo 7, depois de ser a única ferramenta oferecida para enxergar a execução. As APIs do programa M&M ficam para capítulos posteriores, com um "bear with us" explícito.

A quarta são os adiamentos externos. Construir a partir do fonte fica com a documentação. Instalar Java fica com a documentação. Construir programas Java com Maven fica com o site do Apache. O JDK, o sbt, o `JAVA_HOME` e o `SPARK_HOME` são assumidos prontos no momento em que o build acontece.

A quinta é pequena e reveladora: o passo de logging. Para que a saída do primeiro job fique legível, é preciso copiar um template e editar uma propriedade. Um começo de três passos que exige editar configuração para conseguir ler o resultado não é um começo de três passos.

O contra-argumento fecha assim: os três passos entregam um shell interativo e um job local. Eles não entregam execução distribuída, não entregam entendimento do que rodou, e não se sustentam sem o Capítulo 1 antes nem os Capítulos 3 e 7 depois.

**11.** Comprima o argumento do capítulo em uma frase por seção, de modo que remover qualquer frase quebre a cadeia de "baixar um tarball" a "sua primeira aplicação standalone roda".

R:

1. *(Chapter introduction)* Começar exige escolher o local mode, porque ele roda tudo numa máquina e dá o retorno rápido de que o aprendizado precisa, mesmo sendo inadequado para dados grandes.
2. *(Step 1)* Rodar em local mode exige a distribuição pré-construída, baixada como tarball, com Java instalado e `JAVA_HOME` definido.
3. *(Spark's Directories and Files)* O tarball extraído entrega os diretórios que o resto do capítulo usa, e o principal deles é o `bin`, onde ficam os shells e o `spark-submit`.
4. *(Step 2)* Do `bin` sai o shell, que já traz uma SparkSession pronta na variável `spark` e permite executar operações sem escrever aplicação nenhuma.
5. *(Using the Local Machine)* Dentro do shell, ler um arquivo com as Structured APIs devolve um DataFrame e mostra que operações de alto nível viram tasks nos executors sem que ninguém escreva RDD.
6. *(Step 3)* Entender o que aconteceu exige o vocabulário da decomposição, em que a aplicação vira jobs, o job vira um DAG de stages, e o stage vira tasks de um core e uma partição.
7. *(Transformations, Actions, and Lazy Evaluation)* Essa decomposição só é disparada por uma action, porque as transformations apenas gravam uma lineage, e é a lineage que dá otimização e tolerância a falhas.
8. *(Narrow and Wide Transformations)* A lineage vira mais de um stage quando alguma transformation precisa de dados de outras partições, e é o shuffle que corta o job em pedaços.
9. *(The Spark UI)* Como nada disso aparece no código, a UI na porta 4040 é o lugar onde jobs, stages e tasks ficam visíveis.
10. *(Your First Standalone Application; Counting M&Ms)* Com o vocabulário e a ferramenta no lugar, o mesmo código sai do shell e vira arquivo submetido por `spark-submit`, que é a primeira aplicação standalone.
11. *(Building Standalone Applications in Scala)* Em Scala esse arquivo precisa antes virar jar, porque `spark-submit` recebe classe compilada, e é o `build.sbt` que declara versões e dependências para o `sbt clean package`.

Remover qualquer uma quebra a cadeia. Sem a 1, não há razão para o local mode. Sem a 2 e a 3, não há binário nem `bin`. Sem a 4, não há sessão. Sem a 5, não há o que explicar. Sem a 6, 7 e 8, a execução é caixa-preta. Sem a 9, não há como olhar dentro dela. Sem a 10 e a 11, o capítulo termina num shell e nunca chega à aplicação que promete no título.

**12.** Identifique as afirmações feitas sem evidência de suporte, incluindo que a execução de tasks do Spark é "exceedingly parallel", que a paridade de API é "well preserved", e que a saída do run Scala "is the same as for the Python run". Para cada uma, diga que evidência exatamente a resolveria, e verifique se o capítulo contém material que a contradiz.

R:

**1. "Making the execution of Spark's tasks exceedingly parallel."** Superlativo sem parâmetro de comparação nem medida. Resolveria: uma execução com número declarado de partições e de cores, e o tempo de parede em duas configurações diferentes, mostrando como o tempo cai quando os cores aumentam. O capítulo contradiz a afirmação com o único caso que ele mesmo mede: a Figura 2-8 mostra um job, um stage e uma task. Nenhum paralelismo é demonstrado em lugar nenhum do capítulo, e o `master = local[*]` da transcrição é a única pista sobre quantos cores participam.

**2. "In Spark, parity is well preserved across the supported languages, with minor syntax differences."** Avaliação sem critério, com o qualificador "minor" fazendo todo o trabalho. Resolveria: uma comparação API a API, com a lista do que existe numa linguagem e não na outra, e uma definição do que conta como diferença menor. O capítulo contradiz em parte com seus próprios exemplos. `show(10, truncate=False)` contra `show(10, false)`, `ascending=False` contra `desc("sum(Count)")`, `mnm_df.State == "CA"` contra `col("State") === "CA"`, e um `import functions._` que a versão Scala precisa e a Python não. As duas versões também diferem na estrutura, com a checagem de argumento antes da sessão em Python e depois dela em Scala. Isso não é sintaxe, é programa diferente.

**3. "The output is the same as for the Python run."** É a mais direta de checar e a que falha mais feio. Resolveria: as duas saídas impressas lado a lado sobre o mesmo arquivo. O capítulo imprime as duas e elas discordam. A saída Python traz CA/Yellow com 100.956 sob a coluna `sum(Count)`. A saída Scala traz CA/Yellow com 1.807 sob a coluna `Total`. O nome da coluna e a ordem de grandeza divergem. Mais: o Example 2-2 usa `.sum("Count")` e `orderBy(desc("sum(Count)"))`, então o código impresso não produziria uma coluna chamada `Total`. A aritmética diz de onde vem o 1.807, e está no item 16 do Nível 5.

Outras três que considerei:

- "Two imperatives that ease the journey to learning any new platform" apresenta uma tese pedagógica como fato estabelecido.
- "This rapid interactivity with Spark shells is conducive not only to rapid learning but to rapid prototyping" repete a afirmação da introdução sem acrescentar medida.
- "Note the clarity and simplicity with which you can instruct Spark what to do" pede ao leitor que confirme uma avaliação estética do código.

**13.** A imutabilidade é apresentada como a propriedade que torna as transformations seguras. Reformule-a como custo, usando o material de wide transformations e de shuffle, depois diga qual das duas formulações o exemplo M&M de fato demonstra.

R: Como custo, a imutabilidade proíbe atualização no lugar e obriga a produzir dado novo a cada passo. O capítulo dá o mecanismo sem tirar a conclusão: uma transformation devolve um DataFrame novo em vez de alterar o original, e numa transformation wide os dados de outras partições são lidos, combinados e escritos em disco. Escrever em disco é o preço de não poder mutar. Um modelo mutável poderia reagrupar registros no lugar onde estão. O modelo imutável precisa materializar o resultado do reagrupamento como coisa nova. Somando à lazy evaluation, o custo aparece de novo na reexecução: como nada foi guardado, refazer um plano é refazer o trabalho, e o capítulo nunca menciona `cache()`.

O exemplo M&M demonstra as duas formulações, uma de cada lado.

Do lado do benefício, `mnm_df` é usado duas vezes, primeiro pela cadeia de todos os estados e depois pela da Califórnia. Isso só funciona porque a primeira cadeia não alterou nada. É imutabilidade entregando reutilização segura, e é o que o capítulo destaca.

Do lado do custo, essa mesma reutilização é cara. A segunda cadeia relê o CSV, refaz a inferência de schema e paga de novo os dois shuffles do `groupBy` e do `orderBy`, para produzir seis linhas que já estavam calculadas na primeira. O exemplo demonstra o custo com mais força que o benefício, e o capítulo comenta só o benefício.

**14.** Compare as Figuras 2-6 e 2-7. A primeira toma o DataFrame como unidade, a segunda toma a partição. Explique como as duas visões se cruzam numa única chamada de `orderBy()`, e nomeie o que nenhuma das duas mostra sobre onde caem as fronteiras de stage.

R: As duas figuras respondem perguntas diferentes sobre a mesma chamada. A Figura 2-6 toma o DataFrame como unidade e mostra a linha do tempo lógica: DataFrame, seta T, DataFrame, seta T, DataFrame, com uma legenda que define T como transformation e A como action. A segunda faixa da figura termina em A, que é o que dispara tudo. A Figura 2-7 abandona a linha do tempo e desenha o interior de uma passagem: dois painéis lado a lado, Narrow Dependencies à esquerda, com três partições de entrada ligadas uma a uma a três de saída, e Wide Dependencies à direita, com as mesmas três entradas ligadas em cruzamento a todas as saídas.

Num `orderBy()` as duas visões se cruzam assim: pela 2-6 ele é só mais um T, uma seta que produz um novo DataFrame e não computa nada até a action chegar. Pela 2-7 esse mesmo T é o painel da direita, porque ordenar exige que registros de qualquer partição de entrada acabem em qualquer partição de saída. Uma seta na visão lógica esconde um embaralhamento completo na visão física.

O que nenhuma das duas mostra é onde cai a fronteira de stage. A 2-6 trata todo T igual, e a 2-7 mostra o padrão de dependência sem dizer que é justamente ele que corta o job em stages. A informação de que uma dependência wide força fronteira de stage não está em figura nenhuma do capítulo.

A Figura 2-6 mostra a linha do tempo lógica: uma sequência de transformations T, cada uma produzindo um DataFrame novo, todas apenas registradas até a action A disparar. A Figura 2-7 mostra a forma da dependência: narrow, com uma partição de entrada por partição de saída, contra wide, com várias entradas alimentando cada saída.

Numa única chamada de `orderBy()` as duas visões descrevem o mesmo evento em escalas diferentes. Na Figura 2-6 ele é um T, um passo entre dois DataFrames, do mesmo tamanho visual que um `select()`. Na Figura 2-7 ele é o caso wide, um leque de muitas partições convergindo, com dados atravessando executors. O cruzamento é o que a primeira visão esconde: dois T adjacentes podem custar coisas incomparáveis, e o desenho por DataFrame não distingue o passo local do passo que embaralha o cluster. É preciso descer ao nível da partição para ver a diferença.

Nenhuma das duas mostra onde caem as fronteiras de stage. A Figura 2-6 não tem stages, só a cadeia de transformations e a action. A Figura 2-7 mostra a forma da dependência, que é a causa da fronteira, mas não desenha o corte nem o `Exchange`. Falta às duas o passo que liga a teoria à execução: que um `orderBy()` termina um stage e começa outro. Esse passo aparece só na prosa, e a única figura com stages é a 2-8, cujo exemplo não tem nenhuma transformation wide para mostrar.

---

## Nível 5 — Além do capítulo (backlog, não notas)

O capítulo é de 2020 e descreve o Spark 3.0 enquanto ele ainda era preview, o que torna quase todo artefato concreto do Step 1 candidato a defasagem. Verifiquei os itens abaixo contra fonte primária em **2 de agosto de 2026**, quando a versão corrente da documentação era a **4.2.0**. As URLs estão ao fim da seção.

### Afirmações sujeitas a defasagem

**1.** Verifique a estrutura da página de download, as opções dos drop-downs, e a existência do package type "Pre-built for Apache Hadoop 2.7". O tarball `spark-3.0.0-preview2-bin-hadoop2.7.tgz` é artefato de preview.

R: O package type do capítulo não existe mais. O release corrente é o **Spark 4.2.0, de 14 de julho de 2026**, e o diretório oficial de distribuição publica três binários: `spark-4.2.0-bin-hadoop3.tgz`, `spark-4.2.0-bin-hadoop3-connect.tgz` e `spark-4.2.0-bin-without-hadoop.tgz`. Hadoop 2.7 sumiu, restou Hadoop 3 ou o binário sem Hadoop. A documentação corrente confirma o quadro: os downloads vêm pré-empacotados para algumas versões populares de Hadoop, e existe a opção "Hadoop free" para quem quer usar outra versão pelo classpath.

Duas novidades no mesmo diretório merecem registro, porque não existiam em 2020. Há um binário com sufixo `-connect`, e há os tarballs `pyspark_client-4.2.0.tar.gz` e `pyspark_connect-4.2.0.tar.gz`. Os três apontam para o Spark Connect, a separação entre cliente e servidor que o capítulo não podia mencionar.

**2.** Verifique o mínimo de Java 8, a nota de Scala 2.11 com exceção de 2.12, e o Python 3.7 demonstrado. Confira mínimos e máximos suportados hoje para os três.

R: As três informações do capítulo estão mortas. A documentação 4.2.0 é explícita: *"Spark runs on Java 17/21/25, Scala 2.13, Python 3.10+, and R 4.0+ (Deprecated). Java 25 prior to version 25.0.3 support is deprecated as of Spark 4.2.0."* Ela acrescenta que aplicações Scala precisam usar a mesma versão de Scala com que o Spark foi compilado, e que desde o Spark 4.0.0 essa versão é a 2.13.

As quebras vieram todas no Spark 4.0.0: SPARK-45315 removeu JDK 8 e 11 e tornou o JDK 17 padrão, SPARK-45314 removeu Scala 2.12 e tornou a 2.13 padrão, e SPARK-47993 removeu o suporte a Python 3.8. A nota atual da página de download diz que o Spark 4 é pré-construído com Scala 2.13 e que o suporte a 2.12 foi oficialmente descontinuado, com o Spark 3.2 ou superior tendo oferecido distribuição adicional em 2.13.

Consequência prática: uma máquina montada pelas instruções do capítulo, com Java 8 e Python 3.7, não roda o Spark corrente. O máximo também importa, e é onde a atenção costuma faltar: o suporte a Java 25 anterior à 25.0.3 já está depreciado.

**3.** Verifique os extras `pip install pyspark[sql,ml,mllib]`: se esses nomes de grupo ainda existem e se outros foram acrescentados.

R: Os três nomes do capítulo continuam válidos e outros três entraram. A página de instalação do PySpark documenta seis grupos: **sql**, **connect**, **pandas_on_spark**, **ml**, **mllib** e **pipelines**. O `sql` traz pandas e pyarrow, o `connect` traz pandas, pyarrow e a pilha grpcio, o `pandas_on_spark` traz pandas e pyarrow, `ml` e `mllib` trazem numpy, e o `pipelines` reúne as dependências de SQL e de Connect mais pyyaml.

Os três novos contam uma história. `pandas_on_spark` é o Koalas absorvido. `connect` é o cliente do Spark Connect. `pipelines` é a API de Declarative Pipelines, que também aparece como `bin/spark-pipelines` na distribuição. A mesma página fixa o requisito de Python em 3.10 ou superior.

**4.** Verifique o conteúdo do diretório da distribuição. Entradas novas já eram acrescentadas em 2.x e 3.0 pela admissão do próprio capítulo.

R: A listagem de topo só fecha rodando `ls` na distribuição extraída, então este item precisa da GNIX baixar o `spark-4.2.0-bin-hadoop3.tgz` e conferir na máquina. Verifiquei os dois subdiretórios que o capítulo usa, direto do repositório do projeto.

O `bin` cresceu e hoje tem 29 arquivos. Os quatro shells continuam lá: `pyspark`, `spark-shell`, `spark-sql` e `sparkR`. O `spark-submit` e o `run-example` continuam. Entraram `spark-connect-shell`, `spark-pipelines`, `beeline` e o `docker-image-tool.sh`, que é o script de imagens Docker que o capítulo menciona sem nomear.

O `conf` mudou onde importa. Os templates são `log4j2.properties.template`, `log4j2-json-layout.properties.template`, `fairscheduler.xml.template`, `metrics.properties.template`, `spark-defaults.conf.template`, `spark-env.sh.template` e `workers.template`. Nenhum `log4j.properties.template`.

**5.** Verifique a configuração de logging, `conf/log4j.properties` e `log4j.rootCategory=WARN`. Uma migração de backend de logging invalidaria o nome do arquivo e a propriedade.

R: A migração aconteceu, e as duas coisas caíram. O **Spark 3.3.0 migrou de log4j 1 para log4j 2**, via SPARK-37814, listada nos destaques daquele release. A página de configuração atual diz que o logging é configurado por `log4j2.properties`, e o template que vem na distribuição é `conf/log4j2.properties.template`.

A sintaxe da propriedade também mudou, porque log4j2 usa outro formato. O template shipped traz `rootLogger.level = info`, com `rootLogger.appenderRef.stdout.ref = console` e `appender.console.target = SYSTEM_ERR`. A instrução equivalente à do capítulo hoje é copiar `conf/log4j2.properties.template` para `conf/log4j2.properties` e trocar a linha para `rootLogger.level = warn`. O `log4j.rootCategory=WARN` do livro não tem efeito nenhum nesse arquivo.

**6.** Verifique a porta 4040 como padrão da UI, e o conjunto de abas e métricas que a UI expõe.

R: A porta continua. A página de configuração lista `spark.ui.port` com default **4040**, presente desde a versão 0.7.0, e o cluster overview repete que cada driver program tem uma web UI tipicamente na porta 4040. O capítulo acerta aqui.

Duas coisas ele não diz. A primeira responde à questão 8 do Nível 3: `spark.port.maxRetries` tem default **16**, e a documentação explica que cada nova tentativa incrementa em 1 a porta anterior, de modo que o Spark varre um intervalo a partir da porta inicial. Uma segunda sessão, portanto, tende a subir em 4041.

A segunda é a lista de abas, que hoje a documentação enumera: **Jobs, Stages, Storage, Environment, Executors, SQL, Structured Streaming, Streaming (DStreams)** e **JDBC/ODBC Server**. Os cinco itens do capítulo mapeiam em Jobs e Stages, Storage, Environment, Executors e SQL, porque o quinto item, as queries de Spark SQL, corresponde à aba SQL, que é a sexta da lista. As três últimas são posteriores ou específicas de componente.

**7.** Verifique a Databricks Community Edition como oferta gratuita, a URL `try-databricks` e a disponibilidade dos notebooks do livro no repositório GitHub.

R: A Community Edition foi substituída. A oferta gratuita atual chama-se **Free Edition**, e a própria página da Databricks se apresenta como *"Free Edition | Replacing Databricks Community Edition"*. Ela dá acesso a notebooks em Python e SQL, ao Databricks Assistant e a recursos de engenharia de dados, machine learning e dashboards. Recomendar "Community Edition" hoje manda alguém procurar um produto que não existe mais.

O repositório do livro continua de pé. O `databricks/LearningSparkV2` existe, tem aplicações standalone nos capítulos 2, 3, 6 e 7, notebooks e datasets de exemplo, e um diretório `chapter2`. Não consegui confirmar pela página a presença exata do `mnm_dataset.csv` nem a data do último commit, então esse detalhe fica em aberto.

**8.** Verifique o panorama de deployment modes. O capítulo nomeia YARN e Kubernetes como alternativas ao local mode. Confira o conjunto atual de cluster managers e se algum foi depreciado ou removido desde 2020.

R: O cluster overview atual lista três: **Standalone**, um cluster manager simples que vem com o Spark, **Hadoop YARN**, o resource manager do Hadoop 3, e **Kubernetes**. O par que o capítulo cita continua válido, e a lacuna dele é o Standalone, que nunca é apresentado como opção de deployment.

O que saiu foi o **Mesos**, removido no Spark 4.0.0 via SPARK-44442. Isso importa para este capítulo por um motivo indireto: a frase do Step 3 dizendo que o Spark foi instalado "on your laptop in standalone mode" fica ainda mais confusa hoje, porque o standalone é um dos três cluster managers oficiais e não é o que o capítulo instalou.

**9.** Verifique se o `sparklyr` continua sendo o projeto da comunidade R e se o `sparkR` continua sendo um shell embarcado.

R: O `sparkR` continua embarcado. Ele aparece na listagem atual do `bin` e a documentação corrente ainda mostra `./bin/sparkR --master "local[2]"`. O que mudou é o status: a mesma documentação escreve **"R 4.0+ (Deprecated)"** na linha de versões suportadas, e o **Spark 4.0.0 depreciou o SparkR** via SPARK-49347. Continua funcionando e não é caminho para código novo.

Do `sparklyr` confirmei que o repositório está ativo, com histórico recente de commits e mantido sob a organização `sparklyr` no GitHub. Não consegui ler na página nem a versão do último release nem a data dele, então essa parte fica incompleta.

**10.** Verifique a afirmação de que os RDDs estão "consigned to low-level APIs" desde o Spark 2.x, e a apresentação de MLlib, GraphX e Structured Streaming como o conjunto atual de componentes.

R: A afirmação sobre RDDs é mais forte no livro do que na documentação. O RDD Programming Guide atual não traz aviso de depreciação nem recomendação de migrar, e apresenta o RDD como abstração principal daquele guia. O que existe de oficial nesse sentido está no guia de MLlib, e é específico: *"As of Spark 2.0, the RDD-based APIs in the `spark.mllib` package have entered maintenance mode"*, com a API baseada em DataFrame em `spark.ml` como principal. MLlib segue corrigindo bugs no `spark.mllib` e não acrescenta features nele. Não achei data de remoção anunciada.

O GraphX não aparece como depreciado nem removido nas notas do 4.0.0. O Structured Streaming é o engine atual, e o DStream é legado.

O conjunto de componentes é que ficou incompleto. Três coisas entraram depois de 2020 e são visíveis na distribuição de hoje:

- **Spark Connect**, com `bin/spark-connect-shell`, o extra `pyspark[connect]` e um binário `-connect` próprio.
- **pandas API on Spark**, com o extra `pandas_on_spark`.
- **Declarative Pipelines**, com `bin/spark-pipelines` e o extra `pipelines`.

**11.** Verifique o `sbt` 1.2.8 do log de build e o `scalaVersion := "2.12.10"`. Um descasamento aqui falha na resolução, não na compilação.

R: As duas versões precisam mudar. O `pom.xml` do Spark declara `scala.version` **2.13.18** e `scala.binary.version` **2.13**, com `java.version` 17. A nota da página de download confirma que o suporte a Scala 2.12 foi oficialmente descontinuado no Spark 4. Como o `%%` do sbt anexa a versão binária ao nome do artefato, um `build.sbt` com `scalaVersion := "2.12.10"` pede `spark-core_2.12`, que não existe para releases 4.x. A falha vem na resolução de dependências, antes de compilar qualquer coisa.

O `sbt` também mudou de era. A última release é a **2.0.4, de 26 de julho de 2026**, contra a 1.2.8 do log do livro. O Example 2-3 corrigido pede `scalaVersion := "2.13.18"` e as duas dependências em `4.2.0`.

**12.** Verifique os nomes de nó do DAG mostrados nas Figuras 2-8 e 2-9: `BatchScan`, `WholeStageCodegen`, `mapPartitionsInternal` e `DataSourceRDD`.

R: Este item não se resolve por documentação, e é o único do Nível 5 que exige execução. Os rótulos do DAG são nomes de operadores físicos e de RDDs internos, produzidos pelo planner da versão instalada e pela fonte de dados concreta. Um `spark.read.text` sobre um arquivo local não gera os mesmos nós que uma leitura de CSV com inferência de schema, e o planner mudou muito entre o 3.0.0-preview2 e o 4.2.0. A verificação é rodar o exemplo na máquina, abrir a UI e comparar. Não trato os quatro nomes como fato até isso acontecer.

### Inconsistências internas do capítulo

**13.** A introdução diz que o Spark shell "only supports Scala, Python, and R", mas o Step 2 lista quatro interpretadores, incluindo `spark-sql`. Decida se a introdução está errada ou se usa "shell" num sentido mais estreito que o Step 2.

R: Existe leitura que salva a introdução, e ela não é a leitura literal. A frase completa diz que o shell só suporta Scala, Python e R, e que dá para escrever uma aplicação em qualquer linguagem suportada, inclusive Java, e emitir queries em Spark SQL. Nessa leitura, "shell" significa REPL de linguagem de programação, e o `spark-sql` fica de fora porque é cliente SQL, não REPL de linguagem hospedeira, e a própria frase o contempla pela outra metade.

O problema é que o Step 2 chama os quatro de shells, na mesma palavra, e o `bin` os lista juntos como "the Spark shells". Nenhuma passagem enuncia a distinção. Como está escrito, as duas frases se contradizem, e a leitura estreita é reconstrução minha, não algo que o capítulo ofereça.

**14.** O Step 3 diz que o Spark foi instalado "on your laptop in standalone mode", enquanto todas as outras passagens dizem local mode, e a Table 1-1 do Capítulo 1 trata os dois como deployment modes distintos.

R: A frase do Step 3 está errada. Nada no capítulo instala ou inicia um cluster standalone. Isso exigiria os scripts de `sbin`, que o próprio capítulo descreve como administrativos para subir e derrubar componentes, e nenhum deles é executado. O que foi feito é extrair um tarball e rodar um shell, e a transcrição confirma o resultado com `master = local[*]`. A seção seguinte é a "Using the Local Machine", que reafirma o local mode duas vezes.

A verificação do item 8 fecha o caso: o Standalone é um dos três cluster managers oficiais, ao lado de YARN e Kubernetes, e nada tem a ver com rodar tudo numa JVM local.

**15.** O texto fala em "the `submit-spark` script" numa frase e em `spark-submit` em todas as outras.

R: É erro de digitação. O script chama-se `spark-submit`, como aparece na descrição do `bin`, nas duas invocações de `$SPARK_HOME/bin/spark-submit` e na listagem atual do diretório `bin` do projeto. Não existe nem nunca existiu um `submit-spark`.

**16.** A saída do run Scala é dita "the same as for the Python run", mas o Python mostra CA/Yellow com 100.956 sob `sum(Count)` e o Scala mostra CA/Yellow com 1.807 sob `Total`. Descubra qual run é inconsistente com a entrada declarada de 100.000 linhas.

R: Os dois números são compatíveis com um arquivo de 100.000 linhas, mas respondem a perguntas diferentes, e é o run Scala que não corresponde ao código impresso ao lado dele.

A aritmética separa os casos. Há 60 grupos de estado mais cor, o que a linha `Total Rows = 60` confirma. Os valores do Scala giram em torno de 1.700, e 100.000 dividido por 60 dá cerca de 1.667. Ou seja, os números do Scala são **contagens de linhas por grupo**. Os valores do Python giram em torno de 92.000, o que dá cerca de 5,5 milhões somando os 60 grupos, ou uma média de 55 por linha na coluna `Count`. Ou seja, os números do Python são a **soma da coluna `Count`**.

O Example 2-2 faz `.sum("Count")` e ordena por `desc("sum(Count)")`, exatamente como a versão Python. Rodando como está impresso, ele produziria a coluna `sum(Count)` e os valores na casa dos 90.000. A saída mostrada traz uma coluna chamada `Total` e contagens de linha. Ela não pode ter saído daquele código. O run Python é consistente com o próprio código e com a entrada. O run Scala é a peça fora do lugar, e a frase "the output is the same" está desmentida na página seguinte.

**17.** O Example 2-2 atribui `val caCountMnNDF` e depois chama `caCountMnMDF.show(10)`. Como impresso, o programa Scala não compila.

R: Confere. A atribuição usa `MnN` e a chamada usa `MnM`, com N e M trocados. Scala é estaticamente tipada e resolve identificadores em tempo de compilação, então o `sbt clean package` falharia com um erro de valor não encontrado, e nenhum jar sairia. É outra evidência a favor da conclusão do item anterior: a saída Scala publicada não veio deste código.

O mesmo exemplo tem um segundo defeito, esse de projeto e não de digitação: a `SparkSession` é construída antes da checagem de `args.length`, então o caminho de erro sobe uma sessão e sai por `sys.exit(1)` sem chamar `spark.stop()`.

**18.** O parágrafo de narrow transformations nomeia `contains()` ao lado de `filter()` como transformation, embora no código ele apareça como argumento dentro do `filter()`.

R: O parágrafo é impreciso. No código, `contains("Spark")` é chamado sobre uma coluna, dentro de `strings.filter(strings.value.contains("Spark"))`. Ele constrói uma expressão de predicado, não devolve DataFrame nenhum e não pode ser encadeado no lugar de uma transformation. Pelo critério que o próprio capítulo usa na seção anterior, transformation é o que transforma um DataFrame em outro DataFrame, e `contains()` não faz isso. A Table 2-1 também não o lista. A classificação de narrow está correta quanto ao efeito, porque o predicado é avaliado dentro de uma partição sem troca de dados, mas chamar `contains()` de transformation confunde predicado com operação.

### Conceitos de que os exemplos dependem e que o capítulo nunca define

**19.** **DAG** — usado como estrutura central do plano de execução e nunca expandido nem definido.

R: A sigla nunca é aberta no capítulo. Significa directed acyclic graph, um grafo dirigido sem ciclos. No Spark é a forma do plano de execução, em que os nós são stages e as arestas são as dependências entre eles, e a ausência de ciclo é o que garante uma ordem de execução. A definição está em Minhas definições.

**20.** **Partition** — sustenta toda a discussão de tasks e de narrow/wide, e é assumida.

R: É a lacuna mais grave do capítulo, porque três construções dependem dela: uma task trabalha sobre uma partição, narrow e wide são definidas pela relação entre partições, e o shuffle é movimento entre partições. O capítulo usa a palavra dezenas de vezes e nunca diz o que é. O RDD Programming Guide da documentação oficial cobre o conceito.

**21.** **Shuffle** e **exchange** — introduzidos de passagem como sinônimos e nunca distinguidos entre si nem do nó `Exchange` da UI.

R: O capítulo escreve "a shuffle or exchange of data across clusters" como se fossem a mesma coisa, e depois usa `Exchange` como rótulo na Figura 2-8. São três níveis diferentes: shuffle é o mecanismo de redistribuir dados entre executors, exchange é o nome que o Spark SQL dá a esse passo no plano, e `Exchange` é o operador físico que materializa isso e aparece na UI. O RDD Programming Guide define o shuffle como operação all-to-all necessária quando os valores de uma chave não estão na mesma partição.

**22.** **Executor**, **driver** e **cluster manager** — definidos no Capítulo 1, usados aqui como se definidos.

R: O Capítulo 2 usa os três desde a primeira definição do Step 3 e nunca os apresenta. O glossário do cluster overview oficial fecha a lacuna sem depender do Capítulo 1: driver program é o processo que roda o `main()` da aplicação e cria o SparkContext, executor é um processo lançado para uma aplicação num worker node, que roda tasks e mantém dados em memória ou disco, e cluster manager é o serviço externo que adquire recursos no cluster.

**23.** **JVM** — a restrição "only one SparkSession per JVM" é enunciada sem que se diga o que determina as fronteiras de JVM em local mode e em cluster mode.

R: JVM é a Java Virtual Machine, o processo que executa bytecode Java e Scala. A restrição do comentário do Example 2-1 fica opaca porque o capítulo não diz quantas JVMs existem em cada modo. O que ele dá é uma pista solta: em local mode, todas as operações rodam numa única JVM. Em cluster, cada executor é um processo separado, e o driver é outro, então a restrição de uma sessão por JVM só limita o driver. O capítulo não conecta as duas coisas.

**24.** **Lineage** — recebe uma glosa de uma oração e depois sustenta todo o argumento de tolerância a falhas.

R: A glosa é que os resultados não são computados na hora, são "recorded or remembered as a lineage". A partir disso o capítulo constrói otimização e tolerância a falhas, sem dizer o que a lineage guarda, onde ela vive, ou o que exatamente é replayed. Minha definição está na seção final.

**25.** **Query plan** — nomeado, adiado para o próximo capítulo, e usado como explicação no mesmo parágrafo.

R: O capítulo escreve que actions e transformations contribuem para um query plan que será coberto no próximo capítulo, e na frase seguinte usa o mesmo query plan para explicar por que nada executa antes da action. A explicação se apoia num termo que ele acabou de adiar.

**26.** **Whole-stage code generation**, **locality level** e **batch scan** — aparecem só como rótulos nas Figuras 2-8 e 2-9, sem uma linha de prosa.

R: Nenhum dos três é mencionado no texto corrido. Whole-stage code generation é a técnica pela qual o Spark SQL funde os operadores de um stage numa função única gerada em tempo de compilação da query. Locality level é a classificação de quão perto do dado uma task foi escalonada, e a documentação da web UI é a fonte oficial. Batch scan é o operador físico de leitura em lote de uma fonte DataSource v2. Os três são rótulos de UI e o capítulo remete tudo isso ao Capítulo 7.

### Fontes consultadas

Todas acessadas em 2 de agosto de 2026. Todas são fonte primária: documentação oficial, notas de release, diretório de distribuição ou código-fonte.

Documentação do Apache Spark, versão 4.2.0:

- [Overview, com as versões suportadas de Java, Scala, Python e R](https://spark.apache.org/docs/latest/index.html)
- [Cluster Mode Overview, com a lista de cluster managers, o glossário e a porta 4040](https://spark.apache.org/docs/latest/cluster-overview.html)
- [Configuration, com `spark.ui.port`, `spark.port.maxRetries` e o `log4j2.properties`](https://spark.apache.org/docs/latest/configuration.html)
- [Web UI, com a lista de abas](https://spark.apache.org/docs/latest/web-ui.html)
- [RDD Programming Guide, com transformations, actions, laziness e shuffle](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
- [MLlib Guide, com o aviso de maintenance mode da API baseada em RDD](https://spark.apache.org/docs/latest/ml-guide.html)
- [Installing PySpark, com os extras e o requisito de Python](https://spark.apache.org/docs/latest/api/python/getting_started/install.html)

Download, notas de release e tickets:

- [Página de download do Apache Spark](https://spark.apache.org/downloads.html)
- [Diretório de distribuição do Spark 4.2.0](https://dlcdn.apache.org/spark/spark-4.2.0/)
- [Spark Release 3.3.0, com a migração de log4j 1 para log4j 2 (SPARK-37814)](https://spark.apache.org/releases/spark-release-3-3-0.html)
- [Spark Release 4.0.0, com SPARK-45314, SPARK-45315, SPARK-47993, SPARK-44442 e SPARK-49347](https://spark.apache.org/releases/spark-release-4-0-0.html)

Código-fonte e repositórios:

- [`bin` da distribuição, no repositório apache/spark](https://github.com/apache/spark/tree/master/bin)
- [`conf` da distribuição, com os templates de log4j2](https://github.com/apache/spark/tree/master/conf)
- [`conf/log4j2.properties.template`, com `rootLogger.level`](https://raw.githubusercontent.com/apache/spark/master/conf/log4j2.properties.template)
- [`pom.xml` do Spark, com `scala.version` e `java.version`](https://raw.githubusercontent.com/apache/spark/master/pom.xml)
- [Releases do sbt](https://github.com/sbt/sbt/releases)
- [Repositório de código do livro, databricks/LearningSparkV2](https://github.com/databricks/LearningSparkV2)

Outros:

- [Databricks Free Edition, sucessora da Community Edition](https://www.databricks.com/learn/free-edition)
- [Repositório do sparklyr](https://github.com/sparklyr/sparklyr)

Dois itens ficaram abertos e dependem da minha máquina: a listagem de topo da distribuição extraída (item 4) e os nomes de nó do DAG na UI (item 12). Versões e padrões mudam a cada release, então preciso reconferir estes dados antes de usá-los em prova ou em decisão técnica.

---

## Termos-chave para definir antes de seguir adiante

local mode · standalone mode · deployment mode · cluster manager · driver · executor · SparkSession · SparkContext · application · job · stage · task · DAG · partition · core · shuffle · exchange · narrow dependency · wide dependency · transformation · action · lazy evaluation · lineage · immutability · fault tolerance · DataFrame · RDD · Structured APIs · query plan · whole-stage code generation · schema inference · truncate flag · `spark-submit` · sbt · JVM · locality level

Um termo não definido é alvo de releitura, não item de Nível 5. Se o capítulo não o define, o Capítulo 1 ou a documentação oficial define. Não vale carregá-lo adiante como lacuna de conhecimento sobre Spark.

### Minhas definições

Dezessete dos trinta e seis termos o capítulo usa sem definir, ou nem menciona. Marquei esses casos em itálico, porque neles a definição vem de fora do texto.

**local mode** — Modo em que todo o processamento acontece numa única máquina, dentro de uma JVM só. É o modo usado no capítulo inteiro, bom para aprender e prototipar e inadequado para dados grandes.

**standalone mode** — *Nomeado uma vez, por engano.* O Step 3 diz que o Spark foi instalado em standalone mode, mas o que foi feito é local mode. Standalone é o cluster manager que vem com o Spark, uma das três opções atuais ao lado de YARN e Kubernetes.

**deployment mode** — O arranjo em que a aplicação roda, definido por qual cluster manager coordena os recursos e onde o driver vive. O capítulo cita local, YARN e Kubernetes, e remete os detalhes à Table 1-1 do Capítulo 1.

**cluster manager** — *Usado sem definição.* Serviço externo que adquire recursos no cluster para a aplicação. O capítulo só o cita ao dizer que `--help` mostra como conectar a ele.

**driver** — O processo no núcleo da aplicação. Cria a SparkSession, converte a aplicação em jobs, transforma cada job num DAG, e lança a web UI. Num shell interativo, faz parte do próprio shell. *A definição de processo que roda o `main()` vem da documentação oficial, não do capítulo.*

**executor** — Processo para o qual as tasks são enviadas e onde elas rodam. *O capítulo usa o termo desde a primeira definição do Step 3 e nunca o apresenta.*

**SparkSession** — Objeto que fornece o ponto de entrada para interagir com a funcionalidade do Spark e permite programar com as APIs. Vem pronta na variável `spark` nos shells, e é construída com `SparkSession.builder` numa aplicação. Só pode existir uma por JVM.

**SparkContext** — *Aparece só como a variável `sc` na transcrição do `spark-shell`, sem explicação.* É a conexão de baixo nível com o cluster, e hospeda as APIs de RDD. A linha `Spark context available as 'sc' (master = local[*])` é toda a informação que o capítulo dá.

**application** — Programa de usuário construído sobre o Spark com suas APIs, composto de um driver program e de executors no cluster.

**job** — Computação paralela composta de tasks, gerada em resposta a uma action. O capítulo diz "multiple tasks", e no item 5 do Nível 4 argumentei que o plural é excessivo.

**stage** — Cada conjunto menor de tasks em que um job se divide, e que depende dos outros. É um nó do DAG. As fronteiras caem nas computation boundaries do operador, onde os dados precisam atravessar a rede.

**task** — Unidade única de trabalho ou de execução, enviada a um executor. Corresponde a um core e trabalha sobre uma partição.

**DAG** — *Sigla nunca aberta no capítulo.* Directed acyclic graph, grafo dirigido sem ciclos. É a forma do plano de execução do Spark, com stages nos nós, e a ausência de ciclo é o que garante uma ordem de execução.

**partition** — *Usada o tempo todo e nunca definida.* A fatia de um dataset sobre a qual uma task trabalha, residente numa máquina do cluster. O número de partições determina o número de tasks de um stage.

**core** — *Assumido.* A unidade de CPU que executa uma task por vez. O número total de cores determina quantas tasks correm em paralelo.

**shuffle** — Redistribuição de dados entre as partições dos executors pelo cluster. É o que uma transformation wide obriga, e o que faz um job se dividir em mais de um stage.

**exchange** — *Introduzido como sinônimo de shuffle e nunca distinguido dele.* No plano do Spark SQL é o passo que representa a redistribuição, e `Exchange` é o operador físico que aparece como nó no DAG da UI.

**narrow dependency** — Relação em que uma única partição de saída é computada a partir de uma única partição de entrada, sem troca de dados. `filter()` e `select()` são exemplos.

**wide dependency** — Relação em que a partição de saída depende de dados lidos de outras partições, combinados e escritos em disco. `groupBy()` e `orderBy()` são exemplos, e obrigam a um shuffle.

**transformation** — Operação que transforma um DataFrame num DataFrame novo, sem alterar o original. É avaliada de forma lazy e apenas grava um passo na lineage.

**action** — Operação que dispara a avaliação de todas as transformations registradas. Devolve um valor que não é um DataFrame, e por isso obriga a leitura dos dados. `show()`, `take()`, `count()`, `collect()` e `save()`.

**lazy evaluation** — Estratégia de adiar a execução até que uma action seja invocada ou os dados sejam tocados, lidos do disco ou escritos nele. É o que dá ao otimizador acesso à cadeia inteira antes de rodar qualquer coisa.

**lineage** — O registro da sequência de transformations aplicadas. Permite rearranjar passos, fundi-los e agrupá-los em stages, e permite reproduzir o estado original repetindo a gravação depois de uma falha. *O capítulo dá uma glosa de uma oração e apoia toda a tolerância a falhas nela.*

**immutability** — Propriedade de que uma transformation nunca altera o DataFrame de entrada, apenas devolve um novo. É a pré-condição que torna a lineage replayable.

**fault tolerance** — Capacidade de sobreviver a falhas reproduzindo o estado perdido. No capítulo, resulta da lineage mais a imutabilidade. *A suposição de que a fonte de dados continua legível e igual não é enunciada.*

**DataFrame** — A abstração das Structured APIs, resultado de ler dados ou de aplicar uma transformation. O capítulo o usa desde o primeiro exemplo e nunca o define, e remete a definição ao Capítulo 3.

**RDD** — *Sigla nunca aberta neste capítulo.* Resilient Distributed Dataset, a abstração de baixo nível do Spark. Desde o Spark 2.x está confinada às low-level APIs, e continua sendo o substrato em que toda computação das Structured APIs é executada.

**Structured APIs** — O conjunto de APIs de alto nível sobre DataFrame, usado em todos os exemplos do capítulo. Toda computação escrita nelas é decomposta em operações RDD geradas e otimizadas, inacessíveis ao usuário.

**query plan** — A estrutura para a qual transformations e actions contribuem, e que nada executa até uma action chegar. *Nomeado, adiado para o Capítulo 3, e usado como explicação no mesmo parágrafo.*

**whole-stage code generation** — *Só um rótulo na Figura 2-8, sem uma linha de prosa.* Técnica em que o Spark SQL funde os operadores de um stage numa função única, gerada em tempo de compilação da query, em vez de executar operador a operador.

**schema inference** — Descoberta automática dos tipos das colunas a partir dos dados, ligada com `.option("inferSchema", "true")`. Custa uma passagem extra sobre o arquivo, o que o capítulo não menciona.

**truncate flag** — Argumento booleano de `show()` que controla se valores longos são cortados na exibição. O padrão é `true`, e `show(10, false)` mostra dez linhas inteiras.

**`spark-submit`** — Script em `bin` que submete uma aplicação standalone. Recebe o `.py` diretamente em Python, e em Scala recebe `--class` mais o jar. *O capítulo o chama de `submit-spark` numa frase, por engano.*

**sbt** — Scala Build Tool. Lê o `build.sbt`, resolve dependências, compila os fontes e empacota o jar. `sbt clean package` faz o ciclo inteiro descartando a saída anterior.

**JVM** — *Sigla nunca aberta.* Java Virtual Machine, o processo que executa bytecode Java e Scala. Em local mode existe uma só, e é o que dá sentido à restrição de uma SparkSession por JVM.

**locality level** — *Só um rótulo na Figura 2-9, sem uma linha de prosa.* Classificação de quão perto do dado uma task foi escalonada, e um dos primeiros sinais a olhar quando um stage está lento.
