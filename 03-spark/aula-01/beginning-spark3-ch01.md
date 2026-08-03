# Guia de Leitura — *Beginning Apache Spark 3*, Capítulo 1: Introduction to Apache Spark

**Fonte:** Hien Luu, *Beginning Apache Spark 3* (Apress, 2021), Capítulo 1
**Escopo:** toda questão dos Níveis 1 a 4 é respondível apenas com este capítulo. O Nível 5 está deliberadamente fora dele.

## Como usar este guia

1. Leia o capítulo uma vez de ponta a ponta, sem parar.
2. Faça o Nível 1 de memória primeiro, depois volte ao texto para preencher as lacunas. Marque toda questão que você não conseguiu responder, porque essas são seus alvos de releitura.
3. Faça os Níveis 2 e 3 por escrito, em frases completas. Se você não consegue escrever, você não sabe.
4. O Nível 4 é onde o capítulo de fato se torna útil: ele obriga você a conectar seções que o autor manteve separadas.
5. Os itens do Nível 5 vão direto para o seu backlog de estudo, não para as suas notas como fatos.

Cada questão traz um ponteiro para a seção onde a evidência está, então isto também funciona como índice.

---

## Nível 1 — Memorização e definições

Respostas curtas e verificáveis. Uma ou duas frases cada.

**1.** Quais três propriedades o autor identifica como a razão da popularidade e da ampla adoção do Spark? *(Overview; Summary)*

R: Facilidade de uso, velocidade e flexibilidade.

**2.** Quando o Spark 3.0 foi lançado, e com qual marco da história do projeto esse lançamento coincidiu? *(Overview)*

R: Foi lançado em junho de 2020, no décimo aniversário do Spark como projeto open source.

**3.** Qual alegação de performance o site do Spark faz em relação ao Hadoop MapReduce? *(Overview)*

R: Que ele roda certos jobs de processamento de dados até 100 vezes mais rápido.

**4.** Qual benchmark o Spark venceu em 2014, que volume e que contagem de registros ele ordenou, e qual melhoria a submissão da Databricks alegou sobre o detentor do recorde anterior? *(Overview)*

R: O Spark venceu o Daytona GraySort contest, um benchmark da indústria que mede quão rápido um sistema ordena 100 TB de dados, ou 1 trilhão de registros. A Databricks alegou que o Spark ordenou esses 100 TB três vezes mais rápido e com dez vezes menos recursos que o recorde anterior, do Hadoop MapReduce.

**5.** Aproximadamente quantos operadores de processamento de dados de alto nível o Spark oferece, e em quais quatro linguagens eles estão disponíveis? *(Overview; Summary)*

R: Mais de 80 operadores de alto nível, disponíveis em Scala, Java, Python e R.

**6.** Liste os quatro tipos de workload que o autor usa para ilustrar a flexibilidade do Spark. *(Overview)*

R: Aplicações batch, queries interativas, algoritmos de machine learning que exigem muitas iterações, e aplicações de streaming em tempo real.

**7.** Onde e em que ano o Spark começou como projeto de pesquisa? Em quais anos ele foi aberto como open source e promovido a projeto top-level da Apache? *(History)*

R: Começou em 2009 como projeto de pesquisa na University of California, Berkeley, no AMPLab. Foi aberto como open source em 2010 e virou projeto top-level da Apache em 2013.

**8.** Qual empresa foi fundada por pesquisadores do projeto original, quanto ela captou em 2013, e que papel o autor atribui a ela no ecossistema Spark? *(History)*

R: A Databricks, fundada por pesquisadores do projeto, captou mais de US$ 43 milhões em 2013. O autor a chama de principal steward comercial por trás do Spark.

**9.** Cite os dois artigos de pesquisa que o autor recomenda como leitura fundacional, e os anos em que foram publicados. *(History)*

R: "Spark: Cluster Computing with Working Sets", de 2010, e "Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing", de 2012.

**10.** Quais duas características do Scala o autor dá como razão para os criadores do Spark o terem escolhido? *(History)*

R: A concisão e a tipagem estática.

**11.** Segundo o FAQ do Spark citado no capítulo, quantas máquinas tem o maior cluster Spark do mundo? *(Spark Cluster and Resource Management System)*

R: Mais de 8000 máquinas.

**12.** Cite os dois resource management systems que o capítulo menciona pelo nome, mais a terceira opção disponível para empresas que adotam Spark exclusivamente. *(Spark Cluster and Resource Management System)*

R: Apache YARN, Apache Mesos (grafado "Meso" no texto) e o Spark cluster manager, que vem out of the box.

**13.** Quais são os dois componentes principais de um resource management system típico, e o que o master sabe sobre os slaves? *(Spark Cluster and Resource Management System)*

R: O cluster manager e o worker. O master sabe onde os slaves estão, quanta memória e quantos CPU cores cada um tem.

**14.** Quais são as duas partes de uma aplicação Spark, conforme o autor a define? *(Spark Applications)*

R: A lógica de processamento de dados expressa nas Spark APIs, e o driver.

**15.** Através de qual componente um Spark driver realiza seu trabalho? *(Spark Applications)*

R: Da SparkSession.

**16.** Que tipo de processo é um Spark executor, e o que determina seu tempo de vida? *(Spark Drivers and Executors)*

R: É um processo JVM dedicado a uma aplicação Spark específica. Vive o tempo que a aplicação durar, de minutos a dias.

**17.** De quantos drivers e de quantos executors uma aplicação Spark é composta? *(Spark Drivers and Executors; Summary)*

R: De um driver e um ou mais executors.

**18.** Em qual unidade de hardware cada task é executada? *(Spark Drivers and Executors)*

R: Em um CPU core separado.

**19.** Quais três parâmetros de recurso você pode especificar ao lançar uma aplicação Spark? *(Spark Drivers and Executors)*

R: O número de executors, a memória por executor e o número de CPU cores por executor.

**20.** Cite as cinco bibliotecas que ficam sobre o Spark Core no stack unificado, e o workload que cada uma atende. *(Spark Unified Stack; Figure 1-3)*

R: Spark SQL para processamento interativo. Spark Streaming para processamento em tempo real. Spark GraphX para grafos. Spark MLlib para machine learning. Spark R para tarefas de machine learning pelo shell do R.

**21.** Quais são as duas coisas de que o Spark Core é composto? *(Spark Core)*

R: A distributed computing infrastructure e o RDD, que é a abstração de programação.

**22.** Defina um RDD usando os próprios termos do autor. *(Spark Core)*

R: Um Resilient Distributed Dataset é uma coleção de objetos fault-tolerant, particionada por um cluster, que pode ser manipulada em paralelo.

**23.** Qual é o nome do otimizador do Spark SQL, e o que é um DataFrame? *(Spark SQL)*

R: O otimizador é o Catalyst, que realiza otimizações comuns em engines analíticos de banco de dados. Um DataFrame é uma coleção distribuída de dados organizada em colunas nomeadas, equivalente a uma tabela relacional.

**24.** Liste os formatos estruturados e sistemas de armazenamento dos quais o Spark SQL consegue ler e nos quais consegue escrever, conforme enumerado no capítulo. *(Spark SQL)*

R: JSON, CSV, arquivos Parquet ou ORC, bancos de dados relacionais, Hive e outros.

**25.** Segundo a pesquisa Spark de 2021 citada no capítulo, qual componente crescia mais rápido? *(Spark SQL)*

R: Spark SQL.

**26.** Qual é o lema em três partes que o autor dá para o Spark SQL? *(Spark SQL)*

R: Escrever menos código, ler menos dados e deixar o otimizador fazer o trabalho pesado ("write less code, read less data, and the optimizer does the hard work").

**27.** Quais fontes de dados de streaming o capítulo lista? *(Spark Structured Streaming)*

R: Kafka, Flume, Kinesis, Twitter, HDFS ou TCP socket.

**28.** O que significa DStream, e em qual versão do Spark o capítulo diz que o Structured Streaming foi introduzido? *(Spark Structured Streaming)*

R: DStream significa discretized stream. Ele implementa um modelo incremental que divide a entrada em pequenos batches, por intervalo de tempo, e combina cada batch com o estado corrente para produzir novos resultados. O capítulo diz que o Structured Streaming foi introduzido na versão 2.1. *No item 2 do Nível 5 verifiquei que essa versão está errada.*

**29.** Qual garantia de entrega o capítulo atribui ao engine do Structured Streaming? *(Spark Structured Streaming)*

R: Suporte a end-to-end exactly-once.

**30.** Aproximadamente quantos algoritmos a MLlib fornece, e a partir de qual versão do Spark suas APIs são baseadas em DataFrame? *(Spark MLlib)*

R: Mais de 50 algoritmos comuns de machine learning. As APIs são baseadas em DataFrames a partir do Spark 2.0.

**31.** De quais dois componentes do engine do Spark SQL a MLlib baseada em DataFrame se beneficia? *(Spark MLlib)*

R: Catalyst e Tungsten.

**32.** Que abstração o GraphX fornece, e quais algoritmos de grafo já vêm com ele? *(Spark GraphX)*

R: A abstração é um multigrafo direcionado com propriedades em cada vértice e em cada aresta. Vêm com ele page ranks, connected components, shortest paths e outros.

**33.** Qual percentual das melhorias do Spark 3.0 foi para o Spark SQL e o Spark Core, e qual ganho de velocidade sobre o Spark 2.4 é alegado, em qual benchmark? *(Apache Spark 3.0)*

R: Cerca de 60% das melhorias. No benchmark TPC-DS 30 TB feito pela Databricks, o Spark 3.0 é aproximadamente duas vezes mais rápido que o 2.4.

**34.** Cite as três features do Spark 3.0 que o capítulo destaca, e diga em uma linha o que cada uma faz. *(Apache Spark 3.0)*

R: Adaptive Query Execution Framework, que adapta o plano de execução em runtime com base nas estatísticas mais recentes. Dynamic Partition Pruning, que evita ler dados desnecessários. Accelerator-aware Scheduler, que permite requisitar recursos de GPU.

**35.** Qual faixa de ganho de velocidade o DPP entrega, e sobre qual parcela das queries do benchmark? *(Dynamic Partition Pruning)*

R: Num benchmark TPC-DS, acelera 60% das queries numa faixa de 2x a 18x.

**36.** Liste as sete categorias de aplicação que o capítulo dá como usos reais do Spark. *(Apache Spark Applications)*

R: Customer intelligence, data warehouse, streaming em tempo real, recommendation engines, log processing, serviços user-facing e detecção de fraude.

**37.** Que problema o Delta Lake resolve, e quais três capacidades ele fornece? *(Delta Lake)*

R: Resolve a semântica de consistência de dados em data lakes. O capítulo chega lá assim: um data lake guarda dados estruturados e não estruturados para consumidores diversos, e mantê-los utilizáveis exige supervisão em data catalog, data discovery, data quality, controle de acesso e consistência. A consistência é a mais difícil, e as empresas vinham inventando soluções "Band-Aid" para ela. As três capacidades do Delta Lake são formato aberto de armazenamento, garantias transacionais, e suporte a schema enforcement e schema evolution.

**38.** O que o Koalas implementa, quando a versão 1.0 foi lançada, e qual cobertura da API do pandas ela alegou? *(Koalas)*

R: Implementa a pandas DataFrame API sobre o Apache Spark. A versão 1.0 saiu em junho de 2020 com 80% de cobertura das APIs do pandas.

**39.** Qual projeto alternativo o capítulo menciona para computação paralela em Python? *(Koalas)*

R: Dask.

**40.** Cite os quatro componentes do MLflow e a responsabilidade de cada um. *(MLflow)*

R: Tracking registra e compara experimentos. Projects dá um formato consistente para organizar projetos e reproduzir modelos. Models padroniza o empacotamento de modelos e a API para carregá-los e fazer deploy. Registry é um model store que rastreia lineage, versão e transições de estado de deployment.

---

## Nível 2 — Compreensão

Explique com suas próprias palavras. Três a seis frases cada.

**1.** Explique por que a *combinação* de velocidade, facilidade de uso e flexibilidade importa mais que qualquer uma delas isoladamente. O que os times tinham que fazer antes de existir um engine unificado? *(Overview)*

R: Cada propriedade sozinha falha de um jeito. Velocidade sem facilidade de uso restringe o engine a especialistas. Facilidade de uso sem velocidade não aguenta grandes volumes. As duas juntas ainda perdem quase todo o valor se o engine só cobre um tipo de workload. A combinação é o que faz um único engine servir desenvolvedores, cientistas de dados e analistas em todos os workloads ao mesmo tempo. Antes disso, cada tipo de workload exigia uma solução e uma tecnologia diferentes, então os times mantinham vários sistemas especializados e moviam dados entre eles.

**2.** Que ineficiências específicas do Hadoop MapReduce motivaram o projeto de pesquisa de Berkeley, e quais duas ideias foram introduzidas para resolvê-las? *(History)*

R: Os pesquisadores do AMPLab observaram que o MapReduce era ineficiente em processamento interativo e iterativo. Esses são os dois padrões que passam sobre os mesmos dados muitas vezes, e aí o custo de relê-los a cada passagem domina o tempo de execução. As duas ideias foram in-memory storage, que elimina a releitura, e uma forma eficiente de fault recovery, que é o que torna seguro depender de dados em memória.

**3.** Descreva a divisão de trabalho entre o cluster manager e os workers. Quem oferece recursos, quem atribui trabalho e quem monitora a saúde dos processos? *(Spark Cluster and Resource Management System)*

R: O cluster manager é o master e os workers são os slaves. O master sabe onde cada worker está e quanta memória e quantos cores ele tem, e é isso que lhe permite alocar trabalho. Atribuir trabalho é responsabilidade dele. Oferecer recursos e executar o trabalho atribuído é responsabilidade do worker. Monitorar a saúde dos processos também cabe ao worker: o capítulo dá "lançar um processo específico e monitorar sua saúde" como exemplo do trabalho que um worker recebe.

**4.** Percorra o conjunto completo de responsabilidades do driver, em ordem, desde a submissão da aplicação até a devolução dos resultados ao usuário. *(Spark Applications)*

R: O driver é o coordenador central da aplicação e faz tudo pela SparkSession. Primeiro, conversa com o cluster manager para descobrir em quais máquinas rodar a lógica. Segundo, pede ao cluster manager que lance um executor em cada uma dessas máquinas. Terceiro, gerencia e distribui as tasks entre os executors. Por fim, se a lógica exigir apresentar resultados ao usuário, coleta os resultados de cada executor e os mescla antes de apresentá-los.

**5.** O capítulo chama a regra de um executor por aplicação de decisão consciente de projeto. Que benefício ela compra, e que custo ela impõe? *(Spark Drivers and Executors)*

R: O benefício é isolamento. Como o executor é dedicado a uma aplicação e nunca compartilhado, uma aplicação não interfere na memória, nos cores ou nas falhas de outra. O custo é que compartilhar dados entre aplicações fica difícil, porque não existe executor comum para guardá-los. Duas aplicações que precisam do mesmo dataset intermediário têm que escrevê-lo em storage externo como o HDFS e lê-lo de volta, pagando um I/O que um executor compartilhado evitaria.

**6.** Explique como o Spark alcança paralelismo, traçando a cadeia de aplicação → executor → task → CPU core. *(Spark Drivers and Executors)*

R: A arquitetura é master/slave, com o driver como master e os executors como slaves, cada um rodando como processo independente no cluster. Uma aplicação tem um driver e um ou mais executors. O executor executa a lógica na forma de tasks, e cada task roda em um CPU core separado. O paralelismo vem de multiplicar essa cadeia: número de executors vezes cores por executor dá o total de tasks simultâneas. Por isso os três parâmetros do launch são o que define o grau de paralelismo disponível.

**7.** Por que o autor argumenta que melhorias no Spark Core beneficiam automaticamente todo o stack? O que isso implica sobre onde o esforço de otimização compensa? *(Spark Core)*

R: Todos os outros componentes rodam sobre o Spark Core, então qualquer otimização feita no Core fica automaticamente disponível para eles. Spark SQL, Structured Streaming, MLlib, GraphX e SparkR herdam a melhoria sem alterar uma linha do próprio código. A implicação é que otimizar no nível do Core tem a maior alavancagem do sistema, porque uma melhoria se multiplica por todos os workloads de uma vez. O mesmo esforço dentro de uma biblioteca só rende para os usuários dela.

**8.** Quais duas responsabilidades da distributed computing infrastructure o autor destaca como exigindo "conhecimento íntimo" de usuários avançados, e por que essas duas em particular determinariam a performance da aplicação? *(Spark Core)*

R: São lidar com falhas de computing tasks e mover dados entre máquinas de forma eficiente, o data shuffling. As duas determinam performance porque são custos que o código nunca declara, mas que escalam com o tamanho do job. O shuffle move dados pela rede, o que é muito mais lento que ler localmente, então um job que embaralha mais do que precisa passa a maior parte do tempo esperando. Falhas importam porque em escala de cluster elas são rotina, e o volume de recomputação depois de cada uma define um piso para o tempo total.

**9.** Explique o argumento de por que o SQL se tornou a lingua franca do processamento de dados, e o que o Spark SQL acrescenta a essa história na escala de petabytes. *(Spark SQL)*

R: O SQL venceu porque é fácil expressar intenção nele. A pessoa declara o que quer, não como computar, e o engine de execução faz as otimizações. O Spark SQL leva esse mesmo acordo para a escala de petabytes. Ele oferece duas portas de entrada, queries SQL diretas ou a DataFrame API, e o Catalyst otimiza as duas. É por isso que a pesquisa de 2021 o apontou como componente de crescimento mais rápido: ele abre o processamento distribuído para analistas e para quem sabe SQL, além dos engenheiros de big data.

**10.** Descreva o modelo de micro-batch por trás dos DStreams: em cima do que a entrada é dividida, e como o estado corrente de processamento é usado? *(Spark Structured Streaming)*

R: A entrada é dividida em pequenos batches por intervalo de tempo, não por contagem de registros nem por conteúdo dos dados. Cada batch é processado como uma unidade. O estado corrente é combinado com cada batch que chega para produzir novos resultados. Essa combinação é o que torna o modelo incremental: os resultados se apoiam no estado carregado dos batches anteriores, em vez de recomputar o stream inteiro a cada vez.

**11.** Em que sentido o Structured Streaming simplifica o modelo mental do desenvolvedor em comparação com o engine anterior? Relacione isso à observação de Reynold Xin citada no fim daquela seção. *(Spark Structured Streaming)*

R: O engine anterior obrigava a pensar em termos de stream: batches, intervalos e o estado carregado entre eles. O Structured Streaming trata a computação de streaming como uma computação batch sobre dados estáticos, e executa essa lógica de forma incremental e contínua conforme os dados chegam. O desenvolvedor escreve o que parece uma query batch comum, e o engine assume a execução incremental e a garantia de exactly-once. É para isso que aponta a observação de Reynold Xin: a forma mais simples de fazer streaming analytics é não ter que raciocinar sobre streaming. O ganho é cognitivo, não de throughput, porque a parte difícil nunca foi expressar a computação, e sim raciocinar sobre tempo, estado e semântica de entrega.

**12.** Por que a natureza iterativa dos algoritmos de machine learning faz do Spark uma boa escolha para eles? *(Spark MLlib)*

R: Esses algoritmos percorrem muitas iterações sobre os mesmos dados até atingir o objetivo. Esse é o padrão de acesso que os pesquisadores de Berkeley acharam mal atendido pelo MapReduce, e a razão de terem introduzido in-memory storage. O encaixe não é coincidência, é a motivação original do projeto. Como os executors cacheiam parte dos dados em memória, cada iteração lê da memória em vez de pagar o custo de storage de novo, e a economia se acumula a cada passagem. O Spark ainda torna fácil rodar esses algoritmos de forma escalável num cluster, então o código que funciona numa amostra funciona no dataset completo.

**13.** Por que o R sozinho era insuficiente para análise de dados em larga escala, e o que exatamente o SparkR contribui? *(SparkR)*

R: A limitação do R não é expressividade nem qualidade das bibliotecas. É escala: ele não foi projetado para datasets que não cabem em uma máquina, então a análise fica limitada pela memória de um computador só. O SparkR é um pacote R que dá um frontend leve para o Spark. O que ele contribui é o engine de computação distribuída por baixo, não capacidade analítica nova por cima. E faz isso pelo shell R familiar e pelas APIs que os cientistas de dados já usam, então só o substrato de execução muda.

**14.** Explique a ideia central do Adaptive Query Execution. A quais estatísticas de runtime ele reage, e quais três decisões ele consegue revisar? *(Adaptive Query Execution Framework)*

R: A ideia central é que um plano escolhido antes da execução se apoia em estimativas, e estimativas erram. O AQE adapta o plano em runtime com base nas estatísticas mais recentes, em vez de manter o plano inicial. As estatísticas são o tamanho dos dados e o número de partições, entre outras. As três decisões que ele revisa são a estratégia de join, a otimização de skew joins e o número de partições. O ganho vem de agir sobre o que os dados se revelaram ser, e não sobre o que o otimizador supôs.

**15.** Explique o Dynamic Partition Pruning no contexto de um star schema. Qual tabela tem sua contagem de linhas reduzida, e com base em quê? *(Dynamic Partition Pruning)*

R: A ideia do DPP é evitar ler dados desnecessários. Ele foi projetado para queries que fazem join de fact tables com dimension tables em star schema. A tabela que tem as linhas reduzidas é a fact table, que é a grande. A base para a redução são as condições de filtro dadas nas dimension tables. O filtro é escrito contra a tabela pequena, mas a economia é colhida na grande, porque só as linhas da fact table que sobreviveriam ao join chegam a ser lidas.

**16.** Por que o Spark precisou de um accelerator-aware scheduler afinal? O que mudou na forma como as pessoas usam o Spark? *(Accelerator-aware Scheduler)*

R: O que mudou foi a composição dos workloads. Cada vez mais gente usa o Spark tanto para processamento de big data quanto para machine learning, e o segundo tipo costuma precisar de GPU para acelerar o treinamento. O modelo de recursos do Spark foi construído em torno de memória e CPU cores, os únicos recursos que se pode requisitar no launch. Esse modelo não tem vocabulário para GPU, então um job de treinamento não conseguia dizer ao scheduler do que precisava. A melhoria fecha essa lacuna.

**17.** Percorra o exemplo de word count de cinco linhas e explique o que cada linha faz e que forma os dados têm depois dela. *(Spark Example Applications; Listing 1-1)*

R: Linha 1, `sc.textFile("hdfs://<folder>")`, lê os arquivos de texto da pasta. Depois dela a forma é uma coleção de linhas, um elemento string por linha de texto. Linha 2, `flatMap(line => line.split(" "))`, tokeniza cada linha num array de palavras e achata os arrays, então a forma passa a ser uma palavra por elemento e as fronteiras de linha somem. Linha 3, `map(word => (word, 1))`, anexa a contagem 1 a cada palavra, e a forma vira uma coleção de pares (word, 1), com um elemento por ocorrência. Linha 4, `reduceByKey(_ + _)`, soma as contagens por palavra, e a forma colapsa para um par por palavra distinta, (word, totalCount). Linha 5, `saveAsTextFile("hdfs://<output folder>")`, grava o resultado na pasta e não produz coleção nenhuma, porque escreve para fora em vez de retornar dados. O capítulo comenta que muita coisa acontece por trás dessas cinco linhas e deixa os detalhes para depois.

**18.** Que problema o MLflow foi concebido para resolver, e por que o autor o enquadra como um problema de engenharia de software e não de machine learning? *(MLflow)*

R: O MLflow foi concebido em 2018 para gerenciar o ciclo de vida de machine learning. O argumento do autor é que o machine learning ficou mais acessível com os avanços nos algoritmos, o acesso fácil a grandes datasets e a oferta de material educacional, então o algoritmo deixou de ser o gargalo. O que continua difícil é tudo que cerca o modelo: rastrear e comparar experimentos, organizar projetos para reproduzir resultados, empacotar modelos em formato padrão e saber qual versão está em deploy onde. Essas são preocupações de reprodutibilidade, empacotamento, versionamento e deployment, que são assunto de engenharia de software. Os quatro componentes mapeiam essas necessidades: Tracking, Projects, Models e Registry.

---

## Nível 3 — Aplicação e transferência

Use o conteúdo do capítulo para raciocinar sobre situações concretas. O capítulo te dá o suficiente para responder; ele não entrega a resposta pronta.

**1.** Você submete uma aplicação com 10 executors, 4 cores e 8 GB cada. Usando o modelo do capítulo, quantas tasks podem rodar simultaneamente? Quanta memória total fica sob controle da aplicação, e quanto do cluster o próprio driver representa? *(Spark Drivers and Executors)*

R: Como cada task roda em um CPU core separado, são 10 x 4 = 40 tasks simultâneas. A memória total é 10 x 8 GB = 80 GB. Para o driver não dá para responder com o capítulo: os três parâmetros do launch são todos sobre executors, e o texto nunca menciona memória ou cores do driver. Pelo modelo do capítulo o driver custa zero, o que é falso, já que ele é um processo real rodando em algum lugar. A lacuna é do capítulo, não da conta.

**2.** Dois times rodam, cada um, uma aplicação Spark de vida longa e querem compartilhar um dataset intermediário caro de produzir. Com base no modelo de isolamento de executors, quais são as opções deles e o que cada uma custa? *(Spark Drivers and Executors)*

R: Como executors nunca são compartilhados entre aplicações, não existe caminho direto. A opção que o capítulo nomeia é escrever o dataset em storage externo como o HDFS e cada aplicação ler de lá. Custa o I/O de escrita mais o de leitura, a materialização de um dado que só existia em memória, e a coordenação sobre quando ele está pronto. A segunda opção decorre do modelo, mas o capítulo não a enuncia: fundir os dois jobs numa aplicação só, dividindo driver e executors. Custa o isolamento que a decisão de projeto comprava, porque uma falha ou pressão de memória de um lado passa a atingir o outro, e os times ficam acoplados em release e em operação. O trade-off é o mesmo dos dois lados: pagar I/O para manter isolamento, ou abrir mão do isolamento para evitar o I/O.

**3.** Uma empresa já roda um cluster YARN para jobs de Hive e Pig. Com base no capítulo, qual é o caminho dela para adotar Spark, e o que mudaria se fosse uma startup sem cluster existente? *(Spark Cluster and Resource Management System)*

R: O caminho é rodar Spark sobre o YARN que ela já tem, sem trocar de resource management system nem provisionar máquinas. O capítulo observa que a maioria das empresas com tecnologias de big data mantém um cluster YARN para MapReduce, Pig ou Hive, e que o Spark interopera facilmente com esses sistemas. A adoção fica incremental: o Spark vira mais um consumidor do mesmo pool de recursos, convivendo com os jobs existentes. Para uma startup sem cluster a resposta muda. Ela pode usar o Spark cluster manager que vem out of the box para gerenciar máquinas dedicadas só a Spark, sem trazer YARN. A diferença é entre encaixar o Spark numa infraestrutura que já existe e dedicar uma infraestrutura a ele.

**4.** Seu pipeline atualmente escreve resultados intermediários em storage entre um estágio batch e um estágio de streaming. De qual benefício específico do stack unificado o capítulo diz que você está abrindo mão? *(Spark Unified Stack)*

R: Do segundo dos três benefícios que o capítulo lista. Combinar tipos diferentes de processamento é muito mais eficiente porque o Spark roda conjuntos distintos de APIs sobre os mesmos dados sem escrever os intermediários em storage. Materializar entre o estágio batch e o de streaming paga exatamente o I/O que o engine unificado existe para eliminar. Os outros dois benefícios continuam de pé: o conjunto único de APIs e a possibilidade de compor. O que se perde é a eficiência da composição, que é o que torna a diferença mensurável.

**5.** Um join no seu job está muito enviesado (skewed): uma partição demora vinte vezes mais que as demais. Qual feature do Spark 3.0 endereça isso, e do que ela precisa para agir? *(Adaptive Query Execution Framework)*

R: É o Adaptive Query Execution Framework, que entre suas três revisões otimiza skew joins automaticamente. Ele precisa de estatísticas de runtime, ou seja, das informações mais recentes sobre tamanho dos dados e número de partições. O desbalanceamento não está no schema nem na query, está na distribuição concreta das chaves, e nenhum otimizador estático enxerga isso antes da execução. O AQE só descobre que uma partição ficou vinte vezes maior depois que estágios anteriores rodaram e produziram números reais, e é sobre esses números que ele reparticiona. Ele precisa que o job comece a rodar para poder consertá-lo.

**6.** Você consulta uma fact table grande com join em uma dimension table pequena e filtrada. Descreva o que o DPP faz com a leitura física, e por que o formato do star schema é o que torna isso possível. *(Dynamic Partition Pruning)*

R: Na leitura física, boa parte da fact table nunca é lida do storage. A ordem é esta: o filtro da dimension table é avaliado primeiro e produz um conjunto pequeno de chaves de join que podem sobreviver. Esse conjunto descarta antecipadamente as partições da fact table que não têm como casar com nenhuma delas. Sem DPP, o motor leria a fact table inteira e descartaria as linhas depois, no join. Com DPP, esses bytes nunca saem do disco. O star schema viabiliza isso porque a fact table se liga às dimensions por chaves de join bem definidas, e os filtros ficam nas dimensions, que são pequenas. Existe então um caminho barato para traduzir um predicado do lado pequeno numa restrição de leitura do lado grande. Num modelo sem essa separação entre fatos e atributos descritivos, o filtro não teria como ser propagado.

**7.** Um analista que sabe SQL mas não Scala precisa processar dados em escala de petabytes. Quais partes do stack o capítulo diz que tornam isso possível, e quais duas interfaces estão disponíveis para ele? *(Spark SQL)*

R: A parte do stack é o Spark SQL, módulo construído sobre o Spark Core e voltado a processamento de dados estruturados em escala. Ele herda do Core a distribuição, a coordenação e a tolerância a falhas, então o analista recebe a escala sem tocar nela. As duas interfaces são queries SQL diretas ou a DataFrame API. O Catalyst está por trás das duas, e é ele que permite declarar intenção sem decidir estratégia de execução. O capítulo trata isso como a razão do crescimento do componente, porque habilita analistas de dados e quem sabe SQL, além dos engenheiros de big data. Nada disso exige Scala.

**8.** Um cientista de dados tem código pandas funcionando que não cabe mais em memória em uma máquina. Usando apenas o capítulo, exponha dois caminhos possíveis e o trade-off entre eles. *(Koalas)*

R: O primeiro caminho é o Koalas, que implementa a pandas DataFrame API sobre o Spark. A pessoa aproveita o conhecimento de pandas que já tem e passa a operar sobre datasets muito maiores. O segundo é o Dask, citado pelo capítulo como projeto para computação paralela em Python. O trade-off é cobertura contra ecossistema. O Koalas mantém a pessoa dentro do Spark, com acesso ao mesmo engine que roda o resto do pipeline, mas a versão 1.0 cobria 80% das APIs do pandas, então um quinto da superfície pode não estar lá. O Dask é paralelismo nativo de Python e não depende do Spark, mas fica fora do stack unificado. O capítulo é assimétrico aqui: dedica uma seção ao Koalas e uma frase ao Dask, e não dá informação sobre cobertura ou maturidade do Dask para sustentar a comparação.

**9.** Você precisa rodar queries interativas sobre a saída de um modelo pontuando streams em tempo real. Quais componentes isso toca, e por que o capítulo apresenta isso como algo recém-viabilizado, e não meramente conveniente? *(Spark Unified Stack)*

R: Toca três componentes, todos sobre o Spark Core: Structured Streaming para ingerir os streams, MLlib para o modelo que pontua, e Spark SQL para as queries interativas. O capítulo apresenta como recém-viabilizado porque é o exemplo que ele mesmo dá do terceiro benefício do engine unificado. Antes, cada um desses três workloads exigia uma tecnologia diferente, então compor os três significava mover dados entre sistemas distintos, com materialização e latência em cada fronteira. A composição era a barreira, não cada capacidade isolada. Com os três sobre o mesmo engine e os mesmos dados, a aplicação deixa de ser inviável e passa a ser só uma aplicação. É o sentido da analogia do smartphone: câmera, telefone e GPS já existiam separados, e o Waze precisou deles juntos.

**10.** Mapeie o exemplo de word count no modelo de executors: quais linhas fazem o trabalho ser distribuído, onde ocorre movimentação de dados entre máquinas, e qual linha força um resultado de volta pelo driver ou para fora, no storage? *(Spark Example Applications + Spark Drivers and Executors)*

R: A linha 1 distribui o trabalho: os arquivos da pasta são repartidos entre os executors, e cada um lê a sua fatia. As linhas 2 e 3 rodam dentro de cada executor, sobre a partição que ele já tem, sem conversa entre máquinas, porque tokenizar uma linha e anexar 1 a uma palavra são operações locais. A movimentação entre máquinas está na linha 4: somar as ocorrências de uma palavra exige que todas elas terminem no mesmo executor, e reagrupar por chave é o data shuffling que o capítulo descreve. A linha 5 grava no storage a partir dos próprios executors, sem passar pelo driver. O driver só coletaria e mesclaria resultados se a lógica pedisse para apresentá-los ao usuário, o que não é o caso. Uma ressalva: o capítulo nunca distingue transformations de actions nem explica avaliação preguiçosa, então dizer que nada acontece até a linha 5 disparar a execução não sai deste capítulo. É a lacuna registrada no item 7 do Nível 5.

**11.** Das sete categorias de aplicação listadas, escolha as três mais próximas de um pipeline de coleta e parsing de dados públicos e justifique cada escolha a partir das descrições do capítulo. *(Apache Spark Applications)*

R: Uma ressalva antes das escolhas: o capítulo não traz descrições dessas categorias, só os sete rótulos numa lista, apresentados como pequena amostra de aplicações feitas com Spark. A justificativa então se apoia no enquadramento geral sobre tipos de workload. As três mais próximas são estas:

- **log processing**, porque a coleta gera muitos registros semiestruturados cujo tratamento é parsing e agregação sobre texto, a mesma forma do exemplo de word count.
- **data warehouse**, porque o produto do parsing precisa aterrissar estruturado e consultável, que é onde o capítulo posiciona o Spark SQL.
- **streaming em tempo real**, porque documentos que chegam continuamente valem ser processados na chegada em vez de acumulados para um lote noturno.

As outras quatro supõem um produto com usuários e um modelo de negócio sobre os dados, não a etapa de obtê-los e estruturá-los.

**12.** Seu job de ingestão e seu job de analytics intermitentemente leem saída escrita pela metade e discordam sobre schema. Qual componente do ecossistema o capítulo propõe, e qual das três capacidades dele endereça qual sintoma? *(Delta Lake)*

R: O componente é o Delta Lake, e os dois sintomas caem em capacidades distintas. Ler saída escrita pela metade é falta de garantia transacional: sem transação, o leitor enxerga o estado intermediário de uma escrita em andamento. O suporte a transações resolve isso fazendo o leitor ver ou o estado anterior completo ou o novo completo. Discordar sobre schema é o que o schema enforcement resolve, rejeitando escritas fora do schema esperado em vez de deixá-las entrar e quebrar o leitor depois. O schema evolution é o complemento, que permite mudar o schema de forma controlada quando a mudança é intencional. A terceira capacidade, o formato aberto de armazenamento, não ataca nenhum dos dois sintomas diretamente. Ela é a base que permite os outros dois sem prender os dados num sistema proprietário. Os dois sintomas juntos são o que o capítulo chama de data consistency semantics.

---

## Nível 4 — Análise e síntese

Raciocínio que cruza seções. Estas têm respostas defensáveis em vez de resposta única, mas todos os ingredientes estão no capítulo.

**1.** O capítulo descreve o Spark como master/slave, com o driver como master. Mas também descreve um cluster manager com seu próprio master e seus próprios workers. Desenhe as duas hierarquias e explique como elas se cruzam. Qual master não consegue funcionar sem o outro? *(Spark Cluster and Resource Management System + Spark Drivers and Executors + Figure 1-1)*

R: São duas hierarquias em camadas diferentes, com tempos de vida diferentes.

```
CAMADA DE RECURSOS (longeva, serve todas as aplicacoes)
    cluster manager  (master)
        |-- worker 1     (slave)   maquina fisica
        |-- worker 2     (slave)   maquina fisica
        `-- worker 3     (slave)   maquina fisica

CAMADA DA APLICACAO (efemera, uma por aplicacao)
    driver           (master)
        |-- executor A   (slave)   processo JVM
        |-- executor B   (slave)   processo JVM
        `-- executor C   (slave)   processo JVM
```

Elas se cruzam num ponto só: os executors da camada da aplicação são processos que rodam nas máquinas que são os workers da camada de recursos. O cruzamento é mediado, não direto. O driver não lança executor nenhum sozinho. Ele pede ao cluster manager, que é quem sabe onde há máquina com memória e cores livres. O caminho completo é driver, cluster manager, worker, processo executor.

O driver é quem não funciona sem o outro. Ele não descobre máquinas nem inicia um executor por conta própria, então sem a camada de recursos vira um coordenador sem nada para coordenar. A recíproca é falsa. O cluster manager opera sem nenhum driver Spark, gerenciando um cluster ocioso ou servindo outros frameworks, e o capítulo mesmo diz que a maioria das empresas mantinha clusters YARN rodando MapReduce, Pig e Hive antes de qualquer Spark. A dependência é de mão única, do master efêmero para o longevo.

**2.** Compare as Figuras 1-1 e 1-2. O que cada uma mostra que a outra omite, e o que você precisaria acrescentar a qualquer uma delas para ter um retrato completo de uma aplicação em execução?

R: A Figura 1-1 mostra as interações entre a aplicação e o cluster manager, ou seja, o eixo de negociação de recursos. Ela omite a estrutura interna da aplicação em execução, quantos executors existem e como o trabalho se reparte. A Figura 1-2 mostra um cluster com um driver e três executors, ou seja, a topologia de runtime já montada. Ela omite o cluster manager, e com ele omite como aqueles executors vieram a existir, o que faz a topologia parecer autoexplicativa. Uma figura tem o processo de obter os recursos sem o resultado, a outra tem o resultado sem o processo.

Para completar qualquer uma delas faltariam quatro coisas:

- os workers, as máquinas físicas que hospedam os executors, para sobrepor as duas hierarquias.
- as tasks e os cores dentro de cada executor, que é onde o paralelismo acontece.
- as setas de shuffle entre executors, já que a movimentação entre máquinas é o custo dominante e fica invisível.
- a fonte e o destino dos dados.

A ausência mais grave é a das tasks e dos cores. O capítulo afirma que o paralelismo é o habilitador central, e ele não aparece desenhado em lugar nenhum.

**3.** O capítulo aponta o data shuffling como preocupação central da infraestrutura distribuída e, separadamente, cita três otimizações do Spark 3.0. Argumente qual das três reduz mais diretamente o custo de shuffle, e qual reduz I/O em vez disso.

R: O AQE ataca o shuffle mais diretamente, e o argumento está na composição das três revisões que ele faz. Ajustar o número de partições age sobre o resultado do shuffle, decidindo em quantos pedaços os dados reagrupados ficam. Otimizar skew joins age sobre a patologia do shuffle, uma partição desproporcional travando o estágio inteiro. Trocar a estratégia de join pode, no melhor caso, eliminar o shuffle, porque se a tabela menor couber para ser difundida o join dispensa reagrupamento. Duas das três revisões operam sobre o shuffle e a terceira pode dispensá-lo, então a escolha não é apertada.

O DPP é o que reduz I/O, e isso está na formulação do próprio capítulo: evitar ler dados desnecessários. Ele age antes, na leitura física da fact table, não na movimentação posterior. Reduzir o volume lido tende a reduzir também o que trafega no join, mas esse é efeito de segunda ordem, e o alvo declarado é a leitura.

O accelerator-aware scheduler não cai em nenhuma das duas categorias. Ele não trata de movimentação nem de leitura de dados, e sim de vocabulário de recursos de computação. Ele aparece na mesma lista dos outros dois apesar de resolver um problema de natureza diferente.

**4.** O capítulo diz que aplicações sobre o Spark Core herdam suas melhorias automaticamente. Ainda assim, os ganhos de destaque do Spark 3.0 vieram do Spark SQL, e não do Core. Concilie essas duas afirmações. O que isso sugere sobre onde fica, no Spark 3.0, o usuário que só usa RDD?

R: As duas afirmações não se contradizem porque a herança tem direção. Melhorias no Core sobem para os componentes acima dele, e o texto não afirma nada no sentido inverso. Melhorias feitas dentro do Spark SQL não descem para o Core. Além disso, o número do capítulo agrupa os dois: cerca de 60% das melhorias foram para "Spark SQL e Spark Core", sem informar a divisão, e o tema declarado do release foi performance de query, o que concentra o esforço na camada de SQL. Os ganhos do 3.0 fluem para quem passa pela camada SQL/DataFrame, o que inclui a MLlib, baseada em DataFrame desde o 2.0 e que por isso herda Catalyst e Tungsten.

A conclusão sobre o usuário que só usa RDD é desconfortável. Ele está abaixo da camada onde as melhorias aconteceram. AQE e DPP são otimizações de plano de query, e código RDD não produz plano de query nenhum, então não há o que reescrever em runtime nem filtro para empurrar. Ele recebe o que entrou no Core propriamente dito e fica fora das duas features de destaque do release. A promessa de herança automática continua verdadeira e mesmo assim deixa de valer para o usuário da abstração que o capítulo chama de fundamental.

**5.** Trace a escada de abstração que o capítulo constrói: RDD → DataFrame → SQL → pandas API. Em cada degrau, o que o usuário ganha e de que controle ele abre mão?

R: O padrão atravessa a escada inteira: cada degrau troca controle sobre o *como* por alavancagem sobre o *quê*, e entrega mais da decisão de execução a um otimizador.

No **RDD** o controle é máximo. A pessoa decide como a computação é expressa, tem acesso ao particionamento e passa funções locais arbitrárias para rodar no cluster, o que o capítulo destaca como poderoso e distintivo. O preço é que não existe otimizador entre o que ela escreveu e o que roda. A eficiência é responsabilidade dela, e é por isso que o capítulo exige conhecimento íntimo da infraestrutura de usuários avançados.

No **DataFrame** ganha-se estrutura, colunas nomeadas e um modelo relacional, e com ela o Catalyst. Entrega-se a arbitrariedade. O otimizador só otimiza operações que entende, então mandar qualquer função para o cluster passa a ser limitado, e o plano físico sai das mãos do usuário.

No **SQL** ganha-se o alcance máximo de público, porque quem sabe SQL entra sem aprender Scala, e a declaratividade fica completa. Entrega-se o controle procedural inteiro, porque só dá para expressar o que a linguagem expressa.

Na **pandas API** ganha-se familiaridade, um idioma de máquina única aplicado a um engine distribuído. Entrega-se cobertura, já que a versão 1.0 do Koalas cobria 80% das APIs do pandas, e entrega-se visibilidade, porque a abstração esconde a distribuição e permite escrever algo cujo custo distribuído fica invisível. Esse é o único degrau que esconde a natureza distribuída em vez de apenas mediá-la, o que o torna o mais confortável e o mais traiçoeiro.

**6.** O capítulo apresenta a decisão de isolamento de executors como benefício. Reformule-a como custo, usando a seção de resource management: o que a alocação de executors por aplicação implica para a utilização do cluster quando muitas aplicações curtas rodam?

R: Como custo, o isolamento é uma reserva exclusiva de recursos pelo tempo de vida inteiro da aplicação, e não pelo tempo em que ela computa. O capítulo diz que o executor é um processo JVM dedicado a uma aplicação e que vive o que a aplicação viver. Com muitas aplicações curtas isso produz três efeitos.

Primeiro, cada aplicação paga o custo de partida inteiro. Pedir recursos, esperar o lançamento dos processos JVM e depois derrubá-los é um overhead fixo que, num job de poucos segundos, supera o trabalho útil.

Segundo, os recursos ficam reservados mesmo ociosos. Entre estágios, esperando uma partição lenta ou aguardando I/O, aqueles cores contam como em uso e nenhuma outra aplicação os toma emprestado, porque compartilhar é o que a decisão de projeto proíbe.

Terceiro, o cache morre com a aplicação. Um job curto que roda de hora em hora relê e recomputa a mesma coisa toda vez.

O saldo é alto isolamento e baixa utilização. Para quem administra o cluster, a decisão que o capítulo vende como benefício otimiza previsibilidade às custas de aproveitamento, e o texto só mostra o lado da previsibilidade.

**7.** Monte uma tabela com uma linha por componente do stack (Core, SQL, Structured Streaming, MLlib, GraphX, SparkR) e colunas para: abstração primária, tipo de workload, linguagens expostas e dependência de outros componentes. Quais células o capítulo deixa em branco?

R:

| Componente | Abstração primária | Workload | Linguagens expostas | Depende de |
|---|---|---|---|---|
| Spark Core | RDD | fundação: scheduling, coordenação, tolerância a falhas | Scala, Java, Python | nada |
| Spark SQL | DataFrame | processamento estruturado e interativo | *não informado* | Spark Core |
| Structured Streaming | *não informado* (DStream é dado como abstração do engine anterior) | tempo real | *não informado* | Spark Core |
| MLlib | DataFrame (desde o 2.0) | machine learning | *não informado* | Spark Core e o engine do Spark SQL (Catalyst, Tungsten) |
| GraphX | multigrafo direcionado com propriedades em vértices e arestas | processamento de grafos | *não informado* | Spark Core |
| SparkR | *não informado* (descrito como frontend leve) | análise em larga escala. A Figura 1-3 o associa a machine learning via shell R | R | Spark Core |

As lacunas se concentram em duas colunas. **Linguagens por componente** o capítulo nunca enumera. Ele dá dois números globais que não se encaixam: os operadores do Spark disponíveis em Scala, Java, Python e R, e as APIs de RDD expostas a Scala, Java e Python. R não aparece na lista de RDD e entra no stack por outra porta, o SparkR.

**Abstração primária** falta para Structured Streaming e SparkR. A do Structured Streaming é a lacuna mais séria. O capítulo nomeia o DStream como a principal abstração de streaming e depois apresenta o Structured Streaming como engine novo sem dizer sobre o que ele opera, que é o que o item 1 do Nível 5 manda verificar.

As **dependências entre componentes** só aparecem num caso, o da MLlib com Catalyst e Tungsten. Se o Structured Streaming se apoia no Spark SQL, o capítulo não diz, e a Figura 1-3 desenha todos como caixas irmãs sobre o Core, sugerindo uma independência mútua que a própria MLlib contradiz.

**8.** A evidência de performance no capítulo vem de três fontes: a alegação de 100x do site, o resultado do GraySort de 2014 e a comparação TPC-DS 30 TB. Ordene-as por quanto você confiaria nelas para prever o comportamento do seu próprio workload, e diga o que cada uma deixa de fora.

R: Do mais confiável para o menos:

**1. TPC-DS 30 TB, Spark 3.0 contra 2.4, cerca de 2x.** É a melhor porque compara duas versões do mesmo sistema sobre uma suíte padronizada e pública de queries analíticas, o que isola a variável de interesse. Deixa de fora quem rodou, que foi a Databricks, parte interessada e presumivelmente na própria plataforma. O capítulo também não informa hardware, configuração, nem se as duas versões foram igualmente ajustadas. TPC-DS é SQL analítico sintético, então prevê bem para workload de SQL analítico e mal para o resto. E 30 TB pode estar longe da escala real de quem lê.

**2. Daytona GraySort de 2014.** Fica em segundo por ser competição externa e auditada, com tarefa definida e números concretos. Deixa de fora que ordenação é uma operação única, dominada por I/O e shuffle, nada parecida com um pipeline misto. É de 2014, e Spark e MapReduce mudaram muito desde então. E submissão de competição é configuração afinada ao extremo por especialistas, não o comportamento padrão.

**3. "Até 100 vezes mais rápido que o Hadoop MapReduce".** A menos confiável, por larga margem. É alegação de marketing do site do próprio projeto, protegida por dois qualificadores que a tornam irrefutável: "até" e "certos jobs". Deixa de fora tudo que importaria: quais jobs, qual configuração, qual baseline, e se o caso medido é o iterativo em memória, o melhor cenário possível para o Spark, ou um batch de passagem única, onde a diferença encolhe muito. Um número máximo sem distribuição não permite previsão.

As três omitem a mesma coisa: nada nelas fala dos dados, do cluster ou do formato das queries de quem lê. Benchmark compara sistemas entre si, não prevê casos particulares.

**9.** O capítulo argumenta que o Spark reduz custo operacional ao substituir vários sistemas especializados. Construa o contra-argumento usando apenas material deste capítulo: onde um único engine unificado impõe custos próprios?

R: O capítulo dá cinco peças que montam o contra-argumento.

A mais forte é a própria seção de ecossistema. Delta Lake, Koalas e MLflow existem porque o Spark sozinho não cobria consistência de storage, familiaridade de API para quem vem de pandas, nem ciclo de vida de modelos. O capítulo apresenta o engine unificado como quem substitui vários sistemas e, três seções depois, apresenta três projetos adicionais que serão necessários. A conta de "menos sistemas para operar" não fecha dentro do mesmo capítulo.

A segunda é que o isolamento de executors não sai de graça. Como executors nunca são compartilhados, compartilhar dados entre aplicações exige storage externo. O engine unificado remove a materialização entre estágios de uma aplicação e a reimpõe entre aplicações distintas, então parte do ganho é realocação de custo, não eliminação.

A terceira é o custo de especialização. O capítulo diz que usuários avançados precisam de conhecimento íntimo da infraestrutura distribuída para projetar aplicações de alta performance. Trocaram-se várias ferramentas por uma, mas essa uma exige domínio profundo de shuffle e de tratamento de falhas. A superfície de conhecimento encolheu em quantidade e cresceu em profundidade.

A quarta é o custo de tuning, admitido na abertura do capítulo. Ele diz que as otimizações just-in-time do 3.0 servem para reduzir o tempo e o esforço gastos ajustando aplicações Spark. Um engine que precisa de um framework inteiro para reduzir esforço de ajuste reconhece que esse esforço era grande.

A quinta são os três parâmetros de launch. Número de executors, memória e cores são decisões de capacidade tomadas por aplicação, e o capítulo não oferece critério nenhum para tomá-las.

Resumindo: o Spark reduz o número de sistemas, mas concentra num ponto só uma exigência de expertise, de ajuste e de dimensionamento que antes estava diluída, e ainda assim não dispensa storage externo nem os projetos satélites.

**10.** Delta Lake, Koalas e MLflow são todos apresentados como "ecossistema" e não como "stack". Com base em como o capítulo descreve cada um, articule o critério que separa as duas categorias. O Delta Lake de fato se encaixa no seu critério?

R: O critério que se extrai do tratamento dado a cada grupo tem duas condições. Um componente é **stack** quando roda sobre o Spark Core, vem distribuído com o Spark e faz processamento de dados. O texto é explícito ao dizer que os componentes do stack rodam sobre o Core e por isso herdam suas melhorias. Um componente é **ecossistema** quando é projeto open source independente, com versionamento próprio, e endereça uma necessidade ao redor do processamento em vez do processamento em si.

O Delta Lake encaixa pela metade, e é ele que expõe a fragilidade da taxonomia. Pelo critério de independência encaixa, porque é projeto separado com release próprio. Pelo critério de posição na arquitetura não encaixa em nenhum dos dois. Ele não fica sobre o Spark Core como as bibliotecas do stack, nem é camada de interação com o Spark como os outros dois do ecossistema. É formato de armazenamento, então fica embaixo do Spark, no disco. Koalas e MLflow tratam de como a pessoa interage com o processamento. O Delta Lake trata dos bytes antes de qualquer processamento existir.

Levando adiante, o balde "ecossistema" guarda três coisas com três relações distintas com o Spark. O Koalas é uma API sobre o Spark e não faz sentido sem ele. O Delta Lake é um formato que o Spark lê, mas outros engines também leem. O MLflow não é específico de Spark e funciona com qualquer framework de machine learning. A conclusão defensável é que "stack" é categoria coerente e "ecossistema" é resto, o nome do que sobrou depois de definir o stack, e não uma categoria com critério próprio.

**11.** Escreva o argumento do capítulo a favor do Spark em cinco frases, uma por seção, de modo que remover qualquer uma delas quebre o argumento.

R:

1. *(Overview)* O Spark é um engine distribuído de propósito geral cuja combinação de velocidade, facilidade de uso e flexibilidade explica sua adoção, porque nenhuma das três isolada teria bastado.
2. *(History)* Ele chegou a essa posição por ter sido construído para corrigir uma falha diagnosticada do MapReduce em cargas interativas e iterativas, com in-memory storage e fault recovery eficiente, o que torna a velocidade estrutural e não incidental.
3. *(Arquitetura)* Essa velocidade se realiza numa arquitetura master/slave em que um driver distribui tasks para executors e cada task ocupa um CPU core separado, o que faz do paralelismo o mecanismo concreto por trás de toda alegação de performance.
4. *(Unified Stack)* Como cada biblioteca especializada assenta sobre o mesmo Spark Core, um engine só cobre batch, SQL, streaming, machine learning e grafos, e compor esses tipos dispensa materializar intermediários, o que converte "substituí várias ferramentas" em aplicações antes inviáveis.
5. *(Spark 3.0)* O engine segue melhorando onde mais importa, com cerca de 60% do release 3.0 indo para SQL e Core e ganho alegado de 2x, então adotar Spark é apostar numa plataforma que se acumula, não fotografar a performance de hoje.

Remover qualquer uma quebra a cadeia. A frase 1 enuncia a tese. Sem a 2, a velocidade vira alegação de marketing sem causa. Sem a 3, falta o mecanismo. Sem a 4, sobra um engine batch rápido, sem o argumento de unificação que é o coração do capítulo. Sem a 5, o retrato é estático e não sustenta uma decisão de adoção, que é uma aposta no futuro. A cadeia é tese, causa, mecanismo, escopo e trajetória.

**12.** Identifique três afirmações neste capítulo que são feitas sem evidência de suporte. Para cada uma, diga que evidência a resolveria.

R:

**1. "Até 100 vezes mais rápido que o Hadoop MapReduce."** Atribuída ao site do próprio projeto e protegida por dois qualificadores, "até" e "certos jobs", que a tornam impossível de refutar, porque basta um caso favorável para validá-la. Resolveria: um benchmark especificado com tipo de job, volume de dados, configuração de cluster e os dois sistemas ajustados de forma comparável, publicando a distribuição dos ganhos em vez do máximo, e separando o caso iterativo em memória do batch de passagem única.

**2. "Segundo a pesquisa Spark de 2021, o Spark SQL era o componente de crescimento mais rápido."** Não há metodologia, tamanho de amostra, quem foi consultado, nem o que "crescimento" mede. Resolveria: a própria pesquisa, com população amostrada e critério explícito. Crescimento em participação de uso, em novas adoções ou em contribuições de código são três coisas diferentes que podem apontar para lados opostos.

**3. Os números de comunidade: mais de mil contribuidores a mais, mais de 200 mil meetups do Apache Spark, e o total de contribuidores superando o do Apache Hadoop.** Nenhum tem fonte nem janela temporal. O de meetups é o mais suspeito, porque 200 mil *encontros* é implausível e o número quase certamente conta membros de grupos. Resolveria: contagens de contribuidores dos repositórios dos dois projetos num período declarado, com definição de contribuidor, e a definição do que o número de meetups conta.

Outras três que considerei:

- "a Databricks é o principal steward comercial por trás do Spark" é avaliação sem critério.
- "o Spark torna extremamente fácil implementar esses algoritmos" usa superlativo sem parâmetro de comparação.
- a analogia do smartphone com o Waze é ilustração, mas aparece em posição de argumento.

---

## Nível 5 — Além do capítulo (backlog, não notas)

O capítulo foi escrito contra o Spark 3.0/3.1 em 2021. Verifiquei estes itens contra a documentação oficial do Apache Spark em **2 de agosto de 2026**, quando a versão corrente da documentação era a **4.2.0**. As URLs estão ao fim da seção.

**1.** O capítulo apresenta o DStream como a principal abstração de streaming e o Structured Streaming como um engine mais novo. Verifique qual é o atual, qual é legado, e que abstração o Structured Streaming de fato usa.

R: O enquadramento do capítulo está invertido. A documentação atual abre o guia de Spark Streaming (DStreams) com um aviso explícito: *"Spark Streaming is the previous generation of Spark's streaming engine. There are no longer updates to Spark Streaming and it's a legacy project. There is a newer and easier to use streaming engine in Spark called Structured Streaming. You should use Spark Structured Streaming for your streaming applications and pipelines."* O DStream é legado e não recebe mais atualizações. O Structured Streaming é o atual.

A abstração do Structured Streaming não é o DStream. É a **Dataset/DataFrame API**, que trata o stream como uma tabela ilimitada processada de forma incremental. Isso fecha duas coisas que ficaram abertas nas outras respostas: explica a célula vazia de "abstração primária" na tabela do Nível 4 item 7, e explica a dependência do Structured Streaming em relação ao Spark SQL, que o capítulo não menciona.

**2.** O capítulo data o Structured Streaming no Spark 2.1. Verifique as versões de introdução e de disponibilidade geral.

R: O capítulo erra nas duas pontas. A **introdução foi no Spark 2.0.0**, cuja nota de release traz *"the initial experimental release for Structured Streaming, a high level streaming API built on top of Spark SQL and the Catalyst optimizer"*. A própria nota já o descreve como construído sobre o Spark SQL e o Catalyst, o que confirma a dependência do item anterior. A **disponibilidade geral foi no Spark 2.2.0**, via SPARK-20844: *"The Structured Streaming APIs are now GA and is no longer labeled experimental"*. A versão 2.1 citada pelo livro não é nem a de introdução nem a de GA.

**3.** Koalas como projeto separado versus a pandas API integrada ao próprio Spark: descubra o que aconteceu e em qual versão.

R: O Koalas foi absorvido pelo Spark no **3.2.0**, de outubro de 2021, via SPARK-34849, listada nos destaques do release como *"Pandas API on Spark - Pandas users can scale out their applications on Spark with one line code change"*. O projeto separado deixou de ser o caminho e o módulo virou `pyspark.pandas`, que vem com o PySpark. Material de 2021 que manda instalar o Koalas à parte e importar `databricks.koalas` está desatualizado. O livro é de 2021 e a absorção também, então o capítulo já nasceu no limite da validade nesse ponto.

**4.** Mesos como resource manager do Spark: verifique o status atual de suporte. Acrescente também o alvo de deployment que o capítulo omite por completo e que hoje é o mais comum.

R: O Mesos foi **depreciado no Spark 3.2.0** (SPARK-35050, "Deprecate Apache Mesos as resource manager", com a justificativa de que o próprio Apache Mesos estava indo para o attic) e **removido no Spark 4.0.0** (SPARK-44442, "Drop mesos support"). O alvo omitido pelo livro é o **Kubernetes**, declarado GA no **Spark 3.1.1** (SPARK-33005), portanto já GA quando o livro saiu. A página de cluster overview atual lista três cluster managers: **Standalone, Hadoop YARN e Kubernetes**. Mesos não aparece mais. Isso atualiza o que respondi na questão 12 do Nível 1: o par "YARN ou Mesos" do livro hoje é "YARN ou Kubernetes", e o standalone continua de pé.

**5.** A lista de features do Spark 3.0 no capítulo é parcial. Descubra qual é o padrão de AQE, DPP e do accelerator-aware scheduler nas versões atuais, e quais outras otimizações chegaram no 3.2, no 3.4 e no 4.0.

R: O **AQE mudou de padrão**. `spark.sql.adaptive.enabled` era `false` no 3.0 e passou a `true` no **Spark 3.2.0** (SPARK-33679, "Enable adaptive query execution by default"). A documentação de tuning confirma o `true` e detalha os sub-parâmetros, todos ligados: `coalescePartitions.enabled` true, `skewJoin.enabled` true, com `skewedPartitionFactor` 5.0 e `skewedPartitionThresholdInBytes` 256MB, `advisoryPartitionSizeInBytes` 64MB e `localShuffleReader.enabled` true. Isso atualiza o que respondi no Nível 3 item 5: hoje o AQE já vem ligado, então o skew join é tratado sem configuração.

O **DPP já era ligado por padrão** e continua. Em `SQLConf.scala`, `spark.sql.optimizer.dynamicPartitionPruning.enabled` é criado com default `true`, descrito como "When true, we will generate predicate for partition column when it's used as join key". Dos três destaques do 3.0, dois estão ativos sem configuração.

O **accelerator-aware scheduler é o único que não vem ligado**, por uma razão estrutural: ele não é um interruptor, é um vocabulário. A configuração é genérica por tipo de recurso. `spark.executor.resource.{resourceName}.amount` tem default **0** e `spark.task.resource.{resourceName}.amount` tem default **1**. Nenhuma GPU é pedida sem declaração explícita de que ela é necessária e de quanto. Faz sentido, porque pedir por padrão um recurso que a maioria dos clusters não tem só produziria falha de alocação.

Otimizações posteriores, pelos destaques de cada release:

- **3.2.0**: AQE ligado por padrão; pandas API on Spark; RocksDB StateStore para Structured Streaming (SPARK-34198).
- **3.4.0**: cliente Python para Spark Connect (SPARK-39375); Storage-Partitioned Join em DS v2 (SPARK-37375); Bloom filter joins ligados por padrão (SPARK-38841); lateral column alias references; valores DEFAULT para colunas; async progress tracking em Structured Streaming.
- **4.0.0**: **modo ANSI SQL por padrão** (SPARK-44444); tipo de dado VARIANT (SPARK-45827); remoção do Mesos; JDK 17 e Scala 2.13 como padrão, com JDK 8/11 e Scala 2.12 descontinuados; Python Data Source API; Arbitrary State API v2 e State Data Source em Structured Streaming; Spark Connect bastante ampliado, embora a página de release não o declare formalmente GA.

Desses, o **modo ANSI por padrão no 4.0** tem o maior impacto prático. Ele muda o comportamento de conversões e de erros aritméticos, então é o principal candidato a quebrar código escrito para versões anteriores.

**6.** O Delta Lake é apresentado como *a* resposta para consistência de storage. Identifique os open table formats concorrentes e as dimensões em que diferem.

R: Os concorrentes diretos são o **Apache Iceberg** e o **Apache Hudi**. Os três atacam o mesmo problema de fundo que o capítulo descreve, dar semântica transacional a arquivos num object store. Cada um organiza a solução em torno de uma preocupação diferente. As dimensões em que diferem, pela documentação de cada projeto:

**O que cada um se propõe a ser.** O Delta Lake se define como projeto open source que permite construir uma arquitetura de lakehouse sobre data lakes, com transações ACID e isolamento serializável, para que leitores nunca vejam dados inconsistentes. O Iceberg se define como table format para datasets analíticos grandes. O Hudi se organiza em torno de gestão de atualizações em nível de registro. A diferença de vocabulário já indica a ênfase de cada um: arquitetura, formato de tabela e gestão de mutação.

**Estratégia de atualização.** É onde a distinção fica mais concreta, e o Hudi é quem a explicita melhor, com dois tipos de tabela. Em **Copy on Write**, updates e deletes geram novos base files. A escrita fica mais lenta e amplifica mais, na ordem dos file groups reescritos, mas a leitura é rápida porque só lê base files. Em **Merge on Read**, as mudanças vão para log files leves, mesclados com os base files na hora da query e compactados periodicamente. O trade-off inverte: escrita mais barata, com amplificação na ordem dos registros alterados, e leitura mais cara. Escolher entre os dois é escolher onde pagar, na ingestão ou na consulta.

**Dependência de catálogo.** O Iceberg trata catálogo como parte integral da operação e documenta várias implementações: Hive, JDBC, AWS Glue, DynamoDB, Nessie e HadoopCatalog. O Delta se apoia no transaction log que vive junto dos dados. A diferença muda o que precisa ser provisionado além do bucket.

**Evolução de schema e de partição.** Os três suportam evolução de schema. O Iceberg destaca também **partition evolution** e branching/tagging, ou seja, mudar o particionamento sem reescrever a tabela e versionar estados nomeados. O Delta destaca time travel com versionamento para rollback, auditoria e reprodutibilidade de experimentos.

**Amplitude de engines.** Nenhum dos três é exclusivo do Spark. O Iceberg documenta Spark, Flink, Hive e Kafka Connect, além de Trino, Presto, Dremio, ClickHouse, Snowflake e BigQuery. O Delta cita Spark, Flink, Hive, Trino e Athena. Isso desmonta a leitura, sugerida pelo capítulo, de que o Delta Lake seria a camada de storage do Spark.

**Governança.** Iceberg e Hudi são projetos da Apache Software Foundation. A documentação do Delta Lake que consultei se identifica como projeto open source hospedado em `delta-io/delta`, sem citar fundação. Para lakehouse essa dimensão pesa tanto quanto as técnicas, porque define quem controla a evolução do formato.

O capítulo apresenta o Delta Lake como *a* resposta porque em 2021, dentro do universo Databricks, era. A pergunta hoje não é qual formato é melhor, e sim qual trade-off de escrita contra leitura o pipeline exige e qual formato os engines em uso já leem.

**7.** O capítulo nunca distingue narrow de wide transformations, actions de transformations, nem jobs de stages de tasks, apesar de o exemplo de word count depender das três distinções.

R: São três distinções encadeadas, e elas vêm de dois documentos diferentes.

**Transformations contra actions** está no guia de RDDs. RDDs suportam dois tipos de operação: transformations, que criam um novo dataset a partir de um existente, e actions, que devolvem um valor ao driver depois de rodar uma computação. O guia acrescenta o que o capítulo do livro omite por inteiro: todas as transformations são **lazy**. Elas não computam nada na hora, apenas registram a transformação a aplicar sobre o dataset base, e só são computadas quando uma action exige um resultado.

**Jobs, stages e tasks** está no glossário do cluster overview. Uma application consiste em driver mais executors. Um **job** é uma computação paralela disparada por uma action. Cada job se divide em **stages**. Cada stage contém **tasks**, as unidades de trabalho enviadas aos executors.

**Narrow contra wide** não está em nenhum dos dois. Precisei ir ao paper de RDDs de 2012, um dos dois que o capítulo recomenda como leitura fundacional, então fechar essa lacuna é seguir a bibliografia do próprio livro. O paper define dependência **narrow** como aquela em que cada partição do RDD pai é usada por no máximo uma partição do RDD filho, com map, filter e flatMap como exemplos. Define **wide** como aquela em que múltiplas partições filhas dependem da mesma partição pai, com groupByKey, reduceByKey e join como exemplos. O paper explica por que a distinção importa em dois planos. Na execução, narrow permite pipeline dentro de um mesmo stage, sem materializar intermediários, enquanto wide força fronteira de stage, porque é preciso parar, embaralhar dados pela rede e retomar. Na recuperação, perder uma partição com dependência narrow custa recomputar só a partição pai correspondente, enquanto dependência wide pode exigir recomputar todas as partições pais.

Aplicando ao word count da Listing 1-1: as linhas 1 a 3 são narrow, a linha 4 (`reduceByKey`) é wide e marca a fronteira entre dois stages, e a linha 5 é a action que dispara tudo. O exemplo do livro é **um job com dois stages**, e nada acontece até a última linha. O guia de RDDs confirma o mecanismo da linha 4: nem todos os valores de uma chave estão na mesma partição ou na mesma máquina, e precisam ser colocados juntos para o cálculo, o que exige uma operação all-to-all, o shuffle.

**8.** `sc.textFile` na Listing 1-1 usa o SparkContext, enquanto o texto diz que o driver trabalha através da SparkSession. Descubra a relação entre os dois e qual deles o código atual deve usar.

R: Os dois coexistem, e a nota de release do Spark 2.0.0 é precisa sobre o escopo da substituição: *"SparkSession: new entry point that replaces the old SQLContext and HiveContext for DataFrame and Dataset APIs. SQLContext and HiveContext are kept for backward compatibility."* A nota não diz que a SparkSession substituiu o SparkContext. Ela substituiu SQLContext e HiveContext.

O SparkContext continua existindo como a conexão de baixo nível com o cluster, e é ele que hospeda as APIs de RDD, entre elas `textFile`. A SparkSession guarda um SparkContext internamente, acessível por `spark.sparkContext`. O cluster overview atual confirma que ele não foi removido, porque ainda descreve o driver como o processo que cria o SparkContext.

Então o `sc.textFile` da Listing 1-1 não está errado. É a API de nível RDD, e o `sc` do shell é o SparkContext já inicializado. Para código novo o caminho é criar uma SparkSession e trabalhar com DataFrames. Quando a API de RDD for necessária, o acesso é por `spark.sparkContext`, sem criar um SparkContext solto.

### Fontes consultadas

Todas acessadas em 2 de agosto de 2026. Todas são fonte primária: documentação oficial, notas de release, código-fonte ou o paper original.

Documentação do Apache Spark, versão 4.2.0:

- [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/streaming/index.html)
- [Spark Streaming (DStreams) Programming Guide, com o aviso de legado](https://spark.apache.org/docs/latest/streaming-programming-guide.html)
- [Cluster Mode Overview, com a lista de cluster managers e o glossário job/stage/task](https://spark.apache.org/docs/latest/cluster-overview.html)
- [RDD Programming Guide, com transformations, actions, laziness e shuffle](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
- [Performance Tuning, com os padrões de AQE](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [Configuration, com os defaults genéricos de recurso por executor e por task](https://spark.apache.org/docs/latest/configuration.html)

Notas de release e tickets:

- [Spark Release 2.0.0](https://spark.apache.org/releases/spark-release-2-0-0.html)
- [Spark Release 2.2.0](https://spark.apache.org/releases/spark-release-2-2-0.html)
- [Spark Release 3.1.1](https://spark.apache.org/releases/spark-release-3-1-1.html)
- [Spark Release 3.2.0](https://spark.apache.org/releases/spark-release-3-2-0.html)
- [Spark Release 3.4.0](https://spark.apache.org/releases/spark-release-3-4-0.html)
- [Spark Release 4.0.0](https://spark.apache.org/releases/spark-release-4-0-0.html)
- [SPARK-35050, depreciação do Mesos](https://issues.apache.org/jira/browse/SPARK-35050)

Código-fonte e paper:

- [SQLConf.scala, defaults de `dynamicPartitionPruning.enabled` e `adaptive.enabled`](https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala)
- [Zaharia et al., *Resilient Distributed Datasets* (NSDI 2012), seção de dependências narrow e wide](https://people.csail.mit.edu/matei/papers/2012/nsdi_spark.pdf)

Table formats, documentação oficial de cada projeto:

- [Delta Lake, introdução](https://docs.delta.io/latest/delta-intro.html)
- [Apache Iceberg, documentação](https://iceberg.apache.org/docs/latest/)
- [Apache Hudi, tipos de tabela (Copy on Write e Merge on Read)](https://hudi.apache.org/docs/table_types)

Versões e padrões de configuração mudam entre releases, como o AQE demonstra ao virar de `false` para `true` entre o 3.0 e o 3.2. Preciso reconferir estes dados antes de usá-los em prova ou em decisão técnica.

---

## Termos-chave para definir antes de seguir adiante

Escreva uma definição de uma linha para cada, com suas próprias palavras, sem consultar:

Spark cluster · cluster manager · worker · Spark application · driver · executor · task · SparkSession · SparkContext · RDD · partition · data shuffling · DataFrame · Catalyst · Tungsten · DStream · micro-batch · exactly-once · AQE · skew join · DPP · fact table · dimension table · star schema · lakehouse · schema evolution

Qualquer termo que você não conseguir definir é alvo de releitura, não item de Nível 5.

### Minhas definições

Onze dos vinte e seis termos o capítulo usa sem definir, ou nem menciona. Marquei esses casos em itálico, porque neles a definição vem de fora do texto.

**Spark cluster** — O conjunto de máquinas sobre o qual o Spark é implantado. Vai de poucas máquinas a milhares, e o maior do mundo passa de 8000 segundo o FAQ citado.

**cluster manager** — O componente master de um resource management system. Sabe onde os workers estão e quanta memória e quantos cores cada um tem, e orquestra o trabalho atribuindo-o a eles. Exemplos do livro: YARN, Mesos e o cluster manager do próprio Spark. *No item 4 do Nível 5 verifiquei que Mesos foi removido e Kubernetes entrou.*

**worker** — O componente slave de um resource management system. Oferece recursos ao cluster manager e executa o trabalho atribuído, o que inclui lançar processos e monitorar a saúde deles.

**Spark application** — A unidade submetida, composta da lógica de processamento expressa nas Spark APIs e do driver.

**driver** — O coordenador central da aplicação, no papel de master. Negocia com o cluster manager em quais máquinas rodar, pede o lançamento dos executors, distribui as tasks e coleta e mescla os resultados. Opera pela SparkSession.

**executor** — Processo JVM dedicado a uma única aplicação Spark, no papel de slave. Executa a lógica na forma de tasks e cacheia parte dos dados em memória ou em disco quando a aplicação manda. Vive o tempo que a aplicação viver.

**task** — A unidade de execução da lógica de processamento. Cada task roda em um CPU core separado, e é daí que vem o paralelismo.

**SparkSession** — O componente pelo qual o driver realiza seu trabalho. *O capítulo 1 só a nomeia. A definição como ponto de entrada único vem do capítulo 2, e a relação com o SparkContext está no item 8 do Nível 5.*

**SparkContext** — *Fora do capítulo.* Aparece só como a variável `sc` na Listing 1-1, sem explicação. É a conexão de baixo nível com o cluster e hospeda as APIs de RDD. A SparkSession guarda um, acessível por `spark.sparkContext`.

**RDD** — Resilient Distributed Dataset. Coleção de objetos fault-tolerant, particionada pelo cluster, manipulável em paralelo. É a abstração de programação do Spark Core e permite processar em larga escala sem se preocupar com onde os dados residem nem com falhas de máquina.

**partition** — A fatia de um dataset que reside em uma máquina do cluster. *O capítulo usa o conceito o tempo todo e nunca o define.*

**data shuffling** — A movimentação de dados entre máquinas do cluster. É responsabilidade da distributed computing infrastructure e uma das duas coisas que exigem conhecimento íntimo de usuários avançados.

**DataFrame** — Coleção distribuída de dados organizada em colunas nomeadas, equivalente a uma tabela de banco relacional. É a abstração de alto nível do Spark SQL, inspirada nos data frames de R e Python.

**Catalyst** — O otimizador do Spark SQL. Realiza otimizações comuns em engines analíticos de banco de dados.

**Tungsten** — *Só o nome.* Citado ao lado do Catalyst como fonte das otimizações que a MLlib baseada em DataFrame herda. O capítulo não diz o que ele faz.

**DStream** — Discretized stream. Abstração de streaming que divide a entrada em pequenos batches por intervalo de tempo e combina cada um com o estado corrente. *É legado hoje, conforme o item 1 do Nível 5.*

**micro-batch** — Cada um desses pequenos lotes em que a entrada é fatiada por intervalo de tempo. *O capítulo descreve o mecanismo e não usa o termo.*

**exactly-once** — Garantia de que cada registro é processado e refletido no resultado uma única vez, nem perdido nem duplicado. *O capítulo atribui a garantia ao Structured Streaming sem explicar o que ela significa.*

**AQE** — Adaptive Query Execution. Framework que adapta o plano de execução em runtime com base nas estatísticas mais recentes de tamanho dos dados e número de partições. Revisa estratégia de join, skew joins e número de partições. *Ligado por padrão desde o Spark 3.2, conforme o item 5 do Nível 5.*

**skew join** — Join em que as chaves se distribuem de forma desigual entre as partições, deixando uma partição muito maior que as outras e travando o job nela. *O capítulo cita que o AQE os otimiza, sem definir o que são.*

**DPP** — Dynamic Partition Pruning. Otimização que evita ler dados desnecessários, reduzindo as linhas da fact table que entram no join com base nos filtros das dimension tables.

**fact table** — A tabela grande de um star schema, a que guarda os eventos ou as medições. É a que o DPP poupa de ler. *Usada no capítulo sem definição.*

**dimension table** — As tabelas menores de um star schema, com os atributos descritivos pelos quais se filtra. É nelas que o filtro aproveitado pelo DPP é escrito. *Usada no capítulo sem definição.*

**star schema** — Modelagem em que uma fact table central se conecta a várias dimension tables. É o formato para o qual o DPP foi projetado. *Usado no capítulo sem definição.*

**lakehouse** — *Não aparece no capítulo.* O termo vem do próprio guia, no item 6 do Nível 5. Nomeia a arquitetura que combina o armazenamento barato e aberto de um data lake com as garantias transacionais e de schema de um data warehouse.

**schema evolution** — Capacidade de mudar o schema de uma tabela ao longo do tempo sem quebrar leitores e escritores existentes. *O capítulo lista "schema enforcement and evolution" como capacidade do Delta Lake, sem detalhar nenhum dos dois.*
