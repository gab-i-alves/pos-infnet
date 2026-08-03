# Guia de Leitura — *Learning Spark*, 2ª edição, Capítulo 1: Introduction to Apache Spark — A Unified Analytics Engine

**Fonte:** Damji, Wenig, Das & Lee, *Learning Spark: Lightning-Fast Data Analytics*, 2ª ed. (O'Reilly, 2020), Capítulo 1
**Escopo:** toda questão dos Níveis 1 a 4 é respondível apenas com este capítulo. O Nível 5 não é, e o Nível 6 exige o outro Capítulo 1 já lido.

## Como usar este guia

1. Leia o capítulo uma vez de ponta a ponta, sem parar.
2. Faça o Nível 1 de memória primeiro, depois volte ao texto para preencher as lacunas. Marque toda questão que você não conseguiu responder, porque essas são seus alvos de releitura.
3. Faça os Níveis 2 e 3 por escrito, em frases completas. Se você não consegue escrever, você não sabe.
4. O Nível 4 é onde o capítulo se torna útil: ele obriga você a conectar seções que os autores mantiveram separadas, e a notar onde a prosa e o código discordam.
5. O Nível 5 vai para o backlog de estudo, não para as notas. O Nível 6 vai para uma nota de comparação que fica acima dos dois livros.

---

## Nível 1 — Memorização e definições

Respostas curtas e verificáveis. Uma ou duas frases cada.

**1.** Qual é a origem do nome Google, e o que o termo subjacente denota? *(Big Data and Distributed Computing at Google)*

R: Google é um erro de grafia deliberado do termo matemático googol. O capítulo descreve o googol como "1 plus 100 zeros", ou seja, 1 seguido de cem zeros.

**2.** Quais duas abordagens estabelecidas o capítulo diz que não davam conta da escala do Google? *(Big Data and Distributed Computing at Google)*

R: Os sistemas de armazenamento tradicionais, como os RDBMSs, e as formas imperativas de programação.

**3.** Cite os três sistemas do Google cujos papers semearam essa linhagem, e diga o que cada um oferecia. *(Big Data and Distributed Computing at Google)*

R: O Google File System (GFS) oferecia um filesystem distribuído e fault-tolerant sobre muitos servidores de commodity hardware. O Bigtable oferecia armazenamento escalável de dados estruturados sobre o GFS. O MapReduce introduziu um novo paradigma de programação paralela, baseado em programação funcional, para processamento em larga escala de dados distribuídos sobre GFS e Bigtable.

**4.** No modelo MapReduce, o que se move para o quê, código para dados ou dados para código? Quais duas propriedades o capítulo diz que isso favorece? *(Big Data and Distributed Computing at Google)*

R: O código se move para os dados. O sistema envia as funções map e reduce para onde o dado reside, em vez de trazer o dado para a aplicação. Isso favorece data locality e cluster rack affinity.

**5.** O que os workers produzem ao fim de um job MR, e onde isso é escrito? *(Big Data and Distributed Computing at Google)*

R: Os workers agregam e reduzem as computações intermediárias e produzem uma saída final anexada, vinda da função reduce. Essa saída é escrita em um distributed storage, de onde fica acessível à aplicação.

**6.** Qual empresa fora do Google é citada como enfrentando desafios de escala parecidos, e para qual sistema o paper do GFS serviu de blueprint? *(Hadoop at Yahoo!)*

R: O Yahoo!. O paper do GFS serviu de blueprint para o Hadoop File System (HDFS), incluindo a implementação de MapReduce como framework de computação distribuída.

**7.** Em que mês e ano o Hadoop foi doado à ASF, e o que significa ASF? *(Hadoop at Yahoo!)*

R: Em abril de 2006. ASF é Apache Software Foundation, descrita pelo capítulo como organização sem fins lucrativos e neutra em relação a fornecedores.

**8.** Cite os quatro módulos do framework Apache Hadoop. *(Hadoop at Yahoo!)*

R: Hadoop Common, MapReduce, HDFS e Apache Hadoop YARN.

**9.** Quais duas empresas comerciais baseadas em open source cresceram em torno do Hadoop, e o que aconteceu com elas? *(Hadoop at Yahoo!)*

R: Cloudera e Hortonworks. O capítulo registra que elas se fundiram.

**10.** Liste as quatro deficiências do MapReduce sobre HDFS que o capítulo identifica, na ordem. *(Hadoop at Yahoo!)*

R: Primeira, era difícil de gerenciar e administrar, com complexidade operacional pesada. Segunda, a API de batch processing era verbosa, exigia muito código de setup boilerplate e tinha fault tolerance frágil. Terceira, em jobs grandes com muitos pares de tasks MR, o resultado intermediário de cada par era escrito em disco local para o estágio seguinte, e esse I/O repetido fazia jobs grandes rodarem por horas ou dias. Quarta, ele não dava conta de combinar outros workloads, como machine learning, streaming ou queries SQL interativas.

**11.** Quais seis sistemas sob medida o capítulo nomeia como tendo sido construídos para workloads que o MR não cobria? *(Hadoop at Yahoo!)*

R: Apache Hive, Apache Storm, Apache Impala, Apache Giraph, Apache Drill e Apache Mahout.

**12.** De quem é o ditado que enquadra a pergunta de projeto no fim daquela seção, e qual é ele? *(Hadoop at Yahoo!)*

R: De Alan Kay: "Simple things should be simple, complex things should be possible."

**13.** Em que ano o Spark começou, e quais foram os nomes sucessivos do laboratório onde ele nasceu? *(Spark's Early Years at AMPLab)*

R: Em 2009, no RAD Lab, que depois virou AMPLab e hoje se chama RISELab.

**14.** Qual ganho de velocidade sobre o Hadoop MapReduce os primeiros papers do Spark demonstraram, e para que tipo de job? O que o capítulo alega sobre hoje? *(Spark's Early Years at AMPLab)*

R: De 10 a 20 vezes mais rápido, para certos jobs. Sobre o presente, o capítulo alega apenas "many orders of magnitude faster", sem número nem fonte.

**15.** Liste os cinco aprimoramentos que formaram o eixo central do projeto Spark. *(Spark's Early Years at AMPLab)*

R: Torná-lo altamente fault tolerant. Torná-lo embarrassingly parallel. Suportar in-memory storage para resultados intermediários entre computações map e reduce iterativas e interativas. Oferecer APIs fáceis e componíveis em várias linguagens como modelo de programação. Suportar outros workloads de forma unificada.

**16.** Cite os seis criadores originais que doaram o Spark à ASF, e a empresa que fundaram. *(Spark's Early Years at AMPLab)*

R: Matei Zaharia, Ali Ghodsi, Reynold Xin, Patrick Wendell, Ion Stoica e Andy Konwinski. A empresa é a Databricks.

**17.** Em que mês e ano o Apache Spark 1.0 foi lançado? *(Spark's Early Years at AMPLab)*

R: Em maio de 2014.

**18.** Dê a definição de Apache Spark que o capítulo apresenta em uma frase, incluindo onde ele pode rodar. *(What Is Apache Spark?)*

R: "Apache Spark is a unified engine designed for large-scale distributed data processing, on premises in data centers or in the cloud."

**19.** Cite as quatro características-chave da filosofia de projeto do Spark. *(What Is Apache Spark?)*

R: Speed, ease of use, modularity e extensibility.

**20.** Quais são os três caminhos pelos quais o capítulo diz que o Spark persegue velocidade? *(Speed)*

R: Primeiro, a implementação interna se beneficia dos avanços da indústria de hardware em preço e performance de CPU e memória, com servidores de commodity baratos, centenas de gigabytes de memória, múltiplos cores e sistema operacional Unix eficiente em multithreading. Segundo, o Spark constrói suas computações de query como um DAG, e o DAG scheduler mais o query optimizer montam um grafo computacional eficiente, decomponível em tasks executadas em paralelo pelos workers. Terceiro, o engine de execução física, Tungsten, usa whole-stage code generation para gerar código compacto.

**21.** O que é o Tungsten, e qual técnica ele usa para gerar código? *(Speed)*

R: É o engine de execução física do Spark. Ele usa whole-stage code generation para gerar código compacto para execução.

**22.** Qual é a abstração fundamental sobre a qual todas as abstrações estruturadas de nível mais alto são construídas, e quais duas são citadas como construídas sobre ela? *(Ease of Use)*

R: O RDD, descrito como "a simple logical data structure called a Resilient Distributed Dataset". Sobre ele são construídos DataFrames e Datasets.

**23.** Quais duas categorias de operação o Spark oferece como seu modelo de programação? *(Ease of Use)*

R: Transformations e actions.

**24.** Quais cinco linguagens o capítulo lista para as operações do Spark? *(Modularity)*

R: Scala, Java, Python, SQL e R.

**25.** O que o Spark desacopla que o Apache Hadoop não desacoplava, e o que isso viabiliza? *(Extensibility)*

R: O Spark desacopla storage de compute, enquanto o Hadoop incluía os dois. Isso viabiliza ler dados armazenados em muitas fontes diferentes e processar tudo em memória.

**26.** Quais fontes de dados o capítulo lista como legíveis pelo Spark, e quais outras são citadas como alcançáveis ao estender os `DataFrameReader`s e `DataFrameWriter`s? *(Extensibility)*

R: Legíveis diretamente: Apache Hadoop, Apache Cassandra, Apache HBase, MongoDB, Apache Hive, RDBMSs e outras. Alcançáveis por extensão dos readers e writers: Apache Kafka, Kinesis, Azure Storage e Amazon S3.

**27.** Que reconhecimento os criadores do Spark receberam em novembro de 2016, e por um paper com que título? *(Unified Analytics)*

R: O ACM Award, concedido pela Association for Computing Machinery, pelo paper que descreve o Apache Spark como "Unified Engine for Big Data Processing".

**28.** Quais quatro engines o paper premiado nomeia como substituídos pelo stack unificado? *(Unified Analytics)*

R: Storm, Impala, Dremel e Pregel.

**29.** O que acontece com o código escrito em qualquer das Structured APIs antes de ele rodar no cluster? *(Apache Spark Components as a Unified Stack)*

R: O Spark o converte em um DAG executado pelo core engine. O código subjacente é decomposto em bytecode altamente compacto, executado nas JVMs dos workers pelo cluster.

**30.** Quais formatos de arquivo o capítulo lista para o Spark SQL, e a qual padrão SQL o Spark SQL é descrito como aderente? *(Spark SQL)*

R: CSV, text, JSON, Avro, ORC e Parquet, além de tabelas de RDBMS. O capítulo afirma que o Spark SQL é ANSI SQL:2003-compliant e também funciona como engine SQL puro.

**31.** O que o capítulo alega sobre a performance de trechos equivalentes escritos em linguagens diferentes? *(Spark SQL)*

R: Que trechos equivalentes em Python, R ou Java geram bytecode idêntico e, portanto, a mesma performance.

**32.** Desde qual versão do Spark a MLlib está dividida em dois pacotes, quais são eles, qual é baseado em DataFrame e qual está em modo de manutenção? *(Spark MLlib, NOTE)*

R: Desde o Apache Spark 1.6. Os pacotes são `spark.mllib` e `spark.ml`. O baseado em DataFrame é o `spark.ml`, e é para lá que vão todas as features novas. O `spark.mllib` contém as APIs baseadas em RDD e está em modo de manutenção. *Cuidado ao ler essa frase: o 1.6 data a divisão em dois pacotes, e não o modo de manutenção. O "now" do livro é o presente do autor, em 2020. A documentação atual data o modo de manutenção do Spark 2.0, o que não contradiz o capítulo.*

**33.** Quais capacidades as APIs baseadas em DataFrame da MLlib fornecem, e qual primitiva de baixo nível é nomeada explicitamente? *(Spark MLlib)*

R: Extrair ou transformar features, construir pipelines de treino e avaliação, e persistir modelos para salvar e recarregar durante o deployment. Como utilitários adicionais, operações comuns de álgebra linear e estatística. A primitiva de baixo nível nomeada é uma generic gradient descent optimization.

**34.** Em qual versão o Structured Streaming foi introduzido, em que estado, e em qual versão ele ficou geralmente disponível? *(Spark Structured Streaming)*

R: Foi introduzido no Apache Spark 2.0, junto com um Continuous Streaming model experimental, e as próprias Structured Streaming APIs entraram nessa leva. Ficou geralmente disponível no Spark 2.2, quando passou a ser usável em produção.

**35.** Qual é o modelo mental central de um stream no Structured Streaming? *(Spark Structured Streaming)*

R: O stream é visto como uma tabela em crescimento contínuo, com novas linhas de dados anexadas ao fim. O desenvolvedor trata isso como uma tabela estruturada e consulta como consultaria uma tabela estática.

**36.** Quais duas preocupações o core engine do Spark SQL assume no lugar do desenvolvedor no Structured Streaming, e qual modelo antigo isso tornou obsoleto? *(Spark Structured Streaming)*

R: Fault tolerance e late-data semantics. Isso tornou obsoleto o modelo de DStreams da série 1.x do Spark.

**37.** Quais três algoritmos de grafo o capítulo nomeia como disponíveis no GraphX, e quem os contribuiu? *(GraphX)*

R: PageRank, Connected Components e Triangle Counting. Foram contribuídos por usuários da comunidade.

**38.** O que é o GraphFrames, quem o contribuiu, e como ele difere do GraphX? *(footnote 1)*

R: É uma biblioteca de processamento de grafos de propósito geral, contribuída pela Databricks como projeto open source. É similar ao GraphX, mas usa APIs baseadas em DataFrame.

**39.** Liste as quatro responsabilidades do Spark driver. *(Spark driver)*

R: Instanciar a SparkSession. Comunicar-se com o cluster manager. Requisitar recursos ao cluster manager, CPU e memória, para os executors, que são JVMs. Transformar todas as operações Spark em computações DAG, escaloná-las e distribuir sua execução como tasks entre os executors. O capítulo acrescenta que, uma vez alocados os recursos, o driver se comunica diretamente com os executors.

**40.** Quais cinco pontos de entrada a `SparkSession` absorveu no Spark 2.0? O que o capítulo diz sobre código antigo 1.x? *(SparkSession, NOTE)*

R: `SparkContext`, `SQLContext`, `HiveContext`, `SparkConf` e `StreamingContext`. A NOTE diz que os contextos individuais continuam acessíveis com seus métodos, por compatibilidade retroativa, e que código antigo 1.x com `SparkContext` ou `SQLContext` continua funcionando.

**41.** Quais quatro cluster managers o Spark suporta? *(Cluster manager)*

R: O standalone cluster manager embutido, Apache Hadoop YARN, Apache Mesos e Kubernetes. *No item 1 do Nível 5 verifiquei que o Mesos saiu.*

**42.** Quantos executors rodam por nó na maioria dos modos de deployment? *(Spark executor)*

R: Apenas um executor por nó.

**43.** Reproduza a Tabela 1-1 de memória: os cinco modos e, para cada um, onde roda o driver, onde roda o executor e o que atua como cluster manager. *(Deployment modes)*

R:

| Modo | Spark driver | Spark executor | Cluster manager |
|---|---|---|---|
| Local | Roda em uma única JVM, como um laptop ou um nó único | Roda na mesma JVM do driver | Roda no mesmo host |
| Standalone | Pode rodar em qualquer nó do cluster | Cada nó do cluster lança sua própria JVM de executor | Pode ser alocado arbitrariamente em qualquer host do cluster |
| YARN (client) | Roda em um client, fora do cluster | Container do NodeManager do YARN | O Resource Manager do YARN trabalha com o Application Master do YARN para alocar os containers nos NodeManagers para os executors |
| YARN (cluster) | Roda junto com o YARN Application Master | Igual ao YARN client mode | Igual ao YARN client mode |
| Kubernetes | Roda em um pod do Kubernetes | Cada worker roda dentro do seu próprio pod | Kubernetes Master |

**44.** Onde o dado físico reside como partições, e como o Spark trata cada partição? *(Distributed data and partitions)*

R: O dado físico reside distribuído no storage, como partições, em HDFS ou em cloud storage. O capítulo diz que o Spark trata cada partição como uma abstração lógica de alto nível, "as a DataFrame in memory". *Essa frase é o alvo do item 2 do Nível 4.*

**45.** O que recebe sua própria partição de dados para trabalhar? *(Distributed data and partitions; Figure 1-6)*

R: Cada core de cada executor. O texto é literal: "each executor's core is assigned its own data partition to work on".

**46.** O que os dois trechos de código sobre particionamento imprimem, e o que cada trecho faz de diferente para chegar lá? *(Distributed data and partitions)*

R: Os dois imprimem `8`. O primeiro, `spark.read.text("path_to_large_text_file").repartition(8)`, parte dado físico já existente no storage em oito partições. O segundo, `spark.range(0, 10000, 1, 8)`, cria do zero um DataFrame de 10.000 inteiros distribuídos em oito partições em memória. Um reparte dado lido, o outro gera dado.

**47.** Qual foi a motivação primária por trás do Spark 2.x, e o que "express what, not how" significa nesse contexto? *(The Developer's Experience)*

R: Unificar e simplificar o framework, limitando o número de conceitos com que o desenvolvedor precisa lidar. "Express what, not how" significa declarar o que a operação deve computar, sem dizer como computar, e deixar o Spark determinar a melhor forma.

**48.** Quais três papéis são nomeados como os usuários típicos do Spark? *(Who Uses Spark, and for What?)*

R: Data engineers, data scientists e machine learning engineers.

**49.** Em quais ferramentas, bibliotecas e linguagens os cientistas de dados são descritos como proficientes? *(Data science tasks)*

R: Ferramentas analíticas como SQL, bibliotecas como NumPy e pandas, e linguagens como R e Python.

**50.** O que o Spark 2.4 introduziu como parte do Project Hydrogen, e qual necessidade isso atendia? O que o Spark 3.0 acrescentou, e em quais modos de deployment? *(Data science tasks)*

R: O Spark 2.4 introduziu um novo gang scheduler, como parte do Project Hydrogen, para atender às necessidades de fault tolerance no treino e no escalonamento de modelos de deep learning de forma distribuída. O Spark 3.0 acrescentou suporte a coleta de recursos de GPU, nos modos standalone, YARN e Kubernetes.

**51.** Quais dois componentes recebem o crédito pelas melhorias de performance que facilitaram a vida dos data engineers, e o que cada um faz? *(Data engineering tasks)*

R: O Catalyst optimizer, que otimiza SQL, e o Tungsten, responsável por compact code generation.

**52.** Entre quais três APIs do Spark os data engineers podem escolher? *(Data engineering tasks)*

R: RDDs, DataFrames e Datasets. *No item 7 do Nível 5 verifiquei que essa escolha tripla não existe em Python.*

**53.** Liste os cinco casos de uso populares do Spark. *(Popular Spark use cases)*

R: Processar em paralelo grandes datasets distribuídos por um cluster. Executar queries ad hoc ou interativas para explorar e visualizar datasets. Construir, treinar e avaliar modelos de machine learning com a MLlib. Implementar pipelines de dados ponta a ponta a partir de muitos streams. Analisar datasets de grafos e redes sociais.

**54.** Quantos grupos de Meetup e quantos membros o capítulo cita, e o que é nomeado como a maior conferência de Spark? *(Community Adoption and Expansion)*

R: Mais de 600 grupos de Meetup do Apache Spark no mundo, com perto de meio milhão de membros. A maior conferência é o Spark + AI Summit.

**55.** Contra qual versão do Spark o código deste livro foi testado, e o que isso implica sobre quando ele foi escrito? *(Community Adoption and Expansion)*

R: Contra o Spark 3.0-preview2. Isso situa a escrita antes do lançamento do 3.0, porque o texto diz que a comunidade terá lançado o Spark 3.0 até a publicação. O capítulo, portanto, descreve o 3.0 sem tê-lo usado em versão final.

**56.** Dê os quatro números do GitHub que o capítulo cita para contribuidores, releases, forks e commits. *(Community Adoption and Expansion)*

R: Perto de 1.500 contribuidores, bem mais de 100 releases, 21.000 forks e cerca de 27.000 commits.

---

## Nível 2 — Compreensão

Explique com suas próprias palavras. Três a seis frases cada.

**1.** Explique o argumento de data locality no MapReduce. Por que enviar código até o dado reduz tráfego de rede, e o que "cluster rack affinity" acrescenta ao quadro? *(Big Data and Distributed Computing at Google)*

R: A aplicação MR entrega as funções map e reduce ao sistema, que as envia para as máquinas onde o dado já está. O tráfego cai porque o que viaja é o código, que é pequeno, e não o dataset, que é grande. Com isso a maior parte do I/O fica local ao disco em vez de atravessar a rede. Sobre rack affinity o capítulo só cita o termo ao lado de data locality, sem explicar. A leitura defensável é que ela é o segundo melhor caso: quando o código não pode rodar na máquina que guarda o dado, roda em outra máquina do mesmo rack, e a transferência fica dentro do switch do rack. Essa parte é inferência minha, não do texto.

**2.** Das quatro deficiências do MapReduce, explique qual delas o projeto in-memory do Spark ataca mais diretamente, e trace o mecanismo da Figura 1-1 até a correção. *(Hadoop at Yahoo! + Spark's Early Years at AMPLab)*

R: A terceira, o I/O de disco repetido. A Figura 1-1 mostra a iteração intermitente de leituras e escritas entre as computações map e reduce. Em jobs grandes com muitos pares de tasks MR, o resultado intermediário de cada par é escrito em disco local para o estágio seguinte lê-lo de volta. Esse ciclo se repete a cada par, e é ele que faz jobs grandes durarem horas ou dias. A correção do Spark é manter os resultados intermediários em memória, o que elimina o par escrita-leitura entre estágios. Não é coincidência que o ganho de 10 a 20 vezes dos primeiros papers apareça justamente em jobs iterativos e interativos, que são os que mais repetem esse ciclo.

**3.** Explique como a proliferação de sistemas sob medida (Hive, Storm, Impala e outros) piorou o problema original em vez de resolvê-lo. Nomeie os dois custos que o capítulo identifica. *(Hadoop at Yahoo!)*

R: Cada workload que o MR não cobria ganhou um sistema próprio. Cada um desses sistemas trouxe sua própria API e sua própria configuração de cluster. O problema original era que o Hadoop já era difícil de administrar, e a solução multiplicou o número de coisas a administrar. Os dois custos nomeados são o aumento da complexidade operacional do Hadoop e a curva de aprendizado mais íngreme para os desenvolvedores. Resolver a lacuna de capacidade agravou a lacuna de operação.

**4.** Explique a relevância do ditado de Alan Kay para o brief de projeto que os pesquisadores do Spark se impuseram. *(Hadoop at Yahoo!)*

R: O ditado diz que coisas simples devem ser simples e coisas complexas devem ser possíveis. O estado do Hadoop violava as duas metades. Coisas simples não eram simples, porque a API de MR era verbosa e exigia boilerplate. Coisas complexas eram possíveis apenas ao custo de adotar mais um sistema, com mais uma API e mais uma configuração de cluster. O brief que os pesquisadores tiraram disso foi tornar o Hadoop e o MR mais simples e mais rápidos ao mesmo tempo, o que resulta em um engine único com APIs componíveis. A pergunta que fecha a seção é literalmente essa.

**5.** Explique o que um DAG contribui para a velocidade. Quais dois componentes consomem o DAG, e o que eles produzem? *(Speed)*

R: O Spark constrói suas computações de query como um directed acyclic graph em vez de executar comando a comando. O DAG é consumido pelo DAG scheduler e pelo query optimizer. Os dois produzem um grafo computacional eficiente que, em geral, pode ser decomposto em tasks executadas em paralelo pelos workers do cluster. A contribuição para a velocidade tem duas partes: a otimização acontece sobre o plano inteiro, antes de qualquer execução, e o resultado já vem em pedaços paralelizáveis. Executar comando a comando não permitiria nem uma coisa nem outra.

**6.** Explique whole-stage code generation no nível que o capítulo fornece, e por que gerar código compacto torna a execução mais rápida. *(Speed)*

R: O capítulo entrega uma frase. O Tungsten é o engine de execução física e usa whole-stage code generation para gerar código compacto para execução. Nenhum mecanismo é dado, e a explicação fica prometida para o Capítulo 3. Sobre por que código compacto é mais rápido, o texto só oferece o encadeamento com o parágrafo seguinte, que atribui o ganho aos resultados intermediários em memória e ao I/O de disco limitado. Ou seja, o capítulo justifica a velocidade pela memória e não pela geração de código. Anotei isso como lacuna, porque o nome da técnica aparece em posição de argumento sem sustentar argumento nenhum.

**7.** Explique a estratificação em Ease of Use: o que fica embaixo, o que é construído em cima, e por que os autores apresentam a camada de baixo como a fonte da simplicidade, e não as de cima. *(Ease of Use)*

R: Embaixo fica o RDD, apresentado como uma estrutura de dados lógica simples. Em cima dele são construídas todas as abstrações estruturadas de nível mais alto, entre elas DataFrames e Datasets. Os autores localizam a simplicidade embaixo porque o argumento deles é de contagem de conceitos, não de conveniência. Uma abstração de dados mais duas categorias de operação, transformations e actions, já bastam para descrever o modelo de programação inteiro. As camadas de cima acrescentam poder expressivo, mas herdam a simplicidade em vez de criá-la.

**8.** Explique a diferença entre modularidade e extensibilidade como o capítulo usa os termos. Uma é sobre workloads, a outra é sobre outra coisa. Qual? *(Modularity + Extensibility)*

R: Modularidade é sobre o que roda. As operações do Spark se aplicam a muitos tipos de workload e se expressam em cinco linguagens, e as bibliotecas unificadas cobrem SQL, streaming, machine learning e grafos sob um engine só. Extensibilidade é sobre de onde vem o dado. O Spark se concentra em compute e não em storage, lê de muitos sistemas diferentes, e seus `DataFrameReader`s e `DataFrameWriter`s podem ser estendidos para alcançar outros. São dois eixos independentes: variedade de processamento e variedade de origem.

**9.** Explique por que desacoplar storage de compute é uma afirmação arquitetural e não apenas uma lista de features. O que isso permite não se comprometer? *(Extensibility)*

R: O Hadoop entregava storage e compute no mesmo pacote, então adotar o motor implicava adotar o sistema de arquivos. O Spark entrega só compute. A diferença não é a quantidade de conectores, é a ausência de uma camada de armazenamento dentro do produto. O que isso permite não assumir é o compromisso com um sistema de storage específico. O dado pode estar em Hadoop, Cassandra, HBase, MongoDB, Hive ou num RDBMS, e a troca do storage não implica trocar o engine. Uma lista de features diria quais fontes são suportadas. A afirmação arquitetural diz que storage não faz parte do Spark.

**10.** Explique a alegação de que lógica idêntica em Python, R, Java ou Scala rende performance idêntica. O que na arquitetura torna isso possível, e que classe de código quebraria a alegação? *(Spark SQL + Apache Spark Components as a Unified Stack)*

R: Código escrito em qualquer das Structured APIs não é executado na linguagem em que foi escrito. Ele é convertido em um DAG e decomposto em bytecode altamente compacto, executado nas JVMs dos workers. A linguagem, então, é só a superfície de escrita, e o trabalho de runtime é o mesmo em todas. A classe de código que quebra a alegação é a que não passa pelas Structured APIs. Uma UDF em Python ou uma lambda de RDD não pode ser compilada nesse bytecode e precisa rodar em um processo Python, com serialização nas duas pontas. O capítulo enuncia a regra e nunca menciona a exceção.

**11.** Explique o modelo da "continually growing table". O que ele permite ao desenvolvedor parar de raciocinar, e o que o engine assume? *(Spark Structured Streaming)*

R: O stream é modelado como uma tabela que cresce, com novas linhas anexadas ao fim. O desenvolvedor consulta essa tabela como consultaria uma tabela estática. O que ele para de raciocinar são as fronteiras de lote, os intervalos de tempo e o estado carregado de um lote para o seguinte, que era o vocabulário do modelo anterior. O engine do Spark SQL assume duas coisas explicitamente nomeadas pelo capítulo: fault tolerance e late-data semantics. Ou seja, o engine fica com as duas partes difíceis, o que fazer quando algo falha e o que fazer quando um dado chega atrasado.

**12.** Percorra o trecho de Structured Streaming: qual é a source, o que `explode(split(...))` realiza, o que é agregado, e qual é a sink? *(Spark Structured Streaming)*

R: A source é um socket em `localhost` na porta 9999, lido com `readStream` no formato `socket`. O `split(lines.value, " ")` quebra cada linha em um array de palavras pelo espaço, e o `explode` expande esse array em uma linha por palavra, com o alias `word`. O que é agregado é a contagem corrente por palavra, com `words.groupBy("word").count()`. A sink é um tópico Kafka chamado `output`. Uma ressalva sobre o trecho: ele monta o `writeStream` e nunca chama `.start()`, então a query nunca é iniciada. Como está impresso, o código não roda.

**13.** Explique a sequência completa de trabalho do driver, da instanciação de uma `SparkSession` até as tasks rodando nos executors. Em que ponto a comunicação para de passar pelo cluster manager? *(Spark driver)*

R: O driver começa instanciando a `SparkSession`, que é o canal por onde ele acessa os componentes distribuídos. Pela sessão ele se comunica com o cluster manager e requisita recursos, CPU e memória, para os executors, que são JVMs. Em paralelo, ele transforma todas as operações Spark em computações DAG, escalona essas computações e distribui a execução como tasks entre os executors. A comunicação para de passar pelo cluster manager assim que os recursos são alocados. A partir daí o driver fala direto com os executors, e o cluster manager sai do caminho quente.

**14.** Explique que problema a `SparkSession` resolveu no Spark 2.0, usando a descrição que o capítulo faz do código 1.x. *(SparkSession)*

R: No Spark 1.x cada preocupação tinha seu próprio contexto, um para streaming, outro para SQL, e criar cada um deles introduzia código boilerplate extra. O desenvolvedor precisava saber qual contexto correspondia a qual funcionalidade antes de escrever qualquer coisa. No 2.0 a `SparkSession` virou canal unificado para todas as operações e todos os dados, absorvendo `SparkContext`, `SQLContext`, `HiveContext`, `SparkConf` e `StreamingContext`. Uma sessão por JVM passa a cobrir parâmetros de runtime, DataFrames e Datasets, leitura de fontes, metadados de catálogo e queries SQL. O problema resolvido é de contagem de conceitos, o mesmo que o capítulo dá como motivação geral do 2.x.

**15.** Explique a relação entre partições, executors e cores como o capítulo a apresenta. Depois diga com precisão o que o capítulo *não* especifica sobre essa relação. *(Distributed data and partitions)*

R: O dado físico está distribuído no storage em partições. Cada executor recebe, de preferência, uma task que exige ler a partição mais próxima dele na rede. Dentro do executor, cada core recebe a sua própria partição para trabalhar. O paralelismo, então, é a contagem de cores trabalhando em partições distintas ao mesmo tempo. O que o capítulo não especifica é o dimensionamento. Ele não diz quantas partições deve haver por core ou por executor, não relaciona o número de partições ao número de cores, e não dá critério nenhum para escolher esse número. Ele fala em "one or more partitions" por executor e adia o ajuste para os Capítulos 3 e 7.

**16.** Explique data locality no contexto de leitura de partição, e por que o capítulo se resguarda com "though this is not always possible". *(Distributed data and partitions)*

R: A preferência é que cada executor receba a task que lê a partição mais próxima dele na rede, o que mantém a leitura local e minimiza a banda usada. A ressalva existe porque as duas colocações são decididas por atores diferentes. Onde os executors nascem é decisão do cluster manager, que responde a recursos livres. Onde as partições estão é decisão da camada de storage, tomada antes. Nada garante que as duas coincidam, e quando não coincidem a partição atravessa a rede. O capítulo enuncia a ressalva sem dar essa causa, então a explicação acima é minha.

**17.** Explique o contraste que o capítulo traça entre tarefas de data science e de data engineering. Para o que cada papel otimiza, e onde eles se entregam trabalho um ao outro? *(Data science tasks + Data engineering tasks)*

R: O cientista de dados otimiza para descoberta. Ele limpa o dado, explora para achar padrões e constrói modelos para prever ou sugerir resultados, num trabalho que o capítulo descreve como iterativo, interativo, ad hoc ou experimental. O engenheiro de dados otimiza para pipeline em produção. Ele parte de princípios de engenharia de software e constrói pipelines escaláveis que transformam dado bruto e sujo em dado limpo, armazenado na nuvem, em NoSQL ou em RDBMS, ou exposto a analistas por ferramentas de BI. As entregas cruzadas são duas. O engenheiro entrega dado limpo e consumível ao cientista. O cientista entrega um modelo que não existe isolado e que o engenheiro insere num pipeline maior, ao lado de uma aplicação web ou de um engine de streaming.

**18.** Explique por que o capítulo argumenta que workloads de deep learning se tornaram viáveis no Spark, nomeando as adições versão a versão. *(Data science tasks)*

R: Treino distribuído de deep learning tem duas exigências que o modelo de execução original do Spark não atendia. A primeira é que todas as tasks precisam estar rodando ao mesmo tempo, porque elas se comunicam entre si durante o treino, e uma task que falha derruba a rodada inteira. O Spark 2.4 endereçou isso com um gang scheduler, parte do Project Hydrogen, voltado às necessidades de fault tolerance no treino e no escalonamento desses modelos. A segunda exigência é GPU. O Spark 3.0 acrescentou suporte a coleta de recursos de GPU nos modos standalone, YARN e Kubernetes. Com as duas adições, o capítulo conclui que quem precisa de deep learning pode usar Spark.

---

## Nível 3 — Aplicação e transferência

Cenários concretos. O capítulo te equipa para responder, mas não responde por você.

**1.** Você tem um cluster de 5 worker nodes, cada um rodando um executor com 4 cores. Usando apenas o modelo deste capítulo, quantas partições você deve buscar no mínimo para manter todo core ocupado, e o que acontece com o excedente se você fizer `repartition(60)`? *(Distributed data and partitions)*

R: São 5 executors vezes 4 cores, ou seja, 20 cores. Como cada core recebe a sua própria partição, o mínimo para não deixar core parado é 20 partições. Com menos de 20, alguns cores ficam sem partição e o cluster fica subutilizado. Com `repartition(60)` são 3 partições por core. O capítulo não descreve o que acontece com o excedente. O que ele diz é que cada executor recebe "one or more partitions", então a inferência é que as partições excedentes esperam e cada core as processa em sequência, uma após a outra. O texto não usa a palavra onda, não descreve fila e não trata do custo de mover dados que o próprio `repartition` provoca.

**2.** Você chama `spark.read.text(path).repartition(8)` sobre um arquivo de 40 GB num cluster de 3 executors. Descreva o que cada executor termina segurando, e identifique qual parte da sua resposta o capítulo sustenta e qual você teve que inferir. *(Distributed data and partitions)*

R: O que o capítulo sustenta é curto. O dado é quebrado em oito partições, e cada executor recebe uma ou mais delas para ler na sua memória. Isso é literalmente o que o texto do trecho de código diz. Tudo o mais é inferência. Oito não divide por três, então a distribuição é desigual, algo como 3, 3 e 2 partições. Se as partições ficarem parecidas em tamanho, cada uma tem cerca de 5 GB, e cada executor segura entre 10 e 15 GB. O capítulo não fala em tamanho de partição, não fala em memória do executor, não menciona spill para disco e não avisa que `repartition` redistribui dados entre máquinas. As três primeiras lacunas fazem essa conta parecer mais segura do que é.

**3.** Seu job roda num laptop para desenvolvimento e no YARN em produção. Usando a Tabela 1-1, liste tudo o que muda sobre onde vivem o driver e os executors, e nomeie uma classe de bug que passaria localmente e falharia no cluster por causa dessa diferença. *(Deployment modes)*

R: Em Local, driver e executor rodam na mesma JVM, num único host, e o cluster manager roda nesse mesmo host. Em YARN client, o driver roda num client que nem faz parte do cluster, os executors rodam em containers do NodeManager, e o Resource Manager do YARN trabalha com o Application Master para alocar esses containers. Em YARN cluster, o driver se muda para junto do Application Master, dentro do cluster.

O que muda: driver e executor deixam de compartilhar JVM e host, passa a existir fronteira de processo e rede entre eles, e o cluster manager vira serviço externo em vez de coisa local.

A classe de bug é a que depende de estado compartilhado dentro da JVM. Um contador estático incrementado dentro de uma operação, um objeto mutável capturado em uma closure, um caminho de arquivo local aberto pela task, um `println` que se espera ver no console. Em Local tudo isso funciona por acidente, porque driver e executor são o mesmo processo. Em YARN o objeto é serializado e cada executor recebe a sua cópia, o arquivo não existe na máquina do executor, e a saída vai para o log do container.

**4.** Você herda código Spark 1.x que constrói um `SQLContext` e um `StreamingContext` separadamente. O que o capítulo diz que vai acontecer se você rodá-lo como está, e pelo que você substituiria? *(SparkSession, NOTE)*

R: A NOTE diz que os contextos individuais e seus métodos continuam acessíveis no Spark 2.x, que a comunidade manteve compatibilidade retroativa, e que código antigo 1.x com `SparkContext` ou `SQLContext` continua funcionando. A substituição é uma `SparkSession` única por JVM, construída com `SparkSession.builder`, usada para tudo: parâmetros de runtime, DataFrames e Datasets, leitura de fontes, metadados de catálogo e queries SQL. Uma ressalva: a garantia de compatibilidade que a NOTE dá nomeia `SparkContext` e `SQLContext`. Ela não nomeia `StreamingContext`, que aparece só na lista dos pontos de entrada absorvidos. Metade do caso desta questão fica, portanto, fora da promessa explícita do capítulo.

**5.** Um colega quer migrar um pipeline de MLlib escrito contra `spark.mllib`. O que a nota do capítulo diz sobre o futuro desse pacote, e qual deve ser o alvo? *(Spark MLlib, NOTE)*

R: A nota diz que o `spark.mllib` contém as APIs baseadas em RDD, que estão em modo de manutenção, e que todas as features novas vão para o `spark.ml`. O alvo é o `spark.ml`, o pacote com as APIs baseadas em DataFrame. O argumento a favor da migração não é só de features novas: o capítulo credita as melhorias de performance ao Catalyst e ao Tungsten, e é a camada de DataFrame que passa por eles. Modo de manutenção também não é deprecação, então o pipeline antigo não quebra por si só. *No item 4 do Nível 5 verifiquei o status atual do pacote.*

**6.** Você precisa fazer ETL de uma coleção MongoDB e de um tópico Kafka para a mesma análise. Qual das quatro características de projeto isso exercita, e qual mecanismo o capítulo nomeia para cada fonte? *(Extensibility)*

R: Exercita extensibility, que é a característica sobre origem dos dados. Os dois mecanismos são diferentes. O MongoDB está na lista de fontes que o Spark lê diretamente, ao lado de Hadoop, Cassandra, HBase, Hive e RDBMSs. O Kafka está na segunda lista, a das fontes alcançadas ao estender os `DataFrameReader`s e `DataFrameWriter`s, junto com Kinesis, Azure Storage e Amazon S3. Modularity também é exercitada, porque uma aplicação só cobre as duas origens, mas a característica que a questão mira é a primeira.

**7.** Um stakeholder pergunta se você precisa de um sistema separado para alertas em tempo real sobre o seu pipeline batch. Responda com o argumento de unificação do capítulo e nomeie o que você evita aprender e operar. *(Unified Analytics + Modularity)*

R: Não precisa. O argumento está no paper premiado pela ACM: o Spark substitui os engines separados de batch, grafo, stream e query por um stack unificado de componentes que atende workloads diversos sob um único engine distribuído. O Structured Streaming é um dos quatro componentes desse stack, e a seção de modularidade afirma que uma única aplicação Spark pode fazer tudo, sem engines distintos e sem aprender APIs separadas. O que se evita é concreto: um engine adicional como o Storm, que é justamente um dos que o paper cita como substituídos, uma segunda API, uma segunda configuração de cluster, e a complexidade operacional que o capítulo atribui à era dos sistemas sob medida.

**8.** Você precisa que dados públicos coletados e sujos sejam limpos e fiquem consultáveis por analistas através de uma ferramenta de BI. Trace esse pipeline pela seção de data engineering do capítulo: quais APIs, qual otimizador, quais sinks? *(Data engineering tasks)*

R: As APIs são as Structured Streaming APIs para o ETL, que o capítulo indica para construir pipelines complexos a partir de fontes em tempo real e estáticas, e as APIs de alto nível baseadas em DataFrame com queries em DSL para limpar e combinar dados de múltiplas origens. A escolha entre RDDs, DataFrames e Datasets fica aberta, e o capítulo recomenda a camada alta. O otimizador é o Catalyst, para SQL, acompanhado do Tungsten para geração de código compacto. Os sinks que o capítulo nomeia são a nuvem, um NoSQL ou um RDBMS para geração de relatórios, com o dado acessível a analistas por ferramentas de BI. A parte que o capítulo cobre bem é o meio do pipeline. A ingestão de dado sujo de origens heterogêneas ele resolve por extensibility, e a etapa de qualidade do dado não tem tratamento nenhum além da palavra "cleansed".

**9.** Seu time precisa de análise de grafos sobre um pipeline baseado em DataFrame. Usando o capítulo mais a nota de rodapé 1, exponha as duas opções e a fricção que cada uma introduz. *(GraphX + footnote 1)*

R: A primeira opção é o GraphX, que faz parte do stack e é o componente que o capítulo documenta. A fricção é de tipo: a API é construída sobre `Graph(vertices, edges)`, e não sobre DataFrames, então o pipeline precisa converter para entrar e converter de volta para sair. Nessa travessia ele desce da camada das Structured APIs, que é justamente a camada onde o Catalyst e o Tungsten atuam.

A segunda opção é o GraphFrames, da nota de rodapé, similar ao GraphX mas com APIs baseadas em DataFrame. A fricção é de suporte: ele não faz parte do Spark. É um projeto open source contribuído pela Databricks, então entra como dependência externa, com ciclo de release e compatibilidade de versão próprios.

Uma observação sobre o capítulo. Ele apresenta o GraphX como um dos quatro componentes e relega a alternativa alinhada ao resto do stack a uma nota de rodapé de duas linhas, sem versão, sem maturidade e sem declaração de suporte. Para quem já trabalha em DataFrames, a nota de rodapé é a informação mais importante da seção. *No item 3 do Nível 5 verifiquei o estado atual dos dois.*

**10.** Você está escolhendo entre RDDs, DataFrames e Datasets para um novo job de ETL. Com base apenas neste capítulo, que argumento ele dá para a escolha de nível mais alto, e o que ele deixa de te contar que você precisaria antes de decidir? *(Data engineering tasks + The Developer's Experience)*

R: O argumento pela camada alta está em "The Developer's Experience". O Spark 2.x introduziu abstrações de nível mais alto como construções de linguagem de domínio, e a proposta é declarar o que computar em vez de como computar, deixando o Spark determinar a melhor execução. A seção de data engineering complementa: as melhorias de performance vêm do Catalyst e do Tungsten, e o desenvolvedor fica livre para focar nas APIs baseadas em DataFrame porque o Spark esconde a complexidade de distribuição e de fault tolerance.

O que falta é quase tudo o que uma decisão exige. O capítulo nunca define o que é um Dataset nem o distingue de um DataFrame. Nunca diz em quais linguagens cada um existe. Não menciona type safety, que é a razão de existir do Dataset. Não diz o que se perde ao sair do RDD nem quando o RDD ainda é a escolha certa. Ele apresenta "any of the three Spark APIs" como um menu plano, sem critério de escolha. *No item 7 do Nível 5 verifiquei a lacuna mais grave dessa lista.*

**11.** O exemplo do builder de `SparkSession` define `spark.sql.shuffle.partitions` como 6. Explique o que se pode e o que não se pode concluir sobre esse ajuste apenas com este capítulo, e por que um default de 6 seria incomum em produção. *(SparkSession)*

R: Dá para concluir três coisas. É uma configuração passada no builder da `SparkSession`, portanto de escopo de sessão e de JVM. O valor é uma contagem de partições. E é definida antes do `getOrCreate()`, junto do `appName`.

Não dá para concluir o que é um shuffle, porque a palavra não aparece em nenhum outro lugar do capítulo. Também não dá para saber quando o ajuste se aplica, qual é o valor padrão, o que acontece se for alto ou baixo demais, nem como escolher. O capítulo trata a linha como decoração de um exemplo de sintaxe.

Sobre por que 6 é incomum em produção, o próprio capítulo dá o instrumento. Cada core de executor recebe uma partição. Com 6 partições, no máximo 6 cores trabalham, e todo o resto do cluster fica ocioso. O número faz sentido num laptop em modo Local e em quase nenhum cluster real. *No item 8 do Nível 5 verifiquei o default atual e o que mudou nessa recomendação.*

**12.** Seu job precisa de GPUs para treinar modelos. Qual versão do Spark e quais modos de deployment o capítulo diz que tornam isso possível, e o que você checaria antes de supor que seu cluster se qualifica? *(Data science tasks + Deployment modes)*

R: A versão é o Spark 3.0, que introduziu suporte a coleta de recursos de GPU. Os modos são standalone, YARN e Kubernetes.

O que checar antes de supor: qual modo de deployment está em uso, porque a Tabela 1-1 lista cinco modos e a lista de GPU cobre três. O modo Local não aparece nela, e o Mesos, que o capítulo lista como cluster manager suportado, também fica de fora. Depois, a versão do Spark efetivamente instalada, já que o capítulo foi escrito contra o 3.0-preview2. O capítulo não diz mais nada: nada sobre driver de GPU, nada sobre como o Spark descobre a GPU na máquina, nada sobre como se pede o recurso. *Fui atrás desses mecanismos no item 6 do Nível 5.*

**13.** Pegue o trecho de Spark SQL que lê `committers.json` do S3 e descreva o que acontece no cluster para cada uma das suas duas instruções: qual delas move dados, qual planeja trabalho, e onde o resultado vive. *(Spark SQL + Apache Spark's Distributed Execution)*

R: A primeira instrução, `spark.read.json("s3://apache_spark/data/committers.json").createOrReplaceTempView("committers")`, é a que move dados. Ela lê o arquivo do bucket S3 para dentro do cluster e registra uma view temporária com esse nome. Sem schema declarado, a leitura precisa amostrar o arquivo para inferir os tipos, então há trabalho real acontecendo aqui.

A segunda, `val results = spark.sql("SELECT ...")`, planeja trabalho. Ela produz um DataFrame, que o capítulo descreve como o resultado lido em memória.

O resultado vive distribuído nos executors, como partições. Nada no trecho traz dado para o driver, porque não há coleta nem exibição.

Uma ressalva importante: o capítulo nunca explica lazy evaluation nem distingue transformations de actions. Só com ele, não é possível afirmar se a segunda instrução já executou ou apenas registrou um plano. A resposta acima usa conhecimento de fora nesse ponto, e é a lacuna registrada no item 9 do Nível 5.

---

## Nível 4 — Análise e síntese

Raciocínio que cruza seções. Respostas defensáveis em vez de resposta única, mas todos os ingredientes estão no capítulo.

**1.** O capítulo dá três razões para a velocidade do Spark. Uma delas não é conquista do Spark de forma alguma. Identifique-a e reescreva a afirmação da seção de modo que ela fique defensável.

R: É a primeira. O capítulo diz que a implementação interna do Spark "benefits immensely" dos avanços recentes da indústria de hardware em preço e performance de CPU e memória, com servidores de commodity baratos, centenas de gigabytes de memória, múltiplos cores e um sistema operacional Unix eficiente em multithreading. Nada disso é conquista do Spark. É o ambiente em que ele roda, e é o mesmo ambiente disponível para o Hadoop MapReduce, para o Storm e para qualquer concorrente. Uma razão que se aplica igualmente a todos os competidores não explica a diferença entre eles.

O que sobra de defensável é a decisão de projeto de apostar nesse hardware. Reescrita: "O Spark foi projetado assumindo servidores com muita memória e muitos cores, e por isso mantém resultados intermediários em memória em vez de em disco. Frameworks projetados quando memória era cara, como o MapReduce, materializam entre estágios e não conseguem aproveitar o mesmo hardware da mesma forma."

Assim a afirmação vira comparativa e verificável. A original é uma observação sobre a Lei de Moore vestida de argumento de produto.

**2.** O capítulo afirma que uma partição é tratada como "a high-level logical data abstraction — as a DataFrame in memory". Teste essa frase contra o resto do capítulo. Uma partição é um DataFrame, ou outra coisa? Escreva a frase que você acha que os autores quiseram dizer.

R: Não é um DataFrame, e o próprio capítulo derruba a frase em três lugares.

Primeiro, o parágrafo seguinte diz que cada core de executor recebe a sua própria partição. Se cada partição fosse um DataFrame, uma leitura de oito partições produziria oito DataFrames, e não o `log_df` único que o trecho de código imprime.

Segundo, o trecho de particionamento chama `log_df.rdd.getNumPartitions()`. A contagem de partições é uma propriedade de um DataFrame. Uma coisa não é uma propriedade de si mesma.

Terceiro, a definição de DataFrame que o capítulo usa em todo o resto é a de coleção distribuída. Distribuída em quê? Em partições. A frase inverte a relação de contenção.

A frase que os autores quiseram dizer, provavelmente: "Embora o dado esteja distribuído como partições pelo cluster físico, o Spark apresenta o conjunto delas ao desenvolvedor como uma única abstração lógica de alto nível, um DataFrame. Cada partição é uma fatia desse DataFrame, residente na memória de um executor."

O erro tem consequência pedagógica, porque a frase original ensina exatamente o oposto do que a abstração faz. O ganho é esconder o particionamento atrás de um objeto único, e a frase dissolve o objeto único em muitos.

**3.** A seção de `SparkSession` diz que o shell a expõe "via a global variable called `spark` or `sc`". Confira isso contra a lista, na mesma seção, do que a `SparkSession` absorveu. O que está errado na frase, e por que esse deslize em particular tende a confundir um iniciante?

R: A mesma seção afirma que a `SparkSession` absorveu o `SparkContext`, entre outros pontos de entrada. Então `spark` e `sc` não podem ser dois nomes para a mesma coisa. No shell, `spark` é a `SparkSession` e `sc` é o `SparkContext`, que continua existindo por baixo dela. São dois objetos com APIs diferentes, e o "or" da frase os apresenta como alternativas equivalentes.

A confusão para um iniciante é específica. Quase todo material de Spark alterna entre `sc.textFile(...)` e `spark.read.text(...)`, e essas duas linhas pertencem a camadas diferentes: a primeira é API de RDD, a segunda é DataFrame. Quem leu a frase do capítulo espera que as duas variáveis ofereçam os mesmos métodos e vai receber um erro de método inexistente na primeira tentativa de trocar uma pela outra. Pior: o erro vai parecer um problema de versão, e não de camada de API.

O deslize também apaga a única pista que o capítulo dá sobre a existência de duas camadas. Se `sc` fosse apresentado como o contexto de baixo nível acessível pela sessão, o leitor sairia do capítulo sabendo que existe algo abaixo do DataFrame. Do jeito que está, ele sai achando que existe um objeto só, com dois apelidos.

**4.** Compare os números de comunidade do texto com os números visíveis na Figura 1-7. Liste cada discrepância e decida se é arredondamento, defasagem ou erro. O que isso te diz sobre citar figuras de livros?

R: A prosa cita quatro números e a figura mostra os quatro, mais alguns que a prosa ignora.

| Item | Prosa | Figura 1-7 | Veredito |
|---|---|---|---|
| contribuidores | "close to 1,500" | 1.488 | arredondamento honesto |
| releases | "well over 100" | 118 | correto, mas vago |
| forks | "21,000" | 21,5 mil | arredondado para baixo |
| commits | "some 27,000" | 26.991 | arredondamento honesto |

Nenhuma discrepância é erro nem defasagem: as quatro são arredondamento, e todas na direção conservadora ou neutra. O texto não infla.

O que chama atenção é o que a prosa **deixa de fora**. A figura mostra 25,8 mil estrelas, 2,1 mil watchers, 20 branches, 260 pull requests abertos e 292 projetos dependentes, e nada disso é mencionado. A seleção não é aleatória: contribuidores, releases, forks e commits sustentam o argumento de comunidade ativa que o parágrafo está fazendo. Estrelas mediriam popularidade, não atividade.

Sobre citar figuras de livro: os números têm data de captura e ela não aparece em lugar nenhum. A figura é uma foto de um estado que muda todo dia, impressa num livro que vive anos. Comparar prosa e figura serve para checar coerência interna do livro, e não para saber o estado do projeto. Para isso o caminho é abrir a URL que a própria legenda cita.

O que dá para registrar sem a figura são duas coisas.

A primeira é que o texto ancora quatro números em uma captura de tela do GitHub: perto de 1.500 contribuidores, bem mais de 100 releases, 21.000 forks e cerca de 27.000 commits. Contadores do GitHub mudam todo dia. Qualquer valor impresso já está defasado na data em que o livro sai da gráfica, e a defasagem só cresce. Em agosto de 2026 esses quatro números estão seis anos atrasados.

A segunda é que o capítulo dá o próprio exemplo do problema, poucas linhas antes. Ele diz que o Spark 3.0 é "the most recent major release ... coming in 2020" e, na mesma página, que o código foi testado contra o 3.0-preview2. Escrito no futuro do pretérito, o texto envelhece no instante da publicação.

A lição sobre citar figuras de livros: um número em captura de tela é a forma mais frágil de evidência que um livro técnico pode usar, porque não tem data no corpo, não tem definição do que está contando e não pode ser reconferido no estado em que foi capturado. Para uma nota permanente, o número certo é o que eu mesma levanto na fonte, com a data de acesso registrada. Foi o que fiz no Nível 5.

**5.** O capítulo diz que o Spark "decouples" storage e compute, ao contrário do Hadoop. Mesmo assim o HDFS aparece nos seus exemplos e na seção de partições. Concilie a afirmação com a prática: o que exatamente está desacoplado?

R: O que está desacoplado é a dependência, não o uso. O Hadoop entregava storage e compute como um produto só, então adotar o MapReduce implicava adotar o HDFS. O Spark não traz camada de armazenamento nenhuma, então ele precisa de algum storage e não exige um específico.

A presença do HDFS nos exemplos é consistente com isso. A seção de partições diz que o dado físico reside "in either HDFS or cloud storage", e o "either ... or" é justamente a prova do desacoplamento. O exemplo de Spark SQL lê do S3, o de MLlib lê do S3, o de GraphX lê de `hdfs://`. Quatro exemplos, três origens diferentes, mesmo engine.

O que continua acoplado, e o capítulo não diz, é a performance. Data locality só funciona se o executor puder ser colocado perto do dado, e isso pressupõe que compute e storage compartilhem máquinas. É o modelo do HDFS. Com storage em objeto na nuvem, não existe partição "mais próxima na rede" no mesmo sentido, e a ressalva "though this is not always possible" vira a regra em vez da exceção. Então o desacoplamento é real na arquitetura e parcial na prática: escolher storage remoto é escolher pagar rede em toda leitura.

Formulação precisa: o Spark desacopla a escolha de storage da escolha de engine. Ele não desacopla a performance da colocação do dado.

**6.** Quatro componentes são apresentados como pares no stack unificado. Usando as notas de versão espalhadas pelo capítulo (a divisão da MLlib no 1.6, a linha do tempo 2.0/2.2 do Structured Streaming, a nota de rodapé sobre GraphFrames), argumente que eles não estão, de fato, no mesmo nível de maturidade nem de alinhamento arquitetural. Qual é o discrepante?

R: A Figura 1-3 desenha quatro caixas irmãs, e as notas de versão desmontam essa simetria em dois eixos.

No eixo de maturidade, os quatro têm histórias diferentes. O Spark SQL não tem nota de versão nenhuma, e é o único ao qual o capítulo atribui uma conformidade externa, ANSI SQL:2003. A MLlib carrega uma cisão desde o 1.6, com metade do pacote em modo de manutenção, ou seja, ela é um componente com um passivo. O Structured Streaming tem data recente: experimental no 2.0, GA no 2.2, e convive com um Continuous Streaming model que o capítulo cita como experimental e nunca mais menciona. O GraphX não tem nota de versão, e a ausência aqui não significa estabilidade, significa silêncio.

No eixo de alinhamento arquitetural a assimetria é maior e mais decisiva. O Structured Streaming é declarado "built atop the Spark SQL engine and DataFrame-based APIs". A MLlib fornece algoritmos "built atop high-level DataFrame-based APIs". Dois dos quatro componentes se apoiam explicitamente no terceiro. Isso não é uma relação entre pares, é uma pilha de três andares desenhada como fileira.

O discrepante é o GraphX. Ele é o único que não tem API baseada em DataFrame, o único cujo exemplo de código não toca em DataFrame nem em `spark.read`, e o único cujos algoritmos o capítulo credita a contribuições da comunidade em vez de ao projeto. É também o único que tem um substituto: o GraphFrames, que faz a mesma coisa com APIs de DataFrame, existe fora do Spark e aparece em nota de rodapé. Quando a alternativa alinhada ao stack mora numa nota de rodapé, o componente do stack é que está fora de lugar.

**7.** Monte uma tabela com uma linha por componente (Spark SQL, MLlib, Structured Streaming, GraphX) e colunas para: abstração primária, engine subjacente, linguagens mostradas nos exemplos do capítulo, e maturidade declarada. Quais células o capítulo deixa em branco, e o padrão dos brancos é informativo?

R:

| Componente | Abstração primária | Engine subjacente | Linguagem do exemplo | Maturidade declarada |
|---|---|---|---|---|
| Spark SQL | DataFrame, mais tabelas permanentes ou temporárias | *não informado* (só o core engine genérico) | Scala | *não informada*, mas declarado ANSI SQL:2003-compliant |
| Spark MLlib | APIs de alto nível baseadas em DataFrame (`spark.ml`) | *não informado* (crédito genérico às melhorias do engine no 2.x) | Python | dividida desde o 1.6, com `spark.mllib` em modo de manutenção |
| Structured Streaming | continually growing table, sobre APIs baseadas em DataFrame | Spark SQL core engine (explícito) | Python | experimental no 2.0, GA no 2.2 |
| GraphX | grafo, construído com `Graph(vertices, edges)` | *não informado* | Scala | *não informada*; a nota de rodapé aponta o GraphFrames |

Os brancos se concentram na coluna de engine subjacente. Três das quatro células estão vazias, e a única preenchida nomeia o Spark SQL. O padrão é informativo por dois motivos.

Primeiro, ele confirma o argumento da questão anterior por omissão. Se três componentes rodam sobre "o core engine" sem especificação e um roda declaradamente sobre o Spark SQL, a hierarquia real vaza pela única célula preenchida.

Segundo, a coluna de maturidade só tem valor onde a informação é constrangedora. A MLlib declara a cisão, o Structured Streaming declara a idade, e os dois componentes sem declaração são justamente o mais consolidado e o mais parado. Não declarar maturidade serve ao Spark SQL e esconde o GraphX, com o mesmo silêncio.

Terceiro ponto, sobre a coluna de linguagens: o capítulo afirma cinco linguagens suportadas e mostra exemplos em duas. Nenhum componente aparece em mais de uma linguagem. A alegação de paridade entre linguagens, que o capítulo faz de forma forte na seção de Spark SQL, não é demonstrada em nenhum lugar.

**8.** A evidência de performance neste capítulo vem de três fontes: os 10 a 20x dos primeiros papers, a alegação sem qualificação de "many orders of magnitude faster", e o prêmio da ACM. Ordene-as por quanto peso elas devem carregar para prever o seu próprio workload, e diga o que cada uma omite.

R: Da mais forte para a mais fraca:

**1. Os 10 a 20x dos primeiros papers.** É a melhor porque tem número finito, tem escopo declarado ("for certain jobs"), tem baseline nomeado (Hadoop MapReduce) e vem de publicação acadêmica, sujeita a revisão. Também é a única que se conecta a um mecanismo explicado no capítulo: o ganho vem de eliminar o ciclo de escrita e leitura em disco entre estágios, e por isso aparece em jobs iterativos e interativos. Omite quais jobs eram esses, o tamanho dos dados, o hardware e se o MapReduce foi ajustado com o mesmo cuidado. E é de 2009 a 2012, então mede um MapReduce de quinze anos atrás.

**2. O prêmio da ACM de novembro de 2016.** Fica no meio porque não é evidência de performance, é evidência de reconhecimento. O prêmio foi dado ao paper que descreve o Spark como "Unified Engine for Big Data Processing", ou seja, premiou a ideia de unificação, não uma medição. Como sinal de que a abordagem é sólida, vale. Como previsão de quanto o meu job vai demorar, não diz nada. O capítulo o coloca na seção de Unified Analytics, e não na de Speed, o que é honesto. Ele entra nesta lista porque funciona como aval de qualidade.

**3. "Today, it's many orders of magnitude faster."** A mais fraca, e por larga margem. Não tem fonte, não tem data, não tem baseline, não tem tipo de job e não tem número. "Many orders of magnitude" implica pelo menos mil vezes, o que é maior que a alegação de marketing de 100x que o outro livro cita, e ainda assim vem sem nenhum apoio. A frase é mais forte que a evidência que a antecede e vem imediatamente depois dela, o que faz o número medido de 10 a 20x parecer conservadorismo histórico em vez do único dado real do parágrafo. Omite tudo.

As três compartilham a mesma omissão de fundo: nenhuma diz nada sobre os meus dados, o meu cluster ou a forma das minhas queries. Benchmark e prêmio comparam sistemas entre si. Nenhum dos dois prevê um caso particular.

**9.** A tese do capítulo é que a unificação reduz custo e complexidade. Construa o contra-argumento usando apenas o material do próprio capítulo. Onde colocar tudo sob um engine cria custos novos? A história da migração do Spark 2.x contra o 1.x e a matriz Mesos/YARN/Kubernetes/standalone são úteis aqui.

R: O capítulo fornece cinco peças.

A primeira é a migração 2.x. O argumento da unificação é que o desenvolvedor lida com menos conceitos. Mas o capítulo descreve um framework que já teve que se reunificar internamente uma vez: no 1.x havia `SparkContext`, `SQLContext`, `HiveContext`, `SparkConf` e `StreamingContext`, e o 2.0 os absorveu numa `SparkSession`. A NOTE informa que os cinco continuam acessíveis por compatibilidade retroativa. O resultado é que existem duas formas válidas de escrever a mesma coisa, e todo material publicado antes de 2016 ensina a antiga. Unificar depois não apaga a versão anterior, acrescenta uma tradução obrigatória.

A segunda é a matriz de deployment. Quatro cluster managers, cinco modos, e a Tabela 1-1 existe porque driver, executor e cluster manager mudam de lugar em cada combinação. Um engine que roda em toda parte é um engine cuja operação precisa ser aprendida por ambiente. A unificação do que roda não unifica onde roda.

A terceira é a MLlib. Um componente do stack unificado carrega dois pacotes, um deles em modo de manutenção, com features novas indo só para um lado. A pessoa que adota o "engine unificado" adota também essa bifurcação interna.

A quarta é o menu de três APIs. RDDs, DataFrames e Datasets são oferecidos como escolha livre, sem critério. Substituíram-se vários engines por um, e dentro dele apareceram três formas de expressar o mesmo trabalho, com performances diferentes que o capítulo não caracteriza.

A quinta é a Figura 1-2 e o ecossistema de pacotes de terceiros. O capítulo apresenta o stack como suficiente e, na mesma seção, aponta uma lista mantida pela comunidade de conectores, monitores de performance e outros. Se o engine unificado bastasse, não haveria ecossistema a manter.

Fechando: o Spark reduz o número de engines e não reduz o número de decisões. Ele troca "qual sistema uso para cada workload" por "qual API, qual cluster manager, qual modo de deployment, qual pacote de MLlib, quantas partições". A complexidade mudou de lugar, das fronteiras entre sistemas para dentro de um sistema só, e o capítulo só contabiliza o lado que sumiu.

**10.** Trace a escada de abstração que o capítulo sugere: RDD → DataFrame/Dataset → Structured APIs → SQL. Em cada degrau, o que o desenvolvedor ganha e de que controle ele abre mão? Onde a alegação de "express what, not how" começa a custar alguma coisa?

R: No **RDD** o controle é total. O capítulo o chama de estrutura de dados lógica simples e de abstração fundamental, e o modelo é transformations e actions sobre uma coleção. A pessoa decide a sequência de operações e passa funções arbitrárias para rodar no cluster. O custo é que não existe otimizador entre o que ela escreveu e o que executa. Todo ganho de eficiência é responsabilidade dela.

No **DataFrame/Dataset** entra a estrutura. Colunas nomeadas, tabelas temporárias e permanentes, e o Catalyst por trás. O que se entrega é a arbitrariedade das funções, porque o otimizador só otimiza o que consegue enxergar. O Dataset acrescentaria tipagem sobre o DataFrame, mas o capítulo nunca diz o que ele é nem em que difere, então este degrau fica meio desenhado.

Nas **Structured APIs** o ganho é a independência de linguagem. O capítulo afirma que o código escrito em Java, R, Scala, SQL ou Python vira o mesmo bytecode compacto nas JVMs dos workers, com a mesma performance. O que se entrega é a possibilidade de executar código nativo da linguagem de origem. A alegação de paridade só vale enquanto se fica dentro dessa superfície, e o capítulo não menciona o que acontece fora dela.

No **SQL** o alcance é máximo e a declaratividade é completa. O Spark SQL funciona como engine SQL puro e aceita ANSI SQL:2003, então quem sabe SQL entra sem aprender nada de Spark. O que se entrega é todo o controle procedural. Só se pode expressar o que a linguagem expressa.

Onde "express what, not how" começa a custar: no momento em que a execução vai mal. A promessa é declarar o objetivo e deixar o Spark achar o melhor caminho, e ela funciona enquanto o caminho escolhido é bom. Quando não é, a pessoa precisa intervir, e intervir exige o vocabulário do "how", que a abstração escondeu e que o capítulo nunca ensina. O sintoma está no próprio texto: o único ajuste de performance que ele mostra é `spark.sql.shuffle.partitions`, definido como 6 sem explicação, num capítulo que não define shuffle. É a escada inteira em uma linha. Sobe-se até o topo declarativo e desce-se de volta ao primeiro degrau para consertar, sem os degraus intermediários.

**11.** O capítulo menciona `spark.sql.shuffle.partitions` em um exemplo de código e não menciona "shuffle" em nenhum outro lugar. Explique por que um capítulo que gasta páginas com partições e data locality não consegue, de fato, explicar essa chave de configuração, e qual conceito está faltando.

R: O capítulo trata partição como um fato de armazenamento. O dado físico está distribuído no storage como partições, em HDFS ou na nuvem, e o executor lê a partição mais próxima dele. Nesse modelo, partições são coisas que existem antes do job começar e que o job apenas consome, com preferência por proximidade.

`spark.sql.shuffle.partitions` fala de outra coisa. Ele controla quantas partições são produzidas *durante* a execução, quando o engine precisa reagrupar o dado por chave e o resultado desse reagrupamento vira um novo conjunto de partições. Não há como explicar uma quantidade produzida a um leitor que só conhece partições como quantidade encontrada.

O conceito faltante é o shuffle, e ele traz três outros consigo. Sem a distinção entre transformations e actions, não existe momento de execução em que algo possa ser produzido. Sem lazy evaluation, não existe plano acumulado que precise ser cortado em algum ponto. Sem job, stage e task, não existe fronteira onde o corte aconteça. O shuffle é exatamente essa fronteira: o ponto em que o dado precisa atravessar a rede porque os valores de uma mesma chave estão em máquinas diferentes.

Há um segundo problema, mais grave. O modelo de data locality do capítulo diz que o Spark evita mover dados pela rede. O shuffle é a operação que move dados pela rede por necessidade e que domina o custo dos jobs reais. Um leitor que só tem o capítulo sai com a impressão de que movimentação de dados é uma exceção que o scheduler evita. É o oposto: é o custo central, e a chave de configuração que ele viu sem explicação é o principal instrumento para controlá-lo.

**12.** A seção Ease of Use nomeia transformations e actions como o modelo de programação, e depois nunca as distingue. Encontre todos os trechos de código do capítulo e classifique cada linha como transformation ou action usando conhecimento externo. Quantos dos exemplos do capítulo se comportariam de forma diferente do que um leitor ingênuo espera, e por quê?

R: São seis trechos.

**a) Spark SQL, Scala.** `spark.read.json(...)` lê e infere schema, então dispara trabalho real. `.createOrReplaceTempView("committers")` é operação de catálogo, nem transformation nem action. `val results = spark.sql("SELECT ...")` é lazy: produz um DataFrame e não executa. **Diverge da expectativa.** O comentário do capítulo diz "issue a SQL query and return the result", e nada é retornado. O `results` nunca é consumido.

**b) MLlib, Python.** `spark.read.csv(...)` lê. `lr.fit(training)` executa de verdade, porque treinar exige percorrer os dados. `lrModel.transform(test)` é lazy e o resultado é descartado, já que não é atribuído a nada. **Diverge.** A linha comentada como `# Predict` não prediz nada.

**c) Structured Streaming, Python.** `readStream...load()` define a source. `select(explode(split(...)))` é transformation. `groupBy("word").count()` é transformation com agregação. `writeStream.format("kafka").option("topic", "output")` devolve um `DataStreamWriter` e para aí. **Diverge, e é o caso mais grave.** Falta o `.start()`. Sem ele nenhuma query é iniciada, nada é lido do socket e nada chega ao Kafka. Falta também `checkpointLocation`, que a sink Kafka exige. O comentário diz "Write out to the stream to Kafka" e o código só configura um escritor.

**d) GraphX, Scala.** `Graph(vertices, edges)` constrói. `spark.textFile("hdfs://...")` **não existe**: `textFile` é método do `SparkContext`, não da `SparkSession`. O trecho também escreve `messages =` sem `val`. `graph.joinVertices(...)` é transformation. **Diverge**, e aqui por erro de código, não por lazy evaluation.

**e) Builder de `SparkSession`, Scala.** `getOrCreate()` cria a sessão. `spark.read.json("...")` lê. `spark.sql("SELECT ...")` é lazy. Comportamento coerente com o propósito do trecho, que é mostrar sintaxe.

**f) Particionamento, Python.** `spark.read.text(...)` lê. `.repartition(8)` é transformation, e é uma wide transformation, ou seja, um shuffle. `getNumPartitions()` consulta o plano e não processa dado. `spark.range(0, 10000, 1, 8)` é transformation. **Diverge parcialmente.** Um leitor ingênuo espera que os dois trechos tenham reparticionado 40 GB para imprimir `8`. Eles não moveram nada: o `8` sai do plano, não da execução.

**Contagem: quatro dos seis divergem** da leitura ingênua, e um quinto diverge em parte. A causa é sempre a mesma. O capítulo nomeia transformations e actions como o modelo de programação inteiro e nunca diz que as primeiras não executam. Sem essa frase, todo trecho parece uma sequência de comandos, e três deles são, na verdade, planos que nunca são disparados.

**13.** Comprima o argumento do capítulo em uma frase por seção principal (Genesis, What Is Spark, Unified Analytics, Distributed Execution, Developer's Experience), de modo que remover qualquer uma quebre o argumento.

R:

1. *(The Genesis of Spark)* O MapReduce resolveu a escala do Google e do Yahoo!, mas era lento em jobs iterativos por escrever cada resultado intermediário em disco, difícil de programar, e incapaz de cobrir ML, streaming e SQL interativo sem que um sistema separado fosse adotado para cada um.
2. *(What Is Apache Spark?)* O Spark responde a esse diagnóstico com um engine único que mantém intermediários em memória, expõe uma abstração fundamental (RDD) com duas categorias de operação, cobre todos aqueles workloads em cinco linguagens, e não traz storage próprio.
3. *(Unified Analytics)* Como os quatro componentes são bibliotecas sobre o mesmo core engine, e todo código escrito neles vira o mesmo DAG e o mesmo bytecode, uma aplicação só combina SQL, ML, streaming e grafos sem trocar de engine nem de API.
4. *(Apache Spark's Distributed Execution)* Essa unificação é executável porque um driver, através de uma `SparkSession`, negocia recursos com um cluster manager, distribui o DAG como tasks entre executors, e cada core de executor trabalha sobre a sua partição de dados.
5. *(The Developer's Experience)* Com o mecanismo pronto, o Spark 2.x moveu o desenvolvedor para abstrações de nível mais alto, onde ele declara o que quer computar em vez de como, o que é o que permite a data engineers, data scientists e ML engineers usarem o mesmo engine.

Remover qualquer uma quebra a cadeia. Sem a 1 não há problema a resolver, e o Spark vira mais um framework. Sem a 2 falta a resposta ao problema. Sem a 3 sobra um engine batch rápido, sem o argumento de unificação, que é a tese do capítulo. Sem a 4 a unificação é promessa sem mecanismo, e nada explica como o trabalho acontece. Sem a 5 falta o beneficiário: o capítulo termina descrevendo uma máquina, sem dizer por que um humano a escolheria. A cadeia é problema, resposta, escopo, mecanismo e público.

**14.** Identifique três afirmações feitas sem evidência neste capítulo, e diga que evidência resolveria cada uma.

R:

**1. "Today, it's many orders of magnitude faster."** Sem fonte, sem data, sem baseline, sem tipo de job. Vem logo depois do único número medido do parágrafo, os 10 a 20x, e o supera em pelo menos uma ordem de grandeza sem nenhum apoio. Resolveria: um benchmark nomeado, com versão do Spark, versão do MapReduce, hardware, tamanho de dados, tipo de job e o mesmo esforço de tuning dos dois lados. Publicando a distribuição, e não o máximo.

**2. "You can write similar code snippets in Python, R, or Java, and the generated bytecode will be identical, resulting in the same performance."** Duas afirmações fortes, nenhuma sustentada. Nenhum dos exemplos do capítulo aparece em duas linguagens, então nem a parte "similar" é demonstrada. Resolveria: os quatro trechos equivalentes lado a lado, os planos físicos gerados por cada um, e tempos medidos sobre o mesmo dado no mesmo cluster. E, principalmente, a fronteira: em que casos a igualdade deixa de valer, que é o caso de UDFs e de código de RDD.

**3. "To date, Spark SQL is ANSI SQL:2003-compliant."** "To date" é uma data que o texto não escreve. Compliance com um padrão SQL é um espectro, não um booleano, e nenhum engine analítico implementa um padrão inteiro. A frase não diz qual nível de conformidade, nem que partes ficam de fora, nem quem verificou. Resolveria: a lista de features do padrão implementadas e não implementadas, ou uma suíte de conformidade de terceiros com resultado publicado. *No item 5 do Nível 5 verifiquei o que essa frase virou hoje, e a existência de um flag de conformidade a complica bastante.*

Outras três que considerei:

- "In most deployments modes, only a single executor runs per node" não diz quais modos, e o "most" faz o trabalho todo.
- "Spark has close to 1,500 contributors, well over 100 releases, 21,000 forks, and some 27,000 commits" não tem data de contagem.
- "over 600 Apache Spark Meetup groups globally with close to half a million members" não tem fonte nem define membro, e membro de grupo de meetup não é usuário.

---

## Nível 5 — Além do capítulo (backlog, não notas)

Escrito contra o Spark 3.0-preview2, em 2020. Estes são os pontos de decaimento temporal que sustentam peso. Verifiquei todos contra fonte primária em **2 de agosto de 2026**, quando a versão corrente da documentação era a **4.2.0**. As URLs estão ao fim da seção.

**1.** **Cluster managers.** O capítulo lista quatro, incluindo o Mesos. Confira o status de suporte atual de cada um, e anote qual virou o alvo comum desde a publicação.

R: São três hoje, não quatro. O Cluster Mode Overview da documentação 4.2.0 lista apenas **Standalone**, **Hadoop YARN** e **Kubernetes**. O Mesos não aparece.

O Mesos foi **depreciado no Spark 3.2.0**, via SPARK-35050, "Deprecate Apache Mesos as resource manager", com a justificativa de que o próprio Apache Mesos estava indo para o attic. Foi **removido no Spark 4.0.0**, via SPARK-44442, "Drop Mesos support", que consta dos destaques daquele release.

O alvo comum desde a publicação é o **Kubernetes**. As notas do Spark 3.1.1 registram que o suporte a Kubernetes atingiu general availability nessa versão, com scheduling em nível de stage, volumes NFS, criação dinâmica de PVC e gerenciamento de dependências Python. Isso aconteceu em 2021, ou seja, logo depois do livro sair.

Uma observação sobre o próprio capítulo: a Tabela 1-1 já lista cinco modos de deployment e **nenhum deles é Mesos**, enquanto a seção "Cluster manager", meia página antes, lista o Mesos entre os quatro suportados. O livro já era internamente inconsistente nesse ponto em 2020, e o tempo resolveu a inconsistência a favor da tabela.

**2.** **Continuous processing.** O capítulo cita um "experimental Continuous Streaming model" ao lado do Structured Streaming e nunca mais volta a ele. Descubra o que aquilo era de fato, se ainda é experimental, e como difere da execução micro-batch padrão.

R: O capítulo erra a versão. A documentação atual diz que continuous processing é "a new, experimental streaming execution mode introduced in **Spark 2.3**", e não no 2.0 como o capítulo sugere ao citá-lo junto com a introdução do Structured Streaming.

**Ainda é experimental em 2026.** A seção continua marcada com "[Experimental]" na documentação 4.2.0. São oito anos no mesmo rótulo.

A diferença em relação ao padrão é uma troca de latência por garantia. O modo padrão é micro-batch, que "processes data streams as a series of small batch jobs", com latência ponta a ponta de cerca de 100 ms e garantia **exactly-once**. Continuous processing entrega cerca de **1 ms** e garantia apenas **at-least-once**.

O preço é grande. Só operações do tipo map são suportadas, ou seja, projeções (`select`, `map`, `flatMap`, `mapPartitions`) e seleções (`where`, `filter`). Agregações não funcionam, e `current_timestamp()` e `current_date()` também não. As sources são Kafka e Rate, e as sinks são Kafka, Memory e Console. Além disso, é preciso ter cores suficientes no cluster para todas as tasks rodarem ao mesmo tempo, porque elas não se revezam.

O desfecho da história está no Spark 4.1.0, que introduziu o **Real-Time Mode (RTM)** para Structured Streaming, via SPARK-53736, descrito como o primeiro suporte oficial a processamento contínuo com latência sub-segundo, chegando a milissegundos de um dígito em tasks stateless. Ou seja, a promessa que o continuous processing carregou como experimento por oito anos foi entregue por outro caminho, em 2026.

**3.** **GraphX.** Verifique se o processamento de grafos ainda é apresentado como um dos quatro componentes centrais do Spark, qual é o status de manutenção do GraphX, e o que aconteceu com o GraphFrames. A nota de rodapé faz mais trabalho do que o capítulo admite.

R: **O enquadramento de "quatro componentes centrais" não existe mais na documentação.** A página inicial dos docs 4.2.0 lista dez guias de programação lado a lado: Quick Start, RDD Programming Guide, Spark SQL/Datasets/DataFrames, Structured Streaming, Spark Streaming, MLlib, GraphX, SparkR (Deprecated), PySpark, Declarative Pipelines. Não há hierarquia nem quarteto.

O **GraphX não está depreciado**. O guia dele não traz aviso de manutenção nem de legado, e os algoritmos são os mesmos três do capítulo, PageRank, Connected Components e Triangle Counting, mais strongly connected components. Por outro lado, ele não aparece em nenhum destaque de release que li, do 3.2.0 ao 4.1.0. O status é "vivo e parado".

O **GraphFrames não é mencionado em lugar nenhum da documentação do Spark**, nem no guia do GraphX. Ele continua sendo projeto externo, em `github.com/graphframes/graphframes`, e continua ativo: a série corrente é a 0.12.x, o namespace foi modernizado para `io.graphframes`, há suporte a Spark 4.x, e o projeto está migrando seus algoritmos para fora da cópia interna de GraphX, rumo a implementações nativas em DataFrame.

O saldo inverte a hierarquia do capítulo. O componente que está dentro do Spark não evolui. A biblioteca que ficou em nota de rodapé é a que tem cadência de release. Para um pipeline em DataFrame hoje, a nota de rodapé é a resposta e o corpo do texto é o histórico.

**4.** **`spark.mllib`.** O capítulo diz que ele está em modo de manutenção desde o 1.6. Verifique se ele foi depreciado ou removido desde então, e o que isso significa para pipelines legados.

R: O capítulo não erra, e eu quase o acusei injustamente. O MLlib Guide atual diz que a partir do **Spark 2.0** as APIs baseadas em RDD do pacote `spark.mllib` entraram em modo de manutenção. O capítulo data o **1.6** para outra coisa: a divisão do projeto em dois pacotes. Reler a frase dele resolve, porque o "now" que qualifica o modo de manutenção é o presente do autor, em 2020, e não está preso ao 1.6. As duas afirmações convivem. O que importa reter é o estado atual: modo de manutenção não é deprecação, e o FAQ do guia é explícito ao dizer que nenhuma das duas APIs está depreciada.

Não foi depreciado nem removido. O FAQ do próprio guia é explícito: "Is MLlib deprecated? No. MLlib includes both the RDD-based API and the DataFrame-based API. The RDD-based API is now in maintenance mode. But **neither API is deprecated, nor MLlib as a whole**."

As regras do modo de manutenção continuam as mesmas do capítulo. O `spark.mllib` recebe correções de bug e nenhuma feature nova. Todas as features novas vão para o `spark.ml`.

Para pipelines legados: eles continuam rodando, sem prazo anunciado de remoção. A pressão para migrar não vem de deprecação, vem de duas outras coisas. A primeira é que qualquer capacidade nova só existe do lado do DataFrame. A segunda é que só a camada de DataFrame passa pelo Catalyst e pelo Tungsten, então o custo de ficar cresce a cada release de performance. A ironia é que o capítulo acerta o comportamento e erra o número da versão, que é o padrão de erro dele em datas e versões.

**5.** **Conformidade com ANSI SQL.** "To date, Spark SQL is ANSI SQL:2003-compliant" é uma afirmação carimbada no tempo. Descubra a história atual de conformidade e, em particular, o que faz `spark.sql.ansi.enabled` e qual é o default dele. A existência desse flag complica bastante a frase do capítulo.

R: O flag existe e mudou de lado. **`spark.sql.ansi.enabled` tem default `true`** na documentação 4.2.0. A página abre com: "By default, `spark.sql.ansi.enabled` is `true` and Spark SQL uses an ANSI compliant dialect instead of being Hive compliant."

Com o flag ligado, o Spark faz duas coisas. Lança exceções em runtime em operações inválidas, como overflow de inteiro e erro de parsing de string. E usa regras de coerção de tipo baseadas em precedência de tipo de dado, em vez das regras permissivas anteriores. O exemplo da documentação: `SELECT 2147483647 + 1` lança `[ARITHMETIC_OVERFLOW]` com ANSI ligado e devolve `-2147483648` com ANSI desligado.

A linha do tempo: o modo ANSI foi declarado **GA no Spark 3.2.0** (SPARK-35030) e virou **default no Spark 4.0.0** (SPARK-44444, "Use ANSI SQL mode by default").

Isso complica a frase do capítulo de duas maneiras.

Primeiro, o flag prova que conformidade nunca foi uma propriedade do engine, e sim um modo de operação. Em 2020, quando o livro foi escrito, o default era o comportamento compatível com Hive, ou seja, **não** era o modo ANSI. A frase "Spark SQL is ANSI SQL:2003-compliant" descrevia o dialeto que o engine sabia falar, não o que ele falava por padrão.

Segundo, a documentação atual ressalva o próprio rótulo: "Some ANSI dialect features may be not from the ANSI SQL standard directly, but their behaviors align with ANSI SQL's style." Nem o projeto reivindica conformidade estrita hoje. Uma afirmação binária sobre um padrão de 2003 sempre foi forte demais para o que o produto entregava.

Consequência prática: código escrito para Spark 3.x e movido para o 4.x pode passar a lançar exceção onde antes devolvia `null` ou um número circulado. É a mudança de comportamento mais provável de quebrar código antigo em toda a série 4.

**6.** **Project Hydrogen e o gang scheduler.** Verifique o que foi entregue, o que travou, e como o scheduling de GPU é de fato configurado hoje.

R: **O que foi entregue** é a API de barrier execution mode. `pyspark.RDD.barrier` continua documentado na referência de API do PySpark 4.2.0, então o mecanismo que o gang scheduler do 2.4 introduziu existe e é acessível.

**O que travou é a narrativa.** Nem o RDD Programming Guide da 4.2.0 nem o da 3.5.8 trazem seção sobre barrier execution mode, e o nome "Project Hydrogen" não aparece em nenhuma nota de release que li, do 3.2.0 ao 4.1.0. O guarda-chuva sumiu, a peça técnica ficou.

**Como o scheduling de GPU é configurado hoje**, e este é o ponto: não existe configuração de GPU. Existe configuração genérica de recurso, e GPU é um caso dela. Todos os parâmetros abaixo são "since 3.0.0", ou seja, o capítulo aponta a versão certa e não conta nada do mecanismo.

- `spark.executor.resource.{resourceName}.amount`, default **0**. Quantidade por processo de executor. Exige o discovery script correspondente.
- `spark.task.resource.{resourceName}.amount`, default **1**. Quantidade por task. Aceita fração, no máximo 0.5, então o compartilhamento mínimo é de 2 tasks por recurso. Frações são arredondadas para baixo na contagem de slots.
- `spark.executor.resource.{resourceName}.discoveryScript`, default **None**. Script que o executor roda para descobrir o recurso. Precisa escrever no STDOUT um JSON no formato da classe `ResourceInformation`, com nome e array de endereços.
- `spark.executor.resource.{resourceName}.vendor`, default **None**, **só em Kubernetes**. Segue a convenção de nomes do device plugin, como `nvidia.com` ou `amd.com`.

Existem equivalentes para o driver: `spark.driver.resource.{resourceName}.amount` (default 0) e `.discoveryScript` (default None).

No YARN a tradução é automática para os tipos embutidos: `spark.executor.resource.gpu.amount=2` vira uma requisição de `yarn.io/gpu`, remapeável por `spark.yarn.resourceGpuDeviceName`. A documentação exige **YARN 3.0 ou superior** para usar GPU e FPGA, e registra em outra frase que o resource scheduling entrou no YARN 3.1.0. O YARN também suporta **stage level scheduling**: com dynamic allocation desligada, dá para variar os requisitos de recurso de task por stage. Com ela ligada, dá para variar requisitos de task e de executor e pedir executors extras.

Fecha a questão 12 do Nível 3. Antes de supor que o cluster se qualifica, é preciso conferir a versão do YARN, não só a do Spark, e providenciar um discovery script, que o capítulo nunca menciona.

**7.** **Datasets.** O capítulo oferece RDDs, DataFrames e Datasets como três escolhas intercambiáveis. Descubra quais linguagens de fato têm Datasets, e reescreva a frase do capítulo. Isso te afeta diretamente se você trabalha em Python.

R: A escolha tripla não existe em Python. O Dataset é a API tipada e existe apenas em **Scala e Java**.

A evidência no guia atual é estrutural, não uma frase isolada. A seção "Creating Datasets" traz exemplos apenas para Scala e Java, e nenhum para Python ou R. E a página declara que "in Spark 2.0, DataFrames are just Dataset of `Row`s **in Scala and Java API**", separando as "typed transformations" que vêm com "strongly typed Scala/Java Datasets" das "untyped transformations" do lado DataFrame. A qualificação por linguagem está no texto do padrão de equivalência, e nunca há um par Python/R correspondente.

A razão é o Encoder. Um Dataset precisa de um encoder para mapear objetos JVM tipados para o formato interno, e Python não tem tipos JVM a mapear.

Reescrita da frase do capítulo: "Data engineers podem escolher entre RDDs e DataFrames em qualquer linguagem suportada, e adicionalmente Datasets em Scala e Java. Em Python e R, o DataFrame é a única API estruturada disponível."

Efeito direto sobre mim: trabalho em Python, então o menu de três vira menu de dois, e um terço da seção de data engineering do capítulo não se aplica ao meu caso. Uma decisão que o livro apresenta como escolha de projeto já vem tomada pela linguagem.

**8.** **Releases pós-3.0.** O horizonte do capítulo termina no 3.0. Identifique o que chegou no 3.2, no 3.4 e no 4.0, especialmente o que muda os conselhos sobre particionamento e shuffle que este capítulo adia para os Capítulos 3 e 7.

R: Pelos destaques de cada release:

**3.2.0** — pandas API on Spark, absorvendo o Koalas (SPARK-34849). **Adaptive Query Execution ligado por padrão** (SPARK-33679). Push-based shuffle (SPARK-30602). RocksDB StateStore para Structured Streaming (SPARK-34198). Session windows por event time (SPARK-10816). ANSI SQL mode GA (SPARK-35030). Suporte a Scala 2.13 (SPARK-34218). Depreciação do Mesos (SPARK-35050).

**3.4.0** — cliente Python para Spark Connect (SPARK-39375). Storage Partitioned Join em DS v2 (SPARK-37375). **Bloom filter joins ligados por padrão** (SPARK-38841). Valores DEFAULT para colunas (SPARK-38334). Lateral column alias references (SPARK-27561). Async progress tracking em Structured Streaming (SPARK-39591).

**4.0.0** — modo ANSI SQL por padrão (SPARK-44444). Tipo VARIANT (SPARK-45827). Remoção do Mesos (SPARK-44442). Scala 2.13 como padrão e 2.12 removido (SPARK-45314). JDK 17 como padrão e JDK 8/11 removidos (SPARK-45315). Python Data Source API (SPARK-44076). Arbitrary State API v2 (SPARK-46815) e State Data Source reader (SPARK-45511) em Structured Streaming. SQL Pipe syntax (SPARK-49555). Collation de strings (SPARK-46830). **SparkR depreciado** (SPARK-49347).

Fora do escopo da pergunta, mas relevante: o **4.1.0** trouxe Declarative Pipelines (SPARK-51727), o Real-Time Mode de Structured Streaming (SPARK-53736), driver JDBC para Spark Connect (SPARK-53484), SQL Scripting GA (SPARK-54499) e VARIANT GA (SPARK-54454). A documentação corrente é a **4.2.0**, que pede Java 17/21/25, Scala 2.13, Python 3.10+ e R 4.0+ (depreciado).

**O que muda no conselho sobre particionamento e shuffle**, que é o ponto da pergunta:

O capítulo promete que os Capítulos 3 e 7 vão ensinar a ajustar o particionamento com base no número de cores. Esse conselho envelheceu. Com o **AQE ligado por padrão desde o 3.2**, o Spark recalcula o particionamento em runtime a partir de estatísticas reais. Os sub-parâmetros já vêm ligados: `spark.sql.adaptive.coalescePartitions.enabled` é `true` e `spark.sql.adaptive.skewJoin.enabled` é `true`. O default de `spark.sql.shuffle.partitions` continua **200**, mas deixou de ser o número final e virou o ponto de partida que o AQE reduz.

Duas outras mudanças mexem no mesmo terreno: push-based shuffle no 3.2, que altera como os blocos de shuffle são buscados, e Bloom filter joins ligados por padrão no 3.4, que reduz o que entra no shuffle de um join. O ajuste manual que o capítulo antecipa como habilidade central hoje é a exceção, e o valor 6 do exemplo do builder é ainda mais anacrônico do que já parecia.

**9.** **Conceitos de que os exemplos dependem e que o capítulo nunca define:** transformations contra actions, lazy evaluation, shuffle, job/stage/task, dependências narrow contra wide, a distinção DataFrame/Dataset. Anote-os como alvos para os Capítulos 2 e 3, e não como lacunas a preencher pela internet.

R: São seis conceitos e, pelo item 12 do Nível 4, quatro dos seis trechos de código do capítulo se comportam de forma diferente do esperado por falta deles. A lista não é acadêmica, é o que separa ler o código de entendê-lo.

Onde o próprio livro promete cada um:

- **transformations contra actions** e **lazy evaluation**: nomeados em "Ease of Use" e nunca retomados. O capítulo não promete data. Alvo mais provável: Capítulo 2, junto com o shell, e Capítulo 3, com as Structured APIs.
- **shuffle**: aparece só dentro de `spark.sql.shuffle.partitions`. O capítulo adia particionamento para os **Capítulos 3 e 7** explicitamente, então o Capítulo 7 é o alvo.
- **job/stage/task**: "task" é usada o tempo todo, "job" e "stage" não aparecem. Sem promessa no capítulo.
- **narrow contra wide**: não aparece de forma alguma. Sem promessa. É o conceito que explica por que existe fronteira de stage, então provavelmente vem junto do shuffle.
- **DataFrame contra Dataset**: os dois são nomeados em "Ease of Use" como construídos sobre RDD, e a distinção fica prometida para o **Capítulo 3** junto com as Structured APIs.

Registro dois pontos como alvo de leitura, e não como lacuna a fechar agora.

O primeiro é que a ordem importa. Sem transformations contra actions e sem lazy evaluation, nada mais da lista faz sentido, porque não existe momento de execução ao qual amarrar job, stage ou shuffle. Se o Capítulo 2 não entregar essas duas, releio antes de seguir.

O segundo é uma correção de rota que já cabe aqui: o capítulo apresenta DataFrame e Dataset como duas opções paralelas, e o item 7 deste nível mostra que em Python só existe uma. A distinção que o Capítulo 3 promete, portanto, é sobre uma escolha que a minha linguagem já resolveu.

### Fontes consultadas

Todas acessadas em 2 de agosto de 2026. Todas são fonte primária: documentação oficial, notas de release ou o repositório do projeto.

Documentação do Apache Spark, versão 4.2.0:

- [Overview, com a lista de guias de programação e as versões de Java, Scala, Python e R](https://spark.apache.org/docs/latest/index.html)
- [Cluster Mode Overview, com os cluster managers suportados e o glossário job/stage/task](https://spark.apache.org/docs/latest/cluster-overview.html)
- [Structured Streaming Programming Guide, com o modelo micro-batch padrão](https://spark.apache.org/docs/latest/streaming/index.html)
- [Structured Streaming Performance Tips, com a seção Continuous Processing](https://spark.apache.org/docs/latest/streaming/performance-tips.html)
- [RDD Programming Guide, com transformations, actions, laziness e shuffle](https://spark.apache.org/docs/latest/rdd-programming-guide.html)
- [MLlib Guide, com o modo de manutenção do `spark.mllib` e o FAQ de deprecação](https://spark.apache.org/docs/latest/ml-guide.html)
- [GraphX Programming Guide](https://spark.apache.org/docs/latest/graphx-programming-guide.html)
- [Spark SQL Getting Started, com a criação de Datasets só em Scala e Java](https://spark.apache.org/docs/latest/sql-getting-started.html)
- [ANSI Compliance, com o default de `spark.sql.ansi.enabled`](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html)
- [Performance Tuning, com os defaults de AQE e de `spark.sql.shuffle.partitions`](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
- [Configuration, com os defaults genéricos de recurso por executor, por task e por driver](https://spark.apache.org/docs/latest/configuration.html)
- [Running Spark on YARN, com GPU, `yarn.io/gpu` e stage level scheduling](https://spark.apache.org/docs/latest/running-on-yarn.html)
- [Referência de API do PySpark, `RDD.barrier`](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.barrier.html)

Notas de release e tickets:

- [Índice de releases](https://spark.apache.org/releases/)
- [Spark Release 3.1.1, com o GA do Kubernetes](https://spark.apache.org/releases/spark-release-3-1-1.html)
- [Spark Release 3.2.0](https://spark.apache.org/releases/spark-release-3-2-0.html)
- [Spark Release 3.4.0](https://spark.apache.org/releases/spark-release-3-4-0.html)
- [Spark Release 3.5.0](https://spark.apache.org/releases/spark-release-3-5-0.html)
- [Spark Release 4.0.0](https://spark.apache.org/releases/spark-release-4-0-0.html)
- [Spark Release 4.1.0, com Declarative Pipelines e Real-Time Mode](https://spark.apache.org/releases/spark-release-4.1.0.html)
- [SPARK-35050, depreciação do Mesos](https://issues.apache.org/jira/browse/SPARK-35050)
- [Notícias do projeto, com as datas de incubadora, top-level project e 1.0.0](https://spark.apache.org/news/index.html)

Projeto externo:

- [GraphFrames, releases no GitHub](https://github.com/graphframes/graphframes/releases)

Defaults de configuração mudam entre releases, como o AQE e o modo ANSI demonstram. Preciso reconferir estes dados antes de usá-los em prova ou em decisão técnica.

---

## Nível 6 — Cross-book (contra *Beginning Apache Spark 3*, Capítulo 1)

Já li dois capítulos introdutórios sobre o mesmo assunto, escritos com um ano de diferença. As discordâncias ensinam mais que qualquer um dos dois isolado.

**1.** **Streaming.** Luu apresenta o DStream como a principal abstração de streaming do Spark e o Structured Streaming como um engine mais novo. Damji et al. afirmam que o Structured Streaming tornou os DStreams obsoletos. Qual enquadramento está correto, e o que a diferença revela sobre como cada autor esperava que o livro fosse usado?

R: Damji está correto, e a distância entre os dois é maior do que parece.

O que verifiquei no item 1 do Nível 5 do guia do Luu: a documentação atual abre o guia de Spark Streaming com o aviso de que ele é "the previous generation of Spark's streaming engine", que não recebe mais atualizações e que é um projeto legado. Damji diz exatamente isso, em 2020: "This new model obviated the old DStreams model in Spark's 1.x series." Luu diz o contrário, em 2021, e ainda erra a versão de introdução do Structured Streaming, que ele data no 2.1 quando foi 2.0 experimental e 2.2 GA. Damji acerta as duas versões.

O que a diferença revela é a ordem de leitura que cada autor assume.

Luu constrói de baixo para cima. Ele começa no RDD, apresenta o DStream como abstração de streaming e o Structured Streaming como novidade sobre ela. É a ordem histórica, e ela pressupõe um leitor que vai entender o Spark acompanhando como ele foi construído. Faz sentido para quem vem de MapReduce e vai encontrar código de 2016 em produção.

Damji constrói de cima para baixo. Ele nunca ensina o DStream, cita o modelo antigo só para dizer que foi superado, e adia o assunto para o Capítulo 8, onde ele entra como contexto e não como ferramenta. É a ordem do estado da arte, e pressupõe um leitor que vai escrever código novo.

O detalhe que fecha o argumento: Damji são engenheiros da Databricks, ou seja, gente com acesso à direção do projeto. Luu escreveu depois e ficou atrás. Isso é um aviso sobre o meu próprio critério de leitura, porque data de publicação não é proxy de atualidade.

**2.** **Alegações de velocidade.** Luu cita os 100x do site e o resultado do GraySort de 2014. Damji et al. citam 10 a 20x dos primeiros papers e, depois, "many orders of magnitude". Reconcilie os números. O que cada autor teria que especificar para as alegações serem comparáveis?

R: Os números não estão em conflito porque não medem a mesma coisa. Reconciliados:

**10 a 20x** é o resultado de pesquisa, medido em jobs específicos, iterativos ou interativos, contra um MapReduce de 2010 a 2012. É o único dos quatro com escopo declarado e mecanismo conhecido: ele mede o que se ganha ao não escrever intermediários em disco.

**100x** é a alegação do site do projeto, com dois qualificadores que a tornam irrefutável, "até" e "certos jobs". É o máximo de uma distribuição cujo resto não é publicado. Não contradiz os 10 a 20x, porque um máximo sempre pode ser maior que um caso típico.

**Daytona GraySort 2014**, 100 TB ordenados três vezes mais rápido com dez vezes menos recursos, é o único dado auditado externamente dos quatro. Mede uma operação só, ordenação, dominada por I/O e shuffle. Não se transfere para pipeline misto.

**"Many orders of magnitude"** é a pior. Sem número, sem fonte, sem data, sem baseline. E é a mais alta de todas: "many orders of magnitude" implica pelo menos mil vezes, dez vezes mais que a alegação de marketing que Luu cita, sem nenhum apoio. Damji tinha o dado bom no parágrafo e o enfraqueceu com uma frase que não podia sustentar.

Para as alegações serem comparáveis, os dois autores teriam que declarar o mesmo conjunto: tipo de job, volume de dados, configuração de hardware, versão dos dois sistemas comparados, esforço de tuning equivalente dos dois lados, e a distribuição dos ganhos em vez do máximo. Nenhum dos dois declara nada disso.

Uma diferença de postura entre os dois. Luu atribui as alegações: diz que os 100x vêm do site e que o GraySort foi submissão da Databricks. Damji não atribui os 10 a 20x a nenhum paper nomeado e não atribui coisa alguma a "many orders of magnitude". Luu cita evidência mais fraca com melhor procedência.

**3.** **Filosofia de projeto.** Luu nomeia três propriedades (ease of use, speed, flexibility). Damji et al. nomeiam quatro (speed, ease of use, modularity, extensibility). Mapeie uma lista na outra. O que a versão de quatro partes captura que a de três colapsa?

R: Duas propriedades batem diretamente e uma se parte em duas.

| Luu | Damji et al. |
|---|---|
| speed | speed |
| ease of use | ease of use |
| flexibility | modularity **+** extensibility |

Speed e ease of use são as mesmas em substância, embora sustentadas de forma diferente. Luu apoia speed em benchmarks externos, o site e o GraySort. Damji apoia em mecanismo, DAG e Tungsten, e nomeia as peças. Em ease of use, Luu conta operadores, mais de 80 em quatro linguagens. Damji conta conceitos, uma abstração e duas categorias de operação. Contar operadores é argumento de superfície de API. Contar conceitos é argumento de carga cognitiva.

A divergência real está na terceira. A flexibility do Luu é sobre **tipos de workload**: batch, queries interativas, machine learning iterativo e streaming em tempo real. Isso é exatamente a modularity do Damji, que acrescenta as linguagens ao mesmo eixo.

O que a versão de quatro partes captura e a de três não tem é a **extensibility**, e não é uma nuance. É um argumento inteiro que não aparece no Luu: o Spark não traz storage, desacopla compute de armazenamento, lê de muitos sistemas e permite estender readers e writers para alcançar outros. Luu nunca faz essa afirmação. Ele cita o HDFS como destino de escrita entre aplicações e trata storage como parte do cenário, não como decisão arquitetural.

O eixo omitido é o que sustenta a arquitetura de lakehouse, que é justamente o assunto das seções de ecossistema que o Luu tem e o Damji não. Cada livro tem metade da história de storage. Luu tem os formatos e não tem o princípio. Damji tem o princípio e não tem os formatos.

**4.** **Cluster managers e deployment.** Luu nomeia YARN, Mesos e standalone. Damji et al. acrescentam Kubernetes e fornecem uma tabela de cinco modos de deployment. Qual livro é mais útil operacionalmente para você, e o que a omissão do Luu sugere sobre o leitor que ele imaginava?

R: Damji, sem disputa, e por três motivos.

O primeiro é o Kubernetes. O item 1 do Nível 5 confirma que ele atingiu GA no Spark 3.1.1, em 2021, e que hoje é um dos três cluster managers que a documentação lista, enquanto o Mesos foi removido no 4.0.0. O livro de 2020 nomeia o alvo que sobreviveu. O de 2021 nomeia dois, e um deles morreu.

O segundo é a Tabela 1-1. Ela responde a três perguntas por modo: onde roda o driver, onde rodam os executors, e quem faz o papel de cluster manager. Isso é o que separa "meu job roda no laptop" de "meu job roda em produção", e é a base da minha resposta na questão 3 do Nível 3, sobre bugs que passam local e falham no cluster. Luu não tem tabela equivalente e não distingue client mode de cluster mode em lugar nenhum.

O terceiro é o modelo de recursos. Luu tem uma coisa que Damji não tem: os três parâmetros de launch, número de executors, memória por executor e cores por executor. Damji descreve a arquitetura e nunca diz como se pede capacidade. Então a vantagem operacional não é total, é assimétrica. Damji cobre topologia e Luu cobre dimensionamento.

Sobre o leitor que Luu imaginava: alguém dentro de uma organização que já tem cluster Hadoop. Ele diz explicitamente que a maioria das empresas com tecnologias de big data mantém um cluster YARN para MapReduce, Pig ou Hive, e que o Spark interopera com eles. Nesse mundo, quem provisiona a infraestrutura é outra equipe, e o leitor só submete jobs. Por isso ele não precisa de client contra cluster mode, e por isso Kubernetes não entra: não é o que o time de infra dele opera. Damji escreve para quem faz o deployment, ou pelo menos para quem escolhe o ambiente.

**5.** **Ecossistema contra stack.** Luu trata Delta Lake, Koalas e MLflow como ecossistema. Damji et al. mal mencionam o Delta Lake e o mostram apenas num diagrama de conectores. Qual enquadramento combina melhor com um currículo de lakehouse, e o que cada livro deixa para o outro?

R: O Delta Lake aparece no capítulo do Damji em um lugar só, e é a Figura 1-2, legendada "Apache Spark's ecosystem of connectors". O diagrama põe o Spark num círculo central e distribui em volta os logos dos sistemas com que ele conversa: Kafka, Elastic, MySQL, PostgreSQL, HDFS, HBase, Cassandra, MongoDB, Redis, um "And more..." e o Delta Lake. Na prosa, o nome não aparece nenhuma vez.

A posição no diagrama é o achado. O Delta Lake está ali como **mais um conector de dados**, no mesmo nível de um banco relacional ou de uma fila de mensagens, e não como camada de storage com semântica própria. Não há seção, parágrafo ou frase sobre storage transacional, formato de tabela ou consistência de data lake em nenhum ponto do capítulo.

O contraste com o Luu é grande. Ele dá ao Delta Lake uma seção inteira na parte de ecossistema, nomeia o problema que ele resolve (data consistency semantics) e lista as três capacidades. Cada livro fica com metade da história: Luu tem o formato sem o princípio de desacoplamento entre storage e compute, e Damji tem o princípio sem o formato, tratando como conector aquilo que o outro trata como fundação.

Para um currículo de lakehouse, o enquadramento do Luu é incomparavelmente melhor, e não por pouco. Ele dedica seções ao Delta Lake, ao Koalas e ao MLflow, e a do Delta Lake é a única, nos dois livros, que constrói o problema: um data lake guarda dado estruturado e não estruturado para consumidores diversos, e mantê-lo utilizável exige catálogo, descoberta, qualidade, controle de acesso e consistência, sendo a consistência a mais difícil. Sem esse parágrafo, não existe motivação para lakehouse. Damji não o tem.

O que cada livro deixa para o outro:

- **Damji deixa para o Luu** a camada de storage inteira, o ciclo de vida de modelos e a ponte com pandas. Também deixa o modelo de recursos, os três parâmetros de launch e a decisão de isolamento de executors.
- **Luu deixa para o Damji** a arquitetura de execução com nível de detalhe utilizável: modos de deployment, papéis de driver e executor por ambiente, `SparkSession` e sua relação com os contextos antigos, partições e data locality. Deixa também a genealogia Google-Yahoo!-Berkeley, que Luu resume em duas frases e Damji desenvolve em três seções, e é ela que explica *por que* o Spark tem a forma que tem.

Uma observação que muda a leitura das duas omissões: os autores do Damji são da Databricks, a mesma empresa que criou o Delta Lake, o Koalas e o MLflow. A ausência dos três no capítulo 1 dele não é desinteresse. É escolha de escopo, e a comparação justa não é entre um livro que cobre o ecossistema e outro que ignora, mas entre um que o coloca no capítulo 1 e outro que o adia. Sobre o Koalas, o item 3 do Nível 5 do guia do Luu já registrou que ele foi absorvido pelo Spark no 3.2.0 e virou `pyspark.pandas`, então essa seção do Luu envelheceu no mesmo ano em que foi publicada.

**6.** **Word count.** Os dois capítulos usam word count, Luu em Scala estilo RDD e Damji et al. em Python com Structured Streaming. Escreva as duas versões lado a lado e anote o que cada uma revela sobre a era e a camada de API à qual pertence.

R:

**Luu, Listing 1-1, Scala, API de RDD:**

```scala
sc.textFile("hdfs://<folder>")
  .flatMap(line => line.split(" "))
  .map(word => (word, 1))
  .reduceByKey(_ + _)
  .saveAsTextFile("hdfs://<output folder>")
```

**Damji et al., Python, Structured Streaming:**

```python
lines = (spark
  .readStream
  .format("socket")
  .option("host", "localhost")
  .option("port", 9999)
  .load())

words = lines.select(explode(split(lines.value, " ")).alias("word"))

word_counts = words.groupBy("word").count()

query = (word_counts
  .writeStream
  .format("kafka")
  .option("topic", "output"))
```

Anotações, ponto a ponto:

**Ponto de entrada.** `sc` contra `spark`. O primeiro é o `SparkContext`, o segundo é a `SparkSession`. Essa única diferença de duas letras separa a era 1.x da era 2.x, e é exatamente o deslize que apontei no item 3 do Nível 4, onde Damji chama as duas de nomes da mesma variável.

**Camada de API.** Luu está abaixo do otimizador. As lambdas `line => line.split(" ")` e `_ + _` são código Scala arbitrário, opaco para o Catalyst. Damji está acima: `split`, `explode` e `groupBy` são expressões que o otimizador enxerga e pode reordenar. Os dois calculam a mesma coisa, e só um dos dois pode ser otimizado.

**Fronteira de dados.** Luu é batch e fechado: HDFS entra, HDFS sai, o job termina. Damji é streaming e aberto: socket entra, Kafka sai, a query não termina por definição. Um produz um arquivo, o outro produz um fluxo.

**Modelo mental.** Luu pensa em coleção. Cada linha transforma uma coleção em outra, e a forma dos dados muda visivelmente de linha em linha. Damji pensa em tabela. O `explode` produz linhas, o `groupBy` agrega linhas, e a palavra "coleção" nunca aparece.

**O que cada um não mostra.** O de Luu não mostra que nada roda até a última linha, porque ele nunca explica lazy evaluation. O de Damji não roda: falta o `.start()`, e falta o `checkpointLocation` que a sink Kafka exige.

**A era em uma frase.** O de Luu é o idioma de 2014, o Spark se apresentando como MapReduce que não escreve em disco, e a escolha de word count é deliberada, porque era o "hello world" do MapReduce. O de Damji é o idioma de 2020, o Spark se apresentando como banco de dados de streaming, onde o mesmo word count vira uma agregação contínua sobre uma tabela que cresce. Manter o mesmo problema e trocar toda a formulação é a demonstração mais econômica de quanto o produto mudou em seis anos.

**7.** Monte uma linha do tempo única da história do Spark a partir dos dois capítulos. Onde eles discordam em data ou atribuição, e qual é verificável?

R: Linha do tempo combinada. **[L]** é o que só o Luu traz, **[D]** o que só o Damji traz, **[L+D]** o que os dois trazem.

| Data | Evento | Fonte |
|---|---|---|
| 2003 a 2006 | Papers do GFS, MapReduce e Bigtable no Google | *nenhum dos dois data* |
| abr/2006 | Hadoop doado à ASF; HDFS modelado no paper do GFS | [D] |
| 2009 | O projeto Spark começa em Berkeley | [L+D] |
| 2010 | Spark aberto como open source | [L] |
| 2010 | Paper "Spark: Cluster Computing with Working Sets" | [L] |
| 2012 | Paper "Resilient Distributed Datasets" | [L] |
| jun/2013 | Spark aceito na Apache Incubator | *nenhum dos dois* |
| 2013 | Databricks fundada; capta mais de US$ 43 milhões | [L+D] |
| fev/2014 | Spark vira top-level project da Apache | *ver discordância 1* |
| mai/2014 | Spark 1.0 lançado | [D] |
| 2014 | Spark vence o Daytona GraySort, 100 TB | [L] |
| nov/2016 | ACM Award pelo paper "Unified Engine for Big Data Processing" | [D] |
| 2016 | Spark 2.0: `SparkSession`, Structured Streaming experimental | *nenhum dos dois data* |
| 2017 | Spark 2.2: Structured Streaming GA | *nenhum dos dois data* |
| 2018 | Spark 2.4: gang scheduler, Project Hydrogen | *nenhum dos dois data* |
| jun/2020 | Spark 3.0 | [L]; Damji só diz 2020 |

**Discordância 1, a data de top-level project.** Luu diz que o Spark virou top-level project da Apache em **2013**. Damji diz que em 2013 os criadores **doaram** o projeto à ASF e fundaram a Databricks, sem afirmar promoção. **Damji está certo e Luu está errado**, e é verificável na própria página de notícias do projeto: o Spark foi aceito na incubadora em **21 de junho de 2013** e graduou para top-level project em **27 de fevereiro de 2014**. Luu comprimiu doação e graduação num ano só. O verbo do Damji, "donated", é preciso.

**Discordância 2, o nome do laboratório.** Luu diz que o projeto começou no AMPLab, em Berkeley. Damji diz que começou no **RAD Lab, que depois virou AMPLab e hoje é RISELab**. Não é contradição, é precisão diferente: Luu usa o nome pelo qual o laboratório ficou conhecido, Damji dá a sucessão. Damji é mais verificável porque é falseável.

**Discordância 3, o ganho de velocidade dos primeiros anos.** Tratada no item 2 deste nível.

**Discordância 4, a versão do Structured Streaming.** Luu diz 2.1. Damji diz 2.0 experimental e 2.2 GA. Verificado no item 2 do Nível 5 do guia do Luu, contra as notas de release: Damji está certo nas duas pontas.

**Discordância 5, a MLlib, que não é discordância.** Achei que fosse e não é. Damji data do **1.6** a divisão do projeto em dois pacotes. Luu diz que as APIs da MLlib são baseadas em DataFrame **desde o 2.0**. São afirmações sobre fatos diferentes: uma sobre quando os pacotes se separaram, outra sobre quando a API baseada em DataFrame virou a base. Não há disputa a arbitrar, e nenhum dos dois erra. O guia atual do MLlib data o modo de manutenção do 2.0, o que também não conflita com nenhum deles.

**Padrão que fica.** Das cinco divergências, uma não era divergência de verdade. Nas quatro que restam, Damji acerta três, e é o livro mais antigo. Nenhum dos dois cita a data de entrada na incubadora, que é o que explica o intervalo de oito meses que o Luu comprimiu. E as três linhas que os dois livros trazem juntos, 2009, a fundação da Databricks em 2013 e o Spark 3.0 em 2020, são as únicas em que não há o que reconciliar. Onde só um dos livros fala, não há como detectar erro pela leitura cruzada. Foi por isso que fui à página de notícias do projeto.

---

## Termos-chave para definir antes de seguir adiante

Escreva uma definição de uma linha para cada, com suas próprias palavras, sem consultar:

GFS · Bigtable · MapReduce · data locality · rack affinity · HDFS · YARN · unified engine · DAG · DAG scheduler · Tungsten · whole-stage code generation · Catalyst · RDD · DataFrame · Dataset · Structured API · transformation · action · lazy evaluation · shuffle · partition · executor · core · task · driver · SparkSession · SparkContext · cluster manager · deployment mode · client mode · cluster mode · continually growing table · late-data semantics · estimator · transformer · featurizer · gang scheduler

Os termos desta lista que o capítulo usa sem definir são alvos de releitura para os Capítulos 2 e 3, e não itens de Nível 5.

### Minhas definições

Vinte e três dos trinta e sete termos o capítulo usa sem definir, ou nem menciona. Marquei esses casos em itálico. O padrão é nítido: o capítulo define bem os termos de arquitetura de execução e deixa quase toda a terminologia de API sem definição.

**GFS** — Google File System. Filesystem distribuído e fault-tolerant sobre muitos servidores de commodity hardware num cluster, criado porque nem RDBMS nem programação imperativa davam conta da escala do Google.

**Bigtable** — Armazenamento escalável de dados estruturados construído sobre o GFS.

**MapReduce** — Paradigma de programação paralela, baseado em programação funcional, para processamento em larga escala de dados distribuídos sobre GFS e Bigtable. Envia as funções map e reduce até o dado em vez de trazer o dado até a aplicação.

**data locality** — *Nomeada, nunca definida.* A preferência por executar a computação na máquina que já guarda o dado, para que a leitura seja local ao disco em vez de atravessar a rede.

**rack affinity** — *Nomeada uma vez, nunca explicada.* O segundo melhor caso da locality: quando a computação não pode rodar na máquina que guarda o dado, roda em outra do mesmo rack, e a transferência fica no switch do rack. Esta definição é minha inferência.

**HDFS** — Hadoop File System. Implementação open source do modelo do GFS, cujo paper serviu de blueprint. É um dos quatro módulos do Apache Hadoop.

**YARN** — *Usado sem definição.* Aparece como módulo do Hadoop e como cluster manager, e a Tabela 1-1 nomeia suas peças, Resource Manager, NodeManager e Application Master, sem dizer o que ele é. É o gerenciador de recursos do Hadoop, que aloca containers em nós para quem pede.

**unified engine** — Engine único que cobre workloads diversos, batch, SQL interativo, streaming, machine learning e grafos, sob uma mesma arquitetura distribuída, dispensando um sistema especializado por workload.

**DAG** — *Sigla expandida, conceito não explicado.* Directed acyclic graph. O capítulo diz que o Spark constrói suas computações de query como um DAG e nunca diz o que são os nós nem as arestas. Nós são operações, arestas são dependências de dados, e a ausência de ciclo é o que permite ordenar a execução.

**DAG scheduler** — *Só o nome.* Citado como um dos dois consumidores do DAG, junto com o query optimizer, sem descrição do que faz.

**Tungsten** — O engine de execução física do Spark. Usa whole-stage code generation para gerar código compacto. O capítulo também o credita, ao lado do Catalyst, pelas melhorias de performance do 2.x e do 3.0.

**whole-stage code generation** — *Nomeada, mecanismo adiado para o Capítulo 3.* A técnica que o Tungsten usa para gerar código compacto de execução. O capítulo não diz o que é um stage nem por que compactar ajuda.

**Catalyst** — O otimizador de SQL do Spark. O capítulo o nomeia uma única vez, na seção de data engineering, e nunca na de Spark SQL, onde ele faria mais sentido.

**RDD** — Resilient Distributed Dataset. Estrutura de dados lógica simples, apresentada como a abstração fundamental sobre a qual todas as abstrações estruturadas de nível mais alto são construídas. A definição do capítulo termina aí: nada sobre particionamento, tolerância a falhas ou linhagem.

**DataFrame** — *Usado o capítulo inteiro e nunca definido.* Aparece em "read into memory as a Spark DataFrame", em "as a DataFrame in memory" e em "DataFrame-based APIs", sempre pressuposto. É uma coleção distribuída de dados organizada em colunas nomeadas, equivalente a uma tabela relacional. Essa definição vem de fora do capítulo.

**Dataset** — *Só o nome.* Citado como construído sobre o RDD e oferecido como uma das três APIs, sem nunca ser definido nem distinguido do DataFrame. É a versão tipada, e *no item 7 do Nível 5 verifiquei que só existe em Scala e Java.*

**Structured API** — *Usado sem definição.* Nomeia o conjunto de APIs de alto nível cuja explicação é adiada para o Capítulo 3. É a camada que o Catalyst enxerga e otimiza, em oposição ao código arbitrário passado a um RDD.

**transformation** — *Nomeada como metade do modelo de programação e nunca definida.* Operação que produz um novo dataset a partir de um existente, sem executar nada na hora.

**action** — *Nomeada e nunca definida.* Operação que dispara a execução e devolve um valor ao driver ou escreve para fora.

**lazy evaluation** — *Não aparece no capítulo.* A propriedade de que transformations não computam nada quando são escritas, apenas registram o que fazer, e só rodam quando uma action exige um resultado. É a ausência mais cara do capítulo, porque quatro dos seis trechos de código dele se comportam de forma diferente do esperado sem ela.

**shuffle** — *Aparece só dentro da chave `spark.sql.shuffle.partitions`.* A redistribuição de dados entre máquinas quando os valores de uma mesma chave estão em partições diferentes e precisam ser reunidos. É o custo dominante dos jobs reais e o conceito que falta para a chave de configuração fazer sentido.

**partition** — A fatia do dado que reside em uma máquina. O capítulo a descreve fisicamente, como o dado distribuído no storage, e depois a equipara a um DataFrame, o que está errado e é o alvo do item 2 do Nível 4.

**executor** — Processo que roda em cada worker node, comunica-se com o driver e executa tasks. Na maioria dos modos de deployment, roda apenas um por nó.

**core** — *Usado sem definição.* A unidade de paralelismo dentro do executor. Cada core recebe a sua própria partição de dados para trabalhar.

**task** — *Usada o capítulo inteiro e nunca definida.* A unidade de trabalho em que o driver decompõe o DAG e que distribui aos executors.

**driver** — A parte da aplicação que instancia a `SparkSession`, fala com o cluster manager, requisita CPU e memória para os executors, transforma as operações em computações DAG, escalona e distribui as tasks. Depois da alocação, fala direto com os executors.

**SparkSession** — Canal unificado para todas as operações e todos os dados do Spark, introduzido no 2.0. Absorveu `SparkContext`, `SQLContext`, `HiveContext`, `SparkConf` e `StreamingContext`. Cria parâmetros de runtime, define DataFrames e Datasets, lê fontes, acessa metadados de catálogo e emite queries SQL.

**SparkContext** — *Nomeado sem definição.* Aparece na lista dos pontos de entrada absorvidos e como a variável `sc` do shell, tratada erradamente como sinônimo de `spark`. É a conexão de baixo nível com o cluster e hospeda as APIs de RDD.

**cluster manager** — Serviço responsável por gerenciar e alocar os recursos do conjunto de nós em que a aplicação roda. O capítulo lista quatro, e *no item 1 do Nível 5 verifiquei que hoje são três.*

**deployment mode** — A combinação de onde o driver roda, onde os executors rodam e o que atua como cluster manager. O capítulo não define o termo, mas a Tabela 1-1 o define por extensão, com cinco casos.

**client mode** — *Aparece só como o rótulo "YARN (client)".* O modo em que o driver roda numa máquina cliente fora do cluster, enquanto os executors rodam dentro dele.

**cluster mode** — *Aparece só como o rótulo "YARN (cluster)".* O modo em que o driver também roda dentro do cluster, junto com o YARN Application Master.

**continually growing table** — O modelo mental do Structured Streaming. O stream é uma tabela à qual novas linhas são anexadas ao fim, consultável como uma tabela estática.

**late-data semantics** — *Nomeada e nunca explicada.* Uma das duas preocupações que o core engine do Spark SQL assume no lugar do desenvolvedor. É o conjunto de regras sobre o que fazer quando um registro chega depois que o resultado do seu intervalo de tempo já foi computado.

**estimator** — *Nomeado sem definição.* Citado ao lado de transformers e featurizers como peça de alto nível da MLlib. É o componente que aprende com dados e produz um modelo, como o `LogisticRegression` do trecho de código, que vira modelo pelo `.fit()`.

**transformer** — *Nomeado sem definição.* O componente que converte um dataset em outro sem aprender nada, como o `lrModel.transform(test)` do trecho.

**featurizer** — *Nomeado sem definição.* O componente que extrai ou transforma features a partir do dado bruto, preparando a entrada do modelo.

**gang scheduler** — *Nomeado sem definição.* Introduzido no Spark 2.4 como parte do Project Hydrogen. É o escalonamento em que todas as tasks de um grupo partem juntas ou não partem, exigência de treino distribuído de deep learning, onde as tasks se comunicam entre si e uma sozinha não avança. *No item 6 do Nível 5 verifiquei o que sobrou dele.*
