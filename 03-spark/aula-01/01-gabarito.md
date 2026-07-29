---
title: "Aula 01 de Spark - Gabarito da leitura"
aula: "Aula 01 - Arquitetura unificada do Spark, processamento em larga escala e preparação do ambiente"
data: 2026-07-24
tags:
  - spark
  - pos-infnet
  - aula-01
  - gabarito
  - bibliografia
  - registro-de-leitura
---

# Aula 01 · Gabarito da leitura
> **Este documento é o gabarito, e não se abre antes de tentar.**
>
> Ele foi escrito antes do redesenho do método, quando eu lia os capítulos e entregava as conclusões prontas. Isso inverteu o trabalho: as 55 dúvidas foram achadas por mim e as sete divergências foram notadas por mim, e ler conclusão pronta não ensina a achar. O guia de leitura em [01-pre-aula.md](01-pre-aula.md) faz as perguntas cujas respostas estão aqui e nos capítulos. Responda lá primeiro, com os PDFs, e use este arquivo para conferir depois.


Primeira etapa: a leitura que o professor passou, lida antes de qualquer aprofundamento. A ordem importa. Os capítulos descrevem o Spark 3.0 e o [aprofundamento](02-aprofundamento.md) existe para marcar o que mudou até o 4.2.0; ler o aprofundamento primeiro apaga a fronteira entre o que é tese do livro e o que é correção de 2026, e é justamente essa fronteira que rende pergunta boa na aula.

## Sumário

| Seção | Do que trata |
|---|---|
| [O que foi passado](#o-que-foi-passado) | a bibliografia, o [mapa dos quatro capítulos](#mapa-dos-quatro-capítulos) e [o que cada um adia](#para-onde-os-capítulos-apontam) para depois |
| [Como ler](#como-ler) | o método desta etapa: o que anotar e o que deixar passar |
| [Teses que valem marcação](#teses-dos-capítulos-que-valem-marcação) | os oito palpites escritos antes da leitura, cada um com o veredito depois dela |
| [Registro de leitura](#registro-de-leitura) | a anotação capítulo por capítulo, o corpo do documento |
| [Onde eu não acreditei](#onde-eu-não-acreditei) | as 55 dúvidas numeradas, agrupadas por capítulo |
| [Divergências entre os dois livros](#divergências-entre-os-dois-livros) | as sete discordâncias entre Luu e Damji que mudam a conclusão, não só a redação |
| [Vocabulário novo](#vocabulário-novo) | os 29 termos que os capítulos usaram sem definir, e o que falta em cada um |
| [O que fica para o aprofundamento](#o-que-fica-para-o-aprofundamento) | as perguntas que a leitura abriu e não fechou |

Para consulta pontual, o registro de leitura abre por capítulo:

- **[Luu, capítulo 1](#luu-capítulo-1)** · [as três propriedades](#a-tese-das-três-propriedades) · [história](#história) · [arquitetura](#arquitetura-as-peças) · [stack unificada](#a-stack-unificada) · [novidades do 3.0](#as-novidades-do-spark-30) · [aplicações e word count](#aplicações-e-o-exemplo-canônico) · [ecossistema](#ecossistema) · [o resumo do autor](#o-que-o-luu-resume-no-fim-do-capítulo-1) · [o que ficou marcado](#o-que-ficou-marcado-em-luu-1)
- **[Luu, capítulo 2](#luu-capítulo-2)** · [download e instalação](#download-e-instalação) · [os shells](#os-shells) · [Scala mínimo](#scala-mínimo) · [Spark UI](#spark-ui) · [`SparkSession` no shell](#sparksession-no-shell) · [Databricks](#databricks-collaborative-notebooks) · [código-fonte](#código-fonte-do-spark) · [o resumo do autor](#o-que-o-luu-resume-no-fim-do-capítulo-2) · [o que ficou marcado](#o-que-ficou-marcado-em-luu-2)
- **[Damji, capítulo 1](#damji-capítulo-1)** · [Google](#gênese-parte-1-google) · [Hadoop e suas quatro falhas](#gênese-parte-2-hadoop-e-suas-quatro-falhas) · [AMPLab](#gênese-parte-3-amplab) · [as quatro características](#o-que-é-o-spark-quatro-características) · [analytics unificado](#analytics-unificado) · [execução distribuída](#execução-distribuída) · [quem usa Spark](#a-experiência-do-desenvolvedor) · [comunidade](#comunidade) · [o resumo que não existe](#o-que-o-damji-resume-no-fim-do-capítulo-1) · [o que ficou marcado](#o-que-ficou-marcado-em-damji-1)
- **[Damji, capítulo 2](#damji-capítulo-2)** · [passo 1, baixar](#passo-1-baixar) · [passo 2, o shell](#passo-2-o-shell) · [passo 3, os cinco termos](#passo-3-os-cinco-termos) · [transformações, ações e preguiça](#transformações-ações-e-avaliação-preguiçosa) · [narrow e wide](#narrow-e-wide) · [a Spark UI](#a-spark-ui) · [a primeira aplicação](#a-primeira-aplicação-standalone) · [o resumo do autor](#o-que-o-damji-resume-no-fim-do-capítulo-2) · [o que ficou marcado](#o-que-ficou-marcado-em-damji-2)

E a tabela de dúvidas abre nos mesmos quatro blocos: [Luu 1](#dúvidas-luu-capítulo-1) (itens 1 a 17), [Luu 2](#dúvidas-luu-capítulo-2) (18 a 26), [Damji 1](#dúvidas-damji-capítulo-1) (27 a 40), [Damji 2](#dúvidas-damji-capítulo-2) (41 a 55).

## O que foi passado

| Obra | Capítulos | Versão que o texto cobre |
|---|---|---|
| Luu, Hien. *Beginning Apache Spark 3*. Apress, 2021 | 1 e 2 | Spark 3.0 / 3.1 |
| Damji, Jules et al. *Learning Spark*, 2ª edição. O'Reilly, 2020 | 1 e o início do 2 | Spark 3.0 |

Nada de material licenciado é reproduzido aqui: este documento registra a leitura, não o texto. Nomes de comando, de método, de arquivo, de campo de formulário e valores de saída entram como fato, porque é isso que se consulta depois.

Os PDFs dos quatro capítulos estão nesta pasta como `livro1-cap1.pdf`, `livro1-cap2.pdf` (Luu), `livro2-cap1.pdf` e `livro2-cap2.pdf` (Damji).

### Mapa dos quatro capítulos

| Capítulo | Páginas | Do que trata | Para que serve |
|---|---|---|---|
| Luu 1 | 18 | visão geral, história, driver e executores, stack unificada, novidades do 3.0, ecossistema | vocabulário e panorama |
| Luu 2 | 28 | download, shells, Spark UI, `SparkSession`, Databricks CE, código-fonte | montar ambiente |
| Damji 1 | 22 | gênese (Google, Yahoo, AMPLab), quatro características de projeto, componentes, arquitetura distribuída, partições | o porquê do Spark |
| Damji 2 | 26 | os três passos, jobs/stages/tasks, transformações e ações, narrow e wide, Spark UI, primeira aplicação | modelo de execução |

Os pares se sobrepõem de propósito: Luu 1 e Damji 1 contam a mesma história com ênfases diferentes, Luu 2 e Damji 2 montam o mesmo ambiente com escolhas diferentes. A sobreposição é o que torna as divergências visíveis, e elas são a parte mais útil da leitura.

### Para onde os capítulos apontam

Nenhum dos quatro fecha o que abre. O que cada um empurra para frente, e para onde:

| Assunto | Adiado por | Para |
|---|---|---|
| RDD em detalhe | Luu 1 | capítulo posterior, não numerado |
| Delta Lake | Luu 1 | capítulo posterior, não numerado |
| Build do binário a partir do fonte | Luu 2 (prometido para o próprio capítulo) | nunca entregue [22] |
| APIs estruturadas, plano de query, whole-stage codegen | Damji 1 e 2 | capítulo 3 |
| Tuning de particionamento | Damji 1 | capítulos 3 e 7 |
| Spark UI em detalhe | Damji 2 | capítulo 7 |
| DStreams e Structured Streaming | Damji 1 | capítulo 8 |
| MLlib | Damji 1 | capítulos 10 e 11 |
| Modos de deployment | Damji 2 (duas vezes) | Tabela 1-1 do capítulo 1 |

A coluna do meio é a dívida que a leitura deixa, e o [aprofundamento](02-aprofundamento.md) é onde ela é paga.

## Como ler

Cerca de uma hora e meia de leitura corrida. Não pare para pesquisar cada termo desconhecido, isso é trabalho da etapa seguinte. Pare para anotar duas coisas apenas:

1. **Onde eu não acreditei.** Número redondo demais, afirmação sem fonte, benchmark sem condição declarada. Virou a lista de 55 dúvidas numeradas em [Onde eu não acreditei](#onde-eu-não-acreditei).
2. **Onde eu não entendi.** Distinguir "não entendi o mecanismo" de "não entendi o vocabulário", porque as duas coisas se resolvem de formas diferentes. O vocabulário virou a tabela [Vocabulário novo](#vocabulário-novo); o mecanismo virou as perguntas de [O que fica para o aprofundamento](#o-que-fica-para-o-aprofundamento).

## Teses dos capítulos que valem marcação

A lista foi escrita antes da leitura. Depois dela, cada item ficou com o veredito e o lugar exato onde a afirmação aparece. A caixa marcada quer dizer que o palpite está nos capítulos, inteiro ou pela metade; a vazia, que não está.

- [x] O ganho de **100x sobre o MapReduce**, atribuído a processamento em memória. Meio palpite: o número está lá, a atribuição a memória não. Aparece em Luu 1, seção *Overview*, creditado ao site do Apache Spark, e o capítulo não explica de onde vem o ganho. A menção a armazenamento em memória aparece três parágrafos depois, na seção de história, como motivação da pesquisa em Berkeley, sem nenhuma ponte com o número. Damji 1 não repete o 100x: fala em **10 a 20 vezes** nos artigos iniciais e em "muitas ordens de grandeza" hoje, sem fonte.
- [x] O **motor unificado**: batch, streaming, SQL, ML e grafos na mesma engine. É a espinha dorsal dos dois capítulos de abertura. Luu 1 chama de *Spark unified stack*; Damji 1 chama de *Unified Analytics* e apoia no prêmio da ACM de novembro de 2016.
- [x] **RDD como API de baixo nível** que raramente se usa hoje. Só Damji 2 sustenta isso: diz que, desde o Spark 2.x, os RDDs foram relegados a API de baixo nível, e o exemplo completo do capítulo não usa RDD e diz isso nos comentários. Luu 1 diz o contrário em tom, e Damji 1 fica no meio. Ver [divergência 1](#divergências-entre-os-dois-livros).
- [ ] `local[*]`, **client mode** e **cluster mode** apresentados como variações de invocação. Não é o que os capítulos fazem. `local[*]` aparece três vezes, sempre dentro do banner de subida de um shell (Luu 2, figuras 2-2 e 2-3; Damji 2, só no banner do `spark-shell`), e nunca no texto corrido de nenhum dos dois. Client mode e cluster mode aparecem só como duas linhas da tabela de modos de deployment de Damji 1, e sempre presos ao YARN. Nesses dois o Luu não toca.
- [ ] **AQE** apresentado como algo que se liga manualmente. Falso na origem: Luu 1 descreve o *Adaptive Query Execution Framework* como um mecanismo que adapta o plano em runtime e não diz nada sobre como habilitar. Damji não menciona AQE em nenhum dos dois capítulos. A ideia de "ligar manualmente" não vem daqui.
- [x] **Delta Lake** como o formato transacional natural do ecossistema. Luu 1, seção *Apache Spark Ecosystem*, junto com Koalas e MLflow. Damji não cita Delta Lake nestes capítulos, mas ele aparece por outra porta: o aviso do Databricks Runtime 8.x, na captura do formulário de cluster em Luu 2, diz que o Delta é o formato de tabela default.
- [x] A **Databricks Community Edition** como ambiente gratuito recomendado para estudar. Luu 2 dedica um terço do capítulo a ela, com passo a passo de criação de cluster, pasta e notebook. Damji 2 traz a mesma recomendação em um boxe curto.
- [x] Requisitos de ambiente: versões de **Java, Scala e Python**. Os dois trazem, e discordam, mas nenhum dos dois declara versão de Scala como requisito: no Luu 2 ela só aparece dentro de captura de tela (Scala 2.12 no rodapé da página de download, 2.12.10 no banner do shell) e no Damji 2 dentro do `build.sbt`. Ver a tabela em [Luu, capítulo 2](#luu-capítulo-2) e a [divergência 4](#divergências-entre-os-dois-livros).

Dos oito palpites, três se confirmaram (motor unificado, Delta Lake, Community Edition), três se confirmaram pela metade (o 100x, o papel do RDD e os requisitos de versão) e dois não estão nos capítulos. Os dois ausentes são justamente os operacionais: `local[*]` com client e cluster mode, e o AQE como coisa que se configura. A leitura oficial não ensina a rodar nada fora do modo local nem a mexer no otimizador. Isso é lacuna de bibliografia, não engano meu.

Os três últimos itens envelheceram de formas diferentes, e a distinção importa. As versões de Java, Scala e Python e a Community Edition estão factualmente erradas hoje: confira as duas antes de gastar tempo montando ambiente pelo livro. O Delta Lake continua fazendo o que o capítulo descreve; o que mudou é que ele deixou de ser a única escolha.

## Registro de leitura

Anotação por capítulo, na ordem em que foram lidos. Os números entre colchetes remetem à lista de [Onde eu não acreditei](#onde-eu-não-acreditei), que está agrupada por capítulo.

---

### Luu, capítulo 1

*Introduction to Apache Spark*, 18 páginas. Capítulo de panorama: define o Spark por três propriedades, conta a história, apresenta as peças da arquitetura, percorre a stack componente por componente, lista as novidades do 3.0 e termina no ecossistema em volta.

#### A tese das três propriedades

O capítulo abre definindo o Spark como motor de processamento distribuído de propósito geral construído para **velocidade, facilidade de uso e flexibilidade**, e afirma que a combinação das três é o que explica a adoção. Cita Facebook, Microsoft, Netflix e LinkedIn como adotantes. Situa o 3.0 em junho de 2020, décimo aniversário do projeto como open source, e apresenta como principal destaque da release técnicas de **otimização just-in-time** para acelerar aplicações e reduzir o esforço de tuning, termo que o capítulo nunca retoma [17].

Cada propriedade vem com uma evidência:

- **Velocidade.** O site do Apache Spark afirma até 100 vezes mais rápido que o Hadoop MapReduce em certas cargas. O capítulo reforça com o Daytona GraySort de 2014, benchmark de setor que mede em quanto tempo um sistema ordena 100 TB (um trilhão de registros) e que o Spark **venceu**: a submissão da Databricks alegou ordenar três vezes mais rápido usando dez vezes menos recursos que o recorde anterior, do Hadoop MapReduce [1].
- **Facilidade de uso.** Mais de 80 operadores de alto nível, disponíveis em Scala, Java, Python e R.
- **Flexibilidade.** Uma stack única que resolve batch, consultas interativas, algoritmos iterativos de machine learning e streaming quase em tempo real. Antes do Spark, cada carga dessas exigia uma tecnologia diferente, e o argumento econômico é a redução de custo operacional.

Fecha o panorama com dois pontos de integração: o Spark conversa bem com o ecossistema de big data (HDFS, gerenciadores de cluster, formatos binários e colunares) e é open source, o que permite ler o código para entender uma feature ou depurar mais rápido.

#### História

Projeto de pesquisa na UC Berkeley, AMPLab, em **2009**. A motivação declarada foi a ineficiência do MapReduce em cargas interativas e iterativas; as duas ideias trazidas como resposta foram **armazenamento em memória** e um jeito eficiente de lidar com recuperação de falhas. Open source em **2010**, projeto top-level da Apache em **2013**.

Vários pesquisadores fundaram a **Databricks**, que levantou mais de 43 milhões de dólares em 2013 e é apresentada como a principal guardiã comercial do Spark. Em 2015 a IBM anunciou investimento grande em um centro de tecnologia Spark.

Os dois artigos indicados como fundação teórica: *Spark: Cluster Computing with Working Sets* e *Resilient Distributed Datasets: A Fault-Tolerant Abstraction for In-Memory Cluster Computing*. Os dois links apontam para páginas pessoais hospedadas no CSAIL do MIT, sob o diretório `matei`. O capítulo não nomeia os autores nem data os artigos: os anos 2010 e 2012 estão só no caminho da URL, e a atribuição a Matei Zaharia é inferência de quem lê, não afirmação do texto.

Sobre a comunidade, o capítulo afirma que o número de contribuidores cresceu em mais de mil e que existem mais de duzentos mil meetups de Apache Spark, e que o número de contribuidores do Spark ultrapassou o do Hadoop [2].

Sobre a escolha de linguagem: Scala foi escolhida pela combinação de concisão e tipagem estática, o Spark é hoje uma das maiores aplicações escritas em Scala, e o capítulo ainda credita ao Spark parte da popularização da linguagem, dizendo que ela virou mainstream por causa disso [14].

#### Arquitetura: as peças

O capítulo organiza a arquitetura em cinco conceitos.

| Peça | O que o texto diz |
|---|---|
| **Spark cluster** | conjunto de máquinas onde o sistema distribuído roda; pode ir de algumas máquinas a milhares. Cita o FAQ oficial: o maior cluster Spark do mundo tem mais de 8000 máquinas [10] |
| **Resource management system** | YARN ou "Apache Meso" (grafia do original) [3]. Dois componentes: **cluster manager**, que orquestra atribuindo trabalho, e **worker**, que oferece recursos (memória, CPU) e executa o que recebe, por exemplo subir um processo e monitorar sua saúde |
| **Spark application** | duas partes: a lógica de processamento expressa via APIs do Spark, e o driver |
| **Spark driver** | coordenador central; conversa com o cluster manager para decidir em quais máquinas rodar a lógica, e pede ao cluster manager que lance um **executor** em cada uma. Também gerencia e distribui as tasks, e coleta e mescla resultados quando a aplicação precisa devolver algo ao usuário. Faz isso através de um componente chamado `SparkSession` |
| **Spark executor** | um processo JVM dedicado a **uma** aplicação. Vive o tempo inteiro da aplicação, de minutos a dias. A decisão de projeto de não compartilhar executor entre aplicações foi consciente: isola as aplicações, ao custo de não dar para compartilhar dado sem passar por storage externo como HDFS |

A escolha de interoperar com gerenciador de recursos, em vez de trazer o seu, é justificada pelo parque instalado: quase toda empresa que adotou big data já tem um cluster YARN rodando MapReduce, Apache Pig ou Apache Hive, e o Spark foi desenhado para entrar nesse cluster sem pedir outro. Startups que adotam o Spark integralmente podem ficar só com o cluster manager que vem na caixa.

Duas frases estruturais do capítulo:

- O Spark usa arquitetura **master/slave**, com o driver como master e o executor como slave, cada um rodando como processo independente. Uma aplicação tem um driver e um ou mais executores [4].
- Cada task roda em um **core de CPU separado**, e é assim que o paralelismo acontece. Cada executor também é responsável por cachear parte do dado em memória ou disco quando a lógica manda.

Na hora de lançar a aplicação, é possível especificar quantos executores ela precisa e quanta memória e quantos cores cada um deve ter. O capítulo não mostra como.

Duas figuras dizem o que o texto corrido não diz. A **Figura 1-1** desenha o driver, com a `SparkSession` dentro dele, conversando em mão dupla com o cluster manager e, ao mesmo tempo, direto com dois workers; cada worker é uma caixa que contém um executor, e dentro do executor aparecem duas tasks e um bloco de **Cache**. A **Figura 1-2** mostra a topologia mínima do capítulo: um driver e três executores. A Figura 1-1 é o único lugar do capítulo onde o executor aparece **dentro** de um worker: o texto define worker como componente do sistema de gerência de recursos e nunca diz que é ali que o executor roda. Quem diz é o desenho.

#### A stack unificada

Fundação: o **Spark Core**, que entrega escalonamento, coordenação e tolerância a falhas, mais a abstração de programação chamada **RDD** (*resilient distributed datasets*). Em cima dele, uma biblioteca por tipo de carga. A **Figura 1-3** desenha isso como cinco caixas iguais sobre uma faixa larga chamada Spark Core.

| Componente | O que o texto atribui a ele |
|---|---|
| Spark SQL | processamento interativo e dado estruturado em escala |
| Spark Streaming | processamento em tempo real |
| Spark GraphX | processamento de grafos |
| Spark MLlib | machine learning |
| SparkR | tarefas de ML pelo shell do R |

Os três benefícios declarados da unificação: aplicações mais simples de desenvolver e implantar porque usam um conjunto único de APIs sobre um motor só; combinar tipos de processamento fica mais eficiente porque o Spark roda diferentes APIs sobre o mesmo dado **sem escrever o intermediário em storage** [15]; e a unificação habilita aplicações novas, como rodar consulta interativa sobre o resultado de predições de ML feitas em cima de streams em tempo real. A analogia usada é o smartphone, que junta câmera, telefone e GPS e assim viabiliza um Waze.

**Spark Core.** Duas responsabilidades além do RDD: lidar com falhas de tasks e mover dado entre máquinas de forma eficiente, o que o texto chama de **data shuffling**. Diz que usuário avançado precisa conhecer essa infraestrutura em detalhe para desenhar aplicação de alta performance. O RDD é definido como coleção de objetos tolerante a falhas, particionada pelo cluster, manipulável em paralelo, com APIs em Scala, Java e Python [12], que permitem passar funções locais para rodar no cluster. Também registra que qualquer otimização feita no Core beneficia automaticamente os componentes de cima.

**Spark SQL.** Módulo para dado estruturado em escala. Coloca SQL como a *lingua franca* do processamento de dados: o usuário expressa intenção e o motor otimiza. O **DataFrame** é definido como coleção distribuída de dados organizada em colunas nomeadas, inspirada nos data frames de R e Python, conceitualmente equivalente a uma tabela relacional. Por trás, o **Catalyst optimizer** faz as otimizações comuns de engines analíticas. Lê e escreve JSON, CSV, Parquet, ORC, bancos relacionais, Hive e outros. Segundo o texto, a pesquisa Spark de 2021 apontou o Spark SQL como componente que mais cresceu, o que faz sentido porque amplia o público para além do engenheiro de big data [5]. O lema citado: escrever menos código, ler menos dado, e deixar o otimizador fazer o trabalho pesado.

**Spark Structured Streaming.** Abre com a frase de que dado em movimento vale tanto ou mais que dado histórico, e apresenta o módulo como capaz de processar dado de stream em tempo real, de várias fontes, com **alta vazão e tolerância a falhas**. Fontes citadas: Kafka, Flume, Kinesis, Twitter, HDFS e socket TCP [7]. Aqui o capítulo faz uma coisa estranha: diz que a **abstração principal do Spark para streaming é o DStream** [6] (*discretized stream*), que fatia a entrada em micro-lotes por intervalo de tempo, e só no parágrafo seguinte apresenta o **Structured Streaming** como motor novo, introduzido na versão 2.1, que trata computação de stream do mesmo jeito que computação batch sobre dado estático, executa de forma incremental e contínua, e garante **exactly-once** ponta a ponta [8]. Registra também que juntar stream com dado em repouso é fácil, e credita isso à stack unificada. Fecha com a frase de Reynold Xin, arquiteto-chefe da Databricks: o jeito mais simples de fazer análise de stream é não precisar raciocinar sobre stream.

**Spark MLlib.** Mais de 50 algoritmos comuns, mais abstrações para featurização, construção de pipeline, avaliação e tuning, e persistência de modelos para levá-los de desenvolvimento a produção. Saem prontos da caixa os quatro grupos mais usados: classificação, regressão, clusterização e filtragem colaborativa. A partir do Spark 2.0 as APIs de MLlib são baseadas em DataFrame, para aproveitar Catalyst e Tungsten. O argumento de por que Spark serve para ML: algoritmos de ML são iterativos, e o Spark torna fácil rodá-los de forma escalável.

**Spark GraphX.** Abstração de multigrafo direcionado com propriedades em vértices e arestas, para computação paralela em grafo. Algoritmos prontos: page rank, componentes conectados, caminhos mínimos.

**SparkR.** Pacote R que oferece um frontend leve para o Spark, para contornar o fato de o R não ter sido desenhado para dados que não cabem em uma máquina.

#### As novidades do Spark 3.0

O capítulo afirma que cerca de **60% das melhorias** foram para Spark SQL e Spark Core, porque otimização de performance de query foi o tema da release, e que pelo benchmark TPC-DS de 30 TB feito pela Databricks o Spark 3.0 é aproximadamente **duas vezes** mais rápido que o 2.4 [9]. Três features destacadas:

| Feature | O que faz, segundo o texto |
|---|---|
| **Adaptive Query Execution (AQE)** | adapta o plano de execução em runtime com base em estatísticas recentes sobre tamanho de dado e número de partições. Consequências citadas: trocar estratégia de join dinamicamente, otimizar joins com skew automaticamente e ajustar o número de partições |
| **Dynamic Partition Pruning (DPP)** | evitar ler dado desnecessário. Desenhado para joins entre tabela fato e tabelas dimensão em star schema: reduz o número de linhas da fato que precisam entrar no join, dadas as condições de filtro. Pelo TPC-DS, acelera 60% das queries em uma faixa de 2x a 18x |
| **Accelerator-aware scheduler** | permite descrever e pedir recursos de **GPU**, para cargas de ML |

Sobre o AQE, o capítulo não diz se vem ligado, como se configura, nem sob quais condições ele decide. Isso é exatamente o buraco que o [aprofundamento](02-aprofundamento.md) precisa fechar.

#### Aplicações e o exemplo canônico

Lista de casos de uso construídos com Spark: inteligência de clientes, data warehouse, streaming em tempo real, motores de recomendação, processamento de log, serviços voltados ao usuário e detecção de fraude.

O exemplo canônico é o **word count**, tradição herdada do MapReduce. A versão mostrada tem cinco linhas de Scala e usa RDD: ler arquivos de texto de uma pasta HDFS, quebrar cada linha em palavras e achatar, anexar contagem 1 a cada palavra, somar por chave, salvar o resultado. Os operadores, na ordem: `textFile`, `flatMap`, `map`, `reduceByKey` e `saveAsTextFile`. O capítulo explica linha a linha e usa o exemplo como prova de facilidade de uso.

Detalhe que fica pendente: o exemplo começa com `sc.textFile(...)`, e `sc` nunca foi apresentado no capítulo [11].

#### Ecossistema

Três projetos apresentados como inovações recentes em volta do Spark [16].

- **Delta Lake.** A ideia é usar storage distribuído para guardar dado estruturado e não estruturado para vários consumidores (cientista de dados, engenheiro, analista de negócio). Para o dado ser usável, o texto diz que precisa haver cuidado com catálogo, descoberta, qualidade, controle de acesso e semântica de consistência, e que consistência é onde as empresas inventaram gambiarras. Delta Lake é definido como solução open source de semântica de consistência: formato aberto de armazenamento com **garantias transacionais**, **enforcement de schema** e suporte a **evolução de schema**.
- **Koalas.** Implementa a API de DataFrame do pandas em cima do Spark, para que cientista de dados aproveite conhecimento de pandas em dados maiores. Registra que pandas roda em uma máquina só e cita o Dask como alternativa de paralelismo em Python. Koalas 1.0 saiu em junho de 2020 com cobertura de 80% das APIs do pandas [13].
- **MLflow.** Projeto open source concebido em 2018 para gerenciar o ciclo de vida de ML, com quatro componentes: **Tracking** (registrar e comparar experimentos), **Projects** (formato consistente de organização para reprodutibilidade), **Models** (formato padronizado de empacotamento e API consistente de carga e deploy) e **Registry** (repositório de modelos com linhagem, versão e transições de estado de deploy). O enquadramento é bom: aplicar ML a problema de negócio é mais um problema de engenharia de software do que de algoritmo.

Dois assuntos ficam explicitamente adiados para capítulos posteriores: o RDD e o Delta Lake.

#### O que o Luu resume no fim do capítulo 1

Três propriedades (facilidade, velocidade, flexibilidade); arquitetura master/slave com um driver e um ou mais executores; **paralelismo como o habilitador central**; motor unificado para batch, exploração interativa, stream, ML e grafo; e aplicações em Scala, Java, Python ou R.

#### O que ficou marcado em Luu 1

Dúvidas [1] a [17]. Vocabulário aberto sem definição suficiente: shuffling, Catalyst, Tungsten, DStream, star schema, skew, otimização just-in-time.

---

### Luu, capítulo 2

*Working with Apache Spark*, 28 páginas. Capítulo operacional. Anuncia três formas de trabalhar com Spark (shell, submissão pela linha de comando e a plataforma hospedada da Databricks) e entrega duas: `spark-submit` não aparece em nenhuma página, nem como comando nem como exemplo [21]. Termina com como baixar o código-fonte do Spark para leitura.

#### Download e instalação

O argumento de por que instalar local: testar features e lógica com datasets pequenos, de qualquer lugar. Registra que montar cluster de produção multi-tenant está fora do escopo do livro. É declaração honesta, e é o limite que explica a lacuna operacional do capítulo.

Requisito declarado: o Spark é escrito em Scala, empacotado para Windows e sistemas tipo UNIX, e para rodar local **basta ter Java instalado** [24].

A página de download é um formulário de quatro linhas, e a Figura 2-1 mostra o estado exato usado pelo livro: 1. escolher a release, `3.1.1 (Mar 02 2021)`; 2. escolher o tipo de pacote, `Pre-built for Apache Hadoop 2.7`; 3. clicar no link do arquivo, que é o que dispara o download; 4. verificar a release com assinaturas, checksums e as chaves do projeto. A instrução no texto é escolher o pacote com a versão mais nova do Hadoop, e o formulário da figura está em 2.7 [18]. O rodapé da figura registra que o Spark 2.x vem pré-compilado com Scala 2.11 (exceto o 2.4.2) e que o **Spark 3.0 em diante vem com Scala 2.12**: é a única informação de versão de Scala do capítulo, e está dentro de uma captura de tela.

O arquivo é `spark-3.1.1-bin-hadoop2.7.tgz`, um tar comprimido com GZIP, daí o `tar xvf`. No Windows, WinZip ou 7-zip. O capítulo avisa que baixar outra versão muda o nome do diretório, e que existem cerca de uma dúzia de diretórios sob a raiz: os cinco da tabela são os que o autor considera úteis, não a lista completa.

| Diretório | Conteúdo |
|---|---|
| `bin` | executáveis para subir shell Scala ou Python, submeter aplicações e rodar exemplos |
| `data` | arquivos de dados pequenos para os exemplos |
| `examples` | código-fonte e binário de todos os exemplos |
| `jars` | binários necessários para rodar o Spark |
| `sbin` | executáveis de administração do cluster |

#### Os shells

O shell é apresentado como ambiente interativo para aprender Spark e analisar dado, análogo a um shell Unix.

| | Scala | Python |
|---|---|---|
| Comando | `./bin/spark-shell` | `./bin/pyspark` |
| Sair | `:quit` ou `:q` | `ctrl-d` |
| Requisito declarado | **Java 11 ou superior** preferido | **Python 3.7.x ou superior** |

O banner de subida carrega mais informação do que o texto. No shell Scala (Figura 2-2): nível de log default em `WARN`, com `sc.setLogLevel(<nível>)` indicado como a forma de mudar; `Spark context Web UI available at http://192.168.0.22:4040`; `Spark context available as 'sc' (master = local[*], app id = local-...)`; `Spark session available as 'spark'`; versão 3.1.1; `Using Scala version 2.12.10 (OpenJDK 64-Bit Server VM, Java 11.0.6)`. No `pyspark` (Figura 2-3) o banner repete tudo, com `Using Python version 3.7.6` e `SparkSession available as 'spark'`. Duas consequências: o `sc` que o capítulo 1 usa sem apresentar [11] está aqui na tela e continua sem uma linha de explicação, e `local[*]` aparece nos dois banners, também sem explicação.

Os dois shells são extensões dos REPLs de Scala e de Python. O texto explica o que é REPL (*read-eval-print loop*) e por que o feedback imediato aumenta a produtividade: pula a etapa de compilação. Registra que o shell só depende dos arquivos de dado estarem na máquina, e que ler arquivo remoto funciona mas é lento. Declara que **o restante do livro usa o shell Scala**, o que é uma escolha relevante para quem vai estudar em Python.

Comandos que o capítulo destaca, seis na Tabela 2-2 mais o `:help` do parágrafo anterior:

| Comando | Para quê |
|---|---|
| `:help` | lista completa de comandos |
| `:history` | mostra o que foi digitado na sessão atual e nas anteriores, útil para copiar |
| `:load` | carrega e executa código de um arquivo, útil quando a lógica fica longa |
| `:reset` | volta o shell a um estado limpo, quando você perdeu o controle das variáveis |
| `:silent` | desliga a impressão do resultado de cada expressão; digitar de novo religa |
| `:quit` | sair; o texto observa que muita gente tenta `:exit`, que não funciona, e a lista da Figura 2-4 confirma que ele não está entre os comandos |
| `:type <var>` | mostra o tipo de uma variável ou o tipo de retorno de uma função |

A Tabela 2-2 é uma seleção. A Figura 2-4 traz a lista inteira, e abre com a regra que o texto não comenta e que explica por que `:q` funciona: **todo comando pode ser abreviado**, `:he` vale por `:help`. A lista completa: `:completions`, `:edit`, `:help`, `:history`, `:h?`, `:imports`, `:implicits`, `:javap`, `:line`, `:load`, `:paste`, `:power`, `:quit`, `:replay`, `:require`, `:reset`, `:save`, `:sh`, `:settings`, `:silent`, `:type`, `:kind`, `:warnings`. Três da lista completa ficaram de fora da seleção do autor e valem mais do que alguns dos escolhidos, na minha leitura: `:paste`, que entra em modo de colagem e é como se cola bloco de mais de uma linha sem o REPL avaliar linha a linha; `:sh`, que roda comando de shell de dentro do REPL; e `:save`, que grava a sessão em arquivo.

Também apresenta o **tab completion**, que completa nome parcial e lista membros e funções de um objeto, e a seta para cima para recuperar comando anterior.

#### Scala mínimo

A sequência de exemplos é deliberadamente pequena: `println` de uma string; definir um array de idades e ligá-lo a uma variável imutável com `val`; usar tab completion para descobrir `foreach`; definir uma função com `def` que testa se a idade é ímpar; filtrar com `filter` encadeado com `foreach`. Dois pontos de linguagem que o texto faz questão de marcar: em Scala **não se usa a palavra `return`**, o valor da última expressão do corpo é o retorno; e **encadear funções** é prática comum para deixar o código conciso.

Fecha com uma orientação prática que vale anotar: para aprender Spark não é necessário dominar Scala, basta ser confortável com o básico. Indica o repositório `deanwampler/JustEnoughScalaForSpark` como material suficiente.

#### Spark UI

O autor se corrige aqui: tinha dito que o shell é uma aplicação Scala, e emenda que isso é só parcialmente verdade, porque o shell é uma **aplicação Spark escrita em Scala**. Ao subir, ele inicializa a Spark UI e algumas variáveis.

Uma linha do output informa que a **SparkContext Web UI** está disponível em `http://<ip>:4040`. A UI é descrita como aplicação web para monitorar e depurar, com informação de runtime e consumo de recursos, útil para diagnosticar performance. Restrição importante: **a UI só existe enquanto a aplicação está rodando**. A captura (Figura 2-14) mostra a UI identificada como `Spark shell application UI`, com a versão 3.1.1 ao lado do logo, e a página Jobs trazendo **User**, **Total Uptime**, **Scheduling Mode: FIFO** e um link **Event Timeline**.

O texto lista seis abas (Jobs, Stages, Storage, Environment, Executors e SQL) e a figura mostra cinco [26]. O capítulo detalha duas.

**Environment**, com seis seções:

| Seção | Conteúdo |
|---|---|
| Runtime Information | localizações e versões dos componentes de que o Spark depende, incluindo Java e Scala |
| Spark Properties | propriedades básicas (id e nome da aplicação) e avançadas (ligar, desligar ou ajustar features). Aponta `https://spark.apache.org/docs/latest/configuration.html` como lista completa |
| Resource Profiles | número de CPUs e quantidade de memória no cluster |
| Hadoop Properties | propriedades do Hadoop e do sistema de arquivos |
| System Properties | propriedades de SO e de Java, não específicas do Spark |
| Classpath Entries | classpaths e jars usados pela aplicação |

**Executors**: resumo e detalhamento por executor de memória, disco e CPU, com uma seção de sumário que dá a visão agregada.

#### `SparkSession` no shell

Ao subir o shell, uma variável chamada `spark` já vem inicializada, e `:type spark` confirma que é uma instância de `SparkSession`. O capítulo situa a classe: introduzida no **Spark 2.0** como ponto de entrada único para as funcionalidades do Spark, com APIs para ler dado estruturado e não estruturado em texto e binário (JSON, CSV, Parquet, ORC) e com facilidade para ler e ajustar configurações.

As interações demonstradas são propositalmente triviais: `spark.version` para a versão, `println` concatenando a versão, `spark.conf.getAll.foreach(println)` para listar a configuração default, e `spark.<tab>` para descobrir o que está disponível. A Figura 2-6 mostra o que esse último devolve: `baseRelationToDataFrame`, `catalog`, `close`, `conf`, `createDataFrame`, `createDataset`, `emptyDataFrame`, `emptyDataset`, `executeCommand`, `experimental`, `implicits`, `listenerManager`, `newSession`, `range`, `read`, `readStream`, `sessionState`, `sharedState`, `sparkContext`, `sql`, `stop`, `sqlContext`, `streams`, `table`, `time`, `udf`, `version`. Note `sparkContext` e `sqlContext` na lista: é por aí que se chega no `sc`.

#### Databricks Collaborative Notebooks

Produto comercial da Databricks, apresentado como plataforma para engenheiro de dados, cientista de dados e analista, com múltiplas linguagens, visualização embutida, versionamento automático, computação Spark sob demanda e agendamento de pipelines de produção. Quatro propostas de valor: clusters Spark totalmente gerenciados, workspace interativo, agendador de pipelines e plataforma para aplicações baseadas em Spark.

Duas versões: a plataforma completa (paga, com múltiplos clusters, gestão de usuários e agendamento de jobs) e a **Community Edition**, gratuita, apresentada como ideal para quem quer aprender.

O que o capítulo afirma sobre a CE:

- cluster Spark de **nó único com 15 GB de memória**, gratuito, hospedado na AWS;
- desliga sozinho depois de **duas horas** ocioso, então não é preciso derrubar manualmente;
- criar o cluster leva **até 10 minutos**, e se demorar demais vale trocar a *availability zone*;
- o capítulo diz três coisas diferentes sobre quantos clusters cabem por conta, e a última, a que casa com o produto, é **um cluster por vez** [19].

Campos do formulário de criação: nome do cluster (único campo obrigatório, pode ter espaço), Databricks Runtime Version (escolher a mais recente, já preenchida, cada uma amarrada a uma imagem AWS específica), Instance (sem escolha na CE), AWS Availability Zone e Spark Config (configurações específicas da aplicação, por exemplo ajustes de JVM para ligar features). A captura (Figura 2-25) mostra o estado real: runtime `8.3 (Scala 2.12, Spark 3.1.1)`, availability zone em `auto`, alternador **UI | JSON** no canto, cabeçalho com `0 Workers` e `1 Driver: 15.3 GB Memory, 2 Cores, 1 DBU`, e um aviso de que o Databricks Runtime 8.x usa Delta Lake como formato de tabela default. Ou seja, o nó único da CE tem **dois cores**, número que o texto nunca dá e que é o que de fato limita o paralelismo lá.

O **workspace** é explicado pela analogia com o sistema de arquivos da máquina, hierárquico, para organizar notebooks. O notebook é definido como ambiente computacional interativo, semelhante ao shell mas melhor, onde dá para executar código, documentar com Markdown ou HTML e visualizar resultado em gráfico. A prática recomendada: quebrar a lógica em grupos lógicos, cada um em uma ou mais células, pelo mesmo motivo que se faz isso em software mantido.

Os caminhos de clique, que é o que se consulta depois:

| Passo | Caminho na interface |
|---|---|
| Criar cluster | ícone **Clusters** na barra vertical à esquerda [25], botão **Create Cluster**, preencher o nome, **Create Cluster** de novo. Ponto verde ao lado do nome quando sobe |
| Encerrar cluster | clicar no quadrado na coluna **Actions** da lista de clusters |
| Criar pasta | ícone **Workspace** na barra vertical, seta para baixo no canto superior direito da coluna que desliza (ou botão direito em qualquer ponto dela), **Create > Folder**, digitar o nome na caixa **New Folder Name**, botão **Create Folder** |
| Criar notebook | selecionar a pasta, a coluna dela desliza à direita, seta para baixo ou botão direito, item **Notebook**; na caixa **Create Notebook**, dar nome e escolher **Scala** no campo **Language**. O campo de cluster já vem preenchido, porque a CE só tem um |
| Rodar célula | **Shift+Enter**, que executa e cria uma célula nova abaixo |
| Inserir célula no meio | passar o mouse no espaço entre duas células e clicar no ícone de mais |
| Documentar | `%md` na primeira linha da célula |
| Publicar | **File > Publish**, confirmar na caixa de diálogo, copiar a URL da caixa **Notebook Published** [20] |
| Salvar | não existe: o notebook salva sozinho e o menu **File** não tem opção de salvar |

#### Código-fonte do Spark

Última seção, voltada a quem quer ler o código. Dois caminhos: baixar o pacote **Source Code** na mesma página de download (`http://spark.apache.org/downloads.html`), clicando no link da linha 3, ou clonar do GitHub com `git clone git://github.com/apache/spark.git` [23]. As outras URLs citadas: `https://git-scm.com/downloads` para instalar o git, `https://git-scm.com/book/en/v2/Getting-Started-Installing-Git` para as instruções, e `http://spark.apache.org/developer-tools.html` para importar em uma IDE. O argumento oferecido é que o código do Spark é escrito em Scala por gente muito boa, e lê-lo melhora o próprio Scala.

O que a seção **não** faz, apesar de prometido na página 2: ensinar a construir o binário a partir do fonte [22].

#### O que o Luu resume no fim do capítulo 2

Duas opções para aprender (Spark local ou Community Edition); o shell como ambiente interativo em duas variantes, Scala e Python, com um conjunto de comandos que aumentam produtividade; os Collaborative Notebooks como plataforma gerenciada com workspace hierárquico e compartilhamento em poucos cliques; e a leitura do código-fonte para quem quer entender internals.

#### O que ficou marcado em Luu 2

Dúvidas [18] a [26]. O capítulo inteiro se apoia em capturas de tela de um produto que muda sozinho, o que envelhece mal por construção: são 44 figuras em 28 páginas, e 23 delas (2-21 a 2-43) são interface da Databricks. Nota prática sobre este PDF: as figuras 2-35 a 2-43, todas de notebook, saíram em branco na exportação, então a sequência de células e a caixa de publicação não são consultáveis aqui.

---

### Damji, capítulo 1

*Introduction to Apache Spark: A Unified Analytics Engine*, 22 páginas. Mesmo território do Luu 1, mas com outra estratégia: em vez de listar propriedades, o capítulo **argumenta a partir do problema**. Conta o que o Google precisou resolver, o que o Hadoop resolveu e onde falhou, e apresenta o Spark como resposta a falhas nomeadas. Na minha leitura é o capítulo mais forte dos quatro. Ele próprio se apresenta como dispensável: diz que quem já conhece a história do Spark e os conceitos de alto nível pode pular.

#### Gênese, parte 1: Google

O capítulo entra pela escala. Lembra que Google é erro de grafia proposital de *googol*, 1 seguido de 100 zeros, e usa o nome como medida do problema: indexar e buscar a web inteira em velocidade de relâmpago. Nem RDBMS nem programação imperativa davam conta disso, e daí saíram três coisas: **GFS** (sistema de arquivos distribuído e tolerante a falhas sobre hardware comum), **Bigtable** (armazenamento escalável de dado estruturado sobre o GFS) e **MapReduce** (paradigma de programação paralela, baseado em programação funcional, para processar dado distribuído sobre GFS e Bigtable).

A ideia central do MapReduce, e o capítulo insiste nela: a aplicação conversa com o sistema MapReduce, e é o **sistema** que manda o código (as funções map e reduce) para onde o dado está, favorecendo localidade de dado e afinidade de rack em vez de trazer o dado até a aplicação. Os workers agregam e reduzem as computações intermediárias e escrevem a saída final em storage distribuído. O efeito é reduzir tráfego de rede e manter o I/O local ao disco.

O trabalho do Google era proprietário, mas os três artigos alimentaram a comunidade open source, principalmente no Yahoo!.

#### Gênese, parte 2: Hadoop e suas quatro falhas

O artigo do GFS virou o blueprint do **HDFS**, com o MapReduce como framework de computação distribuída. Doado à Apache Software Foundation, descrita como organização sem fins lucrativos e neutra em relação a fornecedor, em **abril de 2006**, virou o Hadoop com seus módulos: Hadoop Common, MapReduce, HDFS e YARN [36]. Cita Cloudera e Hortonworks, hoje fundidas, como as duas empresas comerciais nascidas em volta.

As quatro deficiências, nomeadas na ordem em que o capítulo as apresenta:

1. **Operação difícil.** Complexidade operacional de administrar e gerenciar.
2. **API verbosa.** O MapReduce batch exigia muito código de boilerplate, com tolerância a falhas frágil.
3. **I/O de disco repetido.** Em jobs grandes com muitos pares de tarefas MR, o resultado intermediário de cada par é escrito no disco local para o estágio seguinte. Jobs grandes podiam levar horas ou dias.
4. **Carga única.** Servia para batch em larga escala, mas não combinava com ML, streaming ou consultas SQL interativas.

A Figura 1-1 desenha a terceira falha em cinco quadros: HDFS lê, MR processa, HDFS escreve, HDFS lê de novo, MR processa, HDFS escreve. É esse vaivém de disco que o Spark quer cortar, e é o desenho que sustenta o argumento de memória do resto do capítulo.

A consequência da quarta falha: para cada carga nova, um sistema sob medida (Hive, Storm, Impala, Giraph, Drill, Mahout), cada um com API e configuração de cluster próprias, o que empilhou complexidade operacional e curva de aprendizado. A pergunta que o capítulo formula, com a máxima de Alan Kay de que coisas simples deveriam ser simples e coisas complexas deveriam ser possíveis: dava para fazer Hadoop e MR mais simples e mais rápidos?

#### Gênese, parte 3: AMPLab

Pesquisadores de Berkeley que já tinham trabalhado com Hadoop MapReduce começaram o Spark em **2009**, no RAD Lab, que virou AMPLab e hoje é RISELab. O reconhecimento explícito foi que o MR era ineficiente ou inviável para computação interativa e iterativa, e complexo de aprender.

Os artigos iniciais mostraram **10 a 20 vezes** mais rápido que o Hadoop MapReduce em certos jobs; hoje, diz o texto, muitas ordens de grandeza [27]. O programa do projeto, em cinco itens: pegar as ideias do MapReduce e tornar o sistema altamente tolerante a falhas, embaraçosamente paralelo, com **armazenamento em memória para resultados intermediários** entre computações iterativas e interativas, APIs fáceis e componíveis em várias linguagens, e suporte unificado a outras cargas.

Até 2013 o Spark já tinha uso disseminado, e **alguns dos** criadores e pesquisadores originais (Matei Zaharia, Ali Ghodsi, Reynold Xin, Patrick Wendell, Ion Stoica e Andy Konwinski) doaram o projeto à ASF e fundaram a **Databricks**. O **Spark 1.0** saiu em **maio de 2014**, sob governança da ASF, e o texto credita contribuições de mais de 100 fornecedores comerciais.

#### O que é o Spark: quatro características

Definição: motor unificado para processamento distribuído de dado em larga escala, em data center ou nuvem, com armazenamento em memória para computações intermediárias, e bibliotecas com APIs componíveis para ML (MLlib), SQL interativo (Spark SQL), stream (Structured Streaming) e grafo (GraphX).

| Característica | O que sustenta, segundo o texto |
|---|---|
| **Speed** | três coisas: aproveitamento dos ganhos de preço e performance de CPU e memória (servidores comuns com centenas de GB de RAM, múltiplos cores e SO Unix eficiente em multithreading); a construção da computação como **DAG**, cujo escalonador e otimizador decompõem em tasks paralelas; e o motor físico de execução, o **Tungsten**, que usa *whole-stage code generation* para gerar código compacto. Somado a manter intermediários em memória e limitar I/O de disco |
| **Ease of use** | uma abstração lógica fundamental, o **RDD**, sobre a qual se constroem DataFrame e Dataset, mais um conjunto de **transformações e ações** como modelo de programação |
| **Modularity** | as operações valem para muitos tipos de carga e para Scala, Java, Python, SQL e R, com bibliotecas unificadas e bem documentadas. Uma aplicação só faz tudo: nada de motores distintos nem de aprender APIs separadas |
| **Extensibility** | o Spark foca em computação paralela e **desacopla storage de compute**, diferente do Hadoop, que trazia os dois. Lê de Hadoop, Cassandra, HBase, MongoDB, Hive, RDBMS e outros. `DataFrameReader` e `DataFrameWriter` são extensíveis para Kafka, Kinesis, Azure Storage e S3. Existe uma lista comunitária de pacotes de terceiros com conectores e monitores |

O desacoplamento entre storage e compute é, na minha leitura, a característica com mais consequência prática das quatro, e o capítulo gasta um parágrafo com ela.

#### Analytics unificado

Reconhece que unificação não é ideia exclusiva do Spark, mas afirma que é núcleo da filosofia de projeto. Em **novembro de 2016**, a ACM premiou os criadores pelo artigo que descreve o Spark como *Unified Engine for Big Data Processing* [37]. O argumento premiado: o Spark substitui os motores separados de batch, grafo, stream e query (Storm, Impala, Dremel, Pregel) por uma stack única de componentes sobre um motor distribuído rápido.

Quatro componentes como bibliotecas, todos **separados do motor central tolerante a falhas**: Spark SQL, Spark MLlib, Spark Structured Streaming e GraphX. O mecanismo é o mesmo para todos: você escreve com as APIs, o Spark converte em **DAG**, o motor central executa. Independente de a linguagem ser Java, R, Scala, SQL ou Python, o código vira bytecode compacto executado nas JVMs dos workers. Esse último ponto é a explicação de por que a paridade entre linguagens é possível.

A **Figura 1-3** desenha a stack de outro jeito: quatro caixas em cima (`Spark SQL and DataFrames + Datasets`, `Spark Streaming (Structured Streaming)`, `Machine Learning MLlib`, `Graph Processing GraphX`) sobre uma faixa única chamada `Spark Core and Spark SQL Engine`, e dentro dessa faixa as cinco linguagens: Scala, SQL, Python, Java e R. A figura briga com o texto em dois pontos [38]: batiza a caixa de streaming com o nome do modelo que o próprio capítulo declara obsoleto, e coloca o motor de Spark SQL na fundação enquanto o texto lista Spark SQL como componente separado do motor central.

| Componente | O que o capítulo registra |
|---|---|
| **Spark SQL** | lê de tabela de RDBMS ou de formatos estruturados (CSV, texto, JSON, Avro, ORC, Parquet) e constrói tabelas permanentes ou temporárias. Dá para combinar consulta SQL com as APIs estruturadas. Afirma conformidade **ANSI SQL:2003** [33] e funcionamento como motor SQL puro. Afirma também que o mesmo trecho escrito em Python, R ou Java gera **bytecode idêntico** e entrega a mesma performance [32] |
| **Spark MLlib** | registra que a performance da biblioteca melhorou muito desde a primeira release, por causa das melhorias do motor no 2.x. Algoritmos comuns de ML sobre APIs baseadas em DataFrame. Nota importante: desde o **Spark 1.6** o projeto está **dividido** em dois pacotes, `spark.mllib` (baseado em RDD) e `spark.ml` (baseado em DataFrame, onde entram todas as features novas). O texto diz que o pacote de RDD está "agora" em **modo de manutenção**, sem datar essa entrada: o que ele fixa em 1.6 é a divisão, não a manutenção. Extrai e transforma features, constrói pipelines de treino e avaliação, persiste modelos, e traz utilidades de álgebra linear, estatística e primitivas como gradiente descendente genérico |
| **Spark Structured Streaming** | o Spark 2.0 introduziu duas coisas, um modelo experimental de *Continuous Streaming* e as APIs de Structured Streaming, sobre o motor do Spark SQL e as APIs de DataFrame; no **2.2** virou GA, ou seja, apto a produção. O modelo mental é forte: **um stream é uma tabela que cresce continuamente**, com linhas novas anexadas ao fim, e o desenvolvedor consulta como se fosse tabela estática. Por baixo, o motor do Spark SQL cuida de tolerância a falhas e semântica de dado atrasado. Diz explicitamente que esse modelo **tornou obsoleto o DStreams** da série 1.x. Registra ainda que o 2.x e o 3.0 ampliaram as fontes para Kafka, Kinesis e storage baseado em HDFS ou nuvem, e remete ao capítulo 8 |
| **GraphX** | manipulação de grafos e computação paralela em grafo. Os exemplos dados são rede social, rotas com pontos de conexão e topologia de rede. Traz PageRank, Connected Components e Triangle Counting, contribuídos pela comunidade. Nota de rodapé: o **GraphFrames**, doado pela Databricks, é biblioteca semelhante ao GraphX mas com APIs baseadas em DataFrame |

Cada componente vem com um trecho curto de código, nenhum executável como está. Spark SQL lê um JSON no S3, cria uma view temporária com `createOrReplaceTempView` e consulta com `spark.sql`. MLlib importa `LogisticRegression` de `pyspark.ml.classification`, instancia com `maxIter=10`, `regParam=0.3` e `elasticNetParam=0.8`, chama `fit` no treino e `transform` no teste. Structured Streaming lê um socket em `localhost:9999` com `readStream`, quebra as linhas com `explode(split(...))`, conta com `groupBy("word").count()` e escreve em um tópico Kafka com `writeStream`. GraphX monta `Graph(vertices, edges)` e junta com `joinVertices`.

#### Execução distribuída

A parte mais densa do capítulo. Uma aplicação Spark consiste em um **driver program** que orquestra operações paralelas no cluster e acessa os componentes distribuídos (executores e cluster manager) **através de uma `SparkSession`**.

A **Figura 1-4** é o mapa da peça: uma caixa `Spark Application` que contém o `Spark Driver` e, dentro dele, a `SparkSession`; uma seta de mão dupla ligando driver e `Cluster Manager`; e setas partindo tanto do driver quanto do cluster manager para dois executores, cada um desenhado com quatro cores. O desenho torna visível o que o texto diz em uma frase: alocado o recurso, o driver passa a falar direto com os executores, sem intermediário.

**Spark driver.** Instancia a `SparkSession`, conversa com o cluster manager, pede recursos (CPU, memória) para os executores, transforma todas as operações Spark em computações **DAG**, agenda e distribui a execução como tasks pelos executores.

**`SparkSession`.** No Spark 2.0 virou o canal unificado para todas as operações e todos os dados. Absorveu os pontos de entrada anteriores: `SparkContext`, `SQLContext`, `HiveContext`, `SparkConf` e `StreamingContext`. Nota de compatibilidade: os contextos individuais continuam acessíveis, então código 1.x com `SparkContext` ou `SQLContext` ainda funciona. Por esse canal você define parâmetros de runtime da JVM, cria DataFrames e Datasets, lê de fontes, acessa metadado de catálogo e emite consultas SQL. Em aplicação standalone você cria a sessão com uma API de alto nível da linguagem escolhida; no shell ela já vem criada, e o texto diz que se acessa por uma variável global chamada `spark` **ou** `sc` [28]. Em aplicação 2.x basta criar **uma** `SparkSession` por JVM e usá-la para tudo, no lugar dos contextos separados do 1.x: o capítulo escreve isso como conveniência, não como limite [40].

O exemplo em Scala monta a sessão pelo builder com `appName("LearnSpark")` e `config("spark.sql.shuffle.partitions", 6)`, e usa a mesma sessão para `read.json` e para `spark.sql`. É a única aparição de `spark.sql.shuffle.partitions` no capítulo, e ela passa sem uma palavra de explicação: o número 6 aparece e some.

**Cluster manager.** Aloca recursos para o cluster de nós. O capítulo lista **quatro** suportados: o standalone embutido, Apache Hadoop YARN, Apache Mesos e Kubernetes [29].

**Spark executor.** Roda em cada nó worker, conversa com o driver e executa as tasks. O texto afirma que, na maioria dos modos de deployment, **roda um único executor por nó** [30].

**Modos de deployment.** O ponto que o capítulo faz é que o cluster manager é agnóstico a onde roda, desde que consiga gerenciar executores e atender pedidos de recurso, e é isso que permite tantos modos. A tabela resume onde cada peça vive:

| Modo | Driver | Executor | Cluster manager |
|---|---|---|---|
| Local | uma JVM única, tipo um laptop ou nó único | mesma JVM do driver | mesmo host [35] |
| Standalone | qualquer nó do cluster | cada nó sobe a própria JVM de executor | alocado arbitrariamente em qualquer host |
| YARN (client) | no cliente, fora do cluster | container do NodeManager do YARN | Resource Manager trabalha com o Application Master para alocar containers nos NodeManagers |
| YARN (cluster) | junto do Application Master do YARN | igual ao client mode | igual ao client mode |
| Kubernetes | em um pod | cada worker roda no próprio pod | Kubernetes Master |

Essa tabela é o único lugar dos quatro capítulos onde client mode e cluster mode aparecem lado a lado, e a diferença entre eles fica implícita na primeira coluna: **onde o driver roda**. O texto nunca diz isso em palavras.

**Dado distribuído e partições.** O dado físico está distribuído no storage como **partições**, em HDFS ou storage de nuvem. O Spark trata cada partição como abstração lógica de alto nível, um DataFrame em memória [34]. Cada executor recebe preferencialmente a task que exige ler a partição mais próxima na rede, o que é **localidade de dado**, e o texto ressalva que nem sempre é possível. O particionamento é o que permite paralelismo eficiente: cada core de executor recebe a própria partição, minimizando banda de rede.

A **Figura 1-5** mostra o modelo lógico sobre storage distribuído: faixas de *Data Partitions* apoiadas em arquivos no S3, Azure Blob ou HDFS. A **Figura 1-6** mostra três executores, cada um com quatro cores e quatro partições, um para um. O texto do exemplo é menos rígido que o desenho e diz que cada executor recebe **uma ou mais** partições para ler na memória.

Os dois exemplos em Python: `spark.read.text("path_to_large_text_file").repartition(8)` e `spark.range(0, 10000, 1, 8)`, ambos verificados com `getNumPartitions()`, ambos imprimindo 8. O tuning fica para os capítulos 3 e 7, que discutem como ajustar o número de partições ao número de cores por executor.

#### A experiência do desenvolvedor

O que o Spark 2.x buscou foi **unificar e simplificar limitando o número de conceitos** que o desenvolvedor precisa segurar na cabeça. As APIs de abstração de alto nível são construções de linguagem específica de domínio, e a frase que resume a proposta: você expressa **o que** quer computar, não **como**, e deixa o Spark decidir o melhor jeito.

A seção nomeia o público antes de descrevê-lo: engenheiro de dados, cientista de dados e engenheiro de machine learning, com a ressalva de que em startup e time pequeno a mesma pessoa acumula ciência e engenharia de dados.

**Cientista de dados.** Precisa limpar, explorar para achar padrão e construir modelo. Costuma dominar SQL, NumPy, pandas, R e Python, e trabalha de forma iterativa, interativa, ad hoc ou experimental. O Spark serve com MLlib (estimadores, transformadores e featurizadores de alto nível) e com Spark SQL e o shell para exploração ad hoc. Dois marcos citados: o **Project Hydrogen** trouxe um gang scheduler no Spark 2.4 para as necessidades de tolerância a falhas do treino distribuído de deep learning, e o **Spark 3.0** trouxe coleta de recurso de GPU nos modos standalone, YARN e Kubernetes.

**Engenheiro de dados.** Constrói o pipeline que transforma dado bruto e sujo em dado consumível, e integra o modelo com o resto (aplicação web, Kafka, pipeline maior). Usa Spark porque ele paraleliza e **esconde a complexidade de distribuição e tolerância a falhas**, o que libera foco para as APIs de DataFrame e as consultas em DSL. Nessa seção o capítulo chama o modelo de streaming do 2.x por outro nome, **continuous applications**, e remete ao capítulo 8: é o terceiro rótulo para a mesma família no mesmo capítulo, junto de *Continuous Streaming model* e *Structured Streaming* [39]. Os ganhos de performance do 2.x e do 3.0 são creditados ao **Catalyst** para SQL e ao **Tungsten** para geração de código compacta, e o engenheiro pode escolher entre RDD, DataFrame e Dataset conforme a tarefa.

Casos de uso listados: processar em paralelo grandes conjuntos distribuídos pelo cluster; consulta ad hoc ou interativa para explorar e visualizar; construir, treinar e avaliar modelos com MLlib; implementar pipelines ponta a ponta a partir de vários streams; analisar grafos e redes sociais.

#### Comunidade

Mais de **600 grupos de meetup** no mundo, com perto de meio milhão de membros. O Spark + AI Summit é a maior conferência dedicada ao tema, e o texto afirma que toda semana alguém no mundo dá uma palestra ou publica um post sobre construir pipeline com Spark. Desde o 1.0, em 2014, muitas releases, a mais recente sendo o **3.0 em 2020**; o livro cobre 2.x e 3.0, e a maior parte do código foi testada com o **3.0-preview2** [31].

Os números do GitHub não são medição do autor: saem da captura da Figura 1-7, que mostra 26.991 commits, 118 releases, 1.488 contribuidores, 21,5 mil forks, 25,8 mil estrelas e licença Apache-2.0. O texto arredonda para perto de 1500 contribuidores, mais de 100 releases, 21 mil forks e cerca de 27 mil commits.

#### O que o Damji resume no fim do capítulo 1

Nada. Este é o único dos quatro capítulos sem seção de resumo: ele termina anunciando que o próximo mostra como subir o Spark em três passos. A ausência combina com a estratégia do capítulo, que argumenta a partir do problema em vez de enumerar propriedades, mas obriga quem lê a montar a própria síntese.

#### O que ficou marcado em Damji 1

Dúvidas [27] a [40]. Vocabulário aberto: DAG, Tungsten, whole-stage code generation, Catalyst, gang scheduler, embaraçosamente paralelo, afinidade de rack, semântica de dado atrasado, bytecode, modo de manutenção, DSL.

---

### Damji, capítulo 2

*Downloading Apache Spark and Getting Started*, 26 páginas. Estruturado em três passos (baixar, usar o shell, entender os conceitos da aplicação) e fechado com uma aplicação standalone completa. É o capítulo mais denso por página dos quatro: os cinco termos, o par transformação/ação e a distinção narrow/wide são o vocabulário que sustenta tudo o que vem depois. Também é o que concentra mais fragilidade, e a razão é a mesma: é o único que mostra código rodando de ponta a ponta, e código rodando é o que dá para conferir.

A bibliografia pediu o início do capítulo. Li as 26 páginas inteiras, por dois motivos: o vocabulário de execução que a aula usa só aparece do Passo 3 em diante, e as duas afirmações mais frágeis de toda a leitura estão na última seção, a de que a saída em Scala é igual à saída em Python [44] e a de que há paridade de assinatura entre as duas linguagens [51].

Enquadramento inicial, e ele importa: o capítulo inteiro usa **modo local**, com tudo em uma máquina, porque o ciclo de feedback é curto e serve para prototipar com dado pequeno. Diz explicitamente que **modo local não serve para dado grande nem para trabalho de verdade**, e que aí se usa YARN ou Kubernetes. Também registra que o shell só existe para Scala, Python e R, embora a aplicação possa ser escrita em qualquer linguagem suportada, Java incluído [49].

#### Passo 1: baixar

O livro assume Linux ou macOS e escreve todos os comandos nesse dialeto. Escolher *Pre-built for Apache Hadoop 2.7* e baixar [18]. O arquivo é `spark-3.0.0-preview2-bin-hadoop2.7.tgz`, que traz os binários do Hadoop necessários para rodar local. Extração com `tar -xf spark-3.0.0-preview2-bin-hadoop2.7.tgz`, seguida de `cd` para o diretório criado. Alternativa: escolher a versão de Hadoop que casa com uma instalação existente. Compilar do fonte está fora do escopo. Uma nota admite que, no fechamento do livro, o 3.0 ainda estava em preview. A captura da página de download traz ainda a nota de que o Spark vem pré-compilado com Scala 2.11, exceto o 2.4.2 [45].

Para quem só usa Python, desde o **Spark 2.2** dá para instalar o PySpark do PyPI com `pip install pyspark`, sem arrastar as bibliotecas de Scala, Java e R, o que deixa o binário menor. Dependências extras: `pip install pyspark[sql,ml,mllib]`, ou só `pyspark[sql]`.

Requisitos declarados: **Java 8 ou superior** com `JAVA_HOME` setado [41]. Para R em modo interpretado, instalar R e rodar `sparkR`; para computação distribuída em R, existe também o `sparklyr`, da comunidade.

O `ls` da distribuição traz quinze itens: `LICENSE`, `NOTICE`, `R`, `README.md`, `RELEASE`, `bin`, `conf`, `data`, `examples`, `jars`, `kubernetes`, `licenses`, `python`, `sbin`, `yarn`. O capítulo explica seis. Dois dos silenciados voltam depois sem apresentação: `conf`, onde ele manda editar o `log4j.properties`, e `jars`, para onde o comando de execução da versão Scala aponta.

| Item | Conteúdo |
|---|---|
| `README.md` | instruções de uso dos shells, build do fonte, exemplos standalone, links de documentação e como contribuir |
| `bin` | a maior parte dos scripts de interação, incluindo os quatro shells (`spark-sql`, `pyspark`, `spark-shell`, `sparkR`), o `spark-submit` e o script que constrói e publica imagens Docker para Kubernetes |
| `sbin` | scripts administrativos para subir e derrubar componentes nos vários modos de deployment; remete à Tabela 1-1 do capítulo 1 |
| `kubernetes` | desde o **Spark 2.4**, Dockerfiles para criar imagens da distribuição em cluster Kubernetes, mais instruções de como construir a distribuição antes das imagens |
| `data` | arquivos `.txt` que servem de entrada para MLlib, Structured Streaming e GraphX |
| `examples` | exemplos em Java, Python, R e Scala |

#### Passo 2: o shell

Quatro interpretadores: `pyspark`, `spark-shell`, `spark-sql` e `sparkR`. Foram estendidos para conectar ao cluster e carregar dado distribuído na memória dos workers. O argumento é que servem tanto para gigabytes quanto para dado pequeno, e são o caminho mais rápido para aprender.

Do output de subida, o que vale reter:

- `pyspark` roda com Python 3.7.3 no exemplo, mostra a versão `3.0.0-preview2` e informa que a `SparkSession` está disponível como `spark`;
- `spark-shell` mostra a Web UI do Spark context em `http://10.0.1.7:4040`, informa que o Spark context está disponível como `sc` com `master = local[*]`, que a sessão está disponível como `spark`, e usa Scala 2.12.10 sobre Java 1.8.0.

No Damji 2 o `local[*]` aparece uma vez só, no banner do `spark-shell`: o do `pyspark` não traz a linha do Spark context. Nunca no texto corrido, aqui nem no Luu 2. A leitura de que isso é acidental é minha.

O exemplo de interação é minúsculo e proposital: ler o `README.md` da distribuição com `spark.read.text("../README.md")`, caminho relativo porque o shell foi aberto de dentro de `bin`; mostrar dez linhas sem truncar com `show(10, false)` (o flag `truncate` é `true` por default) e contar as linhas com `count()`, que devolve 109. No Scala o resultado sai identificado (`res2: Long = 109`); no Python sai cru (`109`). Depois repete tudo em `pyspark`, com `truncate=False`, para provar a **paridade de sintaxe e assinatura entre Scala e Python** [51], apresentada como uma das melhorias duradouras desde o 1.x. Sair de qualquer shell com Ctrl-D.

A nota mais importante da seção: o exemplo usou as **APIs estruturadas**, não RDD, e desde o Spark 2.x os RDDs estão relegados a API de baixo nível. Toda computação expressa em API estruturada é decomposta em operações de RDD otimizadas e geradas, convertidas em bytecode Scala para as JVMs dos executores, e esse RDD gerado **não é acessível ao usuário** nem é o mesmo que a API pública de RDD.

#### Passo 3: os cinco termos

O vocabulário que o resto do curso vai assumir como conhecido:

| Termo | Definição do texto |
|---|---|
| **Application** | programa do usuário construído sobre o Spark, com um driver e executores no cluster |
| **`SparkSession`** | objeto que dá o ponto de entrada às funcionalidades. No shell, o driver instancia por você; em aplicação, você cria |
| **Job** | computação paralela composta de várias tasks, criada em resposta a **uma ação** (por exemplo `save()`, `collect()`) |
| **Stage** | subdivisão do job em conjuntos menores de tasks que dependem uns dos outros |
| **Task** | unidade única de trabalho enviada a um executor |

O detalhamento acrescenta a mecânica:

- **Aplicação e sessão.** No núcleo de toda aplicação está o driver, que cria a `SparkSession`. No shell, o driver é parte do shell. Rodando local, tudo acontece em uma JVM única, mas o mesmo shell pode analisar dado em paralelo em um cluster, e `spark-shell --help` ou `pyspark --help` mostram como conectar ao cluster manager.
- **Jobs.** Em sessão interativa de shell, o driver converte a aplicação em **um ou mais jobs** e transforma cada job em um **DAG**, que é o plano de execução do Spark [52]. Cada nó do DAG pode ser um ou vários stages.
- **Stages.** São criados conforme o que pode ser feito em série ou em paralelo. Nem toda operação cabe em um stage só. A frase que importa: os stages costumam ser delimitados nas **fronteiras de computação do operador**, onde se decide a transferência de dado entre executores.
- **Tasks.** Cada stage é composto de tasks, distribuídas pelos executores; **cada task mapeia para um core e trabalha sobre uma partição**. Um executor com 16 cores pode ter 16 ou mais tasks trabalhando sobre 16 ou mais partições em paralelo [42].

As figuras que acompanham:

| Figura | O que desenha |
|---|---|
| 2-2 | a aplicação com driver e `SparkSession` empilhados de um lado, dois nós de cluster do outro, cada nó com quatro executores e um marcado `T`, legendado *Task per Core*. As setas saem e voltam do driver: tudo passa por ele |
| 2-3 | um driver, várias caixas *Job* |
| 2-4 | a mesma cadeia, com um job apontando para uma fila de stages |
| 2-5 | a cadeia completa: driver, jobs, stages, tasks |
| 2-6 | duas faixas. Em cima, `DF -> T -> DF -> T -> DF`, todas tracejadas, nada executa. Embaixo, `DF -> T -> DF -> A -> DF`, e o último quadro fica sólido: a ação materializa |
| 2-7 | narrow com setas um para um entre partições de entrada e saída, wide com um emaranhado de todas para todas |

A 2-6 é a que eu guardaria: o tracejado é a linhagem, o sólido é o dado.

#### Transformações, ações e avaliação preguiçosa

O capítulo divide as operações em dois tipos e explica a consequência de cada um.

**Transformação.** Transforma um DataFrame em outro DataFrame **sem alterar o original**, o que dá a propriedade de imutabilidade. `select()` ou `filter()` não mudam o DataFrame de origem: devolvem o resultado como novo DataFrame.

**Avaliação preguiçosa.** Toda transformação é avaliada de forma preguiçosa: o resultado não é computado na hora, e sim registrado como **linhagem**. A linhagem registrada permite ao Spark, mais tarde, reordenar certas transformações, fundi-las ou organizá-las em stages para execução mais eficiente. A execução fica adiada até que uma ação seja invocada ou o dado seja tocado, isto é, lido do disco ou escrito nele.

**Ação.** Dispara a avaliação preguiçosa de todas as transformações registradas.

O argumento que amarra os dois conceitos: a avaliação preguiçosa permite otimizar espiando a cadeia encadeada, e a linhagem somada à imutabilidade dá **tolerância a falhas**, porque o Spark consegue reproduzir o estado original apenas reexecutando a linhagem registrada.

| Transformações citadas | Ações citadas |
|---|---|
| `orderBy()`, `groupBy()`, `filter()`, `select()`, `join()` | `show()`, `take()`, `count()`, `collect()`, `save()` |

O exemplo tem duas transformações (`read()` [50] e `filter()`) e uma ação (`count()`), em Python e em Scala, e o ponto demonstrado é que **nada acontece** até `filtered.count()` rodar, devolvendo 20 linhas com a palavra Spark no `README.md`. O exemplo de `filter` precisa em Scala de `import org.apache.spark.sql.functions._` e de `col("value").contains("Spark")`, enquanto em Python basta `strings.value.contains("Spark")`.

#### Narrow e wide

A distinção é apresentada como consequência direta da avaliação preguiçosa: como o Spark inspeciona a query inteira antes de executar, ele pode fundir ou encadear operações dentro de um stage, ou quebrá-las em stages ao identificar quais operações exigem **shuffle**, isto é, troca de dado pelo cluster.

- **Narrow.** Qualquer transformação em que uma partição de saída é computada a partir de **uma única** partição de entrada. `filter()` e `contains()` são narrow: operam sobre uma partição e produzem a saída sem troca de dado.
- **Wide.** `groupBy()` e `orderBy()` são wide: dado de outras partições é lido, combinado e escrito em disco. O exemplo explica bem: ordenar o DataFrame filtrado com `.orderBy()` ordena cada partição localmente, mas ainda exige forçar um shuffle das partições de cada executor pelo cluster para ordenar todos os registros. Transformação wide precisa da saída de outras partições para computar a agregação final.

#### A Spark UI

Interface gráfica para inspecionar a decomposição da aplicação em jobs, stages e tasks. Dependendo de como o Spark foi implantado, o driver sobe uma UI web, por default na **porta 4040**, e em modo local ela fica em `http://<localhost>:4040` [54]. O que dá para ver: lista de stages e tasks do escalonador, resumo de tamanhos de RDD e uso de memória, informação de ambiente, informação dos executores em execução, e todas as consultas Spark SQL.

O exercício feito: olhar como o exemplo em Python vira job, stage e task. Clicando em *DAG Visualization*, aparece que o driver criou **um job e um stage**. O texto chama atenção para o que **não** aparece: não há nó `Exchange`, porque com um stage só não existe troca de dado entre executores. As operações individuais do stage aparecem em caixas azuis, e o stage 0 tem uma task só; havendo várias, elas rodariam em paralelo.

Os números concretos, que servem de gabarito para comparar com uma execução própria. Figura 2-8, página *Details for Job 0*: status `SUCCEEDED`, *Associated SQL Query* `0`, *Completed Stages* `1`. O DAG do stage tem três caixas azuis em sequência: `BatchScan`, `WholeStageCodegen`, `mapPartitionsInternal`. Figura 2-9, *Details for Stage 0 (Attempt 0)*: *Total Time Across All Tasks* `0.2 s`, *Locality Level Summary* `Process local: 1`, *Associated Job Ids* `0`, e as caixas abertas em quatro RDDs, um `DataSourceRDD` de id 0 seguido de três `MapPartitionsRDD` de ids 1, 2 e 3, todos rotulados `count at NativeMethodAccessorImpl.java:0`. As abas visíveis nas duas capturas são Jobs, Stages, Storage, Environment, Executors e SQL. O detalhamento da UI fica para o capítulo 7.

Um boxe à parte recomenda a **Databricks Community Edition** como alternativa gratuita ao modo local, com tutoriais e exemplos, onde dá para escrever notebooks em Python, R, Scala ou SQL e importar notebooks de terceiros, inclusive Jupyter. Os notebooks do livro estão no repositório do GitHub dele.

#### A primeira aplicação standalone

A distribuição vem com exemplos por componente, rodáveis com `bin/run-example <class> [params]`. O exemplo dado é `./bin/run-example JavaWordCount README.md`, e o capítulo brinca que contar palavras é o "Hello, World" da computação distribuída.

Como word count já virou clichê, o exemplo real é outro: contar M&Ms. O dado tem mais de 100 mil linhas no formato `<state, mnm_color, count>` [55], e o programa agrega as contagens por estado e cor.

A versão em Python começa com a guarda `if len(sys.argv) != 2`, que imprime `Usage: mnmcount <file>` em `stderr` e sai com `sys.exit(-1)`. Monta a `SparkSession` pelo builder com `appName("PythonMnMCount")` (com o lembrete de que só pode existir **uma `SparkSession` por JVM**), lê o CSV com `header` e `inferSchema` ligados, e encadeia `select("State","Color","Count")`, `groupBy("State","Color")`, `sum("Count")` e `orderBy(...)` em ordem decrescente. Mostra com `show(n=60, truncate=False)`, imprime a contagem total de linhas, repete o recorte da Califórnia com `where` e `show(n=10, truncate=False)`, e encerra com `spark.stop()`. O arquivo se chama `mnmcount.py`.

Os comentários do próprio código carregam as observações que importam: **não se usa RDD em lugar nenhum**, as funções podem ser encadeadas porque devolvem o mesmo tipo de objeto, e `show()` é a ação que dispara toda a query acima. Uma nota reforça o argumento central: a API de DataFrame se lê como consulta DSL de alto nível, e você diz ao Spark **o que** fazer, não **como**, ao contrário da API de RDD.

Execução: `$SPARK_HOME/bin/spark-submit mnmcount.py data/mnm_dataset.csv` [43] [46], com `SPARK_HOME` apontando para a raiz da instalação. Para não poluir o console com mensagens INFO, copiar `log4j.properties.template` para `log4j.properties` e setar `log4j.rootCategory=WARN` em `conf/`.

A saída tem 60 linhas, que são 10 estados (AZ, CA, CO, NM, NV, OR, TX, UT, WA, WY) por 6 cores (Yellow, Green, Brown, Red, Orange, Blue). O capítulo nunca faz essa conta. Topo: `CA Yellow 100956`. Base: `WY Brown 86110`. Depois `Total Rows = 60`. O recorte da Califórnia sai em `sum(Count)`: Yellow 100956, Brown 95762, Green 93505, Red 91527, Orange 90311, Blue 89123.

A versão em Scala do mesmo programa vem em seguida, com as diferenças de sintaxe esperadas (`desc(...)` em vez do parâmetro `ascending`, `col("State") === "CA"` em vez de `mnm_df.State == "CA"`), sob o argumento de que a paridade entre linguagens é bem preservada [51]. Usa `MnMCount` como `appName`, checa `if (args.length < 1)` e sai com `sys.exit(1)`, mas coloca essa checagem **depois** de `getOrCreate()`: rodar sem argumento sobe uma `SparkSession` antes de morrer [53]. O programa impresso também não compila, porque declara `caCountMnNDF` e chama `caCountMnMDF` [47].

O build usa **sbt**: o `build.sbt` declara nome, versão, `scalaVersion := "2.12.10"` e as dependências `spark-core` e `spark-sql` na versão `3.0.0-preview2`. Com JDK e sbt instalados e `JAVA_HOME` e `SPARK_HOME` setados, `sbt clean package` gera o jar. O log fixa `sbt.version` em `1.2.8`, empacota em `target/scala-2.12/main-scala-chapter2_2.12-1.0.jar` e fecha com `[success] Total time: 6 s, completed Jan 11, 2020`. O `spark-submit --class` da linha seguinte aponta para `jars/main-scala-chapter2_2.12-1.0.jar`, diretório diferente do que o sbt acabou de imprimir, e o passo de cópia nunca é mencionado [48].

A saída Scala impressa traz só o recorte da Califórnia, sob o cabeçalho `|State| Color|Total|`: Yellow 1807, Green 1723, Brown 1718, Orange 1657, Red 1656, Blue 1603. O log acima dela mostra `Job 4 finished: show at MnMcount.scala ... took 0.264579 s`, ou seja, a aplicação gerou pelo menos cinco jobs. O texto afirma que a saída é a mesma da versão Python [44]. Sobre Python, esclarece que não há passo de build porque a linguagem é interpretada, e remete ao guia oficial para quem for construir com Maven em Java.

#### O que o Damji resume no fim do capítulo 2

Três passos para começar: baixar o framework, se familiarizar com o shell Scala ou PySpark, e assimilar os conceitos e termos de aplicação. Mais a visão geral de transformações e ações e a introdução à Spark UI para examinar jobs, stages e tasks.

#### O que ficou marcado em Damji 2

Dúvidas [18], [31] e [41] a [55]. Vocabulário aberto sem definição suficiente: `Exchange`, `BatchScan`, `WholeStageCodegen`, `mapPartitionsInternal` e `Process local`, todos nomes que a UI cospe nas figuras 2-8 e 2-9 sem que o capítulo diga o que são; mais `local[*]`, shuffle e bytecode.

---

## Onde eu não acreditei

Lista das afirmações que soaram propaganda, exageradas, contraditórias ou datadas. A voz aqui é minha, não a dos livros: onde a crítica se apoia em informação de fora dos quatro capítulos, ela está dita em linha. Cada item é matéria-prima de pergunta para a aula. Agrupada por capítulo, porque o padrão de erro de cada livro é diferente.

### Dúvidas: Luu, capítulo 1

| # | Afirmação | Por que ficou marcada |
|---|---|---|
| 1 | 100x mais rápido que o MapReduce | O número é atribuído ao site do próprio projeto, sem condição declarada: qual carga, qual dado, qual cluster. A evidência oferecida logo abaixo, o GraySort de 2014, mede outra coisa: 3x mais rápido com 10x menos recursos. Ordenação não é carga iterativa, e 100x é a figura clássica de carga iterativa em memória, o que explica por que os dois números não conversam. Os dois são autorreportados por parte interessada, um pelo site do projeto e outro pela submissão da Databricks, e o capítulo repassa os dois sem qualificar |
| 2 | Mais de 200 mil meetups de Apache Spark | Número implausível na unidade em que está escrito. Damji 1 fala em 600 grupos de meetup e perto de meio milhão de membros. Provavelmente o Luu quis dizer membros, não eventos. O número vizinho, na mesma frase, tem o mesmo defeito: "o número de contribuidores cresceu em mais de mil" não diz de quanto para quanto, nem até quando, nem de onde saiu |
| 3 | "Apache Meso" como gerenciador de recursos, numa lista de dois | Erro de grafia (Mesos). E a lista é curta demais: Kubernetes não aparece uma vez sequer no capítulo, apesar de já ser suportado em 2021 |
| 4 | "master/slave" nomeando dois pares diferentes | Na seção de gerência de recursos, os dois componentes são "cluster manager e worker", e a frase seguinte diz que o master sabe onde os slaves estão: master é o cluster manager. Três páginas depois, "o Spark usa arquitetura master/slave, com o driver como master e o executor como slave". Master e slave nomeiam dois pares distintos, sem nenhum aviso, e worker nunca é definido. A Figura 1-1 piora: coloca o executor dentro de uma caixa chamada Worker, coisa que o texto nunca diz. Esta, e não a grafia de "Meso", é o que estraga a seção de arquitetura |
| 5 | "Segundo a pesquisa Spark de 2021, o Spark SQL foi o componente que mais cresceu" | Não há citação da pesquisa, nem de quem a conduziu, nem da métrica. Livro de 2021 citando pesquisa de 2021 sem publicador, num capítulo onde os outros números vêm todos da Databricks. E cresceu em quê: uso, downloads, linhas de código, tickets? |
| 6 | O DStream apresentado como a abstração principal para streaming | Escrito no presente, e só no parágrafo seguinte o texto apresenta o Structured Streaming como motor novo da versão 2.1, quatro releases menores antes do 3.0 que o livro cobre. A confusão é de nomenclatura, não só de tempo verbal: a Figura 1-3 rotula a caixa como **Spark Streaming**, a seção se chama **Spark Structured Streaming** e o corpo dela define **DStream**. Três nomes para a mesma vaga. Damji 1 é categórico ao dizer que o modelo novo tornou o DStreams obsoleto. Herança de edição anterior |
| 7 | Kafka, Flume, Kinesis, Twitter, HDFS e socket TCP como fontes do Structured Streaming | Essa é a lista de fontes da era DStream, não do motor novo. Flume e Twitter nunca foram fontes nativas do Structured Streaming, e o Twitter já tinha saído do Spark na série 2.x. A lista aparece no mesmo parágrafo em que o texto ainda trata DStream como abstração principal, o que reforça a hipótese de bloco herdado |
| 8 | Garantia de exactly-once ponta a ponta | Apresentada como propriedade do motor, sem uma palavra de condição: exige fonte reprocessável e sink idempotente ou transacional. Ponta a ponta até que ponta, exatamente? |
| 9 | Spark 3.0 é cerca de 2x mais rápido que o 2.4, e os dois 60% | Benchmark TPC-DS de 30 TB feito pela **Databricks**, ou seja, pela empresa que vende a plataforma, sem configuração de cluster e sem dizer quais queries entraram na média. A mesma seção emite dois números de 60% da mesma fonte: 60% das melhorias foram para SQL e Core, e o DPP acelera 60% das queries. Melhoria contada como o quê: ticket, commit, linha de código, feature? |
| 10 | "O maior cluster Spark do mundo tem mais de 8000 máquinas" | Citado do FAQ oficial, que é página sem data e notoriamente parada. Um número de "maior do mundo" sem ano e sem empresa não tem como ser verificado nem refutado |
| 11 | O word count usa `sc` | Todo o texto de arquitetura diz que o driver trabalha através de um componente chamado `SparkSession`, e a Figura 1-1 desenha a `SparkSession` dentro do driver. Aí o único código do capítulo abre com `sc.textFile(...)`. O capítulo nunca apresenta `sc`, nunca escreve a palavra `SparkContext` e nunca explica a relação entre as duas coisas. Quem lê na ordem encontra o exemplo canônico apoiado numa variável que não existe no texto |
| 12 | "As APIs de RDD são expostas a Scala, Java e Python" | Na seção de facilidade de uso, o capítulo diz que os mais de 80 operadores estão em Scala, Java, Python e R; no resumo final, diz que a aplicação pode ser escrita nas quatro. Só na seção do Spark Core o R some da lista. Ou é descuido, ou é um limite real da API de RDD que o capítulo não explica |
| 13 | Koalas com 80% de cobertura das APIs do pandas | Cobertura de API é métrica frouxa, e o capítulo dá o número redondo sem hedge: 80% de quais APIs, contadas como, e cobertura significa existir a função ou reproduzir o comportamento? |
| 14 | "A popularidade do Spark ajudou o Scala a virar linguagem mainstream" | Afirmação sem evidência e discutível no mérito. O próprio capítulo seguinte manda aprender só o Scala mínimo, e o Damji trata a paridade entre linguagens como conquista justamente porque ninguém quer aprender Scala |
| 15 | "Rodar diferentes conjuntos de API sobre o mesmo dado sem escrever o intermediário em storage" | Dito sem ressalva, como propriedade do motor unificado. Duas páginas depois o mesmo capítulo apresenta o **data shuffling** como uma das responsabilidades centrais do Core, e shuffle escreve em disco. A frase só é verdadeira para o intermediário entre etapas de um pipeline, não para o intermediário de execução |
| 16 | Delta Lake, Koalas e MLflow como "o ecossistema em volta do Spark" | Os três são projetos da Databricks. Cada um é apresentado como open source, o que é verdade, e nenhum é apresentado como de fornecedor único, o que também é informação. Uma seção de ecossistema com três projetos da mesma empresa descreve uma estratégia comercial, não um ecossistema |
| 17 | "Técnicas de otimização just-in-time" como o destaque do Spark 3.0 | O termo abre o capítulo, carrega toda a promessa da release e nunca mais aparece. A seção do 3.0 fala de AQE, DPP e scheduler ciente de GPU, e nenhuma das três é descrita como just-in-time. Quem lê termina o capítulo sem saber o que era o destaque anunciado na primeira página |

### Dúvidas: Luu, capítulo 2

| # | Afirmação | Por que ficou marcada |
|---|---|---|
| 18 | Escolher o pacote "com a versão mais nova do Hadoop", que é o `hadoop2.7` (também em Damji 2) | O Luu manda escolher a versão mais nova de Hadoop e, na figura da mesma página, o formulário está em `Pre-built for Apache Hadoop 2.7` e o arquivo é `spark-3.1.1-bin-hadoop2.7.tgz`. Em março de 2021 a página do 3.1.1 já oferecia `Hadoop 3.2 and later`: a instrução briga com o próprio exemplo dentro da mesma figura. O Damji 2 repete o mesmo pacote, mas ele apenas segue a escolha padrão da página em dezembro de 2019 e nunca chama o 2.7 de mais recente. Instrução datada nos dois, erro de fato só em um. Primeira coisa a conferir antes de montar ambiente |
| 19 | A Community Edition permite criar múltiplos clusters simultaneamente | O capítulo se contradiz três vezes em seis páginas: diz que, **por ser gratuita**, a conta permite criar vários clusters ao mesmo tempo (o "por ser gratuita" já é não sequitur, gratuidade é razão para permitir menos), depois diz que tentar criar um segundo não funciona, e por fim diz que a CE só pode ter um cluster por vez, usando isso para explicar por que o campo de cluster já vem preenchido no formulário de notebook. A terceira é a que casa com o produto |
| 20 | Publicar notebook gera URL que qualquer pessoa no mundo pode abrir | Apresentado como conveniência, e o resumo do capítulo repete como vantagem de venda. É uma URL **sem autenticação**: quem tiver o link lê o notebook e importa para o próprio workspace, código e saída de célula juntos. Nenhuma palavra sobre dado sensível em saída de célula, sobre credencial deixada em código, sobre como despublicar ou sobre controle de acesso |
| 21 | "Este capítulo descreve as três opções comuns, incluindo submeter uma aplicação pela linha de comando" | Promessa não cumprida. `spark-submit` não aparece em nenhuma das 28 páginas: as duas ocorrências da palavra "submit" são essa frase de abertura e a descrição do diretório `bin`. O capítulo entrega shell e Databricks, e a terceira opção some. Como é justamente a forma de rodar aplicação de verdade, a lacuna operacional do livro é maior do que o índice sugere |
| 22 | "Há um jeito de construir o binário a partir do código-fonte; as instruções estão mais adiante neste capítulo" | Segunda promessa não cumprida, feita na página 2. A seção final ensina a **baixar** o fonte e remete à página `developer-tools` para importar em IDE. Não há `./build/mvn`, não há `sbt`, não há uma linha sobre build |
| 23 | `git clone git://github.com/apache/spark.git` | O comando não funciona mais. O GitHub desligou o protocolo `git://`, não criptografado e não autenticado, em março de 2022, e a conexão é recusada. Hoje é `git clone https://github.com/apache/spark.git`. Passo datado que quebra na primeira tentativa |
| 24 | "Para rodar o Spark localmente, tudo que é preciso é ter Java instalado" | Falso dentro do próprio capítulo. Duas páginas depois vem a nota de que Java 11 ou superior é preferido para o shell Scala, e a seguinte diz que o shell Python exige Python 3.7.x ou superior. Três requisitos em três páginas, apresentados como se fossem um. E nenhum deles é o que importa: nenhum dos dois livros declara a versão **máxima** de Java suportada |
| 25 | "Clique no ícone Clusters" | O texto e a própria captura discordam: a Figura 2-24 tem "Compute" no cabeçalho, com abas *All-Purpose Clusters* e *Job Clusters*. A interface já tinha sido renomeada quando o livro saiu, e o autor manteve o nome antigo na instrução. Prova concreta do problema geral do capítulo: passo a passo de interface que envelhece mais rápido que a impressão |
| 26 | A barra de navegação da Spark UI tem seis abas | A Figura 2-14, logo acima, mostra cinco: não há SQL. A aba SQL só aparece depois que uma consulta roda, e o capítulo não explica isso, então quem abrir a UI num shell recém-aberto vai procurar uma aba que não está lá. Descuido do mesmo tipo três parágrafos abaixo: o texto que apresenta a Tabela 2-3 lista quatro áreas da aba Environment e a tabela traz seis |

### Dúvidas: Damji, capítulo 1

| # | Afirmação | Por que ficou marcada |
|---|---|---|
| 27 | "Hoje, muitas ordens de grandeza mais rápido" | O capítulo cita corretamente os artigos iniciais (10 a 20 vezes em certos jobs) e, na frase seguinte, salta para "muitas ordens de grandeza" sem carga, sem condição e sem medição. Ir de 20x para 1000x ou mais no mesmo parágrafo. No PDF a expressão é um link, então existe um destino, mas o texto não diz qual. Mesmo defeito do 100x do Luu, com a agravante de a unidade ser vaga: ordem de grandeza a partir de que base? |
| 28 | No shell a `SparkSession` é acessível pela variável global `spark` **ou** `sc` | Confunde as duas coisas. Não é ambiguidade de leitura, é erro do livro, e o próprio livro se desmente no capítulo 2, cujo banner separa os dois objetos: `sc` é o `SparkContext` e `spark` é a `SparkSession` |
| 29 | Quatro cluster managers suportados, com o Mesos entre eles | Não é erro: em 2020 eram quatro mesmo. O problema é a forma. O original escreve "**Currently**, Spark supports four cluster managers", e "currently" sem data é frase que envelhece sem avisar. Como a lista é fechada e curta, qualquer mudança no ecossistema a invalida inteira. Primeira coisa a conferir no aprofundamento |
| 30 | "Na maioria dos modos de deployment, roda um único executor por nó" | Afirmação forte, sem qualificação, e que a própria Tabela 1-1 não sustenta: só a linha Standalone fala em uma JVM de executor por nó, e as linhas de YARN e Kubernetes não dizem nada sobre quantidade. Na prática depende de dimensionamento de container e da configuração do cluster manager, e o capítulo não toca em nenhum dos dois |
| 31 | O livro foi testado com o 3.0-preview2 (também em Damji 2) | Não é crítica, é aviso: nenhum output mostrado veio de release estável. O capítulo 1 admite mais que isso, escrevendo no futuro que "na época da publicação a comunidade já terá lançado o Spark 3.0". No capítulo 2 a preview está por toda parte: nos dois banners de shell, no retorno de `spark.version`, no cabeçalho das capturas da UI e nas dependências do `build.sbt`. Detalhe que confirma que os transcritos foram editados: os identificadores de resultado do shell Scala pulam de `res0` para `res2` e depois `res5`. As sessões mostradas são recortes, não execuções contínuas |
| 32 | "O mesmo trecho em Python, R ou Java gera bytecode idêntico e a mesma performance" | Afirmação forte, sem ressalva e sem fonte. Vale para expressão nativa de DataFrame, que vira o mesmo plano em qualquer linguagem; não vale para UDF em Python, que atravessa a fronteira JVM/Python. A ressalva ausente é justamente a que decide performance em PySpark, e o capítulo usa a afirmação como prova de que a escolha de linguagem é indiferente |
| 33 | "O Spark SQL é ANSI SQL:2003-compliant" | Conformidade com padrão é alegação verificável, e o capítulo não diz por quem foi verificada nem em que grau. Escrito como fato consumado, sem nota sobre modo estrito ou default |
| 34 | "O Spark trata cada partição como abstração lógica de alto nível, um DataFrame em memória" | Simplificação que engana. O DataFrame é a coleção distribuída inteira; a partição é um pedaço dela. Trocar os dois nomes atrapalha exatamente quem está montando o modelo mental de particionamento, que é o público da seção |
| 35 | A Tabela 1-1 diz que em modo local o cluster manager "roda no mesmo host" | Em modo local não existe cluster manager: o escalonamento acontece dentro do próprio processo. A célula foi preenchida por simetria de tabela e inventa uma peça. É a mesma tabela que o resto do curso vai usar como referência de modos |
| 36 | Hadoop Common, MapReduce, HDFS e YARN como os módulos do que foi doado em abril de 2006 | O YARN só chegou com o Hadoop 2, anos depois: a frase junta a data da doação com uma composição de módulos posterior. No mesmo parágrafo o livro escreve "Hadoop File System" onde o nome é Hadoop **Distributed** File System, e credita ao artigo do GFS o blueprint do MapReduce, que é outro artigo |
| 37 | "O prestigioso ACM Award" | Nenhum prêmio se chama assim. O capítulo não nomeia o prêmio, nem o comitê, nem o veículo de publicação. É apelo a autoridade com a autoridade não identificada |
| 38 | A Figura 1-3 desenha uma stack diferente da que o texto descreve | A figura chama a caixa de streaming de "Spark Streaming (Structured Streaming)", nome do modelo que o texto declara obsoleto duas páginas depois, e coloca o Spark SQL Engine dentro da fundação enquanto o texto lista Spark SQL como componente **separado** do motor central. Figura e texto não descrevem a mesma arquitetura |
| 39 | "Continuous Streaming model" no 2.0 e "continuous applications" no 2.x | Dois nomes em duas seções para coisas que podem não ser a mesma, nenhum definido, nenhum retomado. Conferir em que versão o processamento contínuo entrou de fato, e se é a mesma coisa que Structured Streaming |
| 40 | "Em aplicação 2.x você **pode** criar uma `SparkSession` por JVM" | O capítulo 1 escreve como conveniência e o capítulo 2 endurece para limite ("só pode existir uma por JVM"). Os dois usam a mesma frase base para dizer coisas diferentes, e nenhum diz o que acontece se você tentar criar duas |

### Dúvidas: Damji, capítulo 2

| # | Afirmação | Por que ficou marcada |
|---|---|---|
| 41 | "Java 8 ou superior" | O piso está certo para o 3.0, que suportava Java 8 e 11. O que engana é o "ou superior" sem teto: não existe JDK arbitrariamente novo que funcione, e o próprio banner do capítulo roda em Java 1.8.0. O Luu 2 preferir Java 11 não contradiz isso, só mostra que nenhum dos dois declara a faixa suportada, que é a única informação de que quem monta ambiente precisa |
| 42 | Um executor com 16 cores pode ter 16 ou mais tasks em paralelo | A frase se contradiz com a anterior: duas linhas acima o mesmo parágrafo afirma que cada task mapeia para um único core. Se isso vale, 16 cores comportam 16 tasks simultâneas, e o "ou mais" só é verdade contando tasks enfileiradas, que por definição não estão em paralelo. O parâmetro que fecha o argumento, `spark.task.cpus`, não vem do livro: é da documentação do Spark |
| 43 | "o script `submit-spark`", e "standalone mode" para a instalação local | Erro de grafia: o script é `spark-submit`, usado corretamente na linha de comando logo abaixo. Também chama a instalação local de "standalone mode" no início do Passo 3, quando o capítulo inteiro roda em **local mode**, e a Tabela 1-1 do capítulo 1 trata os dois como modos distintos |
| 44 | "A saída é a mesma da execução em Python" | Não é, e são quatro diferenças. Os **valores**: `CA Yellow` sai 100956 em Python e 1807 em Scala, razão de 55,9, e a mesma razão de 54 a 56 se repete nas seis cores. O **nome da coluna**: `sum(Count)` vira `Total`, coisa que `sum("Count")` sem alias não produz, ou seja, a saída Scala impressa não pode ter saído do código Scala impresso. A **ordem**: Brown troca com Green e Red troca com Orange. E a saída Scala mostra **só** o recorte da Califórnia, sem as 60 linhas nem o `Total Rows`. A razão constante indica dois datasets de tamanhos diferentes, não edição manual; extrapolada, a execução Scala soma cerca de 101.600, que é o "mais de 100 mil entradas" que o capítulo declara, enquanto a Python soma perto de 5,5 milhões. Como o capítulo nunca explica o que a coluna `Count` conta [55], não há como saber qual saída é a boa |
| 45 | A nota da página de download: "o Spark vem pré-compilado com Scala 2.11, exceto a versão 2.4.2" | O capítulo reproduz a captura sem comentário, e três páginas depois o próprio banner do `spark-shell` mostra Scala 2.12.10. A nota do site já estava errada para o 3.0 no momento da captura, e o livro a carrega para dentro sem corrigir |
| 46 | `mnn_dataset.csv` | A prosa manda baixar `mnn_dataset.csv` do GitHub; o comando de execução na mesma página usa `data/mnm_dataset.csv`, e é esse o nome real. Terceiro erro de digitação na mesma frase que já traz `submit-spark` |
| 47 | O código Scala do Example 2-2 não compila | Declara `val caCountMnNDF = mnmDF ...` e depois chama `caCountMnMDF.show(10)`. São dois identificadores diferentes. O programa impresso no livro não roda como está, o que também explica por que a saída Scala não pode ter vindo dele |
| 48 | O jar sai em um diretório e é executado de outro | O log do `sbt clean package` empacota em `target/scala-2.12/main-scala-chapter2_2.12-1.0.jar`; o `spark-submit` da linha seguinte aponta para `jars/main-scala-chapter2_2.12-1.0.jar`. O passo de cópia nunca é mencionado |
| 49 | "O shell só suporta Scala, Python e R" | Está na abertura do capítulo. Três páginas depois, o Passo 2 abre dizendo que o Spark vem com **quatro** interpretadores muito usados e lista `spark-sql` entre eles. Ou o `spark-sql` não conta como shell, e o capítulo devia dizer por quê, ou a frase de abertura está errada |
| 50 | `read()` classificado como transformação | O capítulo define avaliação preguiçosa como adiar a execução até que uma ação seja invocada **ou o dado seja tocado, lido do disco ou escrito nele**, e duas páginas depois chama `read()` de transformação. Pela definição do próprio capítulo, ler do disco é justamente o que quebra a preguiça. `read()` também não está na Tabela 2-1 |
| 51 | "Paridade de sintaxe e assinatura entre Scala e Python" | Afirmada duas vezes e desmentida por todos os exemplos do capítulo: `show(10, false)` contra `show(10, truncate=False)`; `col("value").contains(...)` com import extra contra `strings.value.contains(...)`; `desc("sum(Count)")` contra `ascending=False`; `col("State") === "CA"` contra `mnm_df.State == "CA"`. A versão Scala do M&M nem passa `truncate`. Paridade de capacidade, sim; de assinatura, não |
| 52 | "Em sessões interativas com shells, o driver converte a aplicação em um ou mais jobs" | Escopo indevido. A conversão em jobs, DAG, stages e tasks vale para qualquer aplicação, inclusive a que o próprio capítulo submete com `spark-submit` no fim, cujo log mostra `Job 4 finished`. Amarrar o mecanismo ao shell confunde quem lê na ordem |
| 53 | A ordem de execução do Example 2-2 | A versão Scala constrói a `SparkSession` com `getOrCreate()` **antes** de checar `args.length`: rodar sem argumento sobe uma sessão e só depois morre. A versão Python faz o contrário, checa `sys.argv` antes de tudo. Diferença de comportamento entre dois programas apresentados como equivalentes |
| 54 | `http://<localhost>:4040` | Os sinais de maior e menor estão impressos literalmente, como se `localhost` fosse um placeholder a substituir. Não é. E o banner do `spark-shell` mostrado três páginas antes traz um IP de rede local, `http://10.0.1.7:4040`, não `localhost`, sem uma palavra sobre quando é um e quando é o outro |
| 55 | O que a coluna `Count` conta | O capítulo descreve o arquivo como `<state, mnm_color, count>` com mais de 100 mil linhas, agrega `sum("Count")` e imprime os resultados, e nunca diz se `Count` é um saco de M&Ms, uma unidade ou o quê. Sem isso não dá para julgar se 100.956 amarelos na Califórnia é plausível, nem por que a saída Scala dá 1807. Também nunca menciona que as 60 linhas são 10 estados por 6 cores |

## Divergências entre os dois livros

Os pares de capítulos cobrem o mesmo assunto, então onde discordam é onde vale prestar atenção. Estas sete são as que mudam a conclusão, não só a redação.

| # | Assunto | Luu | Damji | Por que importa |
|---|---|---|---|---|
| 1 | Papel do RDD | abstração-chave que **todo** usuário deve aprender e usar bem; é o RDD que aparece no único exemplo de código do capítulo 1 | cap. 2: relegado a API de baixo nível desde o 2.x, e o exemplo completo não usa RDD e diz isso nos comentários. Mas o cap. 1 puxa para o outro lado: chama o RDD de abstração fundamental sobre a qual DataFrame e Dataset são construídos, e lista RDD, DataFrame e Dataset como três APIs entre as quais o engenheiro escolhe conforme a tarefa | Define por onde se começa a estudar. E a divergência não é só entre os dois livros: é interna ao Damji, que muda de posição entre o capítulo 1 e o capítulo 2 |
| 2 | Ganho sobre o MapReduce | 100x, do site oficial, apoiado no GraySort (3x com 10x menos recursos) | 10 a 20x nos artigos iniciais, "muitas ordens de grandeza" hoje | Três números diferentes para a mesma comparação, nenhum com condição declarada |
| 3 | Quando o Structured Streaming chegou | "introduzido na versão 2.1" | o 2.0 introduziu duas coisas, um modelo experimental de *Continuous Streaming* e as APIs de Structured Streaming; GA no 2.2 | Discrepância factual pequena, mas revela que o Luu tratou a linha do tempo de streaming com menos cuidado, o que casa com ele ainda apresentar DStream como abstração principal |
| 4 | Requisito de Java | três afirmações no mesmo capítulo: "basta ter Java instalado", sem versão; Java 11 ou superior **preferido** para o shell Scala; Python 3.7.x ou superior para o `pyspark` | Java 8 ou superior como **requisito**, com `JAVA_HOME` setado | Não é contradição lógica (11 satisfaz "8 ou superior"), é piso diferente e status diferente: um recomenda, o outro exige. O que quebra ambiente não é o piso e sim o teto, que nenhum dos dois declara |
| 5 | Composição da stack | cinco componentes sobre o Spark Core, com Spark Streaming e SparkR entre eles | quatro componentes: Spark SQL, MLlib, Structured Streaming e GraphX | O Luu mistura gerações e o Damji não. O caso extremo é a vaga de streaming no Luu, que muda de nome três vezes dentro do mesmo capítulo [6]. A lista do Damji é a que corresponde ao produto de 2020, embora a figura dele tenha o próprio problema [38] |
| 6 | Quando o MLlib passou a ser baseado em DataFrame | "a partir do Spark 2.0 as APIs de MLlib são baseadas em DataFrame" | a divisão em dois pacotes é do **1.6**, e o `spark.mllib` baseado em RDD está em modo de manutenção, sem data declarada, enquanto o `spark.ml` baseado em DataFrame recebe as features novas | O Luu apaga a existência dos dois pacotes, que é justamente o que confunde quem procura documentação de MLlib hoje. Mesmo padrão da divergência 3: ele simplifica a linha do tempo e perde a informação útil |
| 7 | Gerenciadores de cluster suportados | dois nomeados, YARN e "Meso", mais o cluster manager embutido; Kubernetes não aparece | quatro: standalone embutido, YARN, Mesos e Kubernetes | Dois livros quase contemporâneos, e um deles não registra que o Spark roda em Kubernetes. Para quem vai montar ambiente em 2026, a lista do Luu manda para o lugar errado |

## Vocabulário novo

Termos que apareceram sem definição suficiente e precisam ser resolvidos no aprofundamento. A coluna da direita é o que **falta**, não o que o livro disse.

| Termo | Onde apareceu | O que o texto entrega | O que ficou faltando |
|---|---|---|---|
| **Shuffle / data shuffling** | Luu 1 ("mover dado entre máquinas de forma eficiente"), Damji 2 (o que separa narrow de wide) | que existe, que envolve ler dado de outras partições e escrever em disco, e que é o que delimita stage | o mecanismo: quem escreve, quem lê, onde o dado intermediário fica, o que acontece quando não cabe em memória, e quanto custa |
| **DAG** | Damji 1 e 2, o tempo todo | grafo acíclico dirigido que representa o plano de execução | como o DAG vira stages, e quem faz essa quebra |
| **Catalyst** | Luu 1 (uma linha), Damji 1 (creditado pelos ganhos de performance) | "faz as otimizações comuns de engines analíticas" | quais fases tem, o que decide, e como ler o resultado com `explain()` |
| **Tungsten** | Luu 1 (uma menção, sem definição, na seção de MLlib), Damji 1 (motor físico de execução) | o nome e a atribuição de gerar código compacto | o que é gerar código em runtime, e por que isso muda a performance |
| **Whole-stage code generation** | Damji 1 (atribuído ao Tungsten), Damji 2 (caixa `WholeStageCodegen` no DAG da UI, sem explicação) | gera código compacto para execução | o que aparece no plano quando acontece, por que só parte do plano entra na caixa, e o que **impede** que aconteça |
| **Partição** | Damji 1 (unidade física e lógica), Damji 2 (uma task por partição) | é a unidade de paralelismo e mora no storage | como o número de partições é decidido na leitura, e como mudá-lo sem provocar shuffle |
| **Localidade de dado** | Damji 1 (citada duas vezes, no MapReduce e nas partições, sempre como preferência e nunca com níveis) | preferência por ler a partição mais próxima | quais são os níveis, em que ordem o Spark desce por eles, e o que faz quando o preferido não está disponível |
| **Nível de localidade (`Process local`)** | Damji 2, Figura 2-9 | um valor na captura da UI | o mesmo de cima, visto do lado de quem lê a UI: o que significa cada rebaixamento |
| **Afinidade de rack** | Damji 1 (na explicação do MapReduce, colada em localidade de dado) | só o nome, como coisa que se favorece | o que é um rack no desenho de um cluster, e se o conceito ainda significa algo em nuvem, onde o rack é invisível para quem aluga |
| **Skew** | Luu 1 (o AQE "otimiza skew joins automaticamente") | nada além do nome | o que é uma partição enviesada, como se detecta na UI, e o que o AQE de fato faz com ela |
| **Star schema, fato e dimensão** | Luu 1 (na explicação do DPP) | assumido como conhecido | por que o DPP só serve nesse desenho, e o que acontece fora dele |
| **AQE** | Luu 1 | adapta o plano em runtime; troca estratégia de join, trata skew, ajusta partições | se vem ligado, com quais estatísticas decide, e o que **não** resolve |
| **DPP** | Luu 1 | evita ler dado desnecessário em join de star schema | em que condições dispara, e como confirmar no plano que disparou |
| **Otimização just-in-time** | Luu 1, primeira página, como o destaque do Spark 3.0 | o nome, e a promessa de acelerar aplicações e reduzir tuning | o que é otimizar em JIT no contexto de query, e qual das features do 3.0 o autor tinha em mente, já que o termo nunca reaparece |
| **DStream** (*discretized stream*) | Luu 1 (apresentado como a abstração principal do Spark para streaming), Damji 1 (citado como obsoleto) | que fatia a entrada em micro-lotes por intervalo de tempo | o que exatamente ficou obsoleto, o que ainda usa DStream, e por que o Luu apresenta como atual algo que o Damji dá por morto |
| **Exactly-once** | Luu 1 (garantia ponta a ponta do Structured Streaming) | é uma garantia, e facilita a vida | ponta a ponta até onde, e sob quais condições de fonte e de sink |
| **Semântica de dado atrasado** | Damji 1 (o motor do Spark SQL "cuida" dela no Structured Streaming) | que o motor resolve por você | o que conta como dado atrasado, o que o motor de fato faz, e onde entra watermark |
| **`Exchange`** | Damji 2 (por ausência: "não há `Exchange` porque só há um stage") | é o nó que aparece quando há troca de dado | onde exatamente ele aparece no plano, e como usar isso para contar shuffles |
| **`BatchScan`, `mapPartitionsInternal`** | Damji 2, caixas do DAG nas figuras 2-8 e 2-9 | nada, são só rótulos na captura | o que cada nó do DAG representa, e como ler a sequência de caixas |
| **Linhagem** | Damji 2 | o registro das transformações, que dá tolerância a falhas | qual o custo de linhagem longa, e onde entra checkpoint |
| **Bytecode e JVM** | Damji 1 (código de qualquer linguagem "vira bytecode compacto nas JVMs dos workers"), Damji 2 | usado como explicação de por que as linguagens empatam | por onde o PySpark chega no bytecode da JVM, e em que ponto exato essa cadeia quebra |
| **ANSI SQL:2003** | Damji 1 (afirmação de conformidade) | que o Spark SQL é conforme | o que o padrão exige, o que o Spark cobre de fato, e o papel de `spark.sql.ansi.enabled` |
| **Modo de manutenção** | Damji 1 (`spark.mllib`) | que o pacote está nele | o que isso garante na prática: recebe correção de bug, recebe feature, some quando |
| **Estimador, transformador, featurizador** | Damji 1 (MLlib, na seção do cientista de dados) | listados como abstrações de alto nível | a definição de cada um e como se encaixam em um `Pipeline` |
| **DSL** | Damji 1 (consultas em DSL, na seção do engenheiro de dados) | sigla aberta uma vez, sem definição | o que separa uma DSL de uma API comum, e por que o Spark chama a API de DataFrame assim |
| **Modo cliente e modo cluster** | Damji 1, só na tabela | duas linhas de uma tabela, presas ao YARN | a diferença conceitual (onde o driver roda) e as consequências práticas de cada escolha |
| **`local[*]`** | banner do shell em Damji 2 e em Luu 2 (figuras 2-2 e 2-3) | nada, nos dois livros | o que o asterisco significa e o que muda entre `local`, `local[N]` e `local[*]` |
| **Gang scheduler / Project Hydrogen** | Damji 1 | citado como marco do 2.4 para deep learning | o que é agendar em gangue e por que treino distribuído precisa disso |
| **Embaraçosamente paralelo** | Damji 1 | usado como qualidade desejada | é jargão importado; vale fixar a definição antes de repetir |

## O que fica para o aprofundamento

Perguntas que os capítulos abriram e não fecharam, agrupadas pelo tipo de resposta que exigem.

**Números e alegações.** Qual é a comparação honesta entre Spark e MapReduce hoje, com carga e condição declaradas? O ganho vem de memória, de codegen, do otimizador ou de não escrever intermediário em disco, e em que proporção? Onde estão as medições que **não** são da Databricks?

**Mecanismo de execução.** Como exatamente o DAG vira stages, e por que o shuffle é a fronteira? O que é escrito e lido em um shuffle, e onde? O que a Spark UI mostra que permite reconstruir esse caminho a partir de uma query real? O que significa "skipped stage"? E o que são `BatchScan`, `WholeStageCodegen`, `mapPartitionsInternal` e `Process local`, que a UI cospe sem explicar?

**O que os livros pularam.** O que é `local[*]`, e por que o modo local não é o modo standalone? Qual a diferença prática entre client mode e cluster mode, e por que shell interativo exige um deles? Como se pede número de executores, memória e cores de fato, já que o Luu 1 diz que dá para pedir mas não mostra como, e o Luu 2 promete `spark-submit` e não entrega?

**Otimizador.** O que o Catalyst faz, em fases? Como ler um `explain()` e reconhecer pushdown, pruning e estratégia de join? O que o whole-stage codegen marca no plano, e o que o desliga? Por que UDF em Python é diferente de expressão nativa, já que o Damji afirma que toda linguagem gera o mesmo bytecode?

**AQE e DPP.** O AQE vem ligado ou não, em qual versão isso mudou, e com que estatísticas ele decide? Que problemas o AQE **não** resolve, e que continuam sendo trabalho de quem escreve a query? Como confirmar, em um plano real, que o DPP disparou?

**APIs.** Afinal, RDD é para aprender primeiro ou é assunto de tuning, já que o próprio Damji muda de posição entre os dois capítulos? Existe Dataset tipado em Python? Quando descer para RDD ainda faz sentido em 2026?

**Ambiente, e é aqui que a leitura envelhece pior.** Qual versão do Spark é a atual, e quais versões de Java, Scala e Python ela exige e suporta como **máximo**? O pacote `hadoop2.7` ainda existe? A Databricks Community Edition ainda existe, e o que a substituiu? Como montar ambiente local hoje sem seguir nenhum dos dois livros: pip, uv, Docker, Spark Connect?

**Ecossistema.** O Koalas ainda é projeto separado? O Delta Lake segue sendo a escolha default, ou o cenário de formato transacional mudou? O Mesos ainda está lá? Em que versão o processamento contínuo entrou de fato, e ele é a mesma coisa que Structured Streaming?

As respostas estão em [02-aprofundamento.md](02-aprofundamento.md); as perguntas que sobreviveram ao aprofundamento e viraram pergunta de aula estão em [03-aula.md](03-aula.md); o artefato que fecha o ciclo está em [04-pos-aula.md](04-pos-aula.md).
