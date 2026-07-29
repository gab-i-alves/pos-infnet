---
title: "Aula 02 de Spark - Pré-aula"
aula: "Aula 02 - Transformação e persistência SQL"
data:
tags:
  - pos-infnet
  - aula-02
  - pre-aula
  - spark
  - bibliografia
  - registro-de-leitura
  - spark-sql
  - dataframe
  - catalyst
---

# Aula 02 · Pré-aula

Primeira etapa: a leitura que o professor passou, lida antes de qualquer aprofundamento. Ler o aprofundamento primeiro apaga a fronteira entre o que é tese da bibliografia e o que é correção posterior, e é essa fronteira que rende pergunta boa na aula.

Esta aula vinha para pagar duas dívidas da aula 01. O **Catalyst**, que os quatro capítulos anteriores só nomearam, uma linha cada. E o **papel do RDD**, sobre o qual Luu e Damji discordaram e o Damji discordou de si mesmo. Depois da leitura: uma dívida foi paga pela metade e a outra ficou mais confusa do que estava. O detalhe está em [Divergências entre os livros](#divergências-entre-os-livros).

## Sumário

| Seção | Do que trata |
|---|---|
| [Emenda da aula 01](#emenda-da-aula-01) | o que o ciclo anterior ensinou sobre o método |
| [O que foi passado](#o-que-foi-passado) | a bibliografia, o [mapa das cinco leituras](#mapa-das-cinco-leituras) e a [ordem em que li](#ordem-de-leitura) |
| [Como ler](#como-ler) | o método desta etapa e onde cada anotação foi parar |
| [Teses que valem marcação](#teses-dos-capítulos-que-valem-marcação) | os palpites escritos antes da leitura, com o veredito de cada um |
| [Registro de leitura](#registro-de-leitura) | a anotação leitura por leitura, o corpo do documento |
| [Onde eu não acreditei](#onde-eu-não-acreditei) | as 110 dúvidas, agrupadas e prefixadas por leitura |
| [Divergências entre os livros](#divergências-entre-os-livros) | onde Luu, Damji e Chadha discordam, e onde cada um discorda de si mesmo |
| [Spark contra Databricks](#spark-contra-databricks) | o que é motor e o que é plataforma, separado |
| [Vocabulário novo](#vocabulário-novo) | os termos usados sem definição suficiente |
| [O que fica para o aprofundamento](#o-que-fica-para-o-aprofundamento) | as perguntas que a leitura abriu e não fechou |

Para consulta pontual, o registro de leitura abre por leitura:

- **[1. Luu, cap. 3, seção 1: Understanding RDD](#luu-capítulo-3-seção-1-understanding-rdd)**
- **[2. Damji, cap. 3: Apache Spark's Structured APIs](#damji-capítulo-3-apache-sparks-structured-apis)**
- **[4. Luu, cap. 3, seção 2: Introduction to the DataFrame API](#luu-capítulo-3-seção-2-introduction-to-the-dataframe-api)**
- **[5. Damji, cap. 4: Built-in Data Sources](#damji-capítulo-4-built-in-data-sources)**
- **[3. Chadha, cap. 1: Data Ingestion and Data Extraction](#chadha-capítulo-1-data-ingestion-and-data-extraction)**

E a tabela de dúvidas abre nos mesmos cinco blocos, com prefixo por leitura: [R1](#dúvidas-luu-capítulo-3-seção-1) (10 itens), [R2](#dúvidas-damji-capítulo-3) (23), [R4](#dúvidas-luu-capítulo-3-seção-2) (24), [R5](#dúvidas-damji-capítulo-4) (23), [R3](#dúvidas-chadha-capítulo-1) (30). O prefixo é mudança de método vinda da aula 01, onde a numeração corrida de 1 a 55 teve de ser refeita no meio do caminho.

## Emenda da aula 01

A preencher depois que o [pós-aula da aula 01](../aula-01/04-pos-aula.md) estiver fechado. O método manda copiar as emendas para cá antes de começar a ler: elas são o que o ciclo anterior ensinou sobre o processo, e emenda que não viaja não corrige nada.

## O que foi passado

Cinco leituras em três livros, um deles novo. É mais material que a aula 01, que teve quatro capítulos em dois livros.

| # | Obra | Leitura | Versão que o texto cobre |
|---|---|---|---|
| 1 | Luu, Hien. *Beginning Apache Spark 3*. Apress, 2021 | cap. 3, seção 1, *Understanding RDD* | Spark 3.0 / 3.1 |
| 2 | Damji, Jules et al. *Learning Spark*, 2ª ed. O'Reilly, 2020 | cap. 3, *Apache Spark's Structured APIs* | Spark 3.0 |
| 3 | Chadha, Pulkit. *Data Engineering with Databricks Cookbook*. Packt, 1ª ed., 31/05/2024 | cap. 1, *Data Ingestion and Data Extraction with Apache Spark* | ciclo Spark 3.4 |
| 4 | Luu | cap. 3, seção 2, *Introduction to the DataFrame API* | Spark 3.0 / 3.1 |
| 5 | Damji | cap. 4, *Spark SQL and DataFrames: Introduction to Built-in Data Sources* | Spark 3.0 |

Nada de material licenciado é reproduzido aqui: este documento registra a leitura, não o texto. Nomes de método, classe, opção, formato, caminho e valores de saída entram como fato, porque é isso que se consulta depois.

Sobre o livro novo: o PDF é recorte da plataforma da O'Reilly e **não traz folha de créditos**. O único dado impresso é o ISBN, `9781837633357`. A datação interna vem dos quinze links de *See also*, todos rotulados **Spark 3.4.0**, e do pacote `spark-xml_2.12:0.16.0`. Fora do PDF, esse ISBN é a 1ª edição, de 31 de maio de 2024. Vale saber que o autor é Sr. Solutions Architect na própria Databricks, o que não invalida nada mas explica escolhas.

### Mapa das cinco leituras

| # | Páginas | Do que trata | Para que serve |
|---|---|---|---|
| 1 | ~1 (do rodapé da 2 ao primeiro terço da 3) | RDD: a essência, sem uma linha de código | a seção mais curta de toda a bibliografia até agora |
| 2 | 51 | APIs estruturadas: tipos, schema, `Column`, `Row`, DataFrame, Dataset e o Catalyst | é o capítulo central da aula, e o único que paga dívida |
| 3 | 35 | ingestão e extração em formato de receita: CSV, JSON, Parquet, XML, escrita | o contraponto aplicado, e o mais recente |
| 4 | ~15 (da 3 à 17) | a API de DataFrame no Luu, fontes de dado e escrita | sobreposição proposital com 2 e 5 |
| 5 | 39 | fontes embutidas, tabelas e views, catálogo | fecha a parte de persistência |

**Cerca de 141 páginas no escopo pedido**, contra a estimativa de 135 que eu havia feito no esqueleto. Os PDFs somam 166 porque o `livro1-cap3.pdf` traz o capítulo 3 inteiro: das 41 páginas, o professor pediu 16. As outras 25 estão registradas em bloco no fim da leitura 4, porque várias respondem teses desta lista.

As leituras 2, 4 e 5 se sobrepõem de propósito, e é aí que estão as divergências. Na aula 01, a sobreposição entre Luu e Damji rendeu sete divergências e foi a parte mais útil de tudo.

### Ordem de leitura

O professor listou nesta ordem: 1, 2, 3, 4, 5. Ela interleva os livros e deixa a persistência para o fim, o que faz sentido pela ementa, que é transformação e depois persistência.

Li na ordem **1, 2, 4, 5, 3**, por dois motivos. Primeiro: agrupar Luu com Luu e Damji com Damji deixa a divergência visível, porque a mesma coisa aparece duas vezes com vocabulário diferente e com pouca coisa no meio para embaçar. Segundo: o Chadha por último funciona como teste de realidade sobre o que os outros dois ensinaram, e não como mais uma exposição a absorver.

Deu certo, e a ordem revelou uma coisa que a ordem do professor esconderia: o Luu 3.1 e o Damji 3 tratam do mesmo assunto, o RDD, e chegam a conclusões opostas sobre por onde começar. Lidos em sequência, a contradição salta.

## Como ler

Cerca de 141 páginas, então de duas e meia a três horas de leitura corrida, quase o dobro da aula 01. Não pare para pesquisar cada termo desconhecido, isso é trabalho da etapa seguinte. Pare para anotar duas coisas apenas:

1. **Onde eu não acreditei.** Número redondo demais, afirmação sem fonte, benchmark sem condição declarada, contradição interna. Virou a lista de 110 dúvidas em [Onde eu não acreditei](#onde-eu-não-acreditei).
2. **Onde eu não entendi.** Distinguir "não entendi o mecanismo" de "não entendi o vocabulário", porque as duas coisas se resolvem de formas diferentes. O vocabulário virou a tabela [Vocabulário novo](#vocabulário-novo); o mecanismo virou as perguntas de [O que fica para o aprofundamento](#o-que-fica-para-o-aprofundamento).

Uma terceira anotação, específica desta aula por causa da sobreposição de três livros: **onde dois textos dizem a mesma coisa com palavras diferentes, ou coisas diferentes sobre o mesmo assunto**. Virou [Divergências entre os livros](#divergências-entre-os-livros). E uma quarta, por causa do livro de Databricks: **o que é motor e o que é plataforma**, em [Spark contra Databricks](#spark-contra-databricks).

## Teses dos capítulos que valem marcação

A lista foi escrita **antes** da leitura. Depois dela, cada item ficou com o veredito e o lugar exato onde a afirmação aparece. A caixa marcada quer dizer que a tese está em ao menos uma das cinco leituras, inteira ou pela metade; a vazia, que não está em nenhuma.

O padrão que salta da tabela: **a bibliografia é complementar por acidente, não por desenho**. Quase nenhuma tese é atendida por mais de uma leitura, e a leitura que atende cada uma varia. Quem ler só um dos três livros sai com buracos diferentes.

**Sobre o RDD e as APIs**

- [x] O RDD descrito por **atributos internos**, e não só como coleção distribuída. Confirmada duas vezes, e as duas discordam no número: o Luu 3.1 lista **cinco** peças (três obrigatórias, mais esquema de particionamento e localização do dado como opcionais) e o Damji 3 lista **três**, com a assinatura `Partition => Iterator[T]` explícita. Ver [divergência 2](#divergências-entre-os-livros).
- [ ] **Linhagem** como mecanismo de tolerância a falhas, com recomputação da partição perdida. Meia nos dois lugares onde aparece, e pelo mesmo motivo: Luu 3.1 fala em reproduzir o RDD, Damji 3 atribui resiliência às dependências, e **nenhum dos cinco textos diz que a recomputação é por partição perdida**. Falta justo a granularidade, que é o que torna o mecanismo barato. Ausente em R4, R5 e R3.
- [x] O argumento de que **dar estrutura ao dado dá espaço ao otimizador**. Confirmada e é o eixo declarado do Damji 3, que fecha a seção do RDD com a pergunta "So what's the solution?" e usa o resto do capítulo como resposta. O Luu 3.1 faz o mesmo argumento pelo negativo, listando três otimizações que o RDD impede. Some no meio da leitura 4 e não existe em R5 nem em R3.
- [x] **Dataset como API tipada**, com o alerta de que não existe em Python. Confirmada só no Damji 3, e bem: seção com título próprio, o motivo declarado (Python e R não têm segurança de tipo em tempo de compilação) e uma tabela por linguagem. O Luu 3.2 afirma o contrário no escopo lido, dizendo que a API está disponível nas quatro linguagens sem ressalva.
- [x] Recomendação explícita de **quando usar DataFrame e quando usar Dataset**. Confirmada só no Damji 3, com doze condicionais e uma figura. O que falta é o mecanismo: por que DataFrame ganha em espaço e velocidade não é dito.

**Sobre schema**

- [x] **Duas formas de definir schema**, programática e por string DDL. Confirmada só no Damji 3, que dá as duas com código nas duas linguagens e declara preferência pela DDL. Nas outras leituras cada uma dá metade, e metades diferentes: Luu 3.2 só a programática (`DDL` tem zero ocorrências nas 41 páginas), Damji 4 só a DDL (`StructType` aparece uma vez, numa célula de tabela, e `StructField` não aparece), Chadha só a programática.
- [x] A recomendação de **declarar schema em vez de inferir**, com os motivos. **Este é o achado da rodada, e ele contraria a expectativa.** O Damji 3 diz exatamente o que a aula 01 registrou como coisa que os livros não dizem: declarar impede o Spark de criar um job separado só para ler parte grande do arquivo e determinar o schema. Repete na seção de Dataset e oferece `samplingRatio` como meio-termo. Os outros três recomendam declarar sem explicar o mecanismo do custo, e o Chadha, o livro aplicado que tinha mais motivo para dizer, é o que menos diz.

**Sobre o Catalyst**

- [x] As **quatro fases do Catalyst**. Confirmada só no Damji 3, nome por nome e na ordem, com uma subseção por fase e a Figura 3-4 desenhando a cascata de `Unresolved Logical Plan` até `RDDs`. **A dívida da aula 01 foi paga pela metade**: a nomenclatura veio inteira, o conteúdo não. A fase 3 tem uma frase e nenhum operador físico nomeado, e texto e figura discordam sobre em que fase o custo age. Nos outros quatro capítulos o Catalyst é só um endereço.
- [ ] `explain()` como ferramenta de leitura de plano. Meia no Damji 3, ausente nos outros quatro. O Damji imprime a saída completa dos quatro blocos e não explica nada dela: nem o `*(n)`, nem `Exchange`, nem o `200`, nem `partial_sum`, nem `PushedFilters`. E escolhe um exemplo com `PushedFilters: []`, num capítulo que vende pushdown. Nas 41 páginas do Luu, `explain` tem **zero ocorrências**: não é adiamento, é omissão.

**Sobre persistência**

- [x] **Parquet como formato default e recomendado**, com o motivo. Confirmada em três leituras (Damji 3, Luu 3.2 e Damji 4), com motivos que se somam: autodescritivo, compacto, colunar, e o schema fica preservado no metadado. O Chadha descreve o formato e nunca o recomenda nem diz que é default. Duas ressalvas: o Luu explica o mecanismo errado (ver [divergência 8](#divergências-entre-os-livros)) e o Damji 4 chama compressão rápida de propriedade do formato.
- [x] **Tabela gerenciada contra externa**, e o que `DROP TABLE` faz em cada caso. Confirmada só no Damji 4, e é a tese mais bem servida dele: gerenciada apaga metadado e dado, não gerenciada apaga só o metadado, dito sem ambiguidade. Ressalva de vocabulário: ele diz **unmanaged**, nunca **external**. Ausente dos outros quatro, e `DROP TABLE` não aparece em nenhuma página do Luu.
- [x] **View temporária contra view global**, e o escopo. Confirmada só no Damji 4, que entrega os quatro métodos, a sintaxe SQL e a regra do prefixo `global_temp.`. E é onde ele se contradiz: dá duas definições incompatíveis de escopo global, cluster numa página e aplicação em outra.
- [x] Os **modos de escrita** e `partitionBy`. A única tese inteira do Chadha, e a cobertura mais completa dos cinco: os quatro modos definidos um a um mais `partitionBy` com exemplo. O Damji 4 dá os modos na tabela e **abandona** o `partitionBy`, que aparece só na linha de assinatura, sem definição e sem exemplo.
- [x] O **catálogo** como lugar onde o metadado vive. Confirmada e rasa no Damji 4, sete linhas e três chamadas sem nenhuma saída impressa. Meia no Damji 3, por caminho inesperado: o `Catalog` aparece como peça da fase 1 do Catalyst, não como coisa que o usuário consulta.

**Onde eu apostei que a bibliografia estaria defasada**

- [x] Lista de tipos e de fontes **sem VARIANT e sem os tipos geoespaciais**. Confirmada nas cinco, por construção. O buraco é maior que o palpite: falta também `TimestampNTZType` (3.4) e os tipos de intervalo (3.2), e falta a Python Data Source API (4.0), que muda a resposta à pergunta "e se meu formato não estiver na lista", pergunta que nenhum dos livros faz.
- [x] Comportamento de cast e overflow **antes do modo ANSI**. Confirmada no Luu 3.2, que declara o default de nulificar todas as colunas da linha e oferece `failFast` como única alternativa. Ausente nos outros: o assunto simplesmente não é tratado.
- [ ] Custo de UDF Python descrito **sem Arrow por padrão**. Impossível conferir: **não há uma única UDF nas 141 páginas**. Cinco leituras sobre APIs estruturadas e persistência, e a construção em que a escolha de linguagem custa caro de verdade não aparece. Isso é achado, não lacuna do palpite.
- [ ] No Chadha, dependência da **Databricks Community Edition**. **Errada, e a inversão é o achado mais divertido da rodada.** O capítulo 1 de um livro chamado *Data Engineering with Databricks Cookbook* não usa Databricks: zero menções a Auto Loader, DBFS, Unity Catalog, `dbutils`, magic command e Delta Lake. O ambiente é `docker-compose` local com JupyterLab e Spark standalone, e roda hoje sem gastar nada. Quem depende de `/databricks-datasets/` é o **Damji 4**, em todo caminho de dado das 39 páginas. Ver [Spark contra Databricks](#spark-contra-databricks).

**A conta.** Das 19 teses: **13 confirmadas** em pelo menos uma leitura, **2 meias** (linhagem e `explain()`), **4 refutadas ou impossíveis de conferir** (a CE do Chadha, a UDF ausente, e as duas que só existiam como expectativa de defasagem). Mas o número que importa é outro: **das 13 confirmadas, 7 são atendidas por uma leitura só**. A bibliografia não é redundante, é fragmentada.
## Registro de leitura

Anotação por leitura, na ordem em que foram lidas. Os códigos entre colchetes remetem à lista de [Onde eu não acreditei](#onde-eu-não-acreditei), agrupada por leitura: `R1` é a leitura 1, `R2` a 2, e assim por diante.

---

### Luu, capítulo 3, seção 1: Understanding RDD

*Understanding RDD*, pouco mais de uma página: começa no rodapé da página 2 e termina no primeiro terço da página 3, de um capítulo de 41 páginas. É a seção mais curta do capítulo inteiro. Não tem uma linha de código, não tem figura, não tem tabela e não cita um único método de RDD. O que ela faz é definir o RDD por atributos internos e, no último parágrafo, virar o argumento contra ele. O caráter é de justificativa: a seção existe para explicar por que as APIs estruturadas foram criadas, não para ensinar a usar RDD. A frase que abre a seção anterior deixa isso explícito, ao dizer que vale discutir a abstração inicial para entender melhor as motivações da nova.

#### A moldura do capítulo, antes da seção começar

Três parágrafos e uma figura preparam o terreno e já contêm as posições que a seção depois desenvolve.

O capítulo abre datando o RDD como a **abstração central inicial**, de quando o Spark foi apresentado ao mundo em **2012** [R1-5], e situando as **Structured APIs** na versão **1.6** [R1-4], descritas como "the new and preferred way" para tarefas de engenharia de dados. Guarde essa expressão: uma página depois o mesmo autor escreve que você **precisa** entender o RDD, e o livro nunca reconcilia as duas posições [R1-2]. A condição declarada para essa nova abstração funcionar é dupla: o dado precisa estar em **formato estruturado** e a lógica de cálculo precisa seguir uma **certa estrutura**. Com essas duas informações em mão, diz o texto, o Spark consegue fazer as otimizações que aceleram a aplicação. É a tese central do capítulo, e está na página 1, não na seção sobre RDD.

O módulo Spark SQL é apresentado como tendo **duas partes**: as representações das APIs estruturadas, `DataFrame` e `Dataset`, e o **Catalyst optimizer**, descrito como responsável por "all the complex machinery that works behind the scenes". Essa é a primeira das duas únicas menções ao Catalyst em todo o capítulo [R1-10]. O conceito de DataFrame é creditado ao **DataFrame do pandas**, e a diferença declarada é que o do Spark lida com volume espalhado por muitas máquinas. Nota de contraste interno: o capítulo 1 do mesmo livro creditava a inspiração a R **e** Python.

O que separa dado estruturado de não estruturado, segundo o texto, é o **schema**, definido como a estrutura do dado em forma de nomes de coluna e tipos associados. Formatos citados: texto (CSV, XML, JSON) e binário (**Avro**, Parquet, ORC). O Avro aparece aqui e nunca mais: não está entre as fontes embutidas que o capítulo lista depois. Uma consequência "não antecipada" dessa versatilidade, diz o texto, é que o Spark serve como ferramenta de conversão de formato.

**A Figura 3-1**, legendada *Spark SQL components*, é um diagrama de camadas com quatro caixas. No topo, duas caixas de contorno lado a lado, **Spark shell** e **Spark applications**, cada uma com uma seta azul grossa apontando para baixo. As setas entram numa caixa larga azul-clara chamada **Spark SQL**, que contém duas caixas azul-escuras empilhadas à direita: **DataFrame API** e **Catalyst Optimizer**. Abaixo, separada, uma quarta caixa larga: **Spark Core**. Três leituras da figura. Primeira: o `Dataset` **não aparece**, apesar de o texto ao lado dizer que as representações das APIs estruturadas são DataFrame e Dataset, e apesar de o resumo do capítulo afirmar que a abstração principal do Spark SQL é o Dataset. Segunda: as duas únicas peças que o desenho põe dentro do Spark SQL são justamente a API e o otimizador, o que confirma o Catalyst como metade do módulo e torna mais estranho ele nunca ser explicado. Terceira: não há seta entre Spark SQL e Spark Core, só adjacência vertical; a relação de camadas é afirmada no texto ("built on top of the good old reliable Spark Core"), não desenhada.

#### A definição em cinco peças

A definição curta é uma frase: o RDD representa uma **coleção tolerante a falhas de elementos, particionada pelos nós de um cluster, que pode ser operada em paralelo**. A definição longa é uma lista de cinco itens, apresentada como as características de que o RDD consiste.

| # | Peça | Obrigatória | Para que o runtime usa |
|---|---|---|---|
| 1 | conjunto de **dependências** de RDDs pai | sim | é o dado de entrada do RDD; permite reproduzir o RDD em cenário de falha, e é daí que vem a resiliência |
| 2 | conjunto de **partições**, os pedaços que compõem o dataset inteiro | sim | permite executar a lógica em paralelo, para reduzir o tempo de cálculo |
| 3 | uma **função** que calcula todas as linhas do dataset | sim | é fornecida pelo usuário e enviada a cada executor, para rodar contra cada linha de cada partição [R1-8] |
| 4 | o **metadado sobre o esquema de particionamento** | opcional | o texto não diz [R1-7] |
| 5 | a **localização** de onde o dado reside no cluster | opcional | o texto não diz [R1-7] |

A afirmação estrutural: essas cinco informações são o que o **runtime do Spark** usa para escalonar e executar a lógica expressa com operações de RDD. Note o que não está na lista: nada sobre persistência, nada sobre serialização, nada sobre tipo do elemento.

#### Linhagem são as três primeiras peças

O texto faz uma partição explícita da lista: as **três primeiras** peças compõem a informação de **linhagem** (*lineage*), e a linhagem serve a dois propósitos, nesta ordem: **determinar a ordem de execução dos RDDs** e **recuperar de falha**. A ordem em que o livro lista os dois propósitos importa, porque o primeiro é escalonamento e o segundo é tolerância a falhas, e a maioria dos textos só apresenta o segundo.

A cadeia causal que o texto monta é curta: dependência é entrada, entrada permite reproduzir, reproduzir é o que faz o RDD ser *resilient*. É a única explicação de tolerância a falhas da seção.

O que a seção **não** diz sobre linhagem, e é muito: não diz que a recomputação é **por partição perdida**, e não só do RDD inteiro [R1-6]; não usa a palavra **DAG**; não usa **stage**; não distingue dependência **estreita** de **larga**; não menciona **shuffle**; não menciona **checkpoint** nem custo de linhagem longa (a palavra `checkpoint` não aparece em nenhuma das 41 páginas do capítulo). Ou seja, a peça de vocabulário que a aula 01 deixou aberta continua aberta depois desta leitura, com uma frase a mais.

#### O argumento do otimizador cego

O último parágrafo é o pivô da seção e do capítulo. A abstração de RDD é chamada de **simples e flexível**, e a flexibilidade é apresentada como tendo um custo: o Spark **não tem visão da intenção do usuário**. Não sabe se a lógica está filtrando, fazendo join ou agregando. Logo, diz o texto, o Spark **não pode fazer otimização nenhuma** [R1-3].

Três otimizações são nomeadas como impossíveis sobre RDD, e vale registrá-las porque são a lista pela qual o resto do capítulo pode ser medido:

| Otimização | Como o texto descreve |
|---|---|
| **predicate pushdown** | reduzir a quantidade de dado lido das fontes de entrada [R1-9] |
| recomendar um **tipo de join mais eficiente** | acelerar o cálculo |
| **poda de colunas** que a saída não usa mais | o texto descreve a ação, não nomeia a técnica |

O silêncio importante: o parágrafo diz que o Spark não pode otimizar, e não diz quem passa a poder quando o dado tem estrutura. A palavra Catalyst não aparece na seção que existe para justificá-lo [R1-10].

#### Termo usado antes de ser definido

Dois casos, ambos verificáveis por página.

**`predicate pushdown`** é usado na página 3 como se fosse conhecido e só ganha definição na página 17, dentro da subseção de JDBC, catorze páginas depois: lá o texto explica que o Spark empurra as condições de filtro até o RDBMS o quanto possível, filtrando no nível do banco. Quem lê apenas esta seção não sabe o que o termo significa [R1-9].

**`executor`** aparece na peça 3 sem redefinição. Foi definido no capítulo 1 (processo JVM dedicado a uma aplicação), mas quem começa o livro por aqui não tem isso.

#### O que a seção não faz, e onde isso volta

Vale listar, porque é o mapa do que **não** procurar aqui:

- **Nenhuma linha de código.** `parallelize`, `map`, `filter`, `reduceByKey` não aparecem. A primeira `parallelize` do capítulo está na seção seguinte, e só como matéria-prima para virar DataFrame.
- **Não define transformação e ação de RDD.** A seção seguinte assume que a semântica é conhecida, ao dizer que "the evaluation semantics are identical in RDD". Isso é dívida cruzada: a seção que devia fundar o vocabulário não o funda, e a seguinte se apoia nele.
- **Não diz quando usar RDD hoje.** Nenhuma recomendação, nenhum caso de uso, nenhum aviso de que virou API de baixo nível.
- **Não menciona `DataFrame.rdd`.** A porta de saída para RDD existe e o próprio capítulo a usa, vinte e cinco páginas depois, em `movies.rdd.getNumPartitions`, que devolve `Int = 1`. É a prova, dentro do livro, de que há RDD embaixo do DataFrame, e o livro não conecta os dois pontos.

#### O que ficou marcado em Luu 3.1

Dúvidas [R1-1] a [R1-10]. Vocabulário aberto: linhagem, predicate pushdown, poda de colunas, esquema de particionamento, tipo de join, executor, coleção tolerante a falhas, Catalyst optimizer.

---

### Damji, capítulo 3: Apache Spark's Structured APIs

51 páginas de PDF (e-book reflowado, não a paginação impressa), o maior dos cinco textos da aula e o único que abre o motor. A estratégia é de baixo para cima e por privação: em vez de apresentar as APIs estruturadas pelas virtudes, o capítulo abre desmontando o RDD em três atributos, mostra o que o Spark **não consegue ver** através deles, e usa essa cegueira como o argumento para tudo o que vem depois. Só então constrói a escada: tipos, schema, coluna, linha, operações, exemplo com dado grande, Dataset, comparação entre as duas APIs, e no último quinto o motor de Spark SQL, com o Catalyst e suas quatro fases. É o capítulo que paga a dívida da aula 01, e paga com nome, figura e saída de plano. Também é o capítulo mais malcuidado dos que li: seis tabelas de tipos, e três delas trazem erro que o próprio capítulo desmente algumas páginas adiante.

#### O que existe por baixo de um RDD

A abertura é uma lista de três, e é literalmente a tese que a aula 01 deixou pendente. Um RDD tem **dependências**, **partições** (com alguma informação de localidade) e uma **função de cômputo** com a assinatura `Partition => Iterator[T]`. Cada uma serve a uma coisa:

- **Dependências.** A lista que diz ao Spark como o RDD foi construído a partir das entradas. Quando é preciso reproduzir resultado, o Spark recria o RDD a partir dessas dependências e repete as operações. É daí que vem a resiliência.
- **Partições.** Dão ao Spark a capacidade de quebrar o trabalho e paralelizar o cômputo pelos executores. Em alguns casos, e o exemplo dado é leitura de HDFS, o Spark usa a informação de localidade para mandar trabalho para executores perto do dado, e assim trafega menos pela rede.
- **Função de cômputo.** Produz o `Iterator[T]` com o dado que vai ficar no RDD.

O veredito vem em duas palavras, "simple and elegant", e em seguida a conta. São quatro problemas, e o capítulo os empilha:

1. **A função de cômputo é opaca ao Spark.** O Spark não sabe o que você está fazendo dentro dela. Join, filtro, seleção ou agregação, ele vê **uma expressão lambda** e nada mais.
2. **O `Iterator[T]` também é opaco em RDD de Python.** Ali o Spark só sabe que é objeto genérico de Python.
3. **Sem inspecionar a expressão, não há como otimizar.** O texto é direto: o Spark não tem compreensão da intenção.
4. **Sem conhecer o tipo `T`, não há como comprimir.** Para o Spark é objeto opaco, ele não sabe se você acessa uma coluna de certo tipo dentro dele, e a única coisa que pode fazer é **serializar o objeto como uma série de bytes, sem nenhuma técnica de compressão**.

O fecho é a formulação do problema do capítulo: essa opacidade prejudica a capacidade do Spark de reorganizar o cômputo em um plano de consulta eficiente. Vale marcar o que a lista **não** diz: em nenhum momento aparece a palavra recomputação de partição perdida, nem a palavra linhagem nessa seção. A tolerância a falhas fica implícita em "recreate an RDD from these dependencies".

#### Estruturar o Spark: três esquemas e quatro benefícios

O Spark 2.x introduziu, segundo o capítulo, **três esquemas** de estruturação. Primeiro, expressar cômputo por padrões comuns de análise de dado, na forma de operações de alto nível: filtrar, selecionar, contar, agregar, tirar média, agrupar. Segundo, estreitar isso com um conjunto de operadores comuns em uma **DSL**, disponível como API nas linguagens suportadas, que permite dizer ao Spark **o que** computar em vez de **como**, e é o que deixa o Spark construir plano eficiente. Terceiro, permitir arranjar o dado em **formato tabular**, como tabela SQL ou planilha, com tipos estruturados suportados.

Os benefícios vêm em duas camadas. A camada que o capítulo cita e adia: **melhor performance e melhor eficiência de espaço** através dos componentes do Spark. A camada que ele desenvolve ali: **expressividade, simplicidade, componibilidade e uniformidade**. Não há número em nenhuma das duas, nem benchmark, nem condição declarada.

Há uma concessão explícita e ela é importante para o argumento da aula: alguém pode dizer que usar só operadores de DSL mapeados a padrões recorrentes limita a capacidade do desenvolvedor de instruir o compilador. A resposta do capítulo é que você não está confinado, pode voltar à API de baixo nível de RDD a qualquer momento, "although we hardly ever find a need to do so". A porta fica aberta e desencorajada na mesma frase.

#### A mesma agregação, em RDD e em DataFrame

O exemplo que carrega a tese: agregar idades por nome e tirar a média, sobre cinco tuplas com quatro nomes distintos (Brooke duas vezes, com 20 e 25). Em RDD, em Python, é `sc.parallelize` seguido de três operações encadeadas: `map(lambda x: (x[0], (x[1], 1)))`, `reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))` e outro `map` que divide soma por contagem. O julgamento do autor sobre o próprio código é que ele é "cryptic and hard to read", diz ao Spark **como** agregar, e o Spark não recebe a intenção. Acrescenta que o equivalente em Scala seria bem diferente do de Python.

Em DataFrame, a mesma coisa é uma linha: `data_df.groupBy("name").agg(avg("age"))`, depois de `spark.createDataFrame(dados, ["name", "age"])`. A saída tem quatro linhas, com `Brooke` em 22.5, `Jules` em 30.0, `TD` em 35.0 e `Denny` em 31.0. O argumento é que agora o Spark pode inspecionar ou parsear a consulta, entender a intenção e reorganizar as operações.

Depois vem a versão em Scala do mesmo trecho, com `SparkSession.builder.appName("AuthorsAges")`, `createDataFrame(Seq(...)).toDF("name", "age")` e o mesmo `groupBy().agg()`, para sustentar a tese de **uniformidade**: a API "looks nearly identical". Nas duas versões o capítulo imprime a mesma saída, na mesma ordem de linhas, embora as tuplas de entrada estejam em ordens diferentes nos dois blocos. É o mesmo padrão que a aula 01 marcou no capítulo 2: saída idêntica apresentada como fato onde a ordem não é garantida.

Uma nota lateral e um erro visível: o capítulo lista as linguagens em que a DSL está disponível como "Java, Python, Spark, R, and SQL". "Spark" está no lugar de Scala.

#### Os tipos básicos, em Scala e em Python

O capítulo abre com um exercício no `spark-shell`: `import org.apache.spark.sql.types._` e três `val` amarrados a `StringType`, para mostrar que tipo é objeto que se atribui. Depois vêm as Tabelas 3-2 (Scala) e 3-3 (Python). Elas têm a mesma estrutura de três colunas: nome do tipo do Spark, valor correspondente na linguagem, e API para instanciar. Junto as duas aqui, porque a única diferença real é a coluna do meio:

| Tipo do Spark | Valor em Scala | Valor em Python | API que as tabelas dão |
|---|---|---|---|
| `ByteType` | `Byte` | `int` | `DataTypes.ByteType` |
| `ShortType` | `Short` | `int` | `DataTypes.ShortType` |
| `IntegerType` | `Int` | `int` | `DataTypes.IntegerType` |
| `LongType` | `Long` | `int` | `DataTypes.LongType` |
| `FloatType` | `Float` | `float` | `DataTypes.FloatType` |
| `DoubleType` | `Double` | `float` | `DataTypes.DoubleType` |
| `StringType` | `String` | `str` | `DataTypes.StringType` |
| `BooleanType` | `Boolean` | `bool` | `DataTypes.BooleanType` |
| `DecimalType` | `java.math.BigDecimal` | `decimal.Decimal` | `DecimalType` |

Dois problemas, e são do livro, não da leitura. A Tabela 3-3 repete `DataTypes.ByteType` como forma de instanciar em Python, o que não é PySpark: em Python se escreve `ByteType()`, e o próprio capítulo faz isso no código do schema, com `StringType()` e `IntegerType()`, e faz de novo na Tabela 3-5, com `BinaryType()` e `TimestampType()`. A coluna de API da tabela de Scala foi copiada para a de Python [R2-2]. O segundo problema está na frase que apresenta a Tabela 3-2: diz que todos os tipos são subtipos da classe `DataTypes`, exceto `DecimalType`. `DataTypes` é a classe fábrica de métodos estáticos; a superclasse dos tipos é `DataType` [R2-3].

Sobre o que **não** está nas tabelas: nada de `NullType`, nada dos tipos de intervalo, nada de `CharType` ou `VarcharType`, e nada de `VARIANT` nem tipo geoespacial, que são posteriores ao livro. A ausência de intervalo já era ausência em 2020 para o `CalendarIntervalType`, que existia mas não era exposto ao usuário.

#### Os tipos complexos e estruturados

Tabelas 3-4 (Scala) e 3-5 (Python). Aqui as colunas de API divergem de verdade entre as linguagens, e é a informação mais consultável do capítulo:

| Tipo do Spark | Valor em Scala | Valor em Python | Instanciar em Scala | Instanciar em Python |
|---|---|---|---|---|
| `BinaryType` | `Array[Byte]` | `bytearray` | `DataTypes.BinaryType` | `BinaryType()` |
| `TimestampType` | `java.sql.Timestamp` | `datetime.datetime` | `DataTypes.TimestampType` | `TimestampType()` |
| `DateType` | `java.sql.Date` | `datetime.date` | `DataTypes.DateType` | `DateType()` |
| `ArrayType` | `scala.collection.Seq` | lista, tupla ou array | `DataTypes.createArrayType(ElementType)` | `ArrayType(dataType, [nullable])` |
| `MapType` | `scala.collection.Map` | `dict` | `DataTypes.createMapType(keyType, valueType)` | `MapType(keyType, valueType, [nullable])` |
| `StructType` | `org.apache.spark.sql.Row` | lista ou tupla | `StructType(ArrayType[fieldTypes])` | `StructType([fields])` |
| `StructField` | tipo do valor correspondente ao campo | idem | `StructField(name, dataType, [nullable])` | `StructField(name, dataType, [nullable])` |

Duas coisas para não copiar daqui. A célula `StructType(ArrayType[fieldTypes])` está errada e o próprio capítulo escreve o certo em código, `StructType(Array(StructField(...)))`: `ArrayType` é um tipo de dado do Spark, não o `Array` de Scala [R2-4]. E o terceiro argumento de `ArrayType` em Python é `containsNull`, não `nullable`, coisa que a própria saída de `printSchema` do capítulo mostra, ao imprimir `element: string (containsNull = false)`. O rótulo da tabela está errado, a saída está certa.

Note que `StructType` tem como valor correspondente em Scala o `Row`. Essa é a costura que o capítulo vai apertar duas seções adiante, quando disser que DataFrame é `Dataset[Row]`.

#### Schema: três motivos para declarar

**Schema** é definido como o que fixa nomes de coluna e tipos associados de um DataFrame. O capítulo diz que schema entra em cena principalmente na leitura de dado estruturado de fonte externa, e lista **três benefícios** de declarar em vez de deixar inferir, na abordagem que ele chama de *schema-on-read*:

1. Você **alivia o Spark do ônus de inferir** os tipos.
2. Você **impede o Spark de criar um job separado** só para ler uma porção grande do arquivo e determinar o schema, o que em arquivo grande sai caro e demorado.
3. Você **detecta erro cedo**, quando o dado não casa com o schema.

O motivo 2 é o achado da leitura em relação à aula 01. O registro da aula 01 anotou que nenhum dos livros dizia que inferir dispara job; este capítulo diz, com essas palavras. A recomendação vem sem meio termo: sempre declare o schema quando for ler arquivo grande de uma fonte.

Existe uma saída intermediária, em nota: se você não quiser especificar o schema, o Spark infere de uma **amostra** a custo menor, com a opção `samplingRatio`. O exemplo em Scala usa `.option("samplingRatio", 0.001).option("header", true).csv(...)`. O capítulo não diz o que acontece se a amostra não representar o arquivo, e é o risco óbvio de amostrar 0,1% para decidir tipo.

#### Schema: as duas formas de definir

**Programática**, com `StructType` recebendo uma coleção de `StructField(nome, tipo, nullable)`. Em Scala, `StructType(Array(StructField("author", StringType, false), ...))`. Em Python, `StructType([StructField("author", StringType(), False), ...])`, com o import `from pyspark.sql.types import *`. Note a diferença que a tabela de tipos errou: em Scala o tipo entra sem parênteses, em Python com.

**String DDL**, e o capítulo é explícito em chamá-la de "much simpler and easier to read": `"author STRING, title STRING, pages INT"`, idêntica nas duas linguagens porque é só uma string. No exemplo maior a DDL aparece com **acento grave** em volta de cada nome e com tipo complexo dentro: `` "`Id` INT, `First` STRING, ..., `Campaigns` ARRAY<STRING>" ``. O capítulo não explica por que os acentos graves aparecem ali e não no exemplo anterior, e não diz o que a DDL **não** consegue expressar (nulabilidade por campo, por exemplo, que a forma programática expressa com o terceiro argumento).

#### O exemplo de schema, e a contradição que ele deixa

O capítulo monta o mesmo DataFrame de seis autores da Tabela 3-1 de duas maneiras. Em Python (`Example-3_6.py`), com DDL e dado estático em lista de listas, rodado com `spark-submit`. Em Scala (`Example3_7`), com schema programático e leitura de um `blogs.json` passado por argumento, via `spark.read.schema(schema).json(jsonFile)`. Os dois imprimem `show()`, `printSchema()` e `schema`.

O texto afirma que "the output from the Scala program is no different than that from the Python program". As saídas mostradas dizem outra coisa: no exemplo em Python todos os campos saem com `nullable = false` e o array com `containsNull = false`; no exemplo em Scala, com o mesmo `false` passado em cada `StructField`, todos saem com `nullable = true` e `containsNull = true`. A diferença é visível na página e não é comentada [R2-5]. A explicação real, que o capítulo não dá, é que a leitura de JSON força nulabilidade, e é exatamente o tipo de coisa que quem declara schema para "detectar erro cedo" precisa saber.

Outra fricção no mesmo exemplo: a Tabela 3-1 rotula a coluna `Published` como `(Date)`, e o schema do exemplo a declara `STRING` [R2-6]. E `blogs_df.schema` devolve `StructType(List(...))` em Python contra `StructType(StructField(...), ...)` em Scala, sem o `List`, sem uma palavra sobre a diferença.

O que o exemplo entrega bem: mostra que `printSchema()` desenha árvore com um nível por campo e um nível a mais para `element` dentro do array, e que `df.schema` devolve a definição reutilizável em código.

#### `Column`, expressão e `Row`

**`Column`.** Coluna nomeada em DataFrame é conceitualmente igual a coluna nomeada em pandas, em R ou em tabela de RDBMS: descreve um tipo de campo. Nas linguagens suportadas, colunas são **objetos com métodos públicos**, representados pelo tipo `Column`. A nota que resolve uma confusão de nomenclatura: `Column` é o nome do objeto, e `col()` é uma função embutida que **devolve** um `Column`.

**Expressão.** Dá para usar expressão lógica ou matemática sobre coluna, com `expr("columnName * 5")`, e `expr()` vive em `pyspark.sql.functions` (Python) e `org.apache.spark.sql.functions` (Scala). O que `expr()` recebe é argumento que o Spark **parseia como expressão** e computa.

O bloco de exemplos, todo em Scala sobre o `blogsDF`, é o inventário útil:

- `blogsDF.columns` devolve `Array[String]` com os nomes, e sai em ordem alfabética (`Campaigns, First, Hits, Id, Last, Published, Url`), não na ordem do schema. O capítulo não comenta a reordenação.
- `blogsDF.col("Id")` devolve um `Column`.
- `select(expr("Hits * 2"))` e `select(col("Hits") * 2)` dão o mesmo, e a coluna resultante se chama `(Hits * 2)`.
- `withColumn("Big Hitters", expr("Hits > 10000"))` **adiciona** coluna booleana derivada de condição.
- `withColumn("AuthorsId", concat(expr("First"), expr("Last"), expr("Id")))` concatena três colunas em uma nova, e o `select` seguinte mostra `JulesDamji1`, `BrookeWenig2`, e assim por diante.
- `select(expr("Hits"))`, `select(col("Hits"))` e `select("Hits")` devolvem a mesma coisa: três formas para o mesmo resultado.
- `sort(col("Id").desc)` e `sort($"Id".desc)` são idênticos. O `$` antes do nome é apresentado como "uma função em Spark que converte a coluna Id em um `Column`".

O capítulo admite que arranhou a superfície e remete à documentação para a lista completa de métodos de `Column`. E fecha a seção com a frase que amarra o resto: `Column` não existe isolado, cada coluna é parte de uma linha, e todas as linhas juntas constituem um DataFrame, "que, como vamos ver mais adiante, é na verdade um `Dataset[Row]` em Scala".

**`Row`.** Objeto genérico com uma ou mais colunas, que podem ser do mesmo tipo ou de tipos diferentes. Como é objeto e coleção **ordenada** de campos, dá para instanciar `Row` em qualquer linguagem suportada e acessar campo por **índice começando em 0**. Em Scala, `blogRow(1)` devolve `res62: Any = Reynold`, e o tipo de retorno `Any` é a evidência de que `Row` não é tipado. Existem getters públicos por tipo: `row.getInt(0)`, `row.getBoolean(1)`, `row.getString(2)`. Em Python o acesso é por colchete, `row[1]`, sem getter tipado. E internamente o Spark manipula objetos `Row` convertendo os campos para os tipos das Tabelas 3-2 e 3-3, com o exemplo de um `Int` virando `IntegerType` em Scala ou Java e `IntegerType()` em Python.

`Row` também serve para criar DataFrame rápido, para interatividade e exploração: `spark.createDataFrame(rows, ["Authors", "State"])`. A ressalva prática vem em seguida: na vida real você lê de arquivo, e como os arquivos são enormes, declarar schema e usá-lo é o caminho mais rápido e eficiente.

#### `DataFrameReader` e `DataFrameWriter`

**`DataFrameReader`** é a interface para ler dado para um DataFrame de muitas fontes, nos formatos JSON, CSV, Parquet, Text, Avro e ORC, entre outros. **`DataFrameWriter`** é o caminho inverso. O capítulo credita à comunidade a variedade de conectores, e cita lojas NoSQL, RDBMS e motores de stream como Kafka e Kinesis.

O dataset do capítulo é o de chamadas do corpo de bombeiros de San Francisco, em CSV, com **28 colunas** e **mais de 4.380.660 registros** [R2-10], e é por causa desse tamanho que o capítulo diz ser mais eficiente declarar schema do que inferir. O schema é montado programaticamente com 28 `StructField`, todos `nullable=True`, e a leitura é `spark.read.csv(sf_fire_file, header=True, schema=fire_schema)` em Python, ou `spark.read.schema(fireSchema).option("header", "true").csv(sfFireFile)` em Scala. As duas formas de passar `header` aparecem lado a lado, uma como argumento nomeado e outra como `option`, sem comentário.

Na escrita, o fato que interessa à parte de persistência da aula: **Parquet é o formato default** do `DataFrameWriter`, usa compressão **snappy**, e quando o DataFrame é escrito em Parquet **o schema fica preservado no metadado do Parquet**, de modo que leituras posteriores não exigem schema manual. Duas formas de persistir: `write.format("parquet").save(caminho)` para arquivo, e `write.format("parquet").saveAsTable(nome)` para tabela, o que **registra metadado no metastore do Hive**. Tabela gerenciada contra não gerenciada, metastore e catálogo ficam adiados para o capítulo 4, com essa palavra.

Não aparecem no capítulo: modos de escrita (`append`, `overwrite`, `errorIfExists`, `ignore`), `partitionBy`, nem qualquer menção a formato transacional. Delta Lake não é citado uma vez, nem no texto nem na Figura 3-3, num livro escrito por gente da Databricks e com todos os caminhos de dado apontando para `/databricks-datasets/`.

#### Projeção, filtro, renomear, adicionar e remover

A seção começa com uma definição errada, e ela é o tipo de erro que atravessa a leitura inteira se passar batido: "a *projection* in relational parlance is a way to return only the rows matching a certain relational condition by using filters". Isso é seleção, ou restrição. Projeção é escolher **colunas** [R2-1]. O mapeamento para métodos que vem em seguida está certo: projeção com `select()`, filtro com `filter()` ou `where()`, e o capítulo diz que os dois últimos são intercambiáveis.

O inventário de operações, com a forma nas duas linguagens onde ela difere:

| Operação | Método | Nota do capítulo |
|---|---|---|
| Projetar colunas | `select("a", "b", "c")` | aceita nome, `col()` ou `expr()` |
| Filtrar | `where(col("CallType") != "Medical Incident")` em Python, `where($"CallType" =!= "Medical Incident")` em Scala | o operador de diferença muda de linguagem |
| Contar valores distintos | `agg(countDistinct("CallType").alias("DistinctCallTypes"))` | devolve 32 no dataset |
| Listar distintos | `.distinct().show(10, False)` | `truncate` é `False` em Python e `false` em Scala; o bloco de saída mistura duas consultas [R2-14] |
| Renomear coluna | `withColumnRenamed("Delay", "ResponseDelayedinMins")` | devolve **novo** DataFrame, porque transformação é imutável |
| Adicionar coluna | `withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))` | o primeiro argumento é o nome da coluna nova |
| Remover coluna | `drop("CallDate")` | usado logo depois de cada `withColumn` para trocar a coluna antiga pela nova |
| Ordenar | `orderBy("count", ascending=False)` em Python, `orderBy(desc("count"))` em Scala | duas assinaturas diferentes para a mesma ordem |

Sobre nome de coluna: o capítulo conta que no dataset original os nomes tinham **espaço** (`Incident Number` virou `IncidentNumber`), que declarar o nome desejado no `StructField` já renomeia tudo no DataFrame resultante, e que espaço em nome de coluna é problema "especialmente quando você quer escrever ou salvar como Parquet (which prohibits this)". A afirmação sobre o Parquet vem sem condição nem versão [R2-12].

A conversão de tipo é o exemplo mais didático da seção, e o capítulo desmonta em quatro passos: converter o tipo de string para timestamp suportado pelo Spark, usar a string de formato (`"MM/dd/yyyy"` ou `"MM/dd/yyyy hh:mm:ss a"`), depois de converter dar `drop()` na coluna antiga e anexar a nova nomeada no primeiro argumento de `withColumn()`, e atribuir o DataFrame resultante a `fire_ts_df`. As funções vêm de `spark.sql.functions`: `to_timestamp()` e `to_date()` para converter, e `dayofmonth()`, `dayofyear()`, `dayofweek()` e `year()` para consultar depois. O texto nomeia como uma das colunas a converter a `AlarmDtTm`, que não existe no schema; o código converte `AvailableDtTm` [R2-13].

#### Agregações e estatística descritiva

O padrão que o capítulo diz ser tão comum quanto projetar e filtrar: **agrupar e contar**, com `groupBy()`, `count()` e `orderBy()`. A consulta dos tipos de chamada mais comuns devolve, no topo, `Medical Incident` com 2.843.475, `Structure Fire` com 578.998 e `Alarms` com 483.518, de um total de 32 tipos.

Duas notas com valor operacional. Uma diz que para DataFrame grande sob consulta frequente vale **cache**, e adia a estratégia para capítulos posteriores. A outra é a mais útil do capítulo: a API tem `collect()`, mas para DataFrame muito grande é caro e **perigoso**, porque pode causar exceção de falta de memória; ao contrário de `count()`, que devolve **um número** ao driver, `collect()` devolve a coleção de **todos** os objetos `Row`. Se você só quer espiar, use `take(n)`, que devolve os n primeiros `Row`.

Para estatística, `min()`, `max()`, `sum()` e `avg()`, com o truque de import que evita colisão com as funções embutidas de Python: `import pyspark.sql.functions as F`, e o equivalente em Scala com `import org.apache.spark.sql.{functions => F}`. A consulta única devolve `sum(NumAlarms)` igual a 4.403.441, média de atraso de resposta de 3,902170335891614 minutos, mínimo de 0,016666668 e máximo perto de 1879. Para necessidade mais avançada, o capítulo lista nomes e manda ler a documentação: `stat()`, `describe()`, `correlation()`, `covariance()`, `sampleBy()`, `approxQuantile()`, `frequentItems()`.

#### O exemplo de ponta a ponta que o capítulo não mostra

Existem dois exemplos ponta a ponta anunciados e **nenhum dos dois está no livro**. Os dois remetem a notebook no repositório do GitHub, "for brevity".

O de DataFrame usa o dataset do corpo de bombeiros e promete responder sete perguntas: quais foram os tipos de chamada em 2018; que meses de 2018 tiveram mais chamadas; que bairro de San Francisco gerou mais chamadas em 2018; que bairros tiveram o pior tempo de resposta em 2018; que semana de 2018 teve mais chamadas; se há correlação entre bairro, CEP e número de chamadas; e como usar arquivo Parquet ou tabela SQL para guardar e ler de volta.

O de Dataset usa o dataset de dispositivos IoT, descrito como pequeno e falso, e promete quatro coisas: detectar dispositivo com bateria abaixo de um limite; identificar países com nível alto de CO2; computar mínimo e máximo de temperatura, bateria, CO2 e umidade; e ordenar e agrupar por média de temperatura, CO2, umidade e país. O objetivo declarado é ilustrar clareza e legibilidade, não performance.

Para quem lê só o livro, a consequência é concreta: as sete perguntas do exemplo de DataFrame são bom roteiro de exercício, e nenhuma delas tem resposta no texto.

#### A API de Dataset: objeto tipado e case class

O enquadramento é a unificação: o Spark 2.0 unificou DataFrame e Dataset como APIs estruturadas com interfaces parecidas, para o desenvolvedor aprender um conjunto só. Dataset tem duas caras, **tipada e não tipada**, e é isso que a **Figura 3-1** desenha: duas caixas à esquerda, `DataFrame` (amarela) e `Dataset` (azul), uma flecha para uma caixa verde única chamada `Structured APIs`, e dentro dela duas etiquetas. A etiqueta `Untyped APIs` traz dois marcadores, `DataFrame = Dataset[Row]` e `Alias in Scala`. A etiqueta `Typed APIs` traz `Dataset[T]` e `In Scala & Java`. A figura é a definição inteira em um quadro.

A conceituação em prosa: DataFrame em Scala é apelido para uma coleção de objetos genéricos, `Dataset[Row]`, onde `Row` é objeto genérico **não tipado** da JVM que pode carregar campos de tipos diferentes. Dataset, por contraste, é coleção de objetos **fortemente tipados** da JVM em Scala, ou de uma classe em Java. E o capítulo cita a documentação do Dataset: coleção fortemente tipada de objetos específicos de domínio, transformáveis em paralelo por operações funcionais ou relacionais, e cada Dataset em Scala tem uma **visão não tipada** chamada DataFrame, que é um Dataset de `Row`.

A seção seguinte diz a coisa que a aula 01 pediu, sem rodeio: **Dataset só faz sentido em Java e Scala; em Python e R só DataFrame faz sentido.** A razão dada é que Python e R não têm segurança de tipo em tempo de compilação, os tipos são inferidos ou atribuídos durante a execução; em Scala e Java os tipos se ligam a variáveis e objetos em tempo de compilação. A Tabela 3-6 resume:

| Linguagem | Abstração principal | Tipado ou não |
|---|---|---|
| Scala | `Dataset[T]` e DataFrame (apelido de `Dataset[Row]`) | os dois |
| Java | `Dataset<T>` | tipado |
| Python | DataFrame | `Row` genérico, não tipado |
| R | DataFrame | `Row` genérico, não tipado |

**Criar Dataset** exige conhecer o schema, ou seja, os tipos. O capítulo repete que com JSON e CSV é possível inferir, mas que para dado grande isso é caro. Em Scala o caminho mais fácil é a **case class**; em Java, classes **JavaBean**, e as duas ficam para o capítulo 6. O exemplo é o `DeviceIoTData`, case class de 15 campos (`battery_level: Long`, `c02_level: Long`, `cca2: String`, `cca3: String`, `cn: String`, `device_id: Long`, `device_name: String`, `humidity: Long`, `ip: String`, `latitude: Double`, `lcd: String`, `longitude: Double`, `scale: String`, `temp: Long`, `timestamp: Long`), correspondendo campo a campo às chaves do JSON. A conversão é `spark.read.json(caminho).as[DeviceIoTData]`, e o `.as[T]` é o que transforma o `Dataset[Row]` devolvido pela leitura em `Dataset[DeviceIoTData]`.

#### Operações de Dataset, e o que muda de verdade

Transformações e ações valem igual, e o resultado varia com o tipo de operação. O exemplo central é `ds.filter(d => d.temp > 30 && d.humidity > 70)`. O que ele demonstra, e é a diferença que o capítulo quer que fique:

- `filter()` é **sobrecarregado**, com muitas assinaturas. A versão usada é `filter(func: (T) => Boolean): Dataset[T]`, que recebe **função lambda**.
- O argumento da lambda é um objeto **da JVM** do tipo `DeviceIoTData`, e por isso dá para acessar os campos com **notação de ponto**, como em qualquer classe de Scala ou JavaBean.
- Com DataFrame você expressa a condição de `filter()` como operação de DSL parecida com SQL, **agnóstica de linguagem**. Com Dataset você usa **expressão nativa da linguagem**, código Scala ou Java.

Essa última linha é a definição prática da fronteira entre as duas APIs, e o capítulo a deixa passar em duas frases.

O segundo exemplo encadeia `filter(d => d.temp > 25)`, `map(d => (d.temp, d.device_name, d.device_id, d.cca3))`, `toDF(...)` e `.as[DeviceTempByCountry]`, com uma case class nova de quatro campos, para mostrar que dá para trocar de forma no meio do caminho. `dsTemp.first()` devolve o objeto impresso como `DeviceTempByCountry(34,meter-gauge-1xbYRYcj,1,USA)`, e é a demonstração mais clara de que o elemento é objeto e não linha. Uma nota diz que, semanticamente, `select()` é como `map()` nessa consulta, porque as duas selecionam campos e produzem resultado equivalente.

O recap lista as operações de Dataset como parecidas com as de DataFrame: `filter()`, `map()`, `groupBy()`, `select()`, `take()`. E faz uma comparação que vale marcar: Dataset é parecido com **RDD**, no sentido de oferecer interface semelhante a esses métodos e segurança em tempo de compilação, mas com interface muito mais legível e orientada a objeto.

Sobre o que o motor faz por baixo: quando você usa Dataset, o motor de Spark SQL cuida de **criação, conversão, serialização e desserialização** dos objetos da JVM, e também de gerenciamento de memória **fora do heap** da Java, com ajuda dos **encoders** de Dataset. Tudo isso em três linhas, com o resto adiado para o capítulo 6. O que **não** aparece em nenhum lugar do capítulo: que a lambda tipada é opaca ao Catalyst, e que o custo de materializar objeto da JVM por linha é a razão de DataFrame ser mais rápido que Dataset. O capítulo abre acusando o RDD de exatamente esse defeito e não faz a ligação quando apresenta a lambda de `filter()` do Dataset [R2-21].

Dois trechos de código dessa seção não rodam como estão: o `filter` do primeiro exemplo tem chave desbalanceada, e o `dsTemp2` alternativo seleciona `$"device_id"` duas vezes antes de fazer `.as[DeviceTempByCountry]`, que tem quatro campos [R2-15].

#### DataFrame contra Dataset: as doze recomendações

O capítulo responde à pergunta com uma lista de doze condicionais. Reorganizo por destino, porque a lista original alterna:

| Situação | Recomendação |
|---|---|
| Dizer ao Spark **o que** fazer, não **como** | DataFrame ou Dataset |
| Querer semântica rica, abstração de alto nível e operadores de DSL | DataFrame ou Dataset |
| Precisar de expressão de alto nível, filtro, map, agregação, média, soma, consulta SQL, acesso colunar ou operador relacional sobre dado semiestruturado | DataFrame ou Dataset |
| Querer segurança de tipo **estrita em tempo de compilação**, e não se incomodar de criar várias case classes para um `Dataset[T]` | Dataset |
| Querer aproveitar a serialização eficiente do Tungsten com **Encoders** | Dataset |
| Precisar de transformação relacional parecida com consulta SQL | DataFrame |
| Querer unificação, otimização de código e simplificação de API entre os componentes do Spark | DataFrame |
| Ser usuário de **R** | DataFrame |
| Ser usuário de **Python** | DataFrame, e descer para RDD se precisar de mais controle |
| Querer eficiência de espaço e de velocidade | DataFrame |
| Querer erro pego em compilação em vez de em execução | escolher pela Figura 3-2 |

A **Figura 3-2** é uma matriz de duas linhas por três colunas, com uma flecha de duas pontas no topo indicando espectro da esquerda para a direita, de `SQL` para `DataFrames` para `Datasets`. Para **erro de sintaxe**: `Runtime` em SQL, `Compile Time` em DataFrames, `Compile Time` em Datasets. Para **erro de análise**, que é o erro de nome de coluna ou de tipo que só se descobre resolvendo contra o schema: `Runtime` em SQL, `Runtime` em DataFrames, `Compile Time` em Datasets. A figura resume o argumento inteiro do Dataset em quatro células: o que ele compra é a linha de baixo, à direita.

Duas ressalvas que a figura não faz. Ela é implicitamente sobre Scala e Java, porque "compile time" não quer dizer nada em Python, e aparece duas páginas depois de o capítulo afirmar que Python e R não têm segurança de tipo em compilação [R2-18]. E note que "eficiência de espaço e de velocidade" está na coluna do DataFrame, ao lado de "serialização eficiente do Tungsten" na coluna do Dataset: as duas linhas juntas insinuam que DataFrame é mais rápido, sem que o capítulo diga isso nem explique por quê.

#### Quando usar RDD, e o que o capítulo decide sobre ele

A seção começa com as perguntas na boca do leitor: o RDD virou cidadão de segunda classe? Está sendo depreciado? A resposta é "a resounding **no**", com uma condição na mesma frase: a API vai continuar suportada, **embora todo o trabalho futuro de desenvolvimento no Spark 2.x e no 3.0 vá continuar tendo interface e semântica de DataFrame, não de RDD**.

Três cenários em que o capítulo diz para considerar RDD:

1. Você usa um **pacote de terceiros** escrito com RDD.
2. Você **pode dispensar** a otimização de código, a eficiência de espaço e os ganhos de performance de DataFrame e Dataset.
3. Você quer instruir o Spark com precisão **como** fazer a consulta.

E o mecanismo de saída: dá para transitar entre DataFrame ou Dataset e RDD com `df.rdd`, com a advertência entre parênteses de que **isso tem custo e deve ser evitado a menos que necessário**. O custo não é quantificado nem nomeado. A justificativa final é arquitetural: DataFrame e Dataset **são construídos sobre RDD**, e são decompostos em código compacto de RDD durante a geração de código de estágio completo.

Sobre a divergência que a aula 01 abriu, este capítulo se posiciona, e o posicionamento é duplo. Por **arquitetura**, o RDD é fundação: o capítulo abre por baixo dele, diz que ele é "the most basic abstraction in Spark", que todo o resto é construído sobre ele, e a Figura 3-4 termina em uma caixa `RDDs` como saída da geração de código. Por **prática recomendada**, é legado: "we hardly ever find a need" de voltar, "we can't imagine the opacity and comparative unreadability" de fazer o mesmo com RDD, `df.rdd` tem custo, e o desenvolvimento novo é todo em DataFrame. Ele resolve a contradição do próprio livro separando as duas perguntas sem dizer que as está separando [R2-20]. Contra o Luu, que abre o capítulo de Spark SQL ensinando RDD porque considera o assunto necessário, o Damji ensina RDD para explicar por que não usá-lo.

#### O motor de Spark SQL

No nível de programação, Spark SQL permite emitir consultas compatíveis com **ANSI SQL:2003** sobre dado estruturado com schema, a mesma afirmação do capítulo 1 e com a mesma ausência de condição [R2-23]. Desde a introdução no Spark 1.3, virou motor substancial. O que ele faz, em seis itens:

1. **Unifica** componentes do Spark e permite abstrair para DataFrame e Dataset em Java, Scala, Python e R.
2. **Conecta** ao metastore e às tabelas do Apache Hive.
3. **Lê e escreve** dado estruturado com schema específico a partir de formatos de arquivo estruturados (JSON, CSV, Text, Avro, Parquet, ORC) e converte dado em **tabelas temporárias**.
4. Oferece um **shell interativo** de Spark SQL para exploração rápida.
5. Oferece **ponte** para ferramentas externas nos dois sentidos, via conectores JDBC e ODBC padrão.
6. **Gera plano otimizado e código compacto** para a JVM, para a execução final.

A **Figura 3-3** desenha isso em quatro faixas. No topo, caixas de ferramentas externas: `Tableau`, `Snowflake`, `Talend` e uma caixa de reticências. Abaixo, três entradas na mesma faixa: `Spark Application` à esquerda, `JDBC/ODBC Connectors` no centro (que é por onde as ferramentas do topo falam) e `Spark SQL Shell` à direita. No meio, a faixa azul do motor, com o rótulo `Spark SQL` no centro e duas elipses roxas nas laterais, `Catalyst Optimizer` e `Tungsten`. Na base, cinco fontes: `Hive Tables`, `JSON`, `Avro`, `Parquet`, `ORC`. Todas as setas são de duas pontas. Duas observações sobre o desenho: a fila de fontes na base **não** inclui CSV nem Text, que o texto lista uma linha acima, e não inclui Delta; e Snowflake aparece como ferramenta ao lado de Tableau e Talend, quando é armazém de dado, ou seja, fonte.

A frase que abre o resto do capítulo: no núcleo do motor estão **o otimizador Catalyst e o Project Tungsten**, e juntos eles sustentam as APIs de alto nível e as consultas SQL. Tungsten fica para o capítulo 6.

#### O Catalyst e suas quatro fases

Esta é a seção que a aula 01 esperava, e ela entrega mais do que o esperado em nome e figura, e menos do que o esperado em leitura de plano.

A definição de uma linha: o **Catalyst** pega uma consulta computacional e a converte em um **plano de execução**, passando por **quatro fases transformacionais**: análise, otimização lógica, planejamento físico e geração de código.

A **Figura 3-4** é o mapa da fase inteira e a peça mais valiosa do capítulo. É uma cascata vertical, com o nome da fase à esquerda, ao lado da flecha que a executa:

1. No topo, três entradas em paralelo: `SQL AST`, `DataFrame` e `Datasets`. As três convergem para uma caixa só.
2. `Unresolved Logical Plan`. Daqui sai a flecha rotulada **Analysis**, e nela entra pela lateral uma caixa chamada `Catalog`.
3. `Logical Plan`. Daqui sai a flecha rotulada **Logical Optimization**.
4. `Optimized Logical Plan`. Daqui sai a flecha rotulada **Physical Planning**.
5. `Physical Plans`, desenhada como **pilha** de várias folhas sobrepostas, e não como caixa única. Dela saem **três** flechas para uma caixa só.
6. `Cost Model`. Uma flecha desce.
7. `Selected Physical Plan`. Daqui sai a flecha rotulada **Code Generation**.
8. `RDDs`. É a última caixa do desenho.

O desenho carrega três coisas que o texto não diz. Que as três formas de escrever consulta (SQL, DataFrame, Dataset) entram no **mesmo** cano, o que é a base material da uniformidade que o capítulo vende. Que **existem vários planos físicos** e um é escolhido, e não um plano físico único derivado direto do lógico. E que o fim da linha é **RDD**, o que fecha o argumento da seção anterior sobre o RDD ser fundação.

**Fase 1, análise.** O motor começa gerando uma **árvore sintática abstrata** (AST) para a consulta, seja ela SQL ou DataFrame. Nessa fase, **qualquer coluna ou nome de tabela é resolvido consultando um `Catalog` interno**, descrito como interface programática do Spark SQL que guarda lista de nomes de coluna, tipos de dado, funções, tabelas e bancos. Resolvidos todos, a consulta passa para a fase seguinte. É esta fase que a Figura 3-2 tem em mente quando fala de "erro de análise": é aqui que um nome de coluna errado estoura.

**Fase 2, otimização lógica.** O capítulo diz que esta fase tem **dois estágios internos**. Aplicando uma abordagem baseada em **regras padrão**, o Catalyst primeiro constrói **um conjunto de vários planos** e depois, usando seu **otimizador baseado em custo (CBO)**, atribui custo a cada plano. Esses planos são dispostos como **árvores de operadores**, e podem incluir, por exemplo, **dobra de constante** (*constant folding*), **empurrar predicado para baixo** (*predicate pushdown*), **poda de projeção** (*projection pruning*) e **simplificação de expressão booleana**. O plano lógico resultante é a entrada do plano físico.

Aqui está a contradição mais importante do capítulo, e ela é interna. O **texto** põe o CBO e a escolha entre múltiplos planos na fase 2, a lógica. A **Figura 3-4** põe a pilha de planos e o `Cost Model` depois de `Optimized Logical Plan`, ou seja, dentro da fase 3, o planejamento físico. As duas descrições não podem estar certas ao mesmo tempo, e é a fase que a aula pediu que fosse explicada [R2-7]. O que o capítulo também não diz: se o CBO precisa de estatística coletada, e se ele está ligado por padrão.

**Fase 3, planejamento físico.** Uma frase e nada mais: nesta fase o Spark SQL gera um **plano físico ótimo** para o plano lógico selecionado, usando **operadores físicos** que casam com os disponíveis no motor de execução do Spark. Nenhum operador físico é nomeado no texto, embora a saída de `explain()` duas páginas antes mostre `HashAggregate`, `Exchange`, `Sort` e `FileScan`. É a fase mais curta e a mais pobre das quatro.

**Fase 4, geração de código.** Envolve gerar **bytecode Java eficiente** para rodar em cada máquina. Como o Spark SQL pode operar sobre dado carregado em memória, o Spark pode usar tecnologia de compilador para gerar código e acelerar a execução; nas palavras do capítulo, ele **age como um compilador**. O Project Tungsten, que viabiliza a geração de código de estágio completo, entra aqui.

E vem a definição que a aula 01 pediu, esta sim completa: **geração de código de estágio completo** (*whole-stage code generation*) é uma fase de otimização física de consulta que **colapsa a consulta inteira em uma única função**, eliminando chamadas de função virtual e usando **registradores de CPU** para dado intermediário. O motor Tungsten de segunda geração, introduzido no Spark 2.0, usa essa abordagem para gerar **código compacto de RDD** para a execução final, o que, diz o texto, melhora significativamente eficiência de CPU e performance. Duas ressalvas de leitura: chamar isso de "fase" colide com a contagem de quatro fases, já que é parte da fase 4; e não há uma palavra sobre **o que impede** a geração de código de estágio completo de acontecer, que é a metade prática do conceito.

A **Figura 3-5** é o segundo exemplo do Catalyst, e mostra transformação de uma consulta concreta em três estágios verticais. No topo, rotulado `Logical Plan`: um `filter` acima de um `join`, e o `join` acima de duas folhas, `events file` e `users table`. No meio, rotulado `Physical Plan`: o `join` subiu ao topo, e agora o `filter` está **abaixo** do join, sobre um `scan (events)`, com `scan (users)` do outro lado. Embaixo, rotulado `Physical Plan with Predicate Pushdown and Column Pruning`: o `join` sobre duas folhas, `optimized scan (events)` e `optimized scan (users)`, com o filtro absorvido pelo scan. A figura é o desenho de "empurrar o filtro para baixo até o scan e ler só as colunas necessárias".

O problema é o rótulo. O texto lista predicate pushdown e projection pruning como otimização **lógica**, fase 2; a figura os coloca entre dois planos **físicos** [R2-8]. É a mesma confusão de fronteira da contradição do CBO, no mesmo assunto, em duas páginas.

O código que gera a Figura 3-5 também não roda: define `usersDF` e `eventsDF` e depois usa `users` e `events`, e compara `events("date")` com a string `"2015-01-01"` [R2-16].

#### O plano que o capítulo mostra

O exemplo é a consulta de M&M do capítulo 2, escrita de duas formas: em DataFrame Python, com `select`, `groupBy("State","Color")`, `agg(sum("Count").alias("Total"))` e `orderBy("Total", ascending=False)`; e em SQL, com `SELECT State, Color, sum(Count) AS Total ... GROUP BY State, Color ORDER BY Total DESC`. A afirmação: os dois blocos passam pelo mesmo processo e terminam com plano parecido e **bytecode idêntico**. Na frase seguinte a mesma coisa é dita mais fraco: "the resulting bytecode is **likely** the same" [R2-9]. Idêntico e provavelmente igual, em duas linhas.

As ferramentas para ver isso: `df.explain(True)` em qualquer linguagem, e em Scala também `df.queryExecution.logical` e `df.queryExecution.optimizedPlan`. Como ler plano fica adiado para o capítulo 7, com essa palavra.

A saída de `explain(True)` traz **quatro** blocos nomeados, e vale notar que os nomes não são os da Figura 3-4:

| Bloco do `explain(True)` | Corresponde na Figura 3-4 a | O que muda em relação ao anterior |
|---|---|---|
| `== Parsed Logical Plan ==` | `Unresolved Logical Plan` | árvore com nomes ainda com apóstrofo (`'Sort`, `'Total`), sinal de não resolvido |
| `== Analyzed Logical Plan ==` | `Logical Plan` | ganha a linha de tipos no topo (`State: string, Color: string, Total: bigint`) e os apóstrofos somem |
| `== Optimized Logical Plan ==` | `Optimized Logical Plan` | o nó `Project` desaparece, absorvido pelo `Relation` |
| `== Physical Plan ==` | `Selected Physical Plan` | vira operador físico: `Sort`, `Exchange`, `HashAggregate`, `FileScan` |

A árvore, em qualquer dos blocos, se lê de baixo para cima: a folha é a leitura e o topo é a última operação. Os `+-` marcam filho.

O plano físico mostrado carrega cinco coisas que o capítulo **não** explica, e que são exatamente o vocabulário que a aula 01 deixou aberto:

- `*(3)`, `*(2)`, `*(1)`: o asterisco marca operador dentro de um estágio com geração de código de estágio completo, e o número identifica o estágio. O capítulo define whole-stage codegen em prosa e não liga a definição ao símbolo que a aparece na saída.
- `Exchange rangepartitioning(Total#24L DESC NULLS LAST, 200)` e `Exchange hashpartitioning(State#10, Color#11, 200)`: **duas** trocas, ou seja, dois shuffles. A palavra `Exchange` não aparece uma vez no texto do capítulo.
- O `200` nos dois `Exchange`, que é o default de partições de shuffle. O número aparece duas vezes e não é explicado, mesmo padrão do `spark.sql.shuffle.partitions` que a aula 01 registrou no capítulo 1.
- `HashAggregate` com `partial_sum` antes do primeiro `Exchange` e `HashAggregate` com `sum` depois: agregação parcial antes do shuffle e final depois. Não comentado.
- `FileScan csv ... Batched: false, PushedFilters: [], ReadSchema: struct<State:string,Color:string,Count:int>`. O `PushedFilters` está **vazio**, num capítulo que acabou de vender predicate pushdown [R2-19]. O `ReadSchema` com três colunas é a evidência de poda de projeção, e o capítulo não aponta.

Ou seja: `explain()` aparece com saída completa e sem legenda. Depois desta leitura dá para reconhecer os quatro blocos e saber a ordem das fases, mas não dá para contar shuffles nem para dizer por que um plano é pior que o outro. A dívida do Catalyst foi paga pela metade: as fases, sim; a leitura de plano, não.

Uma última ausência, e é de versão: o capítulo descreve o Catalyst como jornada **estática** de quatro fases, decidida antes de a execução começar, e não menciona **Adaptive Query Execution** nem **Dynamic Partition Pruning**, que chegaram no Spark 3.0, a versão que o livro diz cobrir [R2-17].

#### O que o autor resume no fim do capítulo 3

Existe seção `Summary`, com cinco parágrafos, e ela é honesta sobre o que o capítulo fez. Diz que a imersão nas APIs estruturadas começou pela história e pelos méritos da estrutura. Que, através de operações comuns e exemplos de código, demonstrou que as APIs de DataFrame e Dataset são bem mais expressivas e intuitivas que a API de baixo nível de RDD, e que as APIs estruturadas oferecem operadores específicos de domínio para operações comuns, aumentando clareza e expressividade. Que explorou quando usar RDD, DataFrame e Dataset conforme o caso de uso. E que, por fim, olhou por baixo do capô para ver como os dois componentes principais do motor de Spark SQL, o Catalyst e o Project Tungsten, sustentam as APIs de alto nível: **não importa a linguagem, a consulta passa pela mesma jornada de otimização, da construção do plano lógico e físico à geração final de código compacto**.

O último parágrafo anuncia que os conceitos deste capítulo são a base dos dois seguintes, que vão ilustrar a interoperabilidade entre DataFrame, Dataset e Spark SQL. Uma nota antes do resumo faz a ressalva que explica o teto do capítulo: o funcionamento técnico interno do Catalyst e do Tungsten está **fora do escopo do livro**.

Duas coisas o resumo não lista, e as duas são coisas que o capítulo fez: as duas formas de definir schema, e a tabela de tipos. São as duas seções mais consultáveis, e o autor não as menciona ao recapitular.

#### O que ficou marcado em Damji 3

Dúvidas [R2-1] a [R2-23]. Vocabulário aberto: DSL, `Exchange`, encoder, otimizador baseado em custo (CBO), árvore sintática abstrata, `Catalog`, dobra de constante, poda de projeção, simplificação de expressão booleana, schema-on-read, memória fora do heap, snappy, case class, JavaBean, operador físico, `Batched`, `PushedFilters`, `rangepartitioning` e `hashpartitioning`, `samplingRatio`, o `$` de Scala, ANSI SQL:2003.

---

### Luu, capítulo 3, seção 2: Introduction to the DataFrame API

*Introduction to the DataFrame API*, cerca de quinze páginas: começa na parte de baixo da página 3 e termina no fim da página 17, quando entra *Working with Structured Operations*. É dez vezes o tamanho da seção sobre RDD. A estrutura é um parágrafo de definição seguido de uma cascata de subseções *Creating a DataFrame from X*, uma por caminho de entrada: RDD, faixa de números, coleção local, texto, CSV, JSON, Parquet, ORC e JDBC. O caráter é de catálogo de receitas com tabelas de opções: o peso está em **como o dado entra**, não em o que fazer com ele depois. São seis tabelas de referência (3-1 a 3-6) e vinte listings numerados (3-1 a 3-20), todos em Scala, todos rodáveis no `spark-shell`. Nenhuma figura. Duas coisas que a seção não faz, e que valem dizer de saída: não define `Row` além do adjetivo "generic", e não mostra plano de execução em nenhum lugar.

#### A definição de DataFrame, e o que ela troca com o RDD

A definição literal: coleção **imutável e distribuída de dados organizada em linhas**, onde cada linha consiste num conjunto de colunas e cada coluna tem nome e tipo associado. Em outras palavras, diz o texto, essa coleção distribuída tem uma estrutura definida por um **schema**. A equivalência declarada: quem conhece o conceito de tabela em RDBMS percebe que um DataFrame é essencialmente equivalente. Cada linha é representada por um **objeto `Row` genérico**.

Note a redação: "organized into rows", quando o capítulo 1 do mesmo livro definia DataFrame como coleção organizada em **colunas nomeadas** [R4-1]. Não é bobagem de estilo: linha contra coluna é justamente a diferença de ênfase que separa a definição relacional da definição colunar, e o livro usa as duas sem avisar.

O que muda em relação ao RDD, em duas frases do texto:

| | RDD | DataFrame |
|---|---|---|
| operações | genéricas, o Spark não sabe a intenção | conjunto de operações **de domínio, relacionais e com semântica rica** |
| classificação das operações | transformação e ação | transformação e ação, **idênticas** |
| semântica de avaliação | preguiçosa na transformação, ansiosa na ação | "the evaluation semantics are identical in RDD" |

Ou seja: a mudança é de vocabulário de operação, não de modelo de execução. Isso é uma afirmação útil e o livro a faz de forma direta.

Origens declaradas de um DataFrame: leitura de muitas fontes estruturadas, leitura de **tabelas no Hive ou em outros bancos**, e conversão a partir de RDD desde que se forneça a informação de schema. A menção a Hive aqui não sobrevive até a Tabela 3-3, que lista as fontes embutidas e não inclui Hive [R4-8].

Paridade: uma frase única, "The DataFrame API is available in Scala, Java, Python, and R", sem ressalva de nenhum tipo. É tudo o que a seção diz sobre linguagens, e todos os vinte listings são em Scala.

#### Criar a partir de RDD: as duas rotas

**Rota implícita, `toDF`.** O Listing 3-1 cria um RDD de tuplas de dois inteiros com `spark.sparkContext.parallelize(1 to 10).map(x => (x, Random.nextInt(100)))` e chama `rdd.toDF("key","value")`. Os **nomes** das colunas vêm do argumento; os **tipos** são inferidos dos valores. O Listing 3-2 apresenta as duas funções de inspeção que o resto do capítulo usa sem parar:

- `printSchema`, que imprime nomes e tipos no console. Saída: `|-- key: integer (nullable = false)` e `|-- value: integer (nullable = false)`.
- `show`, que imprime o dado em tabela e **por padrão mostra 20 linhas**. `show(5)` muda o número; mais adiante, `show(5, false)` desliga o truncamento.

Detalhe que o texto não comenta e que vale registrar: `nullable = false` nas duas colunas, porque vieram de `Int` primitivo de Scala. Nullability é derivada do tipo de origem, e isso aparece só nas saídas, nunca no texto corrido. Detalhe pior: os valores mostrados na coluna `value` são 58, 18, **237**, 32, 80, **210**, **567**, **360**, **288**, **260**, e no Listing 3-3 aparece **280**, todos acima do limite de `Random.nextInt(100)` [R4-2]. O livro cobre isso com uma Note dizendo que os números variam porque são aleatórios, o que não explica nenhum valor de três dígitos.

**Rota explícita, `createDataFrame` com schema programático.** O Listing 3-4 monta um RDD de objetos `Row` com `parallelize(Array(Row(1L, "John Doe", 30L), Row(...)))`, constrói o schema à mão e passa os dois para `spark.createDataFrame(peopleRDD, schema)`:

```
StructType(Array(
  StructField("id",   LongType,   true),
  StructField("name", StringType, true),
  StructField("age",  LongType,   true)))
```

Imports necessários, e o texto os mostra: `org.apache.spark.sql.Row` e `org.apache.spark.sql.types._`. Regra declarada: cada `StructField` tem **três** informações, nome, tipo e se o valor aceita nulo. Argumento a favor da rota explícita, e é o único que o livro dá: criar schema programaticamente dá à aplicação a flexibilidade de **ajustar o schema com base em configuração externa**.

Nenhuma das duas rotas vem com recomendação de quando usar qual. E a segunda forma de declarar schema, a **string DDL**, não aparece em nenhuma página do capítulo.

#### Tabela 3-1: o mapa de tipos, só para Scala

Quinze linhas, escalares primeiro e complexos depois. Vale copiar inteira, porque é referência de consulta.

| Tipo Spark | Tipo Scala |
|---|---|
| `BooleanType` | `Boolean` |
| `ByteType` | `Byte` |
| `ShortType` | `Short` |
| `IntegerType` | `Int` |
| `LongType` | `Long` |
| `FloatType` | `Float` |
| `DoubleType` | `Double` |
| `DecimalType` | `java.math.BigDecial` (grafia do original) |
| `StringType` | `String` |
| `BinaryType` | `Array[Byte]` |
| `TimestampType` | `java.sql.Timestamp` |
| `DateType` | `java.sql.Date` |
| `ArrayType` | `scala.collection.Seq` |
| `MapType` | `scala.collection.Map` |
| `StructType` | `org.apache.spark.sql.Row` |

Dois problemas [R4-9]. O primeiro é a grafia `BigDecial`, que não compila. O segundo é maior: a tabela é **só de Scala**, uma página depois de o texto afirmar que a API existe em quatro linguagens. Quem estuda em Python não tem mapa de tipos neste capítulo.

O que a lista não tem, e é matéria de defasagem: `NullType`, `CalendarIntervalType`, `TimestampNTZType` (Spark 3.4), `YearMonthIntervalType` e `DayTimeIntervalType` (3.2), `VARIANT` (4.0) e os tipos geoespaciais `GEOMETRY` e `GEOGRAPHY` (4.2). Para um livro de 2021 cobrindo 3.0 e 3.1, a ausência dos três primeiros já é lacuna da própria época.

#### Criar a partir de faixa de números e de coleção local

`SparkSession` é apresentada aqui como o **ponto de entrada introduzido no Spark 2.0** para aplicações que usam sobretudo DataFrame e Dataset, e a função demonstrada é `range`, que cria um dataset de **uma coluna só**, chamada `id`, do tipo `LongType`.

Três variações, no Listing 3-6, com `.toDF("num")` aplicado em cima para renomear:

| Chamada | Saída |
|---|---|
| `spark.range(5)` | 0, 1, 2, 3, 4 |
| `spark.range(5,10)` | 5, 6, 7, 8, 9 |
| `spark.range(5,15,2)` | 5, 7, 9, 11, 13 |

Regra declarada: na versão de três parâmetros, o primeiro é o valor inicial, o segundo é o final **exclusivo** e o terceiro é o passo. O Listing 3-6 tem um erro de código: `val df1 = spark.range(5).toDF("num").show` liga `df1` ao retorno de `show`, que é `Unit`, não DataFrame [R4-3].

Para mais de uma coluna, o texto faz uma pergunta ao leitor ("Do you have any ideas about how to create a two-column DataFrame?") e responde no parágrafo seguinte: usar os **implicits** do Spark sobre uma `Seq` de tuplas. Listing 3-7:

```
val movies = Seq(("Damon, Matt", "The Bourne Ultimatum", 2007L),
                 ("Damon, Matt", "Good Will Hunting",    1997L))
val moviesDF = movies.toDF("actor", "title", "year")
```

O schema resultante mistura nullability: `actor: string (nullable = true)`, `title: string (nullable = true)`, `year: long (nullable = false)`. Mesma regra do Listing 3-2, mesmo silêncio do texto: primitivo de Scala não aceita nulo, `String` aceita.

O fecho da subseção é honesto sobre o próprio escopo: essas formas divertidas servem para aprender a API sem carregar arquivo, mas para análise séria com dataset grande é imperativo saber carregar de fonte externa.

#### `DataFrameReader`: o padrão de três peças

As duas classes centrais de entrada e saída são nomeadas juntas: **`DataFrameReader`** para ler e **`DataFrameWriter`** para escrever. A seção cobre só a primeira. A instância de `DataFrameReader` está disponível como a variável **`read`** da `SparkSession`, ou seja `spark.read`.

O padrão canônico, no Listing 3-9, com a aspa desbalanceada que está no original [R4-4]:

```
spark.read.format(...).option("key", value").schema(...).load()
```

**Tabela 3-2**, as três peças de informação da leitura:

| Peça | Opcional | O que o texto diz |
|---|---|---|
| `format` | **não** | pode ser fonte embutida ou formato customizado. Para embutido, nome curto: `json`, `parquet`, `jdbc`, `orc`, `csv`, `text`. Para fonte customizada, exige **nome totalmente qualificado**, por exemplo `org.example.mysource` |
| `option` | sim | o `DataFrameReader` tem um conjunto de defaults por formato; a função `option` sobrescreve |
| `schema` | sim | algumas fontes trazem o schema embutido no arquivo, "especialmente Parquet e ORC"; nesses casos "the schema is automatically inferred". Nos outros, pode ser necessário fornecer |

Duas marcações aqui. A primeira: a tabela declara `format` **obrigatório**, e o próprio capítulo o dispensa duas vezes, em `spark.read.load(path)` (porque Parquet é default) e nos atalhos do tipo `spark.read.json(path)` [R4-5]. A segunda: dizer que o schema "é inferido automaticamente" no caso de Parquet e ORC confunde **ler um schema que existe no arquivo** com **inferir um schema que não existe**. São operações de custo completamente diferente, e usar a mesma palavra para as duas apaga exatamente a distinção que interessa a quem paga a conta [R4-6].

O Listing 3-10 lista os atalhos, cada formato com as duas formas equivalentes: `spark.read.json("<path>")` ou `spark.read.format("json")`, e o mesmo par para `parquet`, `jdbc`, `orc`, `csv`, `text`, mais `spark.read.format("org.example.mysource")` para fonte customizada.

Sobre extensibilidade, duas afirmações sem evidência: a camada de fontes de dados foi desenhada para ser extensível, "a comunidade Spark escreve centenas de fontes customizadas" e implementar uma "não é difícil demais" [R4-23]. Nenhuma linha de código de fonte customizada aparece no capítulo.

**Tabela 3-3**, as seis fontes embutidas:

| Nome | Formato do dado | Comentário do livro |
|---|---|---|
| Text file | Text | sem estrutura |
| CSV | Text | valores separados por vírgula; dá para especificar outro delimitador; o nome da coluna pode vir do header |
| JSON | Text | formato semiestruturado popular; **nome e tipo de coluna inferidos automaticamente** |
| Parquet | Binary | **(formato default)** o binário popular na comunidade Hadoop |
| ORC | Binary | outro binário popular na comunidade Hadoop |
| JDBC | Binary | o formato comum para ler e escrever em RDBMS |

Duas marcações. JDBC classificado como **formato de dado binário** é categoria errada: JDBC é protocolo de acesso a banco, não formato de arquivo, e não tem "formato do dado" [R4-7]. E a lista de seis omite **Avro**, citado pelo próprio capítulo três páginas antes como formato binário comum, omite **Hive**, citado na definição de DataFrame como origem possível, e omite **Delta**, que o capítulo 1 do mesmo livro apresentou como a resposta do ecossistema para consistência [R4-8].

#### Texto puro

Arquivo de texto contém dado **não estruturado**, e ao ser lido **cada linha vira uma linha do DataFrame**. Schema resultante de `spark.read.text("README.md")`: uma coluna só, `value: string (nullable = true)`. A forma comum de separar palavras é quebrar cada linha por espaço, do jeito que o exemplo canônico de word count faz. Fonte de material sugerida: livros em texto puro em `www.gutenberg.org`. `textFile.show(5, false)` é onde o segundo parâmetro de `show` aparece, com o comentário "don't truncate".

Recomendação de fecho, e é boa: se o arquivo de texto tem um delimitador aproveitável, **é melhor lê-lo como CSV**.

#### CSV, no Luu

O parser CSV do Spark é descrito como flexível o bastante para ler texto com qualquer delimitador fornecido pelo usuário; a vírgula "just happens to be the default one". Consequência declarada: o formato `csv` serve para ler TSV e qualquer outro separador.

**Tabela 3-4**, opções comuns:

| Chave | Valores | Default | O que faz |
|---|---|---|---|
| `sep` | caractere único | `,` | delimitador de coluna |
| `header` | `true`, `false` | `false` | se true, a primeira linha do arquivo traz os nomes das colunas |
| `escape` | qualquer caractere | `\` | caractere de escape para quando o valor da coluna contém o próprio `sep` |
| `inferSchema` | `true`, `false` | `false` | se o Spark deve tentar inferir o tipo da coluna a partir do valor |

A regra mais útil da subseção, e está em texto corrido: com `header` e `inferSchema` em `true` não é preciso passar schema; caso contrário, defina o schema à mão ou programaticamente e passe em `schema`. **E se `inferSchema` for false e nenhum schema for fornecido, o Spark assume `string` para todas as colunas.** É a resposta direta para o sintoma mais comum de quem lê CSV.

Para a lista completa de opções, o livro aponta a classe **`CSVOptions`** e dá como endereço `https://github.com/apache/spark`, a raiz do repositório, sem caminho de arquivo [R4-10].

O Listing 3-12 mostra os três casos, sobre o arquivo `movies.csv` com header `actor, title, year`:

| Chamada | `year` resultante |
|---|---|
| `spark.read.option("header","true").csv(path)` | `string`, porque `inferSchema` ficou no default |
| mais `.option("inferSchema","true")` | `integer` |
| `.option("header","true").schema(movieSchema).csv(path)` | `long`, e as colunas passam a se chamar `actor_name`, `movie_title`, `produced_year` |

O terceiro caso carrega a regra de precedência: o schema fornecido traz nomes **diferentes** dos do header, e o Spark usa os nomes fornecidos. O `movieSchema` do exemplo é `StructType(Array(StructField("actor_name", StringType, true), StructField("movie_title", StringType, true), StructField("produced_year", LongType, true)))`, e é reaproveitado pelo resto do capítulo. As cinco primeiras linhas mostradas: `McClure, Marc (I)` com *Freaky Friday* 2003, *Coach Carter* 2005, *Superman II* 1980, *Apollo 13* 1995 e *Superman* 1978.

TSV, Listing 3-13: `option("sep", "\t")` combinado com `.schema(movieSchema).csv(path)` sobre `movies.tsv`. O listing tem dois defeitos: cria `movies4` e em seguida imprime `movies.printSchema`, o DataFrame errado, e o schema exibido não é o que `movies` tinha naquele ponto [R4-11]. Os caminhos dos exemplos apontam para `data/chapter4` e `<path>/book/chapter4/data/movies/...`, num capítulo 3 cujos exercícios finais apontam para `chapter3/data/movies` [R4-12].

#### JSON, no Luu

Chamado de **semiestruturado** porque cada objeto (que o texto também chama de linha) tem estrutura e cada coluna tem nome. Força declarada: formato flexível que modela qualquer caso e suporta **estrutura aninhada**. Fraqueza declarada: **verbosidade**, porque o nome da coluna se repete em cada linha do arquivo, e o texto pede que se imagine um arquivo de um milhão de linhas.

Duas coisas exigem atenção, segundo o texto. A primeira: um objeto JSON pode estar em **uma linha ou em várias**, e é preciso avisar o Spark de qual é o caso. A segunda é a parte mais importante da seção inteira para a divergência sobre inferência: dado que o arquivo JSON traz **nome de coluna e nenhum tipo**, o Spark **infere o schema parseando um conjunto de registros de amostra**, e o número de registros amostrados é definido pela opção **`samplingRatio`, com default `1.0`**. A consequência que o livro declara: "it is quite expensive to load a very large JSON file", e a saída sugerida é **baixar `samplingRatio`** para acelerar o carregamento [R4-14].

**Tabela 3-5**, opções comuns de JSON:

| Chave | Valores | Default | O que faz |
|---|---|---|---|
| `allowComments` | `true`, `false` | `false` | ignora comentários no arquivo JSON |
| `multiLine` | `true`, `false` | `false` | trata o arquivo inteiro como um objeto JSON grande espalhado por muitas linhas |
| `samplingRatio` | `0.3` (grafia do original) | `1.0` | o tamanho da amostra a ler para inferir o schema |

A linha de `samplingRatio` traz `0.3` na coluna de valores, onde as outras duas trazem o domínio (`true, false`). `0.3` é um exemplo de valor, não o domínio, e a tabela mistura as duas coisas [R4-13].

O Listing 3-14 mostra dois casos. Primeiro, `spark.read.json(path)` sem opção nenhuma, e o Spark detecta nome e tipo: `produced_year: long`. Segundo, um schema `movieSchema2` que força `produced_year` para `IntegerType`, aplicado com `spark.read.option("inferSchema","true").schema(movieSchema2).json(path)`. Passar `inferSchema` junto com `schema` é contraditório, `inferSchema` não está entre as opções de JSON da Tabela 3-5, e o texto não comenta nem uma coisa nem a outra [R4-15].

**Erro de parsing e o comportamento default.** É a informação operacional mais valiosa da subseção. Por padrão, quando o Spark encontra um registro corrompido ou um erro de parsing, ele **põe `null` em todas as colunas daquela linha**. O Listing 3-15 prova: `badMovieSchema` declara `actor_name` como `BooleanType`, e o resultado de `show(5)` são cinco linhas **inteiramente nulas**, inclusive `movie_title` e `produced_year`, que não tinham problema nenhum. Para falhar em vez de nulificar, `option("mode","failFast")`, e aí a ação levanta:

```
java.lang.RuntimeException: Failed to parse a value for data type BooleanType (current token: VALUE_STRING)
```

Note que o erro aparece como `ERROR Executor: Exception in task 0.0 in stage 3.0 (TID 3)`, ou seja **na ação**, não na chamada de leitura. O texto não chama atenção para isso, e é uma boa evidência de preguiça em cima de um caminho de leitura. O que falta: o nome do modo default (`PERMISSIVE`), os outros modos (`DROPMALFORMED`), e a coluna de registro corrompido (`columnNameOfCorruptRecord`) [R4-16].

Aqui está também a defasagem mais consequente da seção: o comportamento descrito é o de antes do **modo ANSI**, que passou a ser default no Spark 4.0 e faz conversão inválida **falhar** em vez de devolver `null` em vários caminhos. Quem lê este trecho hoje e roda numa versão recente vê comportamento diferente do descrito.

#### Parquet

Descrito como um dos formatos colunares open source mais populares do ecossistema Hadoop, **criado no Twitter**. A popularidade é atribuída a duas coisas: ser **autodescritivo** e guardar dado de forma muito compacta, aproveitando compressão. O formato colunar é apresentado como desenhado para carga analítica, onde só um subconjunto pequeno de colunas é usado.

Aqui está o erro factual mais grave da seção: "Parquet stores each column's data in a **separate file**; therefore, columns that are not needed in data analysis wouldn't have to be unnecessarily read in" [R4-17]. Parquet guarda as colunas em *column chunks* dentro de *row groups*, no **mesmo** arquivo. O benefício descrito é real, o mecanismo descrito é falso, e quem aprende assim vai errar ao raciocinar sobre tamanho de arquivo, layout de diretório e leitura seletiva.

Comparação com texto: CSV e JSON são bons para arquivos pequenos e são legíveis por humanos; Parquet é bem melhor para dataset grande, para reduzir custo de armazenamento e acelerar a leitura. O número dado: `movies.parquet` tem **cerca de um sexto** do tamanho de `movies.csv` [R4-18]. Sem tamanho absoluto, sem codec de compressão declarado, sem descrição do dataset.

Duas afirmações operacionais: **Parquet é o formato default** de leitura e de escrita no Spark, então `spark.read.load(path)` já lê Parquet e não é preciso passar `format`; e não é preciso fornecer nem inferir schema, porque **o Spark recupera o schema do próprio arquivo**. As duas formas, no Listing 3-16: `spark.read.load(path)` e a explícita `spark.read.parquet(path)`, as duas devolvendo `actor_name: string`, `movie_title: string`, `produced_year: long`.

Uma otimização é citada e não explicada: ao ler Parquet, o Spark faz **descompressão e decodificação em lotes de colunas** (*column batches*), o que "acelera consideravelmente" a leitura. Sem número, sem condição, sem o nome da coisa (leitor vetorizado) e sem a configuração que a controla.

O que a subseção não diz, e é bastante: nada sobre codec default, nada sobre `mergeSchema`, nada sobre estatísticas de rodapé, nada sobre predicate pushdown em Parquet (que aparece só de passagem, uma página depois, na subseção de JDBC).

#### ORC, no Luu

*Optimized Row Columnar*, apresentado como outro colunar autodescritivo e open source do ecossistema Hadoop, "**criado pela Cloudera** como parte da iniciativa de acelerar massivamente o Hive" [R4-19]. Isso está errado: o ORC nasceu na **Hortonworks**, e o formato colunar puxado pela Cloudera foi o Parquet. É um erro que inverte a história dos dois formatos, num livro que apresenta os dois lado a lado.

Sobre eficiência: "quite similar to Parquet in terms of efficiency and speed", sem um número. Nenhum critério para escolher entre os dois é oferecido. Uso: `spark.read.orc(path)`, com a mesma saída de schema do Parquet, e cinco linhas de exemplo.

#### JDBC

Definido como API padrão para ler e escrever em RDBMS, com suporte a MySQL, PostgreSQL, Oracle, SQLite e outros. Quatro informações são declaradas necessárias: **driver JDBC**, **URL de conexão**, **informação de autenticação** e **nome da tabela**.

Requisito de runtime: o Spark precisa ter acesso ao **JAR do driver** no classpath. O comando mostrado, no Listing 3-18, passa o jar `mysql-connector-java-5.1.45` direto para `./bin/spark-shell`. Três coisas envelheceram aqui [R4-20]: o Connector/J 5.1 atende MySQL 5.x; a classe `com.mysql.jdbc.Driver`, usada no Listing 3-20, foi substituída por `com.mysql.cj.jdbc.Driver`; e a forma corrente de fornecer o jar é `--jars` ou `--packages`, que o livro não mostra em nenhum lugar (o capítulo 2 já tinha prometido e não entregado `spark-submit`).

Verificação de conexão antes de usar o Spark, Listing 3-19, com `java.sql.DriverManager`:

```
val connectionURL = "jdbc:mysql://localhost:3306/<table>?user=<username>&password=..."
val connection = DriverManager.getConnection(connectionURL)
connection.isClosed()
connection close()
```

A última linha está sem o ponto [R4-22]. O critério de sucesso é declarado de forma útil: se nenhuma exceção de conexão apareceu, o shell conseguiu falar com o banco. Note também que o exemplo põe **usuário e senha na URL**, sem uma palavra sobre o problema disso.

**Tabela 3-6**, as opções principais:

| Chave | O que o texto diz |
|---|---|
| `url` | a URL JDBC; no mínimo host, porta e nome do banco. Exemplo MySQL: `jdbc:mysql://localhost:3306/sakila` |
| `dbtable` | o nome da tabela de onde o Spark lê ou para onde escreve |
| `driver` | o nome da classe do driver que o Spark instancia. Para o MySQL Connector/J, `com.mysql.jdbc.Driver` |

Para a lista completa, aponta `https://spark.apache.org/docs/latest/sql-programming-guide.html#jdbc-to-other-databases`.

O Listing 3-20 lê a tabela `film` do banco de exemplo **Sakila** do MySQL, com `format("jdbc")` mais as opções `driver`, `url`, `dbtable`, `user`, `password` e `.load()`. O schema resultante é o único lugar do capítulo onde se vê o mapeamento de tipos de um banco relacional: `film_id: integer (nullable = false)`, `title: string (nullable = false)`, `description: string (nullable = true)`, `release_year: date`, `language_id: integer (nullable = false)`, `rental_duration: integer (nullable = false)`, `rental_rate: decimal(4,2) (nullable = false)`, `length: integer (nullable = true)`, `replacement_cost: decimal(5,2)`, `rating: string`, `special_features: string`, `last_update: timestamp (nullable = false)`. Vale reparar: é a única fonte do capítulo que produz `nullable = false`, porque é a única que tem restrição de nulo declarada na origem. O texto não comenta.

**A lacuna mais séria da seção**: nenhuma palavra sobre paralelizar a leitura JDBC [R4-21]. `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions` e `fetchsize` não aparecem. O exemplo mostrado lê a tabela inteira por **uma única conexão, numa única partição**, e o próprio capítulo dá a prova disso vinte páginas depois, quando `movies.rdd.getNumPartitions` devolve `Int = 1`. Para um capítulo que ensina a ler de banco de produção, isso é a diferença entre funcionar e derrubar.

**Predicate pushdown, finalmente definido.** O último parágrafo da seção é onde o termo usado na página 3 ganha significado: ao trabalhar com fonte JDBC, o Spark **empurra as condições de filtro até o RDBMS o quanto possível**, de modo que muito do dado é filtrado no nível do banco, o que acelera o filtro e reduz de forma drástica o volume que o Spark precisa ler. O texto diz que o Spark "often does this when it knows the data source can support the filtering capability" e que **Parquet** é outra fonte com essa capacidade. E encerra apontando para a seção "Catalyst Optimizer" do **capítulo 4** para ver um exemplo do que isso parece. Ou seja: a única aparição útil do Catalyst nesta leitura é um endereço, e a dívida continua aberta.

#### O resto do capítulo, fora do que o professor pediu

Da página 18 até a 41 há doze seções que **não** entram nas leituras 1 e 4. Registro em bloco, porque várias respondem teses da lista do pré-aula e é preciso saber que a resposta existe no livro, mas fora do escopo.

| Seção | Fatos que ela carrega |
|---|---|
| *Working with Structured Operations* | operações estruturadas descritas como **DSL** para manipulação distribuída; Tabela 3-7 com 14 transformações: `select`, `selectExpr`, `filter`/`where`, `distinct`/`dropDuplicates`, `sort`/`orderBy`, `limit`, `union`, `withColumn`, `withColumnRenamed`, `drop`, `sample`, `randomSplit`, `join`, `groupBy`. `join` e `groupBy` são adiados para o capítulo 4 |
| *Working with Columns* | Tabela 3-8 com **cinco** formas de referir coluna: string `"columnName"`, `col("columnName")`, `column("columnName")`, `$"columnName"` (só Scala) e `'columnName` (símbolo literal de Scala; a célula do PDF sai grafada `"columnName`). Regra: expressão de coluna exige instância de `Column`, string dá erro de tipo. `col` e `column` são sinônimos e existem em Scala e Python; o autor recomenda `col` para quem alterna as duas linguagens e `'` para quem usa só Scala. `DataFrame.col` desambigua colunas homônimas em join |
| *Working with Structured Transformations* | exemplos sobre `movies` lido de Parquet. `select` é projeção; `as` renomeia a coluna resultante; igualdade é `===` e desigualdade `=!=`; `&&` combina condições; `distinct` e `dropDuplicates` viram o **mesmo plano lógico**; `withColumn` substitui a coluna se o nome já existe; `withColumnRenamed` e `drop` são **silenciosos** quando a coluna não existe; `sample(withReplacement, fraction, seed)`; `randomSplit(Array(0.6,0.3,0.1))` normaliza pesos que não somam 1. Números do dataset: 31393 linhas, 1409 títulos, 6527 atores |
| *Working with Missing or Bad Data* | classe **`DataFrameNaFunctions`**, exposta como `df.na`. `na.drop()` equivale a `na.drop("any")`; `na.drop("all")` só remove a linha toda nula; `na.drop(Array("actor_name"))` restringe a colunas |
| *Working with Structured Actions* | Tabela 3-9: `show()`, `show(numRows)`, `show(truncate)`, `head`/`first`/`head(n)`/`take(n)`, `takeAsList(n)`, `collect`/`collectAsList` (com aviso de estouro de memória no driver), `count`, `describe`. `describe("produced_year")` devolve count 31392, mean 2002.7964449541284, stddev 6.377236851493877, min 1961, max 2012. Note a divergência interna: `count` do dataset é 31393 e o `describe` conta 31392 |
| *Introduction to Datasets* | responde duas teses do pré-aula. A unificação de DataFrame e Dataset foi feita no **Spark 2.0**; existe uma abstração só, `Dataset`, em dois sabores, tipado e não tipado; **`DataFrame` é alias de `Dataset[Row]`**. Tabela 3-10: Scala tem `Dataset[T]` e `DataFrame`, Java só `Dataset[T]`, **Python e R só `DataFrame`**, porque não têm segurança de tipo em tempo de compilação. `Dataset` usa **encoders** para converter objeto de domínio em binário compacto, o que reduz memória em cache e bytes no shuffle. Custo declarado: converter `Row` em objeto de domínio tem preço, e pode pesar com milhões de linhas. Recomendação explícita: **Dataset** para job de produção mantido por equipe de engenharia de dados, **DataFrame** para análise interativa e exploratória |
| *Using SQL in Spark SQL* | SQL no Spark é para **OLAP, não OLTP**, e não serve para baixa latência. Implementa um **subconjunto do ANSI SQL:2003**, o que permite medir com TPC-DS. Três formas de rodar SQL: CLI `./bin/spark-sql`, servidor JDBC/ODBC e programático; as duas primeiras integram o metastore do Hive e o livro cobre só a terceira. `createOrReplaceTempView("movies")` cria view de escopo de **sessão**, que morre com a sessão; `createOrReplaceGlobalTempView("movies_g")` cria view **global**, visível a todas as sessões e consultada com o prefixo `global_temp.`. Tudo fica no **catálogo de metadados**, inspecionável com `spark.catalog.listTables` (colunas name, database, description, tableType, isTemporary) e `spark.catalog.listColumns` (name, description, dataType, nullable, isPartition, isBucket). `spark.sql(...)` devolve DataFrame, e o resultado pode ser encadeado com transformações. Dá para consultar arquivo direto, sem view: ``spark.sql("SELECT * FROM parquet.`<path>/movies.parquet`")`` |
| *Writing Data Out to Storage Systems* | classe **`DataFrameWriter`**, exposta como `df.write`. Padrão: `write.format(...).mode(...).option(...).partitionBy(...).bucketBy(...).sortBy(...).save(...)`. Default também é **Parquet**. O argumento de `save` é **nome de diretório**, não de arquivo. Tabela 3-11, os quatro modos: `append`, `overwrite`, `error`/`errorIfExists` (**default**, lança erro se o destino existe) e `ignore` (não escreve, em silêncio). O número de arquivos de saída acompanha o número de partições, verificável com `movies.rdd.getNumPartitions`; `coalesce(1)` é o truque para arquivo único. Particionamento e bucketing são creditados à comunidade Hive, com a regra de bolso de que a coluna de partição deve ter **cardinalidade baixa**; `partitionBy("produced_year")` gera diretórios `produced_year=1961` até `produced_year=2012`, e esses nomes são o que permite ler menos dado depois. Promessa parcialmente cumprida: `bucketBy` e `sortBy` aparecem no padrão e nunca ganham exemplo |
| *The Trio: DataFrame, Dataset, and SQL* | Tabela 3-12, o espectro de erros: erro de **sintaxe** é runtime em SQL e tempo de compilação em DataFrame e Dataset; erro de **análise** é runtime em SQL e DataFrame, e tempo de compilação só em Dataset |
| *DataFrame Persistence* | `persist` e `unpersist`, os mesmos do RDD. A diferença declarada: como o Spark SQL conhece o schema, ele organiza o cache em **formato colunar** e aplica compressão, então um DataFrame ocupa muito menos memória que um RDD apoiado no mesmo arquivo. Cache nomeado via catálogo: `spark.catalog.cacheTable("num_df")`, com `count` forçando a materialização. Referência errada: o texto manda ver "Table 3-5" para as opções de armazenamento, e a Tabela 3-5 é a de opções de JSON |
| *Summary* | cinco marcadores. Confirma que a abstração principal do Spark SQL é o **Dataset**, que DataFrame é alias de `Dataset[Row]`, e que a API tipada só existe em linguagem de tipagem forte |
| *Spark SQL Exercises* | quatro exercícios sobre `movies.tsv` e `movie-ratings.tsv`, delimitados por tab, apontados para `chapter3/data/movies`: filmes por ano, filmes por ator, filme melhor avaliado por ano com lista de atores (exige join, e o enunciado pergunta por que duas ordens de operação dão resultados diferentes) e o par de atores que mais trabalhou junto (exige self-join) |

**A Figura 3-2**, legendada *Storage tab*, é a única outra figura do capítulo e está na página 39, dentro de *DataFrame Persistence*. Mostra uma captura da Spark UI identificada no canto direito como `Spark shell application UI`, com a barra de abas `Jobs`, `Stages`, `Storage`, `Environment`, `Executors`, `SQL`. O corpo tem o título **Storage** e, abaixo, um grupo recolhível chamado **RDDs** com uma tabela de sete colunas: `ID`, `RDD Name`, `Storage Level`, `Cached Partitions`, `Fraction Cached`, `Size in Memory`, `Size on Disk`. A única linha traz `ID 4`, `RDD Name: In-memory table num_df` (link), `Storage Level: Disk Memory Deserialized 1x Replicated`, `Cached Partitions: 12`, `Fraction Cached: 100%`, `Size in Memory: 4.1 KiB`, `Size on Disk: 0.0 B`. A figura entrega três fatos que o texto não diz: a UI agrupa cache de DataFrame **sob o rótulo RDDs**, o cache nomeado aparece como `In-memory table <nome>`, e um `spark.range(1000)` rendeu **12 partições** nesta máquina. Também dá o nível de armazenamento default do `cacheTable`, que é memória e disco, desserializado, sem replicação.

#### O que ficou marcado em Luu 3.2

Dúvidas [R4-1] a [R4-24]. Vocabulário aberto: `Row`, nullability, schema por string DDL, `inferSchema` e seu custo, `samplingRatio`, modos de parsing, `multiLine`, `allowComments`, lotes de colunas na leitura de Parquet, camada de fontes de dados e fonte customizada, predicate pushdown (parcialmente fechado aqui), paralelismo de leitura JDBC, Sakila, Catalyst.

---

### Damji, capítulo 4: Built-in Data Sources

*Spark SQL and DataFrames: Introduction to Built-in Data Sources*, 39 páginas no PDF exportado. O capítulo tem duas metades. A primeira mostra Spark SQL por dentro de uma aplicação: `spark.sql()`, tabelas, views, catálogo e cache. A segunda percorre as fontes de dados embutidas uma a uma, com `DataFrameReader` e `DataFrameWriter` como o par de construtos que organiza tudo. É o capítulo de consulta do curso, e por isso quase todo o valor dele está nas cinco tabelas de opções e nos nomes de método. Tese, quase nenhuma: a única afirmação forte é que Parquet é o formato default e recomendado. O capítulo se apresenta como continuação do 3 e antecipação do 5, que fica com as fontes externas.

#### Spark SQL dentro de uma aplicação Spark

A abertura lista seis coisas que o Spark SQL faz: serve de motor para as APIs estruturadas do capítulo 3; lê e escreve em vários formatos estruturados (a lista dada aqui é JSON, tabelas Hive, Parquet, Avro, ORC, CSV); permite consultar via conectores JDBC/ODBC a partir de ferramentas de BI (Tableau, Power BI, Talend) ou de bancos relacionais (MySQL, PostgreSQL); oferece interface programática para interagir com dado estruturado guardado como tabela ou view; oferece um shell interativo para consulta; e suporta comandos compatíveis com **ANSI SQL:2003** e com HiveQL [R5-20].

A **Figura 4-1** é a única figura do capítulo e vale mais que o texto que a apresenta. Três faixas. Em cima, cinco caixas de consumidor: Tableau, Snowflake, Talend, Power BI e uma caixa de reticências. No meio, três blocos ligados por setas de mão dupla: **Spark Application** à esquerda, **JDBC/ODBC Connectors** no centro, **Spark SQL Shell** à direita, todos apontando para a faixa larga **Spark SQL**. Embaixo, cinco caixas de fonte: **Hive Tables**, **JSON**, **Avro**, **Parquet**, **ORC**. Duas observações que o texto não faz: Snowflake aparece no desenho e não é citado uma vez no capítulo; e Hive Tables aparece no desenho como fonte embutida sem ganhar seção nenhuma, enquanto CSV, imagens e arquivos binários, que ganham seção, não estão no desenho. A figura e o corpo do capítulo listam fontes diferentes.

O mecanismo é uma linha: a `SparkSession`, introduzida no Spark 2.0, é o ponto de entrada unificado, e `spark.sql("SELECT * FROM myTableName")` devolve **um DataFrame** sobre o qual se continua a operar. Em aplicação standalone você cria a sessão pelo builder; no shell ou em notebook Databricks ela vem pronta na variável `spark`.

#### O exemplo que o capítulo desenvolve

Um só dataset atravessa a primeira metade: *Airline On-Time Performance and Causes of Flight Delays*, CSV com mais de um milhão de registros, cinco colunas. `date` é string no formato `02190925`, que mapeia para 02-19 09:25 am; `delay` é o atraso em minutos entre partida prevista e real, negativo para quem saiu adiantado; `distance` é a distância em milhas; `origin` e `destination` são códigos IATA.

A montagem: `spark.read.format("csv").option("inferSchema","true").option("header","true").load(csvFile)` seguido de `df.createOrReplaceTempView("us_delay_flights_tbl")`. Um boxe mostra a alternativa de schema por **string DDL**, `"date STRING, delay INT, distance INT, origin STRING, destination STRING"`, com a variante Python usando acento grave em volta de cada nome de coluna. É a única forma de schema demonstrada no capítulo: `StructType` só aparece como valor possível numa célula da Tabela 4-1.

Três consultas, em ordem de dificuldade. Voos com `distance > 1000` ordenados desc, e o texto conclui que os mais longos são todos Honolulu para Nova York a partir de um top 10 de dez linhas idênticas [R5-22]. Voos SFO para ORD com `delay > 120`, cujo topo é `02190925 | 1638 | SFO | ORD`. E uma com `CASE`, que rotula todo voo em `Flight_Delays`: `> 360` Very Long Delays, `>= 120 AND <= 360` Long Delays, `>= 60 AND < 120` Short Delays, `> 0 AND < 60` Tolerable Delays, `= 0` No Delays, `ELSE 'Early'` [R5-21]. Os limites em minutos casam com os intervalos em horas do texto (6 horas e 2 horas), e as faixas não deixam buraco.

A primeira consulta é reescrita na API de DataFrame de duas formas, `where(col("distance") > 1000)` e `where("distance > 1000")`, `orderBy(desc("distance"))` e `orderBy("distance", ascending=False)`, para mostrar que o resultado é o mesmo. O argumento declarado: a computação faz a mesma viagem dentro do motor, e o capítulo remete a "The Catalyst Optimizer" do capítulo 3 [R5-13].

#### Tabelas e views: onde o metadado mora

Tabela guarda dado. Junto de cada tabela vive o **metadado**: schema, descrição, nome da tabela, nome do banco, nomes de coluna, partições e a localização física do dado. Tudo isso fica num **metastore central**. O Spark não traz metastore próprio: usa por default o **metastore do Apache Hive**, em `/user/hive/warehouse`. A localização se muda pela variável de configuração `spark.sql.warehouse.dir`, que aceita storage local ou distribuído externo.

#### Tabela gerenciada contra tabela não gerenciada

Este é o ponto mais importante do capítulo e ele é dito sem ambiguidade, em quatro frases.

| | Tabela **gerenciada** (*managed*) | Tabela **não gerenciada** (*unmanaged*) |
|---|---|---|
| O que o Spark gerencia | metadado **e** dado no file store | somente o metadado |
| Quem gerencia o dado | o Spark | você, numa fonte de dados externa |
| Onde o dado fica | filesystem local, HDFS, ou object store como Amazon S3 ou Azure Blob | fonte externa; o exemplo dado é Cassandra [R5-2] |
| O que `DROP TABLE table_name` apaga | **metadado e dado** | **somente o metadado**, o dado real fica |

O capítulo usa **unmanaged** em todas as ocorrências e nunca escreve "external table", que é o nome que o SQL do Spark usa hoje e que a maior parte da documentação prefere. Também não menciona `PURGE`, nem lixeira, nem o que acontece com o diretório sob `spark.sql.warehouse.dir` depois do `DROP`. A afirmação sobre `DROP TABLE` é declarada e nunca demonstrada: em nenhuma página do capítulo alguém dropa uma tabela.

#### Criar banco, criar tabela, e a inserção que não existe

Banco: `spark.sql("CREATE DATABASE learn_spark_db")` e `spark.sql("USE learn_spark_db")`. Por default o Spark cria tabela no banco `default`; depois do `USE`, tudo que for criado na aplicação cai em `learn_spark_db`.

Tabela gerenciada, dois caminhos:

- SQL: `CREATE TABLE managed_us_delay_flights_tbl (date STRING, delay INT, distance INT, origin STRING, destination STRING)`.
- API: `spark.read.csv(csv_file, schema=schema)` e depois `flights_df.write.saveAsTable("managed_us_delay_flights_tbl")` [R5-1].

Tabela não gerenciada, dois caminhos:

- SQL: `CREATE TABLE us_delay_flights_tbl(...) USING csv OPTIONS (PATH '/databricks-datasets/.../departuredelays.csv')`. O que torna a tabela não gerenciada é a cláusula `PATH` apontando para dado que já existe.
- API: `flights_df.write.option("path", "/tmp/data/us_flights_delay").saveAsTable("us_delay_flights_tbl")`. Mesma regra: `option("path", ...)` antes de `saveAsTable()` é o que separa não gerenciada de gerenciada.

**Inserir não aparece.** Não há um `INSERT INTO` em nenhuma das 39 páginas. Dado entra na tabela de três formas, todas em bloco: `saveAsTable()`, `save()` com caminho, ou `CREATE TABLE ... USING <formato> OPTIONS (path ...)` apontando para arquivo existente. O `CREATE TABLE` em SQL da tabela gerenciada cria uma tabela **vazia** e o capítulo nunca coloca uma linha dentro dela. Quem lê termina sabendo criar tabela e não sabendo popular a que criou por SQL.

#### View temporária contra view global temporária

O capítulo abre a seção dizendo que view pode ser global (visível em todas as `SparkSession` de um cluster) ou de escopo de sessão (visível só a uma `SparkSession`), e que **as duas são temporárias**: desaparecem quando a aplicação Spark termina. A diferença entre view e tabela: view **não guarda dado**; tabela persiste depois que a aplicação termina, view não [R5-5].

Criação, em SQL e na API:

| Escopo | SQL | API de DataFrame | Como se lê |
|---|---|---|---|
| Sessão | `CREATE OR REPLACE TEMP VIEW nome AS SELECT ...` | `df.createOrReplaceTempView("nome")` | `SELECT * FROM nome`, sem prefixo |
| Global | `CREATE OR REPLACE GLOBAL TEMP VIEW nome AS SELECT ...` | `df.createOrReplaceGlobalTempView("nome")` | `SELECT * FROM global_temp.nome`, **prefixo obrigatório** |

O prefixo tem explicação: o Spark cria view global temporária num banco global temporário chamado `global_temp`. A leitura de view também funciona por `spark.read.table("us_origin_airport_JFK_tmp_view")`, que é a terceira forma de trazer tabela ou view para DataFrame que o capítulo mostra sem nunca colocar as três lado a lado (as outras duas são `spark.sql()` e `spark.table()`).

Remoção, também nos dois dialetos:

| Escopo | SQL | API |
|---|---|---|
| Global | `DROP VIEW IF EXISTS us_origin_airport_SFO_global_tmp_view` [R5-4] | `spark.catalog.dropGlobalTempView("...")` |
| Sessão | `DROP VIEW IF EXISTS us_origin_airport_JFK_tmp_view` | `spark.catalog.dropTempView("...")` |

Uma subseção inteira, *Temporary views versus global temporary views*, existe só para desfazer a confusão, e o autor a admite: a diferença é sutil e confunde quem é novo no Spark. A definição que ela dá é **outra** que a da abertura da seção [R5-3]: view temporária está amarrada a uma única `SparkSession` dentro de uma aplicação Spark; view global temporária é visível **em várias `SparkSession` dentro de uma aplicação Spark**. E acrescenta o motivo de existir mais de uma sessão: acessar e combinar dado de duas `SparkSession` que não compartilham a mesma configuração de metastore Hive [R5-6].

#### O catálogo

O metadado de toda tabela, gerenciada ou não, é capturado no **`Catalog`**, definido como abstração de alto nível do Spark SQL para guardar metadado. Duas notas de versão: as funcionalidades do `Catalog` foram expandidas no **Spark 2.x** com métodos públicos novos, e o **Spark 3.0** o estende para usar catálogo externo, assunto empurrado para o capítulo 12.

O que o capítulo mostra que dá para inspecionar por ele são três chamadas, e só três:

| Chamada | O que devolve |
|---|---|
| `spark.catalog.listDatabases()` | os bancos |
| `spark.catalog.listTables()` | as tabelas |
| `spark.catalog.listColumns("us_delay_flights_tbl")` | as colunas de uma tabela |

Mais `spark.catalog.dropTempView()` e `spark.catalog.dropGlobalTempView()`, que aparecem na seção de views e não aqui. Nenhuma saída é impressa: o capítulo fecha a seção mandando importar o notebook do repositório e experimentar. Quem consulta o registro depois não fica sabendo o que essas listagens devolvem, nem que existem `listFunctions`, `currentDatabase`, `setCurrentDatabase`, `getTable`, `cacheTable` ou `tableExists`. É a seção mais curta do capítulo para um dos itens que a ementa da aula destaca.

#### Cache de tabela SQL

Duas linhas de sintaxe, adiantadas do capítulo 5: `CACHE [LAZY] TABLE <table-name>` e `UNCACHE TABLE <table-name>`. O `LAZY` é novidade do **Spark 3.0** e significa cachear só no primeiro uso, em vez de imediatamente. Tabela e view se cacheiam como DataFrame.

#### Ler tabela para DataFrame

O enquadramento é de pipeline: engenheiro de dados popula bancos e tabelas do Spark SQL com dado limpo para consumo a jusante. Duas formas equivalentes de trazer a tabela de volta:

```
spark.sql("SELECT * FROM us_delay_flights_tbl")
spark.table("us_delay_flights_tbl")
```

#### `DataFrameReader`

Construto central de leitura. Formato definido e padrão recomendado de uso:

```
DataFrameReader.format(args).option("key", "value").schema(args).load()
```

Onde pode e onde não pode ser usado: **só se chega a um `DataFrameReader` por uma instância de `SparkSession`**, e não dá para instanciar um. Os dois pontos de acesso são `SparkSession.read`, que devolve o handle para leitura de fonte estática, e `SparkSession.readStream`, que devolve instância para fonte de streaming. Uma nota fecha a regra de schema: em fonte Parquet estática não é preciso schema, porque o metadado do Parquet costuma trazê-lo; em **fonte de streaming o schema é obrigatório**.

**Tabela 4-1. Métodos, argumentos e opções do `DataFrameReader`**

| Método | Argumentos | Descrição |
|---|---|---|
| `format()` | `"parquet"`, `"csv"`, `"txt"`, `"json"`, `"jdbc"`, `"orc"`, `"avro"`, etc. [R5-8] | se o método não for especificado, o default é Parquet ou o que estiver em `spark.sql.sources.default` |
| `option()` | `("mode", {PERMISSIVE \| FAILFAST \| DROPMALFORMED})`, `("inferSchema", {true \| false})`, `("path", "path_file_data_source")` | série de pares chave/valor. A documentação do Spark mostra exemplos e explica os modos e suas ações. O modo default é `PERMISSIVE`. As opções `"inferSchema"` e `"mode"` são específicas dos formatos JSON e CSV [R5-7] [R5-9] |
| `schema()` | string DDL ou `StructType`, por exemplo `'A INT, B STRING'` ou `StructType(...)` | para JSON ou CSV dá para pedir inferência pelo `option()`. Em geral, fornecer schema para qualquer formato torna o load mais rápido e garante que o dado obedece ao schema esperado |
| `load()` | `"/path/to/data/source"` | o caminho da fonte. Pode ficar **vazio** se o caminho estiver em `option("path", "...")` |

Exemplos que o capítulo dá: `spark.read.format("parquet").load(file)`; `spark.read.load(file)`, com `format` omitido porque Parquet é o default; CSV com `inferSchema`, `header` e `mode` `PERMISSIVE`; JSON com só `format` e `load`.

#### `DataFrameWriter`

Faz o inverso, e a assimetria de acesso importa: **não se chega ao writer pela `SparkSession`**, e sim pelo DataFrame que se quer salvar. Os dois pontos de acesso são `DataFrame.write` e `DataFrame.writeStream`. Dois padrões recomendados de uso, impressos assim [R5-16]:

```
DataFrameWriter.format(args).option(args).bucketBy(args).partitionBy(args).save(path)
DataFrameWriter.format(args).option(args).sortBy(args).saveAsTable(table)
```

**Tabela 4-2. Métodos, argumentos e opções do `DataFrameWriter`**

| Método | Argumentos | Descrição |
|---|---|---|
| `format()` | `"parquet"`, `"csv"`, `"txt"`, `"json"`, `"jdbc"`, `"orc"`, `"avro"`, etc. | se não especificado, o default é Parquet ou o que estiver em `spark.sql.sources.default` |
| `option()` | `("mode", {append \| overwrite \| ignore \| error ou errorifexists})`, `("mode", {SaveMode.Overwrite \| SaveMode.Append, SaveMode.Ignore, SaveMode.ErrorIfExists})`, `("path", "path_to_write_to")` | série de pares chave/valor. Método sobrecarregado. Os modos default são `error` ou `errorifexists` e `SaveMode.ErrorIfExists`: lançam exceção em runtime se o dado já existe |
| `bucketBy()` | `(numBuckets, col, col..., coln)` | número de buckets e nomes das colunas de bucket. Usa o esquema de bucketing do Hive num filesystem |
| `save()` | `"/path/to/data/source"` | caminho de escrita. Pode ficar vazio se estiver em `option("path", "...")` |
| `saveAsTable()` | `"table_name"` | a tabela onde salvar |

Duas ausências na tabela que valem tanto quanto o que está nela: **`partitionBy` e `sortBy` aparecem nos padrões de uso e não têm linha na Tabela 4-2**, não recebem uma frase de explicação e não são usados em nenhum exemplo do capítulo. Bucketing recebe uma linha que remete ao esquema do Hive sem definir o que é um bucket. O exemplo único da seção é `df.write.format("json").mode("overwrite").save(location)`.

#### Parquet, e por que o capítulo o recomenda

Sim, o capítulo afirma isso, três vezes, e dá motivos. Na nota do `DataFrameReader`: Parquet é a fonte de dados **default e preferida** do Spark porque é eficiente, usa **armazenamento colunar** e emprega um **algoritmo de compressão rápido**, com promessa de benefícios adicionais como *columnar pushdown* quando o Catalyst for tratado em profundidade [R5-12] [R5-13]. Na abertura da seção: formato colunar open source, suportado e muito usado por vários frameworks de big data, com muitas otimizações de I/O, e a recomendação de que, **depois de transformar e limpar o dado, você salve os DataFrames em Parquet** para consumo a jusante. E no fecho da seção: formato de fonte embutida preferido e default, adotado por muitos outros frameworks, recomendado para processos de ETL e ingestão. Um parêntese acrescenta que Parquet é também o formato de tabela aberto default do **Delta Lake**, adiado para o capítulo 9.

Anatomia do diretório Parquet, que é informação útil e o capítulo dá: diretório com arquivos de dado, metadado, arquivos comprimidos e arquivos de status. O metadado fica no **footer** e traz a versão do formato, o schema e dado de coluna como o caminho. O listing de exemplo tem `_SUCCESS`, `_committed_1799640464332036264`, `_started_1799640464332036264` e `part-00000-tid-...-c000.snappy.parquet`.

| Operação | Como o capítulo faz |
|---|---|
| Ler para DataFrame | `spark.read.format("parquet").load(file)`, ou só `spark.read.load(file)` |
| Ler para tabela Spark SQL | `CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl USING parquet OPTIONS (path "...")`, e depois `spark.sql("SELECT * FROM us_delay_flights_tbl").show()` |
| Escrever arquivo | `df.write.format("parquet").mode("overwrite").option("compression","snappy").save("/tmp/data/parquet/df_parquet")` |
| Escrever tabela | `df.write.mode("overwrite").saveAsTable("us_delay_flights_tbl")`, que cria uma tabela **gerenciada** |
| Opções | **nenhuma tabela de opções** |

Duas notas do capítulo: schema não é necessário em Parquet estático porque ele guarda o schema no metadado; e se você omitir `format()` na escrita, o DataFrame **ainda sai como Parquet**. O listing de saída mostra `_SUCCESS` e um único `part-00000-<...>-c000.snappy.parquet` de 966 bytes, com o aviso de que normalmente sairia mais de uma dezena de arquivos.

O formato que o capítulo chama de default e recomendado é justamente o único dos sete que **não** ganha tabela de opções. Nada de `mergeSchema`, nada de `spark.sql.parquet.*`, nada sobre os codecs disponíveis além de `snappy` aparecer num exemplo.

#### JSON, no Damji

Duas representações, as duas suportadas: **single-line mode**, em que cada linha é um objeto JSON, e **multiline mode**, em que o objeto inteiro de várias linhas é um objeto só. Para ler no segundo, `multiLine` igual a `true` no `option()`. O enquadramento histórico é de uma linha: ganhou tração por ser mais fácil de ler e de parsear que XML.

| Operação | Como o capítulo faz |
|---|---|
| Ler para DataFrame | `spark.read.format("json").load(file)` |
| Ler para tabela | `CREATE OR REPLACE TEMPORARY VIEW us_delay_flights_tbl USING json OPTIONS (path "...")` |
| Escrever | `df.write.format("json").mode("overwrite").option("compression","snappy").save("/tmp/data/json/df_json")` [R5-11] |
| Saída | `_SUCCESS` e `part-00000-<...>-c000.json`, 71 bytes |

**Tabela 4-3. Opções de JSON para `DataFrameReader` e `DataFrameWriter`**

| Propriedade | Valores | Significado | Escopo |
|---|---|---|---|
| `compression` | `none`, `uncompressed`, `bzip2`, `deflate`, `gzip`, `lz4`, `snappy` | usa este codec na escrita. A leitura **só detecta** a compressão ou o codec pela extensão do arquivo | escrita |
| `dateFormat` | `yyyy-MM-dd` ou `DateTimeFormatter` | usa este formato ou qualquer formato do `DateTimeFormatter` do Java | leitura e escrita |
| `multiLine` | `true`, `false` | usa modo multilinha. Default `false`, ou seja, single-line | leitura |
| `allowUnquotedFieldNames` | `true`, `false` | aceita nome de campo JSON sem aspas. Default `false` | leitura |

#### CSV, no Damji

Cada campo delimitado por vírgula, cada linha um registro. A vírgula é o separador default mas dá para usar outro delimitador quando a vírgula faz parte do dado. O argumento de popularidade é de planilha: quem gera CSV é o analista.

| Operação | Como o capítulo faz |
|---|---|
| Ler para DataFrame | `spark.read.format("csv").schema(schema).option("header","true").option("mode","FAILFAST").option("nullValue","").load(file)` [R5-10] |
| Ler para tabela | `CREATE OR REPLACE TEMPORARY VIEW ... USING csv OPTIONS (path "...", header "true", inferSchema "true", mode "FAILFAST")` |
| Escrever | `df.write.format("csv").mode("overwrite").save("/tmp/data/csv/df_csv")` |
| Saída | `_SUCCESS` e `part-00000-251690eb-<...>-c000.`, 36 bytes, chamada de comprimida sem nenhum codec ter sido setado [R5-11] |

Detalhe que o capítulo não comenta: a versão em DataFrame **declara** o schema e usa `FAILFAST`, e a versão em SQL da mesma leitura **infere** o schema e usa `FAILFAST` junto. As duas são apresentadas como equivalentes.

**Tabela 4-4. Opções de CSV para `DataFrameReader` e `DataFrameWriter`**

| Propriedade | Valores | Significado | Escopo |
|---|---|---|---|
| `compression` | `none`, `bzip2`, `deflate`, `gzip`, `lz4`, `snappy` | usa este codec na escrita | escrita |
| `dateFormat` | `yyyy-MM-dd` ou `DateTimeFormatter` | usa este formato ou qualquer formato do `DateTimeFormatter` do Java | leitura e escrita |
| `multiLine` | `true`, `false` | usa modo multilinha. Default `false` | leitura |
| `inferSchema` | `true`, `false` | se `true`, o Spark determina os tipos das colunas. Default `false` | leitura |
| `sep` | qualquer caractere | separa valores de coluna numa linha. Delimitador default é a vírgula | leitura e escrita |
| `escape` | qualquer caractere | escapa aspas. Default é `\` | leitura e escrita |
| `header` | `true`, `false` | indica se a primeira linha é cabeçalho com os nomes de coluna. Default `false` | leitura e escrita |

A lista de codecs de CSV difere da de JSON em um item: JSON aceita `uncompressed`, CSV não. O capítulo avisa que CSV tem muitas opções e manda ir na documentação para a lista completa.

#### Avro

Introduzido como fonte embutida no **Spark 2.4** [R5-14]. Usado, por exemplo, pelo Apache Kafka para serializar e desserializar mensagem. Benefícios declarados: mapeamento direto para JSON, velocidade e eficiência, e bindings para muitas linguagens.

| Operação | Como o capítulo faz |
|---|---|
| Ler para DataFrame | `spark.read.format("avro").load(".../avro/*")` |
| Ler para tabela | `CREATE OR REPLACE TEMPORARY VIEW episode_tbl USING avro OPTIONS (path "...")` |
| Escrever | `df.write.format("avro").mode("overwrite").save("/tmp/data/avro/df_avro")` |
| Saída | `_SUCCESS` e `part-00000-ffdf70f4-<...>-c000`, 526 bytes |

**Tabela 4-5. Opções de Avro para `DataFrameReader` e `DataFrameWriter`**

| Propriedade | Default | Significado | Escopo |
|---|---|---|---|
| `avroSchema` | `None` | schema Avro opcional fornecido pelo usuário em JSON. O tipo e o nome dos campos do registro devem casar com o dado Avro de entrada ou com o dado Catalyst (tipo interno do Spark), senão a leitura ou escrita **falha** | leitura e escrita |
| `recordName` | `topLevelRecord` | nome do registro de topo no resultado da escrita, exigido pela especificação do Avro | escrita |
| `recordNamespace` | `""` | namespace do registro no resultado da escrita | escrita |
| `ignoreExtension` | `true` | se ligada, **todos** os arquivos são carregados, com e sem a extensão `.avro`. Se não, arquivos sem `.avro` são ignorados | leitura |
| `compression` | `snappy` | codec de compressão na escrita. Os suportados são `uncompressed`, `snappy`, `deflate`, `bzip2` e `xz`. Se a opção não for setada, vale o valor de `spark.sql.avro.compression.codec` | escrita |

É a única tabela do capítulo com coluna de **default**, e a única que expõe uma variável de configuração dentro de si. `ignoreExtension` está depreciada desde o Spark 3.0 em favor de `pathGlobFilter` e `recursiveFileLookup`, e o capítulo, que cobre 3.0 e apresenta as duas opções novas na seção de arquivos binários, não faz a ligação.

#### ORC, no Damji

Formato colunar otimizado. O que o capítulo entrega aqui é diferente dos outros: em vez de opções, **duas variáveis de configuração** [R5-15]. Quando `spark.sql.orc.impl` está em `native` e `spark.sql.orc.enableVectorizedReader` está em `true`, o Spark usa o **leitor vetorizado** de ORC. Leitor vetorizado é definido, e a definição é boa: lê blocos de linhas, tipicamente **1.024 por bloco**, em vez de uma linha por vez, o que enxuga as operações e reduz uso de CPU em scan, filtro, agregação e join. Para tabela Hive ORC SerDe criada com `USING HIVE OPTIONS (fileFormat 'ORC')`, o leitor vetorizado entra quando `spark.sql.hive.convertMetastoreOrc` está em `true`.

| Operação | Como o capítulo faz |
|---|---|
| Ler para DataFrame | `spark.read.format("orc").load(file)`, e a variante Python `spark.read.format("orc").option("path", file).load()`, que é a única demonstração no capítulo de `load()` vazio com o caminho na opção |
| Ler para tabela | `CREATE OR REPLACE TEMPORARY VIEW ... USING orc OPTIONS (path "...")` |
| Escrever | `df.write.format("orc").mode("overwrite").option("compression","snappy").save("/tmp/data/orc/df_orc")` |
| Saída | `_SUCCESS` e `part-00000-<...>-c000.snappy.orc`, 547 bytes |
| Opções | **nenhuma tabela de opções** |

#### Imagens

Fonte de dados introduzida na comunidade no **Spark 2.4** para suportar frameworks de deep learning e machine learning como TensorFlow e PyTorch. O argumento é de visão computacional: carregar e processar conjunto de imagens importa.

Leitura: `spark.read.format("image").load(imageDir)`. Em Scala pede `import org.apache.spark.ml.source.image`; em Python, `from pyspark.ml import image`. O schema impresso, que é o valor da seção:

```
root
 |-- image: struct
 |    |-- origin: string
 |    |-- height: integer
 |    |-- width: integer
 |    |-- nChannels: integer
 |    |-- mode: integer
 |    |-- data: binary
 |-- label: integer
```

O `select("image.height","image.width","image.nChannels","image.mode","label")` devolve cinco linhas com `288`, `384`, `3` e `16` e `label` variando entre 0 e 1 [R5-23]. Não há escrita, não há tabela de opções, não há criação de tabela SQL a partir de imagem: é a seção mais fina do capítulo, e a única fonte cujo bloco não segue o roteiro de ler para DataFrame, ler para tabela e escrever.

#### Arquivos binários

**Spark 3.0** adiciona suporte a arquivo binário como fonte. O `DataFrameReader` converte **cada arquivo binário em uma linha** de DataFrame, com conteúdo cru e metadado do arquivo. As colunas produzidas, com tipo:

| Coluna | Tipo |
|---|---|
| `path` | `StringType` |
| `modificationTime` | `TimestampType` |
| `length` | `LongType` |
| `content` | `BinaryType` |

Leitura com `format("binaryFile")` e duas opções nomeadas no corpo do texto, não em tabela:

- **`pathGlobFilter`**, por exemplo `"*.jpg"`, carrega os arquivos que casam com o padrão **preservando** o comportamento de descoberta de partição.
- **`recursiveFileLookup`** em `"true"` **ignora** a descoberta de partição no diretório. O capítulo registra a consequência concreta: com essa opção ligada, a coluna `label` **desaparece** da saída. É a única pista, em todo o capítulo, de que `label` vinha de descoberta de partição e não do arquivo.

A saída mostra `file:/Users/jules...`, `2020-02-12 12:04:24`, `length` entre 54475 e 55037, e `content` começando em `[FF D8 FF E0 00 1...]`, que é assinatura de JPEG. E uma limitação que o capítulo declara e que continua valendo: a fonte de arquivo binário **não suporta escrever** um DataFrame de volta ao formato original.

#### O que o autor resume no fim do capítulo 4

Cinco itens, todos na chave de interoperabilidade entre a API de DataFrame e o Spark SQL: criar tabela gerenciada e não gerenciada por Spark SQL e pela API de DataFrame; ler e escrever nas várias fontes embutidas e formatos de arquivo; usar a interface programática `spark.sql` para consultar dado estruturado guardado como tabela ou view; percorrer o `Catalog` do Spark para inspecionar metadado de tabela e view; e usar as APIs `DataFrameWriter` e `DataFrameReader`. Fecha remetendo ao capítulo 5, que trata das fontes externas da Figura 4-1.

A afirmação mais forte do resumo da segunda metade está no fecho da seção de fontes, não no *Summary*: seja pela API de DataFrame ou por SQL, as consultas **produzem resultados idênticos** [R5-18].

#### O que ficou marcado em Damji 4

Dúvidas [R5-1] a [R5-23]. O padrão de erro deste capítulo é diferente do dos capítulos 1 e 2 do mesmo livro: quase não há número sem fonte, porque quase não há número. O que há é **código impresso que não roda**, **tabela que contradiz o texto ao lado** e **terceirização da explicação para a documentação** exatamente nos pontos que mais doem em produção, tratamento de registro ruim à frente de todos. O capítulo cumpre bem o papel de referência de sintaxe e falha como referência de comportamento.

Vocabulário aberto: metastore, metastore do Hive, catálogo externo, SerDe, leitor vetorizado, bucket e bucketing (esquema do Hive citado, nunca definido), `partitionBy`, `sortBy`, `columnar pushdown`, `PERMISSIVE`, `DROPMALFORMED`, `FAILFAST`, descoberta de partição, padrão glob, `LAZY` no cache, dado Catalyst, os codecs `snappy`, `deflate`, `bzip2`, `lz4`, `gzip` e `xz` (listados sem uma linha de comparação entre eles), e o inteiro `mode` do schema de imagem.


---

### Chadha, capítulo 1: Data Ingestion and Data Extraction

*Data Ingestion and Data Extraction with Apache Spark*, 35 páginas no recorte. **O ano não está no PDF.** O recorte é uma impressão da plataforma da O'Reilly, sem folha de créditos, sem página de copyright e sem declaração de edição: o único dado bibliográfico impresso é o ISBN no rodapé de todas as 35 páginas (`9781837633357`) e o código interno da Packt (`B19798`). O que o próprio capítulo entrega como datação indireta: todos os quinze links de *See also* estão rotulados **Spark 3.4.0**, e o pacote de XML pedido é `spark-xml_2.12:0.16.0`, de Scala 2.12. Isso põe o texto no ciclo do Spark 3.4, ou seja, 2023 ou depois. Fora do PDF, esse ISBN corresponde à primeira edição publicada pela Packt em 2024, e é isso que o pré-aula deveria registrar, com a ressalva de que a confirmação tem de sair da folha de créditos, que não está neste recorte.

O formato é livro de receitas, e o molde é rígido: cada receita abre com um parágrafo de contexto, segue com **How to do it...** numerado, e fecha com uma ou duas seções de complemento (**Common issues**, **There's more...**) e uma lista de **See also**. São sete receitas: CSV, JSON, Parquet, XML, estruturas aninhadas, texto e escrita. Toda receita começa criando uma `SparkSession` e termina com `spark.stop()`, o que faz do capítulo uma sequência de sete blocos independentes, sem estado compartilhado. **Não há uma única figura nem uma única tabela em 35 páginas**: só prosa, blocos de código Python e saídas em ASCII. Isso o separa dos outros dois livros da aula, onde a figura carrega parte do argumento.

O achado grande vem antes de qualquer receita, e inverte o palpite do pré-aula. **Este é o capítulo 1 de um livro sobre Databricks que não usa Databricks em lugar nenhum.** Não pede conta, não pede workspace, não pede runtime, não menciona a Community Edition, não usa notebook Databricks. O ambiente declarado é `docker-compose` local com JupyterLab em `http://127.0.0.1:8888/lab` e um Spark standalone em `spark://spark-master:7077`. Auto Loader, DBFS, Unity Catalog, `dbutils`, `%fs`, magic command e Delta Lake: **zero menções, nenhuma delas**. O capítulo é Spark aberto do começo ao fim. A única coisa de Databricks que aparece é a biblioteca `spark-xml`, e ela aparece **rotulada de recurso embutido do Apache Spark**, que é exatamente a confusão que valia caçar [R3-1] [R3-2].

#### Requisitos e ambiente

A seção *Technical requirements* tem seis linhas e nenhuma versão. Manda subir as imagens do `docker-compose`, abrir o JupyterLab no localhost, clonar o repositório do livro, e rodar `docker-compose stop` no fim. O código e o dado ficam em `github.com/PacktPublishing/Data-Engineering-with-Databricks-Cookbook/tree/main/Chapter01`. Nunca declara versão de Spark, nunca declara versão de Python, nunca descreve a topologia do cluster que o compose sobe (quantos workers, quanta memória). Cada receita repete `.config("spark.executor.memory", "512m")` e `spark.sparkContext.setLogLevel("ERROR")` sem uma palavra sobre por que 512 MB nem sobre o que o nível de log esconde [R3-29].

Os datasets, todos por caminho relativo a partir de um diretório de notebooks: `../data/netflix_titles.csv`, `../data/nobel_prizes.json`, `../data/nobel_prizes.xml`, `../data/recipes.parquet`, `../data/partitioned_recipes`, `../data/Stanford Question Answering Dataset.json` (com espaços no nome, que é convite a erro de caminho) e `../data/Reviews.csv`. A escrita sempre vai para `../data/data_lake/<nome>`. Nenhuma procedência, nenhuma licença, nenhum tamanho de arquivo declarado em nenhum dos sete.

#### Receita 1, CSV: sete passos e um catálogo de opções

O caminho feliz tem sete passos. Cria a sessão; lê com `spark.read.format("csv").option("header", "true").load(...)`; mostra com `show()`; declara schema com `StructType` e `StructField` (doze campos do dataset da Netflix, com `DateType` em `date_added` e `IntegerType` em `release_year`); relê passando `.schema(schema)`; mostra de novo; encerra com `spark.stop()`. Uma NOTE avulsa lembra que dá para pôr `header` em `false`.

Duas coisas ficam por dizer, e as duas custam caro em uso real. A primeira: a leitura do passo 2, com `header` e nada mais, devolve **todas as doze colunas como string**, e o capítulo nunca diz isso, nem mostra o `printSchema()` que revelaria. A segunda: o texto do passo 4 descreve um schema de três colunas, `"name"`, `"age"` e `"gender"`, e o código que vem embaixo declara os doze campos da Netflix. É narração de um exemplo com código de outro [R3-14]. No passo 6 ele afirma que `show()` mostra "as primeiras cinco linhas", contra o passo 3 do mesmo caminho, que dizia vinte [R3-15].

A seção **Common issues faced while working with CSV data** traz três problemas com solução, e é aqui que estão os dois erros de opção mais caros do capítulo:

| Problema declarado | Opção receitada | O que está errado |
|---|---|---|
| delimitador presente dentro do dado | `option("escapeQuotes", "true")` | `escapeQuotes` é opção de **escrita** de CSV, não de leitura. Na leitura o que resolve é `quote`, `escape` e `unescapedQuoteHandling`, nenhum dos três citado no capítulo [R3-4] |
| nulo e vazio mal tratados | `option("nullValue", "null")` e `option("emptyValue", "")` | a prosa escreve `emptyValue`, que é o nome certo, e o código logo abaixo escreve `option("emptyValues", "")`, no plural. Opção desconhecida é ignorada em silêncio [R3-5] |
| formato de data diferente | `option("dateFormat", "LLLL d, y")` | funciona, mas só porque o schema declarado tem `DateType`. O padrão `LLLL` nunca é explicado, e o mesmo par de opções reaparece na receita de escrita sobre uma leitura **sem** schema, onde não faz nada [R3-28] |

A NOTE que fecha a receita é a passagem mais importante do capítulo em matéria de conceito, e tem quatro frases: quando o passo 2 usou a API de `read`, **o Spark não executou nenhum job**, porque a avaliação é preguiçosa e a execução das transformações espera uma ação. Diz que isso permite otimizar o plano de execução e recuperar de falhas, e que também atrapalha depurar. É toda a teoria do capítulo. Não há uma linha sobre Catalyst, sobre `explain()`, sobre plano lógico e físico, nem uma lista de quais operações são ação e quais são transformação.

O **There's more...** traz três blocos. Opções de leitura: `delimiter` (com o exemplo `option("delimiter", "|")`) e `inferSchema` (`option("inferSchema", "true")`), este último apresentado como conveniência, em uma linha, **sem uma palavra sobre custo** [R3-13]. Dado faltante ou malformado: `nullValue` (com o exemplo `option("nullValue", "NA")`) e `mode`, e aqui a descrição de `PERMISSIVE` sai errada, "para ignorar erros de parse e seguir processando o arquivo", quando `PERMISSIVE` é o default, não ignora nada e sim anula os campos e guarda o registro cru; `DROPMALFORMED` e `FAILFAST` nunca são nomeados, e o default nunca é declarado [R3-7]. Arquivos grandes: manda usar `spark.read.csv()` com `maxColumns` e `maxCharsPerColumn` "para limitar quantas colunas e caracteres por coluna o Spark lê de cada vez", o que descreve guarda-corpo de parser como se fosse controle de memória [R3-8], e sugere `spark.readStream.csv()` para ler CSV grande "em tempo real conforme é lido do disco", trocando leitura de arquivo estático por streaming [R3-9].

#### Receita 2, JSON: `multiLine`, achatamento e registro corrompido

Sete passos. Lê com `format("json")` e `option("multiLine", "true")`; inspeciona com `printSchema()` e `show()`; achata o array `laureates` com `withColumn("laureates", explode(col("laureates")))` seguido de `select` com notação de ponto (`col("laureates.id")`, `col("laureates.firstname")`, `col("laureates.surname")`, `col("laureates.share")`, `col("laureates.motivation")`); declara schema com `ArrayType(StructType([...]))` para o campo aninhado; relê com `.schema(json_schema)`, `.option("mode", "PERMISSIVE")` e `.option("columnNameOfCorruptRecord", "corrupt_record")`.

`multiLine` em `true` aparece aqui, aparece de novo na receita de aninhados e aparece até em um `read.format("csv")` na receita de texto. **O capítulo nunca diz que `multiLine` torna o arquivo indivisível**, o que colapsa o paralelismo para uma task por arquivo. Em livro que se vende como aplicado, e cujo próprio *There's more* de CSV se preocupa com arquivo grande, é uma omissão que dói [R3-24].

O tratamento de registro corrompido tem um defeito silencioso: `columnNameOfCorruptRecord` está setado como `"corrupt_record"`, mas o `json_schema` declarado logo acima **não tem um campo `corrupt_record`**. Com schema explícito, o Spark só materializa a coluna de registro corrompido se ela estiver no schema. A receita, como impressa, não entrega a coluna que promete [R3-6]. E o **There's more...** anuncia "vamos ver alguns em mais detalhe" e, sob o título *Handling corrupt data*, reimprime o mesmo bloco de código do passo 6, sem acrescentar nada.

O resto do *There's more* é catálogo de função, e é útil: `get_json_object()`, que extrai de uma coluna de string JSON por expressão de caminho (`"$.name"`) e devolve string, com `cast()` no exemplo; `json_tuple()`, que extrai vários campos de uma vez (`json_tuple("json_data", "name", "age")`) e devolve tupla, renomeada com `alias()`. A gramática dessas expressões de caminho nunca é explicada: `$.name` aparece e sai. Depois `collect_list()` para agrupar arrays em um array só e `flatten()` para fundir os arrays internos, com uma NOTE certeira de que `flatten()` só serve para coluna de array e que aninhamento de vários níveis pede `explode()` antes. As duas saídas impressas se contradizem na ordem: o `collect_list` sai com o id 2 antes do id 1 (`[[[7, 8], [9, 10], [11, 12]], [[1, 2], [3, 4], [5, 6]]]`) e o `flatten` do mesmo objeto sai começando pelo 1 [R3-18].

#### Receita 3, Parquet: partição descoberta e `mergeSchema`

A receita mais curta, cinco passos: sessão, `spark.read.format("parquet").load(...)`, `printSchema()`, `show()`, `stop()`. O parágrafo de abertura descreve Parquet como formato de armazenamento colunar para grandes conjuntos, otimizado para compressão eficiente e codificação de tipos complexos. **Não diz que é o formato default do Spark, não recomenda usá-lo e não compara com CSV ou JSON em nenhuma dimensão.** Todo `read` e todo `write` do capítulo passa `format(...)` explícito, então a questão do default nunca aparece.

O valor está em *Common scenarios*. **Leitura de dado particionado**: basta apontar `spark.read` para o diretório e o Spark reconhece o conjunto como particionado e carrega de acordo (descoberta de partição, embora o capítulo não use o termo). Dá também o recorte por curinga: `.load("../data/partitioned_recipes/DatePublished=2020-01*")`. Aqui há duas trapalhadas de uma vez: a prosa diz que as receitas estão particionadas "por categoria de receita" enquanto o curinga filtra por `DatePublished`, e o primeiro `load` escreve o caminho como `partioned_recipes`, sem o segundo `t`, contra `partitioned_recipes` na prosa e no snippet de `mergeSchema` [R3-16].

**Schema merging**: por default o Spark não funde schemas diferentes, porque `spark.sql.parquet.mergeSchema` está em `false`; com `option("mergeSchema", "true")` ele tenta um schema unificado. O exemplo narrado é bom de conceito: lendo todas as partições o Spark inferiu `ReviewCount` e não `Images`; lendo só `2020-01*` inferiu `Images` e não `ReviewCount`; com `mergeSchema` em `true` os dois aparecem. É narração pura, sem um `printSchema()` impresso para conferir [R3-17]. A NOTE que fecha é a única advertência de custo de todo o capítulo: fundir schema **pode ser operação caro**, pode não ser possível (e aí o Spark levanta erro), e pode não dar o schema ótimo, caso em que resta tratar a evolução de schema à mão. Como fazer isso à mão nunca é dito.

#### Receita 4, XML: a receita que confunde Databricks com Spark

O parágrafo de abertura promete explorar "a fonte de dados XML **embutida**", e a NOTE imediatamente abaixo diz que é preciso instalar o pacote `spark-xml`, "uma biblioteca de terceiros para o Apache Spark lançada pela Databricks". Embutida e de terceiros, em duas frases consecutivas [R3-1]. O comando de instalação impresso é `$SPARK_HOME/bin/spark-shell` seguido de `packages com.databricks:spark-xml_2.12:0.16.0****`, e o prefixo do `packages` saiu como um sinal tipográfico longo no lugar dos dois hifens de `--packages`, com quatro asteriscos pendurados no fim, mirando o shell de Scala em um capítulo inteiramente em Python, e duplicando o que o passo 1 já resolve com `.config('spark.jars.packages', 'com.databricks:spark-xml_2.12:0.16.0')` [R3-26]. Fecha dizendo que o pacote já está nas imagens Docker do livro, o que quer dizer que ninguém que siga o livro vai descobrir que a instalação impressa não roda.

O `format` usado é `com.databricks.spark.xml`, e não `"xml"`, embora a prosa do passo 2 afirme usar `spark.read.format()` "com o argumento `"xml"`". A opção de leitura é `rowTag`, aqui `"row"`, e o caminho impresso está corrompido: `.load("..//ta/nobel_prizes.xml")`, que deveria ser `../data/nobel_prizes.xml` [R3-26]. Para XML mais complexo o texto manda usar `option("rootTag", "tagname")` "para especificar o elemento que deve ser tratado como raiz do DataFrame", e isso está errado: no `spark-xml`, `rootTag` é opção de **escrita**; na leitura quem manda é `rowTag` [R3-3].

O resto da receita ensina acesso a tipo complexo: `select("category", "year")`; `getItem`, apresentado como o método que extrai um único elemento por índice, com `col("laureates").getItem(0)`; o operador ponto para elemento filho; e o exemplo `col("laureates").getItem(0).id`. A explicação desse exemplo está trocada: diz que "seleciona `category` e `year` **do elemento do array `laureates`**", quando as duas são colunas de topo. Depois repete, palavra por palavra, o achatamento com `explode` da receita de JSON, e repete a declaração de schema com `ArrayType`.

O **There's more...** lista quatro opções e as credita ao Apache Spark: `excludeAttribute` (lista de atributos a excluir do DataFrame), `inferSchema`, `ignoreSurroundingSpaces` e `mode`. São opções do `spark-xml`, não do Spark [R3-2]. E o *See also* aponta para `github.com/databricks/spark-xml`, o que mostra que o autor sabia da origem.

Nota de realidade, e é de fora do capítulo: hoje a afirmação de "fonte embutida" ficou certa e o código ficou velho. O `spark-xml` foi doado ao projeto Apache e o **XML virou fonte nativa no Spark 4.0**, com `format("xml")`; o repositório da Databricks está arquivado. Quem seguir esta receita em Spark 4 vai instalar um pacote aposentado, compilado para Scala 2.12, em um runtime que só aceita 2.13.

#### Receita 5, estruturas aninhadas: `explode` e as funções de coleção

Cinco passos sobre o dataset do SQuAD. O passo 1 é o único lugar do capítulo com uma definição de arquitetura: `SparkSession` como ponto de entrada unificado que dá acesso a **resilient distributed datasets (RDDs)**, DataFrames, datasets, consultas SQL e streaming; e `SparkContext` como ponto de entrada de qualquer funcionalidade Spark, representando a conexão com o cluster e responsável por coordenar e distribuir operações. **É a única aparição de RDD em 35 páginas, e ela é a sigla expandida dentro de uma lista.** Nada de dependências, partições, função de computação, linhagem ou recomputação. "Datasets" aparece na mesma lista, também só como palavra.

O trabalho: `explode("paragraphs")` com `alias`, e depois um segundo `explode(col("paragraphs.qas"))` no mesmo `select`, mais notação de ponto para `paragraphs.context`. Em seguida `array_distinct("questions.answers")` para tirar duplicata dentro do array sem desmontá-lo, e é aqui que o texto chama `array_distinct` de **higher-order function (HOF)**, com a sigla expandida e sem definição. `array_distinct` não recebe função nenhuma: HOF de coleção no Spark são `transform`, `filter`, `exists`, `aggregate`. O *See also* da própria receita aponta para a âncora `working-with-nested-data-using-higher-order-functions` da documentação, ou seja, a página certa estava aberta [R3-19].

*Common issues and considerations* tem três itens de verdade úteis:

- **`explode()` gera muitas linhas** e isso pode ser ineficiente. O conselho é evitar quando não precisar achatar, ou aplicar sobre um subconjunto, ou agregar antes de explodir. É o conselho de performance mais concreto do capítulo.
- **Notação de ponto é traiçoeira em aninhamento profundo.** Manda usar `getItem` no lugar, com `col("answers").getItem(0).getField("text")`. O conselho briga com o código: `getItem` indexa array e `getField` acessa campo de struct, não substituem o ponto em struct, e a própria receita de XML escreve `getItem(0).id`, misturando os dois [R3-20].
- **Aninhado com nulo** quebra extração. Usar `isNull` e `isNotNull`, com `.filter(col("answers").getItem(0).getField("text").isNotNull())`.

O *There's more* fecha com quatro funções e saídas impressas: `array_contains("fruits", "apple")` devolvendo booleano por linha; `map_keys` e `map_values` extraindo chave e valor de coluna de mapa; e `explode_outer`, com a distinção que importa, ele **devolve linha para o array nulo** e o `explode` não, provada com a saída que traz `null` como sexta linha. O snippet de mapa tem três defeitos: importa só `map_keys` e chama `map_values`; cria uma segunda `SparkSession` com `appName("map_keys_example")` via `getOrCreate()`, que devolve a sessão que já existe e descarta o nome em silêncio; e monta o mapa com valores de tipos diferentes (`"Alice"` e `28`) quando `MapType` tem um tipo de valor só [R3-30].

#### Receita 6, texto: dez passos de pipeline de NLP dentro de um capítulo de ingestão

O passo mais longo do capítulo, e o mais fora de lugar. Lê `Reviews.csv` com `header` e `multiLine`; limpa com `regexp_replace("Text", "[^a-zA-Z ]", "")` seguido de outro `regexp_replace` para colapsar espaços; tokeniza com `split()` ou, alternativamente, com `Tokenizer(inputCol='Text', outputCol='words')` de `pyspark.ml.feature`; remove stop word com `StopWordsRemover(inputCol="words", outputCol="filtered_words")`; conta frequência com `explode()` sobre `filtered_words`, `groupBy("word").count().orderBy("count", ascending=False)` e `show(n=100)`; vetoriza com `CountVectorizer(inputCol='filtered_words', outputCol='features')` em um `fit().transform()`, apresentado como **bag-of-words (BoW)**, sigla expandida e não explicada; e salva com `vectorized_data.repartition(1).write.mode("overwrite").json(...)`.

A promessa da abertura do capítulo era ensinar a "analisar dado de texto com **natural language processing (NLP)**". O que a receita entrega é limpeza por regex, quebra por espaço em branco, lista de stop word e contagem de palavra. Nada de lematização, nada de modelo, nenhuma biblioteca de NLP [R3-12]. E MLlib entra em cena, com três transformadores, em um capítulo cujo título é ingestão e extração.

O *There's more* traz `regexp_extract()` (padrão `\bq\w*` para palavras que começam com q) e `rlike()` via `expr("text rlike 'quick'")`. Este segundo é o parágrafo mais confuso do capítulo: o padrão testa `quick`, a coluna nova se chama `contains_qood` e a prosa diz que o booleano indica se o texto contém a palavra `good`. Três palavras diferentes para uma operação [R3-27]. Fecha com *Customizing stop words*: instanciar `StopWordsRemover` passando `stopWords=custom_stopwords` (a lista impressa é `["/><br", "-", "/>I","/>The"]`, resíduo de HTML do dataset), ou usar `setStopWords()` depois de instanciar. O passo 2 dessa customização escreve `stopwords_remover.transform(data)`, onde `data` é lista Python, não DataFrame [R3-26].

#### Receita 7, escrita: modos, compressão e contagem de arquivo

Seis passos. Lê o CSV da Netflix (com `header`, `nullValue` e `dateFormat`, e **sem** `.schema(schema)`, o que deixa `dateFormat` sem efeito porque nenhuma coluna é `DateType` [R3-28]); escreve CSV com `write.format("csv").option("header", "true").mode("overwrite").option("delimiter", ",").save(...)`; escreve JSON e Parquet com o mesmo molde. Registra que `write` devolve um objeto **`DataFrameWriter`**.

A NOTE dos quatro modos é a tese de persistência mais bem servida do capítulo, e vem com um defeito de vocabulário:

| Modo | Definição do texto |
|---|---|
| `overwrite` | substitui o dado velho pelo novo, "mas derruba índices e constraints" |
| `append` | acrescenta linhas novas ao dado velho, sem alterar nem apagar |
| `ignore` | pula a escrita se o dado ou a tabela existe, "evitando duplicatas" |
| `error` ou `errorifexists` | falha a escrita se o dado ou a tabela existe |

Índice e constraint não existem em escrita de arquivo Parquet, JSON ou CSV: é vocabulário de banco relacional colado em um writer de arquivo [R3-10]. E o texto nunca diz qual dos quatro é o default, que é `errorifexists`.

O *There's more* tem quatro blocos de otimização de escrita:

- **Compressão.** Diz que "é preciso especificar o codec de compressão no método `save`" e o código põe em `option("codec", "org.apache.hadoop.io.compress.GzipCodec")`, que não é o `save`. Cita `BZip2Codec` como o equivalente para BZIP2. A opção documentada do Spark é `compression`, com valores curtos como `gzip` e `bzip2`; `codec` funciona como apelido. E o destino se chama `netflix_csv_data.gz`, que é um **diretório**, não um arquivo gzip [R3-22].
- **Número de partições.** `df.repartition(4)` antes do `write` para gerar quatro arquivos. O parágrafo de definição junge três coisas em uma: particionar é dividir dado em partes processáveis em paralelo, importa para performance porque afeta "a quantidade de shuffle, o balanceamento de carga entre nós e o nível de tolerância a falhas". Shuffle e tolerância a falhas entram como jargão, sem definição, e a ligação entre contagem de arquivo de saída e paralelismo de cluster fica implícita.
- **`coalesce()`** para reduzir partição antes de escrever, com `df.coalesce(1)`, sob o argumento de que muitas partições "podem deixar a escrita lenta". Nem `repartition` é apresentado como shuffle completo, nem `coalesce(1)` como funil que joga todo o dado em uma task só [R3-23].
- **`partitionBy()`**, chamado três vezes de "propriedade" da classe `DataFrameWriter`, quando é método [R3-21]. O exemplo particiona o CSV por `release_year`, coluna que, na leitura sem schema desta mesma receita, é string.

A abertura do capítulo prometia ensinar a otimizar escrita com "buffering, compressão e particionamento". Compressão e particionamento estão aqui. **A palavra buffering não reaparece em nenhuma das 35 páginas** [R3-11].

#### O que ficou marcado em Chadha 1

Dúvidas [R3-1] a [R3-30]. É a leitura com mais defeito verificável das cinco, e o padrão de erro tem uma causa visível: **copiar e colar entre receitas**. A receita de XML fala de "dado JSON" quatro vezes, no parágrafo de abertura (duas), no passo 5 e no passo 6; a receita de Parquet diz que `printSchema()` mostra "o schema do dado JSON"; o achatamento de `laureates` e a declaração de `ArrayType` aparecem duas vezes idênticos, em JSON e em XML [R3-25]. Vários dos erros de opção e de prosa saem disso.

Vocabulário aberto: buffering, higher-order function, bag-of-words, NLP, codec, evolução de schema, shuffle, tolerância a falhas, expressão de caminho JSON, padrão de data e hora (`LLLL d, y`), `spark.executor.memory`, `spark.jars.packages`, `spark.sql.parquet.mergeSchema`, `setLogLevel`, descoberta de partição.

## Onde eu não acreditei

Lista das afirmações que soaram propaganda, exageradas, contraditórias ou datadas. A voz aqui é minha, não a dos livros: onde a crítica se apoia em informação de fora das cinco leituras, ela está dita em linha. Cada item é matéria-prima de pergunta para a aula. Agrupada e prefixada por leitura, porque o padrão de erro de cada livro é diferente, e são 110 itens.

### Dúvidas: Luu, capítulo 3, seção 1

| # | Afirmação | Por que ficou marcada |
|---|---|---|
| R1-1 | "To truly understand how Spark works, you must understand the essence of RDD" abre uma seção de pouco mais de uma página, sem uma linha de código. | Promessa feita e não cumprida dentro da própria seção. Se entender a essência do RDD é condição para entender o Spark, uma página de cinco marcadores não entrega a condição. O capítulo nunca volta ao assunto: RDD reaparece apenas como origem de conversão (`toDF`, `createDataFrame`) e em `movies.rdd.getNumPartitions`. |
| R1-2 | A abertura do capítulo chama as Structured APIs de "the new and preferred way", e a seção seguinte diz que o RDD "provides a solid foundation". | Contradição de enquadramento não reconciliada. O livro afirma ao mesmo tempo que o RDD é obrigatório para entender e que não é o caminho preferido para trabalhar, e não separa os dois planos (o que estudar contra o que escrever). É exatamente o fio de disputa que a aula 01 deixou aberto entre Luu e Damji, e este capítulo repete a ambiguidade em vez de resolvê-la. |
| R1-3 | "Therefore, Spark can't perform any optimizations" sobre a API de RDD. | Absoluto sem condição declarada. O Spark encadeia operações estreitas na mesma task, escolhe número de partições e agenda stages mesmo sobre RDD; o que ele não faz é otimização **relacional** baseada em intenção. O "any" transforma um argumento correto (sem schema não há álgebra relacional para reescrever) numa afirmação forte demais, e o livro não oferece contraexemplo nem ressalva. |
| R1-4 | "In Spark version 1.6, a new programming abstraction called Structured APIs was introduced." | Data que achata a história. O `DataFrame` chegou no Spark 1.3 e o `Dataset` no 1.6; unificar tudo em 1.6 apaga três releases. Crítica apoiada em informação de fora das leituras, e vale conferir na doc oficial antes de virar pergunta. |
| R1-5 | O RDD é chamado de abstração inicial "when Spark was introduced to the world in 2012". | Contradiz o capítulo 1 do mesmo livro, que datou o projeto em 2009 no AMPLab, o open source em 2010 e o top-level Apache em 2013. 2012 é o ano do artigo sobre RDD, não o de apresentação do Spark ao mundo. Contradição interna entre capítulos do mesmo autor. |
| R1-6 | "This information is needed to reproduce the RDD in failure scenarios." | Mecanismo pela metade. O texto fala em reproduzir **o RDD**, nunca em recomputar **a partição perdida**, que é a granularidade real e a razão pela qual o esquema é barato. Sem essa granularidade, o leitor pode concluir que uma falha de nó custa recomputar o dataset inteiro. |
| R1-7 | Peças 4 e 5 da definição vêm marcadas "(optional)" e nada mais. | Afirmação sem consequência declarada. O texto não diz o que muda quando o esquema de particionamento está ausente (nenhuma otimização de co-particionamento) nem o que muda quando a localização está ausente (perda de localidade de dado). Duas das cinco peças ficam como rótulo. |
| R1-8 | "The compute function is sent to each executor in the cluster to execute against each row in each partition." | Duas imprecisões numa frase. A função é serializada e enviada com a **task**, e o executor pode rodar muitas tasks; e "against each row" não vale para operações que recebem um iterador de partição inteira, como `mapPartitions`. Simplificação que atrapalha justamente quem quer entender o mecanismo. |
| R1-9 | `predicate pushdown` aparece na página 3 sem definição. | Termo usado antes de ser definido, e a distância é grande: a definição só chega na página 17, na subseção de JDBC. Na seção onde o termo carrega o argumento, ele é opaco. |
| R1-10 | A seção que existe para justificar o otimizador nunca nomeia o Catalyst. | O nome aparece duas vezes em 41 páginas (na abertura do capítulo e na página 17, apontando para o capítulo 4) e uma vez dentro da Figura 3-1. Zero fases, zero plano, zero exemplo. Somando aos quatro capítulos da aula 01, são cinco capítulos que nomeiam o Catalyst e nenhum que o explica. |

### Dúvidas: Damji, capítulo 3

| # | Afirmação | Por que ficou marcada |
|---|---|---|
| R2-1 | A definição de projeção está errada: o capítulo diz que projeção devolve apenas as **linhas** que casam com uma condição, por meio de filtros | Isso é seleção, ou restrição. Projeção é escolher colunas. Evidência: "A *projection* in relational parlance is a way to return only the rows matching a certain relational condition by using filters". O mapeamento de método que vem depois está certo (`select()` para projetar, `filter()`/`where()` para filtrar), então a definição contradiz o próprio parágrafo |
| R2-2 | A Tabela 3-3 dá `DataTypes.ByteType` como API para instanciar tipos básicos **em Python** | Não é PySpark. Em Python se escreve `ByteType()`, e o próprio capítulo faz isso no código do schema (`StringType()`, `IntegerType()`) e na Tabela 3-5 (`BinaryType()`, `TimestampType()`). A coluna de API da tabela de Scala foi copiada para a tabela de Python, e o capítulo se desmente três páginas adiante |
| R2-3 | "They all are subtypes of the class `DataTypes`, except for `DecimalType`" | `DataTypes` é classe fábrica de métodos estáticos, não superclasse. Os tipos herdam de `DataType`. A frase confunde a fábrica com a hierarquia, e a exceção citada (`DecimalType`) só existe porque a premissa está errada |
| R2-4 | A Tabela 3-4 dá `StructType(ArrayType[fieldTypes])` como forma de instanciar `StructType` em Scala | Não compila e o próprio capítulo escreve o certo em código, `StructType(Array(StructField(...)))`. `ArrayType` é tipo de dado do Spark, não o `Array` de Scala. Na mesma tabela, o terceiro argumento de `ArrayType` em Python é rotulado `nullable` quando é `containsNull`, e a saída de `printSchema` do próprio capítulo imprime `containsNull` |
| R2-5 | "the output from the Scala program is no different than that from the Python program", sobre os dois exemplos que criam o mesmo DataFrame de seis autores | As saídas mostradas **diferem**: em Python todos os campos saem `nullable = false` e o array `containsNull = false`; em Scala, com `false` passado em cada `StructField`, todos saem `true`. A causa (ler JSON força nulabilidade) não é dita, e é justamente o que interessa a quem declara schema para "detectar erro cedo" |
| R2-6 | A Tabela 3-1 rotula a coluna `Published` como `(Date)` e o schema do exemplo a declara `STRING` | Contradição entre a tabela que descreve o dado e o código que o cria, no mesmo par de páginas. A Tabela 3-1 também usa `List[Strings]` como nome de tipo, que não é nome de tipo do Spark |
| R2-7 | O texto põe o otimizador baseado em custo e a escolha entre vários planos na **fase 2**, otimização lógica; a Figura 3-4 põe a pilha de planos e o `Cost Model` **depois** do plano lógico otimizado, dentro da fase 3 | As duas não podem estar certas, e é a fase que a aula pediu. Evidência: "the Catalyst optimizer will first construct a set of multiple plans and then, using its cost-based optimizer (CBO), assign costs to each plan", contra a cascata da figura, que vai `Optimized Logical Plan` para `Physical Plans` para `Cost Model` para `Selected Physical Plan`. O capítulo também não diz se o CBO precisa de estatística coletada nem se está ligado por padrão |
| R2-8 | A Figura 3-5 rotula como `Physical Plan` a etapa em que o filtro desce, e a terceira caixa como `Physical Plan with Predicate Pushdown and Column Pruning` | O texto lista predicate pushdown e projection pruning como otimização **lógica**, fase 2. Figura e texto discordam sobre em que fase o pushdown acontece, no mesmo assunto e a duas páginas de distância da divergência do CBO |
| R2-9 | "identical bytecode for execution" e, na mesma frase seguinte, "the resulting bytecode is **likely** the same" | Duas forças diferentes para a mesma afirmação, em duas linhas, e nenhuma condição declarada. É a mesma afirmação de paridade que o capítulo 1 fez sem ressalva, agora enfraquecida sem que o enfraquecimento seja explicado |
| R2-10 | "this file contains 28 columns and over 4,380,660 records" | Número preciso sobre um dataset que a nota de rodapé 2 diz ter sido **modificado** pelos autores: colunas removidas (o original tinha mais de 60), registros com valor nulo ou inválido removidos, e uma coluna `Delay` acrescentada. O número não descreve o dado público a que a nota de rodapé 1 aponta, e o capítulo o usa para justificar a decisão de declarar schema |
| R2-11 | Todo caminho de dado do capítulo é `/databricks-datasets/learning-spark-v2/...` | O capítulo não diz como obter os arquivos fora do Databricks. Quem lê para aprender Spark, e não a plataforma, não consegue rodar nada do exemplo grande nem do exemplo de IoT sem sair do livro |
| R2-12 | Espaço em nome de coluna é problema "especially when you want to write or save a DataFrame as a Parquet file (which prohibits this)" | Afirmação sem condição e sem versão. Não diz se a proibição é do formato, da implementação do Spark ou de qual release, e é o tipo de detalhe que muda entre versões |
| R2-13 | O texto nomeia `CallDate`, `WatchDate` e `AlarmDtTm` como as colunas string a converter | `AlarmDtTm` não existe no schema de 28 campos definido duas páginas antes, e o código converte `AvailableDtTm`. A prosa cita uma coluna que o exemplo não tem |
| R2-14 | Dentro do bloco de saída do exemplo em Scala de `distinct().show(10, false)` aparece a linha `Out[20]: 32` | `Out[n]:` é prompt de Jupyter, e 32 é o resultado do `countDistinct` do exemplo **anterior**. Saída de duas consultas diferentes, em duas linguagens diferentes, colada num bloco só. Também no mesmo par de exemplos: `col("CallType").isNotNull` sem parênteses num lugar e `$"CallType".isNotNull()` com parênteses no outro, em Scala |
| R2-15 | Dois trechos da seção de Dataset não rodam como estão | `ds.filter({d => {d.temp > 30 && d.humidity > 70})` tem chave desbalanceada. E `dsTemp2` faz `.select($"temp", $"device_name", $"device_id", $"device_id", $"cca3")`, com `device_id` duas vezes, e depois `.as[DeviceTempByCountry]`, que tem quatro campos. O capítulo apresenta esse segundo bloco como forma equivalente de escrever a consulta anterior |
| R2-16 | O trecho que gera a Figura 3-5 define `usersDF` e `eventsDF` e depois usa `users` e `events` | Os identificadores usados não existem. E `.filter(events("date") > "2015-01-01")` compara coluna com string literal sem cast nem uma palavra sobre isso, num capítulo cuja seção anterior é justamente sobre converter string em timestamp |
| R2-17 | O Catalyst é descrito como jornada estática de quatro fases, decidida antes de a execução começar | Nem **Adaptive Query Execution** nem **Dynamic Partition Pruning** são mencionados, e os dois chegaram no Spark 3.0, a versão que o livro diz cobrir. Em 2026, com o Spark em 4.2.0, o AQE é ligado por padrão e reotimiza o plano depois de cada estágio de shuffle, o que quebra a leitura de "o plano é escolhido uma vez". Esta crítica se apoia em informação de fora das cinco leituras |
| R2-18 | A Figura 3-2 põe `Compile Time` na coluna de DataFrames para erro de sintaxe, sem dizer para que linguagens a linha vale | Duas páginas antes o próprio capítulo afirmou que Python e R não têm segurança de tipo em tempo de compilação. A figura é implicitamente sobre Scala e Java e é apresentada como se fosse geral, na seção que decide entre APIs |
| R2-19 | O plano físico mostrado traz `PushedFilters: []` | O `explain()` que o capítulo escolhe para exemplificar o otimizador tem a lista de filtros empurrados **vazia**, num capítulo que apresenta predicate pushdown como benefício central. Não é erro, é escolha de exemplo que não demonstra o que a seção afirma, e nada no texto comenta |
| R2-20 | O capítulo diz "a resounding no" à depreciação do RDD, e no mesmo capítulo diz "we hardly ever find a need" de voltar a ele e "we can't imagine the opacity and comparative unreadability of the code if we were to try to do the same with RDDs" | Trata o RDD como fundação quando fala de arquitetura e como legado quando fala de prática, sem dizer que está separando as duas perguntas. É a contradição que a aula 01 registrou entre os capítulos 1 e 2 do mesmo livro, agora dentro de um capítulo só |
| R2-21 | O custo do Dataset não aparece | O capítulo credita ao encoder o gerenciamento de serialização, desserialização e memória fora do heap, adia o resto para o capítulo 6, e **nunca** diz que a lambda tipada de `filter()` é opaca ao Catalyst. É exatamente o defeito que ele atribui ao RDD nas primeiras cinco páginas, e ele não faz a ligação. A lista de recomendação insinua a conclusão ("If you want space and speed efficiency, use DataFrames") sem dar o mecanismo |
| R2-22 | "available as APIs in Spark's supported languages (Java, Python, Spark, R, and SQL)" | Lista "Spark" onde deveria estar Scala, na frase que apresenta a DSL. Erro pequeno, mas está na página que sustenta a tese de uniformidade entre linguagens |
| R2-23 | "Spark SQL allows developers to issue ANSI SQL:2003-compatible queries on structured data with a schema" | Mesma afirmação do capítulo 1, repetida sem condição e sem fonte. Não diz que parte do padrão é coberta nem o que fica de fora, e é o único parâmetro de conformidade que o livro oferece. Em 2026 a conta mudou de outro jeito: o modo ANSI virou default no Spark 4.0, o que altera comportamento de cast e overflow, e o livro descreve o mundo anterior. Esta segunda parte se apoia em informação de fora das cinco leituras |

### Dúvidas: Luu, capítulo 3, seção 2

| # | Afirmação | Por que ficou marcada |
|---|---|---|
| R4-1 | "A DataFrame is an immutable, distributed collection of data organized into **rows**." | O capítulo 1 do mesmo livro definiu DataFrame como coleção distribuída organizada em **colunas nomeadas**. Duas definições incompatíveis em ênfase dentro do mesmo livro, e a diferença não é cosmética: é a diferença entre pensar o DataFrame como tabela de linhas e como conjunto de colunas, que é o que explica por que Parquet e cache colunar ajudam. Nenhuma das duas passagens reconhece a outra. |
| R4-2 | Listing 3-2 e 3-3: `Random.nextInt(100)` produz saídas 237, 210, 567, 360, 288, 260 e 280. | A saída contradiz o código na mesma página. `nextInt(100)` devolve de 0 a 99. A Note logo abaixo diz que os números variam por serem aleatórios, o que não cobre valores de três dígitos. Ou o código do livro não é o que gerou a saída, ou a saída foi editada à mão. Em qualquer dos casos, o primeiro exemplo do capítulo não reproduz. |
| R4-3 | `val df1 = spark.range(5).toDF("num").show` | Erro de código. `show` é ação e devolve `Unit`, então `df1` não é DataFrame, apesar do nome e do contexto ("examples of using this function to create a DataFrame"). Quem copiar a linha vai descobrir isso na linha seguinte. |
| R4-4 | O padrão canônico de leitura, Listing 3-9: `spark.read.format(...).option("key", value").schema(...).load()` | Aspa desbalanceada no listing que o capítulo usa como forma canônica de tudo o que vem depois. Não compila como está. |
| R4-5 | Tabela 3-2 declara `format` com "Optional: **No**". | Contradição interna, e o contraexemplo está a seis páginas de distância: `spark.read.load("<path>/movies.parquet")` funciona sem `format` porque Parquet é o default, e o próprio Listing 3-10 mostra atalhos (`spark.read.json`, `spark.read.csv`) que dispensam `format`. A tabela deveria dizer que `format` tem default, não que é obrigatório. |
| R4-6 | Tabela 3-2, linha de `schema`: "Some data sources have the schema embedded in the data files, especially Parquet and ORC. In those cases, the schema is automatically **inferred**." | Confusão de dois mecanismos com a mesma palavra. Ler um schema gravado no rodapé de um arquivo Parquet custa uma leitura de metadado; **inferir** um schema de CSV ou JSON custa varrer dado. Chamar as duas coisas de inferência apaga exatamente a distinção de custo que a divergência da aula 01 quer investigar. |
| R4-7 | Tabela 3-3 classifica **JDBC** na coluna "Data Format" como **Binary**. | Categoria errada. JDBC é protocolo de acesso a banco, não formato de arquivo, e não tem formato de dado próprio. A tabela força as seis fontes numa dicotomia texto/binário que não cabe na sexta. |
| R4-8 | A lista de fontes embutidas tem exatamente seis itens: text, CSV, JSON, Parquet, ORC, JDBC. | Três omissões, e duas são internas ao próprio livro. **Avro** é citado na página 2 como formato binário comum que o Spark lê e escrever "out of the box", e não está na lista. **Hive** é citado na definição de DataFrame como origem possível, e não está na lista. **Delta Lake** foi apresentado no capítulo 1 como a resposta do ecossistema para consistência transacional, e não aparece uma vez em todo o capítulo 3. |
| R4-9 | Tabela 3-1: `DecimalType` mapeado para `java.math.BigDecial`, e a tabela toda é só de Scala. | Erro de grafia num nome de classe (`BigDecimal`), numa tabela cujo único propósito é ser consultada. E o mapeamento monolíngue contradiz a afirmação, feita duas páginas antes, de que a API existe em Scala, Java, Python e R: não há mapa de tipos para as outras três. |
| R4-10 | Para a lista completa de opções de CSV, o livro aponta a classe `CSVOptions` em `https://github.com/apache/spark`. | Referência inútil: é a raiz de um repositório com centenas de milhares de arquivos, sem caminho, sem branch, sem tag de versão. O mesmo padrão se repete no link da doc da classe `Column`, que aponta para `docs/latest`, ou seja, para uma versão diferente da que o livro cobre. |
| R4-11 | Listing 3-13 cria `val movies4 = ...` e na linha seguinte imprime `movies.printSchema`. | Imprime o schema do DataFrame errado, e o schema exibido (`actor_name`, `movie_title`, `produced_year: long`) não é o que `movies` tinha naquele ponto do capítulo, que era `actor`, `title`, `year: string`. O leitor não tem como saber se o exemplo de TSV funcionou. |
| R4-12 | Os caminhos de dado do capítulo 3 apontam para `data/chapter4` e `<path>/book/chapter4/data/movies/`, e os exercícios no fim do mesmo capítulo apontam para `chapter3/data/movies`. | Contradição interna de estrutura de pastas dentro de um capítulo só. Quem clonar o repositório do livro não sabe qual das duas é a certa, e nenhuma das duas vem com URL do repositório. |
| R4-13 | Tabela 3-5: `samplingRatio` tem `0.3` na coluna "Value(s)" e `1.0` na coluna "Default". | Inconsistência de tabela. As outras duas linhas trazem o **domínio** de valores (`true, false`); esta traz um exemplo arbitrário. Lida ao pé da letra, a tabela diz que `samplingRatio` só aceita 0.3, com default 1.0. |
| R4-14 | "Therefore, it is quite expensive to load a very large JSON file. In this case, you can lower the `samplingRatio` value to speed the data loading process." | Custo afirmado sem mecanismo, e conselho dado sem o risco. O texto não diz **o que** custa (uma varredura extra sobre o dado antes de qualquer processamento) nem que isso é um job separado, e não avisa que baixar `samplingRatio` compra velocidade **com risco de schema errado**: uma coluna que só tem inteiro nos primeiros 30% e string depois vai ser tipada errado e produzir nulo. É a passagem mais próxima que o Luu chega da divergência sobre custo de inferência, e ele para na metade. |
| R4-15 | Listing 3-14: `spark.read.option("inferSchema","true").schema(movieSchema2).json(path)`. | Combinação contraditória e não comentada: passar `schema` desliga a inferência, então `inferSchema` não faz nada. Pior, `inferSchema` não está entre as opções de JSON da própria Tabela 3-5 do livro (é opção de CSV). O exemplo ensina um hábito errado sem que o texto perceba. |
| R4-16 | "By default, when Spark encounters a corrupted record or runs into a parsing error, it set the value for all the columns in that row to be null." | Comportamento certo, descrição incompleta. O modo nunca é nomeado (`PERMISSIVE`), os outros modos nunca são listados (`DROPMALFORMED`), e `columnNameOfCorruptRecord`, que é como se guarda o registro ruim em vez de perdê-lo, não aparece. Só o oposto extremo (`failFast`) é oferecido, o que deixa o leitor entre nulificar tudo em silêncio e derrubar o job. E o comportamento descrito é o de antes do modo ANSI virar default no Spark 4.0, o que torna o trecho enganoso hoje (crítica apoiada em informação de fora das leituras). |
| R4-17 | "Parquet stores each column's data in a separate file." | Erro factual. Parquet guarda colunas como *column chunks* dentro de *row groups*, no mesmo arquivo; o rodapé com o schema e as estatísticas também está no mesmo arquivo, e é isso que o torna autodescritivo, o que a frase anterior do próprio livro afirma. O benefício descrito existe, o mecanismo é inventado, e é o tipo de erro que estraga o raciocínio sobre layout, tamanho de arquivo e leitura seletiva. |
| R4-18 | "its size is about one-sixth of the size of movies.csv." | Número sem condição declarada. Não diz o tamanho absoluto de nenhum dos dois, não diz o codec de compressão usado, não diz nada sobre o dataset (o `movies.csv` tem três colunas e muita repetição de nome de ator, caso quase ideal para dicionário colunar). A razão pode variar de menos de 2x a mais de 20x conforme o dado, e o livro apresenta 6x como se fosse propriedade do formato. |
| R4-19 | "It was created by **Cloudera** as a part of the initiative to massively speed up Hive." | Erro factual, e inverte a história dos dois formatos apresentados lado a lado. O ORC saiu da **Hortonworks**, junto com a iniciativa Stinger para o Hive; o formato colunar impulsionado pela Cloudera foi o Parquet, que a página anterior credita ao Twitter. Crítica apoiada em informação de fora das leituras. |
| R4-20 | Listing 3-18 e 3-20: jar `mysql-connector-java-5.1.45`, classe `com.mysql.jdbc.Driver`, jar passado como argumento posicional do `spark-shell`. | Instrução operacional envelhecida em três pontos. O Connector/J 5.1 é da linha do MySQL 5.x; a classe correta desde o Connector/J 8 é `com.mysql.cj.jdbc.Driver`; e a forma corrente de fornecer o driver é `--jars` ou `--packages`, que este livro não mostra em nenhuma página, repetindo a lacuna do capítulo 2, que prometeu e nunca entregou `spark-submit`. |
| R4-21 | A subseção de JDBC não menciona `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions` nem `fetchsize`. | Omissão de consequência prática direta. Como está, o exemplo lê a tabela por **uma conexão só, numa partição só**, e o próprio capítulo confirma isso depois, com `movies.rdd.getNumPartitions` devolvendo `Int = 1`. Num livro que ensina a ler de RDBMS de produção, é a diferença entre um job que roda e um job que serializa tudo num executor e trava o banco. Também não há uma palavra sobre a carga que a leitura impõe ao banco de origem. |
| R4-22 | Listing 3-19: `connection close()`. | Falta o ponto do acesso a membro. Erro de digitação num listing de cinco linhas cujo propósito é ser copiado e colado para testar conexão. |
| R4-23 | "The Spark community writes hundreds of custom data sources, and it is not too difficult to implement them." | Número sem fonte e dificuldade afirmada sem evidência. "Centenas" não vem de nenhum lugar, e "não é difícil demais" não é sustentado por nada no capítulo: a única aparição de fonte customizada é a string `org.example.mysource` na Tabela 3-2 e no Listing 3-10. Nenhuma interface, nenhum método, nenhuma menção a v1 contra v2 da API de fonte de dados. |
| R4-24 | Tabela 3-2: "More on these three pieces of information is discussed in later in the chapter." | Promessa não cumprida, e com erro de redação no original. `format`, `option` e `schema` não voltam a ser tratados de forma sistemática: o que vem depois são exemplos por formato. Não há discussão de precedência (o que ganha quando `schema` e `inferSchema` aparecem juntos), nem de como descobrir as opções de um formato, nem do que `load()` faz sem argumento. |

### Dúvidas: Damji, capítulo 4

| # | Afirmação | Por que ficou marcada |
|---|---|---|
| R5-1 | "Both of these statements will create the managed table `us_delay_flights_tbl` in the `learn_spark_db` database" | Os dois comandos logo acima criam `managed_us_delay_flights_tbl`, não `us_delay_flights_tbl`. E `us_delay_flights_tbl` é o nome que a página seguinte usa para a tabela **não gerenciada**, e é também o nome da view temporária do começo do capítulo. O mesmo identificador nomeia três coisas diferentes em cinco páginas, na seção cuja única função é separar gerenciada de não gerenciada |
| R5-2 | Tabela não gerenciada definida como aquela cujo dado você gerencia "in an external data source such as Cassandra" | A definição aponta para sistema externo e todos os exemplos seguintes são **arquivo** em file store: CSV, Parquet, JSON. O capítulo nunca mostra Cassandra, nunca mostra conector algum, e a ligação entre "fonte externa" e "arquivo com `PATH`" fica para o leitor fechar. Some-se que o capítulo nunca escreve "external table", que é o termo do dialeto SQL do Spark e o que vai aparecer em qualquer outro material |
| R5-3 | View global "visible across all `SparkSession`s on a given cluster" contra "a global temporary view is visible across multiple `SparkSession`s **within a Spark application**" | Contradição interna a quatro páginas de distância, e ela muda a conclusão: cluster e aplicação são escopos muito diferentes. A segunda é a correta (`global_temp` vive no `SparkContext` da aplicação), e a primeira está na frase que **abre** a seção, ou seja, é a que fica na cabeça de quem lê na ordem. A subseção que existe para desfazer confusão é a que cria a maior |
| R5-4 | `DROP VIEW IF EXISTS us_origin_airport_SFO_global_tmp_view` | Duas páginas antes o capítulo insistiu que acessar view global temporária **exige** o prefixo `global_temp.<view_name>`. O `DROP` impresso não tem o prefixo. Como está escrito com `IF EXISTS`, ele não levanta erro: falha em silêncio e a view global continua lá. É o pior tipo de exemplo errado, o que parece funcionar |
| R5-5 | "tables persist after your Spark application terminates, but views disappear" | Dito sem condição, e vale só para view **temporária**, que é a única que o capítulo cobre. `CREATE VIEW` sem `TEMP` grava a definição no metastore e sobrevive à aplicação. A frase generaliza de duas variantes temporárias para toda view, e o capítulo nunca mostra a variante persistente existir |
| R5-6 | Criar múltiplas `SparkSession` numa aplicação é útil para combinar dado de duas sessões "that don't share the same Hive metastore configurations" | Duas objeções. A primeira é interna ao livro: o capítulo 2 do mesmo Damji afirma que só pode existir **uma `SparkSession` por JVM**, sem ressalva. A segunda é de mecanismo: se o interesse é combinar dado de duas sessões com metastore diferente, isso não é o que uma view global temporária resolve, e o capítulo apresenta o caso de uso como se fosse a justificativa do escopo global. Nenhum exemplo é dado |
| R5-7 | Tabela 4-1: "The `inferSchema` and `mode` options are specific to the JSON and CSV file formats" | A própria Tabela 4-3, de JSON, **não lista `inferSchema`**; a 4-4, de CSV, lista. As duas tabelas do mesmo capítulo desmentem a frase. E a restrição é falsa na outra direção também: `mode` com registro malformado não é privilégio de JSON e CSV. Como a Tabela 4-1 é o lugar onde `mode` aparece pela primeira e quase única vez, a afirmação errada é a que o leitor leva |
| R5-8 | `"txt"` na lista de valores de `format()`, nas Tabelas 4-1 e 4-2 | Não existe fonte `txt` no Spark: o nome é `text` (ou `textFile` no atalho). Aparece duas vezes, nas duas tabelas, entre nomes corretos, o que faz o erro passar. E é a única fonte da lista das tabelas que nunca ganha seção, junto de `jdbc` |
| R5-9 | Os modos de leitura e a explicação deles | `PERMISSIVE`, `FAILFAST` e `DROPMALFORMED` aparecem numa célula de tabela, com o default declarado (`PERMISSIVE`), e a explicação do que cada um faz é remetida para fora do livro: "The Spark documentation shows some examples and explains the different modes and their actions". Nas 39 páginas não há `_corrupt_record`, não há `columnNameOfCorruptRecord`, não há `badRecordsPath`, não há uma linha sobre o que acontece com a linha ruim em `PERMISSIVE`. O comentário `// Exit if any errors` ao lado de `FAILFAST` é tudo o que se aprende. No capítulo que ensina a ler CSV e JSON, isto é a maior lacuna, porque é o que quebra primeiro em qualquer ingestão real |
| R5-10 | `.option("nullValue", "")   // Replace any null data with quotes` | O comentário está errado e aparece nas duas versões, Scala e Python (na segunda, "Replace any null data field with quotes"). `nullValue` declara **qual string do arquivo representa nulo** na leitura; não substitui nada por aspas, e "quotes" não tem nada a ver com o efeito. Comentário errado em código publicado ensina errado com mais eficiência que texto errado |
| R5-11 | As afirmações sobre compressão nas saídas | Três problemas na mesma família. Um: a escrita de JSON usa `.option("compression","snappy")` e o listing impresso traz `part-00000-<...>-c000.json`, sem sufixo de codec, enquanto a Tabela 4-3 avisa na linha de `compression` que a leitura **só detecta o codec pela extensão**. Pelo próprio livro, o arquivo que o exemplo produz não seria lido de volta comprimido. Dois: o texto chama a saída de CSV de "compressed and compact files" sem que codec nenhum tenha sido setado, e o default de `compression` em CSV é nenhum. Três: o mesmo adjetivo é aplicado a Avro e ORC, onde ora há codec setado, ora não. "Comprimido" virou palavra de enfeite |
| R5-12 | Parquet é default e preferido "because it's efficient, uses columnar storage, and employs a fast compression algorithm" | Dos três motivos, um é circular (eficiente), um é fato (colunar) e um é falso como propriedade do formato: Parquet não emprega um algoritmo de compressão, ele **aceita** vários, e qual você usa é escolha sua (o exemplo do próprio capítulo passa `snappy` na mão). Nenhum número, nenhuma comparação com ORC, que a seção seguinte também chama de colunar e otimizado sem nunca dizer quando um ganha do outro. É a única tese do capítulo, e vem sem condição e sem contra-indicação: nada sobre arquivo pequeno, nada sobre append frequente, nada sobre busca por chave |
| R5-13 | "You will see additional benefits later (such as columnar pushdown), when we cover the Catalyst optimizer in greater depth" | Duas coisas. A remissão aponta para frente e o destino está para trás: o Catalyst é o capítulo **3**, que o leitor acabou de ler, e o capítulo 4 remete ao 3 explicitamente numa outra página. Ou a promessa é para um capítulo posterior que o índice não sustenta, ou é bloco herdado. E `columnar pushdown` não é termo do Spark: o que existe é *column pruning* mais *predicate pushdown*, duas coisas distintas fundidas num nome inventado, jogado entre parênteses e nunca retomado |
| R5-14 | Avro "introduced in Spark 2.4 as a built-in data source" | Chamar de embutido sem ressalva é o que faz o exemplo falhar na primeira tentativa: o `spark-avro` é módulo externo, não vem na distribuição default e precisa ser adicionado (`--packages org.apache.spark:spark-avro_2.12:<versão>`). O capítulo dá quatro exemplos de Avro, uma tabela de opções e zero linhas sobre dependência. Esta é a única das sete fontes do capítulo que exige passo extra, e é a única sobre a qual o capítulo silencia justamente isso |
| R5-15 | As duas configurações do ORC apresentadas como pré-requisito | "When `spark.sql.orc.impl` is set to `native` and `spark.sql.orc.enableVectorizedReader` is set to `true`, Spark uses the vectorized ORC reader" está escrito como condição a satisfazer, e o capítulo nunca diz quais são os **defaults**. Desde o 2.4 os defaults já são `native` e `true`, isto é, o leitor vetorizado já está ligado e não há nada a fazer. O leitor sai da seção acreditando que precisa configurar duas variáveis para ter a otimização que já tem. O mesmo vale para `spark.sql.hive.convertMetastoreOrc` |
| R5-16 | Os dois "recommended usage patterns" do `DataFrameWriter` | Nenhum dos dois roda como impresso, e isso é conferível em um minuto. `bucketBy(...).save(path)` não existe: bucketing só funciona com `saveAsTable()`, e `save()` recusa com erro de análise. E `sortBy(args).saveAsTable(table)` sem `bucketBy` também recusa, porque `sortBy` só vale acompanhado de `bucketBy`. Ou seja, o capítulo imprime como padrão recomendado exatamente a combinação que o Spark rejeita, e ainda deixa `partitionBy` e `sortBy` fora da Tabela 4-2, sem definição e sem um único exemplo. É o achado mais forte da leitura, porque é a assinatura que se copia |
| R5-17 | Todos os caminhos de dado começam em `/databricks-datasets/` | Sem exceção, das 39 páginas: `/databricks-datasets/learning-spark-v2/...` na primeira leitura e em todas as seguintes. Esses caminhos existem dentro do Databricks e em nenhum outro lugar. O capítulo nunca avisa, nunca oferece caminho local alternativo, e a nota que aparece duas vezes manda importar o notebook do repositório do livro. Quem monta Spark local seguindo o capítulo não roda o primeiro exemplo. É a mesma confusão entre Spark e plataforma que a aula 02 pediu para vigiar no Chadha, e ela está aqui, no livro que não é de Databricks |
| R5-18 | "Whether you're using the DataFrame API or SQL, the queries produce identical outcomes" | A afirmação é sobre API contra SQL, e é plausível. Mas as saídas impressas do capítulo não sustentam a leitura fácil que ela induz: sobre o mesmo dataset `summary-data`, Parquet, CSV, Avro e ORC imprimem `United States / Romania / 1` e `Ireland / 264`, e o bloco de JSON imprime `Romania / 15`, `Croatia / 1`, `Ireland / 344`, `Costa Rica / 588`, com uma linha (Moldova) que não aparece em nenhum outro. Os arquivos JSON são outro recorte, e o capítulo não diz uma palavra sobre isso, apesar de apresentar as seis seções como o mesmo exercício repetido em formatos diferentes |
| R5-19 | `spark.sql("SELECT * FROM us_delay_flights_tbl").show()` seguido de "only showing top 10 rows" | `show()` sem argumento mostra 20 linhas. A saída impressa tem dez linhas e o rodapé de dez, tanto no bloco de Parquet quanto no de JSON e no de ORC. Ou o código impresso não é o que gerou a saída, ou a saída foi editada. Mesmo defeito das saídas do capítulo 2 deste livro, e o efeito é o mesmo: não dá para usar o impresso como gabarito de uma execução própria |
| R5-20 | "Supports ANSI SQL:2003-compliant commands" na abertura, e depois "offers an ANSI:2003 compliant SQL interface" (com o `SQL` a menos no nome do padrão e um traço longo no lugar do hífen) | Afirmado duas vezes, com grafias diferentes, sem qualificação, sem suíte de conformidade citada e sem uma palavra sobre o que fica de fora. Conformidade a um padrão SQL é afirmação forte e verificável, e nenhum dos dois usos é acompanhado de qualquer condição. Registro também o que o capítulo **não** diz: nenhuma menção a `spark.sql.ansi.enabled`, que é outra coisa e é a que muda comportamento de cast e overflow. Que o modo ANSI virou default no Spark 4.0 é informação de fora da leitura |
| R5-21 | `ELSE 'Early'` na consulta com `CASE` | O rótulo `Early` é justificado pelo texto como atraso negativo, voo adiantado. Mas em SQL toda comparação com `NULL` devolve desconhecido, então qualquer linha com `delay` nulo cai no `ELSE` e sai rotulada como adiantada. O dataset foi lido com `inferSchema`, sem `nullValue` e sem restrição de nulidade, então nulo é possível. Consulta de exemplo que produz rótulo silenciosamente errado, num capítulo que também não trata de registro ruim [R5-9] |
| R5-22 | "As the results show, all of the longest flights were between Honolulu (HNL) and New York (JFK)" | A evidência é um `show(10)` cujas dez linhas são todas `4330`, `HNL`, `JFK`. De um top 10 truncado não sai um "all": sai que os dez primeiros empatam em 4330. Quantas linhas têm 4330 milhas, e qual é a segunda maior distância, o capítulo não mostra e não tinha como concluir |
| R5-23 | A seção de imagens | A fonte `image` é a única das sete que quebra o roteiro do capítulo: não ensina a escrever, não cria tabela SQL, não tem tabela de opções, e deixa duas colunas do schema sem uma palavra. `mode` sai com o valor 16 e não é explicado (é código de tipo do OpenCV, informação de fora da leitura), e o nome colide com as opções `mode` de leitura e de escrita, três significados para a mesma palavra no mesmo capítulo. E `label` aparece no schema como se fosse propriedade da imagem: só na seção seguinte, sobre arquivos binários, o leitor descobre por acidente que ela vem de descoberta de partição, quando o capítulo nota que ela desaparece com `recursiveFileLookup` |

### Dúvidas: Chadha, capítulo 1

| # | Afirmação | Por que ficou marcada |
|---|---|---|
| R3-1 | A fonte de XML é "embutida" no Apache Spark | O parágrafo de abertura diz `using the built-in XML data source` e a NOTE seguinte diz `We also need to install the spark-xml package ... a third-party library for Apache Spark released by Databricks`. Embutida e de terceiros em duas frases seguidas. É o caso mais limpo de tratar recurso de Databricks como recurso de Spark |
| R3-2 | `excludeAttribute`, `inferSchema`, `ignoreSurroundingSpaces` e `mode` são opções do leitor de XML do Spark | O *There's more* abre com `Apache Spark provides various other options ... of the XML reader`. São opções do pacote `spark-xml`, e o *See also* da mesma receita aponta para `github.com/databricks/spark-xml`, ou seja, o autor sabia da origem |
| R3-3 | `option("rootTag", "tagname")` serve para ler XML com vários elementos aninhados | No `spark-xml`, `rootTag` é opção de escrita; na leitura o que define o registro é `rowTag`. Nenhum exemplo do capítulo usa `rootTag` em código |
| R3-4 | `option("escapeQuotes", "true")` resolve delimitador dentro do valor na leitura de CSV | `escapeQuotes` é opção do writer de CSV. Na leitura o que trata isso é `quote`, `escape` e `unescapedQuoteHandling`, e nenhum dos três aparece no capítulo |
| R3-5 | Nome da opção de valor vazio | A prosa escreve `option("emptyValue", "")` e o código imediatamente abaixo escreve `option("emptyValues", "")`. O nome certo é o singular; o plural é ignorado sem aviso |
| R3-6 | A receita entrega uma coluna `corrupt_record` para investigar erro | Passa `.option("columnNameOfCorruptRecord", "corrupt_record")` sobre um `json_schema` declarado que não contém campo `corrupt_record`. Com schema explícito a coluna só existe se estiver no schema, então a promessa não se cumpre |
| R3-7 | `option("mode", "PERMISSIVE")` serve "para ignorar erros de parse e seguir processando o arquivo" | `PERMISSIVE` já é o default, não ignora e sim anula os campos e guarda o registro cru. `DROPMALFORMED` e `FAILFAST` nunca são nomeados, e o default nunca é declarado |
| R3-8 | `maxColumns` e `maxCharsPerColumn` evitam problema de memória com CSV grande | O texto diz que servem "para limitar o número de colunas e caracteres por coluna que o Spark lê de cada vez". São guarda-corpo de parser (limites por registro), não controle de memória, e o Spark nunca carrega o arquivo inteiro em uma máquina |
| R3-9 | `spark.readStream.csv()` é o jeito de ler CSV grande | `allows you to process the data in real time as it is read from disk`. Troca leitura de arquivo estático por streaming, que é outro modelo de execução, outro ciclo de vida e outro requisito de checkpoint. Nada disso é dito |
| R3-10 | `overwrite` "substitui o dado velho pelo novo mas derruba índices e constraints" | Escrita de CSV, JSON ou Parquet em caminho de arquivo não tem índice nem constraint. É definição de banco relacional colada em um writer de arquivo. O capítulo também não diz que o default é `errorifexists` |
| R3-11 | A abertura promete ensinar a otimizar escrita "com buffering, compressão e particionamento" | Compressão e particionamento aparecem no *There's more* da receita 7. A palavra buffering não reaparece em nenhuma das 35 páginas. Promessa não cumprida |
| R3-12 | A abertura promete "analisar dado de texto com natural language processing (NLP)" | O que a receita 6 entrega é `regexp_replace`, `split`, `StopWordsRemover` e `CountVectorizer`. Sem lematização, sem modelo, sem biblioteca de NLP. E MLlib entra em um capítulo cujo título é ingestão |
| R3-13 | `inferSchema` apresentado como conveniência de uma linha | O `There's more` de CSV oferece `option("inferSchema", "true")` sem uma palavra de custo, no mesmo capítulo cuja NOTE afirma que `read` não executou job nenhum. Ligar `inferSchema` falsifica essa NOTE, porque a inferência dispara uma varredura, e o capítulo não faz a conexão |
| R3-14 | O schema explícito do passo 4 | A prosa descreve um CSV de três colunas, `"name"`, `"age"` e `"gender"`, e o código abaixo declara os doze campos do dataset da Netflix. Além disso, o capítulo nunca diz que a leitura do passo 2, só com `header`, devolve as doze colunas como string |
| R3-15 | Quantas linhas `show()` mostra | Passo 3: "as primeiras 20 linhas". Passo 6, mesma receita, mesmo `df.show()`: "as primeiras cinco linhas". Contradição interna em três páginas |
| R3-16 | Por qual coluna o Parquet de exemplo está particionado | A prosa diz `recipes partitioned by recipe category` e o curinga do exemplo seguinte é `DatePublished=2020-01*`. E o primeiro `load` escreve `../data/partioned_recipes`, sem o segundo `t`, contra `partitioned_recipes` na prosa e no snippet de `mergeSchema` |
| R3-17 | O relato de quais colunas o Spark inferiu com e sem `mergeSchema` | Três casos narrados (`ReviewCount` sim e `Images` não, depois o inverso, depois os dois) sem um único `printSchema()` impresso. É a afirmação mais verificável da receita e a única sem evidência |
| R3-18 | As saídas de `collect_list()` e `flatten()` | O `collect_list` sai como `[[[7, 8], [9, 10], [11, 12]], [[1, 2], [3, 4], [5, 6]]]`, com o id 2 antes do 1, e o `flatten` do mesmo objeto sai começando por `[1, 2]`. As duas saídas impressas não podem ser as duas verdadeiras |
| R3-19 | `array_distinct` é higher-order function | `we can also use higher-order functions (HOFs) to manipulate nested columns in place. For example, ... array_distinct`. `array_distinct` não recebe função. HOF de coleção são `transform`, `filter`, `exists`, `aggregate`. O *See also* da própria receita aponta para a âncora de HOF na documentação |
| R3-20 | `getItem` como substituto de notação de ponto em dado profundo | O conselho é `Use the getItem function instead of dot notation`, e o código do próprio conselho é `col("answers").getItem(0).getField("text")`, que indexa array e acessa struct, não substitui ponto. A receita de XML escreve `getItem(0).id`, misturando os dois |
| R3-21 | `partitionBy()` é "propriedade" da classe `DataFrameWriter` | Chamado de `property` três vezes em dois parágrafos. É método, e a diferença importa para quem vai encadear |
| R3-22 | Como se especifica compressão | O texto diz `we need to specify the compression codec in the save method` e o código põe em `option("codec", ...)`. A opção documentada é `compression`; `codec` é apelido. E o destino `netflix_csv_data.gz` é um diretório, não um arquivo gzip, o que ensina nome errado |
| R3-23 | `repartition(4)` e `coalesce(1)` como controle de contagem de arquivo | Nenhuma palavra sobre `repartition` ser shuffle completo, nem sobre `coalesce(1)` funilar todo o dado em uma task. `repartition(1)` também aparece na receita 6 sem ressalva. O capítulo dá conselho de performance sem dizer o preço |
| R3-24 | `option("multiLine", "true")` como default de leitura de JSON | Aparece nas receitas 2 e 5, e até em um `read.format("csv")` na receita 6. `multiLine` torna o arquivo indivisível, o que colapsa o paralelismo para uma task por arquivo. O capítulo nunca diz isso, embora se preocupe com arquivo grande no *There's more* de CSV |
| R3-25 | Cinco vazamentos de "JSON" para dentro de outras receitas | Receita de XML: `common issues faced while working with JSON data` e `common tasks in data engineering with JSON data` na abertura, `If the JSON data has nested structures` no passo 5, `enforce data types on the JSON data` no passo 6. Receita de Parquet, passo 3: `the schema of the JSON data`. É a assinatura do copiar e colar que produziu vários dos outros defeitos |
| R3-26 | Código que não roda como impresso | `df.printSchema(` sem fechar; `df = (spark.read.format("parquet").load("../data/recipes.parquet")` sem fechar o parêntese externo; `.load("..//ta/nobel_prizes.xml")`, caminho corrompido; `stopwords_remover.transform(data)` sobre uma lista Python; e o comando de instalação do `spark-xml`, onde os dois hifens de `--packages` saíram como um sinal tipográfico longo e sobraram quatro asteriscos no fim de `0.16.0****` |
| R3-27 | O exemplo de `rlike()` | O padrão testa `quick`, a coluna de saída se chama `contains_qood` e a prosa diz que o booleano indica se o texto contém `good`. Três palavras para uma operação, e nenhuma saída impressa para desempatar |
| R3-28 | `dateFormat` na receita de escrita | O passo 2 lê com `.option("dateFormat", "LLLL d, y")` e **sem** `.schema(schema)`. Sem schema, nenhuma coluna é `DateType`, então a opção não faz nada. Depois o exemplo de `partitionBy('release_year')` particiona por uma coluna que, nessa leitura, é string |
| R3-29 | O que o capítulo exige de versão | Nenhuma versão de Spark, nenhuma de Python, nenhuma descrição do cluster do `docker-compose`. Os *See also* estão rotulados `Spark 3.4.0` e apontam para `/docs/latest/`, que hoje serve outra série: o próprio livro criou links que se contradizem com o tempo. `spark.executor.memory` em `512m` e `setLogLevel("ERROR")` repetem sete vezes sem uma linha de explicação |
| R3-30 | O snippet de mapa da receita 5 tem três defeitos somados | Importa só `map_keys` e chama `map_values`; cria uma segunda `SparkSession` com `appName("map_keys_example")` via `getOrCreate()`, que devolve a sessão já existente e **descarta o nome em silêncio**, coisa que o capítulo não avisa em nenhuma das sete receitas; e monta o mapa com valores de tipos diferentes. Copiado como está, o trecho quebra na segunda chamada |

## Divergências entre os livros

Só as que mudam a conclusão, não as de redação. Com três livros e cinco leituras, a coluna vazia também é achado: o silêncio de um texto sobre algo que os outros discutem diz onde ele acha que o assunto mora.

| # | Assunto | Luu (cap. 3) | Damji (caps. 3 e 4) | Chadha (cap. 1) | Por que importa |
|---|---|---|---|---|---|
| 1 | **Papel do RDD** | fundação no discurso, legado no espaço: "you must understand the essence of RDD", entregue em pouco mais de uma página sem código, contra quinze só para criar DataFrame. E o mesmo capítulo chama as APIs estruturadas de "the new and preferred way", sem reconciliar | cap. 3 separa as duas perguntas sem dizer que separou: por arquitetura é fundação (é a abstração mais básica, a Figura 3-4 termina em `RDDs`), por prática é legado (`df.rdd` "has a cost and should be avoided", e restam três cenários). O cap. 4 não menciona RDD **uma única vez** em 39 páginas | uma menção em 35 páginas, expandindo a sigla dentro de uma lista | Três posições em três livros, e o Damji agora tem duas dentro do mesmo livro. Mas a evidência mais forte é o silêncio: um capítulo aplicado e recente escreve ingestão, aninhamento e escrita **sem precisar do conceito de RDD em nenhum passo**. Isso não refuta o Luu, mostra que o trabalho do dia a dia não passa por ali |
| 2 | **Quantos atributos tem um RDD** | **cinco**: dependências, partições, função de cálculo, mais esquema de particionamento e localização do dado, os dois últimos marcados como opcionais | **três**: dependências, partições e função de cálculo, com a assinatura `Partition => Iterator[T]` | não trata | Divergência nova, e checável. Os dois estão descrevendo a mesma classe e contando peças diferentes. Boa pergunta de aula, porque a resposta revela se os dois opcionais são parte da definição ou detalhe de implementação |
| 3 | **Catalyst** | nomeado duas vezes em 41 páginas, mais uma caixa em figura, e adiado para o capítulo 4 do próprio livro. `explain`, `Tungsten` e `Arrow`: zero ocorrências | cap. 3 é a **única fonte da bibliografia** que nomeia as quatro fases e desenha a cascata. O cap. 4 cita uma vez e remete ao cap. 3, ou seja, o livro aponta para si mesmo e fecha o ciclo | a palavra não aparece nas 35 páginas | A dívida da aula 01 tem um pagador só entre cinco capítulos. E há um detalhe de estrutura: o Luu adia para frente e o Damji 4 aponta para trás, então quem lê a bibliografia na ordem do professor encontra o Catalyst explicado no meio e referenciado nas duas pontas |
| 4 | **Inferir schema custa uma varredura** | chega perto e para na metade: diz que carregar JSON grande é caro e manda baixar `samplingRatio`, mas nunca diz que a inferência lê o dado, nada sobre CSV, e usa a palavra "inferido" também para Parquet e ORC, onde o schema só é **lido do metadado** | cap. 3 **diz, com mecanismo**: declarar impede um job separado só para determinar o schema, caro em arquivo grande. Cap. 4 recomenda declarar e dá dois motivos sem mecanismo | mostra as duas formas, não recomenda nenhuma, e afirma numa nota que `read` não disparou job, o que é verdade para o trecho impresso e falso duas páginas depois, quando ele mesmo receita `inferSchema` | O fio da aula 01 **fecha**, e pelo lado inesperado: quem responde é o livro teórico de 2020, não o aplicado de 2024. A distinção que o Luu apaga, entre varrer dado e ler metadado, é justamente a que faz a pergunta valer |
| 5 | **Delta Lake e formato transacional** | **zero ocorrências** no capítulo 3, apesar de o capítulo 1 do mesmo livro apresentar o Delta Lake como a resposta do ecossistema para consistência | cap. 3: zero. Cap. 4: uma, entre parênteses, como consumidor de Parquet, fora da lista de fontes e fora da figura | **zero ocorrências**, num livro cujo título tem Databricks | Silêncio de cinco capítulos. Isso derruba a expectativa de que o Chadha trataria Delta como default e deixa o Luu sozinho, e contradito por si mesmo. Quem lê estes cinco capítulos conclui que persistir Parquet num diretório resolve, e sai sem uma palavra sobre atomicidade de `overwrite`, escrita concorrente ou consistência de leitura durante escrita |
| 6 | **Pushdown de predicado: lógico ou físico** | define na página 17, sem dizer em que fase acontece, e sem meio de verificar, já que `explain` não existe no capítulo | **o cap. 3 se contradiz três vezes**: o texto classifica como otimização lógica (fase 2), a Figura 3-5 desenha entre dois planos **físicos**, e a saída de `explain()` escolhida mostra `PushedFilters: []` | não trata | Divergência interna, e é a mais didática de todas: três apresentações do mesmo conceito no mesmo capítulo e nenhuma confirma a outra. Serve de pergunta direta ao professor |
| 7 | **Registro ruim e contrato de leitura** | dois pontos do espectro: o default nulifica **todas as colunas da linha**, e `failFast` é a única alternativa nomeada. Nada de `PERMISSIVE` por nome | cap. 4 dá os três nomes (`PERMISSIVE`, `DROPMALFORMED`, `FAILFAST`) numa célula de tabela, diz qual é o default e terceiriza a explicação à documentação. Sem `_corrupt_record`, sem `badRecordsPath` | nomeia um modo só, descreve a semântica errado, não diz que é o default, e usa `columnNameOfCorruptRecord` numa receita que não funciona como impressa, porque a coluna não está no schema declarado | Três livros, três coberturas parciais, e nenhuma utilizável em produção. A lacuna é **da bibliografia inteira**, e por isso vira pergunta direta: onde termina o Spark e começa o framework de qualidade, que é exatamente a fronteira que a aula 01 já tinha levantado |
| 8 | **Como o Parquet armazena coluna** | afirma que Parquet guarda cada coluna **num arquivo separado**. Verificável e **falso** | descreve como colunar, sem dizer o layout, e acerta o efeito (ler menos coluna, ler menos byte) | descreve como colunar, sem layout | Erro factual num livro de 2021, sobre o formato que os três recomendam. Confrontar é fácil, e o mecanismo certo (grupos de linha e pedaços de coluna dentro de **um** arquivo) é o que explica por que arquivo pequeno em Parquet é ruim, assunto que a aula 01 já tinha aberto |
| 9 | **Quem criou o ORC** | credita à **Cloudera** | não trata a origem | não trata | Erro de atribuição (foi a Hortonworks, com a Facebook). Pequeno, mas é o tipo de coisa que revela quanto do livro é revisado |
| 10 | **Paralelismo na leitura de RDBMS** | ensina JDBC com quatro peças (driver, URL, autenticação, tabela) e **nenhuma palavra** sobre particionar a leitura | `jdbc` aparece nas tabelas do cap. 4 e não ganha seção | não trata | O Luu é o único que ensina JDBC, e ensina leitura serial sem avisar. `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions` e `fetchsize` não aparecem em nenhum dos três livros. Para quem vai extrair de banco relacional em produção, essa é a lacuna mais cara da bibliografia |
| 11 | **Nulabilidade declarada contra efetiva** | `nullable` aparece em dezenas de linhas de saída e nunca é explicado | cap. 3 mostra que o mesmo schema com `nullable=false` sai `false` ao criar de dado em memória e `true` ao ler o mesmo dado de JSON, e **afirma que as saídas não diferem** | não trata | Decide se "declarar schema detecta erro cedo" é verdade para dado que vem de arquivo. O capítulo dá a evidência contra a própria afirmação e não a comenta |
| 12 | **Cronologia da transição** | data o RDD em 2012 e as APIs estruturadas no Spark **1.6** | não data a transição neste par de capítulos | não data | O consenso é DataFrame no 1.3 e Dataset no 1.6. Se o Luu está juntando as duas coisas, a linha do tempo dele apaga três releases. Mesmo padrão da divergência sobre streaming na aula 01 |

## Spark contra Databricks

A quarta anotação desta leitura, criada por causa do livro novo. O resultado é uma inversão limpa, e é o achado que eu não teria previsto.

**O livro de Databricks não usa Databricks.** Das 35 páginas do Chadha, praticamente tudo é Spark aberto e portável para qualquer cluster: `SparkSession.builder`, `spark.read.format(...).option(...).schema(...).load(...)`, as opções de CSV, JSON e Parquet por nome, `StructType` e companhia, as funções de coleção e aninhamento (`explode`, `explode_outer`, `flatten`, `collect_list`, `array_distinct`, `map_keys`, `getItem`), as de JSON em string (`get_json_object`, `json_tuple`), as de texto (`regexp_replace`, `regexp_extract`, `split`), três transformadores de MLlib, e a escrita inteira com `mode`, `partitionBy`, `repartition`, `coalesce` e codecs do Hadoop. Não aparecem: Auto Loader, `cloudFiles`, DBFS, Unity Catalog, `%fs`, `dbutils`, notebook, cluster, job, Delta Lake, `MERGE`, time travel, `OPTIMIZE`. Nenhum deles tem uma única menção.

O que é de plataforma no Chadha é **um item só, e está rotulado errado**: o `spark-xml`. A abertura da receita promete a fonte XML "embutida" e a nota seguinte manda instalar `com.databricks:spark-xml_2.12:0.16.0`, "biblioteca de terceiros lançada pela Databricks". Embutida e de terceiros em duas frases seguidas. E o tempo virou a mesa: no Spark 4.0 o XML virou fonte nativa, com `format("xml")`, e o pacote foi arquivado. A frase ficou certa por acidente e o código ficou obsoleto.

O ambiente do livro também não é Databricks nem Spark genérico: é montagem do autor, com `docker-compose`, JupyterLab em `127.0.0.1:8888` e Spark standalone em `spark://spark-master:7077`, com `spark.executor.memory` em `512m` nas sete receitas. Dois pontos de atrito de idade: o `docker-compose` com hífen, cujo CLI foi descontinuado em 2023 (hoje `docker compose`), e o `spark-xml_2.12`, que não roda em Spark 4.

**Quem depende de plataforma é o Damji.** Todo caminho de dado das 39 páginas do capítulo 4 começa em `/databricks-datasets/`, que só existe dentro do produto, sem uma palavra de aviso e sem instrução de como obter os arquivos fora dele. As notas remetem a notebooks, e a `SparkSession` pronta é justificada por "in a Spark shell (or Databricks notebook)". O capítulo 3 faz o mesmo. É um livro sobre Spark, editado pela O'Reilly, cujos exemplos rodam sem alteração apenas na plataforma da empresa dos autores.

A consequência prática, e ela inverte o que eu escrevi no esqueleto: **o capítulo que dá para rodar hoje, inteiro, sem conta e sem gastar nada, é o do livro de Databricks.** Os dois do Damji exigem a plataforma ou um trabalho de substituição de dataset que o livro não menciona. Ao ler, a vigilância tem de ir para o Damji, não para o Chadha.

## Vocabulário novo

Termos que apareceram sem definição suficiente. A coluna da direita é o que **falta**, não o que o livro disse. A tabela é longa de propósito: 39 termos, e é a lista de compras do aprofundamento.

### O que a aula 01 deixou em aberto, e o que fechou aqui

| Termo | Estado depois desta leitura | O que ainda falta |
|---|---|---|
| **Catalyst** | **fecha em grande parte**, no Damji 3: as quatro fases nomeadas, a cascata da Figura 3-4 de `Unresolved Logical Plan` até `RDDs`, o `Catalog` entrando na análise e o `Cost Model` escolhendo entre planos físicos | em que fase o CBO age, porque texto e figura discordam; se o CBO precisa de estatística e se vem ligado; quais são os operadores físicos; e o AQE do Spark 3.0, que quebra a jornada estática que a figura desenha |
| **Whole-stage codegen** | **fecha a definição**, no Damji 3: colapsa a consulta inteira em uma única função, elimina chamada de função virtual e usa registrador de CPU para dado intermediário | o que marca no plano. O `*(n)` aparece na saída de `explain()` e nunca é ligado ao conceito. E o que **impede** que aconteça |
| **Tungsten** | **meia**, no Damji 3: um dos dois componentes do núcleo do motor, viabiliza o codegen, segunda geração no Spark 2.0, cuida de memória fora do heap com ajuda dos encoders | o que é gerar código em runtime, o formato binário de memória, e por que isso muda a performance. Adiado ao capítulo 6, com nota de que o funcionamento interno está fora do escopo do livro |
| **`Exchange`** | **segue aberto**. Aparece duas vezes na saída do `explain(True)` do Damji 3, como `rangepartitioning(..., 200)` e `hashpartitioning(..., 200)`, e a palavra não ocorre uma vez na prosa | que `Exchange` é o shuffle, que contar `Exchange` é contar shuffles, o que distingue `rangepartitioning` de `hashpartitioning`, e de onde vem o `200` |
| **Linhagem** | **segue aberto, e piorou**. Cinco capítulos, e nenhum diz que a recomputação é por partição perdida | o custo de linhagem longa, onde entra checkpoint (palavra ausente das cinco leituras), e a relação com DAG e stage |

### Otimização e plano

| Termo | Onde apareceu | O que o texto entrega | O que ficou faltando |
|---|---|---|---|
| **Predicate pushdown** | Luu 3.1 (efeito, sem definição), Luu 3.2 pág. 17 (definição), Damji 3 (fase 2 no texto, entre planos físicos na Figura 3-5) | que empurra o filtro até a fonte, filtrando no nível do banco ou do arquivo | como **verificar** que aconteceu; quais operadores são empurráveis; o que acontece com UDF no filtro; e em que fase isso mora, porque os textos discordam |
| **Poda de colunas** (*column pruning*) | Luu 3.1, como otimização que o RDD impede | a ação | o nome da técnica, a condição para acontecer (formato colunar), e a diferença entre podar na leitura e podar no plano |
| **Otimizador baseado em custo (CBO)** | Damji 3, fase 2 | a sigla expandida e a função de atribuir custo a cada plano | que custo é esse, de onde vem a estatística, se roda por padrão, e em que fase age |
| **Árvore sintática abstrata (AST)** | Damji 3, fase 1 e Figura 3-4 | o nome expandido, e que a fase 1 gera uma | o que é a árvore, e como consulta de DataFrame vira árvore sem passar por parser de SQL |
| **Regras de otimização lógica** | Damji 3, fase 2 | quatro nomes numa lista com "etc.": dobra de constante, pushdown, poda de projeção, simplificação booleana | o que cada uma faz. Só o pushdown ganha desenho; os outros três não ganham nada |
| **`Batched` e `PushedFilters`** | Damji 3, só na saída do plano físico | nada | o que significam, e por que `PushedFilters` está vazio justo no exemplo escolhido |
| **Leitor vetorizado** | Damji 4, seção ORC | **definição utilizável**: lê blocos de linhas, tipicamente 1.024 por bloco, em vez de linha por linha, reduzindo CPU em scan, filtro, agregação e join | por que só o ORC ganha essa explicação, se Parquet também tem, e quais os defaults das duas configurações |
| **Lotes de colunas** | Luu 3.2, seção Parquet | que o Spark descomprime e decodifica em lotes, o que "acelera consideravelmente" | é o mesmo leitor vetorizado com outro nome, e o Luu não diz isso. Sem número e sem configuração |
| **`columnar pushdown`** | Damji 4, uma promessa | nada, o termo é usado e nunca explicado | se é o mesmo que predicate pushdown ou coisa distinta |

### Tipos, schema e contrato

| Termo | Onde apareceu | O que o texto entrega | O que ficou faltando |
|---|---|---|---|
| **`Row`** | Luu 3.2 (definição de DataFrame e construtor), Damji 3 | "a generic `Row` object represents each row" | o que é um `Row`, como se lê um campo, e por que é ele que impede segurança de tipo. Termo central entregue como adjetivo |
| **`nullable`** | Luu 3.2 (dezenas de linhas de saída), Damji 3 (a contradição do JSON) | nada em texto corrido, só o valor nas saídas e o terceiro argumento de `StructField` | por que RDD de `Int` dá `false` e `String` dá `true`; o que o Spark faz se você declarar `false` e vier nulo; que `nullable` é promessa do usuário, não validação |
| **Encoder** | Damji 3, duas linhas | que ajudam na memória fora do heap e trazem "serialização eficiente do Tungsten" | o que é um encoder, por que existe só para tipo conhecido, e por que isso não vale para Python |
| **case class** | Damji 3, seção própria | uso, não definição: o jeito mais fácil de especificar schema de Dataset em Scala | o que é uma case class e por que ela serve e uma classe comum não. Definição adiada ao capítulo 6 |
| **schema-on-read** | Damji 3, uma vez | usado como oposto de declarar schema antes | definição, e onde é a escolha certa. O capítulo só cita para recomendar o contrário |
| **`inferSchema`** | Luu 3.2 (CSV e, indevidamente, JSON), Damji 4 (cinco exemplos), Chadha (uma linha) | o efeito, e que sem ela tudo vira string | o custo, que só o Damji 3 explica. E que a opção é de CSV, não de JSON |
| **`samplingRatio`** | Luu 3.2 (default `1.0`), Damji 3 (exemplo em `0.001`) | que amostra em vez de varrer tudo, e que baixar acelera | o risco de amostra não representativa, para que formatos vale, e como escolher o valor |
| **`mode` de parsing** | Luu 3.2 (default e `failFast`), Damji 4 (os três nomes numa célula), Chadha (um nome, semântica errada) | nomes, e no Damji 4 qual é o default | o que cada um faz com uma linha ruim; `_corrupt_record`; `columnNameOfCorruptRecord` e a regra de que a coluna precisa estar no schema; `badRecordsPath`; e o que muda com o modo ANSI |
| **`mergeSchema` e evolução de schema** | Chadha (opção e chave de sessão), Damji 4 | que existe e que o default é `false` | como tratar evolução de schema "manualmente", frase com que o Chadha encerra a seção sem dar caminho; e qual vence entre a opção da leitura e a chave de sessão |
| **Dado Catalyst** | Damji 4, linha de `avroSchema` | um parêntese: "Spark internal data type" | por que o schema Avro do usuário precisa casar com o tipo interno, e em que momento a falha aparece |
| **ANSI SQL:2003** | Damji 3, abertura da seção do motor | só a afirmação de compatibilidade, repetindo o capítulo 1 | o que é compatível e o que não é, e a relação com `spark.sql.ansi.enabled`, que virou default no 4.0 |

### Persistência, partição e metadado

| Termo | Onde apareceu | O que o texto entrega | O que ficou faltando |
|---|---|---|---|
| **`partitionBy`** | Damji 4 (só na assinatura), Chadha (com exemplo), Luu (fora do escopo pedido) | no Chadha, exemplo completo; no Damji 4, nada além do nome | o que é escrita particionada, que layout de diretório produz, e como se relaciona com a descoberta de partição na leitura. É a opção que mais muda custo de leitura depois, e o Damji 4 a cita e abandona |
| **Descoberta de partição** | Damji 4 (nomeada três vezes), Chadha (descrita sem o nome) | que o Spark reconhece o diretório como conjunto particionado | a convenção `chave=valor` no nome do diretório virando coluna, que nenhum dos dois explica. Sem isso, a coluna que aparece do nada é mágica. E o tipo da coluna de partição |
| **Bucketing** e **bucket** | Damji 4, uma linha na Tabela 4-2 | que se passa número de buckets e colunas, e que usa o esquema do Hive | o que é um bucket, para que serve (evitar shuffle em join e agregação), e por que exige `saveAsTable` |
| **`sortBy`** | Damji 4, só na assinatura | nada | idem, e que só existe junto de `bucketBy` |
| **Metastore** | Damji 4 | repositório central do metadado de tabela, o do Hive por default em `/user/hive/warehouse`, mudável por `spark.sql.warehouse.dir` | se é banco embutido ou externo, o que muda entre modo embutido e remoto |
| **`Catalog`** | Damji 4 (três métodos), Damji 3 (peça da fase 1) | que é a abstração de metadado, com `listDatabases`, `listTables`, `listColumns` | a relação com o metastore: são a mesma coisa vista de dois lados, ou camadas distintas? E o que é catálogo externo, adiado para o capítulo 12 |
| **SerDe** | Damji 4, seção ORC | a expansão da sigla | o que é uma tabela Hive ORC SerDe, e por que precisa de configuração própria |
| **Codecs de compressão** | Damji 4 (listas por formato), Chadha (duas classes Hadoop) | as listas: JSON e CSV aceitam `bzip2`, `deflate`, `gzip`, `lz4`, `snappy`; Avro tem default `snappy` | qual escolher e por quê. Nada sobre **divisibilidade**, e gzip não é divisível, o que decide paralelismo de leitura. Nada sobre a troca entre taxa e CPU, nada sobre o default do Parquet |
| **`LAZY`** | Damji 4, cache de tabela | que é novidade do 3.0 e cacheia no primeiro uso | onde o cache vive, quanto ocupa, e quando vale contra o custo de manter |
| **Camada de fontes de dados** | Luu 3.2, Tabela 3-2 | que é extensível, e que fonte customizada exige nome totalmente qualificado | a interface a implementar, a diferença entre API v1 e v2, e a Python Data Source API do Spark 4.0, que nenhum livro tem |
| **Paralelismo de leitura JDBC** | não apareceu em nenhuma das cinco | nada | tudo: `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions`, `fetchsize`. A lacuna mais cara da bibliografia para quem extrai de banco relacional |
| **Padrão glob** | Damji 4, escrito "global pattern" | o exemplo `"*.jpg"` | o nome certo é glob, e o erro de uma letra troca "casamento de nome de arquivo" por "abrangente". Que sintaxe o Spark aceita |

### Distribuição e execução

| Termo | Onde apareceu | O que o texto entrega | O que ficou faltando |
|---|---|---|---|
| **Esquema de particionamento** (do RDD) | Luu 3.1, peça 4, marcada como opcional | que é metadado opcional | o que é; que estratégias existem (hash, faixa); o que se ganha quando existe |
| **Tipo de join** | Luu 3.1, "recommending a more efficient join type" | que existe escolha e que ela afeta velocidade | quais tipos existem. O capítulo adia joins duas vezes e nunca lista um |
| **shuffle** | Chadha, uma frase | que particionar "afeta a quantidade de shuffle" | o que é trocar dado entre executores, o que dispara, e por que `repartition` dispara e `coalesce` não |
| **`repartition` contra `coalesce`** | Chadha, receitados lado a lado | os dois nomes, como ajuste de número de arquivos | que o primeiro é shuffle completo e o segundo é funil, e quando cada um é a resposta |
| **Memória fora do heap** | Damji 3, operações de Dataset | que o motor cuida dela com ajuda dos encoders | por que sair do heap, e o que isso tem a ver com coletor de lixo |
| **`spark.executor.memory`** | Chadha, sempre `512m` | o nome e o valor | o que a chave controla, e por que 512 MB serve para este dado |
| **DSL** | Damji 3, usado desde a terceira página | nada de definição. Chega perto em "domain-specific operators" | o que a sigla significa, e onde termina a DSL e começa a API. Aberto desde a aula 01 |
| **higher-order function** | Chadha, receita 5 | a sigla expandida, com um exemplo **errado** (`array_distinct` não é HOF) | o que faz de uma função ser de ordem superior, e quais são de fato: `transform`, `filter`, `exists`, `aggregate` |
| **buffering** | Chadha, promessa da abertura | nada, a palavra não volta | o que é bufferizar na escrita, e por que estaria ao lado de compressão e particionamento |

## O que fica para o aprofundamento

Perguntas que as cinco leituras abriram e não fecharam, agrupadas pelo tipo de resposta que exigem.

**Otimizador, e é o grupo mais carregado.** Em que fase o custo age de verdade, já que texto e figura do Damji 3 discordam? O CBO precisa de `ANALYZE TABLE` e vem ligado? Quais são os operadores físicos, que nenhum livro nomeia? Como o AQE, ligado por padrão desde o 3.2, quebra a cascata estática que a Figura 3-4 desenha, e o que sobra daquele desenho em 2026? E a pergunta prática: como se lê um `explain()` de verdade, ou seja, o que são `*(n)`, `Exchange`, `PushedFilters`, `Batched` e de onde vem o `200`, já que a bibliografia imprime a saída e não explica nenhum campo dela?

**Schema e contrato.** Qual é a diferença real entre varrer dado para inferir e ler schema do metadado, distinção que o Luu apaga ao usar "inferido" para CSV, JSON, Parquet e ORC? `nullable=false` é validação ou promessa, e por que o mesmo schema muda de nulabilidade quando o dado vem de arquivo? Qual é o espectro completo de tratamento de registro ruim (`PERMISSIVE`, `DROPMALFORMED`, `FAILFAST`, `_corrupt_record`, `columnNameOfCorruptRecord`, `badRecordsPath`), a regra de que a coluna de corrompido precisa estar no schema, e onde termina o Spark e começa o framework de qualidade? O que o modo ANSI, default desde o 4.0, muda em tudo isso? E o que é o tipo VARIANT, que nenhum dos três livros tem?

**Formatos e persistência.** Como o Parquet realmente organiza coluna, já que o Luu erra o layout, e por que isso explica o problema de arquivo pequeno? Qual codec escolher, e qual é divisível? O que é bucketing, para que serve, e por que exige `saveAsTable`? Qual a relação entre catálogo e metastore? E a pergunta que a bibliografia inteira não faz: e se meu formato não estiver na lista das sete fontes embutidas, o que a Python Data Source API do 4.0 resolve?

**O que a bibliografia não cobre e é trabalho de todo dia.** Leitura paralela de RDBMS via JDBC, que só o Luu ensina e ensina serial. UDF, que não aparece em nenhuma das 141 páginas, e o custo da fronteira JVM contra Python, agora com Arrow ligado por padrão no 4.2. Formato de tabela transacional, silêncio de cinco capítulos: o que Delta, Iceberg e Hudi resolvem que Parquet num diretório não resolve, e por que essa é a decisão de arquitetura mais consequente da camada de persistência.

**O que é motor e o que é plataforma.** Como rodar os exemplos do Damji fora da Databricks, já que todo caminho é `/databricks-datasets/`. O que o Chadha chama de fonte XML embutida, quando virou embutida de verdade, e o que fazer com `spark-xml_2.12` hoje. E, de forma geral, quais recursos que a literatura de Databricks apresenta como Spark são de fato de plataforma.

As respostas vão para [02-aprofundamento.md](02-aprofundamento.md); as perguntas que sobreviverem e virarem pergunta de aula vão para [03-aula.md](03-aula.md); o artefato que fecha o ciclo está em [04-pos-aula.md](04-pos-aula.md).
