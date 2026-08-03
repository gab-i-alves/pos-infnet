# Guia de Leitura — *Beginning Apache Spark 3*, Capítulo 2: Working with Apache Spark

**Fonte:** Hien Luu, *Beginning Apache Spark 3* (Apress, 2021), Capítulo 2
**Escopo:** os Níveis 1 a 4 são respondíveis apenas com este capítulo. O Nível 5 não. O Nível 6 exige o capítulo 1 dos dois livros.

> **Nota de formato.** Este é um capítulo procedural — instalar, iniciar, clicar, digitar — e não conceitual. Aplicar o formato padrão sem alteração produziria um Nível 4 inchado de profundidade inventada sobre caminhos de menu. As proporções foram deslocadas de propósito: **o Nível 3 vira laboratório prático** (o propósito real do capítulo), **o Nível 4 é curto e trata sobretudo de onde o capítulo contradiz a si mesmo ou às próprias capturas de tela**, e **o Nível 5 é a maior seção do guia** — conteúdo procedural envelhece mais rápido que conteúdo conceitual, e este capítulo tem cinco anos.

**Sobre as figuras.** O capítulo é procedural e boa parte das respostas depende de ler capturas de tela. Abri as páginas do PDF e li as imagens em vez de trabalhar só com o texto extraído. Onde uma resposta descreve conteúdo de figura, ela veio da imagem.

## Como usar este guia

1. Leia o capítulo uma vez de ponta a ponta, sem parar.
2. Faça o Nível 1 de memória primeiro, depois volte ao texto para preencher as lacunas. Marque toda questão que você não conseguiu responder.
3. Faça o Nível 2 por escrito, em frases completas.
4. **Faça o Nível 3 num terminal, não no papel.** Este capítulo vale pouco como leitura e muito como prática. Tudo dos Níveis 1 e 2 gruda como efeito colateral.
5. O Nível 4 é curto. Ele é sobre ler as capturas de tela como evidência, não como decoração.
6. O Nível 5 vai para o backlog de estudo. Faça-o *antes* do Nível 3 se quiser que o laboratório funcione, porque várias instruções do capítulo provavelmente mudaram de lugar.

---

## Nível 1 — Memorização e definições

Respostas curtas e verificáveis.

**1.** Quais são as três opções comuns de trabalhar com Spark que o capítulo descreve, mais o quarto tema reservado a software engineers? *(Chapter intro)*

R: Usar o Spark shell, submeter uma aplicação Spark pela linha de comando e usar a plataforma cloud hospedada chamada Databricks. O quarto tema é montar o código-fonte do Apache Spark numa máquina local, para estudar como certas features foram implementadas. O capítulo anuncia as três opções e cumpre duas. Não existe nenhuma seção sobre submissão pela linha de comando, nem uma menção a `spark-submit`.

**2.** Em que linguagem o Spark é escrito, em quais famílias de sistema operacional ele é empacotado para rodar, e qual é o único pré-requisito para rodar Spark localmente? *(Downloading and Installation)*

R: O Spark é escrito em Scala. É empacotado para rodar em Windows e em sistemas UNIX-like, com Linux e macOS como exemplos. O único pré-requisito é ter Java instalado no computador.

**3.** Qual versão do Spark era a corrente à época da escrita, e que data de lançamento a Figure 2-1 mostra para ela? *(Downloading Spark; Figure 2-1)*

R: A versão corrente era a 3.1.1, e a Figure 2-1 mostra a data **02 de março de 2021** ao lado dela no seletor de release. A prosa só diz "at the time of writing this book, the latest version is 3.1.1". Também não achei a data na nota de release da 3.1.1 no site do Spark.

**4.** Qual package type o capítulo manda escolher, e por que o binário pré-empacotado é o caminho mais fácil? *(Downloading Spark)*

R: O capítulo manda escolher o package type com a versão mais recente do Hadoop. O binário pré-empacotado é o caminho mais fácil porque já contém os JAR files necessários para rodar o Spark no computador. O download é disparado pelo link do item 3 da página.

**5.** Segundo a nota na Figure 2-1, com qual versão do Scala o Spark 2.x é pré-construído, qual release é a exceção, e qual versão do Scala vale para o Spark 3.0+? *(Figure 2-1)*

R: A nota da Figure 2-1 diz que o Spark 2.x é pré-construído com **Scala 2.11**, com uma exceção, a versão **2.4.2**, que vem com Scala 2.12. E o **Spark 3.0+** é pré-construído com **Scala 2.12**. A nota equivalente na página de download atual está no item 1 do Nível 5, e hoje diz outra coisa: Scala 2.13 no Spark 4, com o 2.12 removido.

**6.** Qual é o formato do arquivo baixado, qual comando o descompacta em Linux ou macOS, e quais duas ferramentas são nomeadas para Windows? *(Installing Spark)*

R: O arquivo é `spark-3.1.1-bin-hadoop2.7.tgz`, um GZIP compressed tar archive. Em Linux ou macOS o comando é `tar xvf spark-3.1.1-bin-hadoop2.7.tgz`. Em Windows o capítulo nomeia WinZip e 7-zip.

**7.** Quantos diretórios existem, aproximadamente, sob o Spark directory, e quais são os cinco que a Table 2-1 descreve? Dê o propósito de cada um. *(Table 2-1)*

R: Cerca de uma dúzia de diretórios. Os cinco descritos são:

| Nome | Propósito |
|---|---|
| `bin` | Executáveis para subir o Spark shell em Scala ou Python, submeter aplicações Spark e rodar os exemplos |
| `data` | Pequenos arquivos de dados de amostra para os vários exemplos do Spark |
| `examples` | Código-fonte e binário de todos os exemplos do Spark |
| `jars` | Os binários necessários para rodar o Spark |
| `sbin` | Executáveis para gerenciar o Spark cluster |

**8.** Qual comando inicia o Scala shell, e qual comando inicia o Python shell? *(Spark Scala Shell; Spark Python Shell)*

R: `./bin/spark-shell` e `./bin/pyspark`, os dois rodados a partir do Spark directory.

**9.** Como se sai do Scala shell? E do Python shell? *(Spark Scala Shell; Spark Python Shell)*

R: Do Scala shell, com `:quit` ou `:q`. Do Python shell, com ctrl-d.

**10.** Qual versão de Java o capítulo prefere para o Scala shell, e qual versão mínima de Python para o Python shell? *(Notes de ambas as seções)*

R: Java 11 ou superior é preferido para o Scala shell. O Python shell exige Python 3.7.x ou superior.

**11.** Quais versões de Scala e de Java aparecem no banner da Figure 2-2? *(Figure 2-2)*

R: O banner da Figure 2-2 mostra **Scala 2.12.10** e **Java 11.0.6**, esta rodando em OpenJDK 64-Bit Server VM. O mesmo banner traz a versão 3.1.1 do Spark, a Web UI em `:4040`, o `sc` com `master = local[*]` e a linha dizendo que a Spark session está disponível como `spark`. Isso casa com a nota logo abaixo da figura, de que Java 11 ou superior é preferido para o shell Scala.

**12.** O que significa REPL, e quais são as quatro coisas que um REPL faz em sequência? *(Spark Python Shell)*

R: REPL é acrônimo de read-eval-print loop. As quatro coisas são ler a entrada do usuário, avaliar essa entrada, imprimir o resultado e repetir o ciclo. O capítulo descreve o comportamento assim: digitada uma linha, o REPL dá feedback imediato sobre erro de sintaxe, avalia a linha se não houver erro e exibe a saída no shell.

**13.** Qual passo do processo normal de desenvolvimento o ambiente interativo permite pular? *(Spark Python Shell)*

R: O passo de compilação do código.

**14.** Qual shell o resto do livro usa? *(Spark Python Shell)*

R: O Spark Scala shell.

**15.** Qual o capítulo diz ser a única dependência externa do Spark shell? *(Having Fun with the Spark Scala Shell)*

R: Que os arquivos de dados processados precisam residir no próprio computador. Com conexão de internet dá para acessar arquivos remotos, mas o capítulo avisa que fica lento.

**16.** Qual comando lista todos os comandos disponíveis do shell? *(Useful Spark Scala Shell Command and Tips)*

R: `:help`.

**17.** Cite os seis comandos da Table 2-2 e o que cada um faz. *(Table 2-2)*

R:

| Comando | O que faz |
|---|---|
| `:history` | Exibe o que foi digitado na sessão anterior do shell e na atual. Serve para copiar |
| `:load` | Carrega e executa o código de um arquivo. Útil quando a lógica de processamento é longa |
| `:reset` | Devolve o shell a um estado limpo, quando o valor das variáveis já se perdeu de vista |
| `:silent` | Desliga a exibição da saída de cada API chamada. Digitar de novo religa |
| `:quit` | Sai do shell |
| `:type` | Exibe o tipo de uma variável, na forma `:type <variable name>` |

**18.** Qual comando as pessoas tentam por engano no lugar de `:quit`? *(Table 2-2)*

R: `:exit`, que não funciona.

**19.** Qual é a sintaxe do comando `:type`, e a quais duas coisas ele pode ser aplicado, segundo a Figure 2-13? *(Table 2-2; Figure 2-13)*

R: A sintaxe é `:type <variable name>`. A prosa que apresenta a Figure 2-13 diz que o comando é testado sobre uma variável Scala e sobre uma função definidas antes, ou seja, as duas coisas são a variável `ages` e a função `isOddAge`. A Table 2-2 descreve só o caso da variável, e a prosa amplia sem corrigir a tabela.

**20.** O que acontece ao digitar `spa` e apertar Tab? E ao digitar `spark` e apertar Tab? *(Figures 2-5, 2-6)*

R: Com `spa` mais Tab, o ambiente completa para `spark` e mostra os matches possíveis. A Figure 2-5 mostra dois: a própria variável `spark` e a função `spark_partition_id`. Com `spark.` mais Tab, ele lista as member variables e as funções do objeto Scala representado por aquela variável. A Figure 2-6 traz cerca de trinta nomes em quatro linhas, entre eles `conf`, `catalog`, `createDataFrame`, `read`, `readStream`, `sql`, `sparkContext`, `sessionState`, `sqlContext`, `stop`, `table`, `udf` e `version`. A Figure 2-20, mais adiante no capítulo, mostra a saída do mesmo comando com resultado diferente: ela traz cerca de quarenta nomes, porque inclui os métodos herdados de `Any` e `AnyRef`, e ao mesmo tempo omite `executeCommand`, `sessionState` e `sharedState`, que estão na 2-6. Duas capturas do mesmo comando que não coincidem. Em ambas aparece `sparkContext`, que é o caminho de acesso ao SparkContext a partir da SparkSession.

**21.** Além de `:history`, qual é o jeito rápido de recuperar uma linha digitada há pouco? *(fim daquela seção)*

R: Apertar a seta para cima até a linha desejada e dar Enter para executá-la.

**22.** Qual array Scala é usado como exemplo corrente, e quais quatro valores ele guarda? *(Basic Interactions with Scala; Figure 2-8)*

R: O array `ages`, definido como `val ages = Array(20, 50, 35, 41)`. Os valores são 20, 50, 35 e 41.

**23.** Escreva a função `isOddAge` como o capítulo a define. O que o Scala não exige que o Java exige, e o que é retornado no lugar? *(Basic Interactions with Scala)*

R:

```scala
def isOddAge(age:Int) : Boolean = {
  (age % 2) == 1
}
```

O Scala não exige a palavra-chave `return`. No lugar dela, a saída da última instrução do corpo da função é retornada a quem chamou, quando a função foi definida para retornar um valor.

**24.** Escreva a expressão de uma linha que filtra as idades ímpares e as imprime, e dê a saída dela. *(Figure 2-12)*

R:

```scala
ages.filter(age => isOddAge(age)).foreach(println)
```

A Figure 2-12 é imagem e não está no texto extraído, mas a saída é determinável pelo array: `35` e `41`, nessa ordem, que são os ímpares na ordem original.

**25.** Qual recurso o capítulo recomenda para aprender o mínimo de Scala, e de quem é? *(fim de Basic Interactions with Scala)*

R: O repositório `https://github.com/deanwampler/JustEnoughScalaForSpark`, de Dean Wampler. O capítulo registra que o material foi apresentado em várias conferências sobre Spark.

**26.** O capítulo diz que chamar o Spark shell de aplicação Scala é apenas parcialmente verdade. Qual é a correção? *(Spark UI and Basic Interactions with Spark)*

R: O Spark shell é uma aplicação Spark escrita em Scala. Ao iniciar, ele inicializa e deixa prontos o Spark UI e algumas variáveis importantes.

**27.** Em qual porta o Spark UI roda, e onde na saída do shell se encontra a URL dele? *(Spark UI)*

R: Na porta 4040. A URL aparece numa linha da saída de startup do shell, tanto na Figure 2-2 quanto na Figure 2-3, no formato `The SparkContext Web UI is available at http://<ip>:4040`.

**28.** Para que serve o Spark UI, e qual é a limitação crítica de disponibilidade dele? *(Spark UI)*

R: É uma aplicação web para monitorar e depurar aplicações Spark. Traz informação detalhada de runtime, consumo de recursos e métricas úteis para diagnosticar problemas de performance. A limitação é que o Spark UI só está disponível enquanto a aplicação Spark está rodando.

**29.** Cite as seis abas da barra de navegação do Spark UI. *(Spark UI)*

R: Jobs, Stages, Storage, Environment, Executors e SQL.

**30.** Cite as seis seções da aba Environment, conforme a Table 2-3, e o que cada uma contém. *(Table 2-3)*

R:

| Seção | Conteúdo |
|---|---|
| Runtime Information | Localizações e versões dos componentes de que o Spark depende, entre eles Java e Scala |
| Spark Properties | Propriedades básicas e avançadas configuradas na aplicação. As básicas incluem application id e nome. As avançadas ligam, desligam ou ajustam features |
| Resource Profiles | Número de CPUs e quantidade de memória no Spark cluster |
| Hadoop Properties | Propriedades do Hadoop e do Hadoop File System |
| System Properties | Propriedades de nível de sistema operacional e de Java, não específicas do Spark |
| Classpath Entries | Lista de classpaths e jar files usados na aplicação |

A prosa imediatamente acima da tabela lista apenas quatro áreas: runtime information, spark properties, system properties e classpath entries. Ela omite Resource Profiles e Hadoop Properties, que a tabela traz.

**31.** O que a aba Executors contém, quais três recursos são nomeados, e o que a seção Summary fornece? *(Spark UI)*

R: Contém o resumo e o detalhamento de cada executor que sustenta a aplicação, com a capacidade de certos recursos e quanto de cada um está em uso. Os três recursos nomeados são memory, disk e CPU. A seção Summary dá a visão panorâmica do consumo de recursos em todos os executors da aplicação.

**32.** Qual variável é inicializada quando o shell sobe, qual classe ela instancia, e qual é o tipo totalmente qualificado dela segundo a Figure 2-16? *(Basic Interactions with Spark; Figure 2-16)*

R: A variável é `spark`, e ela representa uma instância da classe SparkSession. A Figure 2-16 mostra a saída de `:type spark`, que é o tipo totalmente qualificado `org.apache.spark.sql.SparkSession`. O pacote `sql` no meio do caminho é a parte informativa: o ponto de entrada único do Spark mora no módulo de SQL, não no core.

**33.** Em qual versão do Spark a `SparkSession` foi introduzida, e quais duas capacidades o capítulo atribui a ela? *(Basic Interactions with Spark)*

R: No Spark 2.0, para prover um ponto de entrada único às funcionalidades do Spark. As duas capacidades são: APIs para ler dados não estruturados e estruturados em formatos texto e binário, como JSON, CSV, Parquet e ORC. E o recurso de recuperar e definir configurações do Spark.

**34.** Escreva a expressão que imprime todas as configurações padrão. *(Basic Interactions with Spark)*

R:

```scala
spark.conf.getAll.foreach(println)
```

**35.** Quais são as quatro propostas de valor dos Collaborative Notebooks? *(Introduction to Collaborative Notebooks)*

R: Clusters Spark totalmente gerenciados. Um workspace interativo para exploração e visualização. Um scheduler de pipelines de produção. E uma plataforma para sustentar aplicações baseadas em Spark.

**36.** Quais são as duas versões do produto, e quais três features avançadas a edição paga acrescenta? *(Introduction to Collaborative Notebooks)*

R: As duas versões são a full platform, que é a edição comercial paga, e a community edition, que é gratuita. A edição paga acrescenta criação de múltiplos clusters, user management e job scheduling.

**37.** Quais são os três passos principais para chegar a um notebook funcionando? *(depois da Figure 2-23)*

R: Criar um cluster, criar uma folder e criar um notebook.

**38.** Quanta memória tem o cluster da community edition gratuita, quantos nós, em qual cloud, e depois de quanto tempo ocioso ele desliga? *(Create a Cluster)*

R: 15 GB de memória, um único nó, hospedado na AWS à época da escrita. Ele desliga sozinho após duas horas ocioso, o que dispensa desligá-lo manualmente.

**39.** Qual é o único campo obrigatório do formulário New Cluster, e quais são os outros quatro campos da Table 2-4? *(Table 2-4)*

R: O único obrigatório é o Cluster Name, que precisa ser único e aceita espaços. Os outros quatro são Databricks Runtime Version, com a versão mais recente já preenchida e cada versão atrelada a uma imagem AWS específica. Instance, que na community edition não oferece escolha. AWS – Availability Zone, que define em qual zona o nó roda. E Spark – Spark Config, para configurações específicas da aplicação usadas ao lançar o cluster.

**40.** Quanto tempo a criação do cluster pode levar, o que tentar se ela travar, e qual sinal visual indica sucesso? *(Create a Cluster)*

R: Pode levar até 10 minutos. Se a zona padrão demorar muito, o capítulo sugere trocar de availability zone. O sucesso aparece como um ponto verde ao lado do nome do cluster.

**41.** Qual é a analogia que o capítulo usa para o conceito de workspace no Databricks? *(Create a Folder)*

R: O file system do computador. A analogia serve para explicar que a hierarquia de pastas organiza os notebooks.

**42.** Quais são as duas maneiras de abrir o menu de criação de folder? *(Create a Folder)*

R: Clicar na seta para baixo no canto superior direito da Workspace column. Ou clicar com o botão direito em qualquer ponto da Workspace column.

**43.** O que uma cell de notebook contém, e qual é o magic command para uma cell Markdown? *(Create a Notebook)*

R: Cada cell contém um bloco de código para executar ou markup de documentação. O magic command é `%md`.

**44.** Qual combinação de teclas executa uma cell, e qual efeito colateral ela tem? *(Create a Notebook)*

R: Shift+Enter. O efeito colateral é criar uma nova cell logo abaixo da que foi executada.

**45.** O que o capítulo observa sobre salvar notebooks, e que evidência ele dá? *(Create a Notebook)*

R: Que o notebook salva sozinho, à medida que markup e código são digitados. A evidência é que o menu File não tem nenhuma opção de salvar.

**46.** Como se insere uma cell entre duas cells existentes? *(Create a Notebook)*

R: Levando o cursor do mouse para o espaço entre as duas e clicando no ícone de mais que aparece ali.

**47.** Como se compartilha um notebook, e quais duas coisas o destinatário pode fazer com o resultado? *(Create a Notebook)*

R: Pelo menu File, no submenu Publish, que abre uma caixa de confirmação e depois entrega uma URL. Com essa URL o destinatário pode ver o notebook ou importá-lo para o próprio workspace Databricks.

**48.** Quais são as duas maneiras de obter o código-fonte do Spark, e qual é o comando `git clone` que o capítulo dá? *(Setting up Spark Source Code)*

R: Baixar o package type Source Code na mesma página de download usada para o binário e descompactar o arquivo. Ou clonar o repositório do GitHub, o que exige ter git instalado. O comando dado é:

```
git clone git://github.com/apache/spark.git
```

**49.** Para onde o capítulo aponta para instruções de importar o fonte num IDE? *(Setting up Spark Source Code)*

R: Para `http://spark.apache.org/developer-tools.html`.

---

## Nível 2 — Compreensão

Explique com suas próprias palavras. Três a cinco frases cada.

**1.** Explique por que um REPL torna um iniciante mais produtivo que um ciclo editar-compilar-rodar, e qual classe de erro ele pega na hora. *(Spark Python Shell)*

R: O ciclo editar-compilar-rodar impõe uma espera entre escrever uma linha e saber se ela funciona. O REPL avalia cada linha assim que ela é digitada e devolve o resultado imediatamente, o que remove o passo de compilação. A classe de erro pega na hora é o erro de sintaxe, sinalizado antes de qualquer avaliação. Para quem está aprendendo, isso encurta o intervalo entre a hipótese e a correção, e é esse intervalo que determina o ritmo do aprendizado.

**2.** O capítulo diz que o shell não tem dependências externas "other than the data files you process". Explique o que isso implica sobre onde os dados precisam morar, e o que o capítulo diz sobre arquivos remotos. *(Having Fun with the Spark Scala Shell)*

R: A afirmação transfere a única dependência para os dados. Os arquivos processados precisam residir no próprio computador, e é por isso que o capítulo trata o shell como ferramenta de qualquer lugar, inclusive sem infraestrutura. Sobre arquivos remotos, ele diz que com conexão de internet o acesso é possível, mas lento. A implicação prática é que o shell local serve para datasets pequenos, e a lentidão de rede é o limite que empurra para outro ambiente.

**3.** Explique o modelo mental que o capítulo oferece para o Scala shell: "a Scala application with an empty body". O que esse modelo acerta, e o que a correção posterior acrescenta? *(Basic Interactions with Scala; Spark UI and Basic Interactions with Spark)*

R: O modelo acerta que o shell é um ambiente Scala completo e que quem digita preenche o corpo com funções e lógica. Ele descreve bem a parte da linguagem, que é o assunto daquela seção. A correção posterior diz que o shell é uma aplicação Spark escrita em Scala. O que ela acrescenta é que o corpo não está vazio: iniciar o shell já inicializa o Spark UI e variáveis como `spark`. O primeiro modelo sugere um ponto de partida em branco, e a correção mostra que existe uma aplicação Spark inteira em pé antes da primeira linha digitada.

**4.** Explique `ages.filter(age => isOddAge(age)).foreach(println)` pedaço por pedaço. Qual é a função anônima, o que `filter` retorna, e o que `foreach` consome? *(Basic Interactions with Scala)*

R: A função anônima é `age => isOddAge(age)`. Ela recebe um elemento do array e devolve o Boolean de `isOddAge`. O `filter` aplica essa função a cada elemento e retorna um novo array só com aqueles em que o resultado foi verdadeiro, ou seja, 35 e 41. O `foreach` consome os elementos desse array resultante, um por vez, e passa cada um para `println`. O capítulo chama esse encadeamento de function chaining e o apresenta como prática comum em Scala para deixar o código conciso.

**5.** Explique como o tab completion funciona como mecanismo de descoberta e não só como atalho de digitação, usando o exemplo `ages.fo`. *(Useful Spark Scala Shell Command and Tips)*

R: O cenário do exemplo é não lembrar o nome da função que itera sobre um array, sabendo apenas que ele começa com "fo". Digitar `ages.fo` e apertar Tab devolve os candidatos da classe Array que casam com esse prefixo, e é assim que `foreach` aparece. O ganho não é economizar teclas, é ler a superfície da API sem sair do shell e sem abrir documentação. Atalho de digitação exige saber o nome antes. Descoberta funciona com uma lembrança parcial.

**6.** Explique o que significa `spark` ser uma instância de `SparkSession` e não uma palavra-chave especial do shell. O que `:type spark` prova? *(Basic Interactions with Spark)*

R: Significa que `spark` é uma variável Scala comum, que aponta para um objeto de uma classe pública. O shell só poupa o trabalho de criá-la. `:type spark` prova isso ao devolver o nome da classe, e não algum tipo interno do shell. A consequência é que tudo o que a SparkSession oferece numa aplicação está disponível no shell pelo mesmo caminho, e que código explorado no shell tem tradução direta para uma aplicação, onde a mesma sessão precisa ser criada à mão.

**7.** Explique a diferença de propósito entre a aba Environment e a aba Executors. Qual delas abrir para uma pergunta de configuração e qual para uma pergunta de recursos? *(Spark UI; Table 2-3)*

R: A aba Environment traz informação estática sobre o ambiente em que a aplicação roda: versões, propriedades do Spark, propriedades do sistema, propriedades do Hadoop e classpath. Ela responde o que foi configurado. A aba Executors traz informação de runtime sobre capacidade e uso de memory, disk e CPU em cada executor. Ela responde quanto recurso existe e quanto está sendo consumido. Pergunta de configuração vai na Environment. Pergunta de recursos vai na Executors. A fronteira é a mesma entre o que foi declarado antes de começar e o que está acontecendo agora.

**8.** Explique a que `spark.conf.getAll` dá acesso, e por que a aba Environment e esse comando são duas visões de informação que se sobrepõe. *(Basic Interactions with Spark; Table 2-3)*

R: `spark.conf.getAll` dá acesso, de dentro do código, ao conjunto de configurações do Spark que a sessão enxerga. A aba Environment mostra essas mesmas Spark Properties num navegador. A sobreposição é parcial nos dois sentidos. A aba mostra mais coisa, porque acrescenta system properties, Hadoop properties e classpath entries, que a expressão não devolve. A expressão faz o que a aba não faz: entrega os pares como dado dentro do programa, filtrável e comparável. Uma visão serve para inspecionar, a outra para programar em cima.

**9.** Explique o trade-off entre o shell local e o notebook hospedado como ambientes de aprendizado. O que cada um dá que o outro não dá? *(Downloading and Installation + Introduction to Collaborative Notebooks + Summary)*

R: O shell local dá independência. Ele exige só Java instalado, funciona sem conta e sem internet, e o capítulo o vende como o ambiente da sala de estar, da praia ou de um bar no México. O notebook hospedado dá um cluster Spark totalmente gerenciado, sem instalação, com Markdown, visualização de dados, versionamento automático e compartilhamento por URL. O que o shell não dá é documentação junto do código, gráfico e colaboração. O que o notebook não dá é operação offline e controle sobre a instalação, e o cluster gratuito ainda desliga sozinho depois de duas horas ocioso.

**10.** Explique a prática recomendada pelo capítulo de quebrar a lógica de processamento em várias cells, e o princípio de engenharia de software a que ele compara isso. *(Create a Notebook, Note)*

R: A recomendação é separar a lógica em grupos lógicos, cada um em uma ou mais cells. O capítulo compara isso à prática de desenvolver aplicações de software manuteníveis, ou seja, à decomposição em unidades coesas. No notebook o argumento é mais forte que na aplicação, porque a cell também é a unidade de execução. Grupos pequenos permitem reexecutar só a parte que mudou, e um notebook de cell única perde essa granularidade.

**11.** Explique por que o capítulo trata o auto-save do notebook como algo digno de menção, e o que significa o menu File não ter opção de salvar. *(Create a Notebook)*

R: O auto-save contraria o hábito de quem vem de editor e IDE, onde salvar é ato deliberado e o Ctrl+S é reflexo. Procurar por um comando que não existe gera a dúvida de se o trabalho está perdido. A ausência da opção no menu File é a evidência de que salvar não é responsabilidade de quem escreve. A contrapartida, que o capítulo não discute, é que não existe ponto de salvamento escolhido por mim, então não há como marcar deliberadamente um estado bom antes de experimentar.

**12.** Explique o argumento a favor de ler o código-fonte do Spark mesmo sem nunca contribuir para ele. Que benefício secundário o capítulo alega? *(Setting up Spark Source Code)*

R: O argumento é que o Spark é open source e o código está publicamente disponível, então estudar como certas features foram implementadas é possível para qualquer pessoa. Isso vale mesmo sem contribuir, porque o objetivo é entender o funcionamento no nível do código, não mudá-lo. O benefício secundário alegado é aprender Scala: o capítulo afirma que o código foi escrito por alguns dos programadores Scala mais capazes do planeta, o que faz do repositório um material de leitura da linguagem. O argumento é de qualidade da fonte, e o capítulo não oferece nenhuma evidência para o superlativo.

**13.** Explique o que a seção Summary da aba Executors na Figure 2-15 está de fato mostrando, dado que essa aplicação só tem um driver e nenhum executor separado. *(Figure 2-15)*

R: A Figure 2-15 confirma a premissa: a tabela Executors tem uma linha só, com Executor ID `driver`, status Active e 12 cores. O Summary acima tem três linhas, Active(1), Dead(0) e Total(1). A de Active e a de Total repetem os mesmos 12 cores e a mesma storage memory da linha do driver. A de Dead está zerada, com 0 cores, porque nenhum executor morreu. A seção Summary agrega os recursos de uma única entidade, que é o próprio processo do shell. O total e o detalhamento coincidem, e a visão panorâmica prometida pelo capítulo não agrega nada. O que aparece ali é a memória e o número de CPUs da máquina local, e não uma soma de recursos de cluster. A seção só ganha função quando existem vários executors.

---

## Nível 3 — Laboratório prático

Faça no terminal. Anote o que aconteceu de fato quando diferiu do capítulo, porque esses deltas são a evidência do Nível 5.

**1.** Instale uma versão atual do Spark localmente. Registre a versão obtida, a versão do Scala no banner, a versão do Java e como o nome do diretório difere de `spark-3.1.1-bin-hadoop2.7`.

R: Pela página de download verificada no item 1 do Nível 5, o artefato atual é `spark-4.2.0-bin-hadoop3.tgz` e o diretório resultante é `spark-4.2.0-bin-hadoop3`. Duas diferenças em relação ao nome do livro: a versão passou de 3.1.1 para 4.2.0, e o sufixo `hadoop2.7` virou `hadoop3`, porque não existe mais build com Hadoop 2. O Scala esperado no banner é 2.13, e o Java precisa ser 17, 21 ou 25. Preciso rodar na minha máquina para registrar as versões reais do banner.

**2.** Liste o Spark directory e compare com a Table 2-1. Quais dos cinco subdiretórios documentados existem, e o que mais há que a tabela não menciona?

R: Os cinco da tabela, `bin`, `data`, `examples`, `jars` e `sbin`, devem existir. O capítulo diz que há cerca de uma dúzia de diretórios e descreve cinco, então sobram uns sete não documentados. Pelas notas do Damji sobre o mesmo assunto, `kubernetes` e `python` estão entre os que ficaram de fora, e há ainda `conf`, `licenses` e `yarn`, além de `README.md`. A lista exata só sai do `ls` na minha máquina.

**3.** Inicie o Scala shell. Copie o banner inteiro para as notas e identifique cada linha que reporta uma versão, uma URL ou um nome de variável.

R: O capítulo garante três coisas no banner: a URL do Spark UI, na linha `The SparkContext Web UI is available at http://<ip>:4040`, e as duas variáveis pré-inicializadas, `sc` e `spark`, cada uma anunciada com seu tipo. As versões esperadas são a do Spark, a do Scala e a da JVM. Preciso rodar na minha máquina para copiar o banner real.

**4.** Rode `:help`. Compare a saída com a Figure 2-4. Anote todo comando presente hoje que a figura não tem, e vice-versa.

R: A Figure 2-4 traz a saída de `:help` no Scala 2.12.10 e lista 23 comandos: `:completions`, `:edit`, `:help`, `:history`, `:h?`, `:imports`, `:implicits`, `:javap`, `:line`, `:load`, `:paste`, `:power`, `:quit`, `:replay`, `:require`, `:reset`, `:save`, `:sh`, `:settings`, `:silent`, `:type`, `:kind` e `:warnings`. A primeira linha avisa que todos aceitam abreviação, por exemplo `:he` no lugar de `:help`. A comparação com hoje precisa da máquina, porque essa lista vem do Scala REPL e não do Spark, então ela acompanha a versão do Scala. Com o salto de 2.12 para 2.13 é razoável esperar diferenças. Os seis da Table 2-2 devem continuar lá.

**5.** Reproduza toda a sequência Scala: `println`, `val ages`, tab-complete de `ages.fo`, `foreach(println)`, `def isOddAge`, o `filter(...).foreach(...)` encadeado, e as duas chamadas de `:type`. Confirme se cada saída bate com as figuras.

R: As saídas esperadas são estas. `println("Hello from Spark Scala shell")` imprime a mensagem e não devolve valor. `val ages = Array(20, 50, 35, 41)` devolve a variável com tipo `Array[Int]`. `ages.fo` mais Tab lista os candidatos que começam com "fo", entre eles `foreach`. `ages.foreach(println)` imprime 20, 50, 35 e 41, um por linha. `def isOddAge` devolve a assinatura da função. O encadeamento imprime 35 e 41. `:type ages` devolve `Array[Int]` e `:type isOddAge` devolve o tipo da função, de `Int` para `Boolean`. Preciso rodar na minha máquina para confrontar cada uma com as figuras.

**6.** Use `:paste` para digitar a função `isOddAge` como bloco multilinha. Compare essa experiência com digitá-la linha a linha e anote qual você usaria para trabalho real.

R: `:paste` abre um modo em que o bloco inteiro é lido antes de ser avaliado, encerrado por ctrl-d. Digitada linha a linha, a definição fica em modo de continuação até fechar a chave, e qualquer erro no meio obriga a recomeçar o bloco. Para trabalho real a escolha é `:paste` para blocos colados de fora, e `:load` de um arquivo quando a lógica é longa, que é o que a Table 2-2 recomenda. O capítulo não menciona `:paste` em nenhum momento, e essa ausência é o assunto do item 3 do Nível 4.

**7.** Rode `:type spark` e `:type sc`. Registre os dois tipos totalmente qualificados. Guarde esse resultado, porque ele resolve uma questão levantada pelo outro livro.

R: O esperado é `org.apache.spark.sql.SparkSession` para `spark` e `org.apache.spark.SparkContext` para `sc`. São dois objetos distintos, de pacotes distintos, e é isso que resolve a imprecisão do Damji tratada no item 1 do Nível 6. Preciso rodar na minha máquina para registrar os dois tipos como saída real, e não como expectativa.

**8.** Rode `spark.conf.getAll.foreach(println)`. Separe as entradas de `spark.master`, `spark.app.name`, `spark.app.id` e `spark.home`. O que o valor de `spark.master` diz sobre quantas threads estão em uso?

R: Os valores esperados são `spark.master = local[*]`, `spark.app.name = Spark shell`, um `spark.app.id` do tipo `local-<timestamp>` e o `spark.home` apontando para o diretório de instalação. `local[*]` significa modo local com tantas threads quantos forem os cores lógicos da máquina, ou seja, o asterisco delega a contagem ao hardware. O capítulo nunca explica essa notação, nem usa o termo local mode. Preciso rodar na minha máquina para saber a quantos cores o asterisco corresponde aqui.

**9.** Abra o Spark UI. Visite as seis abas. Na aba Executors, registre a contagem de cores e a storage memory, e reconcilie com a máquina em uso.

R: Em modo local existe uma linha só, rotulada `driver`. A contagem de cores deve bater com os cores lógicos da máquina, o mesmo número que `local[*]` resolveu no item anterior. A storage memory é uma fração da heap do driver, e não a memória total da máquina, então os dois números não coincidem. Preciso rodar na minha máquina para registrar os valores e fazer a reconciliação.

**10.** Na aba Environment, encontre cada seção listada na Table 2-3. `Resource Profiles` está presente? Anote o que ela reporta.

R: As seis seções da Table 2-3 continuam documentadas na versão atual, conforme verifiquei no item 9 do Nível 5, e `Resource Profiles` é uma delas. A documentação atual a descreve como os pedidos de CPU, memória e recursos aceleradores de cada resource profile, o que é mais preciso que a descrição do capítulo. Em modo local deve haver um único profile, o default. A versão atual acrescenta uma sétima seção, `Metrics Properties`, que a Table 2-3 não tem.

**11.** Rode `spark.range(0, 10000, 1, 8)` seguido de uma action, depois olhe as abas Jobs e Stages. Quantas tasks foram criadas, e o que isso diz sobre a relação partition-para-task?

R: O quarto argumento de `spark.range` é o número de partitions, então 8 partitions produzem 8 tasks em um stage. A relação é de uma task por partition por stage. Nada disso está no capítulo, que manda olhar as abas Jobs e Stages sem nunca dizer o que elas contam, e é essa lacuna que o item 12 do Nível 5 registra. Preciso rodar na minha máquina para confirmar a contagem na aba Stages.

**12.** Saia do shell, depois recarregue a URL do Spark UI. Confirme a alegação do capítulo sobre disponibilidade da UI. Depois descubra o que seria necessário para inspecionar uma aplicação já concluída.

R: A porta 4040 deve deixar de responder, porque o Spark UI vive dentro do processo da aplicação. Isso confirma a alegação do capítulo. Para inspecionar uma aplicação concluída é preciso ter ligado o event logging antes de rodá-la, com `spark.eventLog.enabled` e `spark.eventLog.dir`, e depois subir o Spark History Server. Os detalhes verificados estão no item 10 do Nível 5. Preciso rodar na minha máquina para ver a falha de conexão.

**13.** Use `:save` para escrever a sessão em um arquivo, depois inicie um shell novo e faça `:load` dele. Confirme que a ida e volta funciona.

R: `:save` grava as linhas da sessão em um arquivo e `:load` as executa em ordem num shell novo, o que reconstrói variáveis e funções. A volta só funciona se a sessão não tiver linhas com erro, porque `:load` reexecuta tudo. A Table 2-2 documenta `:load` e não documenta `:save`, ou seja, o capítulo ensina a metade da ida e volta. Preciso rodar na minha máquina para confirmar o ciclo.

**14.** Clone o fonte do Spark com o comando que o capítulo dá. Se falhar, diagnostique o motivo e ache o equivalente que funciona. Registre a falha e a correção.

R: O comando do capítulo falha, e a falha é certa, não provável. O GitHub desativou o protocolo `git://` de forma permanente em 15 de março de 2022, depois de dois brownouts, conforme verifiquei no item 4 do Nível 5. O erro esperado é uma recusa de conexão na porta 9418. A correção é usar HTTPS, com `git clone https://github.com/apache/spark.git`, ou SSH, com `git clone git@github.com:apache/spark.git`. Preciso rodar na minha máquina para registrar a mensagem de erro exata.

**15.** Cadastre-se no que hoje é a camada gratuita da Databricks, crie um cluster, crie uma folder e crie um notebook Scala com uma cell `%md` e os exemplos de `ages`. Registre cada passo em que as instruções do capítulo não batem mais com a interface.

R: A divergência começa no cadastro. A Community Edition foi aposentada em 2025 e substituída pela Databricks Free Edition, conforme o item 5 do Nível 5. O passo "criar um cluster" não tem mais equivalente direto, porque a Free Edition entrega serverless compute junto com o workspace, então some o formulário New Cluster inteiro e com ele a Table 2-4, a espera de 10 minutos, a escolha de availability zone e o ponto verde. O que deve sobreviver é o conceito de workspace com pastas, o `%md` e o shift+enter. Preciso fazer o cadastro e percorrer a interface para registrar os deltas restantes.

---

## Nível 4 — Análise

Seção curta por decisão de projeto. Estas questões são sobre ler criticamente os artefatos do próprio capítulo.

**1.** **O dump de configuração se entrega.** Olhe de perto os caminhos na Figure 2-19. Qual versão do Spark aparece em `spark.home`, `hive.metastore.warehouse.dir` e `spark.repl.class.outputDir`? Compare com a versão que o capítulo está ensinando. O que aconteceu, e o que se deve concluir sobre tratar qualquer captura de tela de um livro técnico como evidência da versão em discussão?

R: A figura se entrega mesmo. Os caminhos em `spark.home` e em `hive.metastore.warehouse.dir` apontam para um diretório `spark-2.1.1-bin-hadoop2.7`, e o `spark.repl.class.outputDir` cai num temporário criado pela mesma instalação. Ou seja, o dump foi capturado numa instalação do **Spark 2.1.1**, e não na 3.1.1 que o capítulo manda baixar e usa em todo o resto.

A screenshot é de uma edição anterior do livro e foi reaproveitada sem recaptura. Duas consequências para quem estuda por ele. Primeira, o valor mostrado em `spark.home` contradiz o diretório que a pessoa acabou de criar seguindo a seção de instalação. Segunda, e mais séria, a lista de configurações padrão de uma versão 2.1.1 não é confiável como retrato do que uma 3.1.1 traz, porque propriedades entram e saem entre releases maiores. E é um caso isolado dentro do capítulo, não um padrão. Abri a Figure 2-25 para comparar e ela mostra `Runtime: 8.3 (Scala 2.12, Spark 3.1.1)`, ou seja, uma captura contemporânea do livro, que empacota exatamente a versão do Spark que o capítulo ensina. A Figure 2-19 é a única captura defasada que encontrei.

A conclusão geral independe da figura. Uma captura de tela é evidência da máquina de quem escreveu no dia em que escreveu, e nada mais. Ela não é atualizada quando o texto é atualizado, porque refazer imagem custa mais que corrigir um número. Quando a captura discorda da prosa, a discordância revela a data de gravação, e não uma alternativa válida.

O capítulo tem um caso do mesmo defeito que eu consigo checar no texto. A seção Downloading Spark afirma que existe um jeito de construir o binário a partir do fonte e que as instruções "are covered later in the chapter". A seção Setting up Spark Source Code ensina a baixar e a clonar o fonte e nunca ensina a construir nada. A promessa não é cumprida em lugar nenhum do capítulo. Isso e a divergência de versões nas figuras têm a mesma causa: partes escritas em momentos diferentes e nunca reconciliadas.

**2.** **A community edition contradiz a si mesma.** A seção Create a Cluster afirma que uma conta gratuita pode criar vários clusters ao mesmo tempo. Encontre as duas afirmações posteriores que a contradizem frontalmente. Qual está correta, e o que a contradição sugere sobre como o capítulo foi montado?

R: A afirmação inicial é que, por ser gratuita, a conta CE "provides the capability to create multiple clusters simultaneously". As duas contradições posteriores são estas. Ainda em Create a Cluster, logo após a Figure 2-26: tentar criar outro cluster pelos mesmos passos não é permitido. Em Create a Notebook: o campo de cluster já vem preenchido "because the CE edition can only have one cluster at a time".

Existe ainda uma terceira, anterior às duas. A seção que apresenta as duas versões do produto lista "creating multiple clusters" como feature avançada da edição paga. Se é feature da edição paga, a gratuita não a tem.

O correto é um cluster por vez. Ele é sustentado por três passagens contra uma, e a passagem isolada é a única que não descreve uma tela nem um comportamento observado. Ela também traz um raciocínio que não se sustenta: ser gratuita não é razão para permitir mais clusters, é razão para permitir menos.

Sobre a montagem, a contradição sugere seções escritas em momentos separados, com revisão por seção e sem passe de consistência no capítulo inteiro. As duas afirmações corretas estão coladas à interface, e a errada está num parágrafo de contexto solto.

**3.** **Um comando ausente.** A Figure 2-4 lista todos os comandos do shell, e a Table 2-2 seleciona seis como "commonly used". Um comando da figura é discutivelmente mais útil para trabalho real que vários que entraram na tabela, e o capítulo nunca o menciona. Identifique-o e defenda a inclusão dele.

R: Confirmo a lista pela Figure 2-4, e o comando ausente que eu defendo é o `:paste`. Ele entra em modo de colagem e aceita um bloco inteiro de uma vez, o que resolve o problema mais frequente de quem está aprendendo no shell: colar várias linhas e ver o REPL avaliar cada uma isolada, quebrando definições que dependem umas das outras. A Table 2-2 escolheu `:history`, `:load`, `:reset`, `:silent`, `:type` e `:quit`. O `:load` cobre o caso vizinho, executar código de arquivo, mas exige sair do shell e criar um arquivo. Para quem está experimentando, `:paste` tem uso mais imediato que `:silent` ou `:reset`.

A defesa é direta. O capítulo define `isOddAge` como bloco de três linhas e não diz como entrar com ele. Digitar bloco multilinha no REPL depende do modo de continuação, que quebra em qualquer colagem com linha em branco ou indentação inesperada. `:paste` existe exatamente para isso: ler o bloco inteiro e avaliar depois. É o primeiro obstáculo real de quem sai do exemplo de uma linha e vai para uma função, e é o único exemplo de código com mais de uma linha do capítulo.

Comparado com os seis eleitos, `:paste` ganha de `:silent`, descrito como recurso para usuário avançado cansado de ver saída, e de `:reset`, que corrige um problema criado por uma sessão longa. O runner-up é `:save`, que fecha a ida e volta com `:load`, presente na tabela sem a contraparte.

**4.** **A limitação fundamental da UI.** O capítulo observa que o Spark UI só existe enquanto a aplicação roda, e segue adiante. Trace a consequência: como diagnosticar um job que falhou às três da manhã? Nomeie o componente que o capítulo precisava mencionar e não mencionou.

R: A consequência é que a única ferramenta de diagnóstico ensinada pelo capítulo é inútil justamente no caso em que diagnóstico é necessário. Job que falha às três da manhã é job sem observador, e quando alguém olha a aplicação já morreu junto com a porta 4040. O que sobra são logs de texto, sem stages, sem tempos por task e sem métricas de recurso.

O componente ausente é o Spark History Server. Ele reconstrói a UI de uma aplicação encerrada a partir de event logs persistidos, desde que o event logging tenha sido ligado antes da execução. Verifiquei o funcionamento no item 10 do Nível 5.

A ausência é grave porque a ordem importa. O event log precisa estar ligado antes da falha, então quem só descobre o History Server depois do primeiro incidente não tem o que ler sobre esse incidente. Um capítulo que ensina a abrir o Spark UI e não menciona o custo de não ter ligado o log deixa o leitor descobrir isso do pior jeito possível.

**5.** **O que a Figure 2-15 de fato mostra.** A legenda diz que é uma aplicação "that uses only a single executor", mas a tabela Executors lista uma linha, rotulada `driver`. Reconcilie a legenda com o dado. O que isso revela sobre local mode que o capítulo nunca declara explicitamente?

R: A reconciliação é que a legenda e o dado descrevem a mesma coisa por nomes diferentes. Não há um executor separado e um driver. Há um processo só, que acumula os dois papéis, e a UI o rotula pelo papel de driver. A legenda chama de "único executor" o que a tabela chama de driver, e as duas estão certas porque são a mesma JVM.

O que isso revela é o próprio local mode: driver e executor no mesmo processo, na mesma máquina, com paralelismo por threads em vez de processos. O capítulo nunca usa o termo local mode. Ele também nunca usa as palavras driver, task, core ou partition, o que verifiquei com busca no texto inteiro. Ou seja, o único ambiente que o capítulo ensina a operar é aquele cujo nome ele não pronuncia.

A consequência prática é que a arquitetura desenhada no capítulo 1, com driver de um lado e executors do outro, não é observável neste laboratório. Quem não souber disso vai procurar na aba Executors uma separação que em local mode não existe, e concluir que a UI está incompleta.

**6.** **A aba Environment como ferramenta de ensino.** A Table 2-3 distingue Spark Properties de System Properties de Hadoop Properties. Usando apenas o capítulo, explique por que um problema de configuração pode se esconder em qualquer uma das três, e construa uma ordem de diagnóstico para checá-las.

R: As três camadas existem porque a aplicação empilha três configurações independentes. As Spark Properties são o que a aplicação declarou: id, nome e os ajustes que ligam ou desligam features. As System Properties são de nível de sistema operacional e de Java, e o capítulo é explícito ao dizer que não são específicas do Spark. As Hadoop Properties vêm do Hadoop e do Hadoop File System.

Um problema se esconde em qualquer uma porque cada camada pode contradizer o que a de cima pediu. Uma propriedade Spark pode nunca ter sido aplicada. A JVM pode estar em versão ou locale diferente do esperado. O acesso a arquivos pode falhar por configuração de Hadoop, sem que nada nas Spark Properties pareça errado. A Runtime Information e as Classpath Entries acrescentam duas causas fora das três: versão errada de componente e jar duplicado ou ausente no classpath.

A ordem de diagnóstico que proponho vai do mais provável e mais barato ao mais raro:

1. Spark Properties, para confirmar se a propriedade suspeita está lá e com o valor esperado. Se não estiver, o problema é de como ela foi passada, e não do Spark.
2. Runtime Information, para conferir versão de Java e de Scala, que é a causa mais comum de erro que não parece de configuração.
3. Hadoop Properties, quando o sintoma envolve leitura ou escrita de arquivos.
4. System Properties, para diferenças de ambiente do sistema operacional e da JVM.
5. Classpath Entries, por último, porque é a checagem mais cara de ler e a que resolve os casos raros de conflito de jar.

**7.** **Nomenclatura do produto.** O capítulo diz "Collaborative Notebooks" de forma consistente para aquilo que os praticantes chamam de outra coisa. Encontre todas as formas que o capítulo usa para o produto Databricks e suas edições. O que a nomenclatura inconsistente custa a quem procura ajuda?

R: As formas que o capítulo usa para o produto e para o ambiente são: "Collaborative Notebooks", "Databricks", "the Databricks product", "Spark Notebook", "Databricks notebook" e "Scala Notebook". Para as edições: "the full platform", "the commercial edition", "a paid product", "the community edition", "community edition (CE)", "the CE edition" e "the CE".

"Collaborative Notebooks" é o nome que quase ninguém usa. O produto é Databricks, e o recurso é notebook. "CE edition" ainda é redundante, porque expande para "community edition edition".

O custo para quem procura ajuda é concreto. Busca por "Collaborative Notebooks" não encontra a documentação, o fórum nem as respostas de Stack Overflow, que falam em Databricks notebooks. Quem segue o livro aprende o vocabulário errado e leva esse vocabulário para a busca, então o erro se paga duas vezes. Nomes de produto também são o item que mais envelhece num livro, e este envelheceu: a community edition não existe mais, como registro no item 5 do Nível 5.

**8.** **Honestidade de escopo.** O capítulo declara que montar um cluster Spark de produção multitenant está fora de escopo. Dado o que o capítulo *ensina*, liste o que ainda não se saberia fazer ao terminá-lo, e decida se a declaração de escopo é aviso suficiente.

R: Ao terminar o capítulo continuo sem saber fazer:

- Submeter uma aplicação pela linha de comando. É a segunda das três opções anunciadas na abertura, e não tem seção nenhuma. `spark-submit` não aparece no capítulo.
- Construir o binário a partir do fonte, promessa feita na seção de download e nunca cumprida.
- Escrever, empacotar e rodar uma aplicação Spark fora do shell e fora do notebook.
- Ler as abas Jobs, Stages, Storage e SQL, que o capítulo nomeia e adia para capítulos posteriores.
- Processar um arquivo de dados. Todos os exemplos são um array Scala de quatro inteiros. Nenhuma linha do capítulo lê dado.
- Diagnosticar uma aplicação encerrada, que é o item 4 deste nível.
- Configurar qualquer cluster manager, ou entender o que `local[*]` significa.

A declaração de escopo não é aviso suficiente, por três razões. Ela cobre só o cluster de produção multitenant, que é o item mais distante da lista. Ela não cobre as duas promessas quebradas dentro do próprio capítulo, e promessa quebrada é pior que escopo estreito. E ela não avisa que nenhum dado será processado, que é a lacuna mais surpreendente para quem espera um capítulo prático de uma ferramenta de processamento de dados.

**9.** **Dois shells, um livro.** O capítulo apresenta os shells Scala e Python como iguais, depois anuncia que o resto do livro usa Scala. Se você trabalha em Python, catalogue o que este capítulo ensinou que não transfere. Comece pelos comandos `:`.

R: Não transfere:

- Todos os comandos `:`. `:help`, `:history`, `:load`, `:reset`, `:silent`, `:quit` e `:type` são do REPL do Scala. O próprio capítulo mostra a assimetria sem comentá-la: sai-se do Scala shell com `:quit` e do Python shell com ctrl-d.
- A sintaxe dos exemplos: `val`, `Array`, a assinatura `def isOddAge(age:Int) : Boolean`, o retorno implícito da última expressão e a lambda com `=>`.
- O `:type` como forma de inspecionar tipo. Python resolve isso por outro caminho, que o capítulo não mostra.
- A seção inteira "Basic Interactions with Scala", que é sobre a linguagem e não sobre Spark.

Transfere:

- O modelo REPL e o argumento sobre feedback imediato.
- A variável `spark` como instância de SparkSession, e a ideia de que ela já vem pronta no shell.
- O Spark UI inteiro: porta 4040, as seis abas, Environment e Executors. Nada disso depende de linguagem.
- O notebook do Databricks, com a ressalva de que a linguagem é escolhida na criação.
- O `pyspark` em si, ensinado a subir e a sair.

Uma incerteza que registro: se `spark.conf.getAll.foreach(println)` tem forma idêntica em PySpark é coisa que o capítulo não diz, e eu não verifiquei.

O saldo é que boa parte do capítulo é ensino de Scala, não de Spark. Para quem trabalha em Python, o valor está no Spark UI e no notebook, e a decisão do livro pela via Scala custa mais do que uma escolha de sintaxe.

---

## Nível 5 — Além do capítulo (backlog, não notas)

**A maior seção deste guia, de propósito.** Cada instrução de um capítulo procedural é uma afirmação sobre uma interface ou uma página de download, e essas coisas mudam. O capítulo foi escrito contra o Spark 3.1.1 em 2021. Verifiquei estes itens contra fonte primária em **2 de agosto de 2026**, quando a documentação corrente do Spark era a **4.2.0**. As URLs estão ao fim da seção.

**1.** **Versão atual do Spark e nomenclatura dos pacotes.** O `spark-3.1.1-bin-hadoop2.7` do capítulo reflete uma opção de build com Hadoop 2.7. Descubra a versão atual, as escolhas atuais de package type e a versão atual do Scala. A variante `hadoop2.7` em particular merece checagem.

R: A versão atual é a **Spark 4.2.0**, lançada em **14 de julho de 2026** segundo a página de download. O diretório de release no mirror da Apache lista os artefatos, e é ali que a mudança de nomenclatura fica evidente:

- `spark-4.2.0-bin-hadoop3.tgz`
- `spark-4.2.0-bin-hadoop3-connect.tgz`
- `spark-4.2.0-bin-without-hadoop.tgz`
- `spark-4.2.0.tgz`, que é o fonte
- `pyspark-4.2.0.tar.gz`, `pyspark_connect-4.2.0.tar.gz`, `pyspark_client-4.2.0.tar.gz` e `SparkR_4.2.0.tar.gz`

A variante `hadoop2.7` não existe mais. O único build com Hadoop embutido é `hadoop3`, e a alternativa é o `without-hadoop`, para quem já tem Hadoop instalado. Apareceu também uma variante `hadoop3-connect`, que não tinha equivalente em 2021.

Sobre Scala, a nota da página de download hoje diz: *"Note that Spark 4 is pre-built with Scala 2.13, and support for Scala 2.12 has been officially dropped. Spark 3 is pre-built with Scala 2.12 in general and Spark 3.2+ provides additional pre-built distribution with Scala 2.13."* A instrução do capítulo de "escolher o package type com a versão mais recente do Hadoop" continua correta em espírito, mas o nome do artefato mudou.

**2.** **Matriz de suporte de Java e Scala.** "Java 11 or higher is preferred" e Scala 2.12 eram verdade então. Descubra as versões de Java hoje suportadas e recomendadas, e se o Scala 2.13 virou o padrão. Errar isso produz falhas de startup confusas, e não erros limpos.

R: A documentação 4.2.0 é explícita: *"Spark runs on Java 17/21/25, Scala 2.13, Python 3.10+, and R 4.0+ (Deprecated). Java 25 prior to version 25.0.3 support is deprecated as of Spark 4.2.0. When using the Scala API, it is necessary for applications to use the same version of Scala that Spark was compiled for. Since Spark 4.0.0, it's Scala 2.13."*

Três consequências para o capítulo. Java 11, que ele apresenta como preferido, hoje está abaixo do piso e não é suportado. Scala 2.13 não é só padrão, é o único: o suporte a 2.12 foi oficialmente removido no Spark 4. E o suporte a R está marcado como deprecated. A exigência de casar a versão de Scala da aplicação com a do Spark é a fonte típica das falhas confusas que o enunciado antecipa.

**3.** **Piso da versão de Python.** "Python 3.7.x or higher" certamente envelheceu. Descubra o mínimo atual, e cheque se o PySpark hoje tem caminhos de instalação que o capítulo não menciona, como instalação por índice de pacotes em vez de tarball descompactado.

R: O mínimo atual é **Python 3.10**. O PySpark também exige Java 17 ou superior, com `JAVA_HOME` configurado.

Os caminhos de instalação que o capítulo não menciona existem e são vários:

- `pip install pyspark`, com seis extras documentados: `sql`, `ml`, `mllib`, `connect`, `pandas_on_spark` e `pipelines`
- `PYSPARK_HADOOP_VERSION=3 pip install pyspark`, para escolher a versão de Hadoop
- `pip install pyspark-connect`, que torna o Spark Connect o padrão
- `pip install pyspark-client`, só o cliente Python do Spark Connect
- Conda, via `conda install -c conda-forge pyspark`

A única dependência obrigatória listada é `py4j`, na faixa `>=0.10.9.7,<0.10.9.10`. Para quem só quer PySpark, o tarball do capítulo deixou de ser o caminho natural.

**4.** **O protocolo git.** O capítulo dá `git clone git://github.com/apache/spark.git`. Cheque se o protocolo `git://` não autenticado ainda funciona contra o GitHub. Este é um caso de falha quase certa. Descubra quando e por que mudou, e qual é o esquema de URL correto hoje.

R: Falha certa. O GitHub desativou o protocolo Git não criptografado, com a justificativa de que *"unencrypted `git://` offers no integrity or authentication, making it subject to tampering"*. A cronologia foi: brownouts em **2 de novembro de 2021** e **11 de janeiro de 2022**, e desativação **permanente em 15 de março de 2022**.

O blog do GitHub instrui a garantir que a URL comece com `ssh://`, `https://` ou `git@github.com`. Os equivalentes corretos hoje são:

```
git clone https://github.com/apache/spark.git
git clone git@github.com:apache/spark.git
```

O comando do livro nasceu com pouco mais de meio ano de validade, porque o livro é de 2021 e o primeiro brownout foi em novembro daquele ano.

**5.** **Databricks Community Edition.** É a coisa mais provável de ter mudado no capítulo, e boa parte do terço central depende dela: a camada gratuita, o nome dela, a alocação de recursos, se "15 GB single node on AWS" ainda descreve o produto, e se a URL de cadastro resolve. Estabeleça qual é a opção gratuita atual antes de tentar o item 15 do Nível 3.

R: Mudou, e mudou por inteiro. A documentação da Databricks diz: *"Databricks Free Edition is a no-cost version of Databricks designed for students, educators, hobbyists, and anyone interested in learning or experimenting with data and AI."* E, sobre a antecessora: *"Free Edition replaced the legacy Databricks Community Edition, which was retired in 2025."*

O modelo de recursos também mudou: *"Your new workspace includes serverless compute and default storage, so you can immediately start exploring and building on Databricks."* Ou seja, não há mais cluster de nó único que a pessoa cria e nomeia. A descrição de "15 GB, nó único, AWS" não descreve mais nada, e com ela caem o formulário New Cluster, a Table 2-4, a espera de até 10 minutos, a escolha de availability zone e o ponto verde.

O cadastro hoje é por `https://login.databricks.com/`, e não pela URL `databricks.com/try-databricks` do capítulo. As limitações da Free Edition estão numa página própria da documentação, que não consultei.

**6.** **URLs da documentação Databricks.** Todo link `docs.databricks.com/user-guide/...` do capítulo segue uma estrutura de caminho antiga. Cheque se eles redirecionam ou dão 404, e anote a raiz atual da documentação.

R: Testei dois dos links do capítulo. Nenhum dos dois dá 404, os dois redirecionam:

- `docs.databricks.com/user-guide/index.html` entrega a página "Workspace UI".
- `docs.databricks.com/user-guide/notebooks/index.html` entrega "Databricks notebooks", cujo caminho atual é `/aws/en/notebooks/`.

A raiz atual é `docs.databricks.com/aws/en/`, com o segmento de cloud e o de idioma no caminho. Existem caminhos irmãos para as outras clouds. Os links do livro funcionam por redirecionamento, mas o destino não é mais a página que o capítulo tinha em mente, então usar o caminho atual é mais seguro.

**7.** **Databricks Runtime e Delta.** A Figure 2-25 mostra o Runtime 8.3, com nota de que "Databricks Runtime 8.x uses Delta Lake as the default table format". Descubra a numeração atual dos runtimes e qual é o formato de tabela padrão hoje. Isso conecta direto com a sua disciplina de lakehouse.

R: A numeração avançou muito. Os runtimes suportados hoje, com o Spark que cada um empacota:

| Databricks Runtime | Data | Apache Spark |
|---|---|---|
| 19 | 15 de junho de 2026 | 4.2.0 |
| 18 LTS | 10 de junho de 2026 | 4.1.0 |
| 17.3 LTS | 22 de outubro de 2025 | 4.0.0 |
| 16.4 LTS | 9 de maio de 2025 | 3.5.2 |
| 15.4 LTS | 19 de agosto de 2024 | 3.5.0 |
| 14.3 LTS | 1 de fevereiro de 2024 | 3.5.0 |
| 13.3 LTS | 22 de agosto de 2023 | 3.4.1 |

O Runtime 8.3 da figura está cerca de onze gerações atrás e não consta mais como suportado.

Sobre o formato de tabela, a documentação atual é categórica: *"Delta Lake is the default format for all operations on Databricks. Unless otherwise specified, all tables on Databricks are Delta Lake tables."* A nota da Figure 2-25, que anunciava isso como novidade do Runtime 8.x, virou a condição normal. Não consultei nesta rodada a relação disso com Unity Catalog nem o estado do suporte a Iceberg, e deixo os dois como backlog.

**8.** **Interface do notebook.** O próprio hedge do capítulo, "over time, the welcome page may evolve", está trabalhando demais. Verifique: o magic `%md`, o Shift+Enter, a inserção pelo ícone de mais, o auto-save e o File → Publish. Descubra também que features de notebook existem hoje e não tinham equivalente em 2021.

R: Verifiquei duas coisas e não consegui confirmar três.

Confirmado. O `%md` continua: *"`%md`: Allows you to include various types of documentation, including text, images, and mathematical formulas and equations."* O atalho de execução continua: para rodar uma cell, *"click in the cell and press shift+enter"*.

Não confirmado nas páginas que li. O efeito colateral de criar uma cell nova abaixo, a inserção pelo ícone de mais, o auto-save e o caminho File → Publish. As páginas de execução e de código não tratam desses pontos. Deixo os quatro como pendência de verificação na interface, junto com o item 15 do Nível 3.

Novidades sem equivalente em 2021. A documentação atual lista mais magics de linguagem, `%python`, `%r`, `%scala` e `%sql`, com troca de linguagem por cell tanto pelo magic quanto por um botão. Há atalho de formatação de código, Cmd+Shift+F. A página inicial de notebooks anuncia colaboração em tempo real, versionamento automático, visualizações embutidas e features de IA, além de compute serverless. O capítulo não tem equivalente para nada disso.

**9.** **Abas do Spark UI.** O capítulo lista seis. Cheque o conjunto atual de abas, em especial se entraram abas de Structured Streaming e de connect, e o que a aba Environment expõe hoje.

R: A documentação atual descreve **nove** abas. As seis do capítulo continuam: Jobs, Stages, Storage, Environment, Executors e SQL. Entraram três:

- **Structured Streaming**, disponível para jobs em modo micro-batch
- **Streaming (DStreams)**, para a API antiga
- **JDBC/ODBC Server**, quando o Spark roda como engine SQL distribuído

Não encontrei aba dedicada a Spark Connect na página de web UI, então a hipótese de aba de connect não se confirma por esta fonte.

A aba Environment ganhou uma seção. As seis da Table 2-3 continuam, e a sétima é **Metrics Properties**, com a configuração carregada do sistema de métricas. As descrições também ficaram mais precisas: Resource Profiles hoje é descrita como os pedidos de CPU, memória e recursos aceleradores por resource profile, o que é mais informativo que "número de CPUs e quantidade de memória no cluster".

**10.** **O History Server.** Não é mencionado no capítulo, e é a resposta óbvia para o item 4 do Nível 4. Descubra o que é, como habilitar event logging e como ler a UI de uma aplicação concluída. Dado o seu trabalho de observabilidade de scrapers, este é o item de maior valor da lista.

R: A documentação de monitoring confirma a limitação e dá a saída. Sobre a limitação: *"Note that this information is only available for the duration of the application by default."* Sobre a saída: *"It is still possible to construct the UI of an application through Spark's history server, provided that the application's event logs exist."*

O procedimento tem três passos, e o primeiro precisa acontecer antes da execução:

1. Ligar o event logging antes de iniciar a aplicação: *"To view the web UI after the fact, set `spark.eventLog.enabled` to true before starting the application."* Junto com ele vai o destino, `spark.eventLog.dir`, no exemplo da doc apontado para `hdfs://namenode/shared/spark-logs`.
2. Apontar o History Server para o diretório dos logs, em `spark.history.fs.logDirectory`, cujo default é `file:/tmp/spark-events`. O diretório precisa conter um subdiretório por aplicação.
3. Subir o serviço com `./sbin/start-history-server.sh`, que cria a interface web em `http://<server-url>:18080` por padrão. Ela lista aplicações completas e incompletas.

O ponto operacional é a ordem. Sem o log ligado antes, não há nada para reconstruir depois, então habilitar event logging por padrão em qualquer job agendado é decisão de configuração, não de investigação.

**11.** **`spark.conf` contra `SparkConf` contra propriedades ajustáveis em runtime.** O capítulo mostra `spark.conf.getAll` e diz que a `SparkSession` pode "retrieve and set" configurações, sem distinguir quais propriedades são ajustáveis em runtime e quais precisam ser definidas no launch. Descubra a regra, porque isso aparece na primeira vez que se tenta mudar algo depois que a sessão já existe.

R: A regra tem três partes na documentação de configuração.

**Duas categorias de propriedade.** A doc separa: *"Spark properties mainly can be divided into two kinds: one is related to deploy, like 'spark.driver.memory', 'spark.executor.instances', this kind of properties may not be affected when setting programmatically through `SparkConf` in runtime (...) another is mainly related to Spark runtime control, like 'spark.task.maxFailures', this kind of properties can be set in either way."* Propriedade de deploy vai em arquivo de configuração ou em opção de linha de comando. Propriedade de controle de runtime aceita os dois caminhos.

**O caso duro.** Em client mode, `spark.driver.memory` não pode ser definida via `SparkConf`, *"because the driver JVM has already started at that point"*. A mesma restrição vale para `spark.driver.extraClassPath`, `spark.driver.defaultJavaOptions`, `spark.driver.extraJavaOptions` e `spark.driver.extraLibraryPath`. O motivo é temporal: o processo já subiu, então o parâmetro já foi consumido.

**Precedência.** Do maior para o menor: propriedades definidas direto no `SparkConf`, depois `--conf` e `--properties-file` passados ao `spark-submit` ou ao `spark-shell`, depois o arquivo `conf/spark-defaults.conf`.

Fica uma pendência. A doc de configuração tem uma seção "Static SQL Configuration", separada de "Runtime SQL Configuration", e é ali que mora a lista de propriedades SQL que não mudam depois que a sessão sobe. A página que li truncou essa lista, então não a registro aqui. A distinção entre as duas seções já basta para a regra prática: `spark.conf.set` funciona para configuração de runtime e falha para configuração estática.

**12.** **Conceitos de que o laboratório do capítulo depende e que ele nunca define:** job, stage, task, action, lazy evaluation. O Spark UI tem abas chamadas Jobs e Stages, e o capítulo manda olhar para elas sem explicar o que elas contam. Anote como alvo dos próximos capítulos, e não como busca na internet.

R: Não busquei nada aqui, conforme o enunciado pede. Registro o estado da lacuna e onde ela já foi fechada.

No capítulo 2 a ausência é total. Busquei o texto inteiro: as palavras driver, task, core, partition, action, lazy e shuffle não aparecem uma vez sequer. "Jobs" e "Stages" aparecem só como nomes de aba, e "job" aparece só em "execute jobs for production data pipelines". O capítulo manda abrir duas abas cujo conteúdo ele não nomeia.

Essas cinco definições já foram fechadas no guia do capítulo 1, item 7 do Nível 5, a partir do cluster overview e do RDD programming guide. Em resumo: um **job** é uma computação paralela disparada por uma **action**. Cada job se divide em **stages**, e cada stage contém **tasks**, que são as unidades enviadas aos executors. Toda transformation é **lazy** e nada roda até uma action pedir resultado. A fronteira entre stages é o shuffle.

O que ainda falta, e que espero dos próximos capítulos, é a ligação entre isso e a UI: quantas tasks a aba Stages mostra por stage, e por que uma partition vira uma task. O item 11 do Nível 3 é o experimento que fecha essa parte.

### Fontes consultadas

Todas acessadas em 2 de agosto de 2026. Todas são fonte primária: documentação oficial do projeto, página de download, diretório de release ou blog do próprio fornecedor.

Apache Spark, documentação 4.2.0 e distribuição:

- [Downloads, com a versão atual e a nota sobre Scala](https://spark.apache.org/downloads.html)
- [Overview, com a matriz de Java, Scala, Python e R](https://spark.apache.org/docs/latest/index.html)
- [PySpark Installation, com o piso de Python e os caminhos de pip e conda](https://spark.apache.org/docs/latest/api/python/getting_started/install.html)
- [Web UI, com as nove abas e as sete seções da Environment](https://spark.apache.org/docs/latest/web-ui.html)
- [Monitoring and Instrumentation, com o History Server e o event logging](https://spark.apache.org/docs/latest/monitoring.html)
- [Configuration, com precedência e propriedades de deploy contra runtime](https://spark.apache.org/docs/latest/configuration.html)
- [Diretório de release 4.2.0 no mirror da Apache, com os nomes dos artefatos](https://dlcdn.apache.org/spark/spark-4.2.0/)

GitHub:

- [Improving Git protocol security on GitHub, com as datas dos brownouts e da remoção](https://github.blog/security/application-security/improving-git-protocol-security-github/)

Databricks:

- [Get started with Databricks Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition)
- [Databricks Runtime release notes versions and compatibility](https://docs.databricks.com/aws/en/release-notes/runtime/)
- [What is Delta Lake in Databricks](https://docs.databricks.com/aws/en/delta/)
- [Develop code in Databricks notebooks, com os magic commands](https://docs.databricks.com/aws/en/notebooks/notebooks-code)
- [Run Databricks notebooks, com o shift+enter](https://docs.databricks.com/aws/en/notebooks/run-notebook)
- URLs antigas testadas quanto a redirecionamento: `docs.databricks.com/user-guide/index.html` e `docs.databricks.com/user-guide/notebooks/index.html`

Interface e números de produto mudam rápido, e a aposentadoria da Community Edition em 2025 é a prova disso neste capítulo. Preciso reconferir estes dados antes de usá-los em prova ou em decisão técnica.

---

## Nível 6 — Cross-book e cross-chapter

**1.** **A questão `spark` contra `sc`, resolvida.** No capítulo 1 de *Learning Spark* 2ª edição, Damji et al. escrevem que a `SparkSession` do shell é acessível "via a global variable called `spark` or `sc`". Este capítulo mostra `:type spark` devolvendo `org.apache.spark.sql.SparkSession`, e os banners das Figures 2-2 e 2-3 reportam `sc` e `spark` como duas coisas separadas. Escreva a afirmação correta, e anote o que cada variável é.

R: A afirmação de Damji está errada, e o erro é o "or". As duas variáveis não são dois nomes do mesmo objeto.

A afirmação correta: o shell inicializa duas variáveis globais distintas. `spark` é uma instância de `org.apache.spark.sql.SparkSession`, o ponto de entrada às APIs estruturadas. `sc` é uma instância de `org.apache.spark.SparkContext`, a conexão de baixo nível com o cluster, que hospeda as APIs de RDD. Classes diferentes, pacotes diferentes, papéis diferentes.

A relação entre elas é de contenção, não de sinonímia: a SparkSession guarda um SparkContext internamente, acessível por `spark.sparkContext`, conforme fechei no item 8 do Nível 5 do guia do capítulo 1. Ou seja, `sc` é um atalho para algo que `spark` também alcança, e não um apelido de `spark`.

O próprio Damji acerta no capítulo 2. Minhas anotações daquele capítulo dizem: no shell a SparkSession já vem criada como `spark`, e no `spark-shell` também o SparkContext como `sc`. A imprecisão é do fraseado do capítulo 1, não da posição do livro.

**2.** **História do ponto de entrada.** Os dois livros datam a `SparkSession` no Spark 2.0. Luu diz que ela provê "a single entry point". Damji et al. listam os cinco contextos que ela subsumiu. Combine os dois numa nota só, e explique por que `sc` continua existindo no shell se a `SparkSession` subsumiu o `SparkContext`.

R: A nota combinada: a `SparkSession` foi introduzida no Spark 2.0 como ponto de entrada único às funcionalidades do Spark. Ela unificou cinco pontos de entrada que antes eram separados, segundo Damji: `SparkContext`, `SQLContext`, `HiveContext`, `SparkConf` e `StreamingContext`. Por ela se define parâmetros de runtime, se cria DataFrames e Datasets, se lê fontes de dados, se acessa metadados de catálogo e se emite queries SQL. Há uma SparkSession por JVM. Luu acrescenta as duas capacidades que o capítulo 2 destaca: APIs de leitura de dados em formatos texto e binário, e recuperar e definir configurações.

Sobre por que `sc` sobrevive, "subsumiu" é mais forte do que os fatos autorizam. A nota de release do Spark 2.0.0, que citei no item 8 do Nível 5 do guia do capítulo 1, diz que a SparkSession substitui `SQLContext` e `HiveContext` para as APIs de DataFrame e Dataset, e que os dois foram mantidos por compatibilidade. Ela não diz que o SparkContext foi substituído.

Duas razões concretas para `sc` continuar no shell. Primeira, o SparkContext não virou legado: ele é a conexão com o cluster e hospeda as APIs de RDD, que continuam existindo abaixo das APIs estruturadas. Segunda, compatibilidade com código anterior ao 2.0, que é abundante. Para código novo, o caminho é criar a SparkSession e chegar ao contexto por `spark.sparkContext` quando a API de RDD for necessária, em vez de instanciar um SparkContext solto.

**3.** **O que o capítulo 1 prometeu e o capítulo 2 entregou.** O capítulo 1 deste livro descreveu drivers, executors, tasks e cores de forma abstrata. Mapeie cada um dos quatro em algo que agora dá para *ver* no Spark UI. Qual dos quatro é o mais difícil de observar em local mode, e por quê?

R:

| Conceito do capítulo 1 | Onde ver no Spark UI |
|---|---|
| driver | A linha rotulada `driver` na aba Executors. A Runtime Information, na aba Environment, descreve a JVM dele |
| executor | As linhas da aba Executors, uma por executor, com memory, disk e CPU |
| task | A aba Stages, que conta as tasks de cada stage. A aba Executors mostra tasks ativas, completas e falhas por executor |
| core | A coluna de cores na aba Executors, e o valor de `spark.master` nas Spark Properties |

O mais difícil de observar em local mode é o **executor**, porque em local mode não existe um. Driver e executor são o mesmo processo, e a UI rotula a linha única como `driver`. O conceito que o capítulo 1 apresenta como o slave da arquitetura é justamente o que não tem existência separada no laboratório do capítulo 2. É a mesma contradição que a legenda da Figure 2-15 expõe, tratada no item 5 do Nível 4.

O segundo mais difícil é a **task**, por outro motivo. Ela é observável, mas só depois de rodar alguma coisa. Num shell recém-aberto as abas Jobs e Stages estão vazias, e nenhum dos exemplos do capítulo 2 dispara uma computação Spark, porque todos operam sobre um array Scala local. O item 11 do Nível 3 existe para preencher isso.

**4.** **Local mode contra o diagrama de arquitetura.** Compare a Figure 1-1 deste livro, com driver, cluster manager, workers e executors, contra o que a Figure 2-15 de fato mostra para um shell local. Redesenhe a Figure 1-1 para local mode.

R: A Figure 1-1 mostra duas hierarquias em camadas, como registrei no item 1 do Nível 4 do guia do capítulo 1: o cluster manager como master de vários workers, e o driver como master de vários executors distribuídos por essas máquinas. A Figure 2-15 mostra uma linha só, rotulada `driver`. Em local mode as duas hierarquias colapsam num único processo.

O redesenho:

```
UMA MAQUINA, UM PROCESSO JVM
    driver + executor no mesmo processo
        |-- thread 1   (core logico 1)   -> task
        |-- thread 2   (core logico 2)   -> task
        |-- ...
        `-- thread N   (core logico N)   -> task

    cluster manager: nenhum processo separado.
    spark.master = local[*], onde * = N cores logicos da maquina.
    Spark UI em http://localhost:4040, dentro do mesmo processo.
```

Três diferenças estruturais em relação à Figure 1-1. Não há negociação de recursos, porque não há cluster manager a quem pedir, então a etapa que o capítulo 1 descreve como primeira responsabilidade do driver não acontece. Não há rede entre driver e executor, e portanto não há shuffle entre máquinas, só troca entre threads. E o paralelismo vem de threads dentro de uma JVM, não de processos em máquinas distintas, ainda que a relação de uma task por core se mantenha.

**5.** **Deployment modes.** Damji et al. fornecem uma tabela de cinco modos, incluindo Local. O capítulo 1 do Luu nunca menciona local mode, e este capítulo inteiro roda nele. Usando a linha Local do Damji, diga com precisão onde estão o driver e o executor no shell que você acabou de rodar, e confirme com o `spark.master` do item 8 do Nível 3.

R: Uso aqui minhas anotações da Tabela 1-1 do Damji, no arquivo `learning-spark-caps-01-02-anotacoes.md`, e não o livro em si. A linha Local diz: driver em uma única JVM, por exemplo um laptop. Executor na mesma JVM do driver. Cluster manager no mesmo host.

Aplicado ao shell: o processo iniciado por `./bin/spark-shell` é o driver, e é também o executor. Não há segundo processo, não há segunda máquina e não há cluster manager separado. O Spark UI da porta 4040 roda dentro desse mesmo processo, e é por isso que ele morre junto com o shell.

A confirmação vem de `spark.master = local[*]`. O prefixo `local` é a declaração de que não existe cluster manager externo. O `[*]` define o grau de paralelismo dentro da JVM como o número de cores lógicos da máquina. Registro que a confirmação ainda é expectativa: preciso rodar o item 8 do Nível 3 na minha máquina para ler o valor real.

A lacuna do Luu é notável. O capítulo 1 dele descreve a arquitetura distribuída, o capítulo 2 roda inteiro em local mode, e o termo nunca aparece em nenhum dos dois. Damji, ao contrário, abre o capítulo 2 dizendo que usa local mode e explicando que ele não serve para trabalho em escala.

**6.** **Começar, comparado.** Os dois livros dedicam um capítulo inicial ao setup. Liste o que cada um cobre e o outro omite, e decida qual você entregaria a um colega no primeiro dia dele.

R: Comparo com base nas minhas anotações do capítulo 2 do Damji, e não com o livro na mão.

**Só no Luu:** o Databricks com passo a passo completo, do cadastro ao cluster, à pasta, ao notebook e ao Publish. As abas Environment e Executors do Spark UI, com a Table 2-3 detalhando as seções. Os comandos `:` do shell Scala, com `:help`, `:type`, `:load` e os outros. O setup do código-fonte do Spark para estudo. E uma introdução mínima a Scala, com array, função, `filter` e function chaining.

**Só no Damji:** a instalação por `pip install pyspark`, que o Luu ignora. Os quatro shells, incluindo `spark-sql` e `sparkR`, contra os dois do Luu. O `spark-submit`, que o Luu anuncia e nunca mostra. A leitura de um arquivo de dados real, com `spark.read.text` e `count`. A primeira aplicação standalone, o exemplo dos M&Ms. Os conceitos por trás das abas: transformations contra actions, lazy evaluation, narrow contra wide, e o DAG do Spark UI. A tabela dos cinco deployment modes. E o próprio termo local mode.

**A quem eu entregaria o Damji.** Ele leva o colega a processar dados de verdade e a rodar uma aplicação fora do shell no primeiro dia, e explica o que as abas do Spark UI contam. O capítulo do Luu ensina a subir um shell e a manipular um array de quatro inteiros, sem tocar em nenhum dado, e adia todos os conceitos. A exceção é o colega que vai trabalhar dentro do Databricks: aí o passo a passo do Luu tem valor, com a ressalva de que ele está obsoleto desde a aposentadoria da Community Edition, registrada no item 5 do Nível 5.

---

## Termos-chave para definir antes de seguir adiante

Escreva uma definição de uma linha para cada, com suas próprias palavras, sem consultar:

REPL · Spark shell · `spark-shell` · `pyspark` · variável `spark` · `sc` · SparkSession · SparkContext · Spark UI · porta 4040 · aba Jobs · aba Stages · aba Executors · aba Environment · Spark property · system property · resource profile · classpath entry · local mode · `local[*]` · driver · executor · core · task · notebook · cell · magic command · workspace · Databricks Runtime · edição community/free · tab completion · function chaining · função anônima · variável imutável (`val`) · history server

Qualquer termo que você não conseguir definir é alvo de releitura, não item de Nível 5.

### Minhas definições

Treze dos trinta e cinco termos o capítulo usa sem definir, ou nem menciona. Marquei esses casos em itálico, porque neles a definição vem de fora do texto.

**REPL** — Read-eval-print loop. Ambiente interativo que lê a entrada, avalia, imprime o resultado e repete, dando feedback de sintaxe imediato e dispensando o passo de compilação.

**Spark shell** — Ambiente interativo para aprender Spark e analisar dados, disponível em Scala e em Python. É uma aplicação Spark escrita em Scala, e não apenas uma aplicação Scala.

**`spark-shell`** — O executável em `bin` que sobe o Spark shell em Scala. Saída por `:quit` ou `:q`.

**`pyspark`** — O executável em `bin` que sobe o Spark shell em Python. Saída por ctrl-d. *Hoje também é o nome do pacote instalável por `pip`, o que o capítulo não menciona.*

**variável `spark`** — Variável inicializada pelo shell que aponta para uma instância de SparkSession. Não é palavra-chave: `:type spark` devolve a classe.

**`sc`** — *Fora do capítulo como conceito.* Aparece só na linha do banner "The SparkContext Web UI is available at...", sem uma frase de explicação. É a variável que guarda o SparkContext do shell.

**SparkSession** — Classe introduzida no Spark 2.0 como ponto de entrada único às funcionalidades do Spark. Tem APIs para ler dados estruturados e não estruturados, e para recuperar e definir configurações.

**SparkContext** — *Nomeado e não explicado.* É a conexão de baixo nível com o cluster e hospeda as APIs de RDD. A SparkSession guarda um, acessível por `spark.sparkContext`.

**Spark UI** — Aplicação web para monitorar e depurar uma aplicação Spark, com informação de runtime, consumo de recursos e métricas de performance. Só existe enquanto a aplicação roda.

**porta 4040** — A porta padrão do Spark UI. A URL completa aparece no banner de startup do shell.

**aba Jobs** — *Só o nome.* O capítulo a lista na barra de navegação e adia a descrição para capítulos posteriores. Pelo glossário do Spark, um job é uma computação paralela disparada por uma action.

**aba Stages** — *Só o nome.* Mesmo caso da anterior. Um stage é uma divisão de um job, delimitada por shuffle, e contém as tasks.

**aba Executors** — Aba com resumo e detalhamento por executor, cobrindo capacidade e uso de memory, disk e CPU. A seção Summary agrega todos os executors.

**aba Environment** — Aba com informação estática do ambiente da aplicação, dividida em Runtime Information, Spark Properties, Resource Profiles, Hadoop Properties, System Properties e Classpath Entries.

**Spark property** — Propriedade de configuração da aplicação Spark. As básicas identificam a aplicação, como id e nome. As avançadas ligam, desligam ou ajustam features.

**system property** — Propriedade de nível de sistema operacional e de Java, não específica do Spark.

**resource profile** — Conjunto declarado de recursos de uma aplicação. O capítulo o descreve como número de CPUs e quantidade de memória no cluster. *A documentação atual é mais precisa e inclui recursos aceleradores.*

**classpath entry** — Caminho de classe ou jar file carregado pela aplicação. A aba Environment lista todos.

**local mode** — *Ausente do capítulo.* Modo em que driver e executor rodam no mesmo processo JVM, numa máquina só, com paralelismo por threads. É o modo em que todo o laboratório do capítulo roda.

**`local[*]`** — *Ausente do capítulo.* Valor de `spark.master` que declara modo local com tantas threads quantos forem os cores lógicos da máquina.

**driver** — *A palavra não aparece no capítulo.* Do capítulo 1: o coordenador da aplicação, que negocia recursos, distribui tasks e coleta resultados. No Spark UI ele é a linha rotulada `driver` na aba Executors.

**executor** — *Usado sem definição.* A aba Executors trata "os executors que sustentam a aplicação" como coisa conhecida. Do capítulo 1: processo JVM dedicado a uma aplicação, que executa a lógica na forma de tasks.

**core** — *A palavra não aparece no capítulo,* que fala em CPU. Do capítulo 1: cada task roda em um CPU core separado, e daí vem o paralelismo.

**task** — *A palavra não aparece no capítulo.* Do glossário do Spark: a unidade de trabalho enviada a um executor. Uma partition vira uma task por stage.

**notebook** — Ambiente computacional interativo que combina código executável, documentação em Markdown ou HTML e visualização de resultados.

**cell** — A unidade de um notebook. Contém um bloco de código para executar ou markup de documentação, e é também a unidade de execução.

**magic command** — Prefixo que muda o modo de interpretação de uma cell. O capítulo mostra só `%md`, para documentação. *A documentação atual acrescenta `%python`, `%r`, `%scala` e `%sql`.*

**workspace** — O espaço hierárquico de organização de notebooks no Databricks. O capítulo o compara ao file system de um computador.

**Databricks Runtime** — A versão de ambiente escolhida na criação de um cluster Databricks, atrelada a uma imagem específica. Cada versão empacota uma versão do Apache Spark. *O capítulo não diz isso último, e a numeração atual está no item 7 do Nível 5.*

**edição community/free** — A camada gratuita do Databricks. No capítulo: um cluster de nó único com 15 GB na AWS, que desliga após duas horas ocioso. *Hoje é a Free Edition, com compute serverless, e a Community Edition foi aposentada em 2025.*

**tab completion** — Recurso que completa palavras parcialmente digitadas e lista as member variables e funções de um objeto. O capítulo o chama de code completion e o compara ao de Eclipse e IntelliJ.

**function chaining** — Encadear chamadas de função numa expressão só, como em `filter(...).foreach(...)`. O capítulo o apresenta como prática comum em Scala para deixar o código conciso.

**função anônima** — *O capítulo usa e não nomeia.* Função sem nome passada como argumento, como `age => isOddAge(age)` no exemplo do `filter`.

**variável imutável (`val`)** — *Nomeada e não explicada.* O capítulo diz que `ages` é atribuída a uma "immutable variable" e nunca explica a diferença entre `val` e `var`. É a variável cujo vínculo não pode ser reatribuído depois da criação.

**history server** — *Ausente do capítulo.* Serviço que reconstrói a UI de uma aplicação já encerrada a partir de event logs persistidos, servido na porta 18080 por padrão. Exige `spark.eventLog.enabled` ligado antes da execução.
