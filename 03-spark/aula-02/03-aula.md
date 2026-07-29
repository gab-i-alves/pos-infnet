---
title: "Aula 02 de Spark - Aula ao vivo"
aula: "Aula 02 - Transformação e persistência SQL"
data:
tags:
  - pos-infnet
  - aula-02
  - aula
  - spark
  - perguntas
  - spark-sql
  - catalyst
---

# Aula 02 · Aula ao vivo

Documento da terceira etapa: o que levar para a aula e o que trazer dela. As três primeiras partes são preparação, feitas antes de entrar. A quarta é o banco bruto de perguntas que nasceu do [aprofundamento](02-aprofundamento.md), agrupado pelas cinco partes de origem. A quinta se preenche durante e depois da aula.

Regra que vale para todas as perguntas da Parte 2: **toda premissa atribuída à bibliografia está registrada em [01-pre-aula.md](01-pre-aula.md)**, e o texto da resposta cita o código da dúvida (`R1-6`, `R2-7`, `R4-17`, `R5-16`) ou o número da divergência. Na aula 01, três perguntas foram escritas com premissa falsa e tiveram de ser reescritas antes de entrar na sala. O custo de conferir é de dois minutos; o custo de errar em voz alta é de uma aula.

## Parte 1 - Ancoragem: onde a teoria da Aula 2 vira decisão

Esta aula ensina `read`, `schema`, `write`, tabela e view. Vista de perto, ela é uma aula de contrato e de custo: cada linha de `DataFrameReader` decide quanto se paga em I/O, e cada `StructField` decide se um erro aparece hoje ou dentro de três semanas, num relatório. Seis pontos que valem ser carregados para a sala.

**O plano é a única ferramenta de diagnóstico que existe nesta camada.** Toda pergunta de custo desta aula (o filtro desceu, a coluna foi podada, quantos shuffles existem) se responde lendo `explain("formatted")` depois de rodar a ação, nunca antes. Cinco campos carregam quase toda a informação: `PartitionFilters` diz que diretórios foram eliminados pelo nome, sem abrir arquivo; `PushedFilters` diz que filtros o leitor aceitou, e vazio com filtro no `Filter` acima é I/O pago para jogar fora; `ReadSchema` é a prova da poda de coluna; `Batched` é o leitor vetorizado em uso; e a ausência do `*(n)` marca fronteira de estágio ou queda para interpretado. A regra de bolso mais barata é literalmente o avaliador de custo do motor: contar `Exchange` é medir custo, porque o `CostEvaluator` padrão do AQE coleta os nós `ShuffleExchangeLike` e devolve a quantidade. E um detalhe fecha o ciclo: `isFinalPlan=false` quer dizer que você está lendo um plano especulativo, e os cabeçalhos `== Final Plan ==` e `== Initial Plan ==` só aparecem quando o AQE de fato mudou algo.

**Schema é declaração, não validação, e confundir as duas coisas produz resultado errado em silêncio.** Declarar `nullable = false` na leitura de arquivo não impõe nada: o Spark troca a sua promessa por `true` antes de ler a primeira linha, porque `DataSource.resolveRelation` tem `forceNullable = true` como default de parâmetro e chama `asNullable` sobre o schema inteiro, campos aninhados incluídos. O motivo está escrito na chave equivalente do caminho de streaming: sem isso "the schema might not be compatible with actual data, which leads to corruptions". O caso perigoso é o oposto, quando o `false` sobrevive e mente, porque o Catalyst usa nulabilidade para **eliminar** verificação: `isnotnull(campo)` vira constante verdadeira e o filtro desaparece do plano. O modo de falha não é exceção, é número errado. Logo, a defesa é código seu (contagem de nulo com limiar) ou uma camada que imponha `NOT NULL` na escrita.

**Layout de arquivo é decisão de custo cobrada na leitura, meses depois de você escrever.** Parquet guarda pedaços de coluna dentro de grupos de linhas, no mesmo arquivo, com o rodapé no fim trazendo schema, offsets e estatística de mínimo e máximo. É esse layout, e só ele, que dá poda de coluna por offset e descarte de bloco por estatística. Daí saem três consequências que nenhum livro liga: arquivo pequeno destrói o mecanismo, porque um Parquet de 20 KB tem um grupo de linhas só e a poda por estatística vira poda por arquivo; `openCostInBytes` cobra 4 MB fictícios por arquivo aberto, então dez mil arquivos de 20 KB são 200 MB reais e cerca de 40 GB de custo modelado, o que dá umas 313 tasks lendo 640 KB cada; e ordenar o dado pela coluna de filtro muda o pushdown de zero para quase perfeito, porque o descarte é por intervalo de bloco. Divisibilidade entra na mesma conta com uma distinção que resolve a confusão inteira: codec não divisível estraga paralelismo em CSV, JSON e texto, e não estraga em Parquet e ORC, onde a compressão é por página e o rodapé dá o offset. E `multiLine=true` produz o mesmo estrago do gzip por outra causa, porque o parser precisa do arquivo inteiro para saber onde um registro acaba.

**A fronteira JVM contra Python é o lugar onde a tese da aula se quebra por dentro.** Enquanto você fica em expressão de coluna, nenhuma linha de dado passa pelo interpretador: a API constrói uma árvore, manda a árvore, e Catalyst, Tungsten e `UnsafeRow` fazem o resto. Uma UDF Python reintroduz, dentro da API estruturada, exatamente a opacidade que justifica abandonar o RDD: o Catalyst não inspeciona o corpo da função, o filtro não desce para a fonte, a poda não atravessa o nó e o codegen quebra nos dois lados dele. Duas correções de 2026 importam. Arrow ligado por padrão no 4.2 mata a serialização **por linha** e não a chamada por linha, então `@pandas_udf` continua acima de `@udf`, com a distância menor. E a memória do worker Python vive fora da heap, em `spark.executor.memoryOverhead`, cujo fator default é 10%, exceto **0,40 em job não-JVM no Kubernetes**, o que é a admissão do projeto de que 10% não serve para PySpark. O sintoma é container morto pelo orquestrador, não `OutOfMemoryError` da JVM, e subir `spark.executor.memory` piora.

**A escolha de formato de tabela é decisão de roadmap, não de feature.** Parquet num diretório é formato de arquivo, e a lista do que ele não resolve é longa: `overwrite` não é atômico (apaga e escreve, com janela no meio), leitor concorrente vê estado parcial, não há transação de duas operações, não há `UPDATE`, `DELETE` nem `MERGE`, não há versão, não há compactação, e `_SUCCESS` é convenção, não trava. Delta, Iceberg e Hudi resolvem isso com a mesma ideia: a tabela deixa de ser "os arquivos deste diretório" e passa a ser "a lista de arquivos que esta versão do metadado nomeia". O preço é o que a literatura de fornecedor não escreve: manutenção passa a ser um job permanente (compactação, expiração de snapshot, arquivo órfão), armazenamento cresce com a retenção, você passa a depender de um catálogo, leitor ingênuo apontado para o diretório lê linha apagada sem erro, e a sua versão de Spark passa a ser limitada pelo formato. Em 28/07/2026 isso é literal: não existe runtime de Iceberg, Delta nem Hudi para o Spark 4.2.0.

**O contrato de erro mudou de granularidade, e é a defasagem que quebra código que hoje roda.** Com ANSI desligado, que é o mundo dos três livros, o default era tolerante e você optava por rigor no arquivo, com `mode=failFast`. Com ANSI ligado por padrão desde o 4.0, o default é rigoroso e você opta por tolerância **expressão por expressão**, com `try_cast`, `try_add`, `try_divide`, `try_element_at`. Junte a isso a distinção que quase todo material funde: são duas camadas independentes. A opção `mode` governa o parser das fontes de texto, e `spark.sql.ansi.enabled` governa avaliação de expressão. Desligar ANSI não faz CSV malformado voltar a virar nulo silencioso, e ligar ANSI não faz `PERMISSIVE` levantar exceção.

---

## Parte 2 - As perguntas

Quinze perguntas, numeração corrida, três níveis. Cada uma foi escrita para ser dita em voz alta e cabe em menos de trinta segundos de fala.

### Nível 1 - Ancoragem

### Pergunta 1 - Cinco propriedades ou três?

> "Os dois livros descrevem a mesma classe e contam peças diferentes: o Luu lista **cinco** propriedades do RDD, com esquema de particionamento e localização marcados como opcionais, e o Damji lista **três**, com a assinatura `Partition => Iterator[T]`. Minha leitura é que o Luu está citando o scaladoc do projeto e que o Damji dobrou a localidade dentro das partições, então a única peça que ele realmente omite é o particionador. Está correto? E qual contagem o senhor cobra numa prova?"

**Por que é boa:** transforma uma divergência checável em pergunta de currículo e já traz a reconciliação em vez do confronto. Custa dois minutos e conserta o modelo mental da turma inteira.

**Resposta que você já deve saber:** a divergência 2 do pré-aula é a mais fácil de arbitrar de todas, porque a definição está escrita em inglês no comentário de classe de `RDD.scala`: "Internally, each RDD is characterized by five main properties", com lista de partições, função de cômputo, lista de dependências, `Partitioner` opcional para RDD de chave e valor, e lista opcional de localizações preferidas. É a mesma contagem e o mesmo rótulo de opcionalidade que o Luu apresenta, com duas diferenças que valem saber: ele reordena as três primeiras peças (escreve dependências, partições, função, onde a fonte escreve partições, função, dependências) e diz que a função calcula "todas as linhas do dataset" onde a fonte diz "cada split", imprecisão registrada em `R1-7` e `R1-8`. Cinco é o número do projeto, e o Luu não inflou nada.

Três contagens se sustentam, e cada uma responde a uma pergunta diferente. **Cinco** é a definição, e é o número defensável quando se pergunta o que é um RDD. **Três** é a linhagem, e aí Luu e Damji concordam sem perceber, porque o próprio Luu diz que as três primeiras peças **são** a informação de linhagem, que serve a dois propósitos nesta ordem: determinar a ordem de execução e recuperar de falha. **Dois** é o que o compilador cobra: só `compute` e `getPartitions` são abstratos, `getDependencies` já vem resolvido pelo construtor, e `partitioner` e `getPreferredLocations` vêm com padrão neutro (`None` e sequência vazia).

O que o Luu deixou como rótulo tem consequência, e é a dúvida `R1-7`: as peças 4 e 5 aparecem com "(optional)" e nada mais. Sem `partitioner`, o Spark não sabe que o dado já está no lugar certo, então um `join` entre dois RDDs de mesmo número de partições ganha um `Exchange` que não precisava existir. Sem `getPreferredLocations`, todo agendamento começa em `ANY` e todo byte trafega pela rede. É o mesmo raciocínio que sustenta bucketing na camada estruturada: declarar o layout evita a troca. Registre também a dúvida `R1-6`: nenhum dos capítulos lidos até aqui diz que a recomputação é **por partição perdida**, que é justamente a granularidade que torna o mecanismo barato.

---

### Pergunta 2 - A caixa `Cost Model` que o código não implementa

> "O capítulo 3 do Damji se contradiz sobre onde o custo age: o texto põe o CBO e a escolha entre vários planos na fase 2, a lógica, e a Figura 3-4 põe a pilha de planos físicos e a caixa `Cost Model` depois do plano lógico otimizado, na fase 3. Fui olhar o código do 4.2 e a seleção da figura não existe: o `QueryExecution` chama `planner.plan(...).next()`, pega o primeiro candidato do iterador, com um `TODO` aberto dizendo que um dia vão escolher o melhor. O senhor conhece consulta em que o iterador devolva mais de um candidato e a ordem das estratégias mude o resultado?"

**Por que é boa:** ataca o ponto que a aula 01 deixou em dívida, traz a contradição interna do livro e uma verificação de três linhas de código. Força o professor a dizer onde o custo mora de fato, que é a única resposta útil.

**Resposta que você já deve saber:** a contradição é a dúvida `R2-7`, e a vizinha dela é a `R2-8`, em que o texto classifica pushdown de predicado e poda de projeção como otimização lógica e a Figura 3-5 os desenha entre dois planos **físicos**, no mesmo assunto e a duas páginas de distância. Quem está certo sobre a figura é a figura: ela reproduz o desenho do texto de 2015 do próprio projeto, que também põe a seleção por custo no planejamento físico, com a ressalva que o livro não copiou, de que naquela época "cost-based optimization is only used to select join algorithms". Ou seja, "modelo de custo" já queria dizer "limiar de broadcast".

Quem está errado é o texto. A fase 2 não constrói conjunto de candidatos e não ranqueia: é baseada em regra e determinística, com lotes rodando a ponto fixo até a árvore parar de mudar, com teto em `spark.sql.optimizer.maxIterations`, default 100. A única regra de custo do lote lógico é `CostBasedJoinReorder`, e ela vem desligada.

Em 2026 o custo age em três lugares, nenhum deles sendo a caixa da figura. Fase 2, `CostBasedJoinReorder`, desligado: `spark.sql.cbo.enabled` e `spark.sql.cbo.joinReorder.enabled` são `false` desde a 2.2.0, e ligar sem `ANALYZE TABLE` não faz nada útil, porque a estatística tem de estar no catálogo. Fase 3, dentro de `JoinSelection`, e ali o único número que decide é `spark.sql.autoBroadcastJoinThreshold`, 10 MB desde a 1.1.0; todo o resto é regra, não custo. E execução, no AQE, cujo avaliador de custo padrão é **contagem de shuffle**, literalmente `plan.collect { case s: ShuffleExchangeLike => s }.size`. Fecha com a dúvida `R2-17`: o capítulo descreve o Catalyst como jornada estática numa versão do Spark que já tinha AQE, e o AQE está ligado por padrão desde a 3.2.0, replanejando a cada estágio de shuffle concluído.

---

### Pergunta 3 - Inferir schema e ler rodapé são a mesma palavra para duas coisas

> "O Luu usa a palavra 'inferir' para CSV, JSON, Parquet e ORC, e diz que em Parquet e ORC o schema 'is automatically inferred'. Minha leitura é que essas são duas operações com custos separados por umas cinco ordens de grandeza: inferir CSV varre o dado duas vezes e dispara dois jobs, e ler Parquet toca o rodapé de **um** arquivo, num job de uma task só. O senhor confirma? E declarar schema elimina o job de rodapé do Parquet, ou só a varredura?"

**Por que é boa:** pega a divergência mais consequente da bibliografia para custo de leitura, já traz a hipótese com o número, e termina numa pergunta que o professor pode responder de cabeça ou prometer conferir.

**Resposta que você já deve saber:** é a divergência 4 do pré-aula, e a dúvida exata é a `R4-6`: chamar de inferência tanto varrer dado quanto ler um rodapé apaga a distinção de custo que interessa a quem paga a conta. Quem fecha o buraco é o Damji 3, com o mecanismo: declarar impede o Spark de criar um job separado só para ler uma porção grande do arquivo e determinar o schema. O que falta na frase dele é a fronteira, porque para Parquet e ORC não há varredura, há leitura de rodapé.

Os números, formato por formato. CSV com `inferSchema=true` é duas passadas completas e dois jobs: a documentação diz "It requires one extra pass over the data", e o código faz primeiro um `take(1)` para o cabeçalho e depois agrega os tipos sobre o conjunto todo. JSON **não tem** `inferSchema`, e o pré-aula marca isso na tabela de vocabulário e na dúvida `R4-15`, onde o Listing 3-14 passa `inferSchema` junto com `.schema(...)` num `read.json`, duas vezes inútil. Parquet com `mergeSchema=false`, que é o default desde a 1.5.0, toca o rodapé de um arquivo, escolhido como "a random data file" quando não há arquivo de resumo, e ainda assim dispara um job, porque o caminho passa por `mergeSchemasInParallel`, com comentário literal "Issues a Spark job to read Parquet/ORC schema in parallel". Com um arquivo na lista, é um job de uma task. Com `.schema(...)`, `inferSchema` nunca é chamado.

Sobre `samplingRatio`, a dúvida `R4-14` registra o conselho do Luu de baixar o valor para acelerar o carregamento de JSON grande, e o conselho é meia verdade. Em modo linha única a amostra é um filtro sobre a varredura, então **todo byte continua sendo lido**: economiza CPU de parsing, não I/O. Em `multiLine=true` a amostra é sobre arquivos inteiros, aí economiza I/O de verdade e o risco explode, porque com `0.001` e mil arquivos o schema sai de um arquivo. Três detalhes de implementação que ninguém escreve: valor acima de 0,99 desliga a amostragem por completo, valor zero ou negativo levanta erro, e a semente é fixa em 1, então repetir não muda a amostra.

---

### Pergunta 4 - Parquet guarda cada coluna num arquivo separado?

> "O Luu afirma que o Parquet guarda o dado de cada coluna num arquivo separado, e usa isso para explicar a poda de coluna. Fui à especificação e o `ColumnChunk` tem um campo `file_path`, com a nota de que, se não estiver setado, assume-se o mesmo arquivo do metadado, e a página do formato diz que o desenho 'allows splitting columns into multiple files'. Então a minha pergunta é de leitura: o livro errou o mecanismo, ou citou uma possibilidade do padrão como se fosse o comportamento? O senhor conhece escritor que produza Parquet multiarquivo por coluna na prática?"

**Por que é boa:** é uma correção factual entregue como pergunta de interpretação, o que evita o tom de disputa e ainda dá ao professor a chance de ser mais preciso que o livro. E a resposta abre o assunto de arquivo pequeno, que é onde está o dinheiro.

**Resposta que você já deve saber:** é a divergência 8 e a dúvida `R4-17`, classificada no pré-aula como verificável e falsa. O layout real, de fora para dentro: arquivo delimitado por `PAR1`; grupos de linhas como corte horizontal, com todas as colunas; pedaços de coluna, que são os valores de uma coluna dentro de um grupo de linhas, contíguos; páginas como unidade de codificação e compressão; e o rodapé no fim, com schema, lista de grupos de linhas, offset e tamanho de cada pedaço, e estatística. A ordem tem motivo declarado, "file metadata is written after the data to allow for single pass writing", e o protocolo de leitura também: "readers are expected to first read the file metadata to find all the column chunks they are interested in".

A concessão honesta fortalece a correção em vez de enfraquecê-la. A especificação **permite** a variante que o Luu descreve, e o Thrift tem o campo para isso. O que não existe é escritor mainstream que faça, e o Spark não faz. A frase do livro está errada como descrição do que existe no disco depois de um `df.write.parquet(...)`, e é essa descrição que decide tamanho de arquivo, layout de diretório e leitura seletiva.

O erro passa batido porque o **efeito** é o mesmo nas duas versões, e é aí que ele estraga o raciocínio: se cada coluna estivesse num arquivo próprio, "arquivo pequeno" não teria a mesma implicação sobre estatística, porque a unidade de poda não seria o grupo de linhas. Com o layout real, arquivo pequeno empilha quatro custos: custo fixo de abertura, que em armazenamento de objeto é uma sequência de requisições HTTP cuja latência não diminui porque o arquivo é pequeno; estatística inútil, com um grupo de linhas por arquivo; codificação pior, porque dicionário e codificação de repetição precisam de muitos valores da mesma coluna de uma vez; e rodapé proporcionalmente gigante, lido pelo driver antes de existir a primeira task. Para referência, o projeto Parquet recomenda grupo de linhas de 512 MB a 1 GB e página de 8 KB.

---

### Nível 2 - Profundidade

### Pergunta 5 - O livro imprime a prova contra a própria afirmação sobre `nullable`

> "O capítulo 3 monta o mesmo DataFrame de seis autores duas vezes, com `nullable=false` em cada campo. Em Python, com dado em memória, tudo sai `false`; em Scala, lendo o mesmo dado de um `blogs.json`, tudo sai `true`, e o texto afirma que as saídas não diferem. Achei o mecanismo: `resolveRelation` tem `forceNullable = true` como default de parâmetro e chama `asNullable` sobre o schema todo. Minha leitura é que não existe forma, no Spark aberto, de impor `nullable=false` na leitura de arquivo, e que o terceiro benefício de declarar schema, detectar erro cedo, não vale para nulo. Está correto?"

**Por que é boa:** o livro dá a evidência contra si mesmo na mesma página, e a pergunta traz o mecanismo em vez de só apontar a contradição. Fecha a saída fácil de "declare schema e fica tudo certo".

**Resposta que você já deve saber:** é a divergência 11 e a dúvida `R2-5`. O mecanismo é uma linha, `dataSchema = if (forceNullable) dataSchema.asNullable else dataSchema`, com o parâmetro em `true` por padrão e sem chave pública para o caminho de lote. O `asNullable` percorre o `StructType` inteiro, campos aninhados incluídos. Não é bug, é desenho, e o motivo aparece escrito na chave equivalente do caminho de streaming, `spark.sql.streaming.fileSource.schema.forceNullable`: sem isso "the schema might not be compatible with actual data, which leads to corruptions". Traduzido, o Spark relaxa a sua promessa porque não confia nela.

Por que o dado em memória mantém o `false`: `createDataFrame` não passa por `resolveRelation`, e em PySpark clássico ele **verifica** de verdade, com `verifySchema=True` por padrão, levantando `FIELD_NOT_NULLABLE_WITH_NAME` diante de um `None`. Duas ressalvas da própria documentação: a verificação não é efetiva para `pandas.DataFrame` com Arrow nem para `pyarrow.Table`, e não é suportada no Spark Connect. E a única saída `nullable = false` vinda de fonte externa em toda a bibliografia, que o pré-aula anota sem comentário do autor, é a tabela `film` do Sakila no Listing 3-20: não é o Spark validando, é o Spark **copiando** a restrição declarada no catálogo do MySQL.

O caso perigoso é o `false` que sobrevive e mente, porque o efeito é de otimização. O Catalyst usa nulabilidade para eliminar verificação, e `isnotnull(campo)` vira constante verdadeira: filtro que desapareceu não filtra. Muda também escolha de join e semântica de agregação, e o codegen pode omitir o teste de nulo por registro, que é justamente o ganho que se busca ao declarar `false`. O resultado de mentir não é exceção, é resultado silenciosamente errado, que é o pior modo de falha possível. A regra operacional: use `nullable=false` como documentação do contrato, escreva a verificação você mesma, ou empurre a restrição para uma camada que a imponha na escrita.

---

### Pergunta 6 - A coluna de registro corrompido precisa estar no schema

> "O Chadha passa `columnNameOfCorruptRecord` igual a `corrupt_record` sobre um schema declarado que não tem esse campo, e promete uma coluna para investigar erro. A documentação diz que, se o schema não tem o campo, o Spark descarta o registro corrompido durante o parsing, e o `FailureSafeParser` calcula o índice do campo e exclui a coluna do `actualSchema`. Duas perguntas juntas: a coluna precisa estar no schema declarado, sempre? E como o senhor mede taxa de rejeição, dado que consultar só essa coluna direto do arquivo é bloqueado por `QUERY_ONLY_CORRUPT_RECORD_COLUMN`?"

**Por que é boa:** é a lacuna que a bibliografia inteira tem, cada livro de um jeito, e é o que quebra primeiro em qualquer ingestão real. A segunda metade é operacional e provavelmente ninguém na sala tentou.

**Resposta que você já deve saber:** a divergência 7 resume o estado: três livros, três coberturas parciais, nenhuma utilizável em produção. O Luu dá dois pontos do espectro e nunca nomeia `PERMISSIVE`, o Damji 4 dá os três nomes numa célula de tabela e terceiriza a explicação à documentação (`R5-9`), e o Chadha nomeia um modo só, descreve a semântica errado, não diz que é o default e usa a opção numa receita que não funciona como impressa (`R3-6`, `R3-7`).

Sobre a regra: sim, sempre. O nome default vem de `spark.sql.columnNameOfCorruptRecord`, que é `_corrupt_record` desde a 1.2.0, e a opção por leitura sobrescreve a config de sessão. A documentação de JSON, CSV e XML repete a mesma frase: "To keep corrupt records, an user can set a string type field named `columnNameOfCorruptRecord` in an user-defined schema. If a schema does not have the field, it drops corrupt records during parsing". Duas restrições que nenhum dos três livros tem: o tipo precisa ser `STRING` nulável, senão `INVALID_CORRUPT_RECORD_TYPE`; e não dá para consultar só essa coluna direto do arquivo, por `UNSUPPORTED_FEATURE.QUERY_ONLY_CORRUPT_RECORD_COLUMN`, cuja mensagem manda cachear ou salvar o resultado parseado antes de repetir a consulta.

O desenho de produção tem quatro peças, e nenhuma é opção do leitor. Schema declarado com a coluna de corrompido dentro. Materializar uma vez, com `input_file_name()` e um timestamp anexados, porque é isso que transforma "temos 12 mil registros ruins" em "o arquivo X da origem Y está quebrado". Quarentena como tabela particionada, com o texto cru, o motivo e a **versão do contrato**, porque metade das rejeições na prática é o contrato que envelheceu, não o dado que apodreceu. E taxa de rejeição como métrica com limiar, contada na mesma passada com `count("*")` e `count("_corrupt_record")`. Quatro cuidados na métrica: meça taxa e não contagem, porque contagem sobe com o volume; trate o limiar como parte do contrato, porque fonte que sempre teve 0,3% de sujeira não deve derrubar o pipeline em 0,4%; guarde série temporal, porque a informação útil é a derivada, e salto de 0,1% para 2% costuma ser mudança de schema na origem; e fixe a projeção, porque a documentação de CSV avisa que a poda de coluna muda o que conta como corrompido, e dois jobs sobre o mesmo arquivo com projeções diferentes medem taxas diferentes. Uma última correção de vocabulário: `badRecordsPath` **não existe no Apache Spark**, é recurso de plataforma da Databricks, e a documentação dela mesma avisa que é não transacional.

---

### Pergunta 7 - Duas divergências entre documentação e código que ficaram sem arbitragem

> "Levantei duas contradições entre a documentação do 4.2 e o código da mesma tag, e não tive Spark instalado para arbitrar. Primeira: a documentação de CSV diz que linha com número errado de campos não é registro corrompido, e o `UnivocityParser` tem um `if (tokens.length != parsedSchema.length)` com comentário dizendo o contrário. Segunda: a documentação diz que `PERMISSIVE` sem a coluna de corrompido descarta o registro, e o `FailureSafeParser` usa `row.getOrElse(nullResult)`, que **emite** a linha. Nos dois casos minha aposta é no código. O senhor sabe qual vale?"

**Por que é boa:** é a pergunta mais honesta do conjunto, porque declara o que você não conseguiu verificar e diz onde está apostando. E as duas respostas mudam recomendação de produção, não só curiosidade.

**Resposta que você já deve saber:** se o código estiver certo no primeiro caso, `DROPMALFORMED` sobre CSV com uma vírgula extra **apaga a linha**, o que a documentação nega, e isso muda a recomendação de modo para ingestão de CSV de origem externa. No segundo caso, se o código estiver certo, o registro ruim **sai** no dado bom, como resultado parcial ou linha de nulos, sem marca nenhuma. Aí a regra de produção deixa de ser "ponha a coluna para capturar o texto" e passa a ser "ponha a coluna para conseguir **distinguir** o registro ruim", que é uma justificativa mais forte e menos intuitiva.

Há um terceiro fato ligado a isso, e ele corrige o livro por conta do tempo, não por erro. A dúvida `R4-16` registra o Luu dizendo que o default nulifica **todas** as colunas da linha, e o Listing 3-15 prova, com cinco linhas inteiramente nulas por causa de um `BooleanType` declarado sobre texto. Isso descreve o Spark 3.0. Hoje `PERMISSIVE` salva o que dá para salvar: o branch é `val partialResults = e.partialResults(); if (partialResults.nonEmpty) ...`, e em JSON quem governa é `spark.sql.json.enablePartialResults`, interna, default `true` desde a 3.4.0. O comportamento do livro mudou por esse motivo, e não por causa do modo ANSI, que é a próxima pergunta e é outra camada.

Vale registrar também o que `DROPMALFORMED` é, porque a bibliografia não diz: uma linha, `Iterator.empty`. Sem contador, sem log, sem coluna. É a opção que perde dado em silêncio, e mesmo quando o descarte é aceitável vale preferir `PERMISSIVE` mais filtro, porque assim você **conta** o que jogou fora. E `FAILFAST` levanta na ação, não na leitura, com `MALFORMED_RECORD_IN_PARSING`, coisa que o pré-aula já notou na saída do Luu, onde o erro aparece como exceção de task dentro de um stage.

---

### Pergunta 8 - O modo ANSI reescreve o capítulo de cast, e são duas camadas

> "A tese de cast e overflow que a bibliografia ensina é a de antes do modo ANSI: o Luu declara que o default é nulificar e oferece `failFast` como única alternativa, e nenhum dos três livros cita `spark.sql.ansi.enabled`. Desde o 4.0 a chave vem ligada, então cast inválido e overflow **falham**. Minha leitura é que `mode` e ANSI são camadas independentes, `mode` governando o parser das fontes de texto e ANSI governando avaliação de expressão, e que o `UnivocityParser` não referencia ANSI em nenhum ponto. Está correto? E em pipeline legado que sobe para 4.x, o senhor liga ANSI e conserta as expressões, ou desliga a config e migra aos poucos?"

**Por que é boa:** é a defasagem que pode quebrar código que hoje roda, e a segunda metade é decisão de gestão, não de sintaxe. A hipótese sobre as duas camadas é falseável em uma frase.

**Resposta que você já deve saber:** a tese "comportamento de cast e overflow antes do modo ANSI" está registrada como confirmada no Luu 3.2, e a dúvida `R5-20` registra que o Damji afirma conformidade com ANSI SQL:2003 duas vezes, com grafias diferentes, sem nunca mencionar `spark.sql.ansi.enabled`, que é outra coisa e é a que muda comportamento. O ticket é o SPARK-44444, e o guia de migração é literal: "Since Spark 4.0, `spark.sql.ansi.enabled` is on by default". A chave existe desde a 3.0.0; o que mudou no 4.0 foi o default, e no código ele é calculado a partir da variável de ambiente `SPARK_ANSI_SQL_MODE`.

O que passou a falhar: `2147483647 + 1` dava a volta e virou `ARITHMETIC_OVERFLOW`; `CAST('a' AS INT)` dava `null` e virou `CAST_INVALID_INPUT`; divisão por zero virou `DIVIDE_BY_ZERO`; e `element_at` com índice inválido, `to_date` com string inválida, `parse_url` com URL inválida e `make_date` inválido deixaram todos de devolver nulo. Cada um tem escape na família `try_*`, e a mensagem de erro ensina o escape dentro dela mesma. Um detalhe que morde quem herdou pipeline de Spark 3: o comportamento antigo **não era uniforme**, `decimal` dava nulo e os outros numéricos davam número negativo, então há dois padrões de sujeira convivendo no mesmo dado histórico.

Sobre as duas camadas, a hipótese se sustenta e a evidência é uma ausência: nem em `UnivocityParser`, que é o parser de CSV, nem em `JacksonParser`, que é o de JSON, há uma única referência a ANSI, e a falha de conversão vira `BadRecordException`, que o `FailureSafeParser` resolve pelo `mode`. É exatamente isso que o Listing 3-15 do Luu embaralha, porque declara `BooleanType` sobre texto num **JSON**, que é falha de parsing, e apresenta `failFast` como alternativa ao default de nulificar, que é vocabulário de cast. As duas coisas pareciam uma só em 2021 porque as duas devolviam nulo.

Sobre a decisão, o argumento contra desligar é que o ANSI trocou erro silencioso por falha visível, e desligar é escolher voltar ao erro silencioso, com uma chave global que protege o código velho e envenena o novo. O caminho cirúrgico é rodar com ANSI ligado em ambiente de teste, coletar as condições de erro e trocar por `try_*` só onde a tolerância seja decisão de negócio, tratando o resto como bug encontrado. Duas notas de alcance: no 4.1 o ANSI passou a valer também para a pandas API sobre Spark, e o valor de ANSI passou a ser persistido na criação de view, para a view não mudar de semântica conforme a sessão que a consulta, o que resolve o bug de "a mesma view dá resultado diferente para dois times".

---

### Pergunta 9 - As duas linhas de `DataFrameWriter` que o livro imprime e o Spark recusa

> "O capítulo 4 imprime como padrão recomendado `format(...).option(...).bucketBy(...).partitionBy(...).save(path)` e `format(...).option(...).sortBy(...).saveAsTable(table)`. Nenhum dos dois roda: a documentação diz que 'bucketing and sorting are applicable only to persistent tables', e `sortBy` sem `bucketBy` também recusa. Trocar `save(path)` por `saveAsTable(nome)` faz a primeira funcionar, então é erro de uma palavra numa assinatura de referência. Minha pergunta de verdade é outra: bucketing entra em algum pipeline que o senhor mantém hoje?"

**Por que é boa:** entrega a correção em uma frase e usa o resto do tempo na pergunta que importa, que é se a técnica sobreviveu. Assinatura de referência errada é o pior tipo de erro de livro, porque é justamente o que se copia.

**Resposta que você já deve saber:** é a dúvida `R5-16`, que o pré-aula chama de achado mais forte da leitura por esse motivo exato. A regra não é arbitrária. `partitionBy` produz layout **autodescritivo**, com `chave=valor` no nome do diretório, e qualquer leitor que conheça a convenção reconstrói a informação sem consultar nada. `bucketBy` produz layout **mudo**, com arquivos `part-00000` a `part-00041` e nada dizendo que são o resultado de `hash(name) mod 42`. O número de buckets e as colunas são um contrato, e contrato precisa de lugar para morar, que é o metadado da tabela no catálogo. Sem tabela, você pagaria o custo de bucketizar e receberia zero, então `save(path)` recusa em vez de aceitar em silêncio. O mesmo vale para `sortBy`, e a DDL mostra por quê: a cláusula é `CLUSTERED BY (col) [SORTED BY (col)] INTO n BUCKETS`, com o `SORTED BY` **dentro** do `CLUSTERED BY`, e a API espelha a DDL.

Para que serve de verdade: a definição da documentação de DDL é a melhor, "bucketing is an optimization technique that uses buckets and bucketing columns to determine data partitioning and **avoid data shuffle**". Duas tabelas bucketizadas pela mesma coluna com o mesmo `n` garantem que qualquer chave está no bucket de mesmo número nas duas, então o join lê os pares diretamente e o `Exchange` desaparece do plano. Isso fecha o vocabulário que a aula 01 deixou aberto, e a generalização moderna tem nome: Storage Partition Join, cuja documentação afirma que "if Storage Partition Join is performed, the query plan will not contain Exchange nodes prior to the join".

Por que quase ninguém usa: exige tabela em catálogo, e a maior parte do mundo real é `spark.read.parquet("s3://...")`; o `n` é fixo na criação e o volume não é, então a tabela que tinha 200 arquivos de 250 MB vira 200 arquivos de 2,5 GB e consertar significa reescrever tudo; multiplica arquivos, porque cada task de escrita pode produzir um arquivo por bucket, o que exige um `repartition` antes do `write`, ou seja, pagar shuffle na escrita para economizar na leitura; o AQE resolveu o caso mais comum por outro caminho, convertendo sort-merge em broadcast em tempo de execução; e o ecossistema foi para clustering e compactação nos formatos de tabela. Onde ainda vale: warehouse estável, tabela fato que participa **sempre** do mesmo join pela mesma chave, volume previsível, e leitura muito mais frequente que escrita.

---

### Pergunta 10 - `spark.sql.warehouse.dir`, catálogo e metastore

> "O capítulo 4 diz que o Spark usa por default o metastore do Hive em `/user/hive/warehouse`, mudável por `spark.sql.warehouse.dir`. Minha leitura é que `/user/hive/warehouse` é o default **do Hive**, e que o default do Spark é um diretório `spark-warehouse` criado onde a aplicação foi iniciada, o que explica a pasta que aparece do nada no repositório de quem está aprendendo. E a pergunta que eu não fecho na leitura: catálogo e metastore são a mesma coisa vista de dois lados, ou camadas distintas?"

**Por que é boa:** é uma correção pequena com consequência diária, porque todo mundo já viu o `spark-warehouse` aparecer, e a segunda metade pede a distinção arquitetural que o livro empurra para o capítulo 12.

**Resposta que você já deve saber:** a afirmação do livro está registrada no registro de leitura do Damji 4, na seção sobre onde o metadado mora, e a pergunta sobre catálogo contra metastore está registrada literalmente na entrada de vocabulário de **`Catalog`**, com essas palavras. A documentação do Spark diz que se usa `spark.sql.warehouse.dir` para especificar a localização default de banco no warehouse, com default `spark-warehouse` no diretório corrente de onde a aplicação foi iniciada, e nota que `hive.metastore.warehouse.dir` do `hive-site.xml` está depreciada desde o Spark 2.0.0.

Sobre a distinção, a resposta curta é **catálogo é API, metastore é armazenamento**, e são três camadas, não duas: a API de catálogo (`spark.catalog.listDatabases()`, `SHOW TABLES`, `DESCRIBE`), a implementação que atende a chamada (em memória, Hive, ou um plugin) e o metastore onde o metadado persiste de verdade, que pode ser um banco relacional, um serviço REST ou um arquivo. O catálogo é o balcão, o metastore é o arquivo atrás do balcão: a mesma pergunta no balcão dá resposta diferente se trocarem o arquivo, e nada garante que o metadado sobreviva ao fim da aplicação, porque isso depende da implementação e não da API.

O que o livro empurra para frente é o `CatalogPlugin`, desde a 3.0.0, registrado por `spark.sql.catalog.<nome>=<classe>` mais pares com o mesmo prefixo, passados como mapa de opções no `initialize`. Três consequências. Catálogo deixou de ser singular, e o identificador de tabela passou a ter três níveis, `catalogo.namespace.tabela`, com leitura de dois catálogos na mesma consulta. É por essa API que Delta e Iceberg entram como cidadãos de primeira classe, porque sem ela um formato de tabela sabia ler e escrever arquivo mas não sabia criar, dropar ou versionar tabela pelo dialeto SQL do Spark. E a aproximação "catálogo é o metastore", defensável antes da 3.0, ficou errada.

Fecha com a parte que o Damji serve bem e nunca demonstra, porque em nenhuma das 39 páginas alguém dropa uma tabela: `DROP TABLE` apaga metadado **e** dado quando a tabela é gerenciada, e só o metadado quando é externa, e o que torna a tabela externa é informar a localização (`LOCATION` em SQL, `option("path", ...)` antes de `saveAsTable`). A prática defensiva em data lake é tornar tudo externo com `LOCATION` explícito, justamente para que dropar tabela seja operação de metadado e nunca exclusão recursiva de dado, sobretudo porque o `PURGE` do Hive pula a lixeira. E vale saber que o Damji escreve **unmanaged** em todas as ocorrências e nunca **external**, que é o termo do dialeto SQL do Spark e o que aparece em qualquer outro material.

---

### Pergunta 11 - UDF não aparece em nenhuma das 141 páginas

> "Cinco capítulos sobre APIs estruturadas e persistência, e não há uma única UDF nas 141 páginas. Isso me chamou atenção porque a lista de quatro problemas que o capítulo 3 empilha contra o RDD (função opaca, tipo opaco, sem inspeção não há otimização, sem tipo não há compressão) descreve palavra por palavra uma UDF Python. Ou seja, a UDF reintroduz dentro da API estruturada exatamente a opacidade que a API existe para eliminar. Com Arrow ligado por padrão no 4.2, a hierarquia de preferência muda, ou só encurta a distância entre `@udf` e `@pandas_udf`?"

**Por que é boa:** nomeia uma ausência em vez de atribuir ao livro algo que ele não diz, e usa a própria tese do capítulo para fazer a pergunta. A segunda metade traz a novidade de 2026 com hipótese embutida.

**Resposta que você já deve saber:** a ausência está registrada como achado no pré-aula, na tese sobre custo de UDF Python, classificada como impossível de conferir por não existir UDF nas cinco leituras. Há uma ironia interna que vale citar: o Chadha usa três transformadores de MLlib numa receita de ingestão, ou seja, chega a importar biblioteca de ML antes de mencionar UDF. Some-se a dúvida `R2-21`, que registra o defeito gêmeo: o capítulo nunca diz que a lambda tipada de `filter()` no Dataset é opaca ao Catalyst, que é o mesmo defeito que ele acusa no RDD cinco páginas antes.

O mecanismo: a regra `ExtractPythonUDFs` arranca a UDF da expressão onde você a escreveu e a reescreve como operador dedicado, avaliado em lote antes do operador que consome o resultado; o executor sobe ou reaproveita um worker `pyspark.worker`, com `spark.python.worker.reuse` em `true`; as colunas de entrada são serializadas por socket; o worker chama a função e devolve; a JVM converte para `UnsafeRow`. No plano isso aparece como `BatchEvalPython` (pickle, linha a linha) ou `ArrowEvalPython` (lotes colunares). Em 4.2, ver `BatchEvalPython` é sintoma e não rotina: ou a config foi desligada, ou há um tipo definido pelo usuário na entrada ou na saída, porque o código tem esse fallback comentado.

Sobre a hipótese: a ordem não muda, a distância muda. O ticket é SPARK-54555 e a documentação é literal, "Since Spark 4.2, Arrow optimization is enabled by default for regular Python UDFs". Arrow mata a serialização por linha e **não** mata a chamada por linha: uma `@udf` escalar continua sendo invocada uma vez por linha dentro do worker, com todo o custo do interpretador, e só as formas vetorizadas recebem um lote por chamada e amortizam a invocação. A hierarquia fica: expressão nativa sempre que existir, depois `pandas_udf` e as Pandas Function APIs, depois `@udf`, e nunca `df.rdd.map`, que paga a fronteira **e** perde o motor. Duas armadilhas de migração para 4.2, do guia oficial: PyArrow mínimo sobe de 15 para 18, e coluna de inteiro nulável passa a chegar como dtype de extensão em vez de `float64`, o que não quebra o import e quebra a aritmética. Ou seja, o upgrade não é ganho de graça, é ganho grande com bateria de teste de UDF antes.

E a memória, que é onde o assunto mata job: o worker vive fora da heap e fora do gerenciador unificado de memória, então quem paga é o container. `spark.executor.memoryOverheadFactor` é `0,10`, exceto **0,40 em job não-JVM no Kubernetes**, e a documentação justifica dizendo que tarefa não-JVM "commonly fail with Memory Overhead Exceeded errors". Executor de 8 GB dá cerca de 800 MB de folga para Python, Arrow e buffers, e uma UDF que carrega um modelo por partição estoura isso sem esforço. Setar `spark.executor.pyspark.memory` não reduz o consumo, mas troca "container morto pelo orquestrador sem explicação" por erro legível do lado do Python.

---

### Nível 3 - Ponte com produção

### Pergunta 12 - Não existe runtime de Delta, Iceberg nem Hudi para o Spark 4.2.0

> "Formato de tabela transacional é silêncio de cinco capítulos: zero ocorrências de Delta no Luu 3 e no Chadha, uma menção entre parênteses no Damji 4, num livro cujo título traz Databricks. Fui conferir o Maven Central esta semana e não existe `iceberg-spark-runtime-4.2_2.13`, não existe bundle de Hudi para 4.2, e o Delta mais recente é construído contra o Spark 4.1.0.0. Então adotar a release nova do Spark e adotar formato de tabela são objetivos em tensão. O senhor trata essa defasagem como normal de ciclo, de semanas, ou ela costuma durar meses? Porque a resposta muda a regra de qual versão adotar em produção."

**Por que é boa:** é um problema arquitetural com data, não com opinião, e é a pergunta que alguém precisa responder antes de aprovar a adoção. Também é a que o professor provavelmente vai gostar de responder, porque é operação e não slide.

**Resposta que você já deve saber:** a contagem é a divergência 5 do pré-aula, e a consequência que ela extrai é a certa: quem lê estes cinco capítulos conclui que persistir Parquet num diretório resolve. O que não resolve, em oito itens: commit não é atômico, porque `overwrite` apaga e escreve com janela no meio, e `_SUCCESS` é convenção que não impede leitor nenhum; não há isolamento de leitor, então uma consulta que listou arquivos no começo pode morrer com arquivo não encontrado ou ver metade de um lote novo; não há transação de mais de uma operação; não há `UPDATE`, `DELETE` nem `MERGE`; evolução de schema é frágil, com `mergeSchema` em `false` por default desde a 1.5.0 e o Chadha encerrando a seção dizendo que o resto é "à mão" sem dizer como; não há versão; não há compactação; e a poda é limitada ao que o diretório sabe.

A ideia central dos três formatos cabe numa frase: a tabela deixa de ser "os arquivos deste diretório" e passa a ser "a lista de arquivos que esta versão do metadado nomeia". Duas famílias de mecanismo. Log de commits numerado, que é o `_delta_log`, onde a atomicidade vem de o armazenamento garantir que só um escritor cria o nome `N+1`. E ponteiro de metadado, que é o Iceberg, cuja especificação diz "all changes to table state create a new metadata file and replace the old metadata with an atomic swap" e "reads will be isolated from concurrent writes and always use a committed snapshot of a table's data". Hudi é o terceiro caminho, nascido de upsert de alta frequência, com Copy-on-Write e Merge-on-Read, e é o único dos três que trata índice por chave como peça central.

O custo, que a literatura de fornecedor não escreve, tem oito itens e o primeiro é o desta pergunta. Os outros que importam: manutenção deixa de ser opcional e vira job permanente (compactação, expiração de snapshot, arquivo órfão), e formato de tabela sem compactação agendada fica **mais lento** que Parquet cru, o que leva de seis a doze meses para ficar óbvio; armazenamento cresce com a retenção de versões; você passa a depender de um catálogo, que é mais um serviço a rodar, autenticar e recuperar; concorrência otimista troca corrupção silenciosa por retentativa visível, que é muito melhor e custa tempo de cluster; depurar exige perícia nova, porque `ls` não ajuda; leitor ingênuo apontado para o diretório lê linha apagada sem erro, e "todo consumidor" inclui o analista que descobriu o caminho do bucket.

E a opção legítima que raramente se diz: se a tabela é append-only, escrita por um job só, lida por um motor só, e você tolera alguns minutos de inconsistência, Parquet num diretório particionado por data continua sendo a resposta certa e a mais barata. Adotar formato de tabela para uma tabela assim é pagar imposto sem receber serviço. Sobre a fronteira estar se movendo, vale ter na ponta da língua: o 4.1 trouxe constraint de tabela e `MERGE INTO` com evolução de schema, e o 4.2 trouxe transação em DSv2, `WITH SCHEMA EVOLUTION` no `INSERT` e a cláusula `CHANGES` para CDC de motor. O motor está absorvendo a **interface** e deixando a **implementação** no formato.

---

### Pergunta 13 - Leitura paralela de JDBC, a lacuna mais cara da bibliografia

> "O Luu é o único dos três que ensina JDBC, e ensina leitura serial sem avisar: quatro peças necessárias, três chaves na tabela, e nenhuma palavra sobre `partitionColumn`, `lowerBound`, `upperBound`, `numPartitions` ou `fetchsize`. O próprio capítulo contém a prova, vinte páginas depois, quando `getNumPartitions` devolve 1. Duas perguntas: quando a chave é UUID ou string, `predicates` é solução de produção ou remendo? E como o senhor negocia `numPartitions` com quem opera o banco de origem, já que a documentação diz que ele determina o número de conexões concorrentes?"

**Por que é boa:** é a única parte desta aula em que um erro do aluno tem vítima fora do cluster, e é trabalho de primeira semana em qualquer lugar. A segunda pergunta é sobre processo, não sobre API, e é onde o professor pode dar o que nenhum livro dá.

**Resposta que você já deve saber:** é a divergência 10 e a dúvida `R4-21`, que o pré-aula classifica como a lacuna mais cara da bibliografia para quem extrai de banco relacional. Duas frases da documentação desfazem o mal-entendido mais comum. Primeira: `lowerBound` e `upperBound` **não filtram**, "they are just used to decide the partition stride, not for filtering the rows in table. So all rows in the table will be partitioned and returned". Pôr `lowerBound=1` e `upperBound=100` numa tabela cujo id vai a dez milhões não lê cem linhas: lê dez milhões, com a última partição levando 9.999.900 delas. E o AQE não conserta, porque o desequilíbrio nasce antes do primeiro shuffle, na fatia que cada conexão pediu ao banco. Segunda: `numPartitions` "also determines the maximum number of concurrent JDBC connections", então 200 num Postgres com `max_connections=100` derruba o banco alheio, não o seu job.

Quatro critérios de qualidade para a coluna de particionamento, e nenhum está na documentação. Distribuição próxima de uniforme entre os limites, porque o passo é aritmético e chave sequencial com buraco grande, do tipo que uma migração deixa, produz partição vazia ao lado de partição enorme. Indexada na origem, senão você trocou um full scan por `numPartitions` full scans concorrentes, que é o erro que parece otimização e é degradação. Imutável durante a leitura, porque as consultas não são simultâneas nem transacionais entre si, e uma linha pode aparecer em duas partições ou em nenhuma, o que elimina coluna de data de atualização apesar de ela ser tentadora. E limites vindos de medição, com um `MIN` e `MAX` antes ou estatística do catálogo do banco.

Sobre `predicates`, que nenhum dos três livros menciona: é solução de produção, e a mais honesta das duas, porque você declara a fatia em vez de deixar a aritmética inventar. O código do PySpark documenta que "partitions of the table will be retrieved in parallel if either `column` or `predicates` is specified", com cada expressão definindo uma partição, e que `column` ganha se os dois forem passados. O risco é operacional, não conceitual: os predicados precisam ser mutuamente exclusivos e exaustivos, o Spark não valida nada, e uma fatia esquecida é dado perdido em silêncio. Derivar os predicados de uma coluna de partição do próprio banco, ou de um hash com módulo, garante exaustividade por construção.

Dois fechamentos. `fetchsize` tem default `0`, que significa deixar o driver decidir, e a própria documentação cita Oracle com dez linhas por viagem de rede: subir para alguns milhares costuma dar ganho de ordem de magnitude ao custo de memória do executor. E o pushdown que o Luu define na página 17 continua valendo e cresceu: hoje são quatro chaves com default `true`, `pushDownPredicate`, `pushDownAggregate`, `pushDownLimit` (que inclui Top-N) e `pushDownTableSample`, com a pegadinha de que `pushDownOffset` só desce quando `numPartitions` é 1, porque `OFFSET` por partição não tem significado global.

---

### Pergunta 14 - Tamanho alvo de arquivo, e a reconciliação dos defaults

> "Três números não conversam. O projeto Parquet recomenda grupo de linhas de 512 MB a 1 GB. O Spark empacota leitura em `maxPartitionBytes` de 128 MB. E cobra 4 MB fictícios por arquivo aberto em `openCostInBytes`. Qual tamanho alvo de arquivo o senhor usa, e alinha com qual dos dois? Minha dúvida concreta é se a divisão respeita a fronteira do grupo de linhas: um arquivo de 512 MB com um grupo único vira uma task, ou quatro?"

**Por que é boa:** é uma pergunta de número, respondível, e revela se o professor decide layout por medição ou por hábito. E o caso do grupo único é exatamente o "não divisível" que a aula 01 registrou, por outra causa.

**Resposta que você já deve saber:** a conta do custo modelado é a que dói. `maxSplitBytes = min(maxPartitionBytes, max(openCostInBytes, bytesTotais / minPartitionNum))`, com cada arquivo entrando na soma valendo tamanho real **mais** `openCostInBytes`, que é 4 MB por padrão desde a 2.0.0 e é descrito como "the estimated cost to open a file, measured by the number of bytes that could be scanned in the same time". Dez mil arquivos de 20 KB são 200 MB reais e cerca de 40 GB de custo contábil, o que empacota uns 32 arquivos por partição e produz aproximadamente 313 tasks para ler 200 MB. E o pré-aula registra em `R4-18` o único número que o Luu dá sobre Parquet, o "cerca de um sexto do tamanho do CSV", sem tamanho absoluto, sem codec e sem descrição do dataset, num caso quase ideal para dicionário colunar: é o tipo de número que não se leva para decisão de layout.

De onde vêm os arquivos pequenos: sempre da escrita, e sempre por um de três motivos. O número de partições do DataFrame na hora do `write`, e o próprio Luu diz isso com clareza no bloco fora do escopo pedido, que o número de arquivos de saída acompanha o número de partições, verificável com `movies.rdd.getNumPartitions`, e que `coalesce(1)` é o truque para arquivo único. `partitionBy` em coluna de cardinalidade alta, que multiplica arquivos por diretórios. Ou ingestão frequente em `append`, um microlote por arquivo. Remédios em ordem: ajustar o número de partições antes de escrever, com `coalesce` para reduzir sem shuffle e `repartition` quando é preciso redistribuir e aceitar o shuffle (a dúvida `R3-23` registra que o Chadha receita os dois lado a lado sem dizer que `repartition` é shuffle completo e `coalesce` é funil); limitar linhas por arquivo no writer, para não cair no oposto; e compactação periódica, que é o que os formatos de tabela automatizam.

Sobre alta cardinalidade, o mecanismo são três custos e nenhum é o que a intuição sugere. Explosão de arquivo, com um diretório por valor distinto: particionar por `cliente_id` com 2 milhões de valores dá 2 milhões de diretórios de poucos KB. Listagem no planejamento, porque em armazenamento de objeto listar é uma sequência de requisições paginadas, e milhões de prefixos custam minutos de driver que não aparecem como stage na Spark UI, o que faz o job parecer travado. E metadado, porque cada partição é uma linha na tabela de partições do metastore, e centenas de milhares transformam o metastore no gargalo. A conta de data vale decorar: por dia, cinco anos dão 1.825 diretórios, confortável; por hora, 43.800, o que já pede volume alto por hora; por minuto, não faça. Se a partição por dia deixa arquivos de 2 MB, a partição está errada, não o dado.

E uma resposta que você já pode dar sozinha se o professor devolver a pergunta: no Iceberg a poda deixa de depender de diretório, porque o metadado guarda estatística por arquivo, e o particionamento oculto declara `days(ts)` como transformação de partição, de forma que "reads will be planned using predicates on data values, not partition values". Isso torna partição por diretório desnecessária na maioria dos casos em que hoje se particiona por medo.

---

### Pergunta 15 - VARIANT na camada bruta contra o contrato explícito

> "As tabelas de tipos dos dois livros não têm tipo para dado semiestruturado de schema instável, e o Chadha resolve o assunto com `get_json_object` e `json_tuple` sobre coluna de string, sem nunca explicar a gramática de `$.name`. Hoje existe VARIANT, GA desde o 4.1, com shredding ligado por default. Minha leitura é que ele não elimina o contrato, muda o lugar dele, da ingestão para a consulta, e que o preço é perder a falha útil, que é o job avisar no dia em que a origem mudou. O senhor usaria VARIANT em camada bruta com projeção tipada na curada, ou é solução em busca de problema?"

**Por que é boa:** traz uma feature posterior aos três livros e a coloca como trade-off, não como novidade. A hipótese sobre "perder a falha útil" é a parte que o professor vai querer confirmar ou refinar.

**Resposta que você já deve saber:** a tese "lista de tipos e de fontes sem VARIANT e sem os tipos geoespaciais" está registrada no pré-aula como confirmada nas cinco leituras, por construção, e as dúvidas `R2-2`, `R2-3` e `R2-4` registram que as tabelas de tipos do Damji 3 têm erro que o próprio capítulo desmente páginas adiante, ou seja, são a informação mais consultável e a mais frágil do capítulo. O registro de leitura da receita 2 do Chadha anota o `get_json_object` e a gramática de caminho nunca explicada.

O que VARIANT é: um tipo que guarda valor semiestruturado como par de binários, valor e metadado de chaves, sem schema fixo e sem virar string, com os tipos dos escalares preservados na codificação. Não é `StringType` com JSON dentro nem `MapType<String,String>`, e o acesso a um campo não reparseia o documento inteiro. Cronologia: entrou no 4.0 pelo SPARK-45827 e virou GA e default no 4.1 pelo SPARK-54454, com operador de dois-pontos para acesso (`payload:cliente.id`), suporte em varredura de CSV e XML, e a opção `singleVariantColumn` na leitura, que é do Spark aberto e não só da Databricks. Vale saber de uma lacuna de documentação: `VariantType` **não aparece** na página de referência de tipos do 4.2.0, embora tenha página de API em PySpark.

Shredding é a otimização de armazenamento: o escritor Parquet extrai os campos frequentes para colunas tipadas dentro do mesmo arquivo e mantém o resto no blob, o que devolve pushdown de filtro e poda de coluna sobre dado que nominalmente não tem schema. O detalhe honesto é que as chaves são todas internas, ligadas por default no 4.2.0 (`writeShredding.enabled`, `pushVariantIntoScan`, `allowReadingShredded`), com teto de 300 campos e 50 níveis na inferência e **nenhuma API pública** para dirigir o que é extraído. Funciona, e você não manda nele. Há limite duro de 128 MiB por valor e por metadado, com `VARIANT_CONSTRUCTOR_SIZE_LIMIT`, e cuidado com a confusão de números, porque a Databricks documenta 16 MiB, que é limite de plataforma.

O trade-off sem adoçar: VARIANT não elimina o contrato, muda o lugar, porque `variant_get(..., "int")` levanta `INVALID_VARIANT_CAST` e você trocou "falhar na leitura, num lugar" por "falhar em cada consulta, em muitos lugares"; você perde a falha útil, e o custo aparece semanas depois num painel; nulabilidade e tipo por campo deixam de existir como declaração, então não há nada para o Catalyst usar nem para um contrato de dados apontar; consumidor a jusante precisa entender VARIANT; e performance depende de shredding, que é opaco. A recomendação defensável é a da pergunta: VARIANT na camada bruta para não perder ingestão por mudança de origem, projeção explícita e tipada para a camada curada, e um teste que compare `schema_of_variant_agg` de hoje com o de ontem para detectar deriva. Assim você fica com o benefício, que é não parar, sem perder o sinal, que é saber que mudou.

---

## Parte 3 - Como fazer a pergunta na hora certa

Pergunta boa na hora errada vira ruído; no tom errado, vira disputa. Seis regras para as duas horas, e a última é nova nesta aula.

**Encaixe a pergunta no slide, não no seu roteiro.** Esta aula é de referência: `read`, `schema`, `write`, tabela, view. As perguntas de ancoragem (1 a 4) grudam num slide específico e ajudam a turma, então dispare quando o slide aparecer. As de produção (12 a 15) pedem tempo de conversa e vão para o fim. Se você abrir com a ausência de runtime de Iceberg para o 4.2, sequestra o roteiro dele nos primeiros vinte minutos.

**Traga a hipótese junto, sempre.** O aprofundamento já escreveu uma hipótese para cada pergunta: use a frase pronta. "Minha leitura é que `mode` e ANSI são camadas independentes, está correto?" é colaboração. "O que é o modo ANSI?" é dever de casa não feito. "O livro está desatualizado nisso" é confronto. A hipótese explícita dá ao professor três saídas úteis, confirmar, corrigir ou refinar, e as três servem para você aprender.

**Cite a fonte sem exibi-la.** Uma frase basta: "a documentação de CSV diz que inferir exige uma passada extra sobre o dado". Recitar bibliografia inteira sinaliza outra coisa, e nesta aula a tentação é grande, porque você tem 110 dúvidas numeradas e doze divergências na mão. Escolha uma referência por pergunta.

**Corrija versão só quando a defasagem muda uma decisão.** Se o slide disser Spark 3.4, deixe passar. Se disser que cast inválido devolve nulo, e alguém na sala vai escrever validação contando com isso, pergunte: "isso mudou no 4.0, com o ANSI por padrão, né?". O critério é se a desatualização afeta a decisão de alguém na sala, não se você notou.

**Escolha no máximo três perguntas.** Marque uma de cada nível antes de entrar e registre na tabela da Parte 5. As demais viram material do pós-aula ou mensagem direta, canal melhor para as longas. Deixe a mais afiada para o fim, em tom de curiosidade genuína.

**Quando você tem o código-fonte do lado, pergunte pelo mecanismo, não pela conclusão.** Esta é a primeira aula em que você chega com endereço de arquivo e número de linha, e isso é faca de dois lados: bem usado, rende uma resposta que nenhum livro dá; mal usado, transforma a aula numa disputa em que ninguém quer estar. Cinco cuidados. Primeiro, **separe "o livro errou" de "o livro envelheceu"**, porque as duas coisas pedem respostas diferentes e só a primeira é crítica ao autor: `bucketBy(...).save(path)` sempre recusou, e `PERMISSIVE` mudou de comportamento na 3.4. Segundo, **traga o endereço e não o veredito**: "no `QueryExecution` do 4.2 essa escolha aparece como `.next()`, com um `TODO`" convida à explicação, "a figura está errada" convida à defesa. Terceiro, **ofereça você mesma a reconciliação**, como em cinco propriedades contra três: quando a saída chega junto com o problema, a conversa vira colaboração em um segundo. Quarto, **diga qual das duas fontes você leu**, documentação ou código, e diga qual não conseguiu arbitrar, porque "não tive Spark instalado para conferir" é a frase que separa quem investiga de quem acusa. Quinto, **se o professor mantiver a posição, anote e vá conferir depois**: a aula não é o foro de arbitragem, e insistir custa mais do que a informação vale. Duas das perguntas desta lista existem justamente porque a divergência ficou sem árbitro, e as duas se resolvem com dez linhas de código: se der tempo antes da aula, rode e chegue com o resultado, que é a única forma de encerrar o assunto sem precisar ganhar uma discussão.

---

## Parte 4 - Banco de perguntas do aprofundamento

As perguntas brutas que cada parte do [aprofundamento](02-aprofundamento.md) abriu, antes da curadoria. Ficam registradas porque a formulação bruta às vezes serve melhor para conversa fora da aula, e porque as que não foram refinadas são a primeira fila de mensagem direta ao professor. A marca **[P*n*]** indica que a pergunta reaparece refinada na Parte 2, com o número dela.

### Parte 1 - O otimizador de verdade: Catalyst, AQE e como ler um plano

1. Se nenhum modelo de custo escolhe entre planos físicos, por que a figura canônica ainda circula em livro de 2020 e em slide de 2026? Existe consulta em que o iterador do planejador devolva mais de um candidato e a ordem das estratégias mude o resultado? **[P2]**
2. Vale ligar o CBO em 2026, num ambiente com metastore e `ANALYZE TABLE` agendado, ou o AQE já cobre? **[P2]**
3. A reotimização do AQE usa o `AQEOptimizer`, com cinco regras, e não o otimizador inteiro. Isso quer dizer que estatística real nunca melhora pushdown nem poda?
4. Com AQE ligado, a recomendação é subir `initialPartitionNum` e deixar coalescer, mas `coalescePartitions.parallelismFirst` é `true` e faz o coalescing ignorar o alvo de 64 MB. As duas recomendações não se anulam?
5. Onde termina o pushdown lógico e começa o físico depende de a fonte ser V1 ou V2. Isso tem consequência prática de escolha de fonte? **[P2, parcialmente]**
6. `spark.sql.codegen.maxFields` é 100 e é config interna. Como se detecta em produção que uma consulta caiu para interpretado por largura de tabela, se o único sinal é a **ausência** do asterisco no plano?
7. Se o custo do AQE é contagem de shuffle, quando esse modelo escolhe errado? Trocar dois shuffles baratos por um shuffle mais uma ordenação gigante reduz o custo medido e aumenta o tempo real.
8. Qual é o critério de revisão de plano que cabe num pull request? A minha lista tem três itens (quantos `Exchange` existem e cada um é necessário; `ReadSchema` e `PushedFilters` coerentes com o que a consulta pede; nó sem asterisco que devia ter). Ele acrescentaria um quarto?

### Parte 2 - Schema, contrato e dado ruim

1. A documentação de CSV diz que linha com número errado de campos não é registro corrompido, e o `UnivocityParser` do 4.2.0 diz o oposto, com comentário explícito. Qual das duas vale hoje? **[P7]**
2. Em `PERMISSIVE` sem a coluna de corrompido no schema, o registro ruim é descartado ou sai como linha de nulos? **[P7]**
3. `spark.read.parquet` sempre dispara um job de uma task para ler o rodapé, ou existe caminho que evita? **[P3]**
4. `samplingRatio` em modo linha única economiza I/O ou só CPU? **[P3]**
5. Existe alguma forma de o Spark aberto impor `nullable=false` na leitura de arquivo? **[P5]**
6. A opção `mode` e `spark.sql.ansi.enabled` são camadas de fato independentes? **[P8]**
7. Há API pública para dirigir o shredding de VARIANT, ou só a inferência automática? **[P15]**
8. Onde a taxa de rejeição deve ser calculada para não custar uma varredura extra, dado o `QUERY_ONLY_CORRUPT_RECORD_COLUMN`? **[P6]**

### Parte 3 - Formatos, layout e persistência

1. O Luu descreveu errado o layout do Parquet, ou citou uma possibilidade da especificação como se fosse o comportamento? Existe escritor que produza Parquet multiarquivo por coluna na prática? **[P4]**
2. Bucketing entra em algum pipeline que ele mantém hoje? Se sim, quantos buckets e como decidiu? **[P9]**
3. Sem formato transacional, qual é a prática recomendada para `overwrite` seguro? Ele reconhece que renomear diretório em armazenamento de objeto **não** é atômico, o que derruba a receita clássica de escrever em staging e renomear? **[P12, parcialmente]**
4. O default `snappy` do Parquet no Spark ainda se justifica em 2026, quando o default do ORC já é `zstd`? Em que perfil de job a CPU de descompressão aparece como gargalo?
5. Alguém desliga `spark.sql.sources.partitionColumnTypeInference.enabled`? O caso que me preocupa é código com zero à esquerda: `agencia=0007` volta da leitura como inteiro `7`, e o join com o dado lido do arquivo não casa nada.
6. O Hive Metastore ainda está de pé nas casas grandes, ou todo projeto novo já nasce em catálogo REST? Ele trata o HMS como dívida técnica com prazo? **[P10]**
7. Existe critério para escolher entre Delta e Iceberg que não seja "qual plataforma você paga"? **[P12]**
8. Qual é o tamanho alvo de arquivo Parquet que ele usa, e como reconcilia com os defaults? A divisão de leitura respeita a fronteira do grupo de linhas? **[P14]**

### Parte 4 - RDD, DataFrame, Dataset e a fronteira do Python

1. Cinco atributos, três ou dois? Qual contagem ele cobra numa prova, e considera a divergência resolvida pela fonte? **[P1]**
2. `partitioner` ausente custa um shuffle? Dá para demonstrar em aula um `join` que ganha um `Exchange` a mais só porque o particionador não foi declarado? **[P1, parcialmente]**
3. Com Arrow por padrão no 4.2, `@udf` virou aceitável? A hierarquia de preferência muda, ou só a distância? **[P11]**
4. `BatchEvalPython` num plano de 4.2 é sempre sintoma, ou existe caso legítimo além de tipo definido pelo usuário na borda?
5. Que heurística de `memoryOverhead` para PySpark pesado? Ele cravaria `spark.executor.pyspark.memory` em produção? **[P11, parcialmente]**
6. Python Data Source contra `binaryFile` mais UDF: onde está a fronteira, dado que a documentação promete até uma ordem de magnitude com `yield` de `pyarrow.RecordBatch` e o 4.1 acrescentou pushdown de filtro para fonte Python?
7. Dataset tipado bate DataFrame em algum caso, dado que a lambda insere `DeserializeToObject`, `MapElements` e `SerializeFromObject` e quebra o codegen nos dois lados?
8. A disciplina vai usar Spark Connect ou Classic? Não é curiosidade: o Connect não expõe `SparkContext` nem RDD, e metade dos exemplos de RDD da bibliografia não executa nele.

### Parte 5 - O que mudou desde os livros, e o que a bibliografia não cobre

1. Em pipeline legado que sobe para 4.x, a recomendação é ligar ANSI e consertar as expressões, ou desligar a config e migrar aos poucos? Existe relatório de incompatibilidade, ou a única ferramenta é rodar e ver quebrar? **[P8]**
2. VARIANT contra `StructType` declarado: qual é o critério, e VARIANT com shredding chega perto de coluna tipada em performance de leitura do campo raro, o que não foi materializado? **[P15]**
3. Qual coluna de particionamento usar em JDBC quando a chave é UUID ou string, e `predicates` é solução de produção ou remendo? **[P13]**
4. UDF Python Arrow por padrão no 4.2 muda **resultado**, e não só performance? O caso do inteiro nulável que chega como dtype de extensão é o único, ou há mais na borda de conversão? **[P11]**
5. Onde termina o motor e começa o formato de tabela, agora que o 4.2 tem transação em DSv2, evolução de schema no `INSERT` e CDC de motor? Existe conector open source que já implemente a API de CDC do 4.2? **[P12]**
6. Como se estuda Spark 4.2 e formato transacional ao mesmo tempo, se não há build de Iceberg, Delta nem Hudi para 4.2? **[P12]**
7. Collation resolve comparação insensível a caixa sem custo de otimização, ou troca um bloqueio por outro? Suspeito que pushdown de filtro com collation para Parquet simplesmente não exista, já que o Parquet guarda estatística de mínimo e máximo em ordem binária.

---

## Parte 5 - Anotações da aula ao vivo

**Data da aula:**

### As três perguntas que eu escolhi levar

| Nível | Pergunta | Fiz? | O que ele respondeu |
|---|---|---|---|
| 1 - ancoragem | | | |
| 2 - profundidade | | | |
| 3 - produção | | | |

### O que o professor cobriu que eu não tinha previsto

### Onde eu estava errada

O que o aprofundamento me deu como certo e a aula corrigiu. Esta seção é a mais valiosa do documento: se estiver vazia, ou a aula foi rasa ou eu não prestei atenção. Três candidatos declarados de antemão, porque foram escritos como hipótese e não como fato: as duas divergências entre documentação e código da Pergunta 7, a suposição de que não existe forma de impor `nullable=false` na leitura de arquivo, e a leitura de que a defasagem de runtime dos formatos de tabela é de semanas e não de meses.

### Pendências

Perguntas que não couberam, para mandar por mensagem direta ou levar na aula seguinte. As da Parte 4 sem a marca **[P*n*]** já são a primeira fila.

### O que virou candidato a nota fiscal

Ideias de artefato que apareceram na aula, para decidir em [04-pos-aula.md](04-pos-aula.md). Três nasceram do aprofundamento e já entram como candidatas: um teste executável que conta os jobs disparados por `read` com `statusTracker`, transformando "não infira schema em produção" em guarda-corpo de CI; um checklist de revisão de plano para pull request, com os três critérios da pergunta 8 do bloco "Parte 1" da Parte 4 deste documento; e um script de dez linhas que arbitra as duas divergências entre documentação e código.

---

Documentos irmãos deste ciclo: o registro do que a bibliografia de fato diz está em [01-pre-aula.md](01-pre-aula.md), com as 110 dúvidas e as doze divergências; o estudo por conta própria, com fonte primária citada, está em [02-aprofundamento.md](02-aprofundamento.md); e o artefato que fecha o ciclo está em [04-pos-aula.md](04-pos-aula.md).
