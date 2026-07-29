---
title: "Aula 01 de Spark - Aula ao vivo"
aula: "Aula 01 - Arquitetura unificada do Spark, processamento em larga escala e preparação do ambiente"
data: 2026-07-24
tags:
  - spark
  - pos-infnet
  - aula-01
  - aula
  - perguntas
  - arquitetura-spark
---

# Aula 01 · Aula ao vivo

Documento da terceira etapa: o que levar para a aula e o que trazer dela. As três primeiras partes são preparação, feitas antes de entrar. A quarta é o banco bruto de perguntas que nasceu do [aprofundamento](02-aprofundamento.md). A quinta se preenche durante e depois da aula. Toda premissa citada nas perguntas da Parte 2 vem do registro de leitura em [01-pre-aula.md](01-pre-aula.md), que é onde conferir o que cada capítulo de fato diz antes de falar em voz alta.

## Parte 1 - Ancoragem: onde a teoria da Aula 1 vira decisão

Os capítulos apresentam driver, executor, avaliação preguiçosa e motor unificado como conceitos. Cada um esconde uma decisão de arquitetura que só aparece quando o pipeline cresce. Cinco pontos que valem ser carregados para a aula.

**O driver faz mais do que orquestrar.** Ele monta e otimiza o plano, lista os arquivos do storage antes de qualquer executor trabalhar, e recebe de volta tudo que for pedido com `collect()`. O item do meio dói primeiro: quando o job aponta para um prefixo com muitos objetos, quem varre o prefixo é o driver, paginando de mil em mil. Centenas de milhares de objetos viram centenas de chamadas HTTP em série, com o cluster inteiro ocioso. É o antipadrão do "orquestrador virou gargalo", escondido numa linha que parece barata: `spark.read.json(path)`.

**Arquivo pequeno é, literalmente, problema de particionamento.** Partição no Spark não é o `partitionBy` de diretório: é a unidade de paralelismo, uma partição por task, uma task por core. Na leitura vale `maxSplitBytes = min(maxPartitionBytes, max(openCostInBytes, bytesTotais / paralelismo))`, e cada arquivo entra na conta como **tamanho real mais `openCostInBytes`** (4 MB por padrão). Cem mil arquivos de 20 KB são 2 GB reais e cerca de 400 GB de custo contábil: mais de 3.000 tasks processando 640 KB cada. Não é "o Spark é lento com JSON", é um default de 2015 pensado para HDFS aplicado a object storage ([Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)).

**Avaliação preguiçosa muda como se escreve validação.** Transformações constroem plano, ações disparam job. O ganho é *column pruning* e *predicate pushdown*, que em Parquet viram menos bytes lidos e menos requisições ao storage. O custo escondido: sem `cache()`, cada ação re-executa a DAG desde a leitura, então cinco regras de qualidade escritas como cinco `count()` são cinco varreduras completas.

**Extração de texto de binários é o caso em que a abstração falha por premissa.** As bibliotecas de PDF são Python, então o caminho é `binaryFile` mais UDF. O Spark balanceia por bytes, mas o custo real varia por página, layout e necessidade de OCR: dois arquivos do mesmo tamanho podem custar mil vezes diferente. E o processo Python do executor vive fora da heap da JVM, em `spark.executor.memoryOverhead`, causa número um de container morto em PySpark.

**Motor unificado é argumento de manutenção antes de performance.** O padrão comum é ter dois caminhos de código para a mesma regra: o que processa o lote que acabou de chegar e o que reprocessa histórico quando alguém descobre que a regra estava errada desde algum ponto no passado. Os dois divergem com o tempo, e a divergência é fonte silenciosa de problema de qualidade. Uma definição só, rodando nos dois modos, é o argumento mais forte a favor do motor unificado, e ele é de manutenção, não de velocidade.

---

## Parte 2 - As perguntas

### Nível 1 - Ancoragem

### Pergunta 1 - Os 100x sobre MapReduce

> "O número de 100x aparece uma vez só nos quatro capítulos, no Luu 1, e ele mesmo credita ao site do projeto, sem carga declarada. O Damji 1 dá outros dois para a mesma comparação: 10 a 20 vezes nos artigos iniciais e 'muitas ordens de grandeza' hoje. E nenhum dos dois liga o ganho a memória. Quanto dele é RAM e quanto é o modelo de execução, ou seja, DAG em vez de uma cadeia de jobs materializando no HDFS entre cada passo?"

**Por que é boa:** em vez de atacar um slogan, mostra que você leu os dois e percebeu que eles não fecham a mesma conta. Fecha a saída fácil de responder "é mais rápido porque é em memória".

**Resposta que você já deve saber:** a maior parte vem do modelo de execução. O MapReduce grava cada job no HDFS com replicação tripla, e o job seguinte lê e desserializa de volta; um algoritmo iterativo de vinte passos paga vinte rodadas disso sobre dados que ele mesmo produziu. O Spark evita a materialização entre estágios e funde operações narrow dentro de um estágio, com whole-stage codegen gerando uma função Java única para o subplano.

A contribuição do paper de RDDs não é "usar RAM", é tolerância a falhas sem replicação: cada RDD guarda o grafo de linhagem, e a partição perdida é recomputada a partir dos pais ([Zaharia et al., NSDI 2012](https://www.usenix.org/conference/nsdi12/technical-sessions/presentation/zaharia)). O recorde do Daytona GraySort de 2014, com 100 TB ordenados três vezes mais rápido que o Hadoop e com dez vezes menos recursos, que é a palavra do livro, foi feito **em SSD, não em memória**. Cache em RAM domina o ganho só em cargas iterativas; em ETL de uma passada, espere de 2x a 10x.

---

### Pergunta 2 - RDD ainda faz sentido em 2026?

> "Sobre o papel do RDD os dois livros discordam, e o Damji discorda de si mesmo: o Luu 1 diz que é a abstração-chave que todo usuário deve aprender, e é RDD que está no único exemplo de código do capítulo; o Damji 2 diz que desde o 2.x virou API de baixo nível, e o exemplo completo dele não usa RDD e diz isso nos comentários; e o Damji 1 lista RDD, DataFrame e Dataset como três APIs entre as quais se escolhe conforme a tarefa. Quais são os casos residuais em que descer para RDD é a resposta certa hoje? E o Spark Connect não fecha essa porta de vez, já que não expõe `SparkContext`?"

**Por que é boa:** transforma uma divergência da bibliografia em pergunta de currículo, e força uma posição sobre obsolescência real em vez de opinião sobre estilo.

**Resposta que você já deve saber:** os casos residuais são poucos: controle muito fino de particionamento, dados sem schema possível, e algoritmos iterativos que não se expressam em operações relacionais. Para o resto o Catalyst ganha, porque enxerga a intenção em vez de uma função opaca.

O Connect empurra isso adiante. O cliente fala gRPC com um driver remoto e não tem `SparkContext`, então `sc.parallelize`, accumulators antigos e qualquer estado local da JVM deixam de existir. Registre o dado atualizado: o Connect não é mais experimental, tem cliente Python leve (`pip install pyspark-client`, cerca de 1,7 MB) e ganhou paridade de API no 4.x, mas o modo padrão **continua sendo o Classic**. O default de `spark.api.mode` é `classic`, a menos que `SPARK_CONNECT_MODE=1` esteja setado ([Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html)).

---

### Pergunta 3 - Onde a unificação vaza

> "O capítulo 1 do Damji vende o motor unificado com quatro componentes, e grafo é um deles: o texto do prêmio de 2016 diz que o Spark substitui os motores separados de batch, grafo, stream e query. Onde essa unificação vaza? Quais operações mudam de semântica ou simplesmente não existem quando eu movo o mesmo código de batch para Structured Streaming?"

**Por que é boa:** pega a tese central do capítulo e pede o contra-exemplo, que é onde está o aprendizado.

**Resposta que você já deve saber:** a API é unificada, a semântica não. Em streaming você ganha estado, watermark e modos de output (`append`, `update`, `complete`), e perde ordenação global, alguns tipos de join e agregações sem watermark, cujo estado cresce sem limite. Streaming exige checkpoint e tolera mal mudança de plano entre deploys, o que transforma refatoração em operação de risco.

Há um vazamento maior, no nível de motor. Os próprios autores do paper do CACM de 2016 admitem em nota de rodapé que outros sistemas superam o Spark em computação de grafos. Uma década depois, Spark SQL virou *o* Spark, e a [documentação do 4.2.0](https://spark.apache.org/docs/latest/ml-guide.html) ainda precisa afirmar que a MLlib não está deprecada. O ponto mais afiado é o Photon: a Databricks reescreveu a execução do Spark SQL em C++ fora da JVM, e hoje ele está embaixo de todo SQL warehouse da plataforma. A unificação sobreviveu na API; no motor, bem menos.

---

### Nível 2 - Profundidade

### Pergunta 4 - Topologia e quem morre com a conexão

> "Client mode e cluster mode aparecem uma vez só nos quatro capítulos, na Tabela 1-1 do Damji 1, como duas linhas presas ao YARN, e a diferença fica implícita na coluna de onde o driver roda: o texto nunca a diz em palavras. O Luu não toca no assunto, e `local[*]` só aparece dentro dos banners de shell. Do ponto de vista de onde o meu processo roda e de quem morre quando eu perco a conexão, qual é a diferença real entre os três, e onde o Spark Connect entra nesse quadro?"

**Por que é boa:** nomeia uma lacuna real da bibliografia em vez de atribuir a ela uma seção que não existe, e move a conversa de "como executar" para topologia, que é o ângulo de quem vai para produção.

**Resposta que você já deve saber:** em `local[*]`, driver e executores são threads de uma JVM só, sem rede, sem serialização real e sem shuffle distribuído. Isso ensina sintaxe e esconde o que vai custar caro: `collect()` funciona porque tudo está no mesmo processo, e listar cem mil arquivos não dói porque o disco local responde na hora. Em client mode o driver roda na sua máquina, então perder a conexão mata o job; é obrigatório para shells interativos, porque o REPL precisa do driver local. Em cluster mode o driver roda dentro do cluster e você pode fechar o terminal.

O Connect é um quarto modelo: o driver vira serviço de vida longa, o cliente é fino e descartável, e a sessão sobrevive ao cliente. As dependências passam a ter dois lados, as do cliente e as dos executores que rodam UDF. Em qualquer modo, ligue `spark.eventLog.enabled=true` desde o primeiro dia, senão a Spark UI em `localhost:4040` morre junto com o processo.

---

### Pergunta 5 - O custo fictício de 4 MB por arquivo

> "A fórmula de particionamento na leitura conta cada arquivo como tamanho mais `openCostInBytes`, 4 MB por padrão, então cem mil arquivos de 20 KB viram milhares de tasks minúsculas. Qual heurística o senhor usa para escolher entre compactar upstream, aumentar esses limites, ou usar `coalesce` depois da leitura? Minha hipótese é que a terceira é a que menos resolve."

**Por que é boa:** cita uma fórmula que não está nos capítulos, traz números e já embute a hipótese correta.

**Resposta que você já deve saber:** `coalesce` age depois da leitura. As tasks pequenas já foram criadas, as conexões HTTPS já foram abertas e o custo de listagem já foi pago; ele reduz o número de arquivos de saída, não o custo de entrada. Subir `maxPartitionBytes` e `openCostInBytes` reduz o número de tasks e o overhead de agendamento, mas não reduz o número de requisições ao storage, que continua sendo uma por objeto.

Só compactar upstream ataca a causa. Mire arquivos entre 128 MB e 1 GB e trate compactação como etapa de primeira classe do pipeline, com dono e SLA, não como tuning de última hora. A documentação recomenda de duas a três tasks por core, com pelo menos algumas centenas de milissegundos de trabalho útil cada; milhares de tasks de 640 KB violam as duas coisas.

---

### Pergunta 6 - Schema inference, contrato e o tipo VARIANT

> "Lazy evaluation tem um custo que os livros não destacam, e o Damji 2 dá o exemplo contra si mesmo: define avaliação preguiçosa como adiar até uma ação ser invocada **ou o dado ser tocado, lido do disco ou escrito nele**, e duas páginas depois lista `read()` como uma das duas transformações do exemplo. Na prática `read.json()` sem schema dispara um job completo só para inferir, e a inferência é instável quando as fontes divergem. Quando vale inferir uma vez e versionar o schema, e quando o tipo VARIANT do Spark 4 é melhor do que qualquer schema fixo?"

**Por que é boa:** mostra que você entendeu que lazy não é grátis, e traz uma feature posterior aos livros.

**Resposta que você já deve saber:** schema explícito é o padrão para fontes com contrato conhecido. Elimina o scan de inferência, que em dezenas de milhares de arquivos pode ser metade do tempo do job, e falha alto quando o contrato quebra, o que é desejável. Inferência amostrada (`samplingRatio`) serve para exploração, nunca para produção: um campo nulo em toda a amostra vira `string`, e um campo numérico numa fonte e textual em outra gera conflito silencioso.

VARIANT, GA na linha 4.x com shredding, é a resposta quando o payload é genuinamente heterogêneo e você não controla a origem: guarda o semi-estruturado em binário otimizado e permite acesso por campo sem reparsear a cada query ([Open Variant Data Type](https://www.databricks.com/blog/introducing-open-variant-data-type-delta-lake-and-apache-spark)). O trade-off é o que interessa: você troca falha explícita de contrato por flexibilidade, e empurra a detecção de mudança para a camada de DQ. Sem asserções versionadas e alerta de taxa de rejeição, VARIANT vira silêncio.

---

### Pergunta 7 - O que o AQE não resolve

> "O Luu 1 é o único dos quatro capítulos que fala de AQE, e lista três coisas que ele faz: trocar estratégia de join dinamicamente, otimizar skew join e ajustar o número de partições. Minha leitura é que as três acontecem depois do shuffle, com estatísticas de runtime, e que ele não faz nada pelo particionamento de leitura nem por skew de custo dentro de uma task. Está correto? O capítulo também não diz se ele vem ligado, e isso mudou no 3.2."

**Por que é boa:** traz hipótese específica e falseável, em vez de "o que é AQE", que é pergunta de slide.

**Resposta que você já deve saber:** está correto. O AQE faz três coisas, todas pós-shuffle: coalescing de partições de shuffle (`advisoryPartitionSizeInBytes`, padrão 64 MB), conversão de estratégia de join em runtime quando um lado se revela pequeno, e split de partições com skew. O split exige violar **dois** limiares ao mesmo tempo, `skewedPartitionFactor` (5x a mediana) e `skewedPartitionThresholdInBytes` (256 MB).

Ficam fora do alcance dele o número de partições na leitura, o custo de listar objetos no storage e o skew de custo por registro. Esse último aparece em qualquer carga cujo custo não é proporcional ao tamanho, como extração de texto de binários: as partições podem estar balanceadas em bytes e completamente desbalanceadas em tempo, porque o AQE mede bytes. O lugar de verificar é a aba Stages da Spark UI, nas Summary Metrics por percentil: um Max dez vezes a mediana em Duration explica quase todo estágio lento.

---

### Nível 3 - Ponte com produção

### Pergunta 8 - Onde compactar uma camada bruta de muitos arquivos pequenos

> "Cenário: um pipeline de ingestão deposita dezenas de milhares de JSONs pequenos por dia em object storage, com volume por fonte variando em ordens de magnitude. Qual desenho o senhor recomendaria: compactar já na ingestão, usar Spark só como compactador para uma camada bruta em Parquet, ou adotar um table format e delegar a compactação ao `OPTIMIZE`?"

**Por que é boa:** é um problema arquitetural em que as três opções são defensáveis. Força um debate de trade-off que serve à turma.

**Resposta que você já deve saber:** o critério não é performance, é onde está a latência aceitável e quem é dono da correção. Compactar na ingestão é o mais barato em compute, mas acopla o produtor ao formato analítico: mudou o schema analítico, mexeu no coletor. Usar Spark como compactador para uma camada bruta em Parquet é o mais comum e o mais fácil de justificar, porque preserva o bruto imutável como fonte de verdade.

Table format (Iceberg ou Delta) é o mais robusto a longo prazo: commit atômico de snapshot, time travel, isolamento de leitores concorrentes e manutenção declarativa. O preço é operar mais um componente de plataforma. Em qualquer opção, o skew entre fontes precisa ser tratado no layout: se duas fontes dominam o volume, particionar só por fonte e data cria diretórios gigantes ao lado de vazios, e o tempo do estágio vira o da task mais lenta.

---

### Pergunta 9 - Quando o Spark mede a coisa errada

> "Extração de texto de PDF depende de biblioteca Python, então o caminho é `binaryFile` mais UDF. O Spark particiona por bytes, mas o custo pode variar três ordens de magnitude entre um PDF nativo de duas páginas e um escaneado de oitocentas com OCR. Como se dimensiona executor e particionamento quando a métrica de custo do Spark não é a métrica de custo do trabalho?"

**Por que é boa:** é um caso em que a abstração falha por premissa, não por configuração, e provavelmente ninguém na sala terá pensado nisso.

**Resposta que você já deve saber:** separar por faixa é a resposta prática. Um passo barato de metadados classifica os arquivos por tamanho e número de páginas, e daí rodam dois ou três jobs com perfis diferentes: poucas tasks concorrentes e muita memória para os pesados, muitas tasks leves para o resto. É fazer à mão o balanceamento que o Spark não faz porque não conhece a função de custo do trabalho.

A armadilha de memória é específica: o processo Python do executor vive **fora** da heap da JVM, em `spark.executor.memoryOverhead`, cujo padrão é 10% da memória do executor com piso de 384 MiB. Para UDF pesada isso é pouco, e o sintoma é o container ser morto pelo gerenciador de cluster, não um `OutOfMemoryError` da JVM. Há também qualidade de dados dentro da transformação: uma UDF que levanta exceção mata a task, consome retries e derruba o job por causa de um único arquivo corrompido. Ela precisa capturar o erro e devolver um struct com status, para a taxa de falha virar métrica, não incidente.

---

### Pergunta 10 - Schema drift e a fronteira entre Spark e DQ

> "Quando a fonte é semiestruturada e muda de layout sem aviso, schema drift é rotina. Entre `PERMISSIVE` com `_corrupt_record`, `badRecordsPath`, `FAILFAST` e VARIANT, o que se sustenta em produção? Onde termina a responsabilidade do Spark e começa a de um framework de qualidade de dados?"

**Por que é boa:** pede uma fronteira arquitetural, não uma feature, e é a pergunta que separa quem já operou pipeline de quem só escreveu job.

**Resposta que você já deve saber:** `PERMISSIVE` silencioso é o pior dos mundos. O registro entra com campos nulos, ninguém olha o `_corrupt_record`, e o drift aparece semanas depois em um relatório errado. `FAILFAST` é honesto, mas derruba o lote inteiro por um registro, o que em ingestão contínua é indefensável.

A combinação viável tem duas peças: quarentena explícita, com o registro ruim indo para uma tabela de rejeitos com motivo e origem, e a **taxa de rejeição por fonte** virando métrica com limiar e alerta; e VARIANT quando a intenção é absorver o drift sem quebrar o pipeline. A fronteira é essa: o Spark entrega o mecanismo de captura e o lugar onde o dado ruim aterrissa; o contrato, o limiar e a decisão de bloquear ou seguir são do framework de qualidade. Se essa decisão ficar dentro do job, vira config espalhada por dezenas de pipelines e ninguém sabe qual é a política.

---

### Pergunta 11 - Commit em object storage e o mito do `_SUCCESS`

> "Object storage não tem rename atômico, e o commit protocol do Spark historicamente dependia disso. Se um job de milhares de tasks falha a 80%, o que sobra no bucket, e o que acontece se o orquestrador reexecutar por cima? O marcador `_SUCCESS` é suficiente como sinal de completude para consumidores a jusante?"

**Por que é boa:** é preocupação de operação, não de API, e o `_SUCCESS` é uma crença difundida e frágil.

**Resposta que você já deve saber:** sobram arquivos parciais das tasks que já commitaram. Se o modo de escrita for `append`, a reexecução soma em cima e você duplica dados sem nenhum erro visível. O `_SUCCESS` diz apenas que o job terminou: não é atômico com os dados, não bloqueia leitor concorrente e não distingue "terminou certo" de "terminou depois de uma reexecução parcial".

Existem duas saídas honestas. Tornar a partição idempotente com overwrite dinâmico, para que reexecutar substitua em vez de somar, garantindo que a unidade de escrita coincida com a unidade de retry do orquestrador. Ou usar table format com commit atômico de snapshot, em que ou o snapshot inteiro aparece ou nada aparece. Esse é o argumento mais forte a favor de Iceberg ou Delta em cloud storage, mais forte que time travel: se você já vai adotar table format pela compactação, ganha o commit atômico no mesmo investimento.

---

### Pergunta 12 - O limiar honesto: quando Spark ganha do que já funciona

> "Pergunta de decisão, não de técnica. Um arranjo muito comum hoje é Python paralelizado mais um warehouse na nuvem, e ele funciona. Qual é o limiar honesto, em volume e em tipo de operação, a partir do qual o Spark ganha desse arranjo? E onde adotar Spark é claramente erro de arquitetura, mesmo com dados grandes?"

**Por que é boa:** é a pergunta que alguém precisa responder para a gestão antes de aprovar a adoção, e exige que o professor admita os limites da disciplina.

**Resposta que você já deve saber:** comece pela evidência, porque ela é contraintuitiva. O estudo da Microsoft Research de 2013 analisou 174 mil jobs de produção e achou mediana de input abaixo de 14 GB ([Nobody ever got fired for buying a cluster](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/msrtr-2013-2.pdf)). A AWS publicou o Redset, dataset anonimizado da frota do Redshift ([PVLDB vol. 17](https://www.vldb.org/pvldb/vol17/p3694-saxena.pdf)), e a MotherDuck o analisou ([Redshift Files](https://motherduck.com/blog/redshift-files-hunt-for-big-data/)): 75% das queries escaneiam menos de 1 GB, e as acima de 10 TB ficam na casa de uma em mil. O dado é da AWS, a leitura é de quem vende single-node, e vale dizer isso ao citar. Enquanto isso, o que cabe numa máquina cresceu duas ordens de magnitude, com instâncias EC2 chegando a 32 TiB de RAM.

O Spark ganha quando o dado é semi-estruturado ou binário e precisa de código imperativo antes de virar tabela, que é exatamente PDF, HTML e parsing complexo; quando o perfil é write-heavy, com MERGE, compactação e backfill; quando o job dura horas e recomeçar do zero é inaceitável, porque linhagem, spill e retry são o seguro que você paga o ano inteiro e usa no dia do incidente; e acima de 8 a 16 vCores de paralelismo útil, ponto em que a economia de escala inverte a favor dele.

É erro quando o trabalho é SQL analítico sobre dado limpo e tabelado, quando o volume cabe numa máquina, e quando não há quem opere o cluster, porque o custo dominante do Spark é operacional e cognitivo. No benchmark do Coiled, os autores gastaram mais tempo ajustando Spark do que todos os outros sistemas somados ([TPC-H comparison](https://docs.coiled.io/blog/tpch.html)). A síntese defensável é híbrida: Spark para ELT distribuído e carga write-heavy, single-node para exploração e manutenção leve.

---

### Perguntas que a leitura completa acrescentou

Estas três não vieram do aprofundamento e sim do registro de leitura, depois que os quatro capítulos foram auditados linha a linha. Cada uma nasce de uma contradição interna da bibliografia, então a fonte é o próprio material da aula.

### Pergunta 13 - Modo local não é modo standalone (nível 1)

> "O Damji 2 abre dizendo que o capítulo inteiro roda em local mode, e no Passo 3 escreve que você instalou o Spark no laptop em standalone mode. A Tabela 1-1 do capítulo 1 trata os dois como linhas distintas, e ainda diz que em modo local o cluster manager roda no mesmo host, quando em modo local não existe cluster manager nenhum. Qual é a diferença real entre os dois, e o que o `local[*]` faz que o standalone de uma máquina só não faz?"

**Por que é boa:** é confusão de vocabulário que a turma inteira vai carregar para o resto do curso, e está no capítulo mais lido dos quatro. Custa dois minutos de aula e conserta o modelo mental de todo mundo.

**Resposta que você já deve saber:** em `local[*]` não há cluster manager, não há processo de executor separado e não há rede: driver e executores são threads de uma JVM única, e o asterisco pede uma thread por core disponível. O standalone é um cluster manager de verdade, o que vem na caixa do Spark, com master e workers como processos, e roda numa máquina só ou em cem. A célula da Tabela 1-1 foi preenchida por simetria e inventa uma peça. A consequência prática é que tudo que quebra em cluster (serialização, shuffle pela rede, `collect()` estourando o driver) não aparece em modo local.

---

### Pergunta 14 - Bytecode idêntico e a fronteira do PySpark (nível 2)

> "O Damji 1 afirma que o mesmo trecho escrito em Python, R ou Java gera bytecode idêntico e entrega a mesma performance. Minha leitura é que isso vale para expressão nativa de DataFrame, que vira o mesmo plano em qualquer linguagem, e deixa de valer no instante em que entra uma UDF em Python, que atravessa a fronteira JVM/Python. Está correto? E qual é o custo dessa travessia hoje, com Arrow no caminho?"

**Por que é boa:** a ressalva ausente é justamente a que decide performance em PySpark, que é a linguagem de quase todo mundo na sala, e o livro usa a afirmação como prova de que a escolha de linguagem é indiferente.

**Resposta que você já deve saber:** para expressão nativa a afirmação se sustenta, porque a API de DataFrame em qualquer linguagem produz o mesmo plano lógico e o Catalyst gera o mesmo código. Com UDF Python o dado sai da JVM, é serializado, atravessa para um processo Python do executor e volta, e nada disso é visível para o Catalyst, que trata a função como caixa preta e perde pushdown e codegen no ponto. Arrow reduz o custo de serialização, não elimina a travessia nem devolve a visibilidade ao otimizador. E o processo Python vive fora da heap da JVM, em `spark.executor.memoryOverhead`. Paridade de capacidade existe; de assinatura, não: o próprio capítulo 2 escreve `show(10, false)` contra `show(10, truncate=False)` e `col("State") === "CA"` contra `mnm_df.State == "CA"`.

---

### Pergunta 15 - Um executor por nó (nível 3)

> "O Damji 1 afirma que, na maioria dos modos de deployment, roda um único executor por nó. A própria Tabela 1-1 não sustenta isso: só a linha do standalone fala em uma JVM de executor por nó, e as de YARN e Kubernetes não dizem nada sobre quantidade. Na prática, como o senhor dimensiona isso? Um executor gordo por nó ou vários magros, e qual é o critério, GC, paralelismo ou memória por task?"

**Por que é boa:** é a decisão de dimensionamento que a bibliografia despacha em uma frase e que aparece no primeiro dia de operação real. Força o professor a dar um critério, não uma definição.

**Resposta que você já deve saber:** a afirmação é do livro, não da prática. O padrão comum é vários executores por nó, dimensionados para algo em torno de cinco cores cada, porque a vazão de I/O por executor satura e porque heap muito grande piora pausa de GC. Executor gordo desperdiça paralelismo e concentra risco: se ele morre, o nó inteiro reexecuta. Executor magro multiplica a memória de overhead fixa e o custo de broadcast, que é replicado por executor. Em Kubernetes a conta muda de nome mas não de natureza: o limite é o pod, e sobra o mesmo trade-off entre cores por pod e pods por nó.

---

## Parte 3 - Como fazer a pergunta na hora certa

Pergunta boa na hora errada vira ruído; no tom errado, vira disputa. Cinco regras para as duas horas.

**Guarde as de ancoragem para o meio da exposição e as de produção para o fim.** As primeiras encaixam no slide correspondente e ajudam a turma; as de produção pedem tempo de conversa. Se você disparar a última pergunta nos primeiros vinte minutos, sequestra o roteiro dele.

**Traga sempre a sua hipótese junto.** "Minha leitura é que o AQE só age pós-shuffle, está correto?" é colaboração. "O que o AQE faz?" é dever de casa não feito. "O AQE não resolve isso" é confronto. A hipótese explícita dá ao professor a chance de confirmar, corrigir ou refinar, e as três saídas servem para você aprender.

**Cite a fonte sem exibi-la.** Uma frase basta: "no paper do CACM eles admitem em nota de rodapé que...". Ancora a pergunta em algo verificável; recitar bibliografia inteira sinaliza outra coisa.

**Não corrija versão em público a menos que seja material.** Se o slide disser Java 8, deixe passar. Se disser que o Spark Connect é experimental e você for basear topologia nisso, pergunte: "isso mudou do 3.4 para cá, né? Como está a maturidade hoje?". O critério é se a desatualização afeta a decisão de alguém na sala.

**Escolha no máximo três perguntas.** Marque uma de cada nível antes de entrar. As demais viram material do pós-aula ou mensagem direta ao professor, canal melhor para as longas. Deixe a mais afiada para o fim, em tom de curiosidade genuína: "fiquei em dúvida sobre isso lendo os release notes" abre uma conversa que o professor provavelmente quer ter; "então a tese do livro não se sustenta" fecha a porta.

---

## Parte 4 - Banco de perguntas do aprofundamento

Perguntas que surgiram parte por parte durante o aprofundamento, antes da curadoria. Várias reaparecem refinadas na Parte 2; estas ficam registradas porque a formulação bruta às vezes é melhor para uma conversa fora da aula.

### Vindas da parte sobre arquitetura e modelo de execução

1. Com AQE ligado por padrão desde o 3.2 e reotimização por estatísticas reais, ainda faz sentido investir em CBO e `ANALYZE TABLE`, ou o esforço migrou inteiro para layout de dados (particionamento físico, bucketing, tamanho de arquivo)?
2. Em um pipeline com muitos arquivos pequenos em object storage, qual é a ordem de ataque: compactação no storage, ajuste de `maxPartitionBytes`, ou `repartition` depois da leitura? E como medir qual dominou?
3. Com Spark Connect sem acesso a RDD, quais padrões operacionais reais deixam de ser possíveis, e vale a pena adotá-lo em produção hoje?
4. Qual é o critério prático para decidir entre confiar na recomputação por linhagem e pagar um checkpoint em storage confiável?

### Vindas da parte sobre por que o spark existe, e quando não usar

Ancoradas em fonte, ordenadas da mais construtiva para a mais afiada.

1. O 100x sobre MapReduce é número do site do projeto, repassado pelo Luu 1; o Damji 1 atribui aos artigos iniciais uma faixa bem menor, de 10 a 20 vezes, e o recorde do GraySort em 2014 foi em SSD, não em memória. Que parte do ganho é a linhagem evitando replicação e que parte é apenas "estar na RAM"?
2. O estudo da Microsoft Research de 2013 já mostrava mediana de job abaixo de 14 GB, um ano depois do paper de RDDs. Por que a indústria adotou scale-out em massa com essa evidência disponível desde o início?
3. Se o motor unificado é a tese central do artigo de 2016, por que a Databricks reescreveu a execução do Spark SQL em C++ fora da JVM com o Photon? A unificação sobreviveu na API ou no motor?
4. A documentação do Spark 4.2.0 ainda precisa afirmar que a MLlib não está deprecada. Sem primitivas de GPU e com Ray dominando treino distribuído, a MLlib ainda faz parte da proposta de valor?
5. A evidência de "small data" vem de telemetria de **leitura**, e o caso mais forte do Spark é ELT **write-heavy**. Os dois lados desse debate estão medindo a mesma coisa?
6. O DuckDB processa 1 TB numa máquina de 64 GB em 19 minutos. A AWS vende instâncias de 32 TiB de RAM. Onde está o ponto de inflexão hoje, em números concretos?
7. Se o meu maior job cabe em 256 GB de RAM, qual é o argumento **técnico**, e não organizacional, para eu rodar Spark?

### Vindas da parte sobre rdd, dataframe, dataset, catalyst e tungsten

1. Com AQE ligado por padrão desde o 3.2 e reordenação real de junções acontecendo em tempo de execução, o CBO baseado em `ANALYZE TABLE` ainda tem uso prático, ou virou peça de museu?
2. `spark.sql.codegen.maxFields` com padrão 100: em tabelas largas, qual a estratégia recomendada, aumentar o limite ou reestruturar o schema em structs aninhados?
3. Com UDFs Arrow ligadas por padrão no 4.2, qual a diferença de desempenho que ainda resta entre uma `@udf` comum e uma `@pandas_udf` escrita à mão?
4. Photon, Gluten e Comet substituem o runtime JVM por execução nativa vetorizada. Isso torna o whole-stage codegen do Tungsten uma tecnologia de transição?
5. O Spark Connect não expõe `SparkContext` nem RDD. Se a recomendação é Connect para aplicações novas, os casos legítimos de RDD deixam de existir na prática?

### Vindas da parte sobre o que mudou desde os livros

Três coisas que valem virar pergunta ao professor, porque são exatamente os pontos onde o material oficial e a realidade de 2026 divergem:

1. Se o AQE está ligado por padrão desde o 3.2 e resolve coalescing, conversão de join e skew, o que sobrou de tuning manual que ainda vale a pena ensinar em 2026?
2. O Luu 1 apresenta o DPP como uma das três features do 3.0, e o Damji não menciona nem DPP nem AQE em nenhum dos dois capítulos. As três condições de ativação do DPP quase nunca são verificadas na prática. Como se checa, no plano físico, se o DPP de fato ocorreu?
3. Se a tese do paper de 2016 é o motor unificado, e a Databricks reescreveu a execução em C++ fora da JVM com o Photon, a unificação sobreviveu na API ou no motor?

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

O que o aprofundamento me deu como certo e a aula corrigiu. Esta seção é a mais valiosa do documento: se estiver vazia, ou a aula foi rasa ou eu não prestei atenção.

### Pendências

Perguntas que não couberam, para mandar por mensagem direta ou levar na aula seguinte.

### O que virou candidato a nota fiscal

Ideias de artefato que apareceram na aula, para decidir em [04-pos-aula.md](04-pos-aula.md).
