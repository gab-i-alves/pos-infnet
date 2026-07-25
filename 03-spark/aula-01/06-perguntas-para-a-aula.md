---
title: "Aula 1 de Spark - Perguntas para a aula ao vivo"
aula: "Aula 01 - Arquitetura unificada do Spark, processamento em larga escala e preparação do ambiente"
data: 2026-07-24
tags:
  - spark
  - pos-infnet
  - aula-01
  - perguntas
  - arquitetura-spark
  - data-quality
---

---

## Parte 1 - Ancoragem: onde a Aula 1 encosta no seu trabalho

Você não está aprendendo Spark do zero conceitual: está aprendendo vocabulário novo para problemas que já são seus.

**Driver e executor você já opera com outro nome.** A diferença para o seu orquestrador é que o driver do Spark monta e otimiza o plano, lista os arquivos do storage antes de qualquer executor trabalhar, e recebe de volta tudo que você pedir com `collect()`. O item do meio dói primeiro: quando o job aponta para `gs://bucket/scraping/tribunal=*/data=*/`, quem varre o prefixo é o driver, paginando de mil em mil objetos. Duzentos mil arquivos viram duzentas chamadas HTTP em série, com o cluster ocioso. É o antipadrão do "orquestrador virou gargalo", escondido numa linha que parece barata: `spark.read.json(path)`.

**Seu problema de small files no GCS é, literalmente, particionamento.** Partição no Spark não é o `partitionBy` de diretório do bucket: é a unidade de paralelismo, uma partição por task, uma task por core. Na leitura vale `maxSplitBytes = min(maxPartitionBytes, max(openCostInBytes, bytesTotais / paralelismo))`, e cada arquivo entra na conta como **tamanho real mais `openCostInBytes`** (4 MB por padrão). Duzentos mil JSONs de 20 KB são 4 GB reais e cerca de 800 GB de custo contábil: mais de 6.000 tasks processando 640 KB cada. Não é "o Spark é lento com JSON", é um default de 2015 pensado para HDFS aplicado a object storage ([Performance Tuning](https://spark.apache.org/docs/latest/sql-performance-tuning.html)).

**Lazy evaluation muda como você escreve validação.** Transformações constroem plano, ações disparam job. O ganho é *column pruning* e *predicate pushdown*, que em Parquet viram menos bytes lidos e menos operações Classe B. O custo escondido: sem `cache()`, cada ação re-executa a DAG desde a leitura, então cinco regras de DQ escritas como cinco `count()` são cinco varreduras do bucket.

**PDF é o caso em que a abstração falha por premissa.** `pdfplumber` e `PyMuPDF` são Python, então você cai em `binaryFile` mais UDF. O Spark balanceia por bytes; seu custo varia por página, layout e necessidade de OCR. Dois arquivos de mesmo tamanho podem custar mil vezes diferente. E o processo Python do executor vive fora da heap da JVM, em `spark.executor.memoryOverhead`, causa número um de container morto em PySpark.

**Motor unificado, para você, é argumento de manutenção antes de performance.** Hoje você tem dois caminhos de código para a mesma regra: o parser incremental de cada lote e o backfill que reprocessa histórico quando alguém descobre que o parser errava desde março. Eles divergem, e a divergência é fonte silenciosa de problema de qualidade. Uma definição só, rodando nos dois modos, é o argumento de Tech Lead. É mais forte que "é rápido".

---

## Parte 2 - As perguntas

### Nível 1 - Ancoragem

### Pergunta 1 - Os 100x sobre MapReduce

> "Os dois capítulos repetem o número de 100x sobre MapReduce e atribuem isso a processamento em memória. Quanto desse ganho é RAM e quanto é o modelo de execução, ou seja, DAG em vez de uma cadeia de jobs materializando no HDFS entre cada passo?"

**Por que é boa:** ataca o slogan dos dois livros e mostra que você entendeu o mecanismo, não a propaganda.

**Resposta que você já deve saber:** a maior parte vem do modelo de execução. O MapReduce grava cada job no HDFS com replicação tripla, e o job seguinte lê e desserializa de volta; um algoritmo iterativo de vinte passos paga vinte rodadas disso sobre dados que ele mesmo produziu. O Spark evita a materialização entre estágios e funde operações narrow dentro de um estágio, com whole-stage codegen gerando uma função Java única para o subplano.

A contribuição do paper de RDDs não é "usar RAM", é tolerância a falhas sem replicação: cada RDD guarda o grafo de linhagem, e a partição perdida é recomputada a partir dos pais ([Zaharia et al., NSDI 2012](https://www.usenix.org/conference/nsdi12/technical-sessions/presentation/zaharia)). O recorde do Daytona GraySort de 2014, com 100 TB ordenados três vezes mais rápido que o Hadoop e com dez vezes menos máquinas, foi feito **em SSD, não em memória**. Cache em RAM domina o ganho só em cargas iterativas; em ETL de uma passada, espere de 2x a 10x.

---

### Pergunta 2 - RDD ainda faz sentido em 2026?

> "Os livros tratam RDD como API de baixo nível que raramente se usa, mas todo DataFrame vira RDD no plano físico. Quais são os casos residuais em que descer para RDD é a resposta certa hoje? E o Spark Connect não fecha essa porta de vez, já que não expõe `SparkContext`?"

**Por que é boa:** liga uma afirmação dos livros a uma mudança arquitetural posterior a eles e força uma posição sobre obsolescência real.

**Resposta que você já deve saber:** os casos residuais são poucos: controle muito fino de particionamento, dados sem schema possível, e algoritmos iterativos que não se expressam em operações relacionais. Para o resto o Catalyst ganha, porque enxerga a intenção em vez de uma função opaca.

O Connect empurra isso adiante. O cliente fala gRPC com um driver remoto e não tem `SparkContext`, então `sc.parallelize`, accumulators antigos e qualquer estado local da JVM deixam de existir. Registre o dado atualizado: o Connect não é mais experimental, tem cliente Python leve (`pip install pyspark-client`, cerca de 1,5 MB) e ganhou paridade de API no 4.x, mas o modo padrão **continua sendo o Classic**. O default de `spark.api.mode` é `classic`, a menos que `SPARK_CONNECT_MODE=1` esteja setado ([Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html)).

---

### Pergunta 3 - Onde a unificação vaza

> "O capítulo 1 do Damji vende o motor unificado como batch, streaming, SQL e ML na mesma engine. Onde essa unificação vaza? Quais operações mudam de semântica ou simplesmente não existem quando eu movo o mesmo código de batch para Structured Streaming?"

**Por que é boa:** pega a tese central do capítulo e pede o contra-exemplo, que é onde está o aprendizado.

**Resposta que você já deve saber:** a API é unificada, a semântica não. Em streaming você ganha estado, watermark e modos de output (`append`, `update`, `complete`), e perde ordenação global, alguns tipos de join e agregações sem watermark, cujo estado cresce sem limite. Streaming exige checkpoint e tolera mal mudança de plano entre deploys, o que transforma refatoração em operação de risco.

Há um vazamento maior, no nível de motor. Os próprios autores do paper do CACM de 2016 admitem em nota de rodapé que outros sistemas superam o Spark em computação de grafos. Uma década depois, Spark SQL virou *o* Spark, e a [documentação do 4.2.0](https://spark.apache.org/docs/latest/ml-guide.html) ainda precisa afirmar que a MLlib não está deprecada. O ponto mais afiado é o Photon: a Databricks reescreveu a execução do Spark SQL em C++ fora da JVM, e hoje ele está embaixo de todo SQL warehouse da plataforma. A unificação sobreviveu na API; no motor, bem menos.

---

### Nível 2 - Profundidade

### Pergunta 4 - Topologia e quem morre com a conexão

> "No capítulo 2 do Luu, `local[*]`, client mode e cluster mode aparecem quase como variações de invocação. Do ponto de vista de onde o meu processo roda e de quem morre quando eu perco a conexão, qual é a diferença real, e onde o Spark Connect entra nesse quadro?"

**Por que é boa:** reformula uma seção de "como executar" em pergunta de topologia, que é o ângulo de quem vai para produção.

**Resposta que você já deve saber:** em `local[*]`, driver e executores são threads de uma JVM só, sem rede, sem serialização real e sem shuffle distribuído. Isso ensina sintaxe e esconde o que vai custar caro: `collect()` funciona porque tudo está no mesmo processo, e listar duzentos mil objetos não dói porque o disco local responde na hora. Em client mode o driver roda na sua máquina, então perder a conexão mata o job; é obrigatório para shells interativos, porque o REPL precisa do driver local. Em cluster mode o driver roda dentro do cluster e você pode fechar o terminal.

O Connect é um quarto modelo: o driver vira serviço de vida longa, o cliente é fino e descartável, e a sessão sobrevive ao cliente. As dependências passam a ter dois lados, as do cliente e as dos executores que rodam UDF. Em qualquer modo, ligue `spark.eventLog.enabled=true` desde o primeiro dia, senão a Spark UI em `localhost:4040` morre junto com o processo.

---

### Pergunta 5 - O custo fictício de 4 MB por arquivo

> "A fórmula de particionamento na leitura conta cada arquivo como tamanho mais `openCostInBytes`, 4 MB por padrão, então duzentos mil arquivos de 20 KB viram milhares de tasks minúsculas. Qual heurística o senhor usa para escolher entre compactar upstream, aumentar esses limites, ou usar `coalesce` depois da leitura? Minha hipótese é que a terceira é a que menos resolve."

**Por que é boa:** cita uma fórmula que não está nos capítulos, traz números e já embute a hipótese correta.

**Resposta que você já deve saber:** `coalesce` age depois da leitura. As tasks pequenas já foram criadas, as conexões HTTPS já foram abertas e o custo de listagem já foi pago; ele reduz o número de arquivos de saída, não o custo de entrada. Subir `maxPartitionBytes` e `openCostInBytes` reduz o número de tasks e o overhead de agendamento, mas não reduz o número de requisições ao storage, que continua sendo uma por objeto.

Só compactar upstream ataca a causa. Mire arquivos entre 128 MB e 1 GB e trate compactação como etapa de primeira classe do pipeline, com dono e SLA, não como tuning de última hora. A documentação recomenda de duas a três tasks por core, com pelo menos algumas centenas de milissegundos de trabalho útil cada; seis mil tasks de 640 KB violam as duas coisas.

---

### Pergunta 6 - Schema inference, contrato e o tipo VARIANT

> "Lazy evaluation tem um custo que os livros não destacam: `read.json()` sem schema dispara um job completo só para inferir, e a inferência é instável quando as fontes divergem. Quando vale inferir uma vez e versionar o schema, e quando o tipo VARIANT do Spark 4 é melhor do que qualquer schema fixo?"

**Por que é boa:** mostra que você entendeu que lazy não é grátis, e traz uma feature posterior aos livros.

**Resposta que você já deve saber:** schema explícito é o padrão para fontes com contrato conhecido. Elimina o scan de inferência, que em duzentos mil arquivos pode ser metade do tempo do job, e falha alto quando o contrato quebra, o que é desejável. Inferência amostrada (`samplingRatio`) serve para exploração, nunca para produção: um campo nulo em toda a amostra vira `string`, e um campo numérico num tribunal e textual em outro gera conflito silencioso.

VARIANT, GA na linha 4.x com shredding, é a resposta quando o payload é genuinamente heterogêneo e você não controla a origem: guarda o semi-estruturado em binário otimizado e permite acesso por campo sem reparsear a cada query ([Open Variant Data Type](https://www.databricks.com/blog/introducing-open-variant-data-type-delta-lake-and-apache-spark)). O trade-off é o que interessa: você troca falha explícita de contrato por flexibilidade, e empurra a detecção de mudança para a camada de DQ. Sem asserções versionadas e alerta de taxa de rejeição, VARIANT vira silêncio.

---

### Pergunta 7 - O que o AQE não resolve

> "O AQE está ligado por padrão desde o Spark 3.2 e costuma ser apresentado como quem resolve os problemas de particionamento. Minha leitura é que ele age apenas depois do shuffle, com estatísticas de runtime, e não faz nada pelo particionamento de leitura nem por skew de custo dentro de uma task. Está correto?"

**Por que é boa:** traz hipótese específica e falseável, em vez de "o que é AQE", que é pergunta de slide.

**Resposta que você já deve saber:** está correto. O AQE faz três coisas, todas pós-shuffle: coalescing de partições de shuffle (`advisoryPartitionSizeInBytes`, padrão 64 MB), conversão de estratégia de join em runtime quando um lado se revela pequeno, e split de partições com skew. O split exige violar **dois** limiares ao mesmo tempo, `skewedPartitionFactor` (5x a mediana) e `skewedPartitionThresholdInBytes` (256 MB).

Ficam fora do alcance dele o número de partições na leitura, o custo de listar objetos no storage e o skew de custo por registro. Esse último é o seu caso de PDF: as partições podem estar balanceadas em bytes e completamente desbalanceadas em tempo, porque o AQE mede bytes. O lugar de verificar é a aba Stages da Spark UI, nas Summary Metrics por percentil: um Max dez vezes a mediana em Duration explica quase todo estágio lento.

---

### Nível 3 - Ponte com produção

### Pergunta 8 - Desenho para duzentos mil JSONs por dia

> "Contexto real: meus pipelines de scraping jurídico depositam cerca de duzentos mil JSONs pequenos por dia em GCS, com volume entre tribunais variando por ordens de magnitude. Qual desenho o senhor recomendaria: compactar já na ingestão, usar Spark só como compactador para uma camada bruta em Parquet, ou adotar um table format e delegar a compactação ao `OPTIMIZE`?"

**Por que é boa:** problema arquitetural real, com números, e as três opções são defensáveis. Força um debate de trade-off que serve à turma.

**Resposta que você já deve saber:** o critério não é performance, é onde está a latência aceitável e quem é dono da correção. Compactar na ingestão é o mais barato em compute, mas acopla o scraper ao formato analítico: mudou o schema analítico, mexeu no coletor. Usar Spark como compactador para uma camada bruta em Parquet é o mais comum e o mais fácil de justificar, porque preserva o bruto imutável como fonte de verdade.

Table format (Iceberg ou Delta) é o mais robusto a longo prazo: commit atômico de snapshot, time travel, isolamento de leitores concorrentes e manutenção declarativa. O preço é operar mais um componente de plataforma. Em qualquer opção, o skew entre tribunais precisa ser tratado no layout: se dois tribunais dominam o volume, particionar só por tribunal e data cria diretórios gigantes ao lado de vazios, e o tempo do estágio vira o da task mais lenta.

---

### Pergunta 9 - Quando o Spark mede a coisa errada: PDF

> "Extração de texto de PDF depende de biblioteca Python, então caio em `binaryFile` mais UDF. O Spark particiona por bytes, mas meu custo varia por três ordens de magnitude entre um PDF nativo de duas páginas e um escaneado de oitocentas com OCR. Como se dimensiona executor e particionamento quando a métrica de custo do Spark não é a métrica de custo do trabalho?"

**Por que é boa:** é um caso em que a abstração falha por premissa, não por configuração, e ninguém na sala terá pensado nisso.

**Resposta que você já deve saber:** separar por faixa é a resposta prática. Faça um passo barato de metadados que classifique os arquivos por tamanho e número de páginas, e rode dois ou três jobs com perfis diferentes: poucas tasks concorrentes e muita memória para os pesados, muitas tasks leves para o resto. Você faz à mão o balanceamento que o Spark não faz porque não conhece a sua função de custo.

A armadilha de memória é específica: o processo Python do executor vive **fora** da heap da JVM, em `spark.executor.memoryOverhead`, cujo padrão é 10% da memória do executor com piso de 384 MiB. Para UDF pesada isso é pouco, e o sintoma é o container ser morto pelo gerenciador de cluster, não um `OutOfMemoryError` da JVM. Há também DQ dentro da transformação: uma UDF que levanta exceção mata a task, consome retries e derruba o job por causa de um PDF corrompido. Ela precisa capturar o erro e devolver um struct com status, para a taxa de falha por tribunal virar métrica, não incidente.

---

### Pergunta 10 - Schema drift e a fronteira entre Spark e DQ

> "Sites de tribunal mudam layout sem aviso, então schema drift é rotina. Entre `PERMISSIVE` com `_corrupt_record`, `badRecordsPath`, `FAILFAST` e VARIANT, o que se sustenta em produção? Onde termina a responsabilidade do Spark e começa a de um framework de DQ?"

**Por que é boa:** conecta o tema da aula a uma iniciativa que você lidera e pede uma fronteira arquitetural, não uma feature.

**Resposta que você já deve saber:** `PERMISSIVE` silencioso é o pior dos mundos. O registro entra com campos nulos, ninguém olha o `_corrupt_record`, e o drift aparece semanas depois em um relatório errado. `FAILFAST` é honesto, mas derruba o lote inteiro por um registro, o que em coleta contínua é indefensável.

A combinação viável tem duas peças: quarentena explícita, com o registro ruim indo para uma tabela de rejeitos com motivo e origem, e a **taxa de rejeição por fonte** virando métrica com limiar e alerta; e VARIANT quando você quer absorver o drift sem quebrar o pipeline. A fronteira é essa: o Spark entrega o mecanismo de captura e o lugar onde o dado ruim aterrissa; o contrato, o limiar e a decisão de bloquear ou seguir são do framework de DQ. Se essa decisão ficar dentro do job, vira config espalhada por vinte pipelines e ninguém sabe qual é a política.

---

### Pergunta 11 - Commit em object storage e o mito do `_SUCCESS`

> "Object storage não tem rename atômico, e o commit protocol do Spark historicamente dependia disso. Se um job de seis mil tasks falha a 80%, o que sobra no meu bucket, e o que acontece se o Airflow reexecutar por cima? O marcador `_SUCCESS` é suficiente como sinal de completude para consumidores a jusante?"

**Por que é boa:** é preocupação de operação, não de API, e o `_SUCCESS` é uma crença difundida e frágil.

**Resposta que você já deve saber:** sobram arquivos parciais das tasks que já commitaram. Se o modo de escrita for `append`, a reexecução soma em cima e você duplica dados sem nenhum erro visível. O `_SUCCESS` diz apenas que o job terminou: não é atômico com os dados, não bloqueia leitor concorrente e não distingue "terminou certo" de "terminou depois de uma reexecução parcial".

Existem duas saídas honestas. Tornar a partição idempotente com overwrite dinâmico, para que reexecutar substitua em vez de somar, garantindo que a unidade de escrita coincida com a unidade de retry do orquestrador. Ou usar table format com commit atômico de snapshot, em que ou o snapshot inteiro aparece ou nada aparece. Esse é o argumento mais forte a favor de Iceberg ou Delta em cloud storage, mais forte que time travel: se você já vai adotar table format pela compactação, ganha o commit atômico no mesmo investimento.

---

### Pergunta 12 - O limiar honesto: quando Spark ganha do que já funciona

> "Pergunta de decisão, não de técnica. Hoje eu resolvo boa parte do processamento com Python paralelizado mais BigQuery, e funciona. Qual é o limiar honesto, em volume e em tipo de operação, a partir do qual o Spark ganha desse arranjo? E onde adotar Spark é claramente erro de arquitetura, mesmo com dados grandes?"

**Por que é boa:** é o que um Tech Lead precisa responder para a gestão, e exige que o professor admita os limites da disciplina.

**Resposta que você já deve saber:** comece pela evidência, porque ela é contraintuitiva. O estudo da Microsoft Research de 2013 analisou 174 mil jobs de produção e achou mediana de input abaixo de 14 GB ([Nobody ever got fired for buying a cluster](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/msrtr-2013-2.pdf)). O RedSet, da AWS, analisou meio bilhão de queries do Redshift: 75% escaneiam menos de 1 GB, e só três em cada dez mil passam de 10 TB ([PVLDB vol. 17](https://www.vldb.org/pvldb/vol17/p3694-saxena.pdf)). Enquanto isso, o que cabe numa máquina cresceu duas ordens de magnitude, com instâncias EC2 chegando a 32 TiB de RAM.

O Spark ganha quando o dado é semi-estruturado ou binário e precisa de código imperativo antes de virar tabela, que é exatamente PDF, HTML e parsing complexo; quando o perfil é write-heavy, com MERGE, compactação e backfill; quando o job dura horas e recomeçar do zero é inaceitável, porque linhagem, spill e retry são o seguro que você paga o ano inteiro e usa no dia do incidente; e acima de 8 a 16 vCores de paralelismo útil, ponto em que a economia de escala inverte a favor dele.

É erro quando o trabalho é SQL analítico sobre dado limpo e tabelado, quando o volume cabe numa máquina, e quando não há quem opere o cluster, porque o custo dominante do Spark é operacional e cognitivo. No benchmark do Coiled, os autores gastaram mais tempo ajustando Spark do que todos os outros sistemas somados ([TPC-H comparison](https://docs.coiled.io/blog/tpch.html)). A síntese defensável é híbrida, e é provavelmente o que você vai propor no trabalho.

---

## Parte 3 - Como fazer a pergunta na hora certa

Pergunta boa na hora errada vira ruído; no tom errado, vira disputa. Cinco regras para as duas horas.

**Guarde as de ancoragem para o meio da exposição e as de produção para o fim.** As primeiras encaixam no slide correspondente e ajudam a turma; as de produção pedem tempo de conversa. Se você disparar a última pergunta nos primeiros vinte minutos, sequestra o roteiro dele.

**Traga sempre a sua hipótese junto.** "Minha leitura é que o AQE só age pós-shuffle, está correto?" é colaboração. "O que o AQE faz?" é dever de casa não feito. "O AQE não resolve isso" é confronto. A hipótese explícita dá ao professor a chance de confirmar, corrigir ou refinar, e as três saídas servem para você aprender.

**Cite a fonte sem exibi-la.** Uma frase basta: "no paper do CACM eles admitem em nota de rodapé que...". Ancora a pergunta em algo verificável; recitar bibliografia inteira sinaliza outra coisa.

**Não corrija versão em público a menos que seja material.** Se o slide disser Java 8, deixe passar. Se disser que o Spark Connect é experimental e você for basear topologia nisso, pergunte: "isso mudou do 3.4 para cá, né? Como está a maturidade hoje?". O critério é se a desatualização afeta a decisão de alguém na sala.

**Escolha no máximo três perguntas.** Marque uma de cada nível antes de entrar. As demais viram material do pós-aula ou mensagem direta ao professor, canal melhor para as longas. Deixe a mais afiada para o fim, em tom de curiosidade genuína: "fiquei em dúvida sobre isso lendo os release notes" abre uma conversa que o professor provavelmente quer ter; "então a tese do livro não se sustenta" fecha a porta.
