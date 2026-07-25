---
title: "Montando o ambiente Spark em 2026"
aula: "Aula 1 - Arquitetura unificada e preparação do ambiente"
data: 2026-07-24
tags:
  - spark
  - spark-4.2
  - ambiente
  - pyspark
  - uv
  - docker
  - spark-connect
  - databricks
  - unity-catalog
  - pos-infnet
---

# Montando o ambiente Spark em 2026

---

## 0. Por que você não deve seguir os capítulos ao pé da letra

Os dois livros da aula 1 são bons, mas foram escritos contra o Spark 3.0/3.1. O capítulo 2 de "Beginning Apache Spark 3" e a seção "Downloading Apache Spark" do "Learning Spark" 2ª edição descrevem um mundo que não existe mais. Se você seguir as instruções deles literalmente, você monta um ambiente que **não roda o Spark atual**.

O que mudou, item por item:

| O que os livros dizem | O que vale em 24/07/2026 |
|---|---|
| Instale o **JDK 8** (ou 11) | Java 8 e 11 foram **removidos** no Spark 4.0. Hoje: **Java 17, 21 ou 25** |
| Spark é pré-compilado com **Scala 2.12** | Spark 4.x é pré-compilado só com **Scala 2.13**. O 2.12 foi descontinuado |
| **Python 3.6+** basta | Mínimo é **Python 3.10**. O wheel declara `requires_python >= 3.10` |
| Baixe o **tarball**, descompacte, configure `SPARK_HOME` | `pip install pyspark` já entrega tudo, inclusive `spark-submit`. Tarball só quando você precisa dos scripts de cluster |
| Use a **Databricks Community Edition** para praticar de graça | A **Community Edition morreu em 01/01/2026**. Existe a **Free Edition**, que é outro produto, com outra arquitetura |
| Arquitetura: driver + cluster manager + executors, ponto | Existe agora um **segundo modo de operação**: Spark Connect, cliente e servidor separados por gRPC |

Guarde esta ideia: os livros ensinam bem o **modelo mental** (RDD, DataFrame, driver, executor, lazy evaluation, DAG). Você deve ler os capítulos para isso. Mas as **instruções operacionais** deles estão vencidas, e é este documento que você segue ao teclado.

---

## 1. Versões e requisitos hoje

### 1.1 Qual Spark usar

A release estável mais recente é o **Apache Spark 4.2.0**, de **14/07/2026** (mais de 1.700 tickets do Jira, 250+ contribuidores). Fonte: [Spark Release 4.2.0](https://spark.apache.org/releases/spark-release-4-2-0.html).

Um detalhe que confunde quem olha a página de downloads rápido: existem releases de manutenção **com data posterior** à 4.2.0.

| Release | Data | O que é |
|---|---|---|
| **4.2.0** | 14/07/2026 | **estável mais nova - use esta** |
| 4.1.3 | 15/07/2026 | patch do ramo 4.1 |
| 4.0.4 | 15/07/2026 | patch do ramo 4.0 |
| 3.5.9 | 16/07/2026 | patch do último ramo 3.x mantido |

Data mais recente não significa versão mais nova. Há três ramos 4.x ativos (4.0.x, 4.1.x, 4.2.x) mais o 3.5.x em manutenção estendida. Para estudar, vá de 4.2.0. Fonte: [Downloads](https://spark.apache.org/downloads.html) e [News](https://spark.apache.org/news/).

Ignore as releases de preview (4.2.0-preview1 a preview5, de janeiro a maio de 2026). Preview não é para uso real.

### 1.2 Requisitos oficiais

A [documentação do Spark 4.2.0](https://spark.apache.org/docs/latest/) diz textualmente:

> "Spark runs on Java 17/21/25, Scala 2.13, Python 3.10+, and R 4.0+ (Deprecated). Java 25 prior to version 25.0.3 support is deprecated as of Spark 4.2.0."

Traduzindo para decisão:

| Componente | Suportado | Escolha recomendada |
|---|---|---|
| Java (JDK) | 17, 21, 25 | **Temurin 21 LTS**. Se for 25, exija `>= 25.0.3` |
| Scala | apenas 2.13 | irrelevante se você só usa PySpark, mas importa nos JARs |
| Python | 3.10 a 3.14 | **3.12** |
| R | 4.0+, **deprecado** | não use. O SparkR está em fim de vida |

### 1.3 O que quebra com a versão errada

Vale decorar estes erros, porque você vai encontrá-los:

- **Java 8 ou 11**: `UnsupportedClassVersionError` já no start. É o erro de quem seguiu tutorial antigo.
- **`JAVA_HOME` vazio**: mensagem clara (`JAVA_HOME is not set`) ou, pior, o Spark pega outro JDK do `PATH` e você depura fantasma.
- **JAR com sufixo `_2.12`** em Spark 4: `NoSuchMethodError` ou `ClassNotFoundException` em **runtime**, não em build. Sempre confira o sufixo `_2.13` (`spark-sql-kafka-0-10_2.13`, `delta-spark_2.13`).
- **Python 3.9 ou anterior**: o `pip` nem resolve o pacote.
- **Python diferente no driver e no worker**: `Python in worker has different version ... than that in driver`. É o erro número um de quem mistura venv com Jupyter. Corrija apontando `PYSPARK_PYTHON` e `PYSPARK_DRIVER_PYTHON` para o mesmo interpretador.
- **`py4j` instalado na mão**: o PySpark 4.2.0 fixa a faixa dele. Não mexa.

---

## 2. Caminho recomendado: local, com uv

Este é o caminho principal. Roda no seu WSL2 sem Docker, sem cluster, sem nuvem.

### 2.1 Passo 1 - JDK

```bash
# Ubuntu / WSL2
sudo apt update
sudo apt install -y wget apt-transport-https gpg
wget -qO - https://packages.adoptium.net/artifactory/api/gpg/key/public \
  | sudo gpg --dearmor -o /etc/apt/keyrings/adoptium.gpg
echo "deb [signed-by=/etc/apt/keyrings/adoptium.gpg] https://packages.adoptium.net/artifactory/deb $(awk -F= '/^VERSION_CODENAME/{print$2}' /etc/os-release) main" \
  | sudo tee /etc/apt/sources.list.d/adoptium.list
sudo apt update
sudo apt install -y temurin-21-jdk
```

Fixe o `JAVA_HOME` no shell (`~/.bashrc`):

```bash
echo 'export JAVA_HOME=/usr/lib/jvm/temurin-21-jdk-amd64' >> ~/.bashrc
echo 'export PATH="$JAVA_HOME/bin:$PATH"' >> ~/.bashrc
source ~/.bashrc
java -version   # deve dizer 21.x
```

Se preferir não mexer em repositório apt, o [SDKMAN](https://sdkman.io/) resolve em duas linhas: `sdk install java 21.0.8-tem` e ele já ajusta o `JAVA_HOME`.

### 2.2 Passo 2 - projeto com uv

O `uv` resolve dependências em segundos e, o mais importante aqui, **fixa a versão do Python do projeto**, o que elimina o descasamento driver/worker.

```bash
uv init spark-lab
cd spark-lab
uv python pin 3.12
uv add "pyspark[sql,connect]==4.2.0"
uv run python -c "import pyspark; print(pyspark.__version__)"
```

Extras disponíveis, para você escolher com consciência em vez de instalar tudo:

```bash
uv add "pyspark[sql]==4.2.0"               # pandas + pyarrow (quase sempre você quer)
uv add "pyspark[connect]==4.2.0"           # cliente Connect com a JVM junto
uv add "pyspark[ml]==4.2.0"                # MLlib na API DataFrame
uv add "pyspark[pandas_on_spark]==4.2.0"   # API pandas sobre Spark
uv add "pyspark[pipelines]==4.2.0"         # Spark Declarative Pipelines
```

Execução descartável, sem criar projeto nenhum:

```bash
uv run --with "pyspark[sql]==4.2.0" python meu_script.py
uvx --from "pyspark==4.2.0" pyspark        # abre o shell PySpark
```

Se você preferir o caminho clássico, é o mesmo resultado:

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install "pyspark[sql,connect]==4.2.0"
```

### 2.3 O que o wheel traz e o que não traz

O wheel do PySpark **embarca a distribuição Spark inteira**: os JARs ficam em `site-packages/pyspark/jars/`, e você ganha `spark-submit`, `spark-shell` e `pyspark` no `PATH` do ambiente. Isso é o que torna obsoleto o ritual de baixar tarball dos livros.

O que ele **não** traz: a JVM (por isso o passo 1), o Hadoop completo e conectores externos (Kafka, Delta, JDBC). Conectores vêm por `--packages` ou `spark.jars.packages`, sempre com sufixo `_2.13`.

Variante de Hadoop, se precisar:

```bash
PYSPARK_HADOOP_VERSION=3 pip install pyspark -v        # padrão
PYSPARK_HADOOP_VERSION=without pip install pyspark     # sem Hadoop empacotado
```

### 2.4 O que `local[*]` realmente significa

Aqui os livros ajudam pouco e a confusão é comum. Em modo local **não existe cluster manager, nem master, nem worker separado**. Driver e executor são o **mesmo processo JVM**, e o paralelismo vem de threads.

| URL de master | Significado |
|---|---|
| `local` | 1 thread, zero paralelismo. Ótimo para depurar |
| `local[4]` | 4 threads de worker |
| `local[*]` | uma thread por núcleo lógico da máquina |
| `local[*,2]` | como acima, com até 2 retentativas por task (testa caminho de falha) |

Sessão local bem configurada:

```python
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("spark-lab")
         .master("local[*]")
         .config("spark.driver.memory", "4g")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())
```

Dois ajustes que quase ninguém faz e que deveriam ser padrão em local:

- **`spark.sql.shuffle.partitions`**: o default é 200. Em `local[*]` isso cria 200 tarefas minúsculas e o overhead de agendamento domina o tempo. Ponha algo próximo do número de núcleos.
- **`spark.driver.memory`**: em modo local, o driver **é** o executor. `spark.executor.memory` é simplesmente **ignorado**. Se você aumentar a memória errada, nada acontece e você não entende por quê.

---

## 3. Docker como alternativa

Use Docker quando quiser reproduzir uma versão exata sem sujar a máquina, ou quando for simular um cluster standalone de verdade.

A imagem oficial é **`apache/spark`**, com tags 4.2.0 publicadas em 16-17/07/2026. O padrão de nome é `<versão>-scala<X>-java<Y>-python3[-r]-ubuntu`, com aliases curtos:

```
4.2.0
4.2.0-python3
4.2.0-java21-python3
4.2.0-scala2.13-java21-python3-r-ubuntu
```

Shell interativo:

```bash
docker run -it --rm apache/spark:4.2.0-python3 /opt/spark/bin/pyspark
```

Job com dados montados e a Spark UI exposta:

```bash
docker run -it --rm \
  -p 4040:4040 -p 15002:15002 \
  -v "$PWD/data:/data" \
  apache/spark:4.2.0-scala2.13-java21-python3-r-ubuntu \
  /opt/spark/bin/spark-submit --master "local[*]" /data/job.py
```

**Armadilha dos tutoriais antigos:** muita gente ainda manda usar `apache/spark-py`. Essa imagem está **abandonada** - o último push foi em 15/04/2023, na versão 3.4.0. Hoje o Python vem nas variantes `-python3` da imagem `apache/spark`. Vale o mesmo para `apache/spark-r`.

Imagens de terceiros úteis:

| Imagem | Serve para |
|---|---|
| `quay.io/jupyter/pyspark-notebook` | JupyterLab com PySpark pronto (o namespace no Docker Hub foi descontinuado, use o quay.io) |
| `bitnami/spark` | master e workers standalone via compose |

Compose mínimo para praticar um cluster de verdade, com processos separados, em vez de `local[*]`:

```yaml
services:
  spark:
    image: bitnami/spark:4
    environment: [SPARK_MODE=master]
    ports: ["8080:8080", "7077:7077"]
  spark-worker:
    image: bitnami/spark:4
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark:7077
      - SPARK_WORKER_CORES=2
      - SPARK_WORKER_MEMORY=2G
    deploy: {replicas: 2}
```

Vale rodar isso pelo menos uma vez. Ver dois workers se registrando na UI da porta 8080 fixa a arquitetura driver/master/executor muito melhor do que o diagrama do livro.

---

## 4. Spark Connect: o que é e por que muda o desenvolvimento local

### 4.1 O mecanismo

O Spark Connect é um protocolo **gRPC + Protocol Buffers** que separa cliente de servidor. Ele entrou no Spark 3.4, ganhou cliente Scala no 3.5 e amadureceu no 4.x. Fonte: [Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html).

O fluxo é este:

1. O cliente (uma biblioteca fina) traduz suas operações de DataFrame em **planos lógicos não resolvidos**.
2. O plano vira protobuf e viaja por gRPC.
3. O servidor (o driver Spark) resolve, otimiza, executa e devolve o resultado em **lotes Apache Arrow**.

### 4.2 Por que isso importa para você

- **Sem JVM no cliente.** Existe o pacote `pyspark-client` (cerca de 1,5 MB, contra centenas de MB do `pyspark` completo). Ele só fala com um servidor remoto. Imagem de CI enxuta, container de app sem Java.
- **Depuração na IDE.** O código do cliente é um processo Python comum: breakpoint no VS Code funciona, sem `spark-submit`.
- **Isolamento.** Seu app não derruba o driver, e o driver pode ser atualizado sem tocar no app.
- **O mesmo script roda local ou remoto** trocando só a URL.
- **É o que a Databricks já usa por baixo** em serverless e no Databricks Connect. Aprender Connect agora é aprender o modo de operação padrão da plataforma comercial.

### 4.3 Subir e conectar

```bash
# descubra onde o pyspark instalou os scripts
export SPARK_HOME=$(uv run python -c 'import pyspark,os;print(os.path.dirname(pyspark.__file__))')
$SPARK_HOME/sbin/start-connect-server.sh    # escuta na porta 15002
```

```bash
uv add "pyspark-client==4.2.0"
```

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
spark.range(5).show()
```

Alternativas de ativação, todas equivalentes: variável `SPARK_REMOTE="sc://localhost"`, flag `--remote "sc://localhost"` no `pyspark`/`spark-shell`, ou a config `spark.api.mode=connect`. Para parar: `$SPARK_HOME/sbin/stop-connect-server.sh`.

### 4.4 Duas correções ao que você vai ler por aí

**"O Connect ainda é experimental."** Não é. No 4.0.0 vieram o cliente Python leve, paridade de API no cliente Java e um tarball com Connect habilitado por padrão. No 4.1.0 o **Spark ML on Connect virou GA** no cliente Python. O 4.2.0 seguiu evoluindo: compatibilidade com API RDD, API `GetStatus` no servidor, liberação de sessão ao encerrar o processo, `head()/take()/tail()` otimizados e integração com o History Server. Ressalva honesta: os release notes **nunca rotulam o framework inteiro como GA**, só subcomponentes. Trate como pronto para os casos suportados.

**"No Spark 4 o Connect virou o default."** Também não. O default continua sendo o **Spark Classic**. No código, `spark.api.mode` é `"connect"` apenas quando a variável de ambiente `SPARK_CONNECT_MODE=1` está setada (é o que o tarball `spark-connect` faz); caso contrário é `"classic"`. Você precisa pedir o Connect explicitamente.

### 4.5 O limite que morde

**RDD e `SparkContext` não existem no Connect.** Nada de `spark.sparkContext.parallelize(...)`, nada de `df._jdf` ou `spark._jsc`. Isso tem consequência direta para a disciplina: os capítulos 1 e 2 dos livros ensinam RDD como porta de entrada. Se você estudar RDD dentro de um notebook serverless da Databricks (que é Connect por baixo), **o exemplo do livro falha**. Estude RDD no Spark Classic local, e DataFrame em qualquer um dos dois.

Há também uma diferença semântica: o Connect **adia a análise** para o momento da execução. `spark.sql("select 1 as a").filter("c > 1")` não estoura na hora no Connect; o erro só aparece quando você chama `df.columns`, `df.schema` ou uma ação. Bom saber antes de escrever um `try/except` que nunca dispara.

---

## 5. Databricks gratuito em 2026

### 5.1 A Community Edition acabou

Isto é o mais importante desta seção, porque o livro e boa parte dos tutoriais mandam você criar uma conta CE. **A Community Edition foi aposentada em 1º de janeiro de 2026.** O anúncio oficial diz literalmente que "After that, Community Edition accounts will no longer be accessible", e um community manager confirmou em 09/01/2026 que já estava fora do ar. A ferramenta de migração de um clique também deixou de funcionar depois do prazo. Fonte: [PSA: Community Edition retires on January 1, 2026](https://community.databricks.com/t5/announcements/psa-community-edition-retires-on-january-1-2026-move-to-the-free/td-p/141888).

A substituta é a **Databricks Free Edition**, e ela é um produto diferente, não uma CE renomeada. A CE era um cluster de brinquedo de 6 GB, sem Unity Catalog e sem jobs. A Free Edition é a plataforma **em modo serverless, com Unity Catalog obrigatório e cotas**. Fonte: [Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition).

Atenção a uma inconsistência da própria Databricks: a página de docs diz "retired in 2025", enquanto o anúncio fixa 1º de janeiro de 2026. A data operacional correta é **01/01/2026**.

### 5.2 O que a Free Edition tem

Verificado na [página oficial de limitações](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations), atualizada em 20/07/2026.

- **Unity Catalog: sim.** A doc declara em "Administrative limitations": *"One workspace and one metastore per account"*. Esse metastore é o metastore do UC. E como a Free Edition lista "All legacy Databricks features" entre os não suportados, o ambiente é **UC puro**: sem `hive_metastore` legado, sem navegador DBFS. Isso está alinhado com a diretriz geral da Databricks de que, a partir de 30/09/2026, todo workspace novo nasce UC-only.
- **Lineage do Unity Catalog: sim.** A doc de lineage não restringe por edição e a página de limitações não lista lineage entre os indisponíveis. Requisitos genéricos: tabelas registradas no metastore UC, consultas via DataFrame ou Databricks SQL, e ao menos privilégio `BROWSE` no catálogo pai. O lineage é capturado **em nível de coluna**, para todas as linguagens suportadas, e no Catalog Explorer é retido indefinidamente (tudo capturado após 01/09/2024). Nas system tables a janela é móvel de 1 ano. Fonte: [Lineage in Unity Catalog](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-lineage).
- **External locations e storage credentials: sim, com ressalva.** A página de limitações **não** lista external locations nem storage credentials como não suportados (o que ela lista é "Custom workspace storage locations", que é outra coisa: o storage raiz do workspace). Uma community manager da Databricks confirmou em 05/08/2025 que *"Databricks Free Edition does support external locations via Unity Catalog"*. Fonte: [tópico na Community](https://community.databricks.com/t5/data-governance/if-use-databricks-free-version-not-free-trail-can-use-external/m-p/127421). A ressalva é séria: por padrão o **acesso de saída à internet é restrito a um conjunto limitado de domínios confiáveis**, o que pode bloquear o alcance efetivo ao seu bucket.
- **Structured Streaming: sim, com trigger restrito.** Ver abaixo, porque é onde o projeto quebra.

### 5.3 A pegadinha dos triggers de streaming

A Free Edition é serverless-only e herda todas as limitações do serverless. Em notebooks e jobs serverless:

- **Suportado:** `Trigger.AvailableNow()` (recomendado) e `Trigger.Once()` (deprecado, mas funciona).
- **Não suportado:** `Trigger.ProcessingTime(interval)` e `Trigger.Continuous(interval)`. A query falha com `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED`.

O detalhe cruel: se você **não** declarar trigger nenhum, o Spark usa `ProcessingTime("0 seconds")` por padrão. Ou seja, **o código de streaming padrão dos livros falha na Free Edition**. Você é obrigada a escrever `.trigger(availableNow=True)` explicitamente. Fonte: [Structured Streaming em serverless](https://docs.databricks.com/aws/en/compute/serverless/streaming).

Na prática: processamento incremental agendado, com latência de minutos, funciona. Stream contínuo de verdade em notebook ou job, não. Contínuo só via Lakeflow pipeline, e na Free Edition você tem "one active pipeline per pipeline type".

### 5.4 Cotas e o que não existe

| Recurso | Limite |
|---|---|
| Compute | **só serverless**, tamanho e uso limitados, sem configuração customizada |
| SQL warehouses | 1, tamanho máximo `2X-Small` |
| Jobs | máximo de 5 tasks concorrentes por conta |
| Lakeflow pipelines | 1 pipeline ativo por tipo |
| Databricks Apps | até 3, e cada app para sozinho após 24h |
| Lakebase | 1 projeto, scale-to-zero |

Não suportado, lista literal: **R e Scala** (só Python e SQL), custom workspace storage locations, online tables, clean rooms, todos os recursos legados e o Knowledge Assistant. No lado administrativo: 1 workspace e 1 metastore, sem console de conta, sem APIs de nível de conta, sem SSO nem SCIM (login por OTP de e-mail, Google ou Microsoft).

Estourou a cota, o compute do workspace é desligado pelo resto do dia - em caso extremo, pelo resto do mês. **Os dados e configurações não são apagados.**

Duas notas práticas. Primeira: existe a **verificação por LinkedIn** na conta ("Verify with LinkedIn"). Ela desbloqueia **outbound internet access** e GPU serverless limitada, sem pagar nada. Se o seu projeto precisa alcançar um bucket externo, faça essa verificação **antes** de concluir que external location não funciona. A doc avisa que a verificação não remove todas as limitações. Segunda: **uso comercial é proibido** e não há SLA nem suporte. Uso acadêmico está ok. O marketing fala em "perpetually free"; a documentação só diz "a no-cost version". Trate a perpetuidade como promessa de marketing.

E o mais importante: **não existe upgrade da Free Edition para a plataforma completa.** A doc é direta: *"If you would like access to the full Databricks platform, you must create a new Databricks account by starting a free trial"*. São produtos distintos.

---

## 6. Plano B para Unity Catalog

O projeto da disciplina exige **Unity Catalog + external locations + lineage**. Antes de comprometer semanas, gaste 15 minutos testando. No workspace da Free Edition:

```sql
SHOW CATALOGS;
SHOW EXTERNAL LOCATIONS;
SHOW STORAGE CREDENTIALS;
```

Depois vá em Catalog, Connect, External Locations, Create external location, opção Manual. Se você não conseguir criar uma storage credential apontando para o seu bucket, você tem a resposta e parte para o plano B. Faça a verificação por LinkedIn antes deste teste.

### Plano B1 - trial de 14 dias na sua própria nuvem (o mais indicado)

- **14 dias de DBUs gratuitas.** No Azure, crie o workspace com o tier "Trial (Premium - 14-Days Free DBUs)".
- Você paga só a **infraestrutura** (VMs, storage, rede). Um lab modesto custa poucos dólares.
- Exige assinatura **pay-as-you-go**. A "Free Trial Subscription" do Azure não serve. Você precisa de role `Contributor` ou `Owner` na assinatura.
- Em troca você ganha a **plataforma completa**: console de conta, UC completo, storage credentials e external locations reais apontando para o **seu** ADLS/S3/GCS, lineage completo, compute clássico, Scala, streaming sem restrição de trigger.

Fonte: [Azure Databricks free trial](https://learn.microsoft.com/en-us/azure/databricks/getting-started/free-trial).

**Estratégia:** desenvolva e teste tudo na Free Edition, que não tem prazo, e queime os 14 dias só na fase que exige external locations e lineage de verdade. Os 14 dias correm em **tempo de calendário**, não de uso. Não ative antes de estar pronta.

### Plano B2 - Unity Catalog OSS

O projeto [unitycatalog/unitycatalog](https://github.com/unitycatalog/unitycatalog) é Apache 2.0 e hoje sandbox project da LF AI & Data Foundation. A release mais recente é a **v0.5.1, de 18/07/2026**.

```bash
git clone https://github.com/unitycatalog/unitycatalog && cd unitycatalog
bin/start-uc-server                 # API em localhost:8080
bin/uc table list --catalog unity --schema default
```

Ele entrega catálogos, schemas, tabelas (Delta, Iceberg, Parquet, JSON, CSV), volumes, funções, e desde a v0.4.0 **storage credentials e external locations** com credential vending temporário para S3. Fala a API do Hive Metastore e a REST Catalog do Iceberg, então pluga em Spark, Trino, DuckDB. Use no mínimo a **0.4.1**, que corrigiu a CVE-2026-27478 (validação de issuer de JWT).

O furo é decisivo para você: **não existe lineage no UC OSS.** Também não há system tables, auditoria nem UI comparável ao Catalog Explorer. Serve para estudar o modelo de governança; **não fecha o requisito do projeto**.

### Comparativo

| Requisito | Free Edition | Trial 14 dias | UC OSS |
|---|---|---|---|
| Unity Catalog | sim (obrigatório) | sim, completo | sim, subconjunto |
| External locations para bucket próprio | provável, **teste antes** | sim | sim (S3) |
| Lineage | sim, valide na conta | sim | **não** |
| Streaming | só `AvailableNow`/`Once` | sem restrição | não se aplica |
| Scala | não | sim | sim |
| Prazo | indefinido | **14 dias corridos** | ilimitado |

---

## 7. Smoke test

Rode na ordem. Cada nível pega uma classe diferente de problema.

### Nível 0 - pré-requisitos

```bash
java -version          # 17, 21 ou 25 (se 25, >= 25.0.3)
echo "$JAVA_HOME"      # não pode estar vazio
python --version       # >= 3.10
```

### Nível 1 - PySpark coerente

```bash
uv run python -c "import sys, pyspark; print(sys.version.split()[0], pyspark.__version__)"
# esperado: 3.12.x 4.2.0
```

### Nível 2 - sessão local sobe, computa e faz shuffle

```bash
uv run python - <<'PY'
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder
         .appName("smoke")
         .master("local[*]")
         .config("spark.sql.shuffle.partitions", "4")
         .getOrCreate())

print("spark version:", spark.version)

df = spark.range(1_000_000)
assert df.count() == 1_000_000
assert df.select(F.sum("id").alias("s")).first().s == 499999500000

# groupBy forca shuffle real: pega erro de diretorio temporario e de rede
g = df.groupBy((F.col("id") % 7).alias("k")).count()
assert g.count() == 7

spark.stop()
print("OK: local[*] funcional")
PY
```

O `groupBy` é o ponto importante. Um `count()` simples não força shuffle e passa mesmo com o ambiente meio quebrado.

### Nível 3 - SQL, Arrow e ida e volta com pandas

```bash
uv run python - <<'PY'
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()

spark.sql("SELECT 1 AS a, 'x' AS b").createOrReplaceTempView("t")
assert spark.sql("SELECT count(*) AS c FROM t").first().c == 1

pdf = spark.range(100).toPandas()      # exercita o pyarrow
assert len(pdf) == 100

import pyarrow, pandas
print("pyarrow:", pyarrow.__version__, "| pandas:", pandas.__version__)
spark.stop()
print("OK: SQL + Arrow")
PY
```

### Nível 4 - escrita e leitura em disco

```bash
uv run python - <<'PY'
import tempfile, os
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").getOrCreate()
p = os.path.join(tempfile.mkdtemp(), "out.parquet")
spark.range(1000).write.mode("overwrite").parquet(p)
assert spark.read.parquet(p).count() == 1000
spark.stop()
print("OK: I/O parquet")
PY
```

### Nível 5 - spark-submit e o exemplo canônico

```bash
uv run spark-submit --master "local[*]" \
  --class org.apache.spark.examples.SparkPi \
  "$(uv run python -c 'import pyspark,os;print(os.path.dirname(pyspark.__file__))')"/examples/jars/spark-examples_2.13-*.jar 10
```

Deve imprimir `Pi is roughly 3.14...`. Repare no `_2.13` no nome do JAR: é a prova visual de que o Scala 2.12 saiu de cena.

### Nível 6 - Spark Connect

```bash
# terminal 1
$SPARK_HOME/sbin/start-connect-server.sh
```

```bash
# terminal 2
uv run python - <<'PY'
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
assert spark.range(10).count() == 10

is_connect = hasattr(spark, "client") or not hasattr(spark, "_jsc")
assert is_connect, "caiu em Spark Classic: a URL remote não pegou"
print("modo Connect confirmado")

# prova da análise adiada
df = spark.sql("SELECT 1 AS a").filter("coluna_inexistente > 1")
print("plano não resolvido criado sem erro")
try:
    df.columns          # aqui sim a análise vai ao servidor
    print("ERRO: deveria ter falhado")
except Exception as e:
    print("OK: análise adiada confirmada ->", type(e).__name__)
spark.stop()
PY
```

Este último teste é o mais didático de todos: ele prova que você está no Connect e te acostuma com a diferença de semântica antes que ela apareça em produção.

### Nível 7 - Databricks Free Edition (rode num notebook)

```python
print(spark.version)
print("catalogo atual:", spark.sql("SELECT current_catalog()").first()[0])
display(spark.sql("SHOW CATALOGS"))

# serverless e sempre Spark Connect
print("Connect:", hasattr(spark, "client") or not hasattr(spark, "_jsc"))

# RDD deve FALHAR: comportamento esperado, não bug
try:
    spark.sparkContext.parallelize([1, 2, 3]).collect()
    print("inesperado: RDD funcionou")
except Exception as e:
    print("OK: RDD indisponivel (esperado) ->", type(e).__name__)
```

E o teste de trigger, que é onde o projeto pode quebrar:

```python
# ERRADO na Free Edition: sem trigger explicito o default e
# ProcessingTime("0 seconds") -> INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED

q = (spark.readStream.table("origem")
     .writeStream
     .trigger(availableNow=True)                 # obrigatorio em serverless
     .option("checkpointLocation", "/Volumes/cat/sch/vol/ckpt")
     .toTable("destino"))
q.awaitTermination()
```

---

## 8. Resumo operacional

1. **Local:** Temurin 21 + Python 3.12 + `uv add "pyspark[sql,connect]==4.2.0"`. Rode `local[*]` com `spark.sql.shuffle.partitions` baixo e `spark.driver.memory` ajustado (o `executor.memory` é ignorado).
2. **Docker:** `apache/spark:4.2.0-scala2.13-java21-python3-r-ubuntu`. Nunca `apache/spark-py`, morto desde 2023.
3. **Connect:** use desde já, mas saiba que o default continua Classic e que RDD e `SparkContext` não existem por lá.
4. **Databricks:** Community Edition morreu em 01/01/2026. Free Edition tem UC obrigatório e lineage, só Python e SQL, só serverless, e streaming só com `Trigger.AvailableNow()`.
5. **Projeto com UC + external locations + lineage:** teste a Free Edition em 15 minutos, com verificação LinkedIn feita. Se travar, use o trial de 14 dias em nuvem própria e reserve os dias para a fase final. UC OSS não serve, porque não tem lineage.

---

## Fontes

- [Apache Spark - Downloads](https://spark.apache.org/downloads.html)
- [Apache Spark - News](https://spark.apache.org/news/)
- [Spark Release 4.2.0](https://spark.apache.org/releases/spark-release-4-2-0.html)
- [Spark 4.2.0 - Overview e requisitos](https://spark.apache.org/docs/latest/)
- [PySpark Installation](https://spark.apache.org/docs/latest/api/python/getting_started/install.html)
- [Spark Connect Overview](https://spark.apache.org/docs/latest/spark-connect-overview.html)
- [Application development with Spark Connect](https://spark.apache.org/docs/latest/app-dev-spark-connect.html)
- [PyPI - pyspark](https://pypi.org/project/pyspark/) e [pyspark-client](https://pypi.org/project/pyspark-client/)
- [Docker Hub - apache/spark](https://hub.docker.com/r/apache/spark)
- [PSA: Community Edition retires on January 1, 2026](https://community.databricks.com/t5/announcements/psa-community-edition-retires-on-january-1-2026-move-to-the-free/td-p/141888)
- [Databricks Free Edition](https://docs.databricks.com/aws/en/getting-started/free-edition)
- [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)
- [Structured Streaming em compute serverless](https://docs.databricks.com/aws/en/compute/serverless/streaming)
- [Lineage in Unity Catalog](https://docs.databricks.com/aws/en/data-governance/unity-catalog/data-lineage)
- [External locations na Free Edition (Databricks Community)](https://community.databricks.com/t5/data-governance/if-use-databricks-free-version-not-free-trail-can-use-external/m-p/127421)
- [Azure Databricks free trial](https://learn.microsoft.com/en-us/azure/databricks/getting-started/free-trial)
- [Unity Catalog OSS - releases](https://github.com/unitycatalog/unitycatalog/releases)
