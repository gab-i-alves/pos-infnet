---
title: "Aula 02 de Spark - Pós-aula"
aula: "Aula 02 - Transformação e persistência SQL"
data:
tags:
  - pos-infnet
  - aula-02
  - pos-aula
  - spark
  - nota-fiscal
---

# Aula 02 · Pós-aula

Quarta etapa, a única que deixa prova. As duas primeiras desta aula já renderam material em quantidade: 110 dúvidas prefixadas por leitura, doze divergências entre os livros, um bloco novo sobre Spark contra Databricks e cinco partes de aprofundamento com fonte primária citada. A terceira vai render as anotações da aula ao vivo, que ainda não aconteceu. Nada disso é artefato. Enquanto um terceiro não conseguir rodar algo e chegar ao mesmo resultado, o ciclo terminou em leitura, por mais cara que a leitura tenha sido.

Esta aula facilita a escolha, porque ela produziu um tipo de achado que a aula 01 não tinha: **pergunta que só se resolve com dez linhas de código**. A nota de confiabilidade do aprofundamento registra duas divergências entre a documentação oficial e o código do 4.2.0 que ficaram sem árbitro, por falta de PySpark na máquina onde a pesquisa rodou, e manda resolvê-las rodando. Ao lado delas há um número que o aprofundamento declara como estimativa por decomposição, não medição, e um erro factual sobre layout de Parquet que se confronta abrindo um arquivo. Três formas diferentes de fechar dívida, e as três cabem em código.

O documento registra a nota fiscal desta aula e fecha a cadeia que começa em [01-pre-aula.md](01-pre-aula.md), passa por [02-aprofundamento.md](02-aprofundamento.md) e por [03-aula.md](03-aula.md).

## Candidatos a artefato

Levantados no [aprofundamento](02-aprofundamento.md) e no [registro de leitura](01-pre-aula.md), com o custo estimado. Cada linha nasce de um achado com endereço: o código da dúvida, o número da divergência ou a parte citada. O que a aula acrescentar entra depois, vindo de [O que virou candidato a nota fiscal](03-aula.md#o-que-virou-candidato-a-nota-fiscal).

| Candidato | O que prova | Custo |
|---|---|---|
| **Árbitro das duas divergências entre documentação e código** | resolve por execução as duas perguntas que a [Nota de confiabilidade](02-aprofundamento.md#nota-de-confiabilidade) deixou abertas: se linha de CSV com número errado de campos conta como registro corrompido (a doc diz que não, o `UnivocityParser` sugere que sim) e o que `PERMISSIVE` faz sem a coluna de corrompido no schema (a doc diz que descarta, o `FailureSafeParser` sugere resultado parcial). Duas perguntas da Parte 2, itens 1 e 2, mais as dúvidas [R3-6], [R3-7], [R4-16] e [R5-9]. Se o código ganhar, muda a recomendação de modo para ingestão de CSV externo | baixo, menos de uma hora: dois arquivos de dez linhas e quatro leituras |
| **Prova de que `nullable=false` não valida nada** | declara `NOT NULL` em DDL, escreve nulo no arquivo, lê de volta e conta. Fecha a pergunta 5 da Parte 2 e a [divergência 11](01-pre-aula.md#divergências-entre-os-livros), em que o Damji imprime `nullable = false` em memória e `true` no mesmo schema lido de JSON e afirma que as saídas não diferem ([R2-5]). Se o nulo passar, o benefício "detectar erro cedo" não existe para nulo, só para tipo e nome de coluna | baixo, cerca de uma hora, e reusa a fixture do árbitro |
| **Cronômetro da inferência de schema** | mede o que o aprofundamento só estimou. A Parte 2, seção 1, declara em linha que a tabela de custo é "estimativa minha, por decomposição, não medição". O artefato conta os jobs com `statusTracker` (CSV inferido: dois; com schema: zero; Parquet: um de uma task) e cronometra a fração do tempo que é só inferência. Fecha a [divergência 4](01-pre-aula.md#divergências-entre-os-livros) e as dúvidas [R4-6], [R4-14] e [R3-13], que confundem varrer dado com ler metadado | médio, uma tarde inteira: gerar alguns GB de CSV e o mesmo dado em Parquet custa mais tempo que medir |
| **Rodapé de um Parquet real, aberto na mão** | conta os arquivos, os grupos de linhas e os pedaços de coluna de uma escrita do próprio Spark, mede o rodapé em bytes e mostra o campo `file_path` do `ColumnChunk` vazio. Derruba a frase do Luu de que Parquet guarda cada coluna num arquivo separado ([divergência 8](01-pre-aula.md#divergências-entre-os-livros), [R4-17]) e responde a pergunta 1 da Parte 3, que separa "o livro errou" de "o livro citou uma possibilidade da especificação". O tamanho medido do rodapé é o termo que falta na razão de 10^5 da Parte 2 | baixo, cerca de uma hora, e reusa os arquivos gerados pelo cronômetro |
| **Bancada de três UDFs** | mede `@udf`, `@pandas_udf` e expressão nativa no mesmo dado e mostra o nó do plano em cada caso. Testa a hipótese da pergunta 3 da Parte 4, de que o Arrow por padrão no 4.2 encurtou a distância entre `@udf` e `@pandas_udf` sem zerá-la, porque mata a serialização por linha e não a chamada por linha. Testa também a pergunta 4: se `BatchEvalPython` em 4.2 só aparece com a config desligada ou com UDT. Falseia com número a paridade entre linguagens que o Damji afirma e enfraquece na linha seguinte ([R2-9]) | médio, meia tarde a uma tarde, e exige pandas 2.2.0 e PyArrow 18.0.0 |
| **A linha do Damji que não roda** | roda os dois "recommended usage patterns" do `DataFrameWriter` como estão impressos e registra as duas recusas: `bucketBy(...).save(path)` e `sortBy(...)` sem `bucketBy`. Depois troca `save` por `saveAsTable` e mostra a primeira funcionar. É o achado que o pré-aula chama de mais forte da leitura ([R5-16]), porque é a assinatura que se copia. Enfraqueceu como nota fiscal: a Parte 3, seção 3.3, já confirmou pela documentação, então o que sobra de novo é o texto exato do erro | baixo, meia hora, e é confirmação, não descoberta |
| **Leitura anotada de um `explain()`** | lê um plano de ponta a ponta, campo por campo: `*(n)`, `Exchange`, `PushedFilters`, `PartitionFilters`, `ReadSchema`, de onde vem o `200` e por que `AdaptiveSparkPlan isFinalPlan` muda entre a primeira e a segunda chamada. Enfraqueceu pelo mesmo motivo da aula 01: a Parte 1, seção 8, já faz isso nó por nó. O que ainda é novo é pequeno e honesto: o plano da seção 8.2 foi reconstruído, não executado, então rodar mostra onde a reconstrução errou | baixo, cerca de duas horas, e transcreve trabalho pronto |
| **Custo de leitura de JDBC, serial contra particionada** | mede o que a bibliografia inteira não cobre ([divergência 10](01-pre-aula.md#divergências-entre-os-livros), [R4-21]): `getNumPartitions` igual a 1 na receita do Luu, contra N com `partitionColumn`, e o desequilíbrio que aparece quando `lowerBound` e `upperBound` são palpite, que o AQE não conserta porque nasce antes do primeiro shuffle | **alto, e não cabe numa tarde**. Exige banco em container, tabela semeada com volume que valha medir, e ainda assim a latência quase nula da rede local esconde o ganho de `fetchsize`, que é metade do argumento |

**O mais forte é o árbitro, e por um critério só: é o único em que ninguém sabe a resposta.** Os outros medem o tamanho de algo que já entendi ou confirmam texto de documentação; este resolve uma pergunta que o próprio aprofundamento registrou como aberta, com as duas hipóteses escritas e a instrução de rodar antes da aula. Custa menos de uma hora, qualquer pessoa reproduz com dois arquivos de dez linhas, e o resultado muda uma recomendação de produção: se o código ganhar, `DROPMALFORMED` sobre CSV apaga linha que a documentação garante que ele preserva. O melhor complemento barato é a prova de `nullable`, porque sai da mesma fixture: o mesmo arquivo sujo, o mesmo schema em DDL com `NOT NULL` e `_corrupt_record` dentro, a mesma sessão, três leituras a mais. Se o que eu quiser for número em vez de veredito, o par é outro: o cronômetro da inferência com o rodapé do Parquet, que reusa os arquivos gerados pelo primeiro e fecha a razão que a Parte 2 deixou estimada.

## Artefato escolhido

**O quê:**

**Onde vive:**

**Critério de pronto:** um terceiro consegue rodar do zero e chegar ao mesmo resultado, sem precisar perguntar nada. Onde o resultado é um veredito e não um número, o artefato imprime a evidência bruta ao lado da conclusão, para que quem discordar tenha o que contestar.

## O que ele prova

Preencher depois de rodar o artefato, nunca antes.

**O número que saiu:**

**Em uma frase:** o que eu sei agora e não sabia antes da aula.

**Onde ele contrariou a hipótese:** o que eu esperava medir e não bateu. As duas hipóteses estão escritas nas perguntas 1 e 2 da [Parte 2](02-aprofundamento.md#perguntas-que-a-parte-2-abriu), e as duas apostam no código contra a documentação. Se nada contrariou, o artefato foi confirmação, e vale dizer isso com todas as letras.

## O que ficou de fora

Escopo cortado de propósito, para não transformar a nota fiscal em projeto. Preencher depois de fechar o artefato, com o que eu tive vontade de acrescentar e não acrescentei.

## Retrospectiva das quatro etapas

| Etapa | Previsto | Real | O que rendeu | O que eu faria diferente |
|---|---|---|---|---|
| pre | | | 110 dúvidas (R1 com 10, R2 com 23, R4 com 24, R5 com 23, R3 com 30), doze divergências e o bloco novo sobre Spark contra Databricks | |
| deep | | | cinco partes, 464 links de fonte primária e 39 perguntas abertas com hipótese | |
| live | | | | |
| proof | | | | |

O que ensina é a diferença entre previsto e real, e o que rendeu ao lado do que custou. "O que rendeu" se preenche com número quando dá, e as duas primeiras linhas já estão preenchidas porque as duas etapas aconteceram. As colunas de previsto e real se preenchem com tempo, e a última coluna só depois de fechar a etapa `proof`.

## Emenda para a aula seguinte

O que este ciclo ensinou sobre o método, não sobre o Spark. Cada emenda diz o que muda na aula 03, no lugar de quê, e como vou saber que apliquei. Conselho que não dá para checar na aula seguinte não é emenda, é desabafo.

**Emendas:**

**Para onde elas vão:** copiar para o topo do [`01-pre-aula.md` da aula 03](../aula-03/01-pre-aula.md), na seção que o esqueleto já reserva para a emenda da aula anterior. Emenda que não viaja não corrige nada.
