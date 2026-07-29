# Aula 02 · Transformação e persistência SQL

Spark SQL: a conversão de RDD em DataFrame, as APIs estruturadas e a persistência de consultas, tabelas e resultados em storage.

A bibliografia desta aula tem **cinco leituras em três livros**, uma leitura a mais que a aula 01 e um livro novo. Luu e Damji continuam cobrindo o Spark 3.0; o Chadha é um livro de receitas sobre Databricks, mais recente, e serve de contraponto. A versão de referência do aprofundamento continua sendo o **Spark 4.2.0**.

| Etapa | Documento | Estado |
|---|---|---|
| pre | [01-pre-aula.md](01-pre-aula.md) | pronto, 110 dúvidas, doze divergências e o bloco Spark contra Databricks |
| deep | [02-aprofundamento.md](02-aprofundamento.md) | pronto, cinco partes |
| live | [03-aula.md](03-aula.md) | 15 perguntas preparadas, anotações a fazer |
| proof | [04-pos-aula.md](04-pos-aula.md) | 8 candidatos, artefato a escolher |

Esta é a aula que paga duas dívidas da aula 01: o **Catalyst**, que os quatro capítulos anteriores só nomearam, uma linha cada, e o **papel do RDD**, sobre o qual os dois livros discordaram e o Damji discordou de si mesmo entre os capítulos 1 e 2.

Ordem de leitura sugerida no pré-aula: **1, 2, 4, 5, 3**, agrupando cada livro consigo mesmo e deixando o Chadha por último como teste de realidade. A ordem do professor é 1, 2, 3, 4, 5.
