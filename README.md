# MBA em Engenharia de Dados

Caderno de estudo do MBA em Engenharia de Dados, Big Data e IA na Faculdade Infnet, de julho de 2026 a julho de 2027. Oito disciplinas, sessenta e quatro aulas.

Este repositório é o caderno bruto: anotações, aprofundamentos, exercícios e código de estudo. A versão curada e o acompanhamento de progresso ficam em [gabi-alves.com/curriculum](https://gabi-alves.com/curriculum).

Os Projetos de Disciplina, que são as entregas avaliadas, não ficam aqui: são de autoria integral minha e seguem as regras de cada disciplina.

---

## Método

Cada aula passa por quatro etapas. As três primeiras são consumo; só a quarta prova que aprendi.

| Etapa | O que é |
|---|---|
| **pre** | a leitura que o professor passou |
| **deep** | aprofundamento por conta própria, além da bibliografia da aula |
| **live** | a aula ao vivo, com perguntas preparadas de antemão |
| **proof** | a nota fiscal: um artefato que comprova o aprendizado |

A "nota fiscal" é o compromisso central. Pode ser um post, um mini projeto, exercícios resolvidos, flashcards ou um notebook comentado. Sem ela, as outras três etapas não contam.

---

## Disciplinas

| # | Disciplina | Estado |
|---|---|---|
| 01 | [SQL Avançado para Engenharia de Dados](01-sql/) | a abrir |
| 02 | [Transformação e Qualidade de Dados com dbt](02-dbt/) | a abrir |
| 03 | [Processamento de Big Data com Apache Spark e Spark SQL](03-spark/) | **em curso** |
| 04 | [Implementação de Data Lakehouse com Databricks e Delta Lake](04-databricks/) | a abrir |
| 05 | [ELT com AWS Glue Studio](05-glue/) | a abrir |
| 06 | [Orquestração de Pipelines de Dados com AWS Step Functions](06-step-functions/) | a abrir |
| 07 | [Data Streaming e Processamento em Tempo Real com Kafka](07-kafka/) | a abrir |
| 08 | [Engenharia de Dados para MLOps e Inteligência Artificial](08-mlops/) | a abrir |

---

## Como está organizado

Um documento por etapa, sempre os mesmos quatro, sempre com os mesmos nomes.

```
NN-disciplina/
  README.md              índice e estado da disciplina
  aula-NN/
    README.md            índice da aula e estado das quatro etapas
    01-pre-aula.md       a leitura do professor
    02-aprofundamento.md o estudo por conta própria
    03-aula.md           perguntas preparadas e anotações da aula ao vivo
    04-pos-aula.md       a nota fiscal
  notebooks/             código e experimentos
```

As pastas de aula são criadas quando a disciplina começa. Os documentos são markdown com frontmatter (`title`, `aula`, `data`, `tags`), então servem tanto para leitura direta no GitHub quanto para importação em qualquer sistema de notas.

---

## Licença

Anotações de estudo, livres para uso e adaptação. As fontes citadas pertencem a seus autores; nada de material licenciado (O'Reilly e afins) é reproduzido aqui, apenas referenciado.
