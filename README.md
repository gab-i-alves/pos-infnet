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

### Guia de leitura

As etapas **pre** e **deep** produzem um artefato só: um guia de leitura por capítulo de livro, e não um resumo por aula. Indexar pela fonte importa, porque uma aula costuma cobrir capítulos de livros diferentes, e um capítulo costuma sobreviver à aula que o pediu.

O guia é uma lista de perguntas escritas **antes** da leitura, respondidas por escrito **depois**. A ordem é o método inteiro: ser questionada antes de ler melhora a retenção mesmo quando o palpite sai errado, e puxar da memória fixa mais do que reler. Um resumo pronto para consumir não faz nenhuma das duas coisas.

As perguntas são escritas em português, e os termos técnicos ficam em inglês dentro delas: `partition`, `shuffle`, `lazy evaluation`, `DataFrame`. É o vocabulário que precisa ser fixado, e é ele que permite varrer a página do livro procurando as palavras da pergunta. O resto da frase em português custa menos esforço de leitura e não atrapalha essa varredura.

Os ponteiros de seção também ficam em inglês, porque nomeiam seções reais do livro e servem para achá-las no sumário.

Cada guia tem cinco níveis, ordenados por nível e não por ordem de leitura, para que a base venha inteira primeiro:

| Nível | O que cobra |
|---|---|
| **L1** | recall e definições |
| **L2** | compreensão, explicar com as próprias palavras |
| **L3** | aplicação, raciocinar sobre uma situação concreta |
| **L4** | análise e síntese, conectar seções que o autor manteve separadas |
| **L5** | além do capítulo: o que envelheceu, o que o autor omitiu |

Quando dois livros cobrem o mesmo assunto, entra um **L6** de comparação entre eles. É onde as divergências aparecem, e onde dá para ver qual autor errou uma data ou uma versão.

Três regras que o formato impõe:

- **Toda pergunta de L1 a L4 precisa ter resposta no capítulo.** Conceito que o texto apenas nomeia sem definir vira item de L5, não pergunta de L1. Isso tira de mim a decisão sobre onde termina a leitura e onde começa o aprofundamento: a fronteira vira consequência do que o capítulo entrega.
- **Item de L5 vai para o backlog, nunca para as notas como fato.** Livro de 2020 ou 2021 erra versão, número de porta e nome de produto. Cada item de L5 é verificado contra documentação oficial, nota de release ou código-fonte, com URL e data de acesso registradas no próprio guia.
- **Cada pergunta dos Níveis 1 a 3 traz o ponteiro da seção** entre parênteses, o que faz o guia servir também de índice do capítulo. Nos Níveis 4 a 6 o ponteiro é opcional, porque ali a pergunta cruza seções distantes por definição e um ponteiro único não a localiza.

No fim vem uma lista de termos-chave para definir sem consultar. Termo que eu não consigo definir é alvo de releitura, não item de L5.

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

Um guia de leitura por capítulo de livro, dentro da pasta da aula que pediu aquela leitura.

```
NN-disciplina/
  README.md                    índice da disciplina e tracker das aulas
  aula-NN/
    <livro>-ch<NN>.md          um guia por capítulo de livro
  notebooks/                   código e experimentos
```

O nome do arquivo é o slug do livro mais o capítulo com dois dígitos, sem prefixo, para ordenar por livro na listagem. Slugs em uso:

| Livro | Slug |
|---|---|
| Luu, *Beginning Apache Spark 3* (Apress, 2021) | `beginning-spark3` |
| Damji et al., *Learning Spark, 2nd Edition* (O'Reilly, 2020) | `learning-spark-2ed` |
| Chadha, *Data Engineering with Databricks Cookbook* (Packt) | `databricks-cookbook` |

As pastas de aula são criadas quando a disciplina começa. Os documentos são markdown puro, sem dependência de ferramenta, então servem tanto para leitura direta no GitHub quanto para importação em qualquer sistema de notas.

A nota fiscal de cada aula, quando existir, mora na mesma pasta com nome próprio.

---

## Licença

Anotações de estudo, livres para uso e adaptação. As fontes citadas pertencem a seus autores; nada de material licenciado (O'Reilly e afins) é reproduzido aqui, apenas referenciado.
