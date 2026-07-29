---
title: "Aula 01 de Spark - Pós-aula"
aula: "Aula 01 - Arquitetura unificada do Spark, processamento em larga escala e preparação do ambiente"
data: 2026-07-24
tags:
  - spark
  - pos-infnet
  - aula-01
  - pos-aula
  - nota-fiscal
---

# Aula 01 · Pós-aula

Quarta etapa, a única que deixa prova. As três primeiras rendem material: 55 dúvidas, sete divergências, um estudo de cinco partes e as anotações da aula. Nenhuma delas rende artefato, e sem algo que um terceiro consiga rodar o ciclo termina em leitura. Este documento registra a nota fiscal desta aula, e fecha a cadeia que começa em [01-pre-aula.md](01-pre-aula.md), passa por [02-aprofundamento.md](02-aprofundamento.md) e por [03-aula.md](03-aula.md).

## Candidatos a artefato

Levantados no [aprofundamento](02-aprofundamento.md) e no [gabarito da leitura](01-gabarito.md), com o custo estimado. O que a aula acrescentar entra depois, vindo de [O que virou candidato a nota fiscal](03-aula.md#o-que-virou-candidato-a-nota-fiscal).

| Candidato | O que prova | Custo |
|---|---|---|
| **Notebook do custo fictício de 4 MB** | reproduz a fórmula de particionamento na leitura: gera N arquivos pequenos, conta as tasks, sobe `maxPartitionBytes` e `openCostInBytes`, conta de novo, aplica `coalesce` depois da leitura e mostra que o custo de entrada não muda | baixo, cerca de 2h |
| **Onde a preguiça quebra** | conta quantos jobs cada leitura dispara antes de qualquer ação (`read.parquet`, `read.json` sem schema, `read.csv` com `inferSchema`, `df.columns`) e repete tudo com schema explícito, medindo que fatia do tempo é só inferência. Fecha a dúvida [50], em que o Damji 2 chama `read()` de transformação contra a própria definição de preguiça | baixo, reusa a fixture de arquivos pequenos do primeiro |
| Comparação UDF Python x pandas UDF x expressão nativa | mede as três no mesmo dado e mostra a fronteira de codegen no plano (`ArrowEvalPython` contra o nó nativo). Falseia com número a afirmação do Damji 1, dúvida [32], de que Python, R e Java geram bytecode idêntico e a mesma performance | médio |
| Prova de que o DPP disparou | monta um esquema estrela mínimo, roda a query com as três condições de ativação satisfeitas e depois quebra uma delas, mostrando o filtro dinâmico aparecer e sumir no `FileScan` da fato, com o número de arquivos lidos caindo junto | baixo |
| Auditoria do M&M do Damji 2 | roda o `mnm_dataset.csv` do repositório do livro e confronta com as duas saídas impressas no capítulo: mede a razão de 55,9 entre a saída Python e a Scala e mostra que a saída Scala impressa não pode ter vindo do código Scala impresso (dúvidas [44], [47] e [51]). Prova a auditoria da leitura, não o motor | baixo |
| Leitura anotada de um `explain()` | explica o plano de uma query com join e agregação nó por nó, provando pushdown, pruning e estratégia de join. Enfraqueceu: a seção 5 da Parte 3 do aprofundamento já faz isso, então seria transcrever trabalho pronto | baixo |
| Smoke test do ambiente documentado | roteiro reproduzível dos níveis 0 a 7, incluindo o que a Free Edition não faz. Prova que a máquina está de pé, não algo que eu não sabia, e o roteiro já está escrito na Parte 4 do aprofundamento | baixo |

O primeiro continua o mais forte: reproduz um número que os quatro capítulos não trazem, e a lacuna está registrada, porque o pré-aula lista "como o número de partições é decidido na leitura" entre o que o vocabulário dos livros deixou em aberto. Cabe numa tarde e o resultado é verificável por qualquer pessoa que rode o notebook. O segundo é o melhor complemento, porque sai quase de graça: usa os mesmos arquivos gerados pelo primeiro.

## Artefato escolhido

**O quê:**

**Onde vive:**

**Critério de pronto:** um terceiro consegue rodar do zero e chegar ao mesmo número, sem precisar perguntar nada.

## O que ele prova

**O número que saiu:**

**Em uma frase:** o que eu sei agora e não sabia antes da aula.

**Onde ele contrariou a hipótese:** o que eu esperava medir e não bateu. Se nada contrariou, o artefato foi confirmação, e vale dizer isso com todas as letras.

## O que ficou de fora

Escopo cortado de propósito, para não transformar a nota fiscal em projeto.

## Retrospectiva das quatro etapas

| Etapa | Previsto | Real | O que rendeu | O que eu faria diferente |
|---|---|---|---|---|
| pre | | | | |
| deep | | | | |
| live | | | | |
| proof | | | | |

O que ensina é a diferença entre previsto e real, e o que rendeu ao lado do que custou. "O que rendeu" se preenche com número quando dá: o `pre` rendeu 55 dúvidas, sete divergências e três perguntas de aula.

## Emenda para a aula seguinte

O que este ciclo ensinou sobre o método, não sobre o Spark. Cada emenda diz o que muda na aula 02, no lugar de quê, e como vou saber que apliquei. Conselho que não dá para checar na aula seguinte não é emenda, é desabafo.

**Emendas:**

**Para onde elas vão:** copiar para o topo do [guia de leitura da aula 02](../aula-02/01-pre-aula.md). Emenda que não viaja não corrige nada.
