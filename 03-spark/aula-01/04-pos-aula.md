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

Quarta etapa, a única que conta. Pré-aula, aprofundamento e aula são consumo; sem um artefato que prove o aprendizado, as três não viraram nada. Este documento registra a nota fiscal desta aula.

## Candidatos a artefato

Levantados durante o aprofundamento e a aula, com o custo estimado.

| Candidato | O que prova | Custo |
|---|---|---|
| **Notebook do custo fictício de 4 MB** | reproduz a fórmula de particionamento na leitura: gera N arquivos pequenos, conta as tasks, sobe `maxPartitionBytes` e `openCostInBytes`, conta de novo, aplica `coalesce` depois da leitura e mostra que o custo de entrada não muda | baixo, cerca de 2h |
| Leitura anotada de um `explain()` | pega uma query com join e agregação e explica o plano nó por nó, provando pushdown, pruning e a estratégia de join escolhida | baixo |
| Comparação UDF Python x pandas UDF x expressão nativa | mede as três no mesmo dado e mostra a fronteira de codegen no plano (`ArrowEvalPython` contra o nó nativo) | médio |
| Smoke test do ambiente documentado | roteiro reproduzível dos níveis 0 a 7, incluindo o que a Free Edition não faz | baixo, mas prova menos |

O primeiro é o mais forte: reproduz um número que os capítulos não trazem, cabe numa tarde e o resultado é verificável por qualquer pessoa que rode o notebook.

## Artefato escolhido

**O quê:**

**Onde vive:**

**Critério de pronto:** um terceiro consegue rodar do zero e chegar ao mesmo número, sem precisar perguntar nada.

## O que ele prova

Em uma frase, o que eu sei agora e não sabia antes da aula.

## O que ficou de fora

Escopo cortado de propósito, para não transformar a nota fiscal em projeto.

## Retrospectiva das quatro etapas

| Etapa | Feita em | Tempo real | O que eu faria diferente |
|---|---|---|---|
| pre | | | |
| deep | | | |
| live | | | |
| proof | | | |

## Emenda para a aula seguinte

O que este ciclo ensinou sobre o método, não sobre o Spark.
