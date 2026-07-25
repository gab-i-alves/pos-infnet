---
title: "Aula 01 de Spark - Pré-aula"
aula: "Aula 01 - Arquitetura unificada do Spark, processamento em larga escala e preparação do ambiente"
data: 2026-07-24
tags:
  - spark
  - pos-infnet
  - aula-01
  - pre-aula
  - bibliografia
---

# Aula 01 · Pré-aula

Primeira etapa: a leitura que o professor passou, lida antes de qualquer aprofundamento. A ordem importa. Os capítulos descrevem o Spark 3.0 e o [aprofundamento](02-aprofundamento.md) existe para marcar o que mudou até o 4.2.0; ler o aprofundamento primeiro apaga a fronteira entre o que é tese do livro e o que é correção de 2026, e é justamente essa fronteira que rende pergunta boa na aula.

## O que foi passado

| Obra | Capítulos | Versão que o texto cobre |
|---|---|---|
| Luu, Hien. *Beginning Apache Spark 3*. Apress, 2021 | 1 e 2 | Spark 3.0 / 3.1 |
| Damji, Jules et al. *Learning Spark*, 2ª edição. O'Reilly, 2020 | 1 e o início do 2 | Spark 3.0 |

Nada de material licenciado é reproduzido aqui: este documento registra a leitura, não o texto.

## Como ler

Cerca de uma hora e meia de leitura corrida. Não pare para pesquisar cada termo desconhecido, isso é trabalho da etapa seguinte. Pare para anotar duas coisas apenas:

1. **Onde eu não acreditei.** Número redondo demais, afirmação sem fonte, benchmark sem condição declarada.
2. **Onde eu não entendi.** Distinguir "não entendi o mecanismo" de "não entendi o vocabulário", porque as duas coisas se resolvem de formas diferentes.

## Teses dos capítulos que valem marcação

Marque a página quando cruzar com estas afirmações. Todas reaparecem no aprofundamento, algumas confirmadas, outras não.

- [ ] O ganho de **100x sobre o MapReduce**, atribuído a processamento em memória.
- [ ] O **motor unificado**: batch, streaming, SQL, ML e grafos na mesma engine.
- [ ] **RDD como API de baixo nível** que raramente se usa hoje.
- [ ] `local[*]`, **client mode** e **cluster mode** apresentados como variações de invocação.
- [ ] **AQE** apresentado como algo que se liga manualmente.
- [ ] **Delta Lake** como o formato transacional natural do ecossistema.
- [ ] A **Databricks Community Edition** como ambiente gratuito recomendado para estudar.
- [ ] Requisitos de ambiente: versões de **Java, Scala e Python**.

Os três últimos itens estão factualmente desatualizados, não apenas incompletos. Vale conferir antes de gastar tempo montando ambiente pelo livro.

## Registro de leitura

### Luu, capítulo 1

### Luu, capítulo 2

### Damji, capítulo 1

### Damji, capítulo 2 (início)

## Onde eu não acreditei

Lista das afirmações que soaram propaganda, exageradas ou datadas. Cada linha aqui é matéria-prima de pergunta para a aula.

## Vocabulário novo

Termos que apareceram sem definição suficiente e precisam ser resolvidos no aprofundamento.

## O que fica para o aprofundamento

Perguntas que os capítulos abriram e não fecharam.
