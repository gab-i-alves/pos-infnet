---
title: "Aula 01 de Spark - Guia de leitura"
aula: "Aula 01 - Arquitetura unificada do Spark, processamento em larga escala e preparação do ambiente"
data: 2026-07-24
tags:
  - spark
  - pos-infnet
  - aula-01
  - pre-aula
  - guia-de-leitura
---

# Aula 01 · Guia de leitura

Quarenta perguntas sobre os quatro capítulos. **Todas têm resposta presente na leitura.** As perguntas estão em inglês porque os livros estão em inglês, e assim dá para varrer a página procurando as palavras da pergunta.

Responda em `**A:**`, com as suas palavras. Copiar a frase do livro não serve. Faça um nível por vez, de L1 a L5: a base primeiro. Onde travar, escreva o que conseguiu, marque e siga, porque o que ficou pela metade é a pauta do [aprofundamento](02-aprofundamento.md) e da [aula](03-aula.md).

O [gabarito](01-gabarito.md) não se abre antes de tentar.

## L1 · Vocabulary and concepts

Escreva a definição com as suas palavras e confira no ponteiro; se já conhecia o termo, anote antes o que você achava que era.

#### L1.1 · The opening calls Spark a unified engine for several kinds of workload. In your own words, what is a unified engine?
*Luu 1, opening paragraph and Spark Unified Stack*

**A:**

#### L1.2 · What is an RDD?
*Luu 1, Spark Core*

**A:**

#### L1.3 · What is a DataFrame?
*Luu 1, Spark SQL*

**A:**

#### L1.4 · What is the Catalyst optimizer?
*Luu 1, Spark SQL*

**A:**

#### L1.5 · What is the Spark driver?
*Luu 1, Spark Applications*

**A:**

#### L1.6 · What is a Spark executor?
*Luu 1, Spark Drivers and Executors*

**A:**

#### L1.7 · What does a cluster manager do, and which ones does the chapter list as supported?
*Damji 1, Cluster manager*

**A:**

#### L1.8 · What is a `SparkSession`, and which earlier entry points did it absorb?
*Damji 1, SparkSession*

**A:**

#### L1.9 · What is a `partition`, and where does it live?
*Damji 1, Distributed data and partitions*

**A:**

#### L1.10 · What does the acronym DAG stand for, and which component acts on that graph?
*Damji 1, Speed*

**A:**

#### L1.11 · What does the chapter call an application, and which parts is it made of?
*Damji 2, Step 3: Understanding Spark Application Concepts*

**A:**

#### L1.12 · What is a job, and what causes one to come into existence?
*Damji 2, Step 3: Understanding Spark Application Concepts*

**A:**

#### L1.13 · What is a stage, and how does it relate to a job?
*Damji 2, Spark Stages*

**A:**

#### L1.14 · What is a task, and where is it sent?
*Damji 2, Spark Tasks*

**A:**

#### L1.15 · What separates a transformation from an action, and which examples of each does the chapter list?
*Damji 2, Transformations, Actions, and Lazy Evaluation, Table 2-1*

**A:**

#### L1.16 · What is `lazy evaluation`, and what gets recorded as lineage while nothing executes?
*Damji 2, Transformations, Actions, and Lazy Evaluation*

**A:**

#### L1.17 · What distinguishes a narrow transformation from a wide one, and what example does the chapter give of each?
*Damji 2, Narrow and Wide Transformations*

**A:**

#### L1.18 · What is the Spark UI for, and at which address does the shell announce it?
*Luu 2, Spark UI*

**A:**

## L2 · Mechanism

Explique o encadeamento (o que causa o quê) em duas ou três frases, seguindo o ponteiro.

#### L2.1 · The chapter names four shortcomings of MapReduce on HDFS. What are they, and who felt each one?
*Damji 1, Hadoop at Yahoo!*

**A:**

#### L2.2 · The chapter separates storage from compute and points out that Hadoop shipped the two together. What does that decoupling allow that Hadoop's arrangement did not?
*Damji 1, Extensibility*

**A:**

#### L2.3 · Once the cluster manager has allocated resources, who talks to whom? Trace the messages between driver, cluster manager and executors.
*Damji 1, Spark driver, Figure 1-4*

**A:**

#### L2.4 · Reading only the driver column of Table 1-1, what is the difference between YARN client mode and YARN cluster mode?
*Damji 1, Deployment modes, Table 1-1*

**A:**

#### L2.5 · Why does splitting data into partitions produce parallelism, and where does data locality come into it?
*Damji 1, Distributed data and partitions, Figure 1-6*

**A:**

#### L2.6 · Walk the chain in your own words: how does an application become jobs, a job become stages, and a stage become tasks?
*Damji 2, Spark Jobs, Spark Stages and Spark Tasks*

**A:**

#### L2.7 · Why does the chapter say a task maps to a single core and works on a single partition, and what does that imply for parallelism?
*Damji 2, Spark Tasks, caption of Figure 2-2*

**A:**

#### L2.8 · Why does deferring execution let Spark optimize better than running each operation as it is called?
*Damji 2, Transformations, Actions, and Lazy Evaluation*

**A:**

#### L2.9 · How do lineage and immutability together give fault tolerance, and what does Spark do when something is lost?
*Damji 2, Transformations, Actions, and Lazy Evaluation*

**A:**

#### L2.10 · Why does `orderBy()` on the filtered DataFrame require a `shuffle` when the `filter()` in the same example does not?
*Damji 2, Narrow and Wide Transformations*

**A:**

## L3 · Comparison across readings

Só depois das quatro leituras: escreva o palpite antes de voltar aos capítulos e registre onde os dois textos discordam.

#### L3.1 · What role does the RDD play in each book?
*Compare Luu 1 (Spark Core) with Damji 2 (the note on the Structured APIs). Note which of the two uses an RDD in its own code example.*

**Guess:**

**A:**

#### L3.2 · What number does each book give for Spark's gain over MapReduce, and what does each one attribute the gain to?
*Compare Luu 1 (Overview) with Damji 1 (Spark's Early Years at AMPLab). Write down both numbers, the source each cites and the workload the number would hold for.*

**Guess:**

**A:**

#### L3.3 · How many pieces make up the Spark stack, and which are they?
*Compare Luu 1 (Spark Unified Stack) with Damji 1 (Apache Spark Components as a Unified Stack). Count the boxes in each figure against the list in the text next to it.*

**Guess:**

**A:**

#### L3.4 · Which Java version does each book require, and how firmly?
*Compare Luu 2 (the two version notes, plus the sentence on what is enough to have installed) with Damji 2 (the requirement in Step 1). Watch the difference between recommending and requiring.*

**Guess:**

**A:**

#### L3.5 · Which cluster managers does Spark support, according to each book?
*Compare Luu 1 (Spark Cluster and Resource Management System) with Damji 1 (Cluster manager and Table 1-1). One of the two leaves out a manager that was already supported when the book came out.*

**Guess:**

**A:**

## L4 · Judgment

Sem ponteiro: decida se a afirmação se sustenta e diga o que você olharia para checar.

#### L4.1 · Luu 1 repeats the claim that Spark runs certain workloads up to 100x faster than Hadoop MapReduce, and offers the 2014 Daytona GraySort result as evidence in the same paragraph. Does that evidence support that number, and what would you look at to decide?

**Guess:**

**A:**

#### L4.2 · Damji 1 states that the same snippet written in Python, R or Java compiles to identical bytecode and delivers the same performance. Do you accept that without qualification, and what case would you try in order to break it?

**Guess:**

**A:**

#### L4.3 · Damji 1 says Spark treats each partition as a high-level logical abstraction, a DataFrame in memory. Does that sentence describe the relationship between `partition` and DataFrame well? If not, write the description you would put in its place.

**Guess:**

**A:**

#### L4.4 · Damji 2 classifies `read()` as a transformation. Does that fit the definition of `lazy evaluation` the same chapter gave two pages earlier, and how would you test it?

**Guess:**

**A:**

#### L4.5 · Damji 2 says an executor with 16 cores can have 16 or more tasks working in parallel. Does that sit with the sentence two lines above it, and how would you decide which one is right?

**Guess:**

**A:**

## L5 · Transfer to your own work

Sem resposta no livro: você é a única fonte, e "não sei" já é resultado, virando pauta para a aula.

#### L5.1 · In a pipeline you know, where does the pattern of many small files in object storage show up (order of magnitude: how many objects, at what average size)?

**Guess:**

**A:**

#### L5.2 · Where in your work is a business rule written twice today, once for the incoming batch and once for reprocessing history?

**Guess:**

**A:**
