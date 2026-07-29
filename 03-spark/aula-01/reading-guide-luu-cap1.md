# Reading Guide — *Beginning Apache Spark 3*, Chapter 1: Introduction to Apache Spark

**Source:** Hien Luu, *Beginning Apache Spark 3* (Apress, 2021), Chapter 1
**Scope:** every question in Levels 1–4 is answerable from this chapter alone. Level 5 is deliberately outside it.

## How to use this guide

1. Read the chapter once end to end without stopping.
2. Work Level 1 from memory first, then go back to the text to fill gaps. Mark every question you could not answer — those are your re-read targets.
3. Do Levels 2–3 in writing, in full sentences. If you cannot write it, you do not know it.
4. Level 4 is where the chapter actually becomes useful: it forces you to connect sections that the author kept separate.
5. Level 5 items go straight into your study backlog, not into your notes as facts.

Each question carries a pointer to the section where the evidence lives, so this doubles as an index.

---

## Level 1 — Recall and definitions

Short, verifiable answers. One or two sentences each.

1. What three properties does the author identify as the reason for Spark's popularity and wide adoption? *(Overview; Summary)*
2. When was Spark 3.0 released, and what milestone in the project's history did that release coincide with? *(Overview)*
3. What performance claim does the Spark website make relative to Hadoop MapReduce? *(Overview)*
4. What benchmark did Spark win in 2014, what volume and record count did it sort, and what improvement did the Databricks submission claim over the previous record holder? *(Overview)*
5. Approximately how many high-level data processing operators does Spark offer, and in which four languages are they available? *(Overview; Summary)*
6. List the four workload types the author uses to illustrate Spark's flexibility. *(Overview)*
7. Where and in what year did Spark begin as a research project? In which years was it open sourced and promoted to an Apache top-level project? *(History)*
8. Which company was founded by researchers from the original project, how much did it raise in 2013, and what role does the author assign it in the Spark ecosystem? *(History)*
9. Name the two research papers the author recommends as foundational reading, and the years they were published. *(History)*
10. What two characteristics of Scala does the author give as the reason Spark's creators chose it? *(History)*
11. According to the Spark FAQ as cited in the chapter, how many machines are in the world's largest Spark cluster? *(Spark Cluster and Resource Management System)*
12. Name the two resource management systems the chapter mentions by name, plus the third option available to companies that adopt Spark exclusively. *(Spark Cluster and Resource Management System)*
13. What are the two main components of a typical resource management system, and what does the master know about the slaves? *(Spark Cluster and Resource Management System)*
14. What are the two parts of a Spark application, as the author defines it? *(Spark Applications)*
15. Through which component does a Spark driver perform its work? *(Spark Applications)*
16. What kind of process is a Spark executor, and what determines its life span? *(Spark Drivers and Executors)*
17. How many drivers and how many executors does a Spark application consist of? *(Spark Drivers and Executors; Summary)*
18. On what unit of hardware is each task executed? *(Spark Drivers and Executors)*
19. What three resource parameters can you specify when launching a Spark application? *(Spark Drivers and Executors)*
20. Name the five libraries that sit on top of Spark Core in the unified stack, and the workload each one targets. *(Spark Unified Stack; Figure 1-3)*
21. What are the two things Spark Core consists of? *(Spark Core)*
22. Define an RDD using the author's own terms. *(Spark Core)*
23. What is the name of the Spark SQL optimizer, and what is a DataFrame? *(Spark SQL)*
24. List the structured formats and storage systems Spark SQL can read from and write to, as enumerated in the chapter. *(Spark SQL)*
25. According to the 2021 Spark survey cited in the chapter, which component was growing fastest? *(Spark SQL)*
26. What is the three-part motto the author gives for Spark SQL? *(Spark SQL)*
27. Which streaming data sources does the chapter list? *(Spark Structured Streaming)*
28. What does DStream stand for, and in which Spark version does the chapter say Structured Streaming was introduced? *(Spark Structured Streaming)*
29. What delivery guarantee does the chapter attribute to the Structured Streaming engine? *(Spark Structured Streaming)*
30. Roughly how many algorithms does MLlib provide, and starting from which Spark version are its APIs DataFrame-based? *(Spark MLlib)*
31. Which two Spark SQL engine components does the DataFrame-based MLlib benefit from? *(Spark MLlib)*
32. What abstraction does GraphX provide, and which graph algorithms ship with it? *(Spark GraphX)*
33. What percentage of Spark 3.0 enhancements went into Spark SQL and Spark Core, and what speedup over Spark 2.4 is claimed on which benchmark? *(Apache Spark 3.0)*
34. Name the three Spark 3.0 features the chapter highlights, and state in one line what each does. *(Apache Spark 3.0)*
35. What speedup range does DPP deliver, and on what share of benchmark queries? *(Dynamic Partition Pruning)*
36. List the seven application categories the chapter gives as real-life uses of Spark. *(Apache Spark Applications)*
37. What problem does Delta Lake solve, and what three capabilities does it provide? *(Delta Lake)*
38. What does Koalas implement, when was version 1.0 released, and what pandas API coverage did it claim? *(Koalas)*
39. Which alternative project does the chapter mention for parallel computing in Python? *(Koalas)*
40. Name MLflow's four components and the responsibility of each. *(MLflow)*

---

## Level 2 — Comprehension

Explain in your own words. Three to six sentences each.

1. Explain why the *combination* of speed, ease of use, and flexibility matters more than any one of them individually. What did teams have to do before a unified engine existed? *(Overview)*
2. What specific inefficiencies in Hadoop MapReduce motivated the Berkeley research project, and which two ideas were introduced to address them? *(History)*
3. Describe the division of labour between the cluster manager and the workers. Who offers resources, who assigns work, and who monitors process health? *(Spark Cluster and Resource Management System)*
4. Walk through the driver's full set of responsibilities, in order, from application submission to returning results to the user. *(Spark Applications)*
5. The chapter calls the one-executor-per-application rule a conscious design decision. What benefit does it buy, and what cost does it impose? *(Spark Drivers and Executors)*
6. Explain how Spark achieves parallelism, tracing the chain from application → executor → task → CPU core. *(Spark Drivers and Executors)*
7. Why does the author argue that improvements to Spark Core automatically benefit the whole stack? What does this imply about where optimization effort pays off? *(Spark Core)*
8. What two responsibilities of the distributed computing infrastructure does the author single out as requiring "intimate knowledge" from advanced users, and why would those two in particular determine application performance? *(Spark Core)*
9. Explain the argument for why SQL became the lingua franca of data processing, and what Spark SQL adds to that story at petabyte scale. *(Spark SQL)*
10. Describe the micro-batch model behind DStreams: what is the input split on, and how is the current processing state used? *(Spark Structured Streaming)*
11. In what sense does Structured Streaming simplify the developer's mental model compared with the earlier engine? Relate this to the Reynold Xin remark quoted at the end of that section. *(Spark Structured Streaming)*
12. Why does the iterative nature of machine learning algorithms make Spark a good fit for them? *(Spark MLlib)*
13. Why was R alone insufficient for large-scale data analysis, and what exactly does SparkR contribute? *(SparkR)*
14. Explain the core idea of Adaptive Query Execution. Which runtime statistics does it react to, and what three decisions can it revise? *(Adaptive Query Execution Framework)*
15. Explain Dynamic Partition Pruning in the context of a star schema. Which table gets its row count reduced, and on what basis? *(Dynamic Partition Pruning)*
16. Why did Spark need an accelerator-aware scheduler at all? What changed in how people use Spark? *(Accelerator-aware Scheduler)*
17. Go through the five-line word count example and explain what each line does and what shape the data has after it. *(Spark Example Applications; Listing 1-1)*
18. What problem was MLflow conceived to solve, and why does the author frame it as a software engineering problem rather than a machine learning one? *(MLflow)*

---

## Level 3 — Application and transfer

Use the chapter's content to reason about concrete situations. The chapter gives you enough to answer; it does not hand you the answer.

1. You submit an application with 10 executors, 4 cores and 8 GB each. Using the chapter's model, how many tasks can run concurrently? How much total memory is under the application's control, and how much of the cluster does the driver itself account for? *(Spark Drivers and Executors)*
2. Two teams each run a long-lived Spark application and want to share an expensive intermediate dataset. Based on the executor isolation model, what are their options and what does each cost them? *(Spark Drivers and Executors)*
3. A company already runs a YARN cluster for Hive and Pig jobs. Based on the chapter, what is their path to adopting Spark, and what would change if they were a startup with no existing cluster? *(Spark Cluster and Resource Management System)*
4. Your pipeline currently writes intermediate results to storage between a batch stage and a streaming stage. Which specific benefit of the unified stack does the chapter say you are giving up? *(Spark Unified Stack)*
5. A join in your job is badly skewed — one partition takes twenty times longer than the rest. Which Spark 3.0 feature addresses this, and what does it need in order to act? *(Adaptive Query Execution Framework)*
6. You query a large fact table joined to a small filtered dimension table. Describe what DPP does to the physical read, and why the star schema shape is what makes it possible. *(Dynamic Partition Pruning)*
7. An analyst who knows SQL but not Scala needs to process petabyte-scale data. Which parts of the stack does the chapter say make that possible, and which two interfaces are available to them? *(Spark SQL)*
8. A data scientist has working pandas code that no longer fits in memory on one machine. Using only the chapter, lay out two paths forward and the trade-off between them. *(Koalas)*
9. You need to run interactive queries over the output of a model scoring real-time streams. Which components does that touch, and why does the chapter present this as newly feasible rather than merely convenient? *(Spark Unified Stack)*
10. Map the word count example onto the executor model: which lines cause work to be distributed, where does data movement between machines occur, and which line forces a result back through the driver or out to storage? *(Spark Example Applications + Spark Drivers and Executors)*
11. Of the seven application categories listed, pick the three closest to a public-data crawling and parsing pipeline and justify each choice from the chapter's descriptions. *(Apache Spark Applications)*
12. Your ingestion job and your analytics job intermittently read half-written output and disagree on schema. Which ecosystem component does the chapter propose, and which of its three capabilities addresses which symptom? *(Delta Lake)*

---

## Level 4 — Analysis and synthesis

Cross-section reasoning. These have defensible rather than single answers, but every ingredient is in the chapter.

1. The chapter describes Spark as master/slave, with the driver as master. But it also describes a cluster manager with its own master and workers. Draw both hierarchies and explain how they intersect. Which master can the other not function without? *(Spark Cluster and Resource Management System + Spark Drivers and Executors + Figure 1-1)*
2. Compare Figures 1-1 and 1-2. What does each one show that the other omits, and what would you need to add to either to make it a complete picture of a running application?
3. The chapter names data shuffling as a key concern of the distributed infrastructure, and separately names three Spark 3.0 optimizations. Argue which of those three most directly reduces shuffle cost, and which reduces I/O instead.
4. The chapter says applications on top of Spark Core inherit its improvements automatically. Yet Spark 3.0's headline gains came from Spark SQL, not Core. Reconcile these two claims. What does that suggest about where the RDD-only user sits in Spark 3.0?
5. Trace the abstraction ladder the chapter builds: RDD → DataFrame → SQL → pandas API. At each rung, what does the user gain and what control do they surrender?
6. The chapter presents the executor isolation decision as a benefit. Reframe it as a cost, using the resource management section: what does per-application executor allocation imply for cluster utilization when many short applications run?
7. Build a table with one row per stack component (Core, SQL, Structured Streaming, MLlib, GraphX, SparkR) and columns for: primary abstraction, workload type, languages exposed, and dependency on other components. Which cells does the chapter leave blank?
8. The performance evidence in the chapter comes from three sources: the website's 100x claim, the 2014 GraySort result, and the TPC-DS 30 TB comparison. Rank them by how much you would trust them for predicting your own workload, and say what each one leaves out.
9. The chapter argues that Spark reduces operational cost by replacing several specialized systems. Construct the counterargument using only material from this chapter — where does a single unified engine impose costs of its own?
10. Delta Lake, Koalas, and MLflow are all presented as "ecosystem" rather than "stack." Based on how the chapter describes each, articulate the criterion that separates the two categories. Does Delta Lake actually fit your criterion?
11. Write the chapter's argument for Spark in five sentences, one per section, such that removing any sentence breaks the argument.
12. Identify three claims in this chapter that are asserted without supporting evidence. For each, state what evidence would settle it.

---

## Level 5 — Beyond the chapter (backlog, not notes)

The chapter was written against Spark 3.0/3.1 in 2021. These items are known soft spots — verify against current documentation before you commit anything to your permanent notes.

1. The chapter presents DStream as the main streaming abstraction and Structured Streaming as a newer engine. Check which one is current, which is legacy, and what abstraction Structured Streaming actually uses. The chapter's framing here is the most dated part of it.
2. The chapter dates Structured Streaming to Spark 2.1. Verify the introduction and general-availability versions.
3. Koalas as a separate project versus the pandas API integrated into Spark itself — find out what happened and in which version.
4. Mesos as a Spark resource manager: check its current support status. Also add the deployment target the chapter omits entirely, which is now the common one.
5. The chapter's Spark 3.0 feature list is partial. Find out what AQE, DPP, and the accelerator-aware scheduler default to in current versions, and which further optimizations landed in 3.2, 3.4, and 4.0.
6. Delta Lake is presented as *the* answer for storage consistency. Identify the competing open table formats and the dimensions on which they differ — this is directly relevant to lakehouse coursework.
7. The chapter never distinguishes narrow from wide transformations, actions from transformations, or jobs from stages from tasks — despite the word count example depending on all three distinctions. Note this as a gap to close from the RDD chapter.
8. `sc.textFile` in Listing 1-1 uses the SparkContext, while the text says the driver works through SparkSession. Find out the relationship between the two and which one current code should use.

---

## Key terms to define before moving on

Write a one-line definition for each, in your own words, without looking:

Spark cluster · cluster manager · worker · Spark application · driver · executor · task · SparkSession · SparkContext · RDD · partition · data shuffling · DataFrame · Catalyst · Tungsten · DStream · micro-batch · exactly-once · AQE · skew join · DPP · fact table · dimension table · star schema · lakehouse · schema evolution

Any term you cannot define is a re-read target, not a Level 5 item.
