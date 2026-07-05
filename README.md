# PySpark ETL Pipeline 
This project demonstrates the implementation of a complete **ETL (Extract, Transform, Load) pipeline** using **Apache Spark (PySpark)**. The objective is to understand Spark's distributed data processing capabilities while building an end-to-end data processing workflow for an e-commerce orders dataset.

The project begins by generating a realistic dataset containing customer orders with intentionally introduced duplicate records and missing values. The dataset is then processed through multiple transformation stages, including data cleaning, schema handling, filtering, aggregation, and optimized storage. Finally, the processed data is stored in both **CSV** and **Parquet** formats to compare storage and performance characteristics.

The project also explores several important Spark concepts such as **Spark Architecture**, **Lazy Evaluation**, **DAG Execution**, **Catalyst Optimizer**, **Shuffle Operations**, **Predicate Pushdown**, and **Column Pruning**, providing practical exposure to how Spark processes large-scale datasets efficiently.

---

# Project Objectives

The primary objectives of this project are:

- Understand Apache Spark Architecture and execution model.
- Learn the roles of Driver, Cluster Manager, and Executors.
- Build a complete ETL pipeline using PySpark.
- Read and process structured data from CSV files.
- Handle schemas using both InferSchema and Explicit Schema.
- Perform DataFrame transformations efficiently.
- Handle duplicate and missing values.
- Apply filtering and aggregation operations.
- Understand Spark Actions and Transformations.
- Learn Lazy Evaluation and DAG execution.
- Understand Narrow and Wide transformations.
- Study Shuffle operations and their performance impact.
- Compare CSV and Parquet storage formats.
- Demonstrate Predicate Pushdown and Column Pruning.
- Understand how Catalyst Optimizer improves query execution.
- Store processed datasets in optimized formats.

---

# Dataset Description

The dataset represents a simplified **E-Commerce Order Management System**.

Each record contains information about:

- Order ID
- Customer ID
- Product Name
- Product Category
- Price
- Quantity
- Region
- Order Status

To simulate real-world business scenarios, the generated dataset intentionally contains:

- More than 100 records
- Duplicate records
- Missing product names
- Missing prices
- Missing quantities
- Missing regions
- Multiple product categories
- Multiple order statuses

These imperfections allow the implementation of realistic data cleaning and preprocessing techniques commonly used in production ETL pipelines.

---

# Project Workflow

The project follows a complete ETL workflow.

### Step 1 — Data Generation

A synthetic e-commerce dataset is generated using Python.

The generated dataset contains realistic order information while intentionally introducing duplicate rows and missing values to simulate real-world business data.

---

### Step 2 — Data Extraction

The dataset is read into Spark using the DataFrame API.

Two schema handling approaches are demonstrated:

- Automatic schema inference (`inferSchema=True`)
- Explicit schema definition using `StructType`

---

### Step 3 — Data Cleaning

Several preprocessing operations are performed to improve data quality.

These include:

- Renaming columns
- Casting data types
- Removing duplicate rows
- Handling null values
- Replacing missing values using `fillna()`

---

### Step 4 — Data Transformation

Additional business-related columns are created.

Examples include:

- Total Price
- Tax
- Final Price

These calculated columns improve analytical capabilities and demonstrate the use of `withColumn()`.

---

### Step 5 — Data Filtering

The cleaned dataset is filtered to include only meaningful records.

Examples include:

- Delivered orders
- Valid prices greater than zero

Filtering reduces unnecessary data before aggregation.

---

### Step 6 — Data Aggregation

Revenue analysis is performed using `groupBy()`.

Metrics calculated include:

- Number of Orders
- Total Revenue
- Average Order Value

This stage demonstrates Spark's Wide Transformation and Shuffle behavior.

---

### Step 7 — Data Storage

The processed dataset is written into:

- CSV Format
- Parquet Format

The Parquet version is later used to demonstrate Spark optimization techniques.

---

# Spark Architecture

Apache Spark follows a distributed computing architecture consisting of three primary components.

### Driver

The Driver is the main application process.

Its responsibilities include:

- Creating the SparkSession
- Building the DAG
- Scheduling tasks
- Coordinating execution
- Returning results

---

### Cluster Manager

The Cluster Manager allocates computing resources for Spark applications.

Common cluster managers include:

- Standalone
- Hadoop YARN
- Kubernetes
- Apache Mesos

For this project, Spark runs in **local[*] mode**, allowing all CPU cores of the local machine to simulate distributed execution.

---

### Executors

Executors are worker processes responsible for:

- Processing partitions
- Executing tasks
- Performing transformations
- Storing intermediate data
- Returning computation results

---

# Spark Concepts Demonstrated

## Lazy Evaluation

Spark does not execute transformations immediately.

Instead, it records each transformation and constructs a **Directed Acyclic Graph (DAG)** representing the execution plan.

Actual execution begins only when an **Action** such as `show()`, `count()`, or `first()` is called.

This behavior minimizes unnecessary computation and allows Spark to optimize execution.

---

## DAG (Directed Acyclic Graph)

Spark internally builds a DAG representing all transformations.

Before execution, Spark analyzes the DAG to determine the most efficient execution strategy.

This optimization reduces redundant computations and improves performance.

---

## DataFrame Transformations

The project demonstrates several commonly used DataFrame transformations.

These include:

- select()
- filter()
- where()
- withColumn()
- withColumnRenamed()
- cast()
- fillna()
- dropDuplicates()
- groupBy()

Each transformation returns a new DataFrame without immediately executing any computation.

---

## DataFrame Actions

Actions trigger the execution of all pending transformations.

Actions demonstrated include:

- show()
- count()
- first()
- take()
- write()

These operations cause Spark to execute the DAG.

---

## Narrow Transformations

Narrow transformations process data within the same partition.

Examples include:

- select()
- filter()
- where()
- withColumn()
- cast()

Characteristics:

- No data movement
- Faster execution
- No shuffle required

---

## Wide Transformations

Wide transformations require data to be redistributed across partitions.

Examples include:

- groupBy()
- orderBy()
- dropDuplicates()

Characteristics:

- Shuffle occurs
- Higher execution cost
- Network communication required

---

# Shuffle

Shuffle is one of the most expensive operations in Spark.

During operations such as `groupBy()`, Spark redistributes records across partitions so that identical keys are processed together.

Shuffle increases:

- Network traffic
- Disk I/O
- Memory usage
- Execution time

Minimizing unnecessary shuffle operations is an important Spark optimization technique.

---

# CSV vs Parquet

The project compares two storage formats.

## CSV

Advantages:

- Human-readable
- Easy to edit
- Widely supported

Limitations:

- Larger file size
- No embedded schema
- Slower reads
- No advanced optimization support

---

## Parquet

Advantages:

- Columnar storage
- Built-in compression
- Stores schema metadata
- Faster read performance
- Lower storage requirements
- Supports Spark optimizations

Parquet is the preferred storage format for analytical workloads.

---

# Predicate Pushdown

Predicate Pushdown allows Spark to apply filter conditions while reading Parquet files.

Instead of loading the complete dataset into memory, Spark reads only the rows satisfying the filter condition.

Benefits include:

- Reduced disk I/O
- Faster query execution
- Lower memory usage

---

# Column Pruning

Column Pruning allows Spark to read only the required columns from a Parquet file.

Instead of reading the entire dataset, Spark accesses only the requested columns.

Benefits include:

- Reduced disk access
- Faster execution
- Lower memory consumption

---

# Catalyst Optimizer

Catalyst Optimizer is Spark's built-in query optimization engine.

It automatically improves query execution by generating optimized execution plans.

The optimization process includes:

- Parsed Logical Plan
- Analyzed Logical Plan
- Optimized Logical Plan
- Physical Execution Plan

Using `explain(True)`, the project demonstrates how Spark transforms user-written code into an optimized execution strategy.

---

# Performance Insights

The project demonstrates several important performance observations.

- Lazy Evaluation delays execution until an Action is invoked.
- DAG optimization reduces redundant computation.
- Narrow transformations execute faster because they avoid data movement.
- Wide transformations require shuffle and therefore take longer.
- Parquet files are significantly faster than CSV for analytical workloads.
- Predicate Pushdown reduces unnecessary data reading.
- Column Pruning minimizes disk I/O by reading only required columns.
- Catalyst Optimizer automatically generates efficient execution plans.
- Writing intermediate data in Parquet format improves future processing performance.

---

# Technologies Used

| Technology | Purpose |
|------------|---------|
| Python 3 | Programming Language |
| Apache Spark (PySpark) | Distributed Data Processing |
| Google Colab | Development Environment |
| PySpark DataFrame API | Data Processing |
| CSV | Input Data Storage |
| Parquet | Optimized Output Storage |
| Git | Version Control |
| GitHub | Project Hosting |

---


# Limitations

Although the project successfully demonstrates Spark fundamentals, it has several limitations.

- Developed using a relatively small synthetic dataset.
- Executed only in Local Mode (`local[*]`).
- Does not connect to a real distributed Spark cluster.
- No integration with databases or cloud storage.
- Does not include Spark Streaming.
- Does not implement Spark SQL queries extensively.
- Machine Learning (MLlib) is not included.
- Performance comparisons are limited due to the dataset size.
- No deployment pipeline or production scheduling.

---

# Learning Outcomes

By completing this project, the following concepts were successfully learned and implemented.

- Apache Spark Architecture
- SparkSession Configuration
- Driver and Executor Responsibilities
- Cluster Execution Model
- DataFrame API
- Schema Inference
- Explicit Schema Definition
- Data Cleaning Techniques
- Null Value Handling
- Duplicate Removal
- Data Type Conversion
- Column Transformations
- Data Aggregation
- DataFrame Actions and Transformations
- Lazy Evaluation
- DAG Execution
- Narrow Transformations
- Wide Transformations
- Shuffle Operations
- Catalyst Optimizer
- Predicate Pushdown
- Column Pruning
- CSV and Parquet Storage Formats
- Building a Modular ETL Pipeline

---

# Future Enhancements

The project can be extended further by incorporating advanced Spark features.

Possible improvements include:

- Integration with Apache Hive
- Reading data from relational databases using JDBC
- Spark SQL-based analytics
- Window Functions
- Partitioned Parquet Storage
- Apache Kafka Integration
- Spark Streaming
- Delta Lake Support
- MLlib-based Machine Learning Pipelines
- Deployment on YARN or Kubernetes clusters
- Cloud Storage Integration (AWS S3, Azure Blob Storage, Google Cloud Storage)

---
Performance and Architecture Insights
Spark Architecture
Apache Spark follows a Driver–Executor architecture, where the Driver creates the execution plan (DAG), coordinates tasks, and collects results, while Executors perform parallel data processing.
In this project, Spark was executed in local[*] mode, utilizing all available CPU cores to simulate distributed processing on a single machine.
The Cluster Manager (used in cluster deployments) allocates resources and manages executors, enabling Spark applications to scale across multiple nodes.
Performance Insights
Lazy Evaluation improved performance by delaying execution until an action (show(), count(), etc.) was triggered, allowing Spark to optimize the execution plan.
Narrow Transformations such as select(), filter(), and withColumn() were efficient because they processed data within the same partition without requiring data movement.
Wide Transformations like groupBy() triggered a Shuffle, where data was redistributed across partitions. Although necessary for aggregation, shuffle increases execution time due to disk and network I/O.
The cleaned dataset was saved in both CSV and Parquet formats. Compared to CSV, Parquet provided better compression, faster reads, and lower storage requirements because of its columnar storage format.
Predicate Pushdown reduced disk I/O by filtering records at the storage layer before loading them into Spark.
Column Pruning improved efficiency by reading only the required columns from the Parquet file instead of scanning the entire dataset.
The Catalyst Optimizer automatically optimized the query execution plan by combining transformations, applying predicate pushdown, and eliminating unnecessary operations, resulting in better overall performance.
Following Spark best practices, show() was used instead of collect() to avoid loading the entire dataset into the Driver's memory, making the application more scalable for large datasets.

# Conclusion

This project demonstrates a complete ETL pipeline using Apache Spark and PySpark while providing practical exposure to core big data processing concepts. Beginning with raw data generation, the pipeline performs schema handling, data cleaning, transformation, filtering, aggregation, and optimized data storage. Throughout the implementation, important Spark concepts such as Lazy Evaluation, DAG execution, Shuffle operations, Catalyst Optimizer, Predicate Pushdown, and Column Pruning are explored in a practical context.

Overall, the project provides a strong foundation in distributed data processing and illustrates how Apache Spark efficiently handles structured datasets while maintaining scalability, performance, and code modularity. It serves as a practical learning project for understanding modern ETL workflows and Spark optimization techniques.
