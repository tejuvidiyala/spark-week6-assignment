PySpark ETL Pipeline – E-Commerce Orders Analysis
Project Overview

This project demonstrates a complete ETL (Extract, Transform, Load) pipeline using Apache Spark with PySpark. The main goal of the project is to understand how Spark processes large datasets efficiently while applying different data transformation techniques and performance optimizations. A sample e-commerce orders dataset was generated containing customer information, product details, prices, quantities, regions, and order statuses. The dataset intentionally includes duplicate records and missing values to simulate real-world business data. Throughout the project, the data is cleaned, transformed, analyzed, and finally stored in optimized file formats.

Objective

The objective of this project is to gain practical experience with Apache Spark and understand its architecture, execution model, and optimization techniques. The project covers reading data from CSV files, handling schemas, cleaning and transforming data, performing aggregations, understanding Spark's lazy execution model, and improving performance using Parquet files, Predicate Pushdown, and Column Pruning. It also demonstrates how to build a reusable ETL pipeline that can process data efficiently while following best practices for large-scale data processing.

Dataset Description

The dataset used in this project represents an online shopping platform's order details. It contains more than one hundred records, including order IDs, customer IDs, product names, categories, prices, quantities, regions, and order statuses. To make the project realistic, duplicate records and null values were intentionally added. These imperfections provide an opportunity to demonstrate important data cleaning operations such as removing duplicate rows, handling missing values, and converting incorrect data types before analysis.

Spark Architecture

Apache Spark follows a distributed computing architecture consisting of a Driver, Cluster Manager, and Executors. The Driver acts as the main controller that creates the SparkSession, builds the execution plan, and coordinates the execution of tasks. Executors are responsible for performing the actual computations and processing the data partitions. The Cluster Manager allocates system resources and manages the executors. In this project, Spark runs in local[*] mode, which means all available CPU cores on the local machine are used to simulate distributed processing without requiring a real cluster.

ETL Pipeline

The project follows the ETL process to transform raw data into meaningful information. First, the dataset is extracted from a CSV file using Spark's DataFrameReader with automatic schema detection. Next, several transformation steps are applied, including renaming columns, converting data types, removing duplicate rows, replacing missing values, and creating new calculated columns such as total price, tax, and final price. After cleaning the data, only delivered orders with valid prices are selected for further analysis. Finally, the processed dataset is aggregated to calculate revenue by category and then stored in both CSV and Parquet formats for future use.

Data Transformations

Several DataFrame transformations were applied during data processing to improve the quality of the dataset. The select() function was used to retrieve only the required columns, while filter() and where() were used to remove unnecessary records. The cast() function converted columns into appropriate data types, and withColumnRenamed() improved column readability. Missing values were handled using fillna(), duplicate records were removed using dropDuplicates(), and additional business-related columns such as total price, tax, and final price were created using withColumn(). These transformations prepared the dataset for meaningful analysis.

Lazy Evaluation

One of the most important concepts demonstrated in this project is Lazy Evaluation. In Spark, transformations do not execute immediately when they are written. Instead, Spark records each transformation and builds a Directed Acyclic Graph (DAG) representing the sequence of operations. The actual execution begins only when an action such as show(), count(), or first() is called. This approach allows Spark to optimize the execution plan before processing the data, resulting in better performance.

Narrow and Wide Transformations

This project demonstrates both narrow and wide transformations. Narrow transformations such as select(), filter(), and withColumn() process data within the same partition and therefore do not require any data movement, making them faster. On the other hand, wide transformations such as groupBy() require Spark to redistribute data across multiple partitions through a process called shuffle. Although wide transformations are more computationally expensive, they are necessary for performing operations like aggregations and sorting.

Shuffle

Shuffle is one of the costliest operations in Apache Spark because it involves moving data between different partitions. In this project, shuffle occurs during the groupBy() operation when Spark groups records belonging to the same product category before calculating the total revenue. Since data has to be exchanged between executors, shuffle increases disk I/O, network communication, and execution time. Understanding shuffle helps developers write more efficient Spark applications.

CSV vs Parquet

The project compares two commonly used file formats: CSV and Parquet. CSV files are easy to read and edit but require more storage space and do not store schema information. Parquet is a columnar storage format that stores metadata and schema internally, allowing Spark to read data much faster. Parquet also supports advanced optimization techniques such as Predicate Pushdown and Column Pruning, making it the preferred format for big data processing and analytics.

Predicate Pushdown and Column Pruning

To improve performance, Spark applies Predicate Pushdown and Column Pruning when working with Parquet files. Predicate Pushdown allows Spark to apply filter conditions while reading the file itself, reducing the amount of unnecessary data loaded into memory. Column Pruning ensures that only the required columns are read instead of the entire dataset. These optimizations significantly reduce disk access, memory consumption, and execution time, especially when processing large datasets.

Catalyst Optimizer

Apache Spark includes an intelligent query optimization engine called the Catalyst Optimizer. During execution, Catalyst analyzes the logical plan, applies multiple optimization rules, and generates the most efficient physical execution plan. Using the explain(True) method, this project demonstrates how Spark transforms a simple DataFrame query into an optimized execution strategy. This automatic optimization enables developers to write simple code while Spark handles performance improvements internally.

Performance Insights

Throughout the project, several performance concepts were explored. Lazy Evaluation minimizes unnecessary computation by delaying execution until an action is performed. Narrow transformations execute quickly because they avoid data movement, whereas wide transformations introduce shuffle, making them comparatively slower. Saving the processed dataset in Parquet format improves future read performance, while Predicate Pushdown and Column Pruning further reduce unnecessary disk I/O. These techniques collectively demonstrate how Spark efficiently handles large-scale data processing.

Conclusion

This project successfully demonstrates the complete lifecycle of an ETL pipeline using Apache Spark and PySpark. Starting from generating raw data, the project performs data cleaning, transformation, filtering, aggregation, and finally stores the processed data in optimized formats. Along with practical implementation, it also provides a clear understanding of Spark architecture, lazy evaluation, DAG execution, shuffle operations, Catalyst Optimizer, and storage optimizations. Overall, the project provides hands-on experience with real-world big data processing concepts and showcases how Apache Spark can efficiently process large datasets while maintaining scalability and performance.
