🏎️ Formula 1 Data Engineering Project
This project demonstrates an end-to-end modern data engineering pipeline built using Azure Data Factory, Azure Data Lake, and Databricks. It ingests, processes, and stores Formula 1 racing data from the Github API, transforming it across Bronze, Silver, and Gold layers in a medallion architecture.



🔧 Tech Stack
Azure Data Factory (ADF) – for orchestrating data ingestion workflows

Azure Data Lake Storage Gen2 – as a data lake to store raw and processed data

Databricks (PySpark + Notebooks) – for data transformation, cleaning, and analysis

Delta Lake – to manage versioned, ACID-compliant tables

Github API – public F1 racing data source

Spark SQL – for aggregations and final views.


🗂️ Project Phases
1. 📥 Data Ingestion (Bronze Layer)
Source: Github API (JSON data on races, drivers, results, etc.)

Tool: Azure Data Factory

Action:

Used ADF pipelines to ingest data from REST API into Azure Data Lake (Raw zone)

<img width="954" alt="image" src="https://github.com/user-attachments/assets/fc57286d-bb0f-4183-a69d-a3dfce6a2edf" />


Data stored as-is in JSON format in /raw/<dataset_name>/

2. 🔄 Data Transformation (Silver Layer)
Tool: Databricks with PySpark notebooks

Input: Raw JSON data from the Bronze layer (stored in Azure Data Lake)

✅ Key Transformation Steps:
Reading Raw Data

Read each dataset (drivers, races, results, circuits, etc.) using PySpark.

Applied defined schemas to enforce data types.

Handling Null Values

Dropped records with critical nulls (e.g., missing raceId, driverId) using .dropna().

Filled optional null fields with defaults using .fillna() for consistency (e.g., unknown strings for names or 0 for positions).

example:-- 

df = df.dropna(subset=["raceId", "driverId"])
df = df.fillna({"constructorId": "unknown", "status": "finished"})

Trimmed whitespaces, renamed columns to snake_case, and casted data types.

Added ingestion timestamp for traceability.

Joining Datasets to Create Intermediate Views

Performed joins between results, drivers, races, and constructors using raceId, driverId, and constructorId.

Example:


race_results_df = results_df \
    .join(drivers_df, "driverId", "inner") \
    .join(races_df, "raceId", "inner") \
    .join(constructors_df, "constructorId", "inner")
    
Storing Transformed Data (Silver Layer)


Partitioned larger datasets (like race_results) by year for performance.

3. 📊 Business Layer (Gold Layer)
Tool: Databricks SQL / PySpark

Action:

Created final fact and dimension tables for reporting

Joined multiple silver tables (e.g., races + results + drivers)

Stored clean, analytics-ready Delta Tables in /gold/

✅ Final Output:

race_results (fact table)

drivers_standings, constructor_standings

Stored in Delta format, ready for BI/reporting tools

4. 🧠 Features & Highlights
Incremental Loading: Implemented using Spark's upsert (MERGE) operation into Delta Tables. This ensures only new or changed records are updated, avoiding full refreshes.

Schema Enforcement & Evolution: Enforced strict schema during initial loads and supported schema evolution using Delta Lake features.

Null Handling: Dropped or filled missing values strategically during transformations to ensure data quality.

Modular Notebook Design: Each dataset (drivers, races, results, etc.) is processed through a dedicated, reusable notebook.

Parameterized Workflows: Enabled dynamic execution of notebooks via Databricks parameters (e.g., dataset name, file date).

Optimized Joins: Created the race_results table using efficient joins across multiple cleaned datasets in the Silver layer.

Partitioned Tables: Large tables are partitioned by race_year or circuit_location for better query performance.




