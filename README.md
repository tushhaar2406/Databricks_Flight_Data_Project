
#  Data Pipeline Project

##  Overview

This project implements a **modern data pipeline** for ingesting, transforming, and analyzing data using **Databricks, Spark, dbt, and Azure Synapse Analytics**. The pipeline follows a **medallion architecture (Bronze, Silver, Gold)** and enables building a **Star Schema** for analytical reporting.

---

##  Architecture

![Architecture Diagram](./docs/architecture.png) <!-- Replace with actual image path -->

1. **Autoloader**

   * Ingests raw CSV files from source systems.
   * Handles schema inference and incremental file loading.

2. **Bronze Layer (Raw Data)**

   * Stores ingested data in **Databricks Unity Catalog (Bronze schema)**.
   * Ensures immutability and traceability.

3. **Silver Layer (Cleansed Data)**

   * Data is processed using **Apache Spark**.
   * Stored in **Databricks Unity Catalog (Silver schema)**.
   * Removes duplicates, enforces data quality, and standardizes formats.

4. **Gold Layer (Business-Ready Data)**

   * Aggregated and curated datasets stored in **Databricks Unity Catalog (Gold schema)**.
   * Optimized for analytics and business use cases.

5. **dbt (Data Build Tool)**

   * Transforms curated data into **Star Schema (Fact & Dimension tables)**.
   * Manages version control and modular SQL transformations.

6. **Azure Synapse Analytics (Data Warehouse)**

   * Consumes transformed star schema for reporting and dashboards.
   * Supports BI tools like Power BI / Tableau.

---

##  Tech Stack

* **Databricks** → ETL and Unity Catalog storage (Bronze, Silver, Gold)
* **Apache Spark** → Distributed data transformations
* **Databricks Autoloader** → Incremental data ingestion
* **dbt** → Data modeling & transformations (Star Schema)
* **Azure Synapse Analytics** → Data warehouse for reporting

---

##  How It Works

1. **Ingestion** → CSV files land in **Databricks Unity Catalog Volumes** and are auto-loaded by **Databricks Autoloader**.
2. **ETL** → Spark processes raw data into **Bronze → Silver → Gold schemas in Unity Catalog**.
3. **Modeling** → dbt creates Fact & Dimension tables in a **Star Schema**.
4. **Consumption** → Azure Synapse stores data for BI and analytics.

