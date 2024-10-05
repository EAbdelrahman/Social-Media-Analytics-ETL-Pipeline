# Social Media Analytics ETL Pipeline Overview

## Purpose
This ETL pipeline is designed to automate the extraction, transformation, and loading (ETL) processes for social media analytics data into a PostgreSQL data warehouse. The pipeline organizes raw data from various sources into structured data marts, supporting efficient analysis and reporting using a star schema. The technologies used include PySpark for data processing, Apache Airflow for workflow orchestration, and dbt for data transformation management.

## Objectives
- **Extract** raw social media interaction data from JSON files.
- **Transform** the data into dimensional and factual structures, facilitating insights into user engagement, content performance, tag analysis, and location analysis.
- **Load** the transformed data into a PostgreSQL data warehouse to enable analytics and reporting.

## Scope
The pipeline caters to the following needs:
- Handles and transforms high-volume social media data.
- Generates data marts for analyzing user activity, content trends, geographical distribution, and tag usage.
- Supports business intelligence applications by providing structured data for downstream analysis.

## Key Components and Technologies
- **PySpark:** Utilized for data extraction and transformation processes.
- **Apache Airflow:** Orchestrates the ETL tasks, managing dependencies and scheduling.
- **dbt (Data Build Tool):** Manages SQL transformations and the schema within the PostgreSQL data warehouse.
- **PostgreSQL:** Acts as the central repository for storing and querying transformed data.

## Contact Information
- **Name:** Abdelrahman Mahmoud Abdelaal
- **Email:** [a.Mahmoud1803@gmail.com](mailto:a.Mahmoud1803@gmail.com)
