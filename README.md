# Overview
This ETL pipeline is designed to automate the process of extracting, transforming, and loading (ETL) social media analytics data into a PostgreSQL data warehouse. It enables efficient analysis and reporting by organizing raw data from various sources into well-structured data marts, following the principles of a star schema. The pipeline leverages PySpark for large-scale data processing, Apache Airflow for orchestrating ETL workflows, and dbt for managing transformations in the data warehouse.
## The purpose of this pipeline is to:
•	Extract raw social media interaction data from JSON files.
•	Transform the data into dimensions and fact tables that provide insights into user engagement, content performance, tag analysis, and location analysis.
•	Load the cleaned and transformed data into a PostgreSQL data warehouse to power analytics dashboards and reports.
## Scope of the Pipeline
This pipeline addresses the data processing needs for the social media analytics platform by automating the entire ETL process. The pipeline supports:
•	Handling and transforming high-volume social media data.
•	Creating data marts to analyze user activity, content trends, geographical distribution, and tag usage.
•	Facilitating downstream analysis for business intelligence applications.
Key components and technologies:
•	PySpark: Used for the heavy lifting of data extraction and transformation.
•	Apache Airflow: Orchestrates the entire ETL workflow by managing task dependencies and scheduling.
•	dbt (Data Build Tool): Executes SQL transformations and manages the schema within the PostgreSQL data warehouse.
•	PostgreSQL: Serves as the data warehouse where transformed data is stored and queried for reporting.
