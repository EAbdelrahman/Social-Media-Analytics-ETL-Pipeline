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

# Project File Structure

This section describes the hierarchical organization of project files, making it easier to navigate and understand the project components.

## Directory Layout

```plaintext
Social-Media-Analytics-ETL-Pipeline/
├── dags/
│   ├──  ETL_Pipeline.py     # DAG definition for ETL processes
│   │__  Airflow DAG.PDF     # Documentation of the dag    
│   
│       
├── dbt_project/
│   ├── models/
│   │   ├── marts/
│   │   │   ├── content_performance.sql  # Transformation for content performance mart
│   │   │   ├── location_analysis.sql    # Transformation for location analysis mart
│   │   │   ├── tag_analysis.sql         # Transformation for tag analysis mart
│   │   │   └── user_engagement.sql      # Transformation for user engagement mart
│   │   └── staging/
│   ├── dbt_project.yml        # Configuration file for dbt project
|   |___ dbt.pdf               # Full schema and purpose of the data marts
|   |___ README.md             # Documentation for Dbt
│
|── dbt_profiles
|    |_____  profiles.yml       # dbt profile configuration
|
├── datasets/
│   ├── Data Dictionary.pdf             #Data dictionary and ER diagram
│   │   
│   └── social_media_info.json
│       
├── spark_code/
|   |___ PySpark Scripts.pdf   # Here you will find illustrations for PySpark scripts
|   |____ Write_csv.py/ipynb
│   ├____ Cleaning.py/ipynb
│   │____ Logging.py/ipynb     
│   |____ Connect_Dwh.py/ipynb
│       
├── docker-compose.yaml
│___ Dockerfile               # Dockerfile for setting up the environment
|___ init.sh                  # using to initiate the enviroment 
|___ installed-packages.txt             
└── README.md                  # Documentation of the project
|── ETL Pipeline User Manual.pdf  # User Manual for the  project Here you will find any information you need.
