# dbt Models for Data Mart Transformations

## Introduction
This section provides an overview of the dbt models used for data mart transformations within our social media analytics platform. These models are essential for aggregating and transforming raw data from multiple sources into structured formats that support decision-making and insights.

## Configuration Overview
- **Project Name:** dbt_project
- **Version:** 1.0
- **Profile:** postgres_dw
- **Model Location:** models/marts
- **Artifact Storage:** target directory

## Data Sources Overview
Our dbt models source data from the following tables within our PostgreSQL data warehouse:
- **dim_posts:** Stores metadata about social media posts such as ID, text, and timestamps.
- **fact_post_interactions:** Tracks user interactions with posts, including likes, comments, and shares.

## Model Documentation
Each model is designed to address specific analytical needs:

### Content Performance Mart
- **File:** content_performance_mart.sql
- **Purpose:** Aggregates interaction metrics to assess the performance of content across platforms.
- **Key Metrics:** Total likes, shares, comments, and reaction specifics (e.g., total_angry, total_haha).

### Location Analysis Mart
- **File:** location_analysis_mart.sql
- **Purpose:** Provides insights into geographic distribution of user interactions.
- **Key Metrics:** Total interactions and unique user counts per location.

### Tag Analysis Mart
- **File:** tag_analysis_mart.sql
- **Purpose:** Analyzes post interactions grouped by tags to identify content engagement trends.
- **Key Metrics:** Total and average likes per tag, total posts with specific tags.

### User Engagement Mart
- **File:** user_engagement_mart.sql
- **Purpose:** Identifies key influencers by ranking users based on their engagement.
- **Key Metrics:** Comprehensive interaction metrics per user, engagement rank.

## Schema Definitions
Our `schema.yml` file includes detailed descriptions and tests for each column to ensure data integrity and accuracy.

## Best Practices and Conventions
We adhere to the following standards to maintain and scale our dbt implementation:
- **Materialization:** Models are primarily materialized as tables to optimize query performance.
- **Testing:** Extensive data testing ensures the accuracy and integrity of our transformations.

