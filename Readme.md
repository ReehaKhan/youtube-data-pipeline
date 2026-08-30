# YouTube MENA Most Popular Data Engineering Project

An end-to-end AWS data engineering pipeline that collects and transforms YouTube's daily most-popular videos across countries in the MENA region.

The project uses the YouTube Data API to collect the top 50 most-popular videos for each supported country every day. The data is processed through a Medallion Architecture (Bronze → Silver → Gold) and ultimately transformed into a star schema designed for analytics using Amazon Athena and Power BI.

---

## Project Overview

The goal of this project is to build a complete, production-style data pipeline around YouTube trending data and create a dataset that can answer analytical questions.
The pipeline collects the data daily at 9 PM PKT and stores the data using a country and date-based partitioning strategy.

### Countries

The pipeline currently covers:

1. Algeria
2. Bahrain
3. Egypt
4. Iraq
5. Israel
6. Jordan
7. Kuwait
8. Lebanon
9. Libya
10. Morocco
11. Oman
12. Qatar
13. Saudi Arabia
14. Tunisia
15. United Arab Emirates
16. Yemen

**Note:** YouTube's API does not currently support every MENA country (i.e., Palestine and Syria) as a valid regionCode. Countries unsupported by the API are therefore excluded from the ingestion process.

---

## Architecture

The project follows a **Medallion Architecture** consisting of three layers:

![pipeline.png](https://github.com/ReehaKhan/youtube-data-pipeline/blob/master/images/medallion-architecture.png)

### Data Modelling

In the Gold Layer, the data is transformed into a **Star Schema** consisting of one central fact table and five dimension tables.

![pipeline.png](https://github.com/ReehaKhan/youtube-data-pipeline/blob/master/images/star-schema.png)

---

## Pipeline Orchestration

The workflow is:

![pipeline.png](https://github.com/ReehaKhan/youtube-data-pipeline/blob/master/images/pipeline.png)

### Error Handling 
If any major pipeline step fails, the Step Functions workflow catches the error and publishes a notification to an SNS topic.
An email notification is then sent so that the pipeline failure can be identified without manually monitoring the workflow.

---

## AWS Technology Stack

The following AWS services are used throughout the pipeline:

- Amazon S3 — Data lake storage
- AWS Lambda — YouTube API data ingestion
- AWS Glue ETL — Bronze-to-Silver and Silver-to-Gold transformations
- AWS Glue Data Catalog — Metadata and table definitions
- AWS Glue Crawlers — Schema discovery and catalog updates
- AWS IAM — Access control and service permissions
- AWS Step Functions — Pipeline orchestration
- Amazon SNS — Failure email notifications
- Amazon EventBridge — Daily pipeline scheduling
- Amazon Athena — SQL-based analytical querying

---

## Repository Structure
```
youtube-data-pipeline/
│
├── etl_jobs/
│   ├── bronze_to_silver.py
│   └── silver_to_gold.py
│
├── lambda functions/
│   └── youtube_api_ingestion.py
│
├── step_functions/
│   └── pipeline_orchestration.json
│
├── images/
│   ├── medallion-architecture.png
│   ├── star-schema.png
│   └── pipeline.png
│
└── README.md
```

# About Me

Hiya! I'm **Reeha Khan**, a data scientist with 2.5 years of professional experience. I spent 1 year working at a Think Tank on data-driven narratives for the Government of Pakistan and 1.5 years researching AI in healthcare with 2 publications. I'm passionate about all things data, and you'll find me learning new skills!! :)

Feel free to connect with me:

[![LinkedIn](https://img.icons8.com/?size=30&id=xuvGCOXi8Wyg&format=png&color=000000)](https://www.linkedin.com/in/reehakhan/)
[![Email](https://img.icons8.com/?size=30&id=nQ4dZIRCI0nW&format=png&color=000000)](mailto:khanreeha22@gmail.com)
