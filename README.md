# SQL_datawarehouse
A production-style data warehouse built using SQL that ingests raw data, cleans 
and transforms it, and serves analytics-ready datasets using a Medallion Architecture.

## Architecture

This project follows the Medallion Architecture:

Bronze  -> Raw ingestion layer  
Silver  -> Cleaned & transformed data  
Gold    -> Analytics-ready business views

![image alt](https://github.com/abhi8934/SQL_datawarehouse/blob/main/Medallion+architecture.png?raw=true)

## Tech Stack

- SQL (SQL Server)
- ETL Pipelines
- Star Schema Modeling
- Data Cleaning & Transformations
- Views for Analytics Layer

## Data Modeling

- Designed relational schema
- Created fact & dimension tables
- Implemented star schema for analytics
![image alt](https://github.com/abhi8934/SQL_datawarehouse/blob/main/Star_schema.png?raw=true)
