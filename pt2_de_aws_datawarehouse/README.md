# ETL Process with Python and SQL

This repository contains Python scripts for performing ETL (Extract, Transform, Load) operations using SQL and Python. The scripts are designed to work with a database and Amazon S3 for data storage and processing.

## Table of Contents

- [Scripts](#scripts)
  - [1. ETL Pipeline Preparation](#1-etl-pipeline-preparation)
  - [2. SQL Table Creation and Deletion](#2-sql-table-creation-and-deletion)
  - [3. SQL Table Data Insertion](#3-sql-table-data-insertion)
- [Usage](#usage)
- [Prerequisites](#prerequisites)
- [Configuration](#configuration)
- [License](#license)

## Scripts

### 1. ETL Pipeline Preparation

[sql_queries.py](sql_queries.py) is a Python script that prepares an ETL pipeline by setting up configurations and defining SQL queries. It includes the following key components:

- Loading configurations from a `dwh.cfg` file.
- Defining SQL queries for creating and dropping tables.
- Defining SQL queries for copying data from S3 into staging tables.
- Defining SQL queries for inserting data into final analytics tables.
- Organizing these SQL queries into lists for different stages of the ETL process.

### 2. SQL Table Creation and Deletion

[create_tables.py](create_tables.py) is a Python script responsible for creating and dropping tables in a SQL database. It includes:

- SQL statements for creating tables for staging, analytics, and dimension data.
- SQL statements for dropping existing tables if they already exist in the database.
- A main function that orchestrates the process by connecting to the database, executing the SQL statements, and closing the connection.

### 3. SQL Table Data Insertion

[etl.py](etl.py) is a Python script that inserts data into SQL tables. It includes:

- SQL statements for inserting data into tables for songplays, users, songs, artists, and time.
- A main function that orchestrates the insertion process by connecting to the database, executing the SQL statements, and closing the connection.

## Usage

To use these scripts, follow these steps:

1. Make sure you have the necessary prerequisites and configurations in place.

2. Execute the scripts in the following order:

   - First, run `etl.py` to prepare the ETL pipeline and define SQL queries.
   - Then, run `create_tables.py` to create and drop tables as needed.
   - Finally, run `sql_queries.py` to insert data into the tables.

## Prerequisites

Before using these scripts, ensure that you have:

- A SQL database (e.g., PostgreSQL) set up and running.
- Amazon S3 buckets with the necessary data for extraction and loading.
- Properly configured `dwh.cfg` file with database and S3 bucket information.

## Configuration

The `dwh.cfg` file should contain configuration settings for your ETL process. Here's an example of the structure:

```plaintext
[CLUSTER]
HOST=your_database_host
DB_NAME=your_database_name
DB_USER=your_database_user
DB_PASSWORD=your_database_password
DB_PORT=your_database_port

[S3]
LOG_DATA=s3://your-s3-bucket/log_data
LOG_JSONPATH=s3://your-s3-bucket/log_jsonpath.json
SONG_DATA=s3://your-s3-bucket/song_data

[IAM_ROLE]
ARN=your_iam_role_arn
