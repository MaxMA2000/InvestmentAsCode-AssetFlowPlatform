# InvestmentAsCode-AssetFlowPlatform

AssetFlowPlatform is an evovling data data-centric platform, which provides data ingestion, storage, computing, analytics, data science, business intelligence capabilities to *"InvestmentAsCode"* proejct under one platform.

Click [here for checking Master InvestmentAsCode Repository](https://github.com/MaxMA2000/InvestmentAsCode)


# Table of Content
- [Installation](#installation)
- [Usage](#usage)
- [Features](#features)
- [Documentation](#documentation)
    * [Job List](#jobs-list)
- [RoadMap](#roadmap)


## Installation
To install "AssetFlowPlatform" in your local environment, please follow below instructions:
- Python Batch Jobs
    -  Install Python 3.10.0
    -  Use `pip install -r requirements. txt` to install all necessary python dependencies
- MongoDB
    - [MongoDB Community Server Download](https://www.mongodb.com/try/download/community)
    - [MongoDB Compass Download (GUI)](https://www.mongodb.com/try/download/compass)
- PostgreSQL
    - [PostgreSQL Downloads](https://www.postgresql.org/download/)
    - [pgAdmin 4 Downloads (GUI)](https://www.pgadmin.org/download/)
- Airflow
    - [Install Latest Airflow](https://airflow.apache.org/docs/apache-airflow/stable/start.html)
- Superset
    - [Installing Superset from PyPI](https://superset.apache.org/docs/installation/pypi)
- Other Dependencies
    - `.env` File Variables
        - `FMP_API_KEY`: Create Account on [FMP (Free Stock Market) API](https://site.financialmodelingprep.com/developer/docs) and Get API Key
        - Rest: Take `.env.example` as example


## Usage
- To use AssetFlowPlatform, you need to first install dependencies in [Installation](#installation) section
- Then Start the dependency in sequence: ```MongoDB > PostgreSQL > Superset > Airflow```
- Then you can use each components' services, please keep in mind that some components may need further configuration (eg. username & password, connection URL, etc)


## Features

|Component Name |Main Usage Area  |Key Features   |
|---|---|---|
|Airflow   |Data Orchestration   |Orchestrate Python Job workflow, Schedule batch jobs to run with pipeline dependency checking and monitoring   |    
|MongoDB   |Data Storage   |Used for data ingestion into no-sql database   |   
|PostgreSQL   |Data Storage   |Used for storing and serving structured data    |   
|Superset   |Data Analytics   |Used for doing SQL queries to analysis datasets   |   
|Python Batch Jobs   |Data Computation   |Do data ETL computation  |  



## Documentation

### Jobs List

Jobs config are definied in Airflow DAGs (*./airflow/dags*), while the job logic itself is definied in (*./InvestmentAsCode-AssetFlowPlatform/jobs*), while using other created Classes & Util functions.


|Job Name |Job Logic  |Remarks   |
|---|---|---|
|Ingest_FMPData_To_Mongodb.py|Fetch FMP company general info & ETF list and store into MongoDB| The fetched data will be used for validation in stock and crypto pipelines|
|stock_pipeline.py|first fetch stock list from API and store in MongoDB; then for each definied stock, fetch its historical stock price and store in MongoDB; Finally standardize stock info and stock prices data from MongoDB and load into PostgreSQL database | Currently stock lists definied to be fetched are hard-coded in airflow dag file|
|crypto_pipeline.py|Similar as stock_pipeline, instead use crypto instead of stock|Currently crypto lists definied to be fetched are hard-coded in airflow dag file |



## RoadMap


|Development Type | Priority | Details |
|---|---|---|
|`feature`|`High`|Change hard-coded stock and crypto lists into data stored in database, where admin users can update lists according to changing requirements|
|`component type`|`High`|Change all local components from local installation to docker containers, where can be further deployed to cloud envs|
|`metadata`|`High`|Add metadata management tools (datahub etc)|