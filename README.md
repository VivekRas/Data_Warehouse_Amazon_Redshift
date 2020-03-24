# Data Warehouse on the cloud (Amazon_Redshift)
## Tools - SQL, Python and AWS

An ELT pipeline that extracts data from S3, stages it in Redshift, and transforms it into a set of dimensional tables

## Purpose
Sparkify has grown their business and wants to move their processes and data on the cloud. Since their data has grown, and they do not want to invest in an inhouse server, they need a solution that can scale up and scale down as their business goes thru the cycles of growth and downturn

Their purpose is three fold 
1. They need a cost effective solution for storing and mainitaining the data, that can be quickly scaled up / down
2. Our choice of schema is star schema and we are implementing it on the cloud (Amazon Web Services - Redshift)
2. They need an ETL pipeline to be created, that can automatically update the data base

## Database schema design

Implemented a Star schema
1. We want to enable the user to quickly get the data without writing complex joins. 
2. Since it allows user ease of access, with only 2NF level normalisation

*Data is in a structured form now (extracted from json format to a more readable table format). 
We have done data quilty checks - cleaned it up, removed duplicates, removed null values* 

## DataWarehousing solution

Steps
1. Connected Amazon Redshift to Amazon S3 bucket where data was housed in Json formats
2. Pulled data from S3 and created staging tables in Relational data base (columnar storage)
3. Using the staging tables created dimension and fact tables as per star schema

## Details about Tables

**Staging tables**

- Staging_events (8056 rows) : Staging table used to load data to fact table and dimension tables - songs and artists
- Staging_songs (14896 rows) : Staging table used to load data to fact table and dimension tables - users and time

**Fact Table**

- songplays (319 rows) : Matching records between both staging tables, matched on Artist name, song title and duration of song

**Dimension Tables**

- users table (104 unique rows) : details for each user, who used NextSong feature
- songs and artists tables : (14896 rows each) : details on songs and artists
- time (6813 unique rows) : Time stamps and time details for users who used NextSong feature

## ELT pipeline (how to run the program)

How to run the program
- Step 0: Make sure that the data is stored in AWS s3 bucket in json format. Songs data in s3://udacity-dend/song_data and logs / events data in s3://udacity-dend/log_data
- Step 1: Launch a new cluster (with a minimum of 4 nodes) and update the [CLUSTER] HOST address and [IAM_ROLE] Arn in dwh.cfg to enable Redshift to connect to S3 bucket 
- Step 2: cmd > "python create_tables.py"
- Step 3: cmd > "python etl.py"

The respective Postgres database tables gets updated with the records 

- Step 4: Now we can open AWS Redshift > Query editor to run queries on the database

## Project Template
- `create_tables.py` drops and creates our fact and dimension tables for the star schema in Redshift.
- `etl.py` loads data from S3 into staging tables on Redshift and then processes that data into your analytics tables on Redshift.
- `sql_queries.py` contains all your sql queries, and is imported into the files above.
