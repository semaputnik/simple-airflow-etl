# simple-airflow-etl

Simple etl with airflow framework


## How to run etl
1. Copy files from repository to airflow folder
2. Create data folder at airflow folder
3. Change creds.cfg if it is required
4. Enable dag at airflow

## About
Etl contains 3 steps. 

First step collect data updated in interval between current and previous dag run. After that data stored as csv by partitions. 
Second step read existed csv partitions, transform and write data to stage table. Csv files remove after read. 
Third step update partitions at target table by partitions from stage table and truncate stage table.

Deduplication performs by updating files by partitions.
