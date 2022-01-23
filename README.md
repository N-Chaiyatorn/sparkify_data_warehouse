# Sparkify: Amazon Redshift Data Warehouse

A music streaming startup, Sparkify, has their own user activity database and song database. Both data resides in S3 Buckets, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

This project, *__Sparkify: Amazon Redshift Data Warehouse__*,  responsible for extracting the data from S3, stages them in Redshift, and transforms data into a set of dimensional tables and a fact table for their analytics team to continue finding insights in what songs their users are listening to. This project follow the [Amazon Redshift best practices] to serve the best quality Redshift Data Warehouse. As a result, the ETL pipeline will work properly and the performance will be optimized in the meantime.

## Redshift Data Warehouse Model
The columns and data type within each of below table can be checked out directly in `sql_queries.py`
### Fact Table
- _songplays_ - Records in event data associated with song plays.
    - Since the _songs_ table is large and looks more important than _artists_ and _time_ table (not include _users_ table since it already used 'all' strategy), so the _song_id_ will be a distribution key for join optimization with the _songplays_ table.
    - Use _start_time_ as a sort key since the query on this table will more likely to be time-filtered.
    
### Dimension Tables
- _users_ - users in the app.
    - Use 'all' distribution style since this table size is small (~100 rows).
- _songs_ - songs in music database.
    - Since this table contains the most number of rows and the query will more likely to ask about the songs e.g. 'What is the most trendy song in this week?', so I decide to use _song_id_ as a distribution key for join optimization with songplays table.
- _artists_ - artists in music database.
    - Use 'auto' distribution style to let Redshift determines whether the table should use 'all' or 'auto' style
    - Use _artist_id_ as a sort key.
- _time_ - timestamps of records in songplays broken down into specific units.
    - Use 'auto' distribution style to let Redshift determines whether the table should use 'all' or 'auto' style
    - Use _start_time_ as a sort key.
    
## Extra considerations
- _Users_ table insert query is already considered for user level upgrade and possibility of the duplication of the same user_id rows as per [Defining table constraints in Redshift],
    > Uniqueness, primary key, and foreign key constraints are informational only; they are not enforced by Amazon Redshift. If your application allows invalid foreign keys or primary keys, some queries could return incorrect results.

## File Description

### sql_queries.py
Contains SQL queries for table creation and ETL process.

### create_tables.py
Execute create table queries from sql_queries.py.
Also, execute drop table queries before creation if the table already exists.

### etl.py
Start ETL pipeline from user activity and song database reside in S3 Buckets to Redshift Data Warehouse staging tables. Lastly, the data will be pipelined to the fact table and dimension tables.

### dwh.cfg
Amazon Redshift Data Warehouse configuration file, contain all credentials needed to connect to S3 Buckets and Redshift cluster. *__Never push filled credentials in the public repository__*, filled `dwh.cfg` in this repository is just for the demonstration purpose.

## Usage
Although there are no scripts to set up the Redshift cluster in this repository. The Redshift cluster must be already set up and all crucial information in `dwh.cfg` must be filled before running the first script.

After the Redshift cluster is available and `dwh.cfg` is filled, run

```sh
python create_tables.py
```

The drop-table and create-table queries within `sql_queries.py` will be executed respectively, this script will create all staging tables, fact tables and dimension tables.
Then, run `etl.py` to start pipelining the events and songs data from S3 Buckets to Amazon Redshift Data Warehouse.

```sh
python etl.py
```

## Dependencies
_Sparkify: Amazon Redshift Data Warehouse_ implemented using [Python] and also uses a number of open source projects to work properly:

- [psycopg2] - PostgreSQL database adapter for the Python programming language.
- [configparser] - Python library for parsing various kinds of configuration file.

[//]: # (References)
   [Amazon Redshift best practices]: <https://docs.aws.amazon.com/redshift/latest/dg/best-practices.html>
   [Python]: <https://www.python.org/>
   [psycopg2]: <https://github.com/psycopg/psycopg2>
   [configparser]: <https://github.com/jaraco/configparser/>
   [Defining table constraints in Redshift]: <https://docs.aws.amazon.com/redshift/latest/dg/t_Defining_constraints.html>
   