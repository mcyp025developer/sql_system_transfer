![Tests](https://github.com/mcyp025developer/sql_system_transfer/actions/workflows/tests.yml/badge.svg)

# sql_system_transfer

sql_system_transfer is a Python Library for transferring tables between SQL Systems.  Currently, the only SQL 
systems that sql_system_transfer is compatible is Microsoft SQL Server and MySQL.

## Description

This project started while I was working as an Actuary for a midsize insurance company.  I was working with data in
two different SQL systems (SQL Server and Oracle). There were certain fields (limits, deductibles, rating factors, ...) 
that were in Oracle staging tables, but IT never moved those fields over to the SQL Server 
environment.  As an Actuary, my job was to create reports and analyze data such as those fields mentioned above, so I 
was constantly moving data between different SQL Systems through dynamic queries.  This project was not used for my job,
but inspired me to create a package that automated the process without using a sql transfer wizard tool.

## Project Status

This Python Library is currently being worked on and is not 100% finished.  The package allows for 
a user to transfer data between systems.  

## Roadmap

What is being worked on:

SQL Table Constraints - Working on identifying the constraints for a table and converting them from one system to
another system.  Scope/Difficulty - Easy/Medium. 

SQL Query Editor - Working on creating a search and replace capability to convert embedded SQL statements from one
system to another.  Scope/Difficulty - Hard.

What my goals are:

Database Migrate - Goal is to move over an entire database between SQL Systems.  Scope/Difficulty - Extreme Hard.

Command Line Feature - Goal is to create an easy-to-use command line application feature for this package.  
Scope/Difficulty - Unknown no experience in this feature.

Add more systems - Goal is to add in functionality for postgresql and Oracle.  Scope/Difficulty - Medium.

## Installation

This package is a work in progress.  In the future the package will be available through the package manager 
[pip](https://pip.pypa.io/en/stable/).  

```bash
pip install package name here
```

## Usage

```python
from sql_system_transfer.engine import SQLSystem, Engine

# SQL Server connection dictionary for pyodbc
mssql_connection_dict = {
    'system': SQLSystem.MSSQL,
    'server': '*****',
    'database': 'AdventureWorksDW',
    'trusted_connection': 'yes',
}

# MySQL connection dictionary for pyodbc
mysql_connection_dict = {
    'system': SQLSystem.MYSQL,
    'server': '*****',
    'database': 'employees',
    'uid': '*****',
    'pwd': '*****',
}

# Tables in AdventureWorksDW in SQL Server
tables = [
    "AdventureWorksDW.dbo.DimAccount",
    "AdventureWorksDW.dbo.DimCurrency",
    "AdventureWorksDW.dbo.DimCustomer",
    "AdventureWorksDW.dbo.DimDate",
]

mssql_engine = Engine(**mssql_connection_dict)  # connects to sql server instance 
mysql_engine = Engine(**mysql_connection_dict)  # connects to mysql instance

# engine_transfer_tables takes AdventureWorksDW.dbo.DimAccount, AdventureWorksDW.dbo.DimCurrency, 
# AdventureWorksDW.dbo.DimCustomer and AdventureWorksDW.dbo.DimDate and moves the data from sql server to mysql.
mssql_engine.engine_transfer_tables(tables=tables, engine=mysql_engine)

```
## Contributing

Will update in the future.

## License
[MIT](https://choosealicense.com/licenses/mit/)

## SQL Systems

MySQL Version 8.0.28

SQL Server Version Microsoft SQL Server 2019 (RTM-GDR) (KB4583458) - 15.0.2080.9 


## SQL Drivers

The following drivers must be installed:

MySQL Driver - MySQL ODBC 8.0 Unicode Driver

SQL Server Driver - ODBC Driver 17 for SQL Server

## SQL Datatypes

The following datatypes are not supported at the moment:

1. MySQL GEOMETRY
2. MySQL GEOMETRYCOLLECTION
3. MySQL LINESTRING
4. MySQL MULTILINESTRING
5. MySQL MULTIPOINT
6. MySQL MULTIPOLYGON
7. MYSQL POINT
8. MySQL POLYGON

If you try and move a table(s) in MySQL to another system the program will bypass the table(s) that have one of these
datatypes.