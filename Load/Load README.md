# Load Folder

## Overview

The **Load Folder** in the Sparta ETL project contains a script specifically dedicated to the load phase of the ETL process. 
This phase focuses on loading data into azure data studio using sql queryies,by creating and populating tables with the data from the dataframes created in the **Normalise** class within **notmalise.py** script, found transform folder of this Repo .

## The Process:

- The Python code provided in **load.py** defines a class named **Loadd** designed for an ETL (Extract, Transform, Load) process on data stored in an Amazon S3 bucket. 
- The class encapsulates a method to connect to the a database already created and named **(Name of your choice)**,create and populate tables using the dataframes provided in the **Normalise** class within **notmalise.py** script
- The **Normalise** class within **notmalise.py** script must be imported to allow access to all the dataframes with the required data.
- The class utilises the pyodbc library to connect to the SQL Server which allows for a connection to be made with your database on azure data studio.
- The order in which the tables are created enures that foreign keys can refrence already existing foreign keys.
- The excutions of the creation of all the tables are commited to ensure the tables appear in your database.
- Then finally the connection to the SQL server is closed 

## Contents

1. **load_data_to_sql.py:**
   - This Python script is responsible for connecting to a SQL Server database and loading normalised data from various tables.
   - It utilizes the pyodbc library for database interactions.

## Usage

1. **Configure SQL Server Connection:**
   - Update the `server`, `database`, `username`, and `password` variables in the script with your SQL Server connection details.
   - below is an example to make the SQL server connection, you must adapt the strings for your own details.
   ```bash
     server = 'server you are running your sql server, e.g. localhost,1433 '
     database = 'your_databse_name'
     username = 'your_server_username'
     password = 'your_server_password'

   ```

3. **Install Required Libraries:**
   - Install the necessary Python libraries using the following command:
     ```bash
     pip install pyodbc pandas
     ```
4. **Import:**
  - Import normalise class in normalise.py and import pyodbc:  
   ```bash
     import pyodbc
     from Transform.normalise import Normalise
     ```

6. **Run the Script:**
   - Execute the `load_data_to_sql.py` script to initiate the process of creating tables and loading data into the SQL Server database.
     ```bash
     python load_data_to_sql.py
     ```
     
## Functionality

### Class: Load

#### Methods:

1. **`loading_data_to_sql`:**
   - Establishes a connection to the SQL Server database.
   - Creates tables for various entities (e.g., trainer, weaknesses, strengths) based on the normalized data.
   - Inserts data into the created tables.
   - Commits changes to the database.
   - loses the connection to the SQL server.
