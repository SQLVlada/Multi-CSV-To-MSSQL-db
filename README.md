# Programaticaly reading multiple CSV files from a local folder into MSSQL db tables

## How to find and load multiple CSV files into MSSQL database using Python

If you have a lot of different CSV files in local directories and you want to read them into a MSSQL database, you might think that it is a simple task. 
However, there are some challenges and pitfalls that you need to be aware of. Here I will show you how to use Python pandas and sqlalchemy to find all the CSV files in any directory and load them into a MSSQL database, no matter how many columns or rows each CSV file has.

This will explain the code snippet that performs the following tasks:
- Importing libraries
- Finding and moving CSV files to a new folder
- Creating a dataframe from CSV files
- Formatting table and column names
- Uploading to SQL database

# import libraries
Let's go through the code line by line and see what it does.
The first three lines import the libraries that we need for this code. Pandas is a popular library for data analysis and manipulation. Os is a library that provides functions for interacting with the operating system. Sqlalchemy is a library that allows us to connect to and work with SQL databases.
```python
import pandas as pd
import os
import sqlalchemy as salch
```
# find all csv files in folder
The next few lines create an empty list called csv_files and loop through all the files in the current working directory (os.getcwd()). If the file name ends with .csv, it means it is a CSV file and we append it to the csv_files list.

```python
csv_files = []
for file in os.listdir(os.getcwd()):
    if file.endswith('.csv'):
        csv_files.append(file)
```
# make new folder
The next block of code creates a new folder called datasets. We use a try-except block to handle any possible errors. We use the os.system function to execute a command in the terminal. The command is 'mkdir datasets', which creates a new directory with the name datasets. We use the format method to insert the dataset_dir variable into the command string. We also print the command for debugging purposes. If there is an error, we simply pass and do nothing.

```python
dataset_dir = 'datasets'
try:
    mkdir = 'mkdir {0}'.format(dataset_dir)
    os.system(mkdir)
    print('Directory {0} is created'.format(dataset_dir))
except:
    pass
```
# move csv files to new folder
The next block of code moves all the CSV files from the current working directory to the new datasets folder. We loop through the csv_files list and use the os.system function again to execute a command in the terminal. The command is 'move "filename" datasets', which moves the file with the name filename to the datasets directory. We use the format method again to insert the csv and dataset_dir variables into the command string. We also print the command for debugging purposes.

```python
for csv in csv_files:
    moved_file = 'move "{0}" {1}'.format(csv, dataset_dir)
    os.system(moved_file)
    print('File {0} is moved to {1} directory'.format(csv, dataset_dir))
```    
# create dataframe from csv files
The next block of code creates a dataframe from each CSV file and stores it in a dictionary called df. We create a variable called data_path that contains the path to the datasets folder. We initialize an empty dictionary called df. We loop through the csv_files list and use the pandas read_csv function to read each file as a dataframe. We encoding parameter 'ISO-8859-1' which can handle most of the common characters in European languages. If there is a UnicodeDecodeError, which means that some characters cannot be decoded properly, we use UTF-8 parameter. We also print the file name for debugging purposes.

```python
data_path = os.getcwd() + '/' + dataset_dir + '/'
df = {}
for file in csv_files:
    try:
        df[file] = pd.read_csv(data_path + file, encoding='ISO-8859-1')
    except UnicodeDecodeError:
        df[file] = pd.read_csv(data_path + file, encoding='UTF-8')
    print(file)
```
# format table and column names
The next block of code formats the table and column names for each dataframe. We loop through the csv_files list and access each dataframe from the df dictionary. We create a variable called c_table_name that contains the file name in lower case and replaces any spaces, question marks, dashes, slashes, percentage signs, parentheses, or dollar signs with underscores. This is to make sure that the table name follows the SQL naming conventions and does not contain any special characters that might cause errors. We then create another variable called table_name that contains only the part of the file name before the dot. For example, if the file name is 'sales.csv', the table_name will be 'sales'. We print the table_name for debugging purposes. We then assign a new list of column names to the dataframe.columns attribute. The new list is created by looping through the original column names and applying the same formatting as we did for the table name. We also create a variable called c_name that contains the new column names and print it for debugging purposes.

```python
for eachfile in csv_files:
    dataframe = df[eachfile]
    c_table_name = eachfile.lower().replace(" ", "_").replace("?", "").replace("-", "_")\
        .replace(r"/", "_").replace(r"\\", "_").replace("%", "")\
        .replace(r"(", "_").replace(")", "_").replace("$", "")
    table_name = '{0}'.format(c_table_name.split('.')[0])
    print(table_name)
    dataframe.columns = [x.lower().replace(" ", "_").replace("?", "").replace("-", "_")\
                         .replace(r"/", "_").replace(r"\\", "_").replace("%", "")\
                         .replace(r"(", "_").replace(")", "_").replace("$", "") for x in dataframe.columns]
    c_name = dataframe.columns
    print(c_name)
```
# upload to SQL db
The final block of code uploads each dataframe to a SQL database. We create a variable called connection_uri that contains the connection string to the database. The connection string specifies the database dialect (mssql), the driver (pyodbc), the server name (ServerName), the database name (dbName), and the driver version (SQL Server ODBC Driver). We use the sqlalchemy create_engine function to create an engine object that connects to the database using the connection_uri. We use the fast_executemany parameter to speed up the insertion process. We then use the pandas to_sql function to write each dataframe as a table in the database. We specify the table_name, the engine, and the index and if_exists parameters. The index parameter is set to False to avoid creating an extra column for the dataframe index. The if_exists parameter is set to replace to overwrite any existing table with the same name. We also print some messages for debugging purposes.

```python
connection_uri = (
    "mssql+pyodbc://ServerName/dbName?driver=SQL Server ODBC Driver"
)
engine = salch.create_engine(connection_uri, fast_executemany=True)
dataframe.to_sql(table_name, engine, index=False, if_exists="replace")
print('table {0} import to db is completed'.format(table_name))
```

That's it! We have successfully written a code snippet that can find and move CSV files, create dataframes, format table and column names, and upload to SQL database. 
I hope this was helpful and informative.
