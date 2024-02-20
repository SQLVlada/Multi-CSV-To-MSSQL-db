# import libraries
import pandas as pd
import os
import sqlalchemy as salch
from datetime import datetime
start_time = datetime.now()

# find all csv files in folder
csv_files = []
for file in os.listdir(os.getcwd()):
    if file.endswith('.csv'):
        csv_files.append(file)

# make new folder
dataset_dir = 'datasets'
try:
    mkdir = 'mkdir {0}'.format(dataset_dir)
    os.system(mkdir)
    print('Directory {0} is created'.format(dataset_dir))
except:
    pass

# move csv files to new folder
for csv in csv_files:
    moved_file = 'move "{0}" {1}'.format(csv, dataset_dir)
    os.system(moved_file)
    print('File {0} is moved to {1} directory'.format(csv, dataset_dir))

# create dataframe from csv files
data_path = os.getcwd() + '/' + dataset_dir + '/'
df = {}
for file in csv_files:
    try:
        df[file] = pd.read_csv(data_path + file, encoding='ISO-8859-1')
    except UnicodeDecodeError:
        df[file] = pd.read_csv(data_path + file, encoding='UTF-8')
# format table and column names
for eachfile in csv_files:
    dataframe = df[eachfile]
    c_table_name = eachfile.lower().replace(" ", "_").replace("?", "").replace("-", "_")\
        .replace(r"/", "_").replace(r"\\", "_").replace("%", "")\
        .replace(r"(", "_").replace(")", "_").replace("$", "")
    table_name = '{0}'.format(c_table_name.split('.')[0])
    print('Table {0} has been created'.format(table_name))
    dataframe.columns = [x.lower().replace(" ", "_").replace("?", "").replace("-", "_")\
                         .replace(r"/", "_").replace(r"\\", "_").replace("%", "")\
                         .replace(r"(", "_").replace(")", "_").replace("$", "") for x in dataframe.columns]
    c_name = dataframe.columns

# connect to SQL DB
    connection_uri = (
        "mssql+pyodbc://LENOVO-L14/Post?driver=ODBC+Driver+17+for+SQL+Server"
    )
    engine = salch.create_engine(connection_uri, fast_executemany=True)


# upload to SQL db
    dataframe.to_sql(table_name,
                     engine,
                     index=False,
                     if_exists="replace")
    print('Table {0} import to db is completed'.format(table_name))
end_time = datetime.now()
print('Duration of a transfer: {}'.format(end_time - start_time))




