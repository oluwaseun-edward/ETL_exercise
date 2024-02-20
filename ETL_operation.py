import os
from zipfile import ZipFile
import glob
from datetime import datetime
import requests
import pandas as pd



# Download the data and write it to a file which is saved in the data_source folder.
url = ('https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork'
       '-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/datasource.zip')
response = requests.get(url)
# download data via an HTTP request.
data = response.content

# Specify the directory to store the downloaded data, as well as full path for the datafile itself.
data_file_path = os.path.join(os.getcwd(), 'data_source')
zip_data_file = os.path.join(data_file_path, 'data.zip')

# write data to zip file
with open(zip_data_file, 'wb') as data_file:
    data_file.write(data)

# Unzip the zip file
with ZipFile(zip_data_file, 'r') as unzipped_data:
    unzipped_data.extractall(path=data_file_path)

"""
Implement Data Extraction process. Extract data from the different formats as contained 
in the zip file and add them to a pandas dataframe. 
"""


# Define functions to extract data from .csv, .json, and .xml to a pandas dataframe.
def extract_csv(csv_file):
    df = pd.read_csv(csv_file)
    return df


def extract_json(json_file):
    df = pd.read_json(json_file, lines=True)
    return df


def extract_xml(xml_file):
    df = pd.read_xml(xml_file, xpath="/root/row")
    return df


# define another function to apply the appropriate function to the data according to
# the file extension.
def extract(file_path):
    csv_directory = os.path.join(file_path, '*.csv')
    json_directory = os.path.join(file_path, '*.json')
    xml_directory = os.path.join(file_path, '*.xml')
    csv_data = []
    json_data = []
    xml_data = []
    for csv_file in glob.glob(csv_directory):
        csv_data.append(extract_csv(csv_file))

    for json_file in glob.glob(json_directory):
        json_data.append(extract_json(json_file))

    for xml_file in glob.glob(xml_directory):
        xml_data.append(extract_xml(xml_file))

    # concatenate the lists into pandas dataframes
    csv_df = pd.concat(csv_data, ignore_index=True)
    json_df = pd.concat(json_data, ignore_index=True)
    xml_df = pd.concat(xml_data, ignore_index=True)

    # create one dataframe with all data from the different file types
    dataframe = pd.concat([csv_df, json_df, xml_df], ignore_index=True)
    return dataframe



def transform(dataframe):
    """
    Here we create a function to implement data transformation. Specifically, the 
    price data are rounded to two decimal places.
    """
    dataframe['price'] = round(dataframe.price, 2)
    return dataframe


def load_processed_data(processed_data, save_file):
    """
    Then we load the extracted and transformed data into a target file in .csv format,
    which is suitable for our database
    """
    processed_data.to_csv(save_file)
    # return target_file



# Create a function that logs the ETL process. This function writes the following 
# information to a log.txt file: 1. the process in operation and 2. its timestamp. 

log_file = 'log_file.txt'

def log_function(info):
    time_now = datetime.now()
    # convert the time to string format.
    time_format = "%Y-%B-%d-%H:%M:%S:%f"  # year-month-day-hour:minute:seconds:microseconds
    formatted_time = time_now.strftime(time_format)
    # global log_file
    with open(log_file, 'a') as file:
        file.write("{:<30}: {:^35} \n".format(info, formatted_time))


"""
Here the whole ETL process is run and its progress logged.
"""
log_function("ETL Job started")

log_function("Extraction process started")
extracted_data = extract(data_file_path)
log_function("Extraction process completed")

# Log the start and completion of the Transformation process
log_function("Transform process started")
transformed_data = transform(extracted_data)
print(transformed_data.head(4))
log_function("Transform process completed")

# Log the start and completion of the Loading process
log_function("Load process started")
target_file = "processed_data/etl_processed_data.csv"
load_processed_data(transformed_data, target_file)
log_function("Load step completed")

# Log the completion of the ETL process
log_function("ETL Job Completed")
