"""
Created on Wed Oct 11 11:45:30 2023

@author: rm
"""
# import the modules
import os
import pandas as pd
import json
import glob
from datetime import datetime
import s3fs
import requests
from time import sleep

def extract_financial_data():
    ''' Extract stock data data from the yahoo finance api'''
    # add url
    symbols = ['TSLA', 'DIS', 'AAPL', 'BABA', 'NVDA', 'BAC', 'NFLX', 'PYPL', 'MCD', 'GOOG', 'AMZN', 'AMD', 'INTC', 'PFE']
    stocks_data = {}
    Errors = {}
    api_key = os.environ.get('YAHOO_FINANCE_KEY')
    api_host = os.environ.get('YAHOO_FINANCE_HOST')
    url = os.environ.get('FINANCE_DATA_URL') 


    for symbol in symbols: 
        
        querystring = {"ticker":symbol,"module":"financial-data"}
        
        headers = {
            "X-RapidAPI-Key": api_key,
            "X-RapidAPI-Host": api_host
            }
        
        try:
            # get responce
            response = requests.get(url, headers=headers, params=querystring)
            
            # get the data body
            stock_data = response.json()
            stocks_data[symbol] = stock_data

        except:
            stock_data = response.json()['message']
            Errors[symbol] = stock_data

        # delay api query for 1 second
        sleep(1)

    # .dump creates the json file from dict
    with open('financial_data.json', 'w') as f:  # writing JSON object
        json.dump(stocks_data, f)
    with open('errors/errors.json', 'w') as f:  # writing JSON object
        json.dump(Errors, f)

# json extract function
def extract_from_json(file_to_process):
    ''' Extract data from json files and load to dataframe'''
    # Open JSON file 
    with open(file_to_process, 'r') as openfile: 
        # Reading from json file 
        json_object = json.load(openfile) 

    stocks_list = []
    
    # loop through symbols in file
    for key in json_object:
        # Get wanted data and clean up data
        refined_info = {
            'Symbol': key,
            'OperatingCashflow':json_object[key]['body']['operatingCashflow']['raw'],
            'ReturnOnAssets':json_object[key]['body']['returnOnAssets']['raw'],
            'GrossMargin':json_object[key]['body']['grossMargins']['raw'],
            'TotalDepth':json_object[key]['body']['totalDebt']['raw']
            }
            
        stocks_list.append(refined_info)

    dataframe = pd.DataFrame(stocks_list)
    return dataframe 

# extract function
def Extract():
    """Extrace Stock data and place in dataframe"""

    # get data from api
    extract_financial_data()

    extracted_data = pd.DataFrame(columns=['Symbol','OperatingCashflow','ReturnOnAssets','GrossMargin','TotalDepth']) # create an empty data frame to hold extracted data
      
    # process all json files
    for jsonfile in glob.glob("*.json"):
        extracted_data = extracted_data.append(extract_from_json(jsonfile), ignore_index=True)
        
    return extracted_data
    
# transformation function    
def Transform(data):
    """Transform the acquired data (clean and organize the data)"""
        #Convert height which is in inches to millimeter
        #Convert the datatype of the column into float
        #data.height = data.height.astype(float)
        #Convert inches to meters and round off to two decimals(one inch is 0.0254 meters)
        #data['height'] = round(data.height * 0.0254,2)
        
        #Convert weight which is in pounds to kilograms
        #Convert the datatype of the column into float
        #data.weight = data.weight.astype(float)
        #Convert pounds to kilograms and round off to two decimals(one pound is 0.45359237 kilograms)
        #data['weight'] = round(data.weight * 0.45359237,2)
        return data

# loading function
def Load(targetfile,data_to_load): 
    """Load the data into the repository (database, warehouse or lake)"""

    # Load data into target file
    data_to_load.to_csv(targetfile, index=False)

    # Load metadata

# logging function
def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')

# ETL process function
def ETL:
    ''' Run the full ETL process from source system to Storage where the data is loaded.'''

    tmpfile    = "temp.tmp"               # file used to store all extracted data
    logfile    = "logfile.txt"            # all event logs will be stored in this file
    targetfile = os.environ.get('AMAZON_S3_BUCKET')  # file where transformed data is stored
    
    # running the etl process
    log("ETL Job Started")

    log("Extract phase Started")
    try:
        extracted_data = Extract()
        log("Extract phase Ended")
    except:
        log("Error occured in Extract phase. Error : ", e)
        return

    log("Transform phase Started")
    try:
        transformed_data = Transform(extracted_data)
        log("Transform phase Ended")
    except:
        log("Error occured in the Transform phase. Error : ", e)
        return

    log("Load phase Started")
    try:
        Load(targetfile,transformed_data)
        log("Load phase Ended")
    except:
        log("Error occured in the Loading phase. Error : ", )
        return

    log("ETL Job Ended")

if __name__ == "__main__":
    ETL()