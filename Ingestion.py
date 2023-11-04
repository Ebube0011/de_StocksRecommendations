# -*- coding: utf-8 -*-
"""
Created on Wed Oct 11 11:45:30 2023

@author: rm
"""

# import the modules
import pandas as pd
import json
from datetime import datetime
import s3fs
import requests
from time import sleep

def Extraction():
    """Extract the data from the web Api"""
    # add url
    symbols = ['TSLA', 'DIS', 'AAPL', 'BABA', 'NVDA', 'BAC', 'NFLX', 'PYPL', 'MCD', 'GOOG', 'AMZN', 'BRK.B', 'AMD', 'INTC', 'PFE']
    stock_list = []
    Errors = {}
    Result = {}
    for symbol in symbols:
        url = f"https://yahoo-finance15.p.rapidapi.com/api/yahoo/qu/quote/{symbol}/financial-data"
    
        headers = {
            "X-RapidAPI-Key": "b33fc064admsh6d68833841ea5a1p10fca7jsnce3e8e1254ab",
            "X-RapidAPI-Host": "yahoo-finance15.p.rapidapi.com"
            }
        
        response = requests.get(url, headers=headers)
    
        # Get wanted data and clean up data
        try:
            stock = response.json()['financialData']
            refined_info = {
                'Symbol': symbol,
                'OperatingCashflow' : stock['operatingCashflow']['raw'],
                'ReturnOnAssets' : stock['returnOnAssets']['raw'],
                'GrossMargin' : stock['grossMargins']['raw'],
                'TotalDepth' : stock['totalDebt']['raw']
                }
            stock_list.append(refined_info)
            sleep(1)
        except:
            #Errors[symbol] = response.json()['error']
            sleep(1)
   
    # Compile Result
    Result["Stocks"] = stock_list
    Result["Errors"] = Errors
    
    return Result
    
def Transformation(data):
    """Transform the acquired data (clean and organize the data)"""

    try:
        # Gets Stocks Data
        result = {}
        Error = "Getting stocks list"
        Stocks = data["Stocks"]

        # Put data in dataframe
        Error =  "Converting to df"
        df = pd.DataFrame(data=Stocks)
        result["Data"] = df
    except:
        print("An error occured while ", Error)
        #col_names =  ['A', 'B', 'C']

        # create an empty dataframe
        #df  = pd.DataFrame(columns = col_names)

    return result

def Loading(df):
    """Load the data into the repository (database, warehouse or lake)"""

	df.to_csv("s3://ebube-project-bucket/Stocks_Data.csv", index=False, header=True)
        