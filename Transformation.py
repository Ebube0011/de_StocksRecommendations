# -*- coding: utf-8 -*-
"""
Created on Wed Oct 11 11:46:18 2023

@author: rm
"""

# import the modules
from datetime import datetime
import mysql.connector
import requests
from bs4 import BeautifulSoup as soup
import pandas as pd
import numpy as np


def Transform():
    """Load the data into the repository (database, warehouse or lake)"""
    
    # add url
    url = 'https://en.wikipedia.org/wiki/list_of_largest_recorded_music_markets'
    
    # download to dataframe
    df = pd.DataFrame(url)
    
    # Connect to dbms
    db = mysql.connector.connect(
        host= 'localhost',
        user= 'root',
        passwd= 'Peter-602',
        database= 'Stocks_Data'
        )
    
    # Make a cursor object to control the database
    mycursor = db.cursor()
    
    # Prepare dataframe
    records_to_insert = list(df.itertuples(index=False, name=None))
    
    year = 2021 # datetime.now().year
    table_name = f"Stocks_{year}"
    # Define queries
    query_0 = f"DROP TABLE IF EXISTS {table_name}"
    query_1 = "CREATE TABLE scraped_data \
                        (Symbol VARCHAR(10) PRIMARY KEY, \
                         OperatingCashflow DECIMAL(19, 2), \
                         ReturnOnAssets DECIMAL(5, 4), \
                         GrossMargin DECIMAL(19, 2), \
                         TotalDepth DECIMAL(19, 2))"  
    query_2 = "INSERT INTO scraped_data \
                                (Symbol, OperatingCashflow,  ReturnOnAssets, GrossMargin,TotalDepth) \
                                VALUES (%s, %s, %s, %s, %s)"
    
    # Create table
    mycursor.execute(query_0)
    mycursor.execute(query_1)
    db.commit()
    
    # Execute commands to insert data into database
    mycursor.executemany(query_2, records_to_insert)
    
    # Make the commands permanent
    db.commit()
    
    # Clean up
    db.close()
    
       
# if the module is not imported, run the main process
if __name__ == "__main__":
    Transform()