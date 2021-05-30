#!/home/tom/projects/VENVS/venvStd_p3/bin python

"""
##############################################################################
# title: m5 datachallange for a job @ 7lytix
# author: Thomas Jahn
# git: https://github.com/tomseman
# date: 2021-05-17
# status: Development
#
# about: load m5 data into customized data model (postgresql)
#
# requirements: 
#       * postgreSQL server with permissions to create, delete tables
#       * permissions to install python packages pandas, numpy  
#       * 3 files: calendar.csv, sell_prices.csv, sales_train_*.csv 
#       
#
# to be improved or added :
#       * upload speed of data (use bulk copy)
#       * internationalisation of data model
#       * error handling
#       * more options for customisation (e.g. choose schema)
#       * create foreign keys
#       * flag is working_day
#       * walmart.v_event: dynamic creation: right now its limited to 3 events per day per country
#
# execute the script in terminal:
#               python main.py <calendarData.csv> <priceData.csv> <salesData.csv> <connectionString>
#           e.g.: python main.py "data/calendar.csv" "data/sell_prices.csv" "data/sales_train_evaluation.csv" "postgresql://user:password@localhost:5432/lnz_sale"
#
# issues:
#   * week_of_year: 01-01 is part of last years week
#
##############################################################################
"""

import subprocess       #universelle (os uebergreifende) Schnittstelle zum Betriebssystem
import sys              #for detailed system information

##### install modules if needed #####
def install(package):
    subprocess.check_call([sys.executable, "-m", "pip", "install", package])

install('pandas')
install('numpy')
install('sqlalchemy')

#### import modules ####

import pandas as pd
import numpy as np
from sqlalchemy import create_engine

#### import custom functions ####
import upload_functions as myfun


print ("Script %s started" % (sys.argv[0]))

#### get arguments or use defaults ####
if (len(sys.argv) - 1) != 4 : 
    print("Default csv names will be used")
    path_calendar = "data/calendar.csv"
    path_prices = "data/sell_prices.csv"    
    path_saleAmounts = "data/sales_train_evaluation.csv"
    connectionString = "postgresql://tom:1a5d9g@localhost:5432/lnz_sale"
else :
    path_calendar = sys.argv[1]
    path_prices = sys.argv[2]
    path_saleAmounts = sys.argv[3]
    connectionString = sys.argv[4]


##### database settings #####
engine = create_engine(connectionString)

##### create tables and schema #####
fd = open("postgresql_scripts/createDB.sql", 'r')
sqlFile = fd.read()
fd.close()
sqlFile = sqlFile.replace("\n", " ").strip()
commandList = [x for x in sqlFile.split(';') if len(x) > 0] #separte different sql commands on ';' and remove empty list elements
for x in commandList:
    with engine.begin() as cnx:
                cnx.execute(x)

##### load datasets ####
salesData = pd.read_csv(path_saleAmounts)
calendarData = pd.read_csv(path_calendar)
sellPrices = pd.read_csv(path_prices)

#### update surroundin data ####
print("Update surrounding data: dates, stores, items, events")
myfun.update_surrounding(salesData, calendarData, engine, pd)

#### update main data ####
print("Update prices and sale amounts")

#upload_salesDate: function on chunks of salesData (because of memory shortage) ~ very slow beacuas of INSERT INTO STATEMENT
CHUNK_SIZE = 10
i = 0
for x in np.array_split(salesData, max(round(salesData.shape[0]/CHUNK_SIZE),1)):
        myfun.upload_salesData(x, calendarData, sellPrices, engine, pd)
        i += 1 
        print(i, "von", round(salesData.shape[0]/CHUNK_SIZE))


print("Upload finished")





