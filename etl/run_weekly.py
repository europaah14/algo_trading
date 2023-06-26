'''
ETL script

This script performs ETL process of weekly run
'''

# standard imports
import os
import finnhub
import pandas as pd
from dotenv import load_dotenv

# local imports
from utils.database import RDS
from .etl_tool import ETL

# load environment
load_dotenv('config.env')

# get credentials for rds
rds_cred = [
    os.getenv(key) 
    for key in ['RDS_HOST', 
                'RDS_PORT', 
                'RDS_USER', 
                'RDS_PWD', 
                'RDS_DB']
]

# symbols
symbols = pd.read_csv('./etl/selected_symbols.csv')['symbol']

# log names
log_names = {
    'rds_log': './etl/log/rds_weeklyrun.log',
    'etl_log': './etl/log/etl_weeklyrun.log'
}

# clear previous week's log
for log_name in log_names.values():
    try:
        os.remove(log_name)
    except OSError:
        pass

# ETL weekly run
with (
    finnhub.Client(os.getenv('FINN_API_KEY')) as client,
    RDS(*rds_cred, 20, log_names['rds_log']) as rds
):  
    etl = ETL(client, rds, 20, log_names['etl_log'])
    
    for symbol in symbols:
        etl.run(symbol)
