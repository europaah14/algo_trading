'''
ETL script

This script performs ETL process of batch run
'''

# standard imports
import os
import finnhub
from datetime import datetime
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
    'rds_log': './etl/log/rds_batchrun.log',
    'etl_log': './etl/log/etl_batchrun.log'
}

# ETL batch run
with (
    finnhub.Client(os.getenv('FINN_API_KEY')) as client,
    RDS(*rds_cred, 20, log_names['rds_log']) as rds
):  
    etl = ETL(client, rds, 20, log_names['etl_log'])
    
    for symbol in symbols:
        etl.run(symbol, datetime(2010,1,1), datetime(2023,6,16))
