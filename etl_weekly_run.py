'''
ETL script

This script performs ETL process of weekly run
'''

# standard imports
import sys
import os
import finnhub
import pandas as pd

# local imports
from utils.credentials import *
from utils.database import RDS
from utils.etl_tool import ETL

log_names = {
    'rds_log': './log/rds_weeklyrun.log',
    'etl_log': './log/etl_weeklyrun.log'
}

for log_name in log_names.values():
    try:
        os.remove(log_name)
    except OSError:
        pass

symbols = [
    'AAPL',
    'NVDA',
    'TSLA',
    'TSM',
    'MSFT'
]

with (
    finnhub.Client(finnhub_api_key) as client,
    RDS(rds_cred, 20, log_names['rds_log']) as rds
):  
    etl = ETL(client, rds, 20, log_names['etl_log'])
    
    for symbol in symbols:
        etl.run(symbol)
