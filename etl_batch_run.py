'''
ETL script

This script performs ETL process of batch run
'''

# standard imports
import sys
import os
import finnhub
from datetime import datetime
import pandas as pd

# local imports
from utils.credentials import *
from utils.database import RDS
from utils.etl_tool import ETL

log_names = {
    'rds_log': './log/rds_batchrun.log',
    'etl_log': './log/etl_batchrun.log'
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
        etl.run(symbol, datetime(2010,1,1), datetime(2023,6,16))
