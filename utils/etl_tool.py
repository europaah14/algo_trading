'''
ETL module

This module provides class of functions to run ETL process, 
collecting candle sticks data and loading to database (AWS RDS)
'''

# stardard imports
import sys
import finnhub
import pandas as pd
from datetime import datetime, timedelta, timezone

# local imports
sys.path.append('./utils')
from log_tool import LogUtils

# constant definitions
CANDLE_STICKS_COLS = {
    'o': 'open',
    'c': 'close',
    'h': 'high',
    'l': 'low',
    'v': 'volume',
    't': 'ts'
}

CANDLE_STICKS_CONFIG = {
    'symbol': 'varchar(10)', 
    'date': 'date', 
    'open': 'float(4)',
    'close': 'float(4)', 
    'high': 'float(4)', 
    'low': 'float(4)', 
    'volumne': 'bigint', 
    'ts': 'bigint',
    'primary key': '(symbol, date)'
}

LAST_UPDATE_CONFIG = {
    'symbol': 'varchar(10)', 
    'date': 'date',
    'primary key': '(symbol)'
}

class ETL:
    def __init__(self, api_client, db, verbose=20, log_file=None):
        # following dependency injection, pass client and db instance
        self.api_client = api_client
        self.db = db
        self.logger = LogUtils.setup_logger(
            self.__class__.__name__,
            verbose,
            log_file
        )
    
    def extract(self, symbol, ts_start, ts_end):
        '''
        download candle sticks data 
        '''
        self.logger.debug(f'Downloading {symbol}\'s candle stick data')
        stock = self.api_client.stock_candles(symbol, 'D', ts_start, ts_end)
        self.logger.info(f'Download of {symbol}\'s candle stick data completed')
        self.logger.debug(f'Downloaded {symbol}\'s candle sticks data:\n{stock}')

        return symbol, stock
    
    def transform(self, symbol, raw_data):
        '''
        Reformat raw_data
        '''
        if raw_data['s'] == 'no_data':
            self.logger.warning(f'{symbol}\'s candle sticks data is not available on Finnhub')
            return

        # formatting
        self.logger.debug(f'Transforming {symbol}\'s raw candle sticks data')
        data = (
            pd.DataFrame(raw_data)[CANDLE_STICKS_COLS.keys()]
            .rename(columns=CANDLE_STICKS_COLS)
        )
        data.insert(0, 'symbol', symbol)
        # no timezone applied since ts in data correponds to date in UTC
        data.insert(1, 'date', pd.to_datetime(data.ts, unit='s'))
        
        self.logger.info(f'Transformation of {symbol}\'s raw candle sticks data completed')
        self.logger.debug(f'Transformed {symbol}\'s raw candle sticks data:\n{data}')

        return data
    
    def load(self, df, table_name):
        '''
        Load df to table_name in rds
        '''
        self.logger.debug(f'Loading to <{table_name}>')
        self.db.create_table(table_name, CANDLE_STICKS_CONFIG)
        self.db.load(df, table_name)
        self.logger.info(f'Loading to <{table_name}> completed')

    def get_default_start(self, symbol, table_name):
        '''
        Retrieve previous start date
        '''
        query = f'''
            select date from {table_name}
            where symbol = '{symbol}';
        '''
        date = self.db.retrieve(query, table_name)

        try:
            start = datetime(*date.iat[0, 0].timetuple()[:3]) + timedelta(1)
            self.logger.info(f'{symbol}\'s start date is set as {start.strftime("%Y-%m-%d")}')
            return start

        except IndexError:
            self.logger.error(f'{symbol} does not exist in <{table_name}>')
            return -1
    
    def get_default_end(self, symbol):
        end = datetime.now()
        self.logger.info(f'{symbol}\'s end date is set as {end.strftime("%Y-%m-%d")}')

        return end

    def update_end_date(self, symbol, end, table_name):
        '''
        update end date 
        '''
        # initialize if not exists
        self.db.create_table(table_name, LAST_UPDATE_CONFIG)

        # update last updated end date if valid
        upsert_query = f'''
            insert into last_update
            values ('{symbol}', '{end}')
            on conflict (symbol) 
            do update set date = excluded.date 
            where last_update.date < excluded.date;
        '''
        self.db.modify(upsert_query, table_name)
        
    def run(
        self,
        symbol, 
        start=None, 
        end=None, 
        data_table_name='candle_sticks',
        date_table_name='last_update'
    ):  
        # set start, end
        start = start or self.get_default_start(symbol, date_table_name)
        if start == -1:
            return
        end = end or self.get_default_end(symbol)
        
        ts_start = int(start.replace(tzinfo=timezone.utc).timestamp())
        ts_end = int(end.replace(tzinfo=timezone.utc).timestamp())

        # error handler based on start, end
        if ts_start > ts_end:
            self.logger.error(
                f'The start date {start.strftime("%Y-%m-%d")} > end date {end.strftime("%Y-%m-%d")}'
            )
            return

        # run ETL
        result = self.transform(
            *self.extract(symbol, ts_start, ts_end)
        )
        if result is not None:
            self.load(result, data_table_name)
            self.update_end_date(
                symbol, 
                result.iloc[-1].date.strftime('%Y-%m-%d'), 
                date_table_name
            )

        self.logger.info(
            f'{start.strftime("%Y%m%d")}-{end.strftime("%Y%m%d")} ETL for {symbol} completed'
            )

