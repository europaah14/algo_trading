'''
AWS RDS Connection module

This module provides class of functions to connect to an AWS RDS instance,
create tables, query tables, and update tables.
'''

# standard imports
import sys
import psycopg2 as pg
from io import StringIO
from time import time
import pandas as pd

# local imports
sys.path.append('./utils')
from log_tool import LogUtils

class RDS:
    def __init__(self, cred, verbose=20, log_file=None):
        self.cred = cred
        self.conn = None
        self.cur = None
        self.commit = True
        self.logger = LogUtils.setup_logger(
            self.__class__.__name__, 
            verbose,
            log_file
        )
    
    def __enter__(self):
        '''
        RDS Instance Connection
        '''
        try:
            self.conn = pg.connect(**self.cred)
            self.cur = self.conn.cursor()
            self.logger.info('Connected to RDS')
        
        except pg.OperationalError as error:
            self.logger.error(f'Operational Error\n\n{error}')
            # return
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        '''
        commit and disconnect from RDS
        '''
        # check if conn and cur are assigned
        if not self.conn or not self.cur:
            return
        
        if not exc_type and self.commit:
            self.conn.commit()
            self.logger.info('Successfully committed')
        
        elif not self.commit:
            self.logger.info(f'Auto-Commit set to {self.commit}. Transcation not comitted')

        else:
            self.conn.rollback()
            self.logger.error(f'Unable to commit. Transcation rolled back.')

        self.cur.close()
        self.conn.close() 

        self.logger.info('Disconnected from RDS')

    def list_tables(self):
        '''
        display all non-temp user-created table names
        '''
        self.cur.execute('''
            select table_name 
            from information_schema.tables
            where table_schema='public';
        ''')
        
        return [table[0] for table in self.cur.fetchall()]
    
    def create_table(self, table_name, table_config, is_tmp=False):
        '''
        create table
        '''
        if table_name in self.list_tables():
            self.logger.debug(f'Table <{table_name}> already exists. Skipping Creating')
            return
        
        # config of tables
        tmp = 'temporary ' if is_tmp else ''
        col_type_pairs = ','.join(f'{col} {dtype}' for col, dtype in table_config.items())

        self.logger.debug(f'Creating {tmp}table: <{table_name}>')
        self.cur.execute(f'''
            create {tmp}table {table_name} (
                {col_type_pairs}
            );
        ''')
        self.logger.info(f'{tmp}table <{table_name}> created'.capitalize())
    
    def drop_table(self, table_name):
        self.logger.debug(f'Dropping table: <{table_name}>')
        
        try:
            self.cur.execute(f'''
                drop table {table_name}
            ''')
            self.logger.info(f'Table <{table_name}> dropped')
        
        except pg.ProgrammingError as error:
            self.logger.error(f'SQL Error\n\n{error}')

    def load(self, df, table_name):
        '''
        load data to table on RDS while omitting duplicate based on unique keys
        '''
        # write df as StringIO buffer
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        # load to rds
        self.logger.debug(f'Loading to table: <{table_name}>')

        # i. create staging table
        stage_table_name = f'{table_name}_{int(time() * 1e6)}'
        self.cur.execute(f'''
            create temp table {stage_table_name} as
            select * from {table_name}
            where 1 = 0;
        ''')

        # ii. load buffer to staging table
        self.cur.copy_from(buffer, stage_table_name, sep=',')
        
        # iii. load data from staging table to target table and omit duplicates
        self.cur.execute(f'''
            insert into {table_name}
            select * from {stage_table_name}
            on conflict 
            do nothing;
        ''')
        
        if len(df) > self.cur.rowcount:
            self.logger.warning(f'{len(df) - self.cur.rowcount} duplicated entries were omitted')
        self.logger.info(f'{self.cur.rowcount} entries loaded to table: <{table_name}>')

    def retrieve(self, query, table_name='table'):
        '''
        retrieve data based on query
        '''
        self.logger.debug(f'Retrieving from <{table_name}>')
        
        try:
            self.cur.execute(query)
            result = pd.DataFrame(
                self.cur.fetchall(),
                columns=[col[0] for col in self.cur.description]
            )
            self.logger.info(f'Retrieved from <{table_name}>')
        
        except pg.ProgrammingError as error:
            self.logger.error(f'SQL Error\n\n{error}')
            return

        return result
    
    def modify(self, query, table_name='table'):
        '''
        modify (update/insert/upsert) data based on query
        '''
        self.logger.debug(f'Modifying <{table_name}>')
        
        try:
            self.cur.execute(query)
            self.logger.info(
                f'{self.cur.rowcount} entries are modified in <{table_name}>'
            )
        
        except pg.ProgrammingError as error:
            self.logger.error(f'SQL Error\n\n{error}')

    def set_commit(self, commit):
        '''
        set auto-commit
        '''
        self.commit = commit
        self.logger.info(f'Auto-Commit set to {commit}')

    