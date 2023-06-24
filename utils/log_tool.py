'''
Logging module

This module provides logging tool
'''

# standard imports
import logging

# constant definitions
LOG_FORMAT = {
    'fmt': (
        'Time: %(asctime)s\n'
        'Source: %(name)s\n'
        'Level: %(levelname)s\n'
        'Message: %(message)s\n'
        '-----------------------------------'
    ),

    'datefmt': '%Y-%m-%d %H:%M:%S'
}

class LogUtils:

    @staticmethod
    def setup_logger(name, verbose=20, log_file=None):
        # get logger
        logger = logging.getLogger(name)
        
        # set level
        logger.setLevel(verbose)
        
        # reset handler
        logger.handlers = []

        # add handler
        if log_file:
            handler = logging.FileHandler(log_file) 
        else:
            handler = logging.StreamHandler()
        
        handler.setFormatter(
            logging.Formatter(**LOG_FORMAT)
        )
        logger.addHandler(handler)

        return logger
