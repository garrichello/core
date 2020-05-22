import os
from .base.main_app import MainApp
from .tasks import app

import logging
from logging.config import fileConfig

module_path = os.path.dirname(__file__)
fileConfig(module_path+'/logging_config.ini')
logger = logging.getLogger()

__all__ = ['MainApp']
__prog__ = 'Core'
__author__ = 'Igor Okladnikov'

__version__ = '1.3.0a'
