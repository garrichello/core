from .base.main_app import MainApp
from .tasks import app

import logging
from logging.config import fileConfig

fileConfig('core/logging_config.ini')
logger = logging.getLogger()

__all__ = ['MainApp']
__prog__ = 'Core'
__author__ = 'Igor Okladnikov'

__version__ = '1.2.0a'
