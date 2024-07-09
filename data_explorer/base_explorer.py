#-*- coding: utf-8 -*-
"""
Create on 2024/3/11 16:45
@Author: huangyanduo

"""

from abc import ABC
from typing import Dict, Optional
import logging

class Object(ABC):
    """
    Market Data Provider
    行情数据Provider
    """

    def __init__(self, adapter_name: str, logger: str = None):
        self.logger = logger if logger else self._get_console_logger()
        self.adapter_name = adapter_name
        pass

    def _get_console_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        return logger
