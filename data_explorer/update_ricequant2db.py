

import os
import os.path
import sys
from venv import logger
# append module root directory to sys.path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data_explorer.ricequant_updater import RicequantDataUpdater
from data_explorer.dolphindb_connector import dolphindb_connector
from data_explorer.rqdatac_md_provider import RqdatacMdProvider
from db_setting import setting
from data_explorer.logger_module import setup_logger

log_path = "./log/"
logger=setup_logger(log_path)
# 实例化子类对象
md_provider = RqdatacMdProvider(setting["md_provider"])
db_connector = dolphindb_connector(setting["dolphindb_loginfo"], logger=logger)

# 实例化更新器
ricequant_updater = RicequantDataUpdater(md_provider, db_connector, logger)

# 调用子类方法更新数据
ricequant_updater.update('2022-01-01', '2023-01-01')
