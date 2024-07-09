# -*- coding = utf-8 -*-
"""
Create on 2024/3/11 16:45
@Author: chenqiuxing
@File: factor_db_test.py
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import copy
import warnings
import pandas as pd
from joblib import Parallel, delayed
from dateutil.relativedelta import relativedelta

import datetime
import pandas as pd
from collections import OrderedDict
import sys
from data_explorer.logger_module import setup_logger

from data_explorer.dolphindb_connector import dolphindb_connector
from data_explorer  import db_setting

from datafeed.DBfeed import DBfeed
from config  import vglobal

from alphalens.tears import (create_returns_tear_sheet,
                      create_information_tear_sheet,
                      create_turnover_tear_sheet,
                      create_summary_tear_sheet,
                      create_full_tear_sheet,
                      create_event_returns_tear_sheet,
                      create_event_study_tear_sheet)

from alphalens.utils import get_clean_factor_and_forward_returns

import  source.factor.factor_utils as f_utils

warnings.filterwarnings("ignore", category=RuntimeWarning)



def factor_wide_test(dbPath, tableName, partitionColumns, tableinfo, sortColumns):
    """宽表创建及写入数据

    Args:
        dbPath : _description_
        tableName : _description_
        partitionColumns : _description_
        tableinfo : 表格内容
        sortColumns : 
    """
    factor_k = tableinfo.reset_index()
    db_conn.creat_table(dbPath, tableName, partitionColumns, factor_k, sortColumns)
    db_conn.write_table(dbPath, tableName, factor_k)

def factor_narrow_test(dbPath, tableName, partitionColumns, tableinfo, sortColumns):
    factor_k = tableinfo.reset_index()
    selected_columns = factor_k.columns[:3]
    factor_k = factor_k[selected_columns]
    db_conn.creat_table(dbPath, tableName, partitionColumns, factor_k, sortColumns)
    db_conn.write_table(dbPath, tableName, factor_k)

def factor_eval_test(value_result, dbPath, tableName, partitionColumns, sortColumns):
    result = value_result.reset_index()
    result.rename(columns={'index': 'factor_id'}, inplace=True)
    selected_columns = result.columns[:6]
    result = result[selected_columns]
    current_time = datetime.datetime.now()
    result['eval_time'] = current_time
    db_conn.drop_table(dbPath, tableName)
    db_conn.creat_table(dbPath, tableName, partitionColumns, result, sortColumns)
    db_conn.write_table(dbPath, tableName, result)

if __name__ == "__main__":

    start_date = '2023.01.01'
    end_date = '2024.01.01'
    logger = setup_logger("./log/")
    db_conn = dolphindb_connector(
        db_setting.setting["dolphindb_loginfo"], logger)
    db_feed = DBfeed(db_conn)

    # db_conn.drop_table(dbPath='dfs://factor_management', tableName="alpha_factors")
    market_price = db_feed.get_oed_price(start_date=start_date,
                                    end_date=end_date)
    prices = market_price.pivot(index="trade_date", columns="instrument_code", values="close")
    market_price = market_price.set_index(['trade_date', 'instrument_code'])

    pd_prices  = f_utils.rename(market_price)

    factor_v = f_utils.get_result_factor(pd_prices)

    prices.index =pd.to_datetime(prices.index)
    value_result = pd.DataFrame()
    factor_v.columns = factor_v.columns.droplevel(0)
    # 遍历因子并并行处理
    results = Parallel(n_jobs=-1)(
        delayed(f_utils.process_factor)(column_name, column_data, prices)
        for column_name, column_data in factor_v.items()
    )

    # 将结果整合到 ema DataFrame 中
    for result_table, column_name in zip(results, factor_v.keys()):
        value_result = f_utils.insert_data_with_index(value_result, result_table, column_name)

    # 因子评价结果存储
    factor_eval_test(value_result, dbPath="dfs://factor_management", tableName="factors_eval", partitionColumns='eval_time', sortColumns=['factor_id'])




'''
insert to dolpindb
update factor valuation
update factor  info

'''







# factor_sheet  db operate
# factor_sheet = db_feed.get_factor_sheet()
# db_feed.insert_factor_sheet()
# db_feed.delete_factor_sheet(factor_id='test_factor2')


# factor_evaluation  db operate
# factor_evaluation = db_feed.get_factor_evaluation()
# db_feed.insert_factor_evaluation()
# db_feed.delete_factor_evaluation(factor_id='test_factor2')



def factor_check(factorid =''):
    # if exist
    # repalce
    # 
    pass

