import os
import sys
import pandas as pd
from datetime import datetime
from data_explorer.data_toolkit import data_toolkit
from config.vglobal import vglobal
import logging
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from datafeed.DBfeed import DBfeed
import source.factor.factor_utils as f_utils
from source.factor.factor_db_test import factor_wide_test
# from data_explorer.base_explorer import Object

class RicequantDataUpdater():
    def __init__(self,  md_provider: object, db_conn: object, logger=None):
        # super().__init__(logger=logger, adapter_name="米筐更新器")
        self.md_provider = md_provider
        self.db_conn = db_conn
        self.logger = logger if logger else self._get_console_logger()

    def _get_console_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
        return logger
        
    def update_trading_date(self):
        today = datetime.today()
        current_year = datetime.now().year
        last_day_of_year = datetime(current_year, 12, 31).strftime("%Y%m%d")
        df = self.db_conn.get_table_data(dbPath='dfs://ricequant', tableName='trading_dates')

        if df.empty:
            ls_trade_dates = self.md_provider.get_trading_calendar(20000101, last_day_of_year)
            ls_update_dates = [pd.to_datetime(today, format='%Y%m%d') for x in range(0, len(ls_trade_dates))]
            data = {"trade_date": ls_trade_dates, "update_date": ls_update_dates}
            df = pd.DataFrame(data)
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y-%m-%d')
            self.db_conn.write_table(dbPath='dfs://ricequant', tableName='trading_dates', df=df)
            self.logger.info(f"trade_date 交易日历初始化")
        else:
            max_trade_date = df.sort_values(by='trade_date')['trade_date'].iloc[-1]
            if max_trade_date < today:
                ls_trade_dates = self.md_provider.get_trading_calendar(20000101, last_day_of_year)
                ls_update_dates = [pd.to_datetime(today, format='%Y%m%d') for x in range(0, len(ls_trade_dates))]
                data = {"trade_date": ls_trade_dates, "update_date": ls_update_dates}
                df = pd.DataFrame(data)
                df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y-%m-%d')
                self.db_conn.delete_record(dbPath='dfs://ricequant', tableName='trading_dates')
                self.db_conn.write_table(dbPath='dfs://ricequant', tableName='trading_dates', df=df)
                self.logger.info(f"trade_date 交易日历更新到新一年年底")
            else:
                self.logger.info(f"trade_date 交易日历无需更新")

    def update_data_daily(self, trade_date):
        all_instruments = self.md_provider.get_all_instruments(trade_date=trade_date)
        list_instruments = all_instruments['instrument_code'].replace(vglobal.wind_to_exchg_map,regex=True).tolist()
        if all_instruments is not None:
            self.db_conn.write_table(dbPath='dfs://ricequant',tableName='all_instruments',df=all_instruments)
            # self.db_conn.delete_record(dbPath='dfs://ricequant',tableName='all_instruments')
            # df = self.db_conn.get_table_data(dbPath='dfs://ricequant', tableName='all_instruments')
            # print(df)
        else:
            self.logger.error(f"未从米筐接口all_instruments同步到数据, 数据写入失败, err_msg: trade_date:{trade_date}")

        instrument_industry = self.md_provider.get_instrument_industry(list_instruments, trade_date)
        if instrument_industry is not None:
            self.db_conn.write_table(dbPath='dfs://ricequant',tableName='instrument_industry',df=instrument_industry)
            # self.db_conn.delete_record(dbPath='dfs://ricequant',tableName='instrument_industry')
            # df = self.db_conn.get_table_data(dbPath='dfs://ricequant', tableName='instrument_industry')
            # print(df)
        else:
            self.logger.error(f"未从米筐接口get_instrument_industry同步到数据, 数据写入失败, err_msg: trade_date:{trade_date}")

        split_data = self.md_provider.get_split_data(list_instruments, trade_date, trade_date)
        if split_data is not None:
            self.db_conn.write_table(dbPath='dfs://ricequant',tableName='split_data',df=split_data)
            # self.db_conn.delete_record(dbPath='dfs://ricequant',tableName='split_data')
            # df = self.db_conn.get_table_data(dbPath='dfs://ricequant', tableName='split_data')
            # print(df)
        else:
            self.logger.error(f"未从米筐接口get_split_data同步到数据, 数据写入失败, err_msg: trade_date:{trade_date}")

        dividend_data = self.md_provider.get_dividend_data(list_instruments, trade_date, trade_date)
        if dividend_data is not None:
            self.db_conn.write_table(dbPath='dfs://ricequant',tableName='dividend_data',df=dividend_data)
            # self.db_conn.delete_record(dbPath='dfs://ricequant',tableName='dividend_data')
            # df = self.db_conn.get_table_data(dbPath='dfs://ricequant', tableName='dividend_data')
            # print(df)
        else:
            self.logger.error(f"未从米筐接口get_dividend_data同步到数据, 数据写入失败, err_msg: trade_date:{trade_date}")

        factor_exposure = self.md_provider.get_factor_exposure(list_instruments, trade_date, trade_date)
        if factor_exposure is not None:
            self.db_conn.write_table(dbPath='dfs://ricequant',tableName='factor_exposure',df=factor_exposure)
            # self.db_conn.delete_record(dbPath='dfs://ricequant',tableName='factor_exposure')
            # df = self.db_conn.get_table_data(dbPath='dfs://ricequant', tableName='factor_exposure')
            # print(df)
        else:
            self.logger.error(f"未从米筐接口get_factor_exposure同步到数据, 数据写入失败, err_msg: trade_date:{trade_date}")

        price_data = self.md_provider.get_price(list_instruments, trade_date, trade_date)
        if price_data is not None:
            self.db_conn.write_table(dbPath='dfs://ricequant',tableName='price_data',df=price_data)
            # self.db_conn.delete_record(dbPath='dfs://ricequant',tableName='price_data')
            # df = self.db_conn.get_table_data(dbPath='dfs://ricequant', tableName='price_data')
            # print(df)
        else:
            self.logger.error(f"未从米筐接口get_price同步到数据, 数据写入失败, err_msg: trade_date:{trade_date}")

        indexs_weight = self.md_provider.get_index_weight(vglobal.index_set, trade_date)
        if factor_exposure is not None:
            self.db_conn.write_table(dbPath='dfs://ricequant',tableName='index_weight',df=indexs_weight)
            # self.db_conn.delete_record(dbPath='dfs://ricequant',tableName='index_weight')
            # df = self.db_conn.get_table_data(dbPath='dfs://ricequant', tableName='index_weight')
            # print(df)
        else:
            self.logger.error(f"未从米筐接口get_index_weight同步到数据, 数据写入失败, err_msg: trade_date:{trade_date}")

        index_price_data = self.md_provider.get_index_price(vglobal.index_set, trade_date)
        if index_price_data is not None:
            self.db_conn.write_table(dbPath='dfs://ricequant',tableName='index_price',df=index_price_data)
            # self.db_conn.delete_record(dbPath='dfs://ricequant',tableName='index_price_data')
            # df = self.db_conn.get_table_data(dbPath='dfs://ricequant', tableName='index_price_data')
            # print(df)
        else:
            self.logger.error(f"未从米筐接口get_price同步到数据, 数据写入失败, err_msg: trade_date:{trade_date}")

        # 获取所有因子并存如dolphindb
        factor_table_list = vglobal.factor_table_list
        for tb_name in factor_table_list:
            table_cols= vglobal.factor_table_dict[tb_name]
            factor_data = self.md_provider.fetch_get_factor(list_instruments, factor_list=table_cols, trade_date=trade_date)
            if factor_data is not None:
                self.db_conn.write_table(dbPath='dfs://ricequant',tableName=tb_name, df=factor_data)
                # tmpdf = db_cursor.get_table_data(dbPath='dfs://ricequant', tableName=tb_name)
                # print(len(tmpdf))
            else:
                self.logger.error(f"因子表:{tb_name}, 未从米筐接口同步到数据, 数据写入失败, err_msg: trade_date:{trade_date}")

    def update(self, start_date, end_date):
        self.update_trading_date()
        start_date = data_toolkit.convert_date_format(start_date,"%Y.%m.%d")
        end_date = data_toolkit.convert_date_format(end_date,"%Y.%m.%d")
        df = self.db_conn.selectValue(tableName="trading_dates", dbPath="dfs://ricequant", col=['trade_date'], idcondition=f"trade_date>={start_date} and trade_date<={end_date} order by trade_date asc")
        update_dates = df['trade_date'].to_list()
        for date in update_dates:
            self.logger.info(f"[trading_date]: {date}")
            self.update_data_daily(date)