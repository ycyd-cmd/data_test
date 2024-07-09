#-*- coding: utf-8 -*-
"""
Create on 2024/3/11 16:45
@Author: huangyanduo

"""

from typing import List, Dict, Optional
import pandas as pd
import rqdatac
import numpy as np
from config.vglobal import vglobal
from data_explorer.base_explorer import Object


class RqdatacMdProvider(Object):
    """
    米筐行情数据Provider
    """

    def __init__(self, setting: dict, logger=None):
        super().__init__(logger=logger, adapter_name="米筐行情")
        self.username = ""  # 用户名
        self.password = ""  # 用户密码
        self.srv_addr = ""  # 服务地址
        self.srv_port = 0  # 服务端口
        self._parse_setting(setting)
        self.order_book_ids=None
        self.init()

    def _parse_setting(self, setting: dict):
        self.username = setting['username']
        self.password = setting['password']
        self.srv_addr = setting['srv_addr']
        self.srv_port = setting['srv_port']

    def init(self):
        """
        米筐账号初始化
        :return:
        """
        try:
            rqdatac.init(username=self.username, password=self.password,
                         addr=(self.srv_addr, self.srv_port),
                         use_pool=True,
                         max_pool_size=1,
                         auto_load_plugins=False) #proxy_info = ("HTTP", "10.37.1.180", 3128, "", "")
            self.logger.info("米筐账户连接初始化成功")
            instrument_df=rqdatac.all_instruments(type="CS")
            instrument_df = instrument_df[instrument_df["industry_code"] != "Unknown"]
            instrument_df = instrument_df[instrument_df["special_type"] == "Normal"]
            instrument_df = instrument_df[instrument_df["status"] == "Active"]
            self.order_book_ids=instrument_df['order_book_id']
        except Exception as e:
            self.logger.error(f"米筐账户连接初始化失败，错误信息：{e}")

    def get_trading_days(self, start_date, end_date):
        self.trading_days = rqdatac.get_trading_dates(start_date, end_date)
        return self.trading_days 


    def get_all_instruments(self, trade_date = None) -> Optional[pd.DataFrame]:
        try:
            df = rqdatac.all_instruments(type="CS", market='cn', date=trade_date)
            df.reset_index(inplace=True)
            df['date'] = pd.to_datetime(trade_date)
            df.rename(columns={'order_book_id':'instrument_code','date':'trade_date'},inplace=True)
            df['instrument_code'].replace(vglobal.exchg_to_wind_map,regex=True,inplace =True)
            return df
        except Exception as e:
            self.logger.error(f"米筐获取所有标的信息失败，错误信息：{e}")
            return None
        
    def get_index_indicator(self, index_list, start_date, end_date) -> Optional[pd.DataFrame]:
        # index_indicator(order_book_ids,start_date,end_date,fields)
        pass

    def get_index_components(self, index_list, start_date, end_date) -> Optional[pd.DataFrame]:
        pass

    def get_index_weight(self, index_list, trade_date =  None) -> Optional[pd.DataFrame]:
        try:
            df = pd.DataFrame()
            for index_id in index_list:
                self.logger.info(f"同步指数信息{index_id}")
                pd_data = rqdatac.index_weights(index_id, date=trade_date)
                pd_data = pd_data.to_frame()
                pd_data.reset_index(inplace=True)
                pd_data.columns = ['instrument_code','weight']
                pd_data['trade_date'] = trade_date
                pd_data['index_id'] = index_id
                pd_data['instrument_code'].replace(vglobal.exchg_to_wind_map,regex=True,inplace =True)
                pd_data['index_id'].replace(vglobal.exchg_to_wind_map,regex=True,inplace =True)
                df = pd.concat([df, pd_data], ignore_index=True)
            return df
        except Exception as e:
            self.logger.error(f"米筐获取指数权重，错误信息：{e}")
            return None

    def get_index_price(self, index_list, trade_date =  None) -> Optional[pd.DataFrame]:
        try:
            pd_data = rqdatac.get_price(index_list, start_date=trade_date, end_date=trade_date, frequency='1d', adjust_type='post', market='cn', expect_df=True)
            pd_data.reset_index(inplace=True)
            pd_data.rename(columns={'order_book_id': 'instrument_code', 'date': 'trade_date'}, inplace=True)

            last_price_pd_data = rqdatac.get_price(index_list, start_date=trade_date, end_date=trade_date, frequency='1d',fields=['close'], adjust_type='none', market='cn', expect_df=True)
            last_price_pd_data.reset_index(inplace=True)
            last_price_pd_data.columns = ['instrument_code','trade_date','last_close']
            last_price_pd_data = last_price_pd_data[['instrument_code', 'last_close']]

            pd_data = pd.merge(pd_data,last_price_pd_data,on='instrument_code',how='left')
            pd_data['instrument_code'].replace(vglobal.exchg_to_wind_map,regex=True,inplace =True)
            db_cols = ['trade_date' , 'instrument_code', 'open','high','low','close','last_close','total_turnover' ,'volume']
            pd_data = pd_data[db_cols]
            return pd_data
        except Exception as e:
            self.logger.error(f"米筐获取指数价格，错误信息：{e}")
            return None



    def get_instrument_industry(self, symbols,trade_date =  None) -> Optional[pd.DataFrame]:
        try:
            df= rqdatac.get_instrument_industry(symbols, source='sws', level=1, date=trade_date, market='cn')
            df.reset_index(inplace=True)
            df['date'] = pd.to_datetime(trade_date)
            df.rename(columns={'order_book_id':'instrument_code','date':'trade_date'},inplace=True)
            df['instrument_code'].replace(vglobal.exchg_to_wind_map,regex=True,inplace =True)
            return df
        except Exception as e:
            self.logger.error(f"米筐获取行业分类数据，错误信息：{e}")
            return None
        
    def get_trading_calendar(self, start_date, end_date) -> Optional[pd.DataFrame]:
        try:
            df = rqdatac.get_trading_dates(start_date, end_date, market='cn')
            return df
        except Exception as e:
            self.logger.error(f"米筐获取日历信息失败，错误信息：{e}")
            return None

    def get_split_data(self, symbol, start_date, end_date) -> Optional[pd.DataFrame]:
        try:
            df = rqdatac.get_split(symbol, start_date=start_date, end_date=end_date, market='cn')
            if df is not None:
                df.reset_index(inplace=True)
                df['date'] = pd.to_datetime(start_date)
                df.rename(columns={'order_book_id':'instrument_code','date':'trade_date'},inplace=True)
                df['instrument_code'].replace(vglobal.exchg_to_wind_map,regex=True,inplace =True)
            return df
        except Exception as e:
            self.logger.error(f"米筐获取拆分数据失败，错误信息：{e}")
            return None

    def get_dividend_data(self, symbol, start_date, end_date) -> Optional[pd.DataFrame]:
        try:
            df = rqdatac.get_dividend(symbol, start_date=start_date, end_date=end_date, market='cn')
            if df is not None:
                df.reset_index(inplace=True)
                df['date'] = pd.to_datetime(start_date)
                df.rename(columns={'order_book_id':'instrument_code','date':'trade_date'},inplace=True)
                df['instrument_code'].replace(vglobal.exchg_to_wind_map,regex=True,inplace =True)
            return df
        except Exception as e:
            self.logger.error(f"米筐获取分红数据失败，错误信息：{e}")
            return None
    
    def get_factor_exposure(self, symbols, start_date, end_date) -> Optional[pd.DataFrame]:
        try:
            df=rqdatac.get_factor_exposure(symbols, start_date=start_date, end_date=end_date,factors = None, model = 'v1')
            df.reset_index(inplace=True)
            df.rename(columns={'order_book_id':'instrument_code','date':'trade_date'},inplace=True)
            df['instrument_code'].replace(vglobal.exchg_to_wind_map,regex=True,inplace =True)
            return df
        except Exception as e:
            self.logger.error(f"米筐获取因子暴露数据，错误信息：{e}")
            return None


    def get_price(self, symbols, start_date, end_date) -> Optional[pd.DataFrame]:
        try:
            df = rqdatac.get_price(symbols, start_date=start_date, end_date=end_date, frequency='1d', adjust_type='post', market='cn', expect_df=True)
            df.reset_index(inplace=True)
            is_suspended = rqdatac.is_suspended(symbols, start_date=start_date, end_date=end_date, market='cn')
            is_suspended = is_suspended.melt()
            is_suspended.columns = ['order_book_id','suspended']
            df = pd.merge(df, is_suspended, on='order_book_id', how='left')
            df.rename(columns={'order_book_id':'instrument_code','date':'trade_date'},inplace=True)
            df['instrument_code'].replace(vglobal.exchg_to_wind_map,regex=True,inplace =True)
            return df
        except Exception as e:
            self.logger.error(f"米筐获取标的价格数据失败，错误信息：{e}")
            return None

    def get_current_price(self) -> Optional[pd.DataFrame]:
        """
        调用current_minute获取股票当前最新价
        :param stocks:
        :return:
        """
        try:
            df = rqdatac.current_minute(order_book_ids=self.order_book_ids, skip_suspended=False,
                                        fields=['close'])
            df.rename(columns={'order_book_id':'instrument_code','date':'trade_date'},inplace=True)
            df['instrument_code'].replace(vglobal.exchg_to_wind_map,regex=True,inplace =True)
            if df is None:
                return None
            return df
        except Exception as e:
            self.logger.error(f"米筐获取分钟数据失败，错误信息：{e}")
            return None

    def fetch_get_factor(self, instrument_codes, factor_list=None, trade_date=None):

        sql_cols = ['t_factor%s'%(i) for i in range(len(factor_list))]

        rename_dict = dict(zip(factor_list,sql_cols))
        try:
            df = rqdatac.get_factor(instrument_codes, factor=factor_list, start_date= trade_date, end_date= trade_date)
            df.reset_index(inplace=True)
            df.rename(columns={'order_book_id':'instrument_code','date':'trade_date'},inplace=True)
            df['instrument_code'].replace(vglobal.exchg_to_wind_map,regex=True,inplace =True)
            df = df.replace([np.inf, -np.inf], np.nan)
            return df
        except Exception as e:
            self.logger.error(f"米筐获取factor的信息失败，错误信息：{e}")
            return None


