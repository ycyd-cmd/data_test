#-*- coding: utf-8 -*-
"""
Create on 2024/3/11 16:45
@Author: huangyanduo

"""

from tabnanny import check
import dolphindb as ddb
import dolphindb.settings as keys
import pandas as pd
import numpy as np
import datetime
from functools import wraps
from data_explorer.base_explorer import Object

class dolphindb_connector(Object):

    def __init__(self, setting: dict, logger: str = None):
        super().__init__(logger=logger, adapter_name="ddb")
        self.username = ""  # 用户名
        self.password = ""  # 用户密码
        self.srv_addr = ""  # 服务地址
        self.srv_port = 0  # 服务端口
        self._parse_setting(setting)
        self.connector = None
        self.init()

    
    def _parse_setting(self, setting: dict):
        self.username = setting['username']
        self.password = setting['password']
        self.srv_addr = setting['srv_addr']
        self.srv_port = setting['srv_port']

    def init(self):
        """
        DolphinDB账号初始化&连接
        :return:
        """
        try:
            self.connector = ddb.session(enablePickle=False)
            conn = self.connector.connect(self.srv_addr, self.srv_port,
                                          self.username, self.password)
            if conn:
                self.logger.info("DolphinDB连接成功")
            else:
                self.logger.error("DolphinDB连接失败")

        except Exception as e:
            self.logger.error(f"DolphinDB连接失败，错误信息：{e}")

    def __del__(self):
        if self.connector is not None:
            self.connector.close()
            self.logger.info("DolphinDB连接断开")


    def __check_exists(check_db=False, check_table=False):
        def decorator(func):
            @wraps(func)
            def wrapper(self, dbPath, tableName=None, *args, **kwargs):
                if check_db and not self.connector.existsDatabase(dbPath):
                    self.logger.error(f"DolphinDB不存在数据库 dbPath:{dbPath}")
                    return None
                
                if check_table and tableName and not self.connector.existsTable(dbUrl=dbPath, tableName=tableName):
                    self.logger.error(f"DolphinDB不存在数据表 dbPath:{dbPath} tableName:{tableName}")
                    return None
                
                if check_db and not check_table:
                    return func(self, dbPath, *args, **kwargs)
                if not check_db and check_table:
                    return func(self, dbPath, tableName, *args, **kwargs)
                if check_db and check_table:
                    return func(self, dbPath, tableName, *args, **kwargs)
                
            return wrapper
        return decorator

    @__check_exists(check_db=True, check_table=True)
    def get_table_info_test(self, dbPath=str, tableName=str):
            trade = self.connector.loadTable(tableName=tableName,
                                             dbPath=dbPath)
            return trade.schema

    def create_database(self, dbPath, partitions, partitionType=keys.VALUE, engine="OLAP", atomic="TRANS"):
        db = self.connector.database(dbPath=dbPath, partitionType=partitionType, partitions=partitions, engine=engine, atomic=atomic)
        return db

    @__check_exists(check_db=True, check_table=False)
    def get_database(self, dbPath):
        db = self.connector.database(dbPath=dbPath)
        return db

    def creat_table(self, dbPath, tableName, partitionColumns, tableinfo=None, sortColumns=None, keepDuplicates=None):
        if not self.connector.existsTable(dbUrl=dbPath, tableName=tableName):
            db = self.connector.database(dbPath=dbPath)
            t = self.connector.table(data=tableinfo)
            table = db.createPartitionedTable(table=t, tableName=tableName, partitionColumns=partitionColumns, sortColumns=sortColumns, keepDuplicates=keepDuplicates)
            return table.toDF()
        
        self.logger.error(f"DolphinDB已存在数据库 dbPath:{dbPath} tableName:{tableName}")
        re = self.connector.loadTable(tableName=tableName, dbPath=dbPath).toDF()
        return re
    
    @__check_exists(check_db=True, check_table=True)
    def get_table(self, dbPath, tableName):
        table = self.connector.loadTable(tableName=tableName, dbPath=dbPath)
        return table

    @__check_exists(check_db=True, check_table=True)
    def get_table_info(self, dbPath=str, tableName=str):
        trade = self.connector.loadTable(tableName=tableName, dbPath=dbPath)
        return trade.schema

    @__check_exists(check_db=True, check_table=True)
    def get_table_data(self, dbPath=str, tableName=str):
        trade = self.connector.loadTable(tableName=tableName, dbPath=dbPath)
        return trade.toDF()

    @__check_exists(check_db=True, check_table=True)
    def drop_table(self, dbPath=str, tableName=str):
        self.connector.dropTable(dbPath=dbPath, tableName=tableName)
        return True

    @__check_exists(check_db=True, check_table=False)
    def drop_database(self, dbPath=str):
        self.connector.dropDatabase(dbPath)
        return True

    def delete_record(self, dbPath, tableName, condition=None):
        table = self.connector.loadTable(tableName=tableName, dbPath=dbPath)
        if condition is None:
            table.delete().execute()
        else:
            table.delete().where(condition).execute()
        print(table)
        

    def get_connector(self):
        return self.connector

    def write_table(self, dbPath, tableName, df):
        desired_order = self.get_table_info(
            dbPath=dbPath, tableName=tableName).name.to_list()
        df = df[desired_order]
        df_reordered = df.reindex(columns=desired_order)
        sql = """tableInsert{{loadTable("{}", "{}")}}""".format(
            dbPath, tableName)
        self.logger.info(f"DolphinDB执行sql:{sql}")
        self.connector.run(sql, df_reordered)

    def write_table_bk(self, dbPath, tableName, df):
        desired_order = self.get_table_info(dbPath=dbPath, tableName=tableName).name.to_list()
        df = df[desired_order]
        df_reordered = df.reindex(columns=desired_order)
        # sql = """tableInsert{{loadTable("{}", "{}")}}""".format(dbPath, tableName)
        self.connector.upload({'df_reordered':df_reordered})
        df_reordered =  self.connector.run("""replaceColumn!(df_reordered, "trade_date", datetime(exec temporalParse(trade_date,"yyyy-MM-dd") from df_reordered))""")
        df_reordered =  self.connector.run("""replaceColumn!(df_reordered, "trading_code", string(exec trading_code from df_reordered))""")
        self.connector.run("""tableInsert(loadTable("dfs://ricequant", "all_instruments"), df_reordered)""")

    def selectValue(self,
                    tableName,
                    dbPath,
                    col,
                    idcondition='',
                    pluscondition=''):
        """
        idcondition中, 如有日期应为2000.01.01格式
        参考SQL select trade_date from trading_dates where trade_date>=2023.01.01 and trade_date<=2024.04.19 order by trade_date asc
        """
        strSQL = 'select '
        if col != []:
            for i in range(0, len(col)):
                if i == len(col) - 1:
                    strSQL += col[i] + ' '
                else:
                    strSQL += col[i] + ','
        else:
            strSQL += '*'
        if idcondition == '':
            strSQL += 'from %s ' % (tableName)
        else:
            strSQL += 'from %s where ' % (tableName) + idcondition

        strSQL += pluscondition
        print(strSQL)
        try:
            data = self.connector.loadTableBySQL(tableName=tableName,
                                                dbPath=dbPath,
                                                sql=strSQL)


            if data.rows==0:
                return pd.DataFrame()
            else:
                return data.toDF()
        except Exception as e:
            self.logger.error(f"查询语句失败，错误信息：{e}")

    def selectValue1(self,
                    tableName,
                    dbPath,
                    col,
                    startDate='',
                    endDate=''):
        """
        idcondition中, 如有日期应为2000.01.01格式
        参考SQL select trade_date from trading_dates where trade_date>=2023.01.01 and trade_date<=2024.04.19 order by trade_date asc
        """
        strSQL = 'select '
        if col != []:
            for i in range(0, len(col)):
                if i == len(col) - 1:
                    strSQL += col[i] + ' '
                else:
                    strSQL += col[i] + ','
        else:
            strSQL += '*'
        if startDate=='' and endDate == '':
            strSQL += 'from %s ' % (tableName)
        elif startDate !='' and endDate == '':
            strSQL += 'from %s where trade_date >= %s  order by trade_date asc' % (tableName, startDate)
        elif startDate =='' and endDate != '':
            strSQL += 'from %s where trade_date <= %s order by trade_date asc' % (tableName, endDate)
        elif startDate !='' and endDate != '':
            strSQL += 'from %s where trade_date >= %s and trade_date <= %s order by trade_date asc' % (tableName, startDate, endDate)

        print(strSQL)
        try:
            data = self.connector.loadTableBySQL(tableName=tableName,
                                                dbPath=dbPath,
                                                sql=strSQL)


            if data.rows==0:
                return pd.DataFrame()
            else:
                return data.toDF()
        except Exception as e:
            self.logger.error(f"查询语句失败，错误信息：{e}")
            
    def get_trading_days(self, format='str'):
        trading_dates = self.selectValue(tableName="trading_dates", dbPath="dfs://ricequant", col=['trade_date'], pluscondition=f"order by trade_date asc")
        trading_dates['trade_date'] = trading_dates['trade_date'].apply(lambda x: x.date().isoformat())
        return trading_dates['trade_date'].to_list()
    
    def get_previous_trading_date(self, current_date):
        """
        获取给定日期的前一个交易日。如果给定日期不在列表中，则向前查找最接近的交易日。

        参数:
        current_date (str): 当前日期，格式应为 'YYYY-MM-DD'
        trading_days (list): 交易日列表，每个元素也是 'YYYY-MM-DD' 格式的字符串

        返回:
        str: 给定日期的最近一个交易日
        """
        # 确保日期格式正确并转换为日期对象
        trading_days = self.get_trading_days()
        trading_days = [datetime.datetime.strptime(day, '%Y-%m-%d').date() for day in trading_days]
        current_date = datetime.datetime.strptime(current_date, '%Y-%m-%d').date()

        # 检查当前日期是否直接在列表中
        if current_date in trading_days:
            current_index = trading_days.index(current_date)
            if current_index > 0:
                return trading_days[current_index - 1].isoformat()

        # 日期不在列表中，向前递减直到找到最接近的交易日
        while current_date not in trading_days:
            current_date -= datetime.timedelta(days=1)
            # 确保不会一直循环如果日期列表很小或者给定的日期远早于列表中的任何日期
            if current_date < min(trading_days):
                raise ValueError("没有找到任何接近的交易日，请检查交易日历。")

        return current_date.isoformat()

