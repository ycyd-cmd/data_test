#-*- coding: utf-8 -*-
"""
Create on 2024/3/11 16:45
@Author: huangyanduo

"""

from data_explorer.rqdatac_md_provider import RqdatacMdProvider
from data_explorer.dolphindb_connector import dolphindb_connector
import data_explorer.data_toolkit as data_toolkit
import datetime
import logging
import pandas as pd
from config.vglobal import vglobal

def setup_logger(log_path: str):
    logger = logging.getLogger("slp")
    logger.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    curr_day = datetime.date.today()
    format_day = curr_day.strftime("%Y%m%d")

    fh = logging.FileHandler(f'{log_path}/slp_{format_day}.log', encoding="utf-8")
    fh.setLevel(logging.DEBUG)

    formatter = logging.Formatter('[%(asctime)s][%(levelname)s][%(filename)s:%(lineno)d][%(funcName)s] - '
                                  '%(message)s')
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)

    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger

logger = setup_logger("./log/")

def update_trading_date(data_cursor, db_cursor):
    today = datetime.datetime.today()
    current_year = datetime.datetime.now().year
    last_day_of_year = datetime.datetime(current_year, 12, 31).strftime("%Y%m%d")
    # db_cursor.delete_record(dbPath='dfs://ricequant', tableName='trading_dates')
    df = db_cursor.get_table_data(dbPath='dfs://ricequant', tableName='trading_dates')
    
    if df.empty:
        ls_trade_dates = data_cursor.get_trading_calendar(20000101, last_day_of_year)
        ls_update_dates = [pd.to_datetime(today, format='%Y%m%d') for x in range(0,len(ls_trade_dates))]
        data = {"trade_date":ls_trade_dates,
                "update_date":ls_update_dates}
        df = pd.DataFrame(data)
        df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y-%m-%d')
        db_cursor.write_table(dbPath='dfs://ricequant',tableName='trading_dates',df=df)
        logger.info(f"trade_date 交易日历初始化")
    else:
        max_trade_date = df.sort_values(by='trade_date')['trade_date'].iloc[-1]
        if max_trade_date < today:
            ls_trade_dates = data_cursor.get_trading_calendar(20000101, last_day_of_year)
            ls_update_dates = [pd.to_datetime(today, format='%Y%m%d') for x in range(0,len(ls_trade_dates))]
            data = {"trade_date":ls_trade_dates,
                    "update_date":ls_update_dates}
            df = pd.DataFrame(data)
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y-%m-%d')
            db_cursor.delete_record(dbPath='dfs://ricequant', tableName='trading_dates')
            db_cursor.write_table(dbPath='dfs://ricequant',tableName='trading_dates',df=df)
            logger.info(f"trade_date 交易日历更新到新一年年底")
        else:
            logger.info(f"trade_date 交易日历无需更新")
            

def update_data_daily(data_cursor, db_cursor, trade_date):
    all_instruments = data_cursor.get_all_instruments(trade_date=trade_date)
    list_instruments = all_instruments['instrument_code'].replace(vglobal.wind_to_exchg_map,regex=True).tolist()
    print(trade_date)
    if all_instruments is not None:
        db_cursor.write_table(dbPath='dfs://ricequant',tableName='all_instruments',df=all_instruments)
        # db_cursor.delete_record(dbPath='dfs://ricequant',tableName='all_instruments')
        # df = db_cursor.get_table_data(dbPath='dfs://ricequant', tableName='all_instruments')
        # print(df)
    else:
        logger.error(f"未从米筐接口all_instruments同步到数据, 数据写入失败, err_msg: trade_date:{trade_date}")

    instrument_industry = data_cursor.get_instrument_industry(list_instruments, trade_date)
    if instrument_industry is not None:
        db_cursor.write_table(dbPath='dfs://ricequant',tableName='instrument_industry',df=instrument_industry)
        # db_cursor.delete_record(dbPath='dfs://ricequant',tableName='instrument_industry')
        # df = db_cursor.get_table_data(dbPath='dfs://ricequant', tableName='instrument_industry')
        # print(df)
    else:
        logger.error(f"未从米筐接口get_instrument_industry同步到数据, 数据写入失败, err_msg: trade_date:{trade_date}")

    split_data = data_cursor.get_split_data(list_instruments, trade_date, trade_date)
    if split_data is not None:
        db_cursor.write_table(dbPath='dfs://ricequant',tableName='split_data',df=split_data)
        # db_cursor.delete_record(dbPath='dfs://ricequant',tableName='split_data')
        # df = db_cursor.get_table_data(dbPath='dfs://ricequant', tableName='split_data')
        # print(df)
    else:
        logger.error(f"未从米筐接口get_split_data同步到数据, 数据写入失败, err_msg: trade_date:{trade_date}")

    dividend_data = data_cursor.get_dividend_data(list_instruments, trade_date, trade_date)
    if dividend_data is not None:
        db_cursor.write_table(dbPath='dfs://ricequant',tableName='dividend_data',df=dividend_data)
        # db_cursor.delete_record(dbPath='dfs://ricequant',tableName='dividend_data')
        # df = db_cursor.get_table_data(dbPath='dfs://ricequant', tableName='dividend_data')
        # print(df)
    else:
        logger.error(f"未从米筐接口get_dividend_data同步到数据, 数据写入失败, err_msg: trade_date:{trade_date}")

    factor_exposure = data_cursor.get_factor_exposure(list_instruments, trade_date, trade_date)
    if factor_exposure is not None:
        db_cursor.write_table(dbPath='dfs://ricequant',tableName='factor_exposure',df=factor_exposure)
        # db_cursor.delete_record(dbPath='dfs://ricequant',tableName='factor_exposure')
        # df = db_cursor.get_table_data(dbPath='dfs://ricequant', tableName='factor_exposure')
        # print(df)
    else:
        logger.error(f"未从米筐接口get_factor_exposure同步到数据, 数据写入失败, err_msg: trade_date:{trade_date}")

    price_data = data_cursor.get_price(list_instruments, trade_date, trade_date)
    if price_data is not None:
        db_cursor.write_table(dbPath='dfs://ricequant',tableName='price_data',df=price_data)
        # db_cursor.delete_record(dbPath='dfs://ricequant',tableName='price_data')
        # df = db_cursor.get_table_data(dbPath='dfs://ricequant', tableName='price_data')
        # print(df)
    else:
        logger.error(f"未从米筐接口get_price同步到数据, 数据写入失败, err_msg: trade_date:{trade_date}")

    indexs_weight = data_cursor.get_index_weight(vglobal.index_set, trade_date)
    if factor_exposure is not None:
        db_cursor.write_table(dbPath='dfs://ricequant',tableName='index_weight',df=indexs_weight)
        # db_cursor.delete_record(dbPath='dfs://ricequant',tableName='index_weight')
        # df = db_cursor.get_table_data(dbPath='dfs://ricequant', tableName='index_weight')
        # print(df)
    else:
        logger.error(f"未从米筐接口get_index_weight同步到数据, 数据写入失败, err_msg: trade_date:{trade_date}")

    index_price_data = data_cursor.get_index_price(vglobal.index_set, trade_date)
    if index_price_data is not None:
        db_cursor.write_table(dbPath='dfs://ricequant',tableName='index_price',df=index_price_data)
        # db_cursor.delete_record(dbPath='dfs://ricequant',tableName='index_price_data')
        # df = db_cursor.get_table_data(dbPath='dfs://ricequant', tableName='index_price_data')
        # print(df)
    else:
        logger.error(f"未从米筐接口get_price同步到数据, 数据写入失败, err_msg: trade_date:{trade_date}")


    # 获取所有因子并存如dolphindb
    factor_table_list = vglobal.factor_table_list
    for tb_name in factor_table_list:
        table_cols= vglobal.factor_table_dict[tb_name]
        factor_data = md_provider.fetch_get_factor(list_instruments, factor_list=table_cols, trade_date=trade_date)
        if factor_data is not None:
            db_cursor.write_table(dbPath='dfs://ricequant',tableName=tb_name, df=factor_data)
            # tmpdf = db_cursor.get_table_data(dbPath='dfs://ricequant', tableName=tb_name)
            # print(len(tmpdf))
        else:
            logger.error(f"因子表:{tb_name}, 未从米筐接口同步到数据, 数据写入失败, err_msg: trade_date:{trade_date}")
    

# 数据检查类
def has_date(df, field, trade_dates):
    # 确保日期字段的数据类型正确
    df[field] = pd.to_datetime(df[field], errors='coerce')
    # 检查每个日期是否在交易日列表中
    return df[field].isin(trade_dates).all()

# def validate_tables(config):
#     results = {}
#     trade_dates = load_trade_dates()  # 加载交易日列表
#     for table_name, rules in config['tables'].items():
#         df = load_table_data(table_name)
#         table_results = []
        
#         # 检查字段规则
#         field_rules = rules['data_check_rules'].get('field_rules', [])
#         for rule in field_rules:
#             if rule['check'] == 'has_date':
#                 result = has_date(df, rule['field'], trade_dates)
#                 table_results.append((rule['field'], 'has_date', result))
        
#         # 检查表长度规则
#         table_len_rules = rules['data_check_rules'].get('table_rules', [])
#         for rule in table_len_rules:
#             if rule['check'] == 'min_length':
#                 result = check_table_length(df, min_len=rule['value'])
#                 table_results.append(('min_length', rule['value'], result))
#             elif rule['check'] == 'max_length':
#                 result = check_table_length(df, max_len=rule['value'])
#                 table_results.append(('max_length', rule['value'], result))
        
#         results[table_name] = table_results
    
#     return results

setting = {
    # md_provider 配置
    "md_provider": {
        "username": "license",
        "password":
        # "ccU7Yf9dPikhFXtVXsLQZWM0qiY4qxByfselEs2ZWpd01alOGjGpY2R9hXF1_5dG8PKoaJU99ONuKD56i7ySeKpc0lL5G2AP67B5YuhqEH6pUJ-0DSVHVDWLiS4oFqYGmXSNvHU9i0HIScwhQfbwH9gaMwSIkwMStfEvYxV-zxk=a4YImMRv749w2h25tcyJ691JLtZw7QHlqIEcOycx-rZIUQATTdwgF-jcgDxBU5RN09nHRCE2m77u6EDXzStBNPqXzV3gofzOlhXfp9I5o-6bfOPCqUlIFYNt7O3BkppTdRYo2faD3fCI7oc6EKSp64khcP0BaSnNecN1Gm4tJKA=",
        # "NHiG-lJ7WEhZD484_1f0_dpHhGL2THWmakjofFWwPgxNMXAzi7-1Qap6ZgZMeFATqiTJCoXX87oMpHjQHf4OmyUF2xurOM6QA5FRea4h7aSFHjz4dd7iy7Cdmg2qIFarpLKVKwxfYEVpLEWEuaatOaeAsigMDanpC0EkSgzR0Ig=avQbi6gVc9IUXZuYi4aSDQDsaLp5-HzwMPnqTbmkB1bwwSAvYyG6bq5-KS2RbxFgi6vAHZxUv-eUMXQaS3wNxwDAChRdZTjLTLq__Kbr2fytXes2T3DoMTxBhoal-S3WaGfeYJGWgUYqS2SYexqOj_PgPepTURvSbbL4KynjV1U=",
        "aAtdYbKp-cQrhkgg2x2EaSNKe9CpnGVJgX8jg6T0kkOYQNj7runuuA0Hj0HmMaNohmslKMlLLoP1W4Zg9k7SGfc854ftXet2F0oZTlCthGC8kCgbtCyYWQKJao2gaBL21Vt4wnwjA2ZfL96XRSVKBBxYJ9GhkmxT4kPa4Z9RZaM=OGSfr7brimd7bsp16CzKLgCy4uVk4rpDbKgpg7Gs6QSpPcms0IiO7G_EG6lk12M3AQQD6lDpSxFWj1qaB92CoL6nTxLypxvn4pw-KN8HBpLoEBMHDLPcVoX_3DB660WXMsAxL-i9wa6OZTm6rvtze8tg-jQu9FwJt8jeoYnQAK8=",
        "srv_addr": "rqdatad-pro.ricequant.com",
        "srv_port": 16011
    },
    "dolphindb_loginfo": {
        "username": "admin",
        "password": "123456",
        "srv_addr": "172.139.6.201",
        "srv_port": 8900
    },
    # ci data provider 配置
    "ci_data_provider": {
        "base_dir": "./data"
    }
}

def ricequant_update(start_date, end_date):
    # MD Provider
    md_provider = RqdatacMdProvider(setting["md_provider"], logger)
    # DB Connector
    db_conn = dolphindb_connector(setting["dolphindb_loginfo"], logger)

        # 更新交易日->trading_dates
    update_trading_date(md_provider, db_conn)

    start_date = data_toolkit.data_toolkit.convert_date_format(start_date,"%Y.%m.%d")
    end_date = data_toolkit.data_toolkit.convert_date_format(end_date,"%Y.%m.%d")
    df = db_conn.selectValue(tableName="trading_dates", dbPath="dfs://ricequant", col=['trade_date'], idcondition=f"trade_date>={start_date} and trade_date<={end_date} order by trade_date asc")
    previous_date = db_conn.get_previous_trading_date("2024-04-22")
    # 日更表格
    update_dates = df['trade_date'].to_list()
    for date in update_dates:
        logger.info(f"[trade_date]: {date}")
        update_data_daily(md_provider, db_conn, date)

ricequant_update('20220101','20230101')


