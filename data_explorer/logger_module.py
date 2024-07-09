# logger_module.py
import logging
import datetime

def setup_logger(log_path: str = "./"):
    logger = logging.getLogger("slp")
    if not logger.hasHandlers():  # 确保只配置一次处理程序
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
