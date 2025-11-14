import logging
from logging.handlers import RotatingFileHandler
import sys
import time

LOG_FILE = 'app.log'
LOG_MAX_BYTES = 10 * 1024 * 1024  # 10MB
LOG_BACKUP_COUNT = 5

class LocalTimeFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        ct = time.localtime(record.created)
        if datefmt:
            s = time.strftime(datefmt, ct)
        else:
            t = time.strftime("%Y-%m-%d %H:%M:%S", ct)
            s = "%s,%03d" % (t, record.msecs)
        return s

def init_logger(level=logging.INFO, log_file=LOG_FILE):
    logger = logging.getLogger()
    logger.setLevel(level)
    # 清除旧的handler，避免重复
    logger.handlers.clear()

    # 控制台输出
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(LocalTimeFormatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s'))
    logger.addHandler(console_handler)

    # 文件输出（轮转）
    file_handler = RotatingFileHandler(log_file, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUP_COUNT, encoding='utf-8')
    file_handler.setFormatter(LocalTimeFormatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s'))
    logger.addHandler(file_handler)

    return logger

# 初始化全局logger
logger = init_logger()

def log_info(msg, *args, **kwargs):
    logger.info(msg, *args, **kwargs)

def log_warn(msg, *args, **kwargs):
    logger.warning(msg, *args, **kwargs)

def log_error(msg, *args, **kwargs):
    logger.error(msg, *args, **kwargs)
