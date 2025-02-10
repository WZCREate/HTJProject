import json
import pymysql
from sqlalchemy import create_engine
import logging
import os

def find_config_path():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    search_dir = current_dir
    
    # 向上查找直到找到 StockFilter 目录
    while not os.path.basename(search_dir) == "StockFilter":
        parent_dir = os.path.dirname(search_dir)
        if parent_dir == search_dir:  # 已经到达根目录
            raise Exception("找不到 StockFilter 目录")
        search_dir = parent_dir
    
    root_dir = search_dir  # StockFilter 目录路径
    
    # 确定配置文件路径 - 移除前导斜杠
    config_path_QA = os.path.join(root_dir, "QA", "config.json")
    config_path_PROD = os.path.join(root_dir, "PROD", "config.json")
    return config_path_QA, config_path_PROD, root_dir

def debug_log(func):
    """装饰器：打印函数执行状态"""
    def wrapper(*args, **kwargs):
        # 特殊处理 load_config 函数
        if func.__name__ == 'load_config':
            try:
                result = func(*args, **kwargs)
                # 从加载的配置中获取 DEBUG 设置
                debug_mode = result.get('DEBUG', False)
                if debug_mode:
                    print(f"开始执行: {func.__name__}")
                    print(f"成功执行: {func.__name__}")
                return result
            except Exception as e:
                # 如果配置加载失败，假设处于非调试模式
                print(f"执行失败: {func.__name__}, 错误: {str(e)}")
                raise
        else:
            # 其他函数从 config 参数中获取 debug 模式
            config = args[0] if args else {}
            debug_mode = config.get('DEBUG', False)
            
            if debug_mode:
                print(f"开始执行: {func.__name__}")
            try:
                result = func(*args, **kwargs)
                if debug_mode:
                    print(f"成功执行: {func.__name__}")
                return result
            except Exception as e:
                if debug_mode:
                    print(f"执行失败: {func.__name__}, 错误: {str(e)}")
                raise
    return wrapper

@debug_log
def load_config(config_path):
    """Load configuration from JSON file."""
    with open(config_path, 'r') as file:
        config = json.load(file)
    return config

@debug_log
def db_con_pymysql(config):
    """通过pymysql连接数据库"""
    db_config = config["DBConnection"]
    debug_mode = config.get('DEBUG', False)
    try:
        pymysql_conn = pymysql.connect(
            host=db_config["host"],
            user=db_config["user"],
            password=db_config["password"],
            database=db_config["database"],
            cursorclass=pymysql.cursors.DictCursor,
        )
        if debug_mode:
            print(f"PyMySQL成功连接到数据库: {db_config['host']}/{db_config['database']}")
        return pymysql_conn
    except Exception as e:
        if debug_mode:
            print(f"PyMySQL连接数据库失败: {str(e)}")
        raise

@debug_log
def db_con_sqlalchemy(config):
    """通过sqlalchemy连接数据库"""
    db_config = config["DBConnection"]
    debug_mode = config.get('DEBUG', False)
    try:
        sqlalchemy_conn = create_engine(
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}@{db_config['host']}/{db_config['database']}"
        )
        # 测试连接
        sqlalchemy_conn.connect()
        if debug_mode:
            print(f"SQLAlchemy成功连接到数据库: {db_config['host']}/{db_config['database']}")
        return sqlalchemy_conn
    except Exception as e:
        if debug_mode:
            print(f"SQLAlchemy连接数据库失败: {str(e)}")
        raise

@debug_log
def set_log(config, log_name, prefix="QA"):
    """
    设置日志
    Args:
        config: 配置文件
        log_name: 日志文件名
        prefix: 日志消息前缀，默认为 "QA"
    """
    # 创建logger
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.INFO)

    # 清除现有的处理器
    if logger.handlers:
        logger.handlers.clear()

    # 创建文件处理器
    log_path = os.path.join(config["Log"]["log_path"], log_name)
    fh = logging.FileHandler(log_path, encoding='utf-8')
    fh.setLevel(logging.INFO)

    # 创建格式器
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)

    # 添加处理器到logger
    logger.addHandler(fh)

    # 添加自定义方法
    def info_print(message):
        print(message)
        logger.info(f"{prefix}: {message}")

    def error_print(message):
        print(f"错误: {message}")
        logger.error(f"{prefix}: {message}")

    def warning_print(message):
        print(f"警告: {message}")
        logger.warning(f"{prefix}: {message}")

    # 将新方法添加到logger对象
    logger.info_print = info_print
    logger.error_print = error_print
    logger.warning_print = warning_print

    return logger

def main():
    config_path_QA, config_path_PROD, root_dir = find_config_path()
    print(config_path_QA)
    print(config_path_PROD)
    print(root_dir)
    config = load_config(config_path_QA)
    db_con_pymysql(config)
    db_con_sqlalchemy(config)
    set_log(config, "test.log")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"处理失败：{str(e)}")