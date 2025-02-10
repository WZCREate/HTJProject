'''
本程序运行时,判断运行日期是否为工作日.
Y:使用当日日期为processing_date.
N:使用工作日历中的最后一个工作日日期processing_date.
判断processing_date的数据表是否存在,
Y:不进行操作
N:根据上一个工作日数据表创建processing_date的数据表
或者,缺少多个工作日数据表的时候调用'SubQA002.py'来进行批量数据获取,并写入数据表"buffer_table"
'''
import os
import re
import datetime
import json
import pymysql
from chinese_calendar import is_workday
from QA.SubFunc.SubQA002 import main as MasvImprt
from CommonFunc.DBconnection import find_config_path
from CommonFunc.DBconnection import load_config
from CommonFunc.DBconnection import set_log

def last_workday(date, logger):
    '''确定最后一个工作日,只有当非工作日的时候才会调用'''
    one_day = datetime.timedelta(days=1)
    previous_day = date - one_day
    while not is_workday(previous_day):
        previous_day -= one_day
    return previous_day

def is_today_workday(logger):
    '''
    确定处理日期:
    1. 如果今天不是工作日，返回上一个工作日
    2. 如果今天是工作日：
       - 如果当前时间早于15:30，返回上一个工作日
       - 如果当前时间晚于15:30，返回今天
    返回值: (bool, datetime.date)
    bool: True表示使用当天日期，False表示使用上一工作日
    '''
    today = datetime.date.today()
    current_time = datetime.datetime.now().time()
    market_close = datetime.time(15, 30)  # 下午3:30

    if not is_workday(today):
        last_work_day = last_workday(today, logger)
        logger.info(f"QA: 今天非工作日，将 {last_work_day.strftime('%Y-%m-%d')} 视为处理日期。")
        print(f"QA: 今天非工作日，将 {last_work_day.strftime('%Y-%m-%d')} 视为处理日期。")
        return False, last_work_day
    else:
        if current_time < market_close:
            last_work_day = last_workday(today, logger)
            logger.info(f"QA: 当前时间 {current_time.strftime('%H:%M')} 早于收盘时间 {market_close.strftime('%H:%M')}，"
                       f"将 {last_work_day.strftime('%Y-%m-%d')} 视为处理日期。")
            print(f"QA: 当前时间 {current_time.strftime('%H:%M')} 早于收盘时间 {market_close.strftime('%H:%M')}，"
                  f"将 {last_work_day.strftime('%Y-%m-%d')} 视为处理日期。")
            return False, last_work_day
        else:
            logger.info(f"QA: 当前时间 {current_time.strftime('%H:%M')} 晚于收盘时间 {market_close.strftime('%H:%M')}，"
                       f"将 {today.strftime('%Y-%m-%d')} 视为处理日期。")
            print(f"QA: 当前时间 {current_time.strftime('%H:%M')} 晚于收盘时间 {market_close.strftime('%H:%M')}，"
                  f"将 {today.strftime('%Y-%m-%d')} 视为处理日期。")
            return True, today

def table_exists(cursor, table_name):
    '''检查表是否存在'''
    cursor.execute(f"SHOW TABLES LIKE '{table_name}';")
    return cursor.fetchone() is not None

def create_table_like_previous(table_name_today, table_name_previous, cursor, logger):
    '''根据上一张表创建新表'''
    sql = f"CREATE TABLE {table_name_today} LIKE {table_name_previous};"
    cursor.execute(sql)
    logger.info(f"根据 {table_name_previous} 成功创建了表 {table_name_today}。")
    print(f"根据 {table_name_previous} 成功创建了表 {table_name_today}。")

def update_config_date(date, config_path, logger):
    '''更改配置文件'''
    with open(config_path, "r") as file:
        config = json.load(file)
    # 获取原始值用于日志记录
    original_table_name = config["DB_tables"]["daily_update_table"]
    original_date = config["DBinput"]["last_update_date"]
    
    # 更新配置
    config["DB_tables"]["daily_update_table"] = date.strftime("%b%d")
    config["DBinput"]["last_update_date"] = date.strftime("%Y-%m-%d")
    
    with open(config_path, "w") as file:
        json.dump(config, file, indent=4)

    log_message = f"QA: 配置文件 'daily_update_table': '{original_table_name}' -> '{date.strftime('%b%d')}'.\n"
    log_message += f"QA: 配置文件 'last_update_date': '{original_date}' -> '{date.strftime('%Y-%m-%d')}'."
    logger.info(log_message)
    print(log_message)

def update_config_massive_info(config_path, start_date, end_date, logger):
    # 读取配置文件
    with open(config_path, 'r') as file:
        config = json.load(file)
    
    # 更新配置
    config["ProgormInput"]['massive_insrt_start_date'] = start_date.strftime('%Y%m%d')
    config["ProgormInput"]['massive_insrt_end_date'] = end_date.strftime('%Y%m%d')
    
    # 写回配置文件
    with open(config_path, 'w') as file:
        json.dump(config, file, indent=4)

    logger.info("QA: 配置文件中批量查询起始终止日期已更新.")
    print("QA: 配置文件中批量查询起始终止日期已更新.")

def report_missing(cursor, processing_date, config_path, logger):
    one_day = datetime.timedelta(days=1)
    try:
        cursor.execute("SHOW TABLES;")
        tables = cursor.fetchall()
        
        if not tables:
            logger.info("这可能是一个新的数据库，请手动创建第一张工作日数据表。")
            print("这可能是一个新的数据库，请手动创建第一张工作日数据表。")
            return True
            
        # 获取数据库名称（用于构建正确的字典键）
        cursor.execute("SELECT DATABASE();")
        db_name = cursor.fetchone()['DATABASE()']
        table_key = f'Tables_in_{db_name}'
        
        # 使用正则表达式过滤出符合日期格式的表名
        pattern = re.compile(r'^[A-Za-z]{3}\d{2}$')
        filtered_tables = [table[table_key] for table in tables if pattern.match(table[table_key])]
        
        if not filtered_tables:
            logger.info("这可能是一个新的数据库，请手动创建第一张工作日数据表。")
            print("这可能是一个新的数据库，请手动创建第一张工作日数据表。")
            return True

        # 将表名按日期解析并排序，获取最后一个即最新的
        tables_sorted = sorted(
            filtered_tables,
            key=lambda x: datetime.datetime.strptime(x, '%b%d')
        )
        
        last_table_name = tables_sorted[-1]
        last_table_date = datetime.datetime.strptime(last_table_name, '%b%d').date()
        
        # 处理跨年情况
        if last_table_name.startswith('Dec') and processing_date.month < 12:
            # 如果最后一个表是12月，而当前处理日期是下一年的早期月份
            last_table_date = last_table_date.replace(year=processing_date.year - 1)
        else:
            last_table_date = last_table_date.replace(year=processing_date.year)
        
        missing_duration_start = last_table_date + one_day
        missing_duration_end = processing_date
        
        logger.info(f"QA: 缺少从{missing_duration_start.strftime('%Y-%m-%d')}到{missing_duration_end.strftime('%Y-%m-%d')}的数据表")
        update_config_massive_info(config_path, missing_duration_start, missing_duration_end, logger)
        print(f"QA: 缺少从{missing_duration_start.strftime('%Y-%m-%d')}到{missing_duration_end.strftime('%Y-%m-%d')}的数据表")
        return False
            
    except Exception as e:
        logger.error(f"查找数据库中最后一张数据表出错: {str(e)}")
        print(f"查找数据库中最后一张数据表出错: {str(e)}")
        logger.error(f"错误类型: {type(e)}")
        print(f"错误类型: {type(e)}")
        logger.error(f"错误详情: {repr(e)}")
        print(f"错误详情: {repr(e)}")
        return True

def create_table_in_DB(config_path, logger):
    '''判断被处理的日期是否为工作日'''
    workday_check, processing_date = is_today_workday(logger)

    # 如果是工作日，添加时间判断
    if workday_check:
        current_time = datetime.datetime.now().time()
        market_open = datetime.time(9, 20)
        market_close = datetime.time(15, 30)

        if current_time < market_open:
            logger.info(f"QA: 当前时间 {current_time.strftime('%H:%M')} 早于开盘时间 {market_open.strftime('%H:%M')}, 当前工作日尚未开盘。")
            print(f"QA: 当前时间 {current_time.strftime('%H:%M')} 早于开盘时间 {market_open.strftime('%H:%M')}, 当前工作日尚未开盘。")
            return False
        elif current_time < market_close:
            logger.info(f"QA: 当前时间 {current_time.strftime('%H:%M')} 早于收盘时间 {market_close.strftime('%H:%M')}, 当前工作日尚未收盘，收盘价尚未确定。")
            print(f"QA: 当前时间 {current_time.strftime('%H:%M')} 早于收盘时间 {market_close.strftime('%H:%M')}, 当前工作日尚未收盘，收盘价尚未确定。")
            return False

    config = load_config(config_path)
    db_config = {
        "host": config["DBConnection"]["host"],
        "user": config["DBConnection"]["user"],
        "password": config["DBConnection"]["password"],
        "database": config["DBConnection"]["database"],
        "cursorclass": pymysql.cursors.DictCursor
    }
    buffer_table = config["DB_tables"]["buffer_table"]
    
    update_config_date(processing_date, config_path, logger)
    
    try:
        conn = pymysql.connect(**db_config)
        cursor = conn.cursor()
        
        table_name_processing = processing_date.strftime("%b%d")
        if table_exists(cursor, table_name_processing):
            logger.info("QA: 最后一个工作日的数据表已经存在,无需再次创建")
            print("QA: 最后一个工作日的数据表已经存在,无需再次创建")
            return False
        else:
            previous_day = last_workday(processing_date, logger)
            table_name_previous = previous_day.strftime("%b%d")
            
            # 添加跨年调试日志
            logger.info(f"QA: 处理日期: {processing_date.strftime('%Y-%m-%d')}")
            logger.info(f"QA: 上一工作日: {previous_day.strftime('%Y-%m-%d')}")
            logger.info(f"QA: 当前表名: {table_name_processing}")
            logger.info(f"QA: 上一个表名: {table_name_previous}")
            
            if table_exists(cursor, table_name_previous):
                create_table_like_previous(table_name_processing, table_name_previous, cursor, logger)
                return True
            else:
                logger.info("QA: 缺少了至少两个工作日的数据表")
                print("QA: 缺少了至少两个工作日的数据表")
                is_new_db = report_missing(cursor, processing_date, config_path, logger)
                if not is_new_db:
                    logger.info(f"开始批量请求缺失数据并写入表{buffer_table}")
                    try:
                        MasvImprt()
                    except ValueError as ve:
                        logger.error(f"参数错误: {ve}")
                        print(f"参数错误: {ve}")
                    except IOError as ioe:
                        logger.error(f"文件读写错误: {ioe}")
                        print(f"文件读写错误: {ioe}")
                    except Exception as e:
                        logger.error(f"其他错误: {str(e)}")
                        print(f"其他错误: {str(e)}")
                return False

    except Exception as e:
        logger.error(f"QA: 数据库错误: {str(e)}")
    finally:
        cursor.close()
        conn.close()

def main():
    """
    函数作为统一的程序入口点，集中管理所有配置参数
    Returns:
        bool: 成功返回 True，失败返回 False
    """
    config_path, _, root_dir = find_config_path()
    qa_dir = os.path.dirname(config_path)
    config = load_config(config_path)
    logger = set_log(config, "QA002.log", prefix="QA")
    
    try:
        return create_table_in_DB(config_path, logger)
    except Exception as e:
        print(f"创建表时发生错误: {str(e)}")
        return False

if __name__ == "__main__":
    main()