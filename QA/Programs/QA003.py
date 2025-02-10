'''
收盘后执行本程序 查询实时数据
读取配置文件中的 daily_update_table
将查询到的实时数据写入 daily_update_table 指定的表中
'''

import akshare as ak
import pymysql
import numpy as np
import pandas as pd
import logging
from datetime import datetime
from CommonFunc.DBconnection import (
    load_config, 
    db_con_pymysql,
    set_log,
    find_config_path
)


def fetch_stock_data():
    """
    Fetch real-time stock data using akshare.
    
    Returns:
        pandas.DataFrame: Stock data with NaN values replaced by None
    """
    try:
        stock_data = ak.stock_zh_a_spot_em()
        return stock_data.replace({np.nan: None})
    except Exception as e:
        raise Exception(f"获取股票数据时出错: {str(e)}")


def insert_data_to_mysql(data, table_name, config, logger):
    """
    Insert stock data into MySQL database.
    
    Args:
        data (pandas.DataFrame): Stock data to insert
        table_name (str): Name of the target table
        config (dict): Configuration dictionary
        logger (logging.Logger): Logger instance
    """
    connection = db_con_pymysql(config)
    
    try:
        with connection.cursor() as cursor:
            insert_query = """
            INSERT INTO {table} (
                ord, Id, nname, newprice, chg_percen, chg_amount,
                volume, turnover, amplitude, high, low, opentoday, closeyesterday, volume_ratio,
                turnover_rate, pe_ratio, pb_ratio, market_cap, circulating_market_cap,
                change_speed, change_5min, change_60d, change_ytd, insrt_time
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """.format(table=table_name)
            
            for idx, row in data.iterrows():
                cursor.execute(insert_query, (
                    idx, row['代码'], row['名称'], row['最新价'], row['涨跌幅'], row['涨跌额'],
                    row['成交量'], row['成交额'], row['振幅'], row['最高'], row['最低'],
                    row['今开'], row['昨收'], row['量比'], row['换手率'], row['市盈率-动态'],
                    row['市净率'], row['总市值'], row['流通市值'], row['涨速'], row['5分钟涨跌'],
                    row['60日涨跌幅'], row['年初至今涨跌幅']
                ))
            connection.commit()
            log_message = f"数据已成功插入到 {table_name} 表中。"
            logger.info(log_message)
            print(log_message)
            
    except Exception as e:
        error_message = f"插入数据时出错: {str(e)}"
        logger.error(error_message)
        print(error_message)
        raise
    finally:
        connection.close()


def update_stock_data(logger):
    """
    将实时数据写入到日表中
    
    Args:
        logger (logging.Logger): Logger instance
    
    Returns:
        bool: True if successful, False otherwise
    """
    config_path_QA, _, _ = find_config_path()
    config = load_config(config_path_QA)
    table_name = config["DB_tables"]["daily_update_table"]
    
    try:
        stock_data = fetch_stock_data()
        insert_data_to_mysql(stock_data, table_name, config, logger)
        return True
    except Exception as e:
        error_message = f"更新股票数据时出错: {str(e)}"
        logger.error(error_message)
        print(error_message)
        return False


def main():
    """
    Day table to Main table 主函数
    Returns:
        bool: 成功返回 True，失败返回 False
    """
    try:
        config_path_QA, _, _ = find_config_path()
        config = load_config(config_path_QA)
        logger = set_log(config, "QA003.log", prefix="QA")
        success = update_stock_data(logger)
        return success
    except Exception as e:
        print(f"处理过程中出现错误：{e}")
        return False


if __name__ == "__main__":
    main()