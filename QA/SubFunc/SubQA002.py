'''
本程序读取 "daily_update_csv" 文件中的股票代码并请求日期范围内的数据写入数据库 (stock_zh_a_hist)
数据表名为 配置文件中的 "buffer_table"
用于批量请求长时间范围, 
!!!!日常更新勿用
'''

import akshare as ak
import pandas as pd
import logging
import os
from datetime import datetime
import time
from requests.exceptions import SSLError
from CommonFunc.DBconnection import find_config_path
from CommonFunc.DBconnection import load_config
from CommonFunc.DBconnection import set_log
from CommonFunc.DBconnection import db_con_pymysql

def check_and_clear_table(connection, table_name):
    """清空指定表"""
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {table_name}")
        return True
    except Exception as e:
        print(f"清空表 {table_name} 时发生错误: {e}")
        return False

# MySQL 插入数据函数
def insert_data_to_mysql(config, data):
    """
    将数据插入MySQL数据库
    Args:
        config: 配置信息
        data: 要插入的数据列表
    """
    buffer_table_name = config["DB_tables"]["buffer_table"]
    connection = db_con_pymysql(config)
    try:
        with connection.cursor() as cursor:
            insert_query = f"""
            INSERT INTO {buffer_table_name} (
                date, id, open_price, close_price, high, low, volume, 
                turnover, amplitude, chg_percen, chg_amount, turnover_rate,
                Insrt_time, Latest
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), 0)
            """
            cursor.executemany(insert_query, data)  # 批量插入数据
            connection.commit()
    except Exception as e:
        logging.error(f"数据插入失败: {str(e)}")
        connection.rollback()
        raise
    finally:
        connection.close()

def fetch_stock_data(stock_code, start_date, end_date):
    """获取单只股票的数据"""
    retries = 3
    while retries > 0:
        try:
            stock_data = ak.stock_zh_a_hist(
                symbol=stock_code,
                period="daily",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )
            return stock_data if not stock_data.empty else None
        except SSLError as e:
            logging.warning(f"SSL 错误：{e}. 正在重试 {3 - retries + 1} 次...")
            retries -= 1
            if retries > 0:
                time.sleep(3)
    return None

def main():
    """主函数"""
    config_path, _, root_dir = find_config_path()
    config = load_config(config_path)
    set_log(config, "SubQA002")

    print("开始执行数据导入程序...")  # 添加开始提示

    try:
        connection = db_con_pymysql(config)
        if check_and_clear_table(connection, config["DB_tables"]["buffer_table"]):
            # 读取股票列表
            csv_path = os.path.join(root_dir, "QA", config["CSVs"]["MainCSV"])
            stock_list_df = pd.read_csv(csv_path, dtype={1: str})
            stock_codes = stock_list_df.iloc[:, 1].tolist()
            
            start_date = config["ProgormInput"]["massive_insrt_start_date"]
            end_date = config["ProgormInput"]["massive_insrt_end_date"]
            
            total_stocks = len(stock_codes)  # 获取总数
            print(f"共需处理 {total_stocks} 只股票")  # 添加总数提示
            
            # 遍历处理每只股票
            for idx, stock_code in enumerate(stock_codes, start=1):
                print(f"正在处理 {idx}/{total_stocks}: {stock_code}")  # 添加进度提示
                data = fetch_stock_data(stock_code, start_date, end_date)
                if data is not None:
                    data_to_insert = [
                        (
                            row["日期"], row["股票代码"], row["开盘"], row["收盘"], row["最高"],
                            row["最低"], row["成交量"], row["成交额"], row["振幅"], row["涨跌幅"],
                            row["涨跌额"], row["换手率"]
                        )
                        for _, row in data.iterrows()
                    ]
                    insert_data_to_mysql(config, data_to_insert)
                    print(f"✓ {stock_code} 数据入库完成")  # 添加完成提示
                else:
                    print(f"✗ {stock_code} 数据获取失败")  # 添加失败提示

            print("\n所有数据处理完成！")  # 添加结束提示

    except Exception as e:
        print(f"程序执行出错: {str(e)}")  # 添加错误提示
        logging.error(f"程序执行出错: {str(e)}")
        raise

if __name__ == "__main__":
    main()