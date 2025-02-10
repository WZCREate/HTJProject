"""
本程序非优先使用
本程序对MainCSV中所有骨牌哦代码进行周K数据请求
开始日期: 20230101
结束日期: 程序运行的日期
使用单线程 
"""

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

def insert_data_to_mysql(config, data):
    """
    将数据插入MySQL数据库，如果记录已存在则更新
    Args:
        config: 配置信息
        data: 要插入的数据列表
    """
    connection = db_con_pymysql(config)
    try:
        with connection.cursor() as cursor:
            insert_query = """
            INSERT INTO WK (
                id, wkn, open, close, high, low, update_time, status
            ) VALUES (%s, %s, %s, %s, %s, %s, NOW(), 'active')
            ON DUPLICATE KEY UPDATE
            open = VALUES(open),
            close = VALUES(close),
            high = VALUES(high),
            low = VALUES(low),
            update_time = VALUES(update_time),
            status = VALUES(status)
            """
            cursor.executemany(insert_query, data)  # 批量插入或更新数据
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
            print(f"正在请求股票 {stock_code} 的数据...")
            stock_data = ak.stock_zh_a_hist(
                symbol=stock_code,
                period="weekly",
                start_date=start_date,
                end_date=end_date,
                adjust="qfq"
            )
            
            if stock_data is None or stock_data.empty:
                print(f"警告：股票 {stock_code} 返回空数据，可能已退市")
                return None
                
            print(f"成功获取股票 {stock_code} 的数据")
            return stock_data
            
        except SSLError as e:
            print(f"SSL 错误：股票 {stock_code} 请求失败，将在3秒后进行第 {3-retries+1} 次重试")
            retries -= 1
            if retries > 0:
                time.sleep(3)
            else:
                print(f"错误：股票 {stock_code} 在重试3次后仍然失败")
                return None
                
        except Exception as e:
            print(f"未知错误：股票 {stock_code} 请求失败 - {str(e)}")
            retries -= 1
            if retries > 0:
                print(f"将在3秒后进行第 {3-retries+1} 次重试")
                time.sleep(3)
            else:
                print(f"错误：股票 {stock_code} 在重试3次后仍然失败")
                return None
    
    return None

def main():
    """主函数"""
    config_path, _, root_dir = find_config_path()
    config = load_config(config_path)
    set_log(config, "SubQA002")

    print("开始执行数据导入程序...")

    try:
        # 读取股票列表
        csv_path = os.path.join(root_dir, "QA", config["CSVs"]["MainCSV"])
        stock_list_df = pd.read_csv(csv_path, dtype={1: str})
        stock_codes = stock_list_df.iloc[:, 1].tolist()
        
        start_date = config["ProgormInput"]["massive_insrt_start_date"]
        end_date = config["ProgormInput"]["massive_insrt_end_date"]
        
        total_stocks = len(stock_codes)
        print(f"共需处理 {total_stocks} 只股票")
        
        for idx, stock_code in enumerate(stock_codes, start=1):
            print(f"正在处理 {idx}/{total_stocks}: {stock_code}")
            data = fetch_stock_data(stock_code, start_date, end_date)
            if data is not None:
                # 转换日期为周格式
                data['周数'] = pd.to_datetime(data['日期']).apply(
                    lambda x: f"{str(x.year)[2:]}W{x.strftime('%V')}"
                )
                
                data_to_insert = [
                    (
                        row["股票代码"],
                        row["周数"],
                        row["开盘"],
                        row["收盘"],
                        row["最高"],
                        row["最低"]
                    )
                    for _, row in data.iterrows()
                ]
                insert_data_to_mysql(config, data_to_insert)
                print(f"✓ {stock_code} 数据入库完成")
            else:
                print(f"✗ {stock_code} 数据获取失败")

        print("\n所有数据处理完成！")

    except Exception as e:
        print(f"程序执行出错: {str(e)}")
        logging.error(f"程序执行出错: {str(e)}")
        raise

if __name__ == "__main__":
    main()
    main()