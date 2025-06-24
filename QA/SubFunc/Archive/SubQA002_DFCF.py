'''
本程序读取 "daily_update_csv" 文件中的股票代码并请求日期范围内的数据写入数据库
数据表名为 配置文件中的 "buffer_table"
用于批量请求长时间范围
!!!!日常更新勿用
东方财富直接API版本：绕过akshare，直接调用东方财富接口
'''

import pandas as pd
import logging
import os
import sys
from datetime import datetime
import time
import requests
import json

# 确保导入当前项目的CommonFunc模块
current_file_dir = os.path.dirname(os.path.abspath(__file__))  # QA/SubFunc/
qa_func_dir = os.path.dirname(current_file_dir)               # QA/
project_root = os.path.dirname(qa_func_dir)                  # StockFilter/
if project_root not in sys.path:
    sys.path.insert(0, project_root)  # 将当前项目路径插入到最前面

from CommonFunc.DBconnection import find_config_path
from CommonFunc.DBconnection import load_config
from CommonFunc.DBconnection import set_log
from CommonFunc.DBconnection import db_con_pymysql

class EastMoneyDataFetcher:
    """东方财富数据获取器"""
    
    def __init__(self):
        self.base_url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Referer': 'https://quote.eastmoney.com/',
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'
        }
    
    def get_stock_code_with_prefix(self, stock_code):
        """为股票代码添加市场前缀"""
        if stock_code.startswith('6'):
            return f"1.{stock_code}"  # 上海市场
        elif stock_code.startswith(('0', '3')):
            return f"0.{stock_code}"  # 深圳市场
        elif stock_code.startswith(('688', '689')):
            return f"1.{stock_code}"  # 科创板
        elif stock_code.startswith('8'):
            return f"0.{stock_code}"  # 北交所
        else:
            return f"1.{stock_code}"  # 默认上海市场
    
    def fetch_stock_data(self, stock_code, start_date, end_date, adjust_type="1"):
        """
        从东方财富获取股票K线数据
        
        参数:
        stock_code: 股票代码 (如 '600734')
        start_date: 开始日期 (格式: '20250101')
        end_date: 结束日期 (格式: '20250120')
        adjust_type: 复权类型 ('1': 前复权, '2': 后复权, '0': 不复权)
        """
        
        # 构造完整的股票代码
        full_code = self.get_stock_code_with_prefix(stock_code)
        
        # API参数
        params = {
            'fields1': 'f1,f2,f3,f4,f5,f6',
            'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61',
            'ut': 'fa5fd1943c7b386f172d6893dbfba10b',
            'klt': '101',  # 日K线
            'fqt': adjust_type,  # 复权类型
            'secid': full_code,
            'beg': start_date,
            'end': end_date,
            '_': str(int(time.time() * 1000))
        }
        
        retries = 3
        while retries > 0:
            try:
                response = requests.get(self.base_url, params=params, headers=self.headers, timeout=15)
                response.raise_for_status()
                
                data = response.json()
                
                if data.get('rc') != 0:
                    logging.warning(f"API返回错误 {stock_code}: {data.get('rt', 'Unknown error')}")
                    return None
                
                klines = data.get('data', {}).get('klines', [])
                if not klines:
                    logging.info(f"股票 {stock_code} 在指定日期范围内没有数据")
                    return None
                
                # 解析K线数据
                df_data = []
                for kline in klines:
                    parts = kline.split(',')
                    if len(parts) >= 11:
                        df_data.append({
                            '日期': parts[0],
                            '股票代码': stock_code,
                            '开盘': float(parts[1]),
                            '收盘': float(parts[2]),
                            '最高': float(parts[3]),
                            '最低': float(parts[4]),
                            '成交量': int(parts[5]),
                            '成交额': float(parts[6]),
                            '振幅': float(parts[7]),
                            '涨跌幅': float(parts[8]),
                            '涨跌额': float(parts[9]),
                            '换手率': float(parts[10])
                        })
                
                if df_data:
                    df = pd.DataFrame(df_data)
                    return df
                else:
                    return None
                    
            except requests.exceptions.Timeout:
                logging.warning(f"请求超时 {stock_code}, 重试 {3 - retries + 1} 次...")
                retries -= 1
                if retries > 0:
                    time.sleep(2)
                    
            except requests.exceptions.RequestException as e:
                logging.error(f"网络请求失败 {stock_code}: {e}")
                retries -= 1
                if retries > 0:
                    time.sleep(3)
                    
            except json.JSONDecodeError as e:
                logging.error(f"JSON解析失败 {stock_code}: {e}")
                return None
                
            except Exception as e:
                logging.error(f"获取数据时出错 {stock_code}: {e}")
                return None
        
        return None

def check_and_clear_table(connection, table_name):
    """清空指定表"""
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {table_name}")
        return True
    except Exception as e:
        print(f"清空表 {table_name} 时发生错误: {e}")
        return False

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

def main():
    """主函数"""
    config_path, _, root_dir = find_config_path()
    config = load_config(config_path)
    set_log(config, "SubQA002_EastMoney")

    print("开始执行数据导入程序 (东方财富直接API版本)...")
    
    # 初始化数据获取器
    fetcher = EastMoneyDataFetcher()
    
    # 记录统计信息
    successful_stocks = []
    failed_stocks = []
    total_records = 0

    try:
        connection = db_con_pymysql(config)
        if check_and_clear_table(connection, config["DB_tables"]["buffer_table"]):
            # 读取股票列表
            csv_path = os.path.join(root_dir, "QA", config["CSVs"]["MainCSV"])
            stock_list_df = pd.read_csv(csv_path, dtype={1: str})
            stock_codes = stock_list_df.iloc[:, 1].tolist()
            
            start_date = config["ProgormInput"]["massive_insrt_start_date"]
            end_date = config["ProgormInput"]["massive_insrt_end_date"]
            
            total_stocks = len(stock_codes)
            print(f"共需处理 {total_stocks} 只股票")
            print(f"日期范围: {start_date} 到 {end_date}")
            
            # 遍历处理每只股票
            for idx, stock_code in enumerate(stock_codes, start=1):
                print(f"正在处理 {idx}/{total_stocks}: {stock_code}")
                
                try:
                    # 尝试前复权数据
                    data = fetcher.fetch_stock_data(stock_code, start_date, end_date, "1")
                    
                    if data is not None and not data.empty:
                        # 转换数据格式用于数据库插入
                        data_to_insert = [
                            (
                                row["日期"], row["股票代码"], row["开盘"], row["收盘"], row["最高"],
                                row["最低"], row["成交量"], row["成交额"], row["振幅"], row["涨跌幅"],
                                row["涨跌额"], row["换手率"]
                            )
                            for _, row in data.iterrows()
                        ]
                        
                        insert_data_to_mysql(config, data_to_insert)
                        successful_stocks.append(stock_code)
                        total_records += len(data)
                        print(f"✓ {stock_code} 数据入库完成，共 {len(data)} 条记录")
                        
                    else:
                        failed_stocks.append(stock_code)
                        print(f"✗ {stock_code} 数据获取失败或为空")
                        
                except Exception as e:
                    failed_stocks.append(stock_code)
                    print(f"✗ {stock_code} 处理出错: {e}")
                    logging.error(f"处理股票 {stock_code} 时出错: {str(e)}")
                
                # 添加适当的延时，避免请求过快
                if idx % 10 == 0:  # 每处理10只股票稍作休息
                    time.sleep(1)

            # 输出统计信息
            print(f"\n=== 处理完成统计 ===")
            print(f"成功处理: {len(successful_stocks)} 只股票")
            print(f"失败处理: {len(failed_stocks)} 只股票")
            print(f"总记录数: {total_records} 条")
            print(f"成功率: {len(successful_stocks)/len(stock_codes)*100:.2f}%")
            
            if failed_stocks:
                print(f"\n失败的股票代码 (前20个):")
                for code in failed_stocks[:20]:
                    print(f"  - {code}")
                if len(failed_stocks) > 20:
                    print(f"  ... 还有 {len(failed_stocks) - 20} 个")
                
                # 保存失败列表
                failed_df = pd.DataFrame(failed_stocks, columns=['股票代码'])
                failed_df.to_csv('failed_stocks_eastmoney.csv', index=False)
                print("已保存失败股票列表到 failed_stocks_eastmoney.csv")

            print("\n所有数据处理完成！")

    except Exception as e:
        print(f"程序执行出错: {str(e)}")
        logging.error(f"程序执行出错: {str(e)}")
        raise

if __name__ == "__main__":
    main() 