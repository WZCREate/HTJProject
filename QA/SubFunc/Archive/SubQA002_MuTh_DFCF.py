'''
本程序读取 "MainCSV" 文件中的股票代码并请求日期范围内的数据写入数据库
数据表名为 配置文件中的 "buffer_table"
用于批量请求长时间范围
!!!!日常更新勿用
多线程 + 东方财富直接API版本：绕过akshare，直接调用东方财富接口，使用多线程加速
'''

import pandas as pd
import sys
import logging
import os
from datetime import datetime
import time
import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import random

# 确保导入当前项目的CommonFunc模块
current_file_dir = os.path.dirname(os.path.abspath(__file__))  # QA/SubFunc/
qa_func_dir = os.path.dirname(current_file_dir)               # QA/
project_root = os.path.dirname(qa_func_dir)                  # StockFilter/
if project_root not in sys.path:
    sys.path.insert(0, project_root)  # 将当前项目路径插入到最前面

from CommonFunc.DBconnection import find_config_path, load_config, set_log, db_con_pymysql

class EastMoneyDataFetcher:
    """东方财富数据获取器 - 线程安全版本"""
    
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
        从东方财富获取股票K线数据 - 线程安全版本
        
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
                # 添加随机延时，避免请求过于频繁
                time.sleep(random.uniform(0.1, 0.5))
                
                response = requests.get(
                    self.base_url, 
                    params=params, 
                    headers=self.headers, 
                    timeout=15
                )
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
                retries -= 1
                if retries > 0:
                    logging.warning(f"请求超时 {stock_code}, 重试 {3 - retries + 1} 次...")
                    time.sleep(random.uniform(2, 4))  # 随机延时重试
                    
            except requests.exceptions.RequestException as e:
                retries -= 1
                if retries > 0:
                    logging.error(f"网络请求失败 {stock_code}: {e}, 重试 {3 - retries + 1} 次...")
                    time.sleep(random.uniform(3, 5))  # 随机延时重试
                    
            except json.JSONDecodeError as e:
                logging.error(f"JSON解析失败 {stock_code}: {e}")
                return None
                
            except Exception as e:
                logging.error(f"获取数据时出错 {stock_code}: {e}")
                return None
        
        return None

def check_and_clear_table(connection, table_name, logger):
    """清空指定表"""
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE {table_name}")
            logger.info_print(f"成功清空表 {table_name}")
        return True
        
    except Exception as e:
        logger.error_print(f"清空表 {table_name} 时发生错误: {e}")
        return False

def process_single_stock(stock, logger, config, start_date, end_date, fetcher):
    """处理单个股票的数据获取和保存 - 使用东方财富API"""
    try:
        # 建立数据库连接（每个线程独立的连接）
        connection = db_con_pymysql(config)
        
        try:
            # 使用东方财富API获取数据，添加重试机制
            retries = 3
            stock_data = None
            
            while retries > 0:
                try:
                    # 添加随机延时，避免请求过于频繁
                    time.sleep(random.uniform(0.2, 1.0))
                    
                    stock_data = fetcher.fetch_stock_data(
                        stock_code=stock,
                        start_date=start_date,
                        end_date=end_date,
                        adjust_type="1"  # 前复权
                    )
                    
                    # 请求成功，检查数据是否为空
                    if stock_data is None or stock_data.empty:
                        logger.warning_print(f"股票 {stock} 在指定时间范围内无数据")
                        return "no_data"  # 直接返回无数据状态，不进行重试
                    
                    # 数据获取成功且不为空，跳出重试循环
                    break
                        
                except (requests.exceptions.Timeout, 
                        requests.exceptions.ConnectionError,
                        requests.exceptions.RequestException) as e:
                    retries -= 1
                    if retries > 0:
                        logger.warning_print(f"股票 {stock} 连接错误: {str(e)}，将在3-5秒后进行第 {3-retries} 次重试")
                        time.sleep(random.uniform(3, 5))
                    else:
                        logger.error_print(f"股票 {stock} 在重试3次后仍然失败: {str(e)}")
                        return "api_fail"
                    continue
                    
                except Exception as e:
                    retries -= 1
                    if retries > 0:
                        logger.warning_print(f"股票 {stock} 未知错误: {str(e)}，将在3-5秒后进行第 {3-retries} 次重试")
                        time.sleep(random.uniform(3, 5))
                    else:
                        logger.error_print(f"股票 {stock} 在重试3次后仍然失败: {str(e)}")
                        return "api_fail"
                    continue
            
            # 准备数据 - 转换DataFrame为插入格式
            data_to_insert = [
                (
                    row["日期"], row["股票代码"], row["开盘"], row["收盘"], row["最高"],
                    row["最低"], row["成交量"], row["成交额"], row["振幅"], row["涨跌幅"],
                    row["涨跌额"], row["换手率"]
                )
                for _, row in stock_data.iterrows()
            ]
            
            try:
                with connection.cursor() as cursor:
                    # 插入数据
                    insert_query = f"""
                    INSERT INTO {config["DB_tables"]["buffer_table"]} (
                        date, id, open_price, close_price, high, low, volume, 
                        turnover, amplitude, chg_percen, chg_amount, turnover_rate,
                        Insrt_time, Latest
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), 0)
                    """
                    cursor.executemany(insert_query, data_to_insert)
                    connection.commit()
                    return "success"
                    
            except Exception as e:
                connection.rollback()
                logger.error_print(f"股票 {stock} 数据库写入失败: {str(e)}")
                return "db_fail"
                
        except Exception as e:
            logger.error_print(f"股票 {stock} 的数据获取失败: {str(e)}")
            return "api_fail"
            
        finally:
            connection.close()
            
    except Exception as e:
        logger.error_print(f"股票 {stock} 处理过程出现错误: {str(e)}")
        return "error"

def main():
    """主函数"""
    config_path, _, root_dir = find_config_path()
    config = load_config(config_path)
    logger = set_log(config, "SubQA002_MuTh_DFCF.log", "QA")  # 设置日志记录器

    logger.info_print("开始执行数据导入程序 (多线程 + 东方财富直接API版本)...")

    try:
        connection = db_con_pymysql(config)
        if check_and_clear_table(connection, config["DB_tables"]["buffer_table"], logger):
            # 读取股票列表
            csv_path = os.path.join(root_dir, "QA", config["CSVs"]["MainCSV"])
            stock_list_df = pd.read_csv(csv_path, dtype={1: str})
            stock_codes = stock_list_df.iloc[:, 1].tolist()
            
            start_date = config["ProgormInput"]["massive_insrt_start_date"]
            end_date = config["ProgormInput"]["massive_insrt_end_date"]
            
            total_stocks = len(stock_codes)
            logger.info_print(f"成功读取股票列表，共 {total_stocks} 只股票")
            logger.info_print(f"数据获取时间范围: {start_date} 至 {end_date}")
            
            # 初始化东方财富数据获取器
            fetcher = EastMoneyDataFetcher()
            
            # 初始化计数器
            api_success = 0
            db_success = 0
            no_data_stocks = []
            failed_stocks = []
            completed = 0
            
            # 使用线程池处理所有数据 - 适当减少线程数避免过于频繁的请求
            max_workers = min(12, len(stock_codes))  # 最多12个线程，避免过度并发
            logger.info_print(f"使用 {max_workers} 个线程进行并发处理")
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                futures = {
                    executor.submit(
                        process_single_stock, 
                        stock, 
                        logger,
                        config, 
                        start_date, 
                        end_date,
                        fetcher
                    ): stock for stock in stock_codes
                }
                
                # 获取任务结果
                for future in as_completed(futures):
                    stock = futures[future]
                    completed += 1
                    try:
                        result = future.result()
                        if result == "success":
                            api_success += 1
                            db_success += 1
                        elif result == "no_data":
                            no_data_stocks.append(stock)
                            logger.warning_print(f"股票 {stock} 在指定时间范围内无数据")
                        elif result == "api_fail":
                            failed_stocks.append(stock)
                            logger.error_print(f"股票 {stock} API请求失败")
                        elif result == "db_fail":
                            api_success += 1
                            failed_stocks.append(stock)
                            logger.error_print(f"股票 {stock} 数据库写入失败")
                        else:
                            failed_stocks.append(stock)
                            logger.error_print(f"股票 {stock} 处理失败，未知原因")
                            
                    except Exception as e:
                        logger.error_print(f"股票 {stock} 执行出现异常: {str(e)}")
                        failed_stocks.append(stock)
                    
                    finally:
                        # 无论成功失败都更新进度
                        success_rate = api_success/completed if completed > 0 else 0
                        db_rate = db_success/completed if completed > 0 else 0
                        print(f"\r进度: {completed}/{total_stocks} | "
                              f"数据获取成功率: {success_rate:.1%} | "
                              f"数据写入成功率: {db_rate:.1%} | "
                              f"当前处理: {stock}", 
                              end="", flush=True)
            
            print()  # 换行
            
            # 打印最终结果
            logger.info_print("=== 处理完成统计 ===")
            logger.info_print(f"成功处理: {db_success} 只股票")
            logger.info_print(f"失败处理: {len(failed_stocks)} 只股票")
            logger.info_print(f"无数据股票: {len(no_data_stocks)} 只股票")
            logger.info_print(f"总体数据获取成功率: {api_success/total_stocks:.2%}")
            logger.info_print(f"数据写入成功率: {db_success/total_stocks:.2%}")
            
            if no_data_stocks:
                logger.warning_print(f"无数据股票 (前20个): {', '.join(no_data_stocks[:20])}")
                if len(no_data_stocks) > 20:
                    logger.warning_print(f"... 还有 {len(no_data_stocks) - 20} 个无数据股票")
                    
            if failed_stocks:
                logger.warning_print(f"处理失败的股票 (前20个): {', '.join(failed_stocks[:20])}")
                if len(failed_stocks) > 20:
                    logger.warning_print(f"... 还有 {len(failed_stocks) - 20} 个失败股票")
                
                # 保存失败列表
                failed_df = pd.DataFrame(failed_stocks, columns=['股票代码'])
                failed_df.to_csv('failed_stocks_multithread_dfcf.csv', index=False)
                logger.info_print("已保存失败股票列表到 failed_stocks_multithread_dfcf.csv")
            
            logger.info_print("所有数据处理完成！")
            
            return True

    except Exception as e:
        logger.error_print(f"程序执行出错: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        result = main()
        if result:
            print("程序正常结束")
            sys.exit(0)
        else:
            print("程序未正常完成")
            sys.exit(1)
    except Exception as e:
        print(f"程序异常终止: {str(e)}")
        sys.exit(1) 