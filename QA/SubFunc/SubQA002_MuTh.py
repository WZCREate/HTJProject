'''
本程序读取 "MainCSV" 文件中的股票代码并请求日期范围内的数据写入数据库 (stock_zh_a_hist)
数据表名为 配置文件中的 "buffer_table"
用于批量请求长时间范围, 
!!!!日常更新勿用
本程序为多线程版本, 使用时断开 VPN
限制：每小时最多请求300支股票，最大线程数8
'''

import akshare as ak
import pandas as pd
import sys
import logging
import os
from datetime import datetime, timedelta
import time
from requests.exceptions import SSLError
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# 确保导入当前项目的CommonFunc模块
current_file_dir = os.path.dirname(os.path.abspath(__file__))  # QA/SubFunc/
qa_func_dir = os.path.dirname(current_file_dir)               # QA/
project_root = os.path.dirname(qa_func_dir)                  # StockFilter/
if project_root not in sys.path:
    sys.path.insert(0, project_root)  # 将当前项目路径插入到最前面

from CommonFunc.DBconnection import find_config_path, load_config, set_log, db_con_pymysql
import random
import requests

# 全局请求限制器
class RequestLimiter:
    def __init__(self, max_requests_per_hour=300):
        self.max_requests_per_hour = max_requests_per_hour
        self.request_times = []
        self.lock = threading.Lock()
        
    def can_make_request(self):
        """检查是否可以发起新请求"""
        with self.lock:
            current_time = time.time()
            # 清理一小时前的请求记录
            self.request_times = [t for t in self.request_times if current_time - t < 3600]
            
            # 检查是否超过限制
            return len(self.request_times) < self.max_requests_per_hour
    
    def record_request(self):
        """记录一次请求"""
        with self.lock:
            self.request_times.append(time.time())
    
    def get_wait_time(self):
        """获取需要等待的时间（秒）"""
        with self.lock:
            if not self.request_times:
                return 0
            
            current_time = time.time()
            # 清理一小时前的请求记录
            self.request_times = [t for t in self.request_times if current_time - t < 3600]
            
            if len(self.request_times) < self.max_requests_per_hour:
                return 0
            
            # 计算需要等待到最早请求过期的时间
            oldest_request = min(self.request_times)
            wait_time = 3600 - (current_time - oldest_request) + 1  # 多等1秒确保安全
            return max(0, wait_time)
    
    def get_current_count(self):
        """获取当前小时内的请求数量"""
        with self.lock:
            current_time = time.time()
            self.request_times = [t for t in self.request_times if current_time - t < 3600]
            return len(self.request_times)

# 全局请求限制器实例
request_limiter = RequestLimiter(max_requests_per_hour=300)

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

def process_single_stock(stock, logger, config, start_date, end_date):
    """处理单个股票的数据获取和保存"""
    try:
        # 检查请求限制
        if not request_limiter.can_make_request():
            wait_time = request_limiter.get_wait_time()
            logger.warning_print(f"股票 {stock} 达到每小时请求限制，需等待 {wait_time:.0f} 秒")
            time.sleep(wait_time)
        
        # 记录请求
        request_limiter.record_request()
        
        # 建立数据库连接（每个线程独立的连接）
        connection = db_con_pymysql(config)
        
        try:
            # 获取股票数据，添加重试机制
            retries = 3
            timeout = 30  # 设置超时时间为30秒
            
            while retries > 0:
                try:
                    # 添加随机延时，避免请求过于频繁
                    time.sleep(random.uniform(1.0, 3.0))  # 增加延时范围
                    
                    stock_data = ak.stock_zh_a_hist(
                        symbol=stock,
                        period="daily",
                        start_date=start_date,
                        end_date=end_date,
                        adjust="qfq",
                        timeout=timeout
                    )
                    
                    # 请求成功，检查数据是否为空
                    if stock_data is None or stock_data.empty:
                        logger.warning_print(f"股票 {stock} 在指定时间范围内无数据")
                        return "no_data"  # 直接返回无数据状态，不进行重试
                    
                    # 数据获取成功且不为空，跳出重试循环
                    break
                        
                except (SSLError, requests.exceptions.Timeout, 
                        requests.exceptions.ConnectionError) as e:
                    retries -= 1
                    if retries > 0:
                        wait_time = (3 - retries) * 5  # 递增等待时间：5, 10秒
                        logger.warning_print(f"股票 {stock} 连接错误: {str(e)[:100]}，将在{wait_time}秒后进行第 {3-retries} 次重试")
                        time.sleep(wait_time)
                    else:
                        logger.error_print(f"股票 {stock} 在重试3次后仍然失败: {str(e)[:100]}")
                        return "api_fail"
                    continue
            
            # 准备数据
            data_to_insert = [
                (
                    row["日期"], stock, row["开盘"], row["收盘"], row["最高"],
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
                    logger.info_print(f"✓ 股票 {stock} 成功获取并入库 {len(data_to_insert)} 条记录")
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
    logger = set_log(config, "SubQA002_MulTh.log", "QA")  # 设置日志记录器
    logger.info_print("📋 限制条件：每小时最多300支股票，最大8个并发线程")

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
            
            # 计算预估完成时间
            estimated_hours = (total_stocks / 300) + 1  # 加1小时缓冲
            logger.info_print(f"预估完成时间: {estimated_hours:.1f} 小时")
            
            # 初始化计数器
            api_success = 0
            db_success = 0
            no_data_stocks = []
            failed_stocks = []
            completed = 0
            start_time = time.time()
            
            # 使用较小的线程池处理所有数据
            max_workers = 8  # 线程数
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交所有任务
                futures = {
                    executor.submit(
                        process_single_stock, 
                        stock, 
                        logger,  # 传递logger给process_single_stock
                        config, 
                        start_date, 
                        end_date
                    ): stock for stock in stock_codes
                }
                
                # 获取任务结果
                for future in as_completed(futures):
                    stock = futures[future]
                    completed += 1
                    current_hour_requests = request_limiter.get_current_count()
                    
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
                        # 计算速度和剩余时间
                        elapsed_time = time.time() - start_time
                        if elapsed_time > 0:
                            speed = completed / elapsed_time * 3600  # 每小时处理的股票数
                            remaining = total_stocks - completed
                            eta = remaining / max(speed, 1) if speed > 0 else 0
                        else:
                            speed = 0
                            eta = 0
                        
                        # 无论成功失败都更新进度
                        print(f"\r进度: {completed}/{total_stocks} | "
                              f"成功率: {api_success/completed:.1%} | "
                              f"本小时请求: {current_hour_requests}/300 | "
                              f"速度: {speed:.0f}股/时 | "
                              f"预计剩余: {eta:.1f}小时", 
                              end="", flush=True)
                        
                        # 每处理100只股票输出一次详细信息
                        if completed % 100 == 0:
                            print()  # 换行
                            logger.info_print(f"📊 已处理 {completed}/{total_stocks} 只股票")
                            logger.info_print(f"📈 当前成功率: {api_success/completed:.1%}")
                            logger.info_print(f"🕐 本小时已请求: {current_hour_requests}/300")
                            logger.info_print(f"⚡ 处理速度: {speed:.0f} 股票/小时")
            
            print()  # 换行
            
            # 计算最终统计
            total_time = time.time() - start_time
            final_speed = completed / total_time * 3600 if total_time > 0 else 0
            
            # 打印最终结果
            logger.info_print("="*60)
            logger.info_print("🎉 所有数据处理完成!")
            logger.info_print(f"📊 最终统计:")
            logger.info_print(f"  - 总处理股票: {completed}")
            logger.info_print(f"  - 成功获取: {api_success} ({api_success/completed:.1%})")
            logger.info_print(f"  - 数据库成功: {db_success} ({db_success/completed:.1%})")
            logger.info_print(f"  - 无数据股票: {len(no_data_stocks)}")
            logger.info_print(f"  - 失败股票: {len(failed_stocks)}")
            logger.info_print(f"⏱️  总耗时: {total_time/3600:.2f} 小时")
            logger.info_print(f"⚡ 平均速度: {final_speed:.0f} 股票/小时")
            logger.info_print("="*60)
            
            if no_data_stocks:
                logger.warning_print(f"⚠️  无数据股票 ({len(no_data_stocks)} 只): {', '.join(no_data_stocks[:10])}{'...' if len(no_data_stocks) > 10 else ''}")
            if failed_stocks:
                logger.warning_print(f"❌ 失败股票 ({len(failed_stocks)} 只): {', '.join(failed_stocks[:10])}{'...' if len(failed_stocks) > 10 else ''}")
            
            return True

    except Exception as e:
        logger.error_print(f"❌ 程序执行出错: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        result = main()
        if result:
            print("✅ 程序正常结束")
            sys.exit(0)
        else:
            print("❌ 程序未正常完成")
            sys.exit(1)
    except Exception as e:
        print(f"💥 程序异常终止: {str(e)}")
        sys.exit(1)