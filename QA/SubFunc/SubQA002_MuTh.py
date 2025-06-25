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
            logger.info_print(f"清空表 {table_name} 成功")
        return True
        
    except Exception as e:
        logger.error_print(f"清空表 {table_name} 失败: {e}")
        return False

def process_single_stock(stock, logger, config, start_date, end_date):
    """处理单个股票的数据获取和保存"""
    try:
        # 检查请求限制
        if not request_limiter.can_make_request():
            wait_time = request_limiter.get_wait_time()
            logger.warning_print(f"达到请求限制，等待 {wait_time:.0f}s - {stock}")
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
                        return "no_data"  # 直接返回无数据状态，不进行重试
                    
                    # 数据获取成功且不为空，跳出重试循环
                    break
                        
                except (SSLError, requests.exceptions.Timeout, 
                        requests.exceptions.ConnectionError) as e:
                    retries -= 1
                    if retries > 0:
                        wait_time = (3 - retries) * 5  # 递增等待时间：5, 10秒
                        time.sleep(wait_time)
                    else:
                        logger.error_print(f"重试失败 - {stock}: {str(e)[:50]}...")
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
                    return "success"
                    
            except Exception as e:
                connection.rollback()
                logger.error_print(f"数据库写入失败 - {stock}: {str(e)[:50]}...")
                return "db_fail"
                
        except Exception as e:
            logger.error_print(f"数据获取失败 - {stock}: {str(e)[:50]}...")
            return "api_fail"
            
        finally:
            connection.close()
            
    except Exception as e:
        logger.error_print(f"处理异常 - {stock}: {str(e)[:50]}...")
        return "error"

def main():
    """主函数"""
    config_path, _, root_dir = find_config_path()
    config = load_config(config_path)
    logger = set_log(config, "SubQA002_MulTh.log", "QA")  # 设置日志记录器

    # 程序启动信息
    logger.info_print("=" * 50)
    logger.info_print("启动股票数据批量导入程序")
    logger.info_print("限制: 300股票/小时, 8线程")
    logger.info_print("=" * 50)

    try:
        connection = db_con_pymysql(config)
        if not check_and_clear_table(connection, config["DB_tables"]["buffer_table"], logger):
            return False
        connection.close()
        
        # 读取股票列表
        csv_path = os.path.join(root_dir, "QA", config["CSVs"]["MainCSV"])
        stock_list_df = pd.read_csv(csv_path, dtype={1: str})
        stock_codes = stock_list_df.iloc[:, 1].tolist()
        
        start_date = config["ProgormInput"]["massive_insrt_start_date"]
        end_date = config["ProgormInput"]["massive_insrt_end_date"]
        
        total_stocks = len(stock_codes)
        estimated_hours = (total_stocks / 300) + 1
        
        logger.info_print(f"股票总数: {total_stocks}")
        logger.info_print(f"时间范围: {start_date} - {end_date}")
        logger.info_print(f"预估耗时: {estimated_hours:.1f}小时")
        
        # 初始化计数器
        api_success = 0
        db_success = 0
        no_data_count = 0
        failed_count = 0
        completed = 0
        start_time = time.time()
        
        # 批次处理设置
        batch_size = 50  # 每批50只股票
        batches = [stock_codes[i:i+batch_size] for i in range(0, len(stock_codes), batch_size)]
        total_batches = len(batches)
        
        logger.info_print(f"分批处理: {total_batches}批, 每批{batch_size}只")
        logger.info_print("开始处理...")
        
        # 使用较小的线程池处理所有数据
        max_workers = 8
        
        for batch_idx, batch in enumerate(batches, 1):
            batch_start_time = time.time()
            batch_success = 0
            batch_failed = 0
            batch_no_data = 0
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # 提交当前批次的任务
                futures = {
                    executor.submit(
                        process_single_stock, 
                        stock, 
                        logger,
                        config, 
                        start_date, 
                        end_date
                    ): stock for stock in batch
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
                            batch_success += 1
                            # 成功时只打印简洁信息
                            print(f"\r[{completed}/{total_stocks}] {stock} ✓", end="", flush=True)
                        elif result == "no_data":
                            no_data_count += 1
                            batch_no_data += 1
                        elif result == "api_fail":
                            failed_count += 1
                            batch_failed += 1
                        elif result == "db_fail":
                            api_success += 1
                            failed_count += 1
                            batch_failed += 1
                        else:
                            failed_count += 1
                            batch_failed += 1
                            
                    except Exception as e:
                        logger.error_print(f"执行异常 - {stock}: {str(e)[:50]}...")
                        failed_count += 1
                        batch_failed += 1
            
            # 批次完成统计
            batch_time = time.time() - batch_start_time
            batch_total = len(batch)
            batch_success_rate = batch_success / batch_total if batch_total > 0 else 0
            
            # 总体进度统计
            elapsed_time = time.time() - start_time
            overall_success_rate = api_success / completed if completed > 0 else 0
            speed = completed / elapsed_time * 3600 if elapsed_time > 0 else 0
            remaining = total_stocks - completed
            eta = remaining / max(speed, 1) if speed > 0 else 0
            current_hour_requests = request_limiter.get_current_count()
            
            print()  # 换行
            logger.info_print(f"批次{batch_idx}/{total_batches}完成: 成功{batch_success}/{batch_total}({batch_success_rate:.1%}) 耗时{batch_time:.1f}s")
            logger.info_print(f"总进度: {completed}/{total_stocks}({completed/total_stocks:.1%}) 成功率{overall_success_rate:.1%} 速度{speed:.0f}股/时 剩余{eta:.1f}h 本时请求{current_hour_requests}/300")
            
            # 批次间休息
            if batch_idx < total_batches:
                rest_time = max(2, 300 / 50)  # 根据限制计算休息时间
                logger.info_print(f"批次间休息 {rest_time:.1f}s...")
                time.sleep(rest_time)
        
        # 最终统计
        total_time = time.time() - start_time
        final_speed = completed / total_time * 3600 if total_time > 0 else 0
        
        logger.info_print("=" * 50)
        logger.info_print("处理完成!")
        logger.info_print(f"总计: {completed}只股票")
        logger.info_print(f"成功: {api_success}只 ({api_success/completed:.1%})")
        logger.info_print(f"无数据: {no_data_count}只")
        logger.info_print(f"失败: {failed_count}只")
        logger.info_print(f"耗时: {total_time/3600:.2f}小时")
        logger.info_print(f"平均速度: {final_speed:.0f}股票/小时")
        logger.info_print("=" * 50)
        
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