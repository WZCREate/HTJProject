"""
本程序对MainCSV中所有骨牌哦代码进行周K数据请求
开始日期: 20230101
结束日期: 程序运行的日期
使用多线程 (需断开VPN)
优先使用本程序 than Ini_WK.py
"""

import akshare as ak
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from CommonFunc.DBconnection import find_config_path, load_config, db_con_pymysql, set_log
import os
import time
from requests.exceptions import SSLError
import sys

def convert_date_to_week(date_input):
    """
    将日期转换为周数格式 (YYWww)
    参数:
        date_input: 日期输入 (可以是字符串或datetime对象)
    返回:
        周数格式字符串 (例如: '23W01')
    """
    # 如果输入是字符串，转换为datetime对象
    if isinstance(date_input, str):
        if len(date_input) == 8:  # 如果是 '20230101' 格式
            date_input = f"{date_input[:4]}-{date_input[4:6]}-{date_input[6:]}"
        date_obj = datetime.strptime(date_input, "%Y-%m-%d")
    else:
        # 如果已经是datetime对象或pandas Timestamp，直接使用
        date_obj = date_input

    # 获取ISO年份和周数
    iso_year, week_num, _ = date_obj.isocalendar()
    
    # 格式化输出为"XXWYY"的格式
    return f"{str(iso_year)[2:]}W{week_num:02d}"

def process_single_stock(stock, logger, config):
    """处理单个股票的数据获取和保存"""
    try:
        # 建立数据库连接（每个线程独立的连接）
        conn = db_con_pymysql(config)
        cursor = conn.cursor()
        
        # 获取当前日期
        current_date = datetime.now().strftime("%Y%m%d")
        update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        try:
            # 首先尝试获取数据，不进入重试循环
            try:
                df = ak.stock_zh_a_hist(
                    symbol=stock,
                    period="weekly",
                    start_date="20230101",
                    end_date=current_date,
                    adjust="qfq"
                )
                
                # 检查是否为空数据
                if df is None or df.empty:
                    logger.warning_print(f"股票 {stock} 在指定时间范围内无数据")
                    return "no_data"
                    
            except SSLError as e:
                # 只有SSL错误（可能是请求频繁）才进入重试循环
                retries = 3
                while retries > 0:
                    logger.warning_print(f"股票 {stock} 请求失败（SSL错误），将在3秒后进行第 {3-retries+1} 次重试")
                    time.sleep(3)
                    try:
                        df = ak.stock_zh_a_hist(
                            symbol=stock,
                            period="weekly",
                            start_date="20230101",
                            end_date=current_date,
                            adjust="qfq"
                        )
                        if df is not None and not df.empty:
                            break
                        retries -= 1
                    except SSLError:
                        retries -= 1
                        if retries == 0:
                            logger.error_print(f"股票 {stock} 的周K数据获取失败，已重试3次")
                            return "api_fail"
            
            # 转换日期为周格式 (替换原有的转换逻辑)
            df['周数'] = df['日期'].apply(convert_date_to_week)
            
            try:
                # 插入或更新数据
                for _, row in df.iterrows():
                    sql = """
                    INSERT INTO WK (id, wkn, WK_date, open, close, high, low, chg_percen, update_time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                    WK_date = VALUES(WK_date),
                    open = VALUES(open),
                    close = VALUES(close),
                    high = VALUES(high),
                    low = VALUES(low),
                    chg_percen = VALUES(chg_percen),
                    update_time = VALUES(update_time)
                    """
                    values = (
                        stock,
                        row['周数'],
                        row['日期'],
                        row['开盘'],
                        row['收盘'],
                        row['最高'],
                        row['最低'],
                        row['涨跌幅'],
                        update_time
                    )
                    cursor.execute(sql, values)
                
                conn.commit()
                return "success"
                
            except Exception as e:
                conn.rollback()
                logger.error_print(f"股票 {stock} 数据库写入失败: {str(e)}")
                return "db_fail"
                
        except Exception as e:
            logger.error_print(f"股票 {stock} 的周K数据获取失败: {str(e)}")
            return "api_fail"
            
        finally:
            cursor.close()
            conn.close()
            
    except Exception as e:
        logger.error_print(f"股票 {stock} 处理过程出现错误: {str(e)}")
        return "error"

def get_stock_list(config, root_dir):
    """从CSV文件获取股票代码列表"""
    try:
        # 构建CSV文件的完整路径
        csv_path = os.path.join(root_dir, "QA", config['CSVs']['MainCSV'])
        
        # 读取CSV文件的第二列
        df = pd.read_csv(csv_path)
        stock_list = df.iloc[:, 1].astype(str).tolist()
        
        # 确保股票代码格式正确（6位数字）
        stock_list = [code.zfill(6) for code in stock_list if code.isdigit()]
        
        return stock_list
    except Exception as e:
        raise Exception(f"读取股票列表失败: {str(e)}")

def process_batch(stock_batch, logger, config, batch_num, total_batches):
    """处理单个批次的股票"""
    batch_size = len(stock_batch)  # 使用实际的批次大小
    logger.info_print(f"开始处理第 {batch_num}/{total_batches} 批次，包含 {batch_size} 只股票")
    
    # 初始化批次计数器
    api_success = 0
    db_success = 0
    failed_stocks = []
    st_stocks = []
    completed = 0
    
    # 使用线程池处理当前批次
    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = {
            executor.submit(process_single_stock, stock, logger, config): stock 
            for stock in stock_batch
        }
        
        for future in as_completed(futures):
            stock = futures[future]
            completed += 1
            try:
                result = future.result()
                if result == "success":
                    api_success += 1
                    db_success += 1
                elif result == "st_stock":
                    st_stocks.append(stock)
                elif result == "api_fail":
                    failed_stocks.append(stock)
                elif result == "db_fail":
                    api_success += 1
                    failed_stocks.append(stock)
                else:
                    failed_stocks.append(stock)
                    
            except Exception as e:
                logger.error_print(f"股票 {stock} 执行出现异常: {str(e)}")
                failed_stocks.append(stock)
            
            finally:
                # 显示当前批次的进度，使用实际的batch_size
                print(f"\r批次 {batch_num}/{total_batches} 进度: {completed}/{batch_size} | "
                      f"数据获取成功率: {api_success/batch_size:.1%} | "
                      f"数据写入成功率: {db_success/batch_size:.1%}", 
                      end="", flush=True)
    
    print()  # 换行
    
    # 记录当前批次的结果
    logger.info_print(f"第 {batch_num}/{total_batches} 批次处理完成 | "
                     f"总体数据获取成功率: {api_success/batch_size:.1%} | "
                     f"数据写入成功率: {db_success/batch_size:.1%} | ")
    
    if st_stocks:
        logger.warning_print(f"第 {batch_num} 批次退市股票: {', '.join(st_stocks)}")
    if failed_stocks:
        logger.warning_print(f"第 {batch_num} 批次失败股票: {', '.join(failed_stocks)}")
    
    return api_success, db_success, st_stocks, failed_stocks

def save_weekly_data(start_date=None):
    """
    获取并保存周K数据
    参数:
        start_date: 开始日期，格式为 'YYYYMMDD'，如果为None则默认使用'20230101'
    返回:
        success: bool, 程序是否成功执行
        message: str, 执行结果信息
    """
    # 设置默认开始日期
    if start_date is None:
        start_date = '20230101'
    
    # 获取配置文件路径和加载配置
    config_path_QA, config_path_PROD, root_dir = find_config_path()
    config = load_config(config_path_QA)  # 使用QA环境配置
    
    # 设置日志
    logger = set_log(config, "Ini_WK_MuTh.log", "QA")
    logger.info_print(f"开始获取并保存周K数据，开始日期: {start_date}")
    
    try:
        # 获取股票列表
        stock_list = get_stock_list(config, root_dir)
        total_stocks = len(stock_list)
        
        # 设置批次大小和计算批次数
        batch_size = 6000  # 每批处理100只股票
        total_batches = (total_stocks + batch_size - 1) // batch_size
        
        logger.info_print(f"成功读取股票列表，共 {total_stocks} 只股票，将分 {total_batches} 批处理")
        
        # 初始化总计数器
        total_api_success = 0
        total_db_success = 0
        all_st_stocks = []
        all_failed_stocks = []
        
        # 按批次处理，确保每个批次的大小正确
        for batch_num in range(1, total_batches + 1):
            start_idx = (batch_num - 1) * batch_size
            end_idx = min(batch_num * batch_size, total_stocks)
            current_batch = stock_list[start_idx:end_idx]
            
            # 处理当前批次
            api_success, db_success, st_stocks, failed_stocks = process_batch(
                current_batch, logger, config, batch_num, total_batches
            )
            
            # 更新总计数
            total_api_success += api_success
            total_db_success += db_success
            all_st_stocks.extend(st_stocks)
            all_failed_stocks.extend(failed_stocks)
            
            # 每个批次完成后强制刷新输出
            sys.stdout.flush()
        
        # 打印最终汇总结果
        logger.info_print("\n=== 全部批次处理完成，最终统计 ===")
        logger.info_print(f"总体数据获取成功率: {total_api_success/total_stocks:.1%} | "
                         f"数据写入成功率: {total_db_success/total_stocks:.1%}")
        
        if all_failed_stocks:
            logger.warning_print(f"请求失败股票: {', '.join(all_failed_stocks)}")
        
        return True, "数据获取和保存成功完成"
        
    except Exception as e:
        error_msg = f"程序执行出现错误: {str(e)}"
        logger.error_print(error_msg)
        return False, error_msg

if __name__ == "__main__":
    try:
        success, message = save_weekly_data()  # 独立运行时使用默认参数
        if success:
            print("程序正常结束")
            sys.exit(0)
        else:
            print(f"程序执行失败: {message}")
            sys.exit(1)
    except Exception as e:
        print(f"程序异常终止: {str(e)}")
        sys.exit(1)