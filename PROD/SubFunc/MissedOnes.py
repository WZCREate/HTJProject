import pandas as pd
import akshare as ak
import sys
from datetime import datetime
import os
import time
from requests.exceptions import SSLError
from concurrent.futures import ThreadPoolExecutor, as_completed
from CommonFunc.DBconnection import (
    load_config, 
    db_con_pymysql,
    set_log,
    find_config_path
)

def fetch_stock_codes(csv_file, root_dir, logger):
    """从CSV文件中读取股票代码"""
    prod_dir = os.path.join(root_dir, "PROD")
    full_path = os.path.join(prod_dir, csv_file)
    
    if not os.path.exists(full_path):
        logger.error_print(f"PROD: 文件不存在: {full_path}")
        raise FileNotFoundError(f"文件不存在: {full_path}")
    
    # 读取CSV文件，确保股票代码作为字符串处理
    df = pd.read_csv(full_path, header=None, dtype={0: str})
    # 去掉多余的空格
    df[0] = df[0].str.strip()
    # 确保所有股票代码都是6位数
    df[0] = df[0].str.zfill(6)
    # 去掉空值并去重
    stock_codes = df[0].dropna().unique().tolist()
    logger.info_print(f"PROD: 成功读取到 {len(stock_codes)} 支股票代码。")
    return stock_codes

def process_single_stock(stock, logger, config, start_date, end_date):
    """处理单个股票的数据获取和保存"""
    try:
        # 建立数据库连接（每个线程独立的连接）
        connection = db_con_pymysql(config)
        
        try:
            # 首先尝试获取数据，不进入重试循环
            try:
                stock_data = ak.stock_zh_a_hist(
                    symbol=stock,
                    period="daily",
                    start_date=start_date,
                    end_date=end_date,
                    adjust="qfq"
                )
                
                # 检查是否为空数据（可能是退市股票）
                if stock_data is None or stock_data.empty:
                    logger.warning_print(f"PROD: 股票 {stock} 在指定时间范围内无数据")
                    return "no_data"
                    
            except SSLError:
                # 只有SSL错误（可能是请求频繁）才进入重试循环
                retries = 3
                while retries > 0:
                    logger.warning_print(f"PROD: 股票 {stock} 请求失败（SSL错误），将在3秒后进行第 {3-retries+1} 次重试")
                    time.sleep(3)
                    try:
                        stock_data = ak.stock_zh_a_hist(
                            symbol=stock,
                            period="daily",
                            start_date=start_date,
                            end_date=end_date,
                            adjust="qfq"
                        )
                        if stock_data is not None and not stock_data.empty:
                            break
                        retries -= 1
                    except SSLError:
                        retries -= 1
                        continue
                
                if retries == 0:
                    logger.error_print(f"PROD: 股票 {stock} 在重试后仍然失败")
                    return "api_fail"
            
            # 处理数据
            stock_data.rename(columns={
                "日期": "date",
                "开盘": "open_price",
                "收盘": "close_price",
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
                "成交额": "turnover",
                "振幅": "amplitude",
                "涨跌幅": "chg_percen",
                "涨跌额": "chg_amount",
                "换手率": "turnover_rate"
            }, inplace=True)
            
            # 添加额外字段
            stock_data["id"] = stock
            stock_data["Insrt_time"] = datetime.now()
            stock_data["Latest"] = 1
            
            try:
                with connection.cursor() as cursor:
                    # 插入数据
                    for _, row in stock_data.iterrows():
                        insert_query = f"""
                        INSERT INTO {config["DB_tables"]["buffer_table"]}
                        (date, id, open_price, close_price, high, low, volume, turnover, 
                        amplitude, chg_percen, chg_amount, turnover_rate, Insrt_time, Latest)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """
                        values = (
                            row["date"], row["id"], row["open_price"], row["close_price"], 
                            row["high"], row["low"], row["volume"], row["turnover"], 
                            row["amplitude"], row["chg_percen"], row["chg_amount"], 
                            row["turnover_rate"], row["Insrt_time"], row["Latest"]
                        )
                        cursor.execute(insert_query, values)
                    connection.commit()
                    return "success"
                    
            except Exception as e:
                connection.rollback()
                logger.error_print(f"PROD: 股票 {stock} 数据库写入失败: {str(e)}")
                return "db_fail"
                
        except Exception as e:
            logger.error_print(f"PROD: 股票 {stock} 数据获取失败: {str(e)}")
            return "api_fail"
            
        finally:
            connection.close()
            
    except Exception as e:
        logger.error_print(f"PROD: 股票 {stock} 处理过程出现错误: {str(e)}")
        return "error"

def main():
    """主函数"""
    try:
        # 获取配置文件路径并加载配置
        _, config_path_PROD, root_dir = find_config_path()
        config = load_config(config_path_PROD)
        
        # 设置日志
        logger = set_log(config, "MissedOnes.log", "PROD")
        logger.info_print("PROD: 开始执行数据导入程序...")
        
        try:
            # 获取股票列表
            stock_codes = fetch_stock_codes(config['CSVs']['MissedOnes'], root_dir, logger)
            total_stocks = len(stock_codes)
            
            start_date = config['ProgormInput']['missed_ones_start_date']
            end_date = config['ProgormInput']['missed_ones_end_date']
            
            logger.info_print(f"PROD: 数据获取时间范围: {start_date} 至 {end_date}")
            
            # 初始化计数器
            api_success = 0
            db_success = 0
            no_data_stocks = []
            failed_stocks = []
            completed = 0
            
            # 使用线程池处理所有数据
            with ThreadPoolExecutor(max_workers=5) as executor:
                # 提交所有任务
                futures = {
                    executor.submit(
                        process_single_stock, 
                        stock, 
                        logger,
                        config, 
                        start_date, 
                        end_date
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
                            logger.warning_print(f"PROD: 股票 {stock} 在指定时间范围内无数据")
                        elif result == "api_fail":
                            failed_stocks.append(stock)
                            logger.error_print(f"PROD: 股票 {stock} API请求失败")
                        elif result == "db_fail":
                            api_success += 1
                            failed_stocks.append(stock)
                            logger.error_print(f"PROD: 股票 {stock} 数据库写入失败")
                        else:
                            failed_stocks.append(stock)
                            logger.error_print(f"PROD: 股票 {stock} 处理失败，未知原因")
                            
                    except Exception as e:
                        logger.error_print(f"PROD: 股票 {stock} 执行出现异常: {str(e)}")
                        failed_stocks.append(stock)
                    
                    finally:
                        # 无论成功失败都更新进度
                        print(f"\r进度: {completed}/{total_stocks} | "
                              f"数据获取成功率: {api_success/total_stocks:.1%} | "
                              f"数据写入成功率: {db_success/total_stocks:.1%}", 
                              end="", flush=True)
            
            print()  # 换行
            
            # 打印最终结果
            if no_data_stocks:
                logger.warning_print(f"PROD: 无数据股票: {', '.join(no_data_stocks)}")
            if failed_stocks:
                logger.warning_print(f"PROD: 处理失败的股票: {', '.join(failed_stocks)}")
            logger.info_print(f"PROD: 所有数据处理完成 | "
                          f"总体数据获取成功率: {api_success/total_stocks:.1%} | "
                          f"数据写入成功率: {db_success/total_stocks:.1%} | "
                          f"无数据股票数量: {len(no_data_stocks)}")
            
            return True

        except Exception as e:
            logger.error_print(f"PROD: 程序执行出错: {str(e)}")
            raise

    except Exception as e:
        print(f"PROD: 程序执行出错: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        result = main()
        if result:
            print("PROD: 程序正常结束")
            sys.exit(0)
        else:
            print("PROD: 程序未正常完成")
            sys.exit(1)
    except Exception as e:
        print(f"PROD: 程序异常终止: {str(e)}")
        sys.exit(1)