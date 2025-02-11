import argparse
import pandas as pd
import random
import os
from Week_K_v2 import ResistanceLineAnalyzer, DataLoader
from CommonFunc.DBconnection import find_config_path, load_config, set_log, db_con_pymysql
from QA.Programs.QA002 import is_today_workday, last_workday
from QA.SubFunc.SubQA001 import save_filter_result
import time
from datetime import datetime
import concurrent.futures
from functools import partial

def process_single_stock(stock_id, data_loader, analyzer, threshold=2.8):
    """处理单个股票"""
    try:
        df = data_loader.get_stock_weekly_data(stock_id)
        latest_data = df.iloc[-1]
        weekly_change = ((latest_data['close'] - latest_data['open']) / latest_data['open']) * 100
        is_qualified = weekly_change >= threshold
        
        # 只有涨幅合格的股票才进行阻力线分析
        if is_qualified:
            results = analyzer.analyze(df, stock_id)
        else:
            results = None
            
        return df, results, is_qualified, weekly_change
        
    except Exception as e:
        print(f"处理股票 {stock_id} 时出错: {str(e)}")
        return None, None, False, 0

def process_single_stock_mp(stock_id, threshold=2.8):
    """为多进程设计的处理函数"""
    data_loader = None
    try:
        # 在每个进程中创建自己的 DataLoader 和 analyzer
        data_loader = DataLoader()
        analyzer = ResistanceLineAnalyzer()
        
        df = data_loader.get_stock_weekly_data(stock_id)
        latest_data = df.iloc[-1]
        weekly_change = ((latest_data['close'] - latest_data['open']) / latest_data['open']) * 100
        is_qualified = weekly_change >= threshold
        
        # 只有涨幅合格的股票才进行阻力线分析
        if is_qualified:
            results = analyzer.analyze(df, stock_id)
        else:
            results = None
            
        return stock_id, df, results, is_qualified, weekly_change
        
    except Exception as e:
        print(f"处理股票 {stock_id} 时出错: {str(e)}")
        return stock_id, None, None, False, 0
    
    finally:
        # 确保数据库连接被关闭
        if data_loader and hasattr(data_loader, 'engine'):
            data_loader.engine.dispose()

def process_single_mode(stock_id, threshold, debug=False):
    """处理单只股票模式"""
    data_loader = DataLoader()
    analyzer = ResistanceLineAnalyzer()
    
    df, results, is_qualified, weekly_change = process_single_stock(
        stock_id, data_loader, analyzer, threshold)
    
    # 打印详细信息
    print(f"\n股票代码: {stock_id}")
    print(f"涨幅检查: {'合格' if is_qualified else '不合格'} ({weekly_change:.2f}% {'≥' if is_qualified else '<'} {threshold}%)")
    if is_qualified and results:
        print(f"阻力线检查: 发现 {len(results['connections'])} 条符合条件的连线")
    
    if df is not None and is_qualified and results is not None:
        analyzer.plot(df, stock_id, results, debug=debug, batch_mode=False)

def process_batch_mode(stock_list, threshold, debug=False):
    """处理批量股票模式"""
    total_stocks = len(stock_list)
    stocks_with_lines = []  # 只保留最终有效的股票列表
    
    # 多进程处理
    process_start = time.time()
    max_workers = min(max(1, os.cpu_count() - 1), 8)
    
    # 将股票列表分成更大的批次
    batch_size = max(20, total_stocks // max_workers)
    stock_batches = [stock_list[i:i + batch_size] for i in range(0, len(stock_list), batch_size)]
    
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_single_stock_mp, stock_id, threshold) 
                  for stock_id in stock_batches[0]]
        
        # 处理结果
        completed_stocks = 0
        for future in concurrent.futures.as_completed(futures):
            completed_stocks += 1
            print(f"\r处理进度: {completed_stocks}/{total_stocks} "
                  f"({completed_stocks/total_stocks*100:.1f}%)", end="")
            
            stock_id, df, results, _, _ = future.result()
            if df is not None and results and len(results['connections']) > 0:
                stocks_with_lines.append(stock_id)
    
    process_time = time.time() - process_start
    logger.info_print(f"\n处理完成, 耗时: {process_time:.2f} 秒")
    
    # 只显示最终结果的统计
    logger.info_print(f"发现 {len(stocks_with_lines)} 只股票存在周线突破形态 "
          f"({len(stocks_with_lines)/total_stocks*100:.1f}%)")
    
    if stocks_with_lines:
        logger.info_print("周线突破的股票:\n " + ", ".join(stocks_with_lines))
    
    return stocks_with_lines, process_time

def plot_sample_stocks(qualified_stocks, threshold, debug=False):
    """绘制样本股票图表"""
    plot_start = time.time()
    
    if qualified_stocks:
        stocks_to_plot = random.sample(qualified_stocks, min(3, len(qualified_stocks)))
        
        for stock_id in stocks_to_plot:
            stock_id, df, results, _, _ = process_single_stock_mp(stock_id, threshold)
            if results:
                analyzer = ResistanceLineAnalyzer()
                analyzer.plot(df, stock_id, results, debug=debug, batch_mode=True)
    
    return time.time() - plot_start

def fetch_stock_codes(cursor, filter_results_table, processing_date, debug=False):
    """从数据库获取需要处理的股票代码"""
    try:
        # 将处理日期转换为字符串格式
        date_str = processing_date.strftime('%Y-%m-%d') if isinstance(processing_date, datetime) else processing_date
        
        # 使用完整的表名（包含数据库名）
        query = f"""
        SELECT ID 
        FROM StkFilterQA.{filter_results_table}
        WHERE FilterDate = %s AND FilteredBy = 0
        ORDER BY ID
        """
        
        # 只在debug模式下打印调试信息
        if debug:
            print(f"Debug - Query: {query}")
            print(f"Debug - Date parameter: {date_str}")
        
        cursor.execute(query, [date_str])
        results = cursor.fetchall()
        
        if debug:
            print(f"Debug - Found {len(results)} records")
        
        if not results:
            if debug:
                print("Debug - No records found")
            return []
            
        stock_codes = [str(row['ID']).zfill(6) for row in results]
        if debug:
            print(f"Debug - First few stock codes: {stock_codes[:5]}")
        return stock_codes
        
    except Exception as e:
        error_msg = f"获取股票代码时发生错误: {str(e)}"
        if debug:
            error_msg += f"\nSQL: {query}\nDate: {date_str}"
        raise Exception(error_msg)

def update_filter_results(cursor, filter_results_table, processing_date, filtered_out_stocks, logger, debug=False):
    """更新FilterResults表中的F_WK列"""
    try:
        if filtered_out_stocks:
            update_query = f"""
            UPDATE {filter_results_table}
            SET F_WK = 1
            WHERE FilterDate = %s AND ID IN ({','.join(['%s'] * len(filtered_out_stocks))})
            """
            cursor.execute(update_query, [processing_date] + filtered_out_stocks)
            if debug:
                logger.debug(f"更新查询: {update_query}")
                logger.debug(f"更新参数: {[processing_date] + filtered_out_stocks}")
            logger.info_print(f"已更新 {cursor.rowcount} 条FilterResults记录")
        else:
            logger.info_print("没有需要更新的记录")
        return True
    except Exception as e:
        logger.error_print(f"更新FilterResults表时出错: {str(e)}")
        return False

def main():
    start_time = time.time()
    threshold = 2.8
    
    # 命令行参数解析
    parser = argparse.ArgumentParser(description='股票周线突破工具')
    parser.add_argument('--stock', type=str, help='单个股票代码，例如：000001')
    parser.add_argument('--debug', action='store_true', help='开启调试模式，显示详细信息')
    args = parser.parse_args()
    
    try:
        # 获取配置文件路径并加载配置
        config_path_QA, _, root_dir = find_config_path()
        config = load_config(config_path_QA)
        
        # 设置日志
        global logger
        logger = set_log(config, "Filter4.log", prefix="QA")
        logger.info_print("开始执行周线突破分析...")
        
        # 确定处理日期
        is_today, processing_date = is_today_workday(logger)
        
        # 获取数据库表名
        filter_results_table = config['DB_tables']['filter_results']
        
        if args.stock:
            # 单股处理模式
            stock_id = f"{int(args.stock):06d}"
            logger.info_print(f"单股处理模式: {stock_id}")
            process_single_mode(stock_id, threshold, args.debug)
        else:
            # 批量处理模式
            connection = db_con_pymysql(config)
            cursor = connection.cursor()
            
            try:
                # 从数据库获取股票代码
                stock_list = fetch_stock_codes(cursor, filter_results_table, processing_date, args.debug)
                total_stocks = len(stock_list)
                
                if total_stocks == 0:
                    logger.warning_print("没有找到需要处理的股票代码")
                    return
                
                logger.info_print(f"批量处理模式: 共 {total_stocks} 只股票")
                
                # 处理所有股票
                stocks_with_lines, process_time = process_batch_mode(
                    stock_list, threshold, args.debug)
                
                # 计算被过滤掉的股票
                filtered_out_stocks = list(set(stock_list) - set(stocks_with_lines))
                
                # 更新FilterResults表
                if not update_filter_results(cursor, filter_results_table, processing_date, 
                                          filtered_out_stocks, logger, args.debug):
                    logger.error_print("更新过滤结果失败")
                    return
                
                # 保存过滤结果到数据库
                details = "分析股票的阻力线形态，识别具有潜在突破机会的股票"
                save_filter_result(
                    cursor, 
                    config, 
                    "Filter4", 
                    total_stocks, 
                    len(stocks_with_lines), 
                    logger,
                    details,
                    "FilterResults",  # 使用数据表名作为来源
                    "FilterResults"   # 使用数据表名作为输出
                )
                
                connection.commit()
                
                # 打印过滤前后的数量对比
                logger.info_print(f"过滤前股票数量: {total_stocks}")
                logger.info_print(f"过滤后股票数量: {len(stocks_with_lines)}")
                
                # 绘制样本图表
                plot_time = plot_sample_stocks(stocks_with_lines, threshold, args.debug)
                if args.debug:
                    logger.debug(f"图形绘制耗时: {plot_time:.2f} 秒")
                
            finally:
                connection.close()
            
            # 记录执行完成
            logger.info_print("周线突破分析执行完成")
                
    except Exception as e:
        import traceback
        logger.error_print(f"程序执行失败: {str(e)}")
        logger.error_print("详细错误信息:")
        logger.error_print(traceback.format_exc())

if __name__ == "__main__":
    main()
