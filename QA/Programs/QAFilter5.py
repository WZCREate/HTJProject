import argparse
import pandas as pd
import random
import os
from QA.Programs.Triangle_v2 import ResistanceLineAnalyzer, DataLoader
from CommonFunc.DBconnection import find_config_path, load_config, set_log, db_con_pymysql
from QA.Programs.QA002 import is_today_workday, last_workday
from QA.SubFunc.SubQA001 import save_filter_result
import time
from datetime import datetime
import concurrent.futures
from functools import partial
import cProfile
import pstats
from pstats import SortKey

def process_single_stock(stock_id, data_loader, analyzer, debug=False):
    """处理单个股票"""
    # 获取数据
    df = data_loader.get_stock_data(stock_id)
    if df is None or len(df) < 3:
        return None
        
    # 分析
    results = analyzer.analyze(df, stock_id)
    if results is None:
        return None
    
    # 检查上边界连线
    if not results['connections']:
        return None
        
    # 检查下边界连线
    if not results['low_connections']:
        return None
    
    # 检查上下边界的对称性
    if not check_triangle_symmetry(results):
        return None
    
    # 如果通过所有检查，绘制图表
    analyzer.plot(df, stock_id, results, debug=debug, batch_mode=True)
    
    return {
        'stock_id': stock_id,
        'up_connections': len(results['connections']),
        'low_connections': len(results['low_connections']),
        'results': results
    }

def check_triangle_symmetry(results):
    """
    检查三角形的对称性
    
    Args:
        results: 分析结果字典
    
    Returns:
        bool: 如果三角形足够对称返回True
    """
    if not results['connections'] or not results['low_connections']:
        return False
    
    # 获取上下边界的斜率
    up_slope = calculate_average_slope(results['connections'])
    low_slope = calculate_average_slope(results['low_connections'])
    
    # 检查斜率的对称性（绝对值应该接近）
    slope_ratio = abs(up_slope / low_slope) if low_slope != 0 else float('inf')
    if not 0.5 <= slope_ratio <= 2.0:  # 允许一定的不对称
        return False
    
    # TODO: 可以添加更多对称性检查
    # 例如：检查连线数量的平衡性
    # 检查穿越影线数量的平衡性
    # 检查左侧点的分布等
    
    return True

def calculate_average_slope(connections):
    """计算连线的平均斜率"""
    if not connections:
        return 0
    
    slopes = []
    for conn in connections:
        left = conn['left_point']
        right = conn['right_point']
        time_diff = (right[1] - left[1]).days
        if time_diff > 0:
            slope = (right[0] - left[0]) / time_diff
            slopes.append(slope)
    
    return sum(slopes) / len(slopes) if slopes else 0

def process_single_stock_mp(stock_id, threshold, filter_config):
    """为多进程设计的处理函数"""
    data_loader = None
    try:
        data_loader = DataLoader()
        analyzer = ResistanceLineAnalyzer()
        
        df = data_loader.get_stock_data(stock_id)
        
        # 数据量检查（至少3天）
        if df is None or len(df) < 3:
            return stock_id, None, None, False, False, 0, False
        
        # 涨幅检查
        latest_data = df.iloc[-1]
        daily_change = ((latest_data['close'] - latest_data['open']) / latest_data['open']) * 100
        is_qualified_rise = True
        
        if filter_config['rise_check']['enabled']:
            is_qualified_rise = daily_change < filter_config['rise_check']['threshold']
        
        # 最低价检查
        is_qualified_low = True  # 默认通过
        if filter_config['low_price_check']['enabled']:
            if len(df) >= 3:
                base_day = df.iloc[-1]
                base_day_minus_1 = df.iloc[-2]
                base_day_minus_2 = df.iloc[-3]
                is_qualified_low = not (base_day['low'] < base_day_minus_2['low'] or 
                                     base_day_minus_1['low'] < base_day_minus_2['low'])
        
        # Triangle分析
        results = None
        has_valid_lines = False
        if is_qualified_rise and is_qualified_low:
            results = analyzer.analyze(df, stock_id)
            has_valid_lines = bool(results and results['connections'] and results['low_connections'])
            
        return stock_id, df, results, is_qualified_rise, is_qualified_low, daily_change, has_valid_lines
        
    except Exception as e:
        print(f"处理股票 {stock_id} 时出错: {str(e)}")
        return stock_id, None, None, False, False, 0, False
    
    finally:
        if data_loader and hasattr(data_loader, 'engine'):
            data_loader.engine.dispose()

def process_single_mode(stock_id, threshold, config, debug=False):
    """处理单只股票模式"""
    data_loader = DataLoader()
    analyzer = ResistanceLineAnalyzer()
    
    # 使用传入的config
    stock_id, df, results, is_qualified_rise, is_qualified_low, daily_change, has_valid_lines = process_single_stock_mp(
        stock_id, threshold, config['Programs']['Filter5']['filters'])
    
    # 打印详细信息
    print(f"\n股票代码: {stock_id}")
    
    # 检查数据是否获取成功
    if df is None:
        print("数据获取失败或数据为空")
        return
        
    # 检查数据量是否足够
    if len(df) < 3:
        print(f"数据量不足: 当前数据量 {len(df)} 天，需要至少 3 天")
        return
    
    # 打印涨幅检查结果
    print(f"涨幅检查: {'合格' if is_qualified_rise else '不合格'} ({daily_change:.2f}% {'<' if is_qualified_rise else '≥'} {threshold}%)")
    
    # 如果涨幅不合格，直接返回
    if not is_qualified_rise:
        return
        
    # 打印最低价检查结果
    print(f"最低价检查: {'合格' if is_qualified_low else '不合格'}")
    if not is_qualified_low:
        base_day = df.iloc[-1]
        base_day_minus_1 = df.iloc[-2]
        base_day_minus_2 = df.iloc[-3]
        print(f"  最近三天最低价: {base_day['low']:.2f}, {base_day_minus_1['low']:.2f}, {base_day_minus_2['low']:.2f}")
        print(f"  不满足条件: 最近两天的最低价都应该高于第三天的最低价 ({base_day_minus_2['low']:.2f})")
        return
    
    # Triangle分析结果
    if not has_valid_lines:
        print("Triangle分析: 未找到符合条件的形态")
    else:
        print("Triangle分析: 发现完整的三角形形态")
        print(f"  上边界连线数量: {len(results['connections'])}")
        print(f"  下边界连线数量: {len(results['low_connections'])}")
        
        # 如果开启了调试模式，显示更多细节
        if debug:
            if results['connections']:
                print("\n上边界连线详情:")
                for i, conn in enumerate(results['connections'], 1):
                    left = conn['left_point']
                    right = conn['right_point']
                    print(f"连线 {i}:")
                    print(f"  左端点: 价格 {left[0]:.2f} @ {left[1].strftime('%Y-%m-%d')}")
                    print(f"  右端点: 价格 {right[0]:.2f} @ {right[1].strftime('%Y-%m-%d')}")
                    print(f"  穿越上影线数量: {conn['crossed_shadows']}")
            
            if results['low_connections']:
                print("\n下边界连线详情:")
                for i, conn in enumerate(results['low_connections'], 1):
                    left = conn['left_point']
                    right = conn['right_point']
                    print(f"连线 {i}:")
                    print(f"  左端点: 价格 {left[0]:.2f} @ {left[1].strftime('%Y-%m-%d')}")
                    print(f"  右端点: 价格 {right[0]:.2f} @ {right[1].strftime('%Y-%m-%d')}")
                    print(f"  穿越下影线数量: {conn['crossed_shadows']}")
    
    # 只有当所有检查都通过且有结果时才绘图
    if has_valid_lines:
        analyzer.plot(df, stock_id, results, debug=debug, batch_mode=False)

def process_batch_mode(stock_list, threshold, config, debug=False):
    """批量处理模式"""
    total_stocks = len(stock_list)
    process_start = time.time()
    
    # 设置进程数
    max_workers = min(max(1, os.cpu_count() - 1), 8)
    logger.info_print(f"开始分析 {total_stocks} 只股票，使用 {max_workers} 个进程")
    
    # 预先加载所有数据
    data_loader = DataLoader()
    all_stock_data = data_loader._get_all_stock_data(stock_list)
    data_loader.close()
    
    # 获取筛选条件配置
    filter_config = config['Programs']['Filter5']['filters']
    
    # 准备批处理参数
    stock_data_chunks = []
    chunk_size = len(stock_list) // max_workers
    for i in range(0, len(stock_list), chunk_size):
        chunk = {k: all_stock_data[k] for k in stock_list[i:i+chunk_size] if k in all_stock_data}
        stock_data_chunks.append((chunk, threshold, filter_config))
    
    # 多进程处理
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for chunk_data in stock_data_chunks:
            futures.append(executor.submit(process_stock_chunk, *chunk_data))
        
        # 收集结果
        qualified_rise_stocks = []
        qualified_low_stocks = []
        stocks_with_lines = []
        completed_stocks = 0
        
        for future in concurrent.futures.as_completed(futures):
            chunk_results = future.result()
            completed_stocks += len(chunk_results['processed'])
            
            # 直接使用 chunk_results 中的结果
            qualified_rise_stocks.extend(chunk_results['rise'])
            qualified_low_stocks.extend(chunk_results['low'])
            stocks_with_lines.extend(chunk_results['lines'])
            
            print(f"\r处理进度: {completed_stocks}/{total_stocks} "
                  f"({completed_stocks/total_stocks*100:.1f}%)", end="")
        
        process_time = time.time() - process_start
        
        # 打印统计信息
        logger.info_print(f"\n分析完成，耗时: {process_time:.1f} 秒")
        logger.info_print(f"涨幅合格: {len(qualified_rise_stocks)}/{total_stocks} ({len(qualified_rise_stocks)/total_stocks*100:.1f}%)")
        logger.info_print(f"最低价合格: {len(qualified_low_stocks)}/{len(qualified_rise_stocks)} ({len(qualified_low_stocks)/len(qualified_rise_stocks)*100:.1f}%)")
        logger.info_print(f"Triangle形态: {len(stocks_with_lines)}/{len(qualified_low_stocks)} ({len(stocks_with_lines)/len(qualified_low_stocks)*100:.1f}%)")
        
        return qualified_rise_stocks, qualified_low_stocks, stocks_with_lines, process_time

def process_stock_chunk(stock_data_dict, threshold, filter_config):
    """处理一组股票数据"""
    analyzer = ResistanceLineAnalyzer()
    results = {
        'processed': [],
        'rise': [],
        'low': [],
        'lines': []
    }
    
    for stock_id, df in stock_data_dict.items():
        results['processed'].append(stock_id)
        
        # 数据量检查（至少3天）
        if df is None or len(df) < 3:
            continue
        
        # 涨幅条件检查
        latest_data = df.iloc[-1]
        daily_change = ((latest_data['close'] - latest_data['open']) / latest_data['open']) * 100
        
        if filter_config['rise_check']['enabled']:
            is_qualified_rise = daily_change < filter_config['rise_check']['threshold']
            if not is_qualified_rise:
                continue
        
        results['rise'].append(stock_id)
        
        # 最低价条件检查
        is_qualified_low = True  # 默认通过
        if filter_config['low_price_check']['enabled']:
            if len(df) >= 3:
                base_day = df.iloc[-1]
                base_day_minus_1 = df.iloc[-2]
                base_day_minus_2 = df.iloc[-3]
                is_qualified_low = not (base_day['low'] < base_day_minus_2['low'] or 
                                     base_day_minus_1['low'] < base_day_minus_2['low'])
        
        if not is_qualified_low:
            continue
            
        results['low'].append(stock_id)
        
        # Triangle分析
        analysis_results = analyzer.analyze(df, stock_id)
        if (analysis_results and analysis_results['connections'] 
            and analysis_results['low_connections']):
            results['lines'].append(stock_id)
    
    return results

def plot_sample_stocks(qualified_stocks, threshold, config, debug=False):
    """绘制样本股票图表"""
    plot_start = time.time()
    
    if qualified_stocks:
        stocks_to_plot = random.sample(qualified_stocks, min(3, len(qualified_stocks)))
        
        for stock_id in stocks_to_plot:
            stock_id, df, results, _, _, _, has_valid_lines = process_single_stock_mp(
                stock_id, threshold, config['Programs']['Filter5']['filters'])
            if results:
                analyzer = ResistanceLineAnalyzer()
                analyzer.plot(df, stock_id, results, debug=debug, batch_mode=True)
    
    return time.time() - plot_start

def print_summary(qualified_rise_stocks, qualified_low_stocks, stocks_with_lines, total_stocks, start_time, threshold):
    """打印汇总信息"""
    logger.info_print("\n分析汇总:")
    logger.info_print(f"总股票数: {total_stocks}")
    logger.info_print(f"涨幅 < {threshold}% 的股票数量: {len(qualified_rise_stocks)} "
          f"({len(qualified_rise_stocks)/total_stocks*100:.1f}%)")
    logger.info_print(f"最低价合格的股票数量: {len(qualified_low_stocks)} "
          f"({len(qualified_low_stocks)/len(qualified_rise_stocks)*100:.1f}%)")
    logger.info_print(f"存在完整Triangle形态的股票数量: {len(stocks_with_lines)} "
          f"({len(stocks_with_lines)/len(qualified_low_stocks)*100:.1f}%)")

def save_analysis_result(stocks_with_lines, total_stocks, config, root_dir, logger):
    """保存分析结果到数据库和CSV"""
    try:
        # 保存到CSV
        output_path = os.path.join(root_dir, "QA", config['CSVs']['Filters']['Filter5'])
        df_output = pd.DataFrame({
            'Index': range(1, len(stocks_with_lines) + 1),
            'Stock Code': stocks_with_lines
        })
        df_output.to_csv(output_path, index=False)
        
        # 保存到数据库
        connection = db_con_pymysql(config)
        cursor = connection.cursor()
        
        # 准备数据
        filter_name = "Filter5"
        input_count = total_stocks
        output_count = len(stocks_with_lines)
        reduction = input_count - output_count
        reduction_rate = (reduction / input_count * 100) if input_count > 0 else 0
        run_date = datetime.now()
        
        # 获取源文件和输出文件名
        source_file = os.path.basename(config['CSVs']['Filters']['Filter3'])
        output_file = os.path.basename(config['CSVs']['Filters']['Filter5'])
        
        # 准备详细信息
        details = "日K对称三角形"
        
        # 构建SQL语句
        sql = """
        INSERT INTO filter_history 
        (filter_name, source_file, output_file, input_count, output_count, 
         reduction, reduction_rate, run_date, details)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # 执行SQL
        cursor.execute(sql, (
            filter_name, source_file, output_file, input_count, output_count,
            reduction, reduction_rate, run_date, details
        ))
        
        connection.commit()
        logger.info_print(f"分析结果已保存到数据库 filter_history 表")
        
    except Exception as e:
        logger.error_print(f"保存分析结果时出错: {str(e)}")
        raise
    finally:
        if connection:
            connection.close()

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
    """更新FilterResults表中的F_Triangle列"""
    try:
        if filtered_out_stocks:
            update_query = f"""
            UPDATE {filter_results_table}
            SET F_Triangle = 1
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
    threshold = 10
    
    # 命令行参数解析
    parser = argparse.ArgumentParser(description='对称三角形')
    parser.add_argument('--stock', type=str, help='单个股票代码，例如：000001')
    parser.add_argument('--debug', action='store_true', help='开启调试模式，显示详细信息')
    args = parser.parse_args()
    
    try:
        # 获取配置文件路径并加载配置
        config_path_QA, _, root_dir = find_config_path()
        config = load_config(config_path_QA)
        
        # 设置日志
        global logger
        logger = set_log(config, "Triangle.log", prefix="QA")
        logger.info_print("开始执行对称三角形分析...")
        
        # 确定处理日期
        is_today, processing_date = is_today_workday(logger)
        
        # 获取数据库表名
        filter_results_table = config['DB_tables']['filter_results']
        
        if args.stock:
            # 单股处理模式
            stock_id = f"{int(args.stock):06d}"
            logger.info_print(f"单股处理模式: {stock_id}")
            process_single_mode(stock_id, threshold, config, args.debug)
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
                qualified_rise_stocks, qualified_low_stocks, stocks_with_lines, process_time = process_batch_mode(
                    stock_list, threshold, config, args.debug)
                
                # 计算被过滤掉的股票
                filtered_out_stocks = list(set(stock_list) - set(stocks_with_lines))
                
                # 更新FilterResults表
                if not update_filter_results(cursor, filter_results_table, processing_date, 
                                          filtered_out_stocks, logger, args.debug):
                    logger.error_print("更新过滤结果失败")
                    return
                
                # 保存过滤结果到数据库
                details = "分析股票的对称三角形形态"
                save_filter_result(
                    cursor, 
                    config, 
                    "Filter5", 
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
                plot_time = plot_sample_stocks(stocks_with_lines, threshold, config, args.debug)
                if args.debug:
                    logger.debug(f"图形绘制耗时: {plot_time:.2f} 秒")
                
            finally:
                connection.close()
            
            # 记录执行完成
            logger.info_print("对称三角形分析执行完成")
            
    except Exception as e:
        import traceback
        logger.error_print(f"程序执行失败: {str(e)}")
        logger.error_print(traceback.format_exc())

if __name__ == "__main__":
    main()
