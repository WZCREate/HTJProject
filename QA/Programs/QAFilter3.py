"""
Filter3:
本程序根据 缺口 数据进行过滤
1. 无缺口的股票保留
2. 有缺口的股票，若 close_price * 1.1 < min(to_price) 则保留
输出: QA/CSVs/Filter3Out.csv
"""

import pandas as pd
import os
from tqdm import tqdm
from CommonFunc.DBconnection import (
    find_config_path,
    load_config,
    db_con_pymysql,
    set_log
)
from datetime import datetime
from QA.SubFunc.SubQA001 import save_filter_result
from QA.Programs.QA002 import is_today_workday, last_workday
import time

def fetch_latest_prices(cursor, stock_codes, main_table, processing_date):
    """获取指定日期的收盘价"""
    placeholders = ', '.join(['%s'] * len(stock_codes))
    query = f"""
    SELECT id, close_price
    FROM {main_table}
    WHERE id IN ({placeholders})
    AND date = %s
    """
    cursor.execute(query, stock_codes + [processing_date])
    results = cursor.fetchall()
    return pd.DataFrame(results)

def fetch_unfilled_gaps(cursor, stock_codes, gap_table):
    """获取未填充的缺口数据"""
    placeholders = ', '.join(['%s'] * len(stock_codes))
    query = f"""
    SELECT id, to_price
    FROM {gap_table}
    WHERE id IN ({placeholders})
    AND filled = 0
    ORDER BY id, to_price  # 确保按股票代码和价格排序
    """
    cursor.execute(query, stock_codes)
    results = cursor.fetchall()
    return pd.DataFrame(results, columns=['id', 'to_price'])

def process_filter_condition(prices_df, gaps_df):
    """处理过滤条件
    1. 无缺口的股票保留
    2. 有缺口的股票：
       a. 单个缺口：close * 1.1 < to_price 则保留
       b. 多个缺口：close * 1.1 < min(to_price[to_price > close]) 则保留
    """
    filtered_stocks = []
    filtered_out_details = []  # 新增：用于存储被过滤掉的股票详情
    
    # 确保数据类型正确
    prices_df['close_price'] = pd.to_numeric(prices_df['close_price'], errors='coerce')
    gaps_df['to_price'] = pd.to_numeric(gaps_df['to_price'], errors='coerce')
    
    # 对每个股票进行处理
    for _, price_row in prices_df.iterrows():
        stock_id = price_row['id']
        close_price = price_row['close_price']
        
        # 获取该股票的所有缺口记录
        stock_gaps = gaps_df[gaps_df['id'] == stock_id]
        
        # 记录过滤原因
        filter_reason = ""
        is_filtered = False
        min_gap_price = None
        
        if len(stock_gaps) == 0:
            # 无缺口，保留
            filtered_stocks.append(stock_id)
            continue
            
        if len(stock_gaps) == 1:
            # 单个缺口
            gap_price = stock_gaps['to_price'].iloc[0]
            if close_price * 1.1 < gap_price:
                filtered_stocks.append(stock_id)
            else:
                is_filtered = True
                filter_reason = "单缺口且收盘价*1.1 >= 缺口价格"
                min_gap_price = gap_price
        else:
            # 多个缺口
            valid_gaps = stock_gaps[stock_gaps['to_price'] > close_price]
            if len(valid_gaps) > 0:
                to_price_nearest = valid_gaps['to_price'].min()
                if close_price * 1.1 < to_price_nearest:
                    filtered_stocks.append(stock_id)
                else:
                    is_filtered = True
                    filter_reason = "多缺口且收盘价*1.1 >= 最近缺口价格"
                    min_gap_price = to_price_nearest
            else:
                filtered_stocks.append(stock_id)
        
        # 如果股票被过滤掉，记录详情
        if is_filtered:
            filtered_out_details.append({
                'stock_code': stock_id,
                'close_price': close_price,
                'gap_price': min_gap_price,
                'price_x1.1': round(close_price * 1.1, 2),
                'filter_reason': filter_reason
            })
    
    return filtered_stocks, filtered_out_details

def mark_filtered_stocks(filtered_out_details, input_csv, logger, program_debug=False):
    """在Filter0Out.csv中标注被过滤的股票"""
    try:
        # 读取原始CSV
        filter0_df = pd.read_csv(input_csv)
        
        # 确保Stock Code列的格式一致（补齐6位）
        filter0_df['Stock Code'] = filter0_df['Stock Code'].astype(str).str.zfill(6)
        filtered_out_stocks = [str(detail['stock_code']).zfill(6) for detail in filtered_out_details]
        
        # 如果没有FilteredBy列，添加该列
        if 'FilteredBy' not in filter0_df.columns:
            filter0_df['FilteredBy'] = None
        
        # 标注被Filter3过滤的股票
        mask = filter0_df['Stock Code'].isin(filtered_out_stocks)
        filter0_df.loc[mask, 'FilteredBy'] = 'Filter3'
        
        # 保存回原文件
        filter0_df.to_csv(input_csv, index=False)
        logger.info_print(f"已在 {os.path.basename(input_csv)} 中标注被过滤股票")
        
        # 打印调试信息
        if program_debug:
            logger.debug(f"被标注的股票数量: {mask.sum()}")
            logger.debug(f"被过滤的股票: {', '.join(filtered_out_stocks)}")
        
    except Exception as e:
        logger.error_print(f"标注被过滤股票时出错: {str(e)}")
        if program_debug:
            logger.error_print(f"详细错误: {str(e.__class__.__name__)}: {str(e)}")

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
        
        # 执行查询
        cursor.execute(query, [date_str])
        results = cursor.fetchall()
        
        # 只在debug模式下打印调试信息
        if debug:
            print(f"Debug - Found {len(results)} records")
        
        if not results:
            if debug:
                print("Debug - No records found")
            return []
            
        # 由于使用DictCursor，需要通过'ID'键获取值
        stock_codes = [str(row['ID']).zfill(6) for row in results]
        if debug:
            print(f"Debug - First few stock codes: {stock_codes[:5]}")
        return stock_codes
        
    except Exception as e:
        error_msg = f"""
        获取股票代码时发生错误: {str(e)}
        SQL: {query}
        Date: {date_str}
        """
        if debug:
            error_msg += f"""
            Results type: {type(results) if 'results' in locals() else 'Not available'}
            First result: {results[0] if 'results' in locals() and results else 'No results'}
            """
        raise Exception(error_msg)

def main():
    """主函数"""
    # 获取配置文件路径并加载配置
    config_path_QA, _, root_dir = find_config_path()
    config = load_config(config_path_QA)
    
    # 添加程序特定的DEBUG配置
    program_debug = config.get('Programs', {}).get('Filter3', {}).get('DEBUG', False)
    
    # 设置日志文件名与程序名相同
    logger = set_log(config, "Filter3.log", prefix="QA")
    logger.info_print("开始执行 QAFilter3 过滤程序...")
    
    # 确定处理日期
    is_today, processing_date = is_today_workday(logger)
    
    # 获取数据库表名
    main_table = config['DB_tables']['main_query_table']
    gap_table = config['DB_tables']['gap_table']
    filter_results_table = config['DB_tables']['filter_results']
    
    # 获取CSV路径仅用于DEBUG输出
    qa_dir = os.path.dirname(config_path_QA)
    
    connection = None
    try:
        # 连接数据库
        connection = db_con_pymysql(config)
        cursor = connection.cursor()
        
        # 从数据库读取股票代码
        try:
            stock_codes = fetch_stock_codes(cursor, filter_results_table, processing_date, program_debug)
            input_count = len(stock_codes)
            if input_count == 0:
                logger.warning_print("没有找到需要处理的股票代码")
                return False
        except Exception as e:
            logger.error_print(f"读取股票代码失败: {str(e)}")
            return False
        
        # 获取数据并处理
        try:
            # 获取指定日期的收盘价
            prices_df = fetch_latest_prices(cursor, stock_codes, main_table, processing_date)
            if program_debug:
                logger.debug(f"获取到 {len(prices_df)} 条收盘价数据")
            
            # 获取未填充的缺口数据
            gaps_df = fetch_unfilled_gaps(cursor, stock_codes, gap_table)
            if program_debug:
                logger.debug(f"获取到 {len(gaps_df)} 条缺口数据")
            
            # 处理过滤条件
            filtered_stocks, filtered_out_details = process_filter_condition(prices_df, gaps_df)
            output_count = len(filtered_stocks)
            
            # 打印过滤前后的数量对比
            logger.info_print(f"过滤前股票数量: {input_count}")
            logger.info_print(f"过滤后股票数量: {output_count}")
            
            # DEBUG输出逻辑
            if program_debug and filtered_out_details:
                debug_df = pd.DataFrame(filtered_out_details)
                debug_file = os.path.join(qa_dir, 'CSVs', 'debug_filter3.csv')
                debug_df.to_csv(debug_file, index=False)
                logger.info_print(f"Debug信息已保存至 {os.path.basename(debug_file)}")

            # 更新FilterResults表中被过滤的股票
            try:
                filtered_out_stocks = [detail['stock_code'] for detail in filtered_out_details]
                if filtered_out_stocks:  # 只在有需要更新的股票时执行更新
                    update_query = f"""
                    UPDATE {filter_results_table}
                    SET FilteredBy = 3
                    WHERE FilterDate = %s AND ID IN ({','.join(['%s'] * len(filtered_out_stocks))})
                    """
                    cursor.execute(update_query, [processing_date] + filtered_out_stocks)
                    connection.commit()
                    logger.info_print(f"已更新 {cursor.rowcount} 条FilterResults记录")
                else:
                    logger.info_print("没有需要更新的记录")
            except Exception as e:
                logger.error_print(f"更新FilterResults表时出错: {str(e)}")
                connection.rollback()

        except Exception as e:
            logger.error_print(f"处理股票数据失败: {str(e)}")
            return False

        # 保存过滤结果到数据库
        details = "根据缺口数据进行过滤"
        save_filter_result(
            cursor, 
            config, 
            "Filter3", 
            input_count, 
            output_count, 
            logger, 
            details,
            "FilterResults",  # 使用数据表名作为来源
            "FilterResults"   # 使用数据表名作为输出
        )
        connection.commit()
        
        logger.info_print("QAFilter3 过滤程序执行完成")
        return True

    except Exception as e:
        logger.error_print(f"执行过程中发生错误: {str(e)}")
        return False
    
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    main()
