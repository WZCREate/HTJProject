"""
Filter3:
本程序根据 缺口 数据进行过滤
1. 无缺口的股票保留
2. 有缺口的股票，若 close_price * 1.1 < min(to_price) 则保留
输出: PROD/CSVs/Filter3Out.csv
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
from PROD.SubFunc.SubAK001 import save_filter_result
from PROD.Programs.AK002 import is_today_workday, last_workday
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

def main():
    """主函数"""
    # 获取配置文件路径并加载配置
    _, config_path_PROD, root_dir = find_config_path()
    config = load_config(config_path_PROD)
    
    # 添加程序特定的DEBUG配置
    program_debug = config.get('Programs', {}).get('Filter3', {}).get('DEBUG', False)
    
    # 设置日志文件名与程序名相同
    logger = set_log(config, "Filter3.log", prefix="PROD")
    logger.info_print("开始执行 Filter3 过滤程序...")
    
    # 确定处理日期
    is_today, processing_date = is_today_workday(logger)
    logger.info_print(f"处理日期: {processing_date.strftime('%Y-%m-%d')}")
    
    # 获取数据库表名
    main_table = config['DB_tables']['main_query_table']
    gap_table = config['DB_tables']['gap_table']
    
    # 获取CSV文件路径配置
    qa_dir = os.path.dirname(config_path_PROD)
    input_csv = os.path.join(qa_dir, config['CSVs']['Filters']['Filter2'])
    output_csv = os.path.join(qa_dir, config['CSVs']['Filters']['Filter3'])
    
    connection = None
    try:
        # 连接数据库
        connection = db_con_pymysql(config)
        cursor = connection.cursor()
        
        # 读取股票代码
        try:
            stock_df = pd.read_csv(input_csv)
            input_count = len(stock_df)
            stock_codes = stock_df.iloc[:, 1].tolist()
        except Exception as e:
            logger.error_print(f"读取股票代码文件失败: {str(e)}")
            return False
        
        # 获取数据并处理
        try:
            start_time = time.time()
            
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
            execution_time = time.time() - start_time
            
            # 在Filter0Out.csv中标注被过滤的股票
            original_input = os.path.join(qa_dir, config['CSVs']['Filters']['Input'])
            mark_filtered_stocks(filtered_out_details, original_input, logger, program_debug)
            
            # 打印过滤前后的数量对比
            logger.info_print("\n过滤结果统计:")
            logger.info_print(f"过滤前股票数量: {input_count}")
            logger.info_print(f"过滤后股票数量: {output_count}")
            logger.info_print(f"处理耗时: {execution_time:.2f} 秒")
            
            # 添加DEBUG输出逻辑
            if program_debug and filtered_out_details:
                debug_df = pd.DataFrame(filtered_out_details)
                debug_file = os.path.join(qa_dir, 'CSVs', 'debug_filter3.csv')
                debug_df.to_csv(debug_file, index=False)
                logger.info_print(f"Debug信息已保存至 {os.path.basename(debug_file)}")
                logger.debug(f"被过滤掉的股票数量: {len(filtered_out_details)}")

        except Exception as e:
            logger.error_print(f"处理股票数据失败: {str(e)}")
            return False

        # 保存过滤结果到数据库
        details = "根据缺口数据进行过滤"
        source_file = os.path.basename(input_csv)
        output_file = os.path.basename(output_csv)
        
        if program_debug:
            logger.debug(f"准备保存过滤结果：")
            logger.debug(f"source_file: {source_file}")
            logger.debug(f"output_file: {output_file}")
            logger.debug(f"input_count: {input_count}")
            logger.debug(f"output_count: {output_count}")
        
        save_filter_result(
            cursor, 
            config, 
            "Filter3", 
            input_count, 
            output_count, 
            logger, 
            details,
            source_file,
            output_file
        )
        connection.commit()

        if program_debug:
            logger.info_print(f"请增加DEBUG逻辑")

        # 创建输出DataFrame
        df_output = pd.DataFrame({
            'Index': range(1, len(filtered_stocks) + 1),
            'Stock Code': filtered_stocks
        })
        df_output.to_csv(output_csv, index=False)
        logger.info_print(f"已将过滤后的股票保存至 {os.path.basename(output_csv)}")
        
        logger.info_print("\nQAFilter3 过滤程序执行完成")
        return True

    except Exception as e:
        logger.error_print(f"执行过程中发生错误: {str(e)}")
        return False
    
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    main()
