"""
Filter1: 过滤三天累加增长超过12%的股票
输出: PROD/CSVs/Filter1Out.csv
"""

import pandas as pd
import numpy as np
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
import time

def fetch_all_data(cursor, stock_codes, table_name):
    """一次性获取所有股票的最近三天数据"""
    query = f"""
    WITH recent_dates AS (
        SELECT DISTINCT date 
        FROM {table_name}
        WHERE Latest = 1
        AND open_price is not null
        ORDER BY date DESC
        LIMIT 3
    )
    SELECT t.id, t.date, t.chg_percen
    FROM {table_name} t
    INNER JOIN recent_dates rd ON t.date = rd.date
    WHERE t.Latest = 1
    AND t.id IN ({','.join(['%s'] * len(stock_codes))})
    """
    cursor.execute(query, stock_codes)
    return cursor.fetchall()

def process_data_vectorized(data, logger):
    """使用向量化操作处理数据"""
    try:
        # 转换为DataFrame
        df = pd.DataFrame(data, columns=['id', 'date', 'chg_percen'])
        
        # 转换数据类型
        df['chg_percen'] = pd.to_numeric(df['chg_percen'], errors='coerce')
        df['date'] = pd.to_datetime(df['date'])
        df['id'] = df['id'].astype(str)
        
        # 按股票代码和日期排序
        df = df.sort_values(['id', 'date'], ascending=[True, False])
        
        # 使用transform计算每个股票的最近三天数据
        df['date_rank'] = df.groupby('id')['date'].transform(lambda x: range(len(x)))
        
        # 只保留最近三天的数据
        df = df[df['date_rank'] < 3]
        
        # 计算每个股票的累计涨幅
        gains_sum = df.groupby('id').agg({
            'chg_percen': 'sum',
            'date': lambda x: ','.join(x.dt.strftime('%Y-%m-%d')),
            'date_rank': 'count'  # 用于检查是否有完整的三天数据
        })
        
        # 只保留有完整三天数据的股票
        valid_stocks = gains_sum[gains_sum['date_rank'] == 3].copy()
        
        # 重命名列
        valid_stocks.columns = ['sum_gains', 'dates', 'days_count']
        
        # 添加daily_gains列
        daily_gains = df.groupby('id')['chg_percen'].agg(
            lambda x: ','.join(map(str, x))
        )
        valid_stocks['daily_gains'] = daily_gains
        
        return valid_stocks
        
    except Exception as e:
        logger.error_print(f"数据处理过程中出错: {str(e)}")
        return pd.DataFrame()

def main():
    """主函数（向量化版本）"""
    # 保持PROD环境配置
    _, config_path_PROD, root_dir = find_config_path()
    config = load_config(config_path_PROD)
    
    # 添加程序特定的DEBUG配置
    program_debug = config.get('Programs', {}).get('Filter1', {}).get('DEBUG', False)
    
    logger = set_log(config, "Filter1.log", prefix="PROD")
    
    # 获取数据库表名
    table_name = config['DB_tables']['main_query_table']
    
    # 获取CSV文件路径配置（使用PROD路径）
    prod_dir = os.path.dirname(config_path_PROD)
    input_csv = os.path.join(prod_dir, config['CSVs']['Filters']['Input'])
    output_csv = os.path.join(prod_dir, config['CSVs']['Filters']['Filter1'])
    
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
            logger.info_print(f"成功读取 {input_count} 个股票代码")
        except Exception as e:
            logger.error_print(f"读取股票代码文件失败: {str(e)}")
            return False
        
        # 获取并处理数据
        start_time = time.time()
        
        # 一次性获取所有数据
        results = fetch_all_data(cursor, stock_codes, table_name)
        
        # 向量化处理数据
        gains_details = process_data_vectorized(results, logger)
        
        if gains_details.empty:
            logger.error_print("没有获取到有效的涨幅数据")
            return False
        
        # 过滤股票
        filtered_stocks = gains_details[gains_details['sum_gains'] <= 12].index.values
        filtered_out_stocks = gains_details[gains_details['sum_gains'] > 12].index.values
        output_count = len(filtered_stocks)
        
        # Debug输出
        if program_debug:
            debug_df = pd.DataFrame({
                'stock_code': filtered_out_stocks,
                'total_gain': gains_details.loc[filtered_out_stocks, 'sum_gains'],
                'dates': gains_details.loc[filtered_out_stocks, 'dates'],
                'daily_gains': gains_details.loc[filtered_out_stocks, 'daily_gains']
            })
            debug_file = os.path.join(prod_dir, 'CSVs', 'debug_filter1.csv')
            debug_df.to_csv(debug_file, index=False)
            logger.debug(f"Debug信息已保存至 {os.path.basename(debug_file)}")
        
        # 保存结果
        details = "过滤三天累计涨幅超过12%的股票"
        source_file = os.path.basename(input_csv)
        output_file = os.path.basename(output_csv)
        
        # 创建一个不输出信息的logger用于save_filter_result
        silent_logger = logger.getChild('silent')
        silent_logger.info_print = lambda x: None
        
        save_filter_result(
            cursor, 
            config, 
            "Filter1", 
            input_count, 
            output_count, 
            silent_logger,  # 使用静默logger
            details,
            source_file,
            output_file
        )
        connection.commit()
        
        # 保存CSV
        df_output = pd.DataFrame({
            'Index': range(1, len(filtered_stocks) + 1),
            'Stock Code': filtered_stocks
        })
        df_output.to_csv(output_csv, index=False)
        
        # 在Filter0Out.csv中标注被过滤的股票
        try:
            # 读取原始CSV
            filter0_df = pd.read_csv(input_csv)
            
            # 确保Stock Code列的格式一致（补齐6位）
            filter0_df['Stock Code'] = filter0_df['Stock Code'].astype(str).str.zfill(6)
            filtered_out_stocks = pd.Series(filtered_out_stocks).astype(str).str.zfill(6)
            
            # 如果没有FilteredBy列，添加该列
            if 'FilteredBy' not in filter0_df.columns:
                filter0_df['FilteredBy'] = None
            
            # 标注被Filter1过滤的股票
            mask = filter0_df['Stock Code'].isin(filtered_out_stocks)
            filter0_df.loc[mask, 'FilteredBy'] = 'Filter1'
            
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
        
        total_time = time.time() - start_time
        logger.info_print(f"过滤信息已保存到数据库, 总耗时: {total_time:.2f} 秒")
        logger.info_print(f"已将过滤后的股票保存至 {os.path.basename(output_csv)}")
        
        return True
        
    except Exception as e:
        logger.error_print(f"程序执行过程中发生错误: {str(e)}")
        return False
    
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    main()