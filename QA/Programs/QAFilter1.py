"""
Filter1: 过滤三天累加增长超过12%的股票
输出: QA/CSVs/Filter1Out.csv
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
from QA.SubFunc.SubQA001 import save_filter_result
from QA.Programs.QA002 import is_today_workday, last_workday  # 新增导入
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

def save_filter_results(cursor, stock_codes, filtered_out_stocks, processing_date, logger, program_debug=False):
    """将过滤结果保存到数据库"""
    try:
        # 使用传入的处理日期而不是当前日期
        
        # 准备批量插入的数据
        insert_data = []
        
        # 对所有股票进行处理
        for stock_id in stock_codes:
            # 统一格式化股票代码为6位字符串
            stock_id = str(stock_id).zfill(6)
            
            # 确定FilteredBy的值
            filtered_by = 1 if stock_id in filtered_out_stocks else 0
            
            # 添加到批量插入列表
            insert_data.append((stock_id, processing_date, filtered_by))
        
        # 构建REPLACE语句（避免主键冲突）
        sql = """
        REPLACE INTO FilterResults 
        (ID, FilterDate, FilteredBy)
        VALUES (%s, %s, %s)
        """
        
        # 执行批量插入
        cursor.executemany(sql, insert_data)
        
        if program_debug:
            logger.debug(f"插入记录数: {len(insert_data)}")
            logger.debug(f"其中被过滤股票数: {len(filtered_out_stocks)}")
        
        return True
        
    except Exception as e:
        logger.error_print(f"保存过滤结果到数据库时出错: {str(e)}")
        if program_debug:
            logger.error_print(f"详细错误: {str(e.__class__.__name__)}: {str(e)}")
        return False

def main():
    """主函数（向量化版本）"""
    # 获取配置文件路径并加载配置
    config_path_QA, _, root_dir = find_config_path()
    config = load_config(config_path_QA)
    
    # 添加程序特定的DEBUG配置
    program_debug = config.get('Programs', {}).get('Filter1', {}).get('DEBUG', False)
    
    logger = set_log(config, "Filter1.log", prefix="QA")
    logger.info_print("开始执行 QAFilter1 过滤程序...")
    
    # 确定处理日期
    is_today, processing_date = is_today_workday(logger)
    
    # 获取数据库表名
    table_name = config['DB_tables']['main_query_table']
    filter_results_table = config['DB_tables']['filter_results']
    
    # 获取CSV文件路径配置
    qa_dir = os.path.dirname(config_path_QA)
    input_csv = os.path.join(qa_dir, config['CSVs']['Filters']['Input'])
    
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
        
        # 打印过滤前后的数量对比
        logger.info_print(f"过滤前股票数量: {input_count}")
        logger.info_print(f"过滤后股票数量: {output_count}")
        
        # Debug输出
        if program_debug:
            debug_df = pd.DataFrame({
                'stock_code': filtered_out_stocks,
                'total_gain': gains_details.loc[filtered_out_stocks, 'sum_gains'],
                'dates': gains_details.loc[filtered_out_stocks, 'dates'],
                'daily_gains': gains_details.loc[filtered_out_stocks, 'daily_gains']
            })
            debug_file = os.path.join(qa_dir, 'CSVs', 'debug_filter1.csv')
            debug_df.to_csv(debug_file, index=False)
            logger.debug(f"Debug信息已保存至 {os.path.basename(debug_file)}")
        
        # 保存过滤历史到filter_history表
        details = "过滤三天累计涨幅超过12%的股票"
        source_file = os.path.basename(input_csv)
        output_file = os.path.join(qa_dir, config['CSVs']['Filters']['Filter1'])
        
        save_filter_result(
            cursor, 
            config, 
            "Filter1", 
            input_count, 
            output_count, 
            logger,
            details,
            source_file,
            output_file
        )
        
        # 保存过滤结果到FilterResults表
        if not save_filter_results(cursor, stock_codes, filtered_out_stocks, processing_date, logger, program_debug):
            logger.error_print("保存过滤结果到数据库失败")
            return False
            
        # 提交事务
        connection.commit()
        
        total_time = time.time() - start_time
        logger.info_print(f"过滤结果已保存到数据库, 总耗时: {total_time:.2f} 秒")
        
        return True
        
    except Exception as e:
        logger.error_print(f"程序执行过程中发生错误: {str(e)}")
        return False
    
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    main()