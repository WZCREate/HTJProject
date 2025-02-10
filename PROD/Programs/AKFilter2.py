"""
Filter2:
本程序根据 MA 均线的压力线进行过滤
{close price * (1+10%) < MA120 & MA250} or {close price > MA120  & MA250}
输出: PROD/CSVs/Filter2Out.csv
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

def fetch_data_for_date(cursor, stock_codes, main_table, ma_table, processing_date):
    """获取指定日期的收盘价和均线数据"""
    placeholders = ', '.join(['%s'] * len(stock_codes))
    query = f"""
    SELECT m.id, m.close_price, ma.MA120, ma.MA250
    FROM {main_table} m
    LEFT JOIN {ma_table} ma ON m.id = ma.id AND m.date = m.date
    WHERE m.id IN ({placeholders})
    AND m.date = %s
    """
    cursor.execute(query, stock_codes + [processing_date])
    results = cursor.fetchall()
    return results

def process_filter_condition(df):
    """处理过滤条件
    条件：{close price * (1+10%) < MA120 & MA250} or {close price > MA120 & MA250}
    """
    # 确保所有需要的列都存在
    required_columns = ['close_price', 'MA120', 'MA250']
    if not all(col in df.columns for col in required_columns):
        raise ValueError("缺少必要的数据列")
    
    # 将所有列转换为数值类型
    for col in required_columns:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 计算条件
    condition1 = (df['close_price'] * 1.1 < df['MA120']) & (df['close_price'] * 1.1 < df['MA250'])
    condition2 = (df['close_price'] > df['MA120']) & (df['close_price'] > df['MA250'])
    
    # 满足条件的股票
    filtered_stocks = df[condition1 | condition2]['id'].tolist()
    
    # 被过滤掉的股票详情
    filtered_out = df[~(condition1 | condition2)]
    filtered_out_details = []
    
    for _, row in filtered_out.iterrows():
        filter_reason = []
        if not (row['close_price'] * 1.1 < row['MA120'] and row['close_price'] * 1.1 < row['MA250']):
            if row['close_price'] * 1.1 >= row['MA120']:
                filter_reason.append("收盘价*1.1 >= MA120")
            if row['close_price'] * 1.1 >= row['MA250']:
                filter_reason.append("收盘价*1.1 >= MA250")
        if not (row['close_price'] > row['MA120'] and row['close_price'] > row['MA250']):
            if row['close_price'] <= row['MA120']:
                filter_reason.append("收盘价 <= MA120")
            if row['close_price'] <= row['MA250']:
                filter_reason.append("收盘价 <= MA250")
        
        filtered_out_details.append({
            'stock_code': row['id'],
            'close_price': row['close_price'],
            'MA120': row['MA120'],
            'MA250': row['MA250'],
            'price_x1.1': round(row['close_price'] * 1.1, 2),
            'filter_reason': ' 且 '.join(filter_reason)
        })
    
    return filtered_stocks, filtered_out_details

def main():
    """主函数"""
    # 获取配置文件路径并加载配置
    _, config_path_PROD, root_dir = find_config_path()
    config = load_config(config_path_PROD)
    
    # 添加程序特定的DEBUG配置
    program_debug = config.get('Programs', {}).get('Filter2', {}).get('DEBUG', False)
    
    # 设置日志文件名与程序名相同
    logger = set_log(config, "Filter2.log", prefix="PROD")
    logger.info_print("开始执行 AKFilter2 过滤程序...")
    
    # 确定处理日期
    is_today, processing_date = is_today_workday(logger)
    
    # 获取数据库表名
    main_table = config['DB_tables']['main_query_table']
    ma_table = config['MA_config']['ma_table']
    
    # 获取CSV文件路径配置
    prod_dir = os.path.dirname(config_path_PROD)
    input_csv = os.path.join(prod_dir, config['CSVs']['Filters']['Filter1'])
    output_csv = os.path.join(prod_dir, config['CSVs']['Filters']['Filter2'])
    
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
        
        # 获取最新数据并处理
        try:
            start_time = time.time()
            results = fetch_data_for_date(cursor, stock_codes, main_table, ma_table, processing_date)
            df = pd.DataFrame(results)
            
            filtered_stocks, filtered_out_details = process_filter_condition(df)
            output_count = len(filtered_stocks)
            
            # 打印过滤前后的数量对比
            logger.info_print("\n过滤结果统计:")
            logger.info_print(f"过滤前股票数量: {input_count}")
            logger.info_print(f"过滤后股票数量: {output_count}")
            
            # 添加DEBUG输出逻辑
            if program_debug and filtered_out_details:
                debug_df = pd.DataFrame(filtered_out_details)
                debug_file = os.path.join(prod_dir, 'CSVs', 'debug_filter2.csv')
                debug_df.to_csv(debug_file, index=False)
                logger.info_print(f"Debug信息已保存至 {os.path.basename(debug_file)}")

            # 在Filter0Out.csv中标注被过滤的股票
            try:
                # 读取Filter0Out.csv
                filter0_csv = os.path.join(prod_dir, config['CSVs']['Filters']['Input'])
                filter0_df = pd.read_csv(filter0_csv)
                
                # 确保Stock Code列的格式一致（补齐6位）
                filter0_df['Stock Code'] = filter0_df['Stock Code'].astype(str).str.zfill(6)
                filtered_out_stocks = pd.Series(filtered_stocks).astype(str).str.zfill(6)
                
                # 如果没有FilteredBy列，添加该列
                if 'FilteredBy' not in filter0_df.columns:
                    filter0_df['FilteredBy'] = None
                
                # 标注被Filter2过滤的股票
                mask = filter0_df['Stock Code'].isin(filtered_out_stocks)
                filter0_df.loc[mask, 'FilteredBy'] = 'Filter2'
                
                # 保存回Filter0Out.csv
                filter0_df.to_csv(filter0_csv, index=False)
                logger.info_print(f"已在 {os.path.basename(filter0_csv)} 中标注被过滤股票")
            except Exception as e:
                logger.error_print(f"标注被过滤股票时出错: {str(e)}")

        except Exception as e:
            logger.error_print(f"处理股票数据失败: {str(e)}")
            return False

        # 保存过滤结果到数据库
        details = "根据均线MA120和MA250进行过滤"
        source_file = os.path.basename(input_csv)
        output_file = os.path.basename(output_csv)
        save_filter_result(
            cursor, 
            config, 
            "Filter2", 
            input_count, 
            output_count, 
            logger, 
            details,
            source_file,
            output_file
        )
        connection.commit()

        # 创建输出DataFrame并保存
        df_output = pd.DataFrame({
            'Index': range(1, len(filtered_stocks) + 1),
            'Stock Code': filtered_stocks
        })
        df_output.to_csv(output_csv, index=False)
        logger.info_print(f"已将过滤后的股票保存至 {os.path.basename(output_csv)}")
        
        logger.info_print("\nAKFilter2 过滤程序执行完成")
        return True

    except Exception as e:
        logger.error_print(f"执行过程中发生错误: {str(e)}")
        return False
    
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    main()