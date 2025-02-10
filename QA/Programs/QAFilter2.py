"""
Filter2:
本程序根据 MA 均线的压力线进行过滤
{close price * (1+10%) < MA120 & MA250} or {close price > MA120  & MA250}
输出: QA/CSVs/Filter2Out.csv
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
        # 添加更详细的错误信息
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
    program_debug = config.get('Programs', {}).get('Filter2', {}).get('DEBUG', False)
    
    # 设置日志文件名与程序名相同
    logger = set_log(config, "Filter2.log", prefix="QA")
    logger.info_print("开始执行 QAFilter2 过滤程序...")
    
    # 确定处理日期
    is_today, processing_date = is_today_workday(logger)
    
    # 获取数据库表名
    main_table = config['DB_tables']['main_query_table']
    ma_table = config['MA_config']['ma_table']
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
        
        # 获取最新数据并处理
        try:
            start_time = time.time()
            results = fetch_data_for_date(cursor, stock_codes, main_table, ma_table, processing_date)
            df = pd.DataFrame(results)
            
            filtered_stocks, filtered_out_details = process_filter_condition(df)
            output_count = len(filtered_stocks)
            
            # 打印过滤前后的数量对比
            logger.info_print(f"过滤前股票数量: {input_count}")
            logger.info_print(f"过滤后股票数量: {output_count}")

            # 添加DEBUG输出逻辑
            if program_debug and filtered_out_details:
                debug_df = pd.DataFrame(filtered_out_details)
                debug_file = os.path.join(qa_dir, 'CSVs', 'debug_filter2.csv')
                debug_df.to_csv(debug_file, index=False)
                logger.info_print(f"Debug信息已保存至 {os.path.basename(debug_file)}")

            # 更新FilterResults表中被过滤的股票
            try:
                filtered_out_stocks = [detail['stock_code'] for detail in filtered_out_details]
                if filtered_out_stocks:  # 只在有需要更新的股票时执行更新
                    update_query = f"""
                    UPDATE {filter_results_table}
                    SET FilteredBy = 2
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
        details = "根据均线MA120和MA250进行过滤"
        save_filter_result(
            cursor, 
            config, 
            "Filter2", 
            input_count, 
            output_count, 
            logger, 
            details,
            "FilterResults",  # 使用数据表名作为来源
            "FilterResults"   # 使用数据表名作为输出
        )
        connection.commit()
        
        logger.info_print("QAFilter2 过滤程序执行完成")
        return True

    except Exception as e:
        logger.error_print(f"执行过程中发生错误: {str(e)}")
        return False
    
    finally:
        if connection:
            connection.close()

if __name__ == "__main__":
    main()