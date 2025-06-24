'''
手动更新指定日期的FilterResults表的NextCHG列
'''
import pandas as pd
from datetime import datetime, timedeltas
from CommonFunc.DBconnection import load_config, db_con_pymysql, find_config_path, set_log

def get_filtered_stocks(conn, filter_date):
    """获取FilterResults中满足条件的股票"""
    query = """
    SELECT ID, FilterDate 
    FROM FilterResults 
    WHERE FilterDate = %s
    AND (
        (FilteredBy = 0 AND F_WK = 0) 
        OR 
        (FilteredBy = 0 AND F_Triangle = 0)
    )
    """
    with conn.cursor() as cursor:
        cursor.execute(query, (filter_date,))
        return pd.DataFrame(cursor.fetchall())

def get_next_trading_day_changes(conn, stock_ids, start_date):
    """获取下一个交易日的涨跌幅"""
    if not stock_ids:
        return pd.DataFrame()
        
    # 将股票ID列表转换为SQL的IN子句格式
    placeholders = ','.join(['%s'] * len(stock_ids))
    
    query = f"""
    SELECT id, date, chg_percen
    FROM StockMain
    WHERE id IN ({placeholders})
    AND date > %s
    AND Latest = 1
    ORDER BY id, date
    """
    
    # 构建参数列表
    params = list(stock_ids) + [start_date]
    
    with conn.cursor() as cursor:
        cursor.execute(query, params)
        changes_df = pd.DataFrame(cursor.fetchall())
        
    if changes_df.empty:
        return changes_df
        
    # 对每个股票只保留最早的那条记录（即下一个交易日的记录）
    return changes_df.sort_values('date').groupby('id').first().reset_index()

def update_next_changes(conn, updates_df, filter_date):
    """更新FilterResults表的NextCHG列"""
    if updates_df.empty:
        return
        
    update_query = """
    UPDATE FilterResults 
    SET NextCHG = %s 
    WHERE ID = %s AND FilterDate = %s
    """
    
    # 批量更新
    update_data = [
        (row['chg_percen'], row['id'], filter_date) 
        for _, row in updates_df.iterrows()
    ]
    
    with conn.cursor() as cursor:
        cursor.executemany(update_query, update_data)
        conn.commit()

def process_next_changes(filter_date):
    """主处理函数"""
    # 获取配置文件路径
    config_path_QA, _, _ = find_config_path()
    
    # 加载配置
    config = load_config(config_path_QA)
    
    # 设置日志
    logger = set_log(config, "NextChange.log")
    
    # 连接数据库
    conn = db_con_pymysql(config)
    
    try:
        # 1. 获取需要处理的股票
        filtered_stocks = get_filtered_stocks(conn, filter_date)
        if filtered_stocks.empty:
            logger.info_print(f"No stocks found for date {filter_date}")
            return
        
        # 2. 获取这些股票下一个交易日的涨跌幅
        next_day_changes = get_next_trading_day_changes(
            conn, 
            filtered_stocks['ID'].tolist(), 
            filter_date
        )
        
        if next_day_changes.empty:
            logger.info_print(f"No next day changes found after {filter_date}")
            return
            
        # 3. 更新FilterResults表
        update_next_changes(conn, next_day_changes, filter_date)
        
        logger.info_print(f"Successfully updated NextCHG for {len(next_day_changes)} stocks")
        
    except Exception as e:
        logger.error_print(f"Error processing next changes: {str(e)}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    # 示例用法
    filter_date = "2025-02-14"
    process_next_changes(filter_date)
