'''
每日更新上一个交易日FilterResults表的NextCHG列
'''

import pandas as pd
from datetime import datetime, timedelta
from CommonFunc.DBconnection import load_config, db_con_pymysql, find_config_path, set_log
from QA.Programs.QA002 import is_today_workday, last_workday

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

def get_previous_trading_day_changes(conn, stock_ids, target_date):
    """获取目标日期的涨跌幅
    target_date: T日，用于获取当天的涨跌幅数据"""
    if not stock_ids:
        return pd.DataFrame()
        
    placeholders = ','.join(['%s'] * len(stock_ids))
    
    query = f"""
    SELECT id, date, chg_percen
    FROM StockMain
    WHERE id IN ({placeholders})
    AND date = %s
    AND Latest = 1
    """
    
    params = list(stock_ids) + [target_date]
    
    with conn.cursor() as cursor:
        cursor.execute(query, params)
        return pd.DataFrame(cursor.fetchall())

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

def process_next_changes(today_date, prev_workday, logger):
    """主处理函数
    today_date: T日，用于获取涨跌幅数据
    prev_workday: T-1日，用于获取和更新FilterResults记录
    """
    # 连接数据库
    config_path_QA, _, _ = find_config_path()
    config = load_config(config_path_QA)
    conn = db_con_pymysql(config)
    
    try:
        # 1. 获取T-1日需要处理的股票
        filtered_stocks = get_filtered_stocks(conn, prev_workday)
        if filtered_stocks.empty:
            logger.info_print(f"No stocks found for date {prev_workday}")
            return
        
        # 2. 获取这些股票在T日的涨跌幅
        today_changes = get_previous_trading_day_changes(
            conn, 
            filtered_stocks['ID'].tolist(), 
            today_date  # 使用T日获取涨跌幅
        )
        
        if current_day_changes.empty:
            logger.info_print(f"No changes found for {current_date}")
            return
            
        # 3. 更新FilterResults表
        update_next_changes(conn, current_day_changes, previous_date)
        
        logger.info_print(f"Successfully updated NextCHG for {len(current_day_changes)} stocks")
        
    except Exception as e:
        logger.error_print(f"Error processing next changes: {str(e)}")
        raise
    finally:
        conn.close()

def main():
    """主函数：处理当日或最近工作日的数据"""
    # 获取配置和设置日志
    config_path_QA, _, _ = find_config_path()
    config = load_config(config_path_QA)
    logger = set_log(config, "DailyNextCHG.log")
    
    try:
        # 获取当前日期
        today = datetime.now().date()
        
        # 判断是否为工作日，如果不是则获取最近的工作日
        if is_today_workday(today):
            previous_workday = last_workday(today)
            logger.info_print(f"Processing data for workday {today}, updating {previous_workday}'s records")
            
            # 先处理昨日记录
            process_next_changes(
                current_date=today,
                previous_date=previous_workday,
                logger=logger
            )
            
            # 同时更新今日记录为待处理状态（如果需要）
            # ... [可根据需要添加]
        else:
            logger.info_print("Non-workday, skip processing")
        
    except Exception as e:
        logger.error_print(f"Daily NextCHG processing failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()