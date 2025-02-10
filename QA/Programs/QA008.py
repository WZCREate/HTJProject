"""
QA008.py
用于每天更新当前周周K数据
"""

import pandas as pd
import datetime
import chinese_calendar as cc
from CommonFunc.DBconnection import find_config_path, load_config, set_log, db_con_sqlalchemy
from QA.SubFunc.Ini_WK_MuTh import convert_date_to_week
import os
from sqlalchemy import text

def get_stock_list(config, root_dir):
    """从CSV文件获取股票代码列表"""
    try:
        csv_path = os.path.join(root_dir, "QA", config['CSVs']['MainCSV'])
        df = pd.read_csv(csv_path)
        stock_list = df.iloc[:, 1].astype(str).tolist()
        stock_list = [code.zfill(6) for code in stock_list if code.isdigit()]
        return stock_list
    except Exception as e:
        raise Exception(f"读取股票列表失败: {str(e)}")

def get_week_workdays(current_date):
    """获取当前周的工作日列表"""
    # 获取本周一的日期
    day_of_week = current_date.isoweekday()
    monday = current_date - datetime.timedelta(days=day_of_week - 1)
    
    # 获取本周的所有工作日
    workdays = []
    for i in range(7):
        check_date = monday + datetime.timedelta(days=i)
        if cc.is_workday(check_date):
            workdays.append(check_date.strftime('%Y-%m-%d'))
    
    return workdays

def get_last_week_workday(current_date):
    """获取上周最后一个工作日"""
    # 获取上周一的日期
    day_of_week = current_date.isoweekday()
    this_monday = current_date - datetime.timedelta(days=day_of_week - 1)
    last_monday = this_monday - datetime.timedelta(days=7)
    
    # 获取上周的所有工作日
    last_week_workdays = []
    for i in range(7):
        check_date = last_monday + datetime.timedelta(days=i)
        if cc.is_workday(check_date):
            last_week_workdays.append(check_date)
    
    # 返回上周最后一个工作日
    return last_week_workdays[-1] if last_week_workdays else None

def update_weekly_data():
    """更新当前周的周K数据"""
    # 获取配置信息
    config_path_QA, _, root_dir = find_config_path()
    config = load_config(config_path_QA)
    
    # 设置日志
    logger = set_log(config, "QA008.log", "QA")
    logger.info_print("开始更新本周周K数据")
    
    # 获取debug模式设置
    debug_mode = config.get('Programs', {}).get('QA008', {}).get('DEBUG', False)
    
    try:
        # 获取当前日期和计算周的起止时间
        current_date = datetime.datetime.now()
        day_of_week = current_date.isoweekday()
        week_start = current_date - datetime.timedelta(days=day_of_week - 1)
        week_end = week_start + datetime.timedelta(days=6)
        
        # 获取当前时间作为更新时间
        update_time = current_date.strftime("%Y-%m-%d %H:%M:%S")
        current_week = convert_date_to_week(current_date)
        
        # 获取股票列表
        stock_list = get_stock_list(config, root_dir)
        total_stocks = len(stock_list)
        
        if debug_mode:
            logger.info_print(f"查询日期范围: {week_start.strftime('%Y-%m-%d')} 到 {week_end.strftime('%Y-%m-%d')}")
        
        # 使用 SQLAlchemy 创建数据库连接
        engine = db_con_sqlalchemy(config)
        
        # 获取上周最后一个工作日
        last_week_workday = get_last_week_workday(current_date)
        
        # 修改查询语句，同时获取上周最后一个工作日的收盘价
        stock_list_str = "','".join(stock_list)
        sql = f"""
        SELECT a.id, a.date, a.open_price, a.close_price, a.high, a.low,
               b.close_price as last_week_close
        FROM {config['DB_tables']['main_query_table']} a
        LEFT JOIN (
            SELECT id, close_price
            FROM {config['DB_tables']['main_query_table']}
            WHERE date = '{last_week_workday.strftime('%Y-%m-%d')}'
        ) b ON a.id = b.id
        WHERE a.id IN ('{stock_list_str}')
        AND a.date >= '{week_start.strftime('%Y-%m-%d')}'
        AND a.date <= '{week_end.strftime('%Y-%m-%d')}'
        """
        
        print("正在查询数据...")
        df = pd.read_sql_query(sql, engine)
        
        if debug_mode:
            logger.info_print(f"查询到 {len(df)} 条数据")
        
        # 使用pandas进行分组计算，确保数值类型正确
        print("正在计算周K数据...")
        weekly_data = df.groupby('id').agg({
            'open_price': lambda x: float(x.iloc[0]) if pd.notna(x.iloc[0]) else None,
            'close_price': lambda x: float(x.iloc[-1]) if pd.notna(x.iloc[-1]) else None,
            'high': lambda x: float(x.max()) if pd.notna(x.max()) else None,
            'low': lambda x: float(x.min()) if pd.notna(x.min()) else None,
            'last_week_close': 'first'  # 获取上周收盘价
        }).reset_index()
        
        # 计算涨跌幅
        weekly_data['chg_percen'] = weekly_data.apply(
            lambda row: ((float(row['close_price']) - float(row['last_week_close'])) / float(row['last_week_close']) * 100)
            if pd.notna(row['close_price']) and pd.notna(row['last_week_close']) else None,
            axis=1
        )
        
        # 添加其他必要的列
        weekly_data['wkn'] = current_week
        weekly_data['WK_date'] = current_date.strftime('%Y-%m-%d')
        weekly_data['update_time'] = update_time
        # 根据open值判断status
        weekly_data['status'] = weekly_data['open_price'].apply(lambda x: 'st' if pd.isna(x) else 'active')
        
        # 处理没有数据的股票
        missing_stocks = set(stock_list) - set(weekly_data['id'].tolist())
        if missing_stocks:
            missing_df = pd.DataFrame({
                'id': list(missing_stocks),
                'wkn': current_week,
                'WK_date': current_date.strftime('%Y-%m-%d'),
                'open': None,
                'close': None,
                'high': None,
                'low': None,
                'chg_percen': None,
                'update_time': update_time,
                'status': 'st'
            })
            weekly_data = pd.concat([weekly_data, missing_df], ignore_index=True)
        
        # 重命名列以匹配数据库表结构
        weekly_data = weekly_data.rename(columns={
            'open_price': 'open',
            'close_price': 'close'
        })
        
        # 批量更新数据库
        print("正在更新数据库...")
        def show_progress(current, total):
            print(f"\r处理进度: {current}/{total} ({current/total:.1%})", end="", flush=True)
        
        for idx, row in weekly_data.iterrows():
            show_progress(idx + 1, len(weekly_data))
            
            sql_update = text(f"""
            INSERT INTO {config['DB_tables']['WK_table']} 
            (id, wkn, WK_date, open, close, high, low, chg_percen, update_time, status)
            VALUES ('{row['id']}', '{row['wkn']}', '{row['WK_date']}', 
                    {row['open'] if pd.notna(row['open']) else 'NULL'}, 
                    {row['close'] if pd.notna(row['close']) else 'NULL'}, 
                    {row['high'] if pd.notna(row['high']) else 'NULL'}, 
                    {row['low'] if pd.notna(row['low']) else 'NULL'},
                    {row['chg_percen'] if pd.notna(row['chg_percen']) else 'NULL'},
                    '{row['update_time']}', '{row['status']}')
            ON DUPLICATE KEY UPDATE
            WK_date = VALUES(WK_date),
            open = VALUES(open),
            close = VALUES(close),
            high = VALUES(high),
            low = VALUES(low),
            chg_percen = VALUES(chg_percen),
            update_time = VALUES(update_time),
            status = VALUES(status)
            """)
            
            with engine.connect() as conn:
                conn.execute(sql_update)
                conn.commit()
        
        print("\n")
        
        # 统计信息
        success_count = len(weekly_data[weekly_data['status'] == 'active'])
        st_count = len(weekly_data[weekly_data['status'] == 'st'])
        
        logger.info_print(f"""
=== QA008更新周K完成统计 ===
总计处理: {total_stocks} 只股票
正常股票: {success_count} 只 ({success_count/total_stocks:.1%})
ST股票: {st_count} 只 ({st_count/total_stocks:.1%})
""")
        
        return True, "数据更新成功完成"
        
    except Exception as e:
        error_msg = f"程序执行出现错误: {str(e)}"
        logger.error_print(error_msg)
        return False, error_msg

def main():
    """
    主函数，用于独立运行时的程序入口
    返回:
        success: bool, 程序是否成功执行
        message: str, 执行结果信息
    """
    try:
        success, message = update_weekly_data()
        if success:
            print("程序正常结束")
            return True, message
        else:
            print(f"程序执行失败: {message}")
            return False, message
    except Exception as e:
        error_message = f"程序异常终止: {str(e)}"
        print(error_message)
        return False, error_message

if __name__ == "__main__":
    success, _ = main()
    exit(0 if success else 1)
