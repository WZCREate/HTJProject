"""
OutputTargets.py
输出当天未被过滤的目标股票列表
"""

import pandas as pd
import os
from CommonFunc.DBconnection import (
    find_config_path,
    load_config,
    db_con_pymysql,
    set_log
)
from datetime import datetime
from QA.Programs.QA002 import is_today_workday, last_workday

def fetch_target_stocks(cursor, filter_results_table, processing_date, debug=False):
    """获取目标股票列表"""
    try:
        # 将处理日期转换为字符串格式
        date_str = processing_date.strftime('%Y-%m-%d') if isinstance(processing_date, datetime) else processing_date
        
        query = f"""
        SELECT ID
        FROM {filter_results_table}
        WHERE FilterDate = %s 
        AND FilteredBy = 0
        AND (F_WK = 0 OR F_Triangle = 0)
        ORDER BY ID
        """
        
        if debug:
            print(f"Debug - Query: {query}")
            print(f"Debug - Date parameter: {date_str}")
        
        cursor.execute(query, [date_str])
        results = cursor.fetchall()
        
        if debug:
            print(f"Debug - Found {len(results)} records")
            if results:
                print(f"Debug - First result: {results[0]}")
        
        # 从DictCursor结果中提取ID值
        stock_codes = [row['ID'] for row in results]
        return stock_codes
        
    except Exception as e:
        error_msg = f"获取目标股票时发生错误: {str(e)}"
        if debug:
            error_msg += f"\nSQL: {query}\nDate: {date_str}"
        raise Exception(error_msg)

def main():
    """主函数"""
    # 获取配置文件路径并加载配置
    config_path_QA, _, root_dir = find_config_path()
    config = load_config(config_path_QA)
    
    # 添加程序特定的DEBUG配置
    program_debug = config.get('Programs', {}).get('OutputTargets', {}).get('DEBUG', False)
    
    # 设置日志
    logger = set_log(config, "OutputTargets.log", prefix="QA")
    logger.info_print("开始执行目标股票导出程序...")
    
    # 确定处理日期
    is_today, processing_date = is_today_workday(logger)
    
    # 获取数据库表名
    filter_results_table = config['DB_tables']['filter_results']
    
    connection = None
    try:
        # 连接数据库
        connection = db_con_pymysql(config)
        cursor = connection.cursor()
        
        # 获取目标股票
        stock_codes = fetch_target_stocks(cursor, filter_results_table, processing_date, program_debug)
        
        if not stock_codes:
            logger.warning_print("没有找到符合条件的股票")
            return
        
        # 转换为DataFrame
        df = pd.DataFrame({'Stock Code': stock_codes})
        
        # 确保股票代码格式正确（6位）
        df['Stock Code'] = df['Stock Code'].astype(str).str.zfill(6)
        
        # 构建输出路径
        output_path = os.path.join(root_dir, "QA", "CSVs", "Targets.csv")
        
        # 保存到CSV
        df.to_csv(output_path, index=False)
        logger.info_print(f"已找到 {len(stock_codes)} 只目标股票")
        logger.info_print(f"结果已保存至: {os.path.basename(output_path)}")
        
    except Exception as e:
        logger.error_print(f"程序执行失败: {str(e)}")
        return False
    
    finally:
        if connection:
            connection.close()
        
    logger.info_print("目标股票导出程序执行完成")
    return True

if __name__ == "__main__":
    main()
