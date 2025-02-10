"""
this file is used to remove some backup csv files and databales in DB (QA)
"""

import os
import re
from datetime import datetime
import glob
from typing import List
import sys
from pathlib import Path

# 假设这些是从其他模块导入的
from DBconnection import find_config_path, load_config, db_con_pymysql, set_log

def get_sorted_csv_files(csv_dir: str) -> List[str]:
    """获取并排序指定格式的CSV文件"""
    pattern = r'StkList_(\d{8})_(\d{6})\.csv$'
    files = []
    
    for file in os.listdir(csv_dir):
        match = re.match(pattern, file)
        if match:
            date_str = match.group(1)
            time_str = match.group(2)
            timestamp = datetime.strptime(f"{date_str}_{time_str}", "%Y%m%d_%H%M%S")
            files.append((file, timestamp))
    
    return sorted(files, key=lambda x: x[1], reverse=True)
    

def get_sorted_tables(cursor) -> List[str]:
    """获取并排序月份数据表，并检查是否存在所有12个月份的数据"""
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()
    
    month_pattern = r'^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)(\d{1,2})$'
    month_tables = []
    found_months = set()
    
    for table in tables:
        # 根据返回类型获取表名
        if isinstance(table, dict):
            table_name = list(table.values())[0]  # 如果是字典，获取第一个值
        else:
            table_name = table[0]  # 如果是元组，获取第一个元素
            
        match = re.match(month_pattern, table_name)
        if match:
            month_str = match.group(1)
            day_str = match.group(2)
            # 转换月份为数字
            month_num = datetime.strptime(month_str, "%b").month
            found_months.add(month_str)  # 记录找到的月份
            # 使用当前年份构建完整日期
            date = datetime(2024, month_num, int(day_str))
            month_tables.append((table_name, date))
    
    return sorted(month_tables, key=lambda x: x[1], reverse=True)

def main():
    # 1. 获取配置路径
    qa_config_dir, _, root_dir = find_config_path()
    # 2. 加载QA配置
    config = load_config(qa_config_dir)
    
    # 设置日志
    log_dir = Path("CommonFunc/Logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    logger = set_log(config, "clean_qa.log", prefix="QA")
    
    try:
        # 处理CSV文件
        csv_dir = os.path.join(root_dir, "QA", "CSVs")
        csv_files = get_sorted_csv_files(csv_dir)
        
        # 记录找到的CSV文件
        csv_msg = f"Found {len(csv_files)} CSV files: {', '.join(f[0] for f in csv_files)}"
        logger.info(csv_msg)
        print(csv_msg)
        
        # 删除多余的CSV文件，保留最新的两个
        if len(csv_files) > 2:
            files_to_delete = csv_files[2:]
            kept_files = [f[0] for f in csv_files[:2]]
            delete_msg = f"Keeping newest files: {', '.join(kept_files)}"
            logger.info(delete_msg)
            print(delete_msg)
            
            for file, _ in files_to_delete:
                file_path = os.path.join(csv_dir, file)
                os.remove(file_path)
                delete_msg = f"Removed old CSV file: {file}"
                logger.info(delete_msg)
                print(delete_msg)
        
        # 处理数据库表
        conn = db_con_pymysql(config)
        cursor = conn.cursor()
        
        tables = get_sorted_tables(cursor)
        
        # 记录找到的数据表
        tables_msg = f"Found {len(tables)} month tables: {', '.join(t[0] for t in tables)}"
        logger.info(tables_msg)
        print(tables_msg)
        
        # 删除多余的表，保留最新的4个
        if len(tables) > 4:
            tables_to_keep = [t[0] for t in tables[:4]]
            tables_to_delete = tables[4:]
            keep_msg = f"Keeping newest tables: {', '.join(tables_to_keep)}"
            logger.info(keep_msg)
            print(keep_msg)
            
            for table_name, _ in tables_to_delete:
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                conn.commit()
                delete_msg = f"Dropped old table: {table_name}"
                logger.info(delete_msg)
                print(delete_msg)
        
        cursor.close()
        conn.close()
        
        logger.info("Clean-up completed successfully")
        print("Clean-up completed successfully")
        
    except Exception as e:
        error_msg = f"Error during clean-up: {str(e)}"
        logger.error(error_msg)
        print(error_msg)
        raise

if __name__ == "__main__":
    main()