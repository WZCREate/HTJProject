"""
用于初始化缺口数据表
用于查询最后一个下跌缺口
日常更新缺口数据使用 QA007.py
"""

import pandas as pd
from CommonFunc.DBconnection import db_con_pymysql as connect_db, load_config, find_config_path
import csv
import os
from CommonFunc.DBconnection import set_log

def fetch_all_stock_data(connection, table, stock_ids, start_date, end_date):
    """
    批量获取多个股票的数据
    """
    stock_ids_str = ",".join([f"'{id}'" for id in stock_ids])
    query = f"""
    SELECT id, date, open_price, close_price, high, low 
    FROM `{table}`
    WHERE id IN ({stock_ids_str})
      AND date BETWEEN '{start_date}' AND '{end_date}'
      AND open_price IS NOT NULL
      AND close_price IS NOT NULL
      AND high IS NOT NULL
      AND low IS NOT NULL
    ORDER BY id, date ASC;
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()
    return pd.DataFrame(result)

def read_stock_codes_from_csv(csv_path):
    try:
        stock_codes = []
        with open(csv_path, mode="r", newline="", encoding="utf-8") as file:
            reader = csv.reader(file)
            next(reader)  # 跳过表头
            for row in reader:
                if len(row) >= 2:  # 确保每行至少有两列
                    stock_codes.append(row[1])  # 提取第二列作为股票代码
        return stock_codes
    except Exception as e:
        print(f"读取 CSV 文件时出错: {e}")
        return []

def calculate_down_gap(stock_data):
    gaps = []  # 用于存所有检测到的下跌缺口
    
    # 检查每一对相邻的日期，看看是否形成下跌缺口
    for i in range(1, len(stock_data)):
        prev_row = stock_data.iloc[i - 1]  # 获取前一行数据
        curr_row = stock_data.iloc[i]      # 获取当前行数据
        
        # 判断是否形成下跌缺口：当前交易日的最高价小于前一交易日的最低价
        if curr_row["high"] < prev_row["low"]:
            gaps.append({
                "start_date": prev_row["date"],  # 缺口开始日期
                "end_date": None,                # 初始时，缺口的结束日期设为 None
                "gap_low": curr_row["high"],     # 缺口的最低价是当前交易日的最高价
                "gap_high": prev_row["low"],     # 缺口的最高价是前一交易日的最低价
                "filled": False,                 # 初始时，缺口未被填满
                "filled_date": None,             # 填满日期暂时为 None
            })
    
    # 检查缺口是否被填满
    for gap in gaps:
        for _, row in stock_data.iterrows():
            if gap["end_date"] is None and gap["start_date"] < row["date"]:
                # 判断是否填满
                if row["high"] >= gap["gap_high"]:
                    gap["filled"] = True  # 缺口被填满
                    gap["filled_date"] = row["date"]  # 记录填满日期
                    gap["end_date"] = row["date"]    # 设置缺口的结束日期为填满日期
                    break  # 找到填满缺口的日期后就跳出当前循环
                elif gap["gap_low"] <= row["high"] < gap["gap_high"]:
                    # 更新缺口的最低价为当前行的最高价
                    gap["gap_low"] = row["high"]
    
    return gaps

def calculate_gaps_batch(stock_data):
    """
    批量计算缺口
    Args:
        stock_data: 包含多只股票的 DataFrame
    Returns:
        包含所有缺口信息的 DataFrame
    """
    all_gaps = []

    # 按股票代码分组
    grouped = stock_data.groupby("id")
    for stock_id, group in grouped:
        group = group.sort_values("date")  # 按日期排序
        gaps = calculate_down_gap(group)  # 计算单只股票的缺口

        # 转换为 DataFrame 格式
        for gap in gaps:
            all_gaps.append({
                "id": stock_id,
                "sdate": gap["start_date"],
                "filled": 1 if gap["filled"] else 0,
                "edate": gap["filled_date"] if gap["filled"] else None,
                "from_price": gap["gap_high"],
                "to_price": None if gap["filled"] else gap["gap_low"],
            })

    return pd.DataFrame(all_gaps)

def write_gaps_batch_to_mysql(connection, gaps):
    """
    批量写入缺口数据到 MySQL
    Args:
        connection: MySQL 数据库连接
        gaps: 包含缺口信息的 DataFrame
    """
    try:
        with connection.cursor() as cursor:
            # 使用批量插入
            insert_query = """
            INSERT INTO `Gap` (id, sdate, filled, edate, from_price, to_price, gap_update_time)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            """
            data = gaps.to_records(index=False).tolist()  # 转换为元组列表
            cursor.executemany(insert_query, data)  # 批量插入
            connection.commit()
            print(f"成功批量写入 {len(gaps)} 条缺口数据。")
    except Exception as e:
        print(f"批量写入缺口数据时出错: {e}")
        connection.rollback()

def clear_gap_table(connection):
    """
    清空 Gap 表中的所有数据
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE `Gap`")
            connection.commit()
            print("成功清空 Gap 表")
    except Exception as e:
        print(f"清空 Gap 表时出错: {e}")
        connection.rollback()

def main(config_path, start_date, end_date, batch_size=600):
    # 加载配置
    config = load_config(config_path)
    
    # 获取程序特定的调试模式设置
    debug_mode = config.get("Programs", {}).get("Init_Gap", {}).get("DEBUG", False)
    
    # 设置日志
    logger = set_log(config, "Init_Gap.log", prefix="QA")
    
    # 获取根目录
    _, _, root_dir = find_config_path()
    
    if debug_mode:
        logger.info_print("DEBUG模式已启用")
        logger.info_print(f"配置文件路径: {config_path}")
        logger.info_print(f"根目录: {root_dir}")
    
    # 构建完整的 CSV 文件路径
    csv_relative_path = config["CSVs"]["Filters"]["Filter1"]
    csv_path = os.path.join(root_dir, "QA", "CSVs", "Filter1Out.csv")
    
    if debug_mode:
        logger.info_print(f"配置中的CSV相对路径: {csv_relative_path}")
        logger.info_print(f"完整的CSV文件路径: {csv_path}")
    
    stock_codes = read_stock_codes_from_csv(csv_path)
    
    if debug_mode:
        logger.info_print(f"读取到的股票代码数量: {len(stock_codes)}")

    # 使用 DBconnection 中的连接函数
    if debug_mode:
        logger.info_print("尝试连接数据库...")
        # 创建一个不包含密码的数据库配置副本
        db_config = config.get('DBConnection', {}).copy()
        if 'password' in db_config:
            db_config['password'] = '********'  # 用星号替换密码
        logger.info_print(f"数据库配置: {db_config}")
        
    connection = connect_db(config)

    try:
        # 在处理数据前先清空表
        if debug_mode:
            logger.info_print("准备清空 Gap 表...")
        clear_gap_table(connection)
        
        # 从配置文件中读取主查询表名
        main_query_table = config.get("DB_tables", {}).get("main_query_table")
        
        if debug_mode:
            logger.info_print(f"使用的数据库表名: {main_query_table}")
            
        if not main_query_table:
            raise ValueError("配置文件中未找到 DB_tables.main_query_table 配置项")
        
        # 获取股票代码列表并处理
        for i in range(0, len(stock_codes), batch_size):
            batch_codes = stock_codes[i:i + batch_size]
            if debug_mode:
                logger.info_print(f"正在处理第 {i//batch_size + 1} 批数据，包含 {len(batch_codes)} 个股票代码")
                logger.info_print(f"查询表名: {main_query_table}")
                
            stock_data = fetch_all_stock_data(connection, main_query_table, batch_codes, start_date, end_date)
            if stock_data.empty:
                logger.info_print(f"批次 {batch_codes} 未找到任何数据")
                continue

            # 批量计算缺口
            gaps = calculate_gaps_batch(stock_data)
            if gaps.empty:
                logger.info_print(f"批次 {batch_codes} 未计算出任何缺口")
                continue

            # 批量写入数据库
            write_gaps_batch_to_mysql(connection, gaps)

    finally:
        connection.close()

# 调用主函数
if __name__ == "__main__":
    config_path_QA, _, _ = find_config_path()  # 只获取 QA 环境的配置路径
    start_date = "2024-01-01"
    end_date = "2024-12-22"
    main(config_path_QA, start_date, end_date)
