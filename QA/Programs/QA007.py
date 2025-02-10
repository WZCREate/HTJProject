"""
用于日常更新缺口数据表(数据库Gap表)
初始化数据表请执行 SubFunc/Init_Gap.py
"""

import pandas as pd
from datetime import datetime
from typing import Tuple, List, Dict, Any
from QA002 import last_workday, is_today_workday
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from pymysql.connections import Connection
from pathlib import Path
import os
from CommonFunc.DBconnection import find_config_path, load_config, db_con_pymysql, db_con_sqlalchemy, set_log

class GapManager:
    def __init__(self, env: str, logger, connection: Connection, engine: Engine, config: Dict[str, Any]):
        self.env = env
        self.logger = logger
        self.connection = connection
        self.engine = engine
        self.config = config
        self.gap_table = config["DB_tables"]["gap_table"]
        self.main_query_table = config["DB_tables"]["main_query_table"]
        
    def update_existing_gaps(self, trade_date: str, debug: bool = False) -> None:
        """更新现有尚未填满的缺口信息"""
        # 读取未填满的缺口
        query_gaps = f"SELECT * FROM `{self.gap_table}` WHERE filled = 0"
        gaps_df = pd.read_sql(query_gaps, self.engine)
        if debug:
            self.logger.debug(f"Debug: Found {len(gaps_df)} unfilled gaps")
        
        stock_ids = tuple(gaps_df['id'].unique())
        query_high = f"""
        SELECT id, high 
        FROM `{self.main_query_table}` 
        WHERE id IN {stock_ids if len(stock_ids) > 1 else f"('{stock_ids[0]}')"} 
        AND date = '{trade_date}'
        AND Latest = 1
        """
        if debug:
            self.logger.debug(f"Debug: Query for high prices: {query_high}")
        
        high_prices_df = pd.read_sql(query_high, self.engine)
        if debug:
            self.logger.debug(f"Debug: Found {len(high_prices_df)} records with high prices")
        
        # 找出哪些股票没有获取到最高价
        stocks_without_high = set(gaps_df['id']) - set(high_prices_df['id'])
        if debug:
            self.logger.debug(f"Debug: {len(stocks_without_high)} stocks without high price data")
            if stocks_without_high:
                self.logger.debug(f"Debug: Sample stocks without high price: {list(stocks_without_high)[:5]}")
                
                # 对样本股票进行详细查询
                sample_stock = list(stocks_without_high)[0]
                verify_query = f"""
                SELECT id, date, high, Latest
                FROM `{self.main_query_table}`
                WHERE id = '{sample_stock}'
                AND date <= '{trade_date}'
                ORDER BY date DESC
                LIMIT 5
                """
                verify_data = pd.read_sql(verify_query, self.engine)
                self.logger.debug(f"Debug: Recent data for sample stock {sample_stock}:")
                self.logger.debug(verify_data)
        
        # 确保 id 列的类型一致
        gaps_df['id'] = gaps_df['id'].astype(str)
        high_prices_df['id'] = high_prices_df['id'].astype(str)
        
        # 使用 left join 合并数据
        gaps_df = gaps_df.merge(high_prices_df, on='id', how='left')
        
        if debug:
            self.logger.debug(f"Debug: Merged data summary:")
            self.logger.debug(f"Total gaps: {len(gaps_df)}")
            self.logger.debug(f"Gaps with high price: {len(gaps_df[gaps_df['high'].notna()])}")
            self.logger.debug(f"Gaps without high price: {len(gaps_df[gaps_df['high'].isna()])}")
        
        if gaps_df.empty:
            self.logger.info("QA: 没有未填满的缺口需要更新。")
            return
        
        self.logger.info(f"QA: 共有 {len(gaps_df)} 个未填满的缺口需要更新。")
        
        # 根据不同情况分组处理
        # 1. 无今日数据的股票 - 仅更新时间戳
        no_data_stocks = gaps_df[gaps_df['high'].isna()][['id', 'sdate']]
        self.logger.info_print(f"QA: {len(no_data_stocks)} 个股票无今日数据,仅更新时间戳。")
        
        # 2. 无需更新状态的缺口 (to_price >= today_high)
        no_update_gaps = gaps_df[gaps_df['to_price'] >= gaps_df['high']][['id', 'sdate']]
        self.logger.info_print(f"QA: {len(no_update_gaps)} 个缺口没有缩小,无需更新。")
        
        # 3. 填满的缺口 (to_price < today_high && from_price <= today_high)
        filled_gaps = gaps_df[
            (gaps_df['to_price'] < gaps_df['high']) & 
            (gaps_df['from_price'] <= gaps_df['high'])
        ][['id', 'sdate']]
        self.logger.info_print(f"QA: {len(filled_gaps)} 个缺口已填满。")
        
        # 4. 缺口减小的情况 (to_price < today_high && from_price > today_high)
        reduced_gaps = gaps_df[
            (gaps_df['to_price'] < gaps_df['high']) & 
            (gaps_df['from_price'] > gaps_df['high'])
        ][['id', 'sdate', 'high']]
        self.logger.info_print(f"QA: {len(reduced_gaps)} 个缺口缩小。")

        with self.connection.cursor() as cursor:
            # 批量更新无数据股票
            if not no_data_stocks.empty:
                values = no_data_stocks.apply(lambda x: (x['id'], x['sdate']), axis=1).tolist()
                cursor.executemany(
                    f"UPDATE `{self.gap_table}` SET gap_update_time = NOW() WHERE id = %s AND sdate = %s",
                    values
                )
                self.logger.info(f"QA: 更新 {len(no_data_stocks)} 个无数据股票的时间戳")
            
            # 批量更新无需变化的缺口
            if not no_update_gaps.empty:
                values = no_update_gaps.apply(lambda x: (x['id'], x['sdate']), axis=1).tolist()
                cursor.executemany(
                    f"UPDATE `{self.gap_table}` SET gap_update_time = NOW() WHERE id = %s AND sdate = %s",
                    values
                )
                self.logger.info(f"QA: 更新 {len(no_update_gaps)} 个无需变化的缺口")
            
            # 批量更新已填满的缺口
            if not filled_gaps.empty:
                values = filled_gaps.apply(lambda x: (trade_date, x['id'], x['sdate']), axis=1).tolist()
                cursor.executemany(
                    f"UPDATE `{self.gap_table}` SET filled = 1, edate = %s, gap_update_time = NOW() WHERE id = %s AND sdate = %s",
                    values
                )
                self.logger.info(f"QA: 更新 {len(filled_gaps)} 个已填满的缺口")
            
            # 批量更新减小的缺口
            if not reduced_gaps.empty:
                values = reduced_gaps.apply(lambda x: (x['high'], x['id'], x['sdate']), axis=1).tolist()
                cursor.executemany(
                    f"UPDATE `{self.gap_table}` SET to_price = %s, gap_update_time = NOW() WHERE id = %s AND sdate = %s",
                    values
                )
                self.logger.info(f"QA: 更新 {len(reduced_gaps)} 个减小的缺口")
        
        self.connection.commit()
        self.logger.info("QA: 现有缺口更新完成。")

    def detect_new_gaps(self, trade_date: str, csv_path: str, debug: bool = False) -> None:
        """检测新的缺口"""
        # 读取股票代码
        stock_codes_df = pd.read_csv(csv_path, encoding="utf-8")
        stock_codes = [str(code).zfill(6) for code in stock_codes_df.iloc[:, 1]]
        
        if debug:
            self.logger.info(f"QA: 读取到 {len(stock_codes)} 个股票代码")
        
        # 批处理大小
        batch_size =1500
        new_gaps = []
        total_batches = (len(stock_codes) + batch_size - 1) // batch_size
        
        # 分批处理股票
        for batch_num in range(total_batches):
            start_idx = batch_num * batch_size
            end_idx = min((batch_num + 1) * batch_size, len(stock_codes))
            batch_codes = stock_codes[start_idx:end_idx]
            
            self.logger.debug(f"处理批次 {batch_num + 1}/{total_batches} ({start_idx + 1}-{end_idx})")
            print(f"处理批次 {batch_num + 1}/{total_batches} ({start_idx + 1}-{end_idx})")
            
            # 批量获取上一交易日数据
            prev_query = f"""
            SELECT t1.id, t1.date as prev_date, t1.low as previous_low
            FROM {self.main_query_table} t1
            INNER JOIN (
                SELECT id, MAX(date) as max_date
                FROM {self.main_query_table}
                WHERE date < '{trade_date}'
                AND Latest = 1
                AND id IN ({','.join(f"'{code}'" for code in batch_codes)})
                GROUP BY id
            ) t2 ON t1.id = t2.id AND t1.date = t2.max_date
            WHERE t1.Latest = 1
            """
            prev_df = pd.read_sql(prev_query, self.engine)
            
            # 批量获取当日数据
            current_query = f"""
            SELECT id, high as current_high
            FROM {self.main_query_table}
            WHERE id IN ({','.join(f"'{code}'" for code in batch_codes)})
            AND date = '{trade_date}'
            AND Latest = 1
            """
            current_df = pd.read_sql(current_query, self.engine)
            
            # 合并数据并检查缺口
            if not prev_df.empty and not current_df.empty:
                merged_df = pd.merge(prev_df, current_df, on='id')
                gaps = merged_df[merged_df['previous_low'] > merged_df['current_high']]
                
                if not gaps.empty:
                    batch_gaps = gaps.apply(
                        lambda row: {
                            'id': row['id'],
                            'sdate': row['prev_date'],
                            'from_price': row['previous_low'],
                            'to_price': row['current_high']
                        }, axis=1
                    ).tolist()
                    new_gaps.extend(batch_gaps)
            
            if debug:
                self.logger.debug(f"QA: 本批次处理了 {len(batch_codes)} 只股票")
                self.logger.debug(f"QA: 找到 {len(batch_gaps) if 'batch_gaps' in locals() else 0} 个新缺口")
        
        # 在插入新缺口之前，先获取已存在的缺口
        if new_gaps:
            existing_gaps_query = f"""
            SELECT id, sdate
            FROM {self.gap_table}
            WHERE (id, sdate) IN (
                {','.join(f"('{gap['id']}', '{gap['sdate']}')" for gap in new_gaps)}
            )
            """
            existing_gaps_df = pd.read_sql(existing_gaps_query, self.engine)
            
            # 创建已存在缺口的集合，用于快速查找
            existing_gaps_set = {(row['id'], row['sdate']) for _, row in existing_gaps_df.iterrows()}
            
            # 过滤掉已存在的缺口
            unique_gaps = [
                gap for gap in new_gaps 
                if (gap['id'], gap['sdate']) not in existing_gaps_set
            ]
            
            if unique_gaps:
                with self.connection.cursor() as cursor:
                    cursor.executemany(
                        f"""INSERT INTO `{self.gap_table}` 
                        (id, sdate, filled, from_price, to_price, gap_update_time)
                        VALUES (%s, %s, 0, %s, %s, NOW())""",
                        [(gap['id'], gap['sdate'], gap['from_price'], gap['to_price']) 
                         for gap in unique_gaps]
                    )
                self.connection.commit()
                
                self.logger.info_print(f"\nQA: 共发现 {len(new_gaps)} 个缺口.")
                if debug:
                    for gap in unique_gaps:
                        self.logger.debug(f"股票: {gap['id']}, 开始日期: {gap['sdate']}, "
                              f"从 {gap['from_price']} 到 {gap['to_price']}")
            else:
                self.logger.info_print(f"\nQA: 发现 {len(new_gaps)} 个缺口，但都已存在于数据库中")
        else:
            self.logger.info_print("\nQA: 没有发现新的缺口。")

def setup_environment(env: str) -> Tuple[Dict[str, Any], str, Any]:
    """设置运行环境并返回必要的配置和日志记录器
    
    Args:
        env: 运行环境（"QA" 或 "PROD"）
    
    Returns:
        Tuple[配置字典, 根目录路径, 日志记录器]
    """
    config_path_QA, config_path_PROD, root_dir = find_config_path()
    config_path = config_path_QA if env == "QA" else config_path_PROD
    config = load_config(config_path)
    logger = set_log(config, "QA007.log", prefix="QA")
    return config, root_dir, logger

def run_gap_detection(env: str, trade_date: str, run_update: bool = True, 
                     run_detect: bool = True, debug: bool = False) -> None:
    """运行缺口检测程序
    
    Args:
        env: 运行环境
        trade_date: 交易日期
        run_update: 是否更新现有缺口
        run_detect: 是否检测新缺口
        debug: 是否开启调试模式
    """
    config, root_dir, logger = setup_environment(env)
    logger.info_print(f"开始运行缺口检测程序 - 环境: {env}, 交易日期: {trade_date}")
    
    connection = None
    engine = None
    try:
        connection = db_con_pymysql(config)
        engine = db_con_sqlalchemy(config)
        
        gap_manager = GapManager(env, logger, connection, engine, config)
        
        if run_update:
            logger.info_print("开始更新现有缺口...")
            gap_manager.update_existing_gaps(trade_date, debug)
        
        if run_detect:
            logger.info_print("开始检测新缺口...")
            csv_path = os.path.join(root_dir, env, config["CSVs"]["MainCSV"])
            gap_manager.detect_new_gaps(trade_date, csv_path, debug)
            
    except Exception as e:
        logger.error_print(f"程序运行出错: {str(e)}")
        raise
    finally:
        if connection:
            connection.close()
        if engine:
            engine.dispose()
        logger.info_print("程序运行结束\n")

def main():
    """
    程序执行入口
    
    功能：
    1. 更新现有缺口状态
    2. 检测新的缺口
    
    Returns:
        bool: 执行成功返回True，失败返回False
    """
    env = "QA"
    config, _, logger = setup_environment(env)
    
    # 从配置文件获取 debug 设置
    debug_mode = config.get("Programs", {}).get("QA007", {}).get("DEBUG", False)
    
    # 获取处理日期
    is_workday, trade_date = is_today_workday(logger)
    
    try:
        run_gap_detection(
            env=env,
            trade_date=trade_date,
            run_update=True,
            run_detect=True,
            debug=debug_mode
        )
        return True  # 添加明确的成功返回值
    except Exception as e:
        logger.error_print(f"QA: 程序执行失败: {str(e)}")
        return False  # 添加明确的失败返回值

if __name__ == "__main__":
    main()