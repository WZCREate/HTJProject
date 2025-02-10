'''
本程序计算MA并写入配置文件中 ma_table 指向的数据表
'''
import os
import pandas as pd
from sqlalchemy.sql import text
from CommonFunc.DBconnection import (
    load_config,
    db_con_sqlalchemy,
    set_log,
    find_config_path
)

def read_target_stock_codes(csv_file, root_dir):
    """从CSV文件中读取目标股票代码"""
    # 构建到 QA/CSVs 的路径
    qa_dir = os.path.join(root_dir, "QA")
    full_path = os.path.join(qa_dir, csv_file)
    
    if not os.path.exists(full_path):
        logger.error_print(f"文件不存在: {full_path}")
        raise FileNotFoundError(f"文件不存在: {full_path}")
    
    stock_codes = pd.read_csv(full_path, usecols=[1], dtype=str).iloc[:, 0].tolist()
    logger.info_print(f"成功读取到 {len(stock_codes)} 支股票代码。")
    return stock_codes

def fetch_stock_data(engine, source_table, stock_codes):
    """批量从数据库中提取股票数据，按时间降序排列"""
    query = f"""
    SELECT id, date, close_price 
    FROM {source_table}
    WHERE id IN :stock_codes AND Latest = 1 AND high IS NOT NULL
    ORDER BY id, date DESC;
    """
    with engine.connect() as connection:
        data = pd.read_sql_query(
            sql=text(query),
            con=connection,
            params={'stock_codes': tuple(stock_codes)}
        )
    logger.info_print(f"返回数据行数：{len(data)}")
    return data

def calculate_ma(data, ma_days):
    """使用Pandas计算MA值"""
    results = []
    for stock_code, group in data.groupby('id'):
        group = group.sort_values('date')  # 确保按时间升序排列
        for ma in ma_days:
            group[f'MA{ma}'] = group['close_price'].rolling(window=ma).mean()
        # 提取最新日期的 MA 值
        latest_row = group.iloc[-1]
        results.append({
            'date': latest_row['date'],
            'id': stock_code,
            **{f'MA{ma}': latest_row[f'MA{ma}'] for ma in ma_days}
        })
    return pd.DataFrame(results)

def insert_results_to_db(engine, ma_table, results):
    """将结果插入到数据库"""
    from sqlalchemy.types import Date, String, Float
    dtype_mapping = {
        'date': Date,
        'id': String(10),
        **{f'MA{ma}': Float for ma in range(1, 6)}
    }
    results.to_sql(ma_table, engine, if_exists='append', index=False, method='multi', dtype=dtype_mapping)
    logger.info_print(f"插入完成，共插入 {len(results)} 条记录。")

def clear_ma_table(engine, ma_table):
    """清空目标表"""
    logger.info_print(f"清空目标表 {ma_table}...")
    with engine.connect() as connection:
        connection.execute(text(f"TRUNCATE TABLE {ma_table}"))
    logger.info_print(f"目标表 {ma_table} 已清空。开始计算MA")
 
def main():
    """
    主函数作为统一的程序入口点，集中管理所有配置参数
    Returns:
        bool: 成功返回 True，失败返回 False
    """
    try:
        # 获取配置文件路径并加载配置
        config_path, _, root_dir = find_config_path()
        config = load_config(config_path)
        
        # 设置日志
        global logger
        logger = set_log(config, "QA006.log", prefix="QA")
        
        # 获取配置信息
        ma_config = config['MA_config']
        csv_path = ma_config['ma_source_csv']  # 这里的路径应该是相对于 QA/CSVs 的路径
        ma_table = ma_config['ma_table']
        ma_days = [ma_config[f'ma{i}'] for i in range(1, 6)]
        batch_size = ma_config['ma_batch_size']

        # 连接数据库
        engine = db_con_sqlalchemy(config)
        
        try:
            # 读取目标股票代码
            stock_codes = read_target_stock_codes(csv_path, root_dir)
            
            # 清空目标表
            clear_ma_table(engine, ma_table)

            # 分批处理
            for i in range(0, len(stock_codes), batch_size):
                batch = stock_codes[i:i + batch_size]
                logger.info_print(f"开始处理第 {i // batch_size + 1} 批，共 {len(batch)} 支股票。")

                data = fetch_stock_data(engine, config['DB_tables']['main_query_table'], batch)

                if data.empty:
                    logger.info_print(f"第 {i // batch_size + 1} 批没有有效数据，跳过。")
                    continue

                ma_results = calculate_ma(data, ma_days)
                insert_results_to_db(engine, ma_table, ma_results)
                logger.info_print(f"第 {i // batch_size + 1} 批处理完成，已插入结果。")

            return True
        except Exception as e:
            logger.error_print(f"处理过程中出现错误: {str(e)}")
            return False
        finally:
            engine.dispose()
    except Exception as e:
        logger.error_print(f"配置文件读取错误: {str(e)}")
        return False

if __name__ == "__main__":
    main()