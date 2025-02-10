'''
本函数是初次过滤器,进行基础过滤 (Filter0)
'''
import pandas as pd
import os
from CommonFunc.DBconnection import (
    load_config,
    set_log,
    find_config_path,
    db_con_pymysql
)

# 筛选股票的具体条件
def criteria(df, stock_code_column):
    df = df[~df['名称'].str.contains('ST', na=False)]
    df = df[~df['名称'].str.contains('PT', na=False)]
    df = df[~df['名称'].str.contains('退', na=False)]
    df = df[~df[stock_code_column].str.startswith(('4', '8'))]
    df[stock_code_column] = df[stock_code_column].str.zfill(6)
    if '涨跌幅' in df.columns:
        df['涨跌幅'] = pd.to_numeric(df['涨跌幅'], errors='coerce')  # 确保是数值类型
        df = df[df['涨跌幅'] < 10]
    else:
        raise ValueError("输入文件中未找到 '涨跌幅' 列，请检查文件格式。")
    return df

# 筛选股票并保存到目标文件
def filter_stocks(input_file, output_file, logger):
    try:
        # 直接读取文件，同时指定股票代码列为字符串类型
        df = pd.read_csv(input_file)
        
        # 检查第二列是否存在
        if len(df.columns) < 2:
            error_msg = "输入文件中第二列不存在，请检查文件格式"
            logger.error(error_msg)
            raise ValueError(error_msg)
            
        # 获取第二列列名（股票代码列）
        stock_code_column = df.columns[1]
        
        # 将股票代码列转换为字符串类型
        df[stock_code_column] = df[stock_code_column].astype(str)
        
        # 检查必要的列是否存在
        if '名称' not in df.columns:
            raise ValueError("输入文件中未找到 '名称' 列，请检查文件格式。")
            
        # 筛选股票
        filtered_df = criteria(df, stock_code_column)
        logger.info(f"筛选后行数：{len(filtered_df)}")
        
        # 创建新的DataFrame，只包含需要的列
        output_df = pd.DataFrame({
            'Index': range(1, len(filtered_df) + 1),
            'Stock Code': filtered_df[stock_code_column]
        })
        
        # 将结果写入新CSV文件
        output_df.to_csv(output_file, index=False)
        
        # 确保文件系统刷新
        os.sync()
        
    except Exception as e:
        logger.error(f"筛选过程失败：{str(e)}")
        raise

def save_filter_result(cursor, config, filter_name, input_count, output_count, 
                      logger, details, source_file=None, output_file=None):
    """保存过滤结果到数据库"""
    try:
        reduction = input_count - output_count
        reduction_rate = (reduction / input_count * 100) if input_count > 0 else 0
        
        # 检查是否有 source_file 和 output_file
        if source_file is None or output_file is None:
            logger.warning_print("警告：未提供源文件或输出文件名")
        
        # 打印调试信息
        logger.debug(f"保存过滤结果：")
        logger.debug(f"filter_name: {filter_name}")
        logger.debug(f"source_file: {source_file}")
        logger.debug(f"output_file: {output_file}")
        logger.debug(f"input_count: {input_count}")
        logger.debug(f"output_count: {output_count}")
        logger.debug(f"reduction: {reduction}")
        logger.debug(f"reduction_rate: {reduction_rate}")
        logger.debug(f"details: {details}")
        
        query = """
        INSERT INTO filter_history 
        (filter_name, source_file, output_file, input_count, output_count, 
         reduction, reduction_rate, run_date, details)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s)
        """
        
        cursor.execute(
            query, 
            (filter_name, source_file, output_file, input_count, output_count, 
             reduction, reduction_rate, details)
        )
        
        logger.info_print(f"过滤信息已保存到数据库")
        return True
        
    except Exception as e:
        logger.error_print(f"保存过滤结果到数据库失败: {str(e)}")
        return False

def main():
    # 加载配置
    config_path, _, root_dir = find_config_path()
    config = load_config(config_path)
    logger = set_log(config, "QA001.log", prefix="QA")
    logger.info("开始执行 SubQA001 函数")
    csv_config = config["CSVs"]
    
    # 修改输入输出文件路径的获取方式
    input_file = f"{root_dir}/QA/{csv_config['MainCSV']}"
    output_file = f"{root_dir}/QA/{csv_config['Filters']['Input']}"
    
    logger.info(f"输入文件路径: {os.path.basename(input_file)}")
    logger.info(f"输出文件路径: {os.path.basename(output_file)}")

    # 确保目标目录存在
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # 验证文件路径
    if not os.path.exists(input_file):
        error_msg = f"输入文件不存在: {input_file}"
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)
    
    # 验证输出目录是否可写
    output_dir = os.path.dirname(output_file)
    if not os.access(output_dir, os.W_OK):
        error_msg = f"输出目录无写入权限: {output_dir}"
        logger.error(error_msg)
        raise PermissionError(error_msg)

    try:
        # 连接数据库
        connection = db_con_pymysql(config)
        cursor = connection.cursor()

        # 读取输入文件获取初始股票数量
        input_df = pd.read_csv(input_file)
        input_count = len(input_df)
        logger.info(f"输入股票数量: {input_count}")

        # 执行过滤
        filter_stocks(input_file, output_file, logger)
        
        # 获取过滤后的数据用于计数
        filtered_df = pd.read_csv(output_file)
        output_count = len(filtered_df)
        logger.info(f"过滤后股票数量: {output_count}")

        # 保存过滤结果到数据库
        details = "初次过滤：剔除ST、PT、退市股票及代码以4、8开头的股票，涨跌幅超过10%的股票"
        source_file = os.path.basename(input_file)
        output_file = os.path.basename(output_file)
        save_filter_result(cursor, config, "Filter0", input_count, output_count, logger, details, source_file, output_file)
        connection.commit()
        logger.info("过滤结果已记录到数据库")

        return output_file

    except Exception as e:
        logger.error(f"筛选过程失败：{str(e)}")
        raise

    finally:
        if 'connection' in locals() and connection:
            connection.close()

if __name__ == "__main__":
    try:
        output_file = main()
        print(f"QA:初次过滤完成, 已写入 {output_file}.")
    except Exception as e:
        print(f"处理失败：{str(e)}")