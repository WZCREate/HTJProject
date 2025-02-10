'''
本程序将数据库中的日表数据写入年表
'''
from CommonFunc.DBconnection import (
    load_config,
    db_con_pymysql,
    set_log,
    find_config_path
)

def transfer_day_to_main_table(logger):
    '''将数据库中的日表数据写入年表'''
    # 读取配置文件
    _, config_path_PROD, _ = find_config_path()  # 修改配置文件路径获取
    config = load_config(config_path_PROD)  # 使用PROD配置
    
    # 获取配置信息
    main_table = config["DB_tables"]["main_query_table"]
    daily_table = config["DB_tables"]["daily_update_table"]
    last_update_date = config["DBinput"]["last_update_date"]
    
    # 检查日期和表名是否一致
    month_map = {
        'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04', 
        'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
        'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
    }
    
    # 从 last_update_date 提取年份 (例如从 "2025-01-03" 提取 "2025")
    year = last_update_date.split('-')[0]
    
    # 从 daily_table 提取月份和日期 (例如从 "Jan03" 提取 "Jan" 和 "03")
    month = ''.join([c for c in daily_table if c.isalpha()])
    day = ''.join([c for c in daily_table if c.isdigit()])
    
    # 构建日期字符串
    table_date = f"{year}-{month_map[month]}-{day.zfill(2)}"
    
    if table_date != last_update_date:
        error_message = f"PROD: 配置不一致: daily_update_table={daily_table}, last_update_date={last_update_date}"
        logger.error(error_message)
        print(error_message)
        return False

    # 构建SQL插入语句
    insert_query = f"""
    INSERT INTO {main_table}(
        date,
        id,
        open_price,
        close_price,
        high,
        low,
        volume,
        turnover,
        amplitude,
        chg_percen,
        chg_amount,
        turnover_rate,
        Insrt_time
    )
    SELECT 
        '{last_update_date}',             
        Id,
        opentoday,
        newprice,
        high,
        low,
        volume,
        turnover,
        amplitude,
        chg_percen,
        chg_amount,
        turnover_rate,
        NOW()                  
    FROM 
        {daily_table};
    """

    # 连接数据库并执行查询
    connection = db_con_pymysql(config)
    try:
        with connection.cursor() as cursor:
            cursor.execute(insert_query)
            connection.commit()
            log_message = f"PROD: 数据已成功从 {daily_table} --> {main_table}"  # 修改日志前缀
            logger.info(log_message)
            print(log_message)
            return True
    except Exception as e:
        error_message = f"PROD: 数据转移过程中出错: {str(e)}"  # 修改错误信息前缀
        logger.error(error_message)
        print(error_message)
        return False
    finally:
        connection.close()

def main():
    """
    Day table to Main table 主函数
    Returns:
        bool: 成功返回 True，失败返回 False
    """
    try:
        _, config_path_PROD, _ = find_config_path()  # 修改配置文件路径获取
        config = load_config(config_path_PROD)  # 使用PROD配置
        logger = set_log(config, "AK004.log", prefix="PROD")  # 修改日志文件名
        success = transfer_day_to_main_table(logger)
        return success
    except Exception as e:
        print(f"PROD: 处理过程中出现错误：{e}")  # 修改错误信息前缀
        return False

if __name__ == "__main__":
    main()