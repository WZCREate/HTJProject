'''
本程序将配置文件中 table_to_update_flag 指向的数据表中的 Latest更新
'''

import time
from datetime import datetime, timedelta
from CommonFunc.DBconnection import (
    load_config,
    db_con_pymysql,
    set_log,
    find_config_path
)

def update_latest_flag(table, config):
    '''更新 Latest 列，只更新最近5天的数据'''
    connection = db_con_pymysql(config)
    try:
        with connection.cursor() as cursor:
            # 获取5天前的日期
            date_limit = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
            
            # 将5天内的 Latest 列初始化为 0
            cursor.execute(f"UPDATE {table} SET Latest = 0 WHERE date >= '{date_limit}';")
            
            # 更新最5天每个日期和股票代码的最新行为 Latest = 1
            cursor.execute(f"""
                UPDATE {table} a
                JOIN (
                    SELECT date, id, MAX(Insrt_time) AS max_insrt_time
                    FROM {table}
                    WHERE date >= '{date_limit}'
                    GROUP BY date, id
                ) AS latest_records ON a.date = latest_records.date 
                                   AND a.id = latest_records.id 
                                   AND a.Insrt_time = latest_records.max_insrt_time
                SET a.Latest = 1
                WHERE a.date >= '{date_limit}';
            """)
        connection.commit()
        logger.info_print(f"{config['DBConnection']['database']}.{table} 中，最近5天内的 'Latest' 标识符更新完成。")
        return True
    except Exception as e:
        logger.error_print(f"更新过程中出现错误: {str(e)}")
        return False
    finally:
        connection.close()

def main():
    """
    主函数作为统一的程序入口点，集中管理所有配置参数
    Returns:
        bool: 成功返回 True，失败返回 False
    """
    try:
        start_time = time.time()
        
        # 获取配置文件路径并加载配置
        config_path, _, _ = find_config_path()
        config = load_config(config_path)
        
        # 设置日志
        global logger
        logger = set_log(config, "QA005.log", prefix="QA")
        
        # 获取需要更新的表名
        table_name = config["DB_tables"]["table_to_update_flag"]
        
        # 执行更新操作
        success = update_latest_flag(table_name, config)
        
        end_time = time.time()
        if success:
            logger.info_print(f"执行时间：{end_time - start_time:.2f} 秒")
        return success
    except Exception as e:
        logger.error_print(f"处理过程中出现错误：{e}")
        return False

if __name__ == "__main__":
    main()