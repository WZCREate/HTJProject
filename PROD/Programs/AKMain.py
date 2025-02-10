'''
本程序顺序执行 AK001~AK006
当且仅当上一个程序执行成功时，才会执行下一个程序
任何错误都会被记录并导致程序终止
'''

import os
from datetime import datetime
from AK001 import main as ak001_main
from AK002 import main as ak002_main
from AK003 import main as ak003_main
from AK004 import main as ak004_main
from AK005 import main as ak005_main
from AK006 import main as ak006_main
from AK007 import main as ak007_main
from AK008 import main as ak008_main
from AKFilter1 import main as akfilter1_main
from AKFilter2 import main as akfilter2_main
from AKFilter3 import main as akfilter3_main
from CommonFunc.DBconnection import find_config_path, load_config, set_log

def execute_ak_sequence():
    """按顺序执行AK001~AK006程序"""
    ak_functions = [
        (ak001_main, "AK001"), # 获取最新的股票代码列表
        (ak002_main, "AK002"), # 判断日期,创建新数据表,或者进行批量请求
        (ak003_main, "AK003"), # 获取最新数据, 并写入到日表中
        (ak004_main, "AK004"), # 日表 --> 年表
        (ak005_main, "AK005"), # 更新 Latest 标识符
        (ak006_main, "AK006"), # 计算MA
        (ak007_main, "AK007"),  # 更新缺口数据
        (ak008_main, "AK008")  # 更新周K数据
    ]
    
    for func, name in ak_functions:
        try:
            logger.info_print(f"PROD: 开始执行 {name}")
            result = func()
            
            if result is None or result is False:
                error_msg = f"PROD: {name} 执行失败"
                logger.error_print(error_msg)
                return False
                
            logger.info_print(f"PROD: {name} 执行成功")
            
        except Exception as e:
            error_msg = f"PROD: {name} 执行过程中发生错误: {str(e)}"
            logger.error_print(error_msg)
            return False
    return True

def excute_filters():
    """执行过滤程序"""
    filters = [
        (akfilter1_main, "AKFilter1"),
        (akfilter2_main, "AKFilter2"),
        (akfilter3_main, "AKFilter3")
    ]
    for func, name in filters:
        try:
            logger.info_print(f"PROD: 开始执行 {name}")
            result = func()
            logger.info_print(f"PROD: {name} 执行成功")

            if result is None or result is False:
                error_msg = f"PROD: {name} 执行失败"
                logger.error_print(error_msg)
                return False
            
            logger.info_print(f"PROD: {name} 执行成功")
        except Exception as e:
            logger.error_print(f"PROD: {name} 执行过程中发生错误: {str(e)}")
            return False
    return True

def main():
    """主函数"""
    # 获取配置文件路径并设置日志
    _, config_path_PROD, _ = find_config_path()
    config = load_config(config_path_PROD)
    global logger
    logger = set_log(config, "AK_main.log", prefix="PROD")
    
    logger.info_print("PROD: 开始执行 AK 程序序列")
    
    try:
        # 执行AK序列
        success_ak_sequence = execute_ak_sequence()
        if success_ak_sequence:
            logger.info_print("PROD: 所有 AK 程序执行完成")
        else:
            logger.error_print("PROD: AK 程序序列执行中断")
            # 不要在这里直接返回，继续执行过滤程序
        
        # 无论AK序列是否成功，都执行过滤程序
        logger.info_print("PROD: 开始执行过滤程序")
        success_filters = excute_filters()
        if success_filters:
            logger.info_print("PROD: 所有过滤程序执行完成")
        else:
            logger.error_print("PROD: 过滤程序执行中断")
        
        # 只有当两个程序都成功时才返回True
        return success_ak_sequence and success_filters
    
    except Exception as e:
        logger.error_print(f"PROD: 执行过程中发生未预期的错误: {str(e)}")
        return False

if __name__ == "__main__":
    main() 