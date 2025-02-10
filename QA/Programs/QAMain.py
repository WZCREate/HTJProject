'''
本程序顺序执行 QA001~QA006
当且仅当上一个程序执行成功时，才会执行下一个程序
任何错误都会被记录并导致程序终止
'''

import os
from datetime import datetime
from QA001 import main as qa001_main
from QA002 import main as qa002_main
from QA003 import main as qa003_main
from QA004 import main as qa004_main
from QA005 import main as qa005_main
from QA006 import main as qa006_main
from QA007 import main as qa007_main
from QA008 import main as qa008_main
from QAFilter1 import main as qafilter1_main
from QAFilter2 import main as qafilter2_main
from QAFilter3 import main as qafilter3_main
from CommonFunc.DBconnection import find_config_path, load_config, set_log

def execute_qa_sequence():
    """按顺序执行QA001~QA006程序"""
    qa_functions = [
        (qa001_main, "QA001"), # 获取最新的股票代码列表
        (qa002_main, "QA002"), # 判断日期,创建新数据表,或者进行批量请求
        (qa003_main, "QA003"), # 获取最新数据, 并写入到日表中
        (qa004_main, "QA004"), # 日表 --> 年表
        (qa005_main, "QA005"), # 更新 Latest 标识符
        (qa006_main, "QA006"), # 计算MA
        (qa007_main, "QA007"),  # 更新缺口数据
        (qa008_main, "QA008")  # 更新周K数据
    ]
    
    for func, name in qa_functions:
        try:
            logger.info_print(f"开始执行 {name}")
            result = func()
            
            if result is None or result is False:
                error_msg = f"{name} 执行失败"
                logger.error_print(error_msg)
                return False
                
            logger.info_print(f"{name} 执行成功")
            
        except Exception as e:
            error_msg = f"{name} 执行过程中发生错误: {str(e)}"
            logger.error_print(error_msg)
            return False
    
    return True

def excute_filters():
    """执行过滤程序"""
    filters = [
        (qafilter1_main, "QAFilter1"),
        (qafilter2_main, "QAFilter2"),
        (qafilter3_main, "QAFilter3")
    ]
    for func, name in filters:
        try:
            logger.info_print(f"QA: 开始执行 {name}")
            result = func()
            logger.info_print(f"QA: {name} 执行成功")

            if result is None or result is False:
                error_msg = f"QA: {name} 执行失败"
                logger.error_print(error_msg)
                return False
            
            logger.info_print(f"QA: {name} 执行成功")
        except Exception as e:
            logger.error_print(f"QA: {name} 执行过程中发生错误: {str(e)}")
            return False
    return True

def main():
    """主函数"""
    # 获取配置文件路径并设置日志
    config_path_QA, _, _ = find_config_path()
    config = load_config(config_path_QA)
    global logger
    logger = set_log(config, "QA_main.log", prefix="QA")
    
    logger.info_print("开始执行 QA 程序序列")
    
    try:
        # 执行QA序列
        success_qa_sequence = execute_qa_sequence()
        if success_qa_sequence:
            logger.info_print("所有 QA 程序执行完成")
        else:
            logger.error_print("QA 程序序列执行中断")
            # 不要在这里直接返回，继续执行过滤程序
        
        # 无论QA序列是否成功，都执行过滤程序
        logger.info_print("开始执行过滤程序")
        success_filters = excute_filters()
        if success_filters:
            logger.info_print("QA: 所有过滤程序执行完成")
        else:
            logger.error_print("QA: 过滤程序执行中断")
        
        # 只有当两个程序都成功时才返回True
        return success_qa_sequence and success_filters
    
    except Exception as e:
        logger.error_print(f"QA: 执行过程中发生未预期的错误: {str(e)}")
        return False

if __name__ == "__main__":
    main()
