'''
本程序使用 stock_zh_a_spot_em 查询当日市面上所有股票
输出最新的 StkList_AK.csv 同时将上一份备份
但是不写入数据库
然后调用 SubQA001 函数进行初次过滤
'''

import akshare as ak
import pandas as pd
import os
import time
from CommonFunc.DBconnection import find_config_path
from CommonFunc.DBconnection import load_config
from CommonFunc.DBconnection import set_log
from QA.SubFunc.SubQA001 import main as FirstFilter
from datetime import datetime

def backup_existing_file(file_path, logger):
    """Backup existing stock list file if it exists."""
    if os.path.exists(file_path):
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        dir_path = os.path.dirname(file_path)
        backup_file = os.path.join(dir_path, f'StkList_{timestamp}.csv')
        os.rename(file_path, backup_file)
        logger.info(f"已将 {os.path.basename(file_path)} 备份为 {os.path.basename(backup_file)}")
        print(f"已将 {os.path.basename(file_path)} 备份为 {os.path.basename(backup_file)}")
        return backup_file
    logger.info(f"{os.path.basename(file_path)} 不存在，无需备份")
    print(f"{file_path} 不存在，无需备份")
    return None

def fetch_and_save_stock_list(file_path, logger):
    """Fetch current stock list and save to CSV."""
    stock_list_df = ak.stock_zh_a_spot_em()
    logger.info(f"获取到的股票数量：{len(stock_list_df)}")
    stock_list_df.to_csv(file_path, index=False)
    logger.info(f"最新的股票列表已保存到 {os.path.basename(file_path)} 文件中")
    return stock_list_df

def compare_stock_lists(new_file, backup_file, logger):
    """Compare new and old stock lists to identify changes."""
    if not backup_file:
        logger.info("未找到备份文件，因此无法进行比对。")
        print("未找到备份文件，因此无法进行比对。")
        return
        
    backup_df = pd.read_csv(backup_file)
    new_df = pd.read_csv(new_file)
    
    backup_codes = set(backup_df.iloc[:, 1].astype(str))
    new_codes = set(new_df.iloc[:, 1].astype(str))
    
    # Check for delisted stocks
    delisted_codes = backup_codes - new_codes
    if delisted_codes:
        logger.info("可能已经退市：")
        print("可能已经退市：")
        logger.info(", ".join(list(delisted_codes)))
        print(", ".join(list(delisted_codes)))
    else:
        logger.info("没有发现可能退市的股票代码。")
        
    # Check for newly listed stocks
    newly_listed_codes = new_codes - backup_codes
    if newly_listed_codes:
        logger.info("可能是新上市的股票：")
        print("可能是新上市的股票：")
        logger.info(", ".join(list(newly_listed_codes)))
        print(", ".join(list(newly_listed_codes)))
    else:
        logger.info("没有发现可能新上市的股票代码。")

def run_first_filter(logger):
    """Execute the first filter operation."""
    try:
        output_file = FirstFilter()
        logger.info(f"SubQA001 脚本已成功运行，初次过滤: {os.path.basename(output_file)}已生成。")
        print(f"SubQA001 脚本已成功运行，初次过滤: {os.path.basename(output_file)}已生成。")
        return output_file
    except Exception as e:
        logger.error(f"运行 SubQA001 脚本时出错：{e}")
        print(f"运行 SubQA001 脚本时出错：{e}")
        raise

def update_stock_list(config_path, qa_dir, logger):
    """Execute the complete stock list update process."""
    try:
        config = load_config(config_path)
        csv_config = config["CSVs"]
        
        # 修改文件路径获取方式
        file_path = os.path.join(qa_dir, csv_config["MainCSV"])
        
        backup_file = backup_existing_file(file_path, logger)
        fetch_and_save_stock_list(file_path, logger)
        compare_stock_lists(file_path, backup_file, logger)
        
        # 修改为使用Filters下的Input路径
        output_file = os.path.join(qa_dir, csv_config["Filters"]["Input"])
        stock_list_df = pd.read_csv(file_path)
        stock_list_df.to_csv(output_file, index=False)
        logger.info(f"已将股票列表保存到 {os.path.basename(output_file)} 文件中")
        
        return output_file
    except Exception as e:
        logger.error(f"更新股票列表时出错：{e}")
        raise

def manage_backup_files(dir_path, logger):
    """管理备份文件，只保留每天最后一份，最多保留三天"""
    # 获取所有StkList_开头的csv文件
    backup_files = [f for f in os.listdir(dir_path) if f.startswith('StkList_') and f.endswith('.csv')]
    if not backup_files:
        return
    
    # 解析文件名中的日期和时间
    file_dates = {}
    for file in backup_files:
        try:
            # 从文件名中提取日期时间（格式：StkList_20240301_123456.csv）
            date_str = file.split('_')[1]
            time_str = file.split('_')[2].replace('.csv', '')
            datetime_str = f"{date_str}_{time_str}"
            file_datetime = datetime.strptime(datetime_str, "%Y%m%d_%H%M%S")
            file_dates[file] = file_datetime
        except (IndexError, ValueError):
            logger.warning(f"无法解析文件名日期: {file}")
            continue
    
    # 按日期分组
    files_by_date = {}
    for file, dt in file_dates.items():
        date_key = dt.date()
        if date_key not in files_by_date:
            files_by_date[date_key] = []
        files_by_date[date_key].append((file, dt))
    
    # 获取排序后的日期列表
    sorted_dates = sorted(files_by_date.keys(), reverse=True)
    
    # 处理每个日期的文件
    for date in sorted_dates:
        # 按时间排序该日期的文件
        day_files = sorted(files_by_date[date], key=lambda x: x[1])
        
        # 如果这个日期超过3天，删除所有文件
        days_old = (datetime.now().date() - date).days
        if days_old > 3:
            for file, _ in day_files:
                file_path = os.path.join(dir_path, file)
                try:
                    os.remove(file_path)
                    logger.info(f"删除过期备份文件: {file}")
                except Exception as e:
                    logger.error(f"删除文件失败 {file}: {str(e)}")
            continue
        
        # 保留最后一个文件，删除其他文件
        for file, _ in day_files[:-1]:
            file_path = os.path.join(dir_path, file)
            try:
                os.remove(file_path)
                logger.info(f"删除重复备份文件: {file}")
            except Exception as e:
                logger.error(f"删除文件失败 {file}: {str(e)}")

def main():
    """Main function to run the stock list update process."""
    config_path, _, root_dir = find_config_path()
    qa_dir = os.path.dirname(config_path)
    config = load_config(config_path)
    logger = set_log(config, "QA001.log", prefix="QA")
    
    try:
        # 管理备份文件
        csv_dir = os.path.join(qa_dir, "CSVs")
        logger.info("开始管理备份文件...")
        manage_backup_files(csv_dir, logger)
        logger.info("备份文件管理完成")
        
        # 更新股票列表
        output_file = update_stock_list(config_path, qa_dir, logger)
        logger.info("股票列表更新完成")
        
        # 运行初次过滤
        logger.info("开始执行初次过滤...")
        filtered_file = run_first_filter(logger)
        logger.info(f"初次过滤完成，结果保存在: {os.path.basename(filtered_file)}")
        
        logger.info("QA001全部处理完成")
        return filtered_file
    except Exception as e:
        logger.error(f"处理过程中出现错误：{e}")
        return None

if __name__ == "__main__":
    main()