import pandas as pd

def compare_csv_files(file1_path, file2_path):
    """
    比较两个CSV文件的差异
    
    Args:
        file1_path: 第一个CSV文件路径
        file2_path: 第二个CSV文件路径
    """
    # 读取两个CSV文件
    df1 = pd.read_csv(file1_path)
    df2 = pd.read_csv(file2_path)
    
    # 转换股票代码为字符串格式，确保格式一致
    df1['Stock Code'] = df1['Stock Code'].astype(str).str.zfill(6)
    df2['Stock Code'] = df2['Stock Code'].astype(str).str.zfill(6)
    
    # 转换为集合以便比较
    set1 = set(df1['Stock Code'])
    set2 = set(df2['Stock Code'])
    
    # 计算差异
    only_in_file1 = sorted(set1 - set2)
    only_in_file2 = sorted(set2 - set1)
    
    # 打印结果
    print(f"文件1: {file1_path}")
    print(f"文件2: {file2_path}")
    print(f"\n文件1总数: {len(set1)}")
    print(f"文件2总数: {len(set2)}")
    
    print(f"\n仅在文件1中的股票 ({len(only_in_file1)}):")
    for code in only_in_file1:
        print(code)
    
    print(f"\n仅在文件2中的股票 ({len(only_in_file2)}):")
    for code in only_in_file2:
        print(code)

if __name__ == "__main__":
    file1 = "PROD/CSVs/Filter1Out.csv"
    file2 = "QA/CSVs/Filter1Out.csv"
    compare_csv_files(file1, file2)