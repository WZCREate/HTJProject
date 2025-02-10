import re
from pathlib import Path

def shrink_logs(log_file):
    """
    清理日志文件,只保留包含"存在阻力线的股票"的相关行
    
    Args:
        log_file: 日志文件路径
    """
    # 读取日志文件
    with open(log_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # 用于存储需要保留的行
    keep_lines = []
    
    # 匹配日期时间格式的正则表达式
    datetime_pattern = r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}'
    
    # 遍历日志行
    for i, line in enumerate(lines):
        # 检查是否包含目标文本
        if '存在阻力线的股票:' in line and re.match(datetime_pattern, line):
            keep_lines.append(line)
            # 检查下一行是否包含股票代码列表
            if i + 1 < len(lines):
                next_line = lines[i + 1]
                # 股票代码格式检查
                if re.search(r'\d{6}', next_line):
                    keep_lines.append(next_line)
    
    # 生成新的文件名
    log_path = Path(log_file)
    new_file = log_path.parent / f"{log_path.stem}_cleaned{log_path.suffix}"
    
    # 写入新文件
    with open(new_file, 'w', encoding='utf-8') as f:
        f.writelines(keep_lines)
    
    print(f"日志清理完成,已保存至: {new_file}")

if __name__ == "__main__":
    # 获取当前脚本所在目录
    current_dir = Path(__file__).parent
    log_file = current_dir / "Filter4.log"
    
    if log_file.exists():
        shrink_logs(str(log_file))
    else:
        print(f"未找到日志文件: {log_file}")
