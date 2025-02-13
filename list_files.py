import os

def save_directory_structure(startpath, file, indent=''):
    """将目录结构保存到文件"""
    file.write(f'{indent}📂 {os.path.basename(startpath)}\n')
    indent += '  '
    
    try:
        # 获取目录下的所有文件和文件夹
        items = os.listdir(startpath)
        items.sort()  # 按字母顺序排序
        
        # 先处理文件夹
        for item in items:
            path = os.path.join(startpath, item)
            if os.path.isdir(path):
                # 跳过名为"akshare"的文件夹和隐藏文件夹
                if item != 'akshare' and not item.startswith('.'):
                    # 检查Windows隐藏属性
                    if hasattr(os, 'stat'):
                        try:
                            is_hidden = bool(os.stat(path).st_file_attributes & 2)  # 2表示隐藏属性
                        except AttributeError:
                            is_hidden = False
                    else:
                        is_hidden = False
                    if not is_hidden:
                        save_directory_structure(path, file, indent)
                
        # 再处理文件
        for item in items:
            path = os.path.join(startpath, item)
            if os.path.isfile(path):
                file.write(f'{indent}📄 {item}\n')
                
    except PermissionError:
        file.write(f'{indent}❌ 没有权限访问此目录\n')
    except Exception as e:
        file.write(f'{indent}❌ 错误: {str(e)}\n')

if __name__ == '__main__':
    current_path = os.path.abspath('.')
    output_file = os.path.join(current_path, 'directory_structure.txt')
    
    with open(output_file, 'w', encoding='utf-8') as f:
        print(f'正在将目录结构保存到 {output_file}...')
        save_directory_structure(current_path, f)
        print('保存完成！') 