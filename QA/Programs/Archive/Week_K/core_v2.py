import numpy as np
from scipy.signal import find_peaks as scipy_find_peaks
from .utils_v2 import (
    get_left_points,
    check_line_crosses_body,
    count_crossed_shadows
)

class ResistanceLineAnalyzer:
    """使用基于base week实体的阻力线分析方法"""
    
    def analyze(self, df, stock_id):
        """
        分析股票的阻力线
        
        Args:
            df (pandas.DataFrame): 周K数据，包含 'open', 'high', 'low', 'close' 列
            stock_id (str): 股票代码
            
        Returns:
            dict: 分析结果，包含连线、波峰等信息
        """
        # 计算波峰
        base_week = df.iloc[-1]  # 最新的一周
        boundary_week = df.iloc[0]  # 最早的一周
        
        # 计算波峰 - 使用scipy的find_peaks
        peaks, _ = scipy_find_peaks(df['high'].values, prominence=0.01)
        peak_dates = df.index[peaks]
        peak_prices = df['high'].values[peaks]
        
        # 在base week实体上生成right points
        right_points = self._generate_base_week_points(base_week)
        
        # 分析连线
        connections = self._analyze_shadow_connections(df, right_points)
        
        return {
            'connections': connections,
            'right_points': right_points,
            'peaks': peaks,
            'peak_dates': peak_dates,
            'peak_prices': peak_prices,
            'base_week': base_week,
            'boundary_week': boundary_week
        }
    
    def _generate_base_week_points(self, base_week):
        """在base week实体和实体下方生成更密集分布的点"""
        body_high = max(base_week['open'], base_week['close'])
        body_low = min(base_week['open'], base_week['close'])
        base_week_date = base_week.name
        
        # 计算向下扩展的范围：使用实体高度作为向下扩展的距离
        body_height = body_high - body_low
        extended_low = body_low - body_height  # 向下扩展一个实体高度
        
        # 生成更密集的点
        body_points = np.linspace(body_low, body_high, 15)  # 实体内15个点
        below_points = np.linspace(extended_low, body_low, 15)[:-1]  # 实体下方14个点（排除body_low重复点）
        
        # 合并所有点（从下到上的顺序）
        prices = np.concatenate([below_points, body_points])
        right_points = [(price, base_week_date, f'right_point{i+1}') 
                        for i, price in enumerate(prices)]
        
        return right_points
    
    def _analyze_shadow_connections(self, df, right_points):
        """分析连线"""
        if len(df) <= 1:
            return []
        
        # 获取基础数据
        base_week = df.iloc[-1]
        base_week_date = base_week.name
        
        # 存储符合条件的连线
        connections = []
        
        # 记录已经使用过的周
        used_weeks = set()
        
        # 重新排序right_points（从中间向两端）
        total_points = len(right_points)
        middle = total_points // 2
        reordered_indices = []
        for i in range(middle + 1):
            if i == 0:
                reordered_indices.append(middle)
            else:
                # 先添加中间点下方的点，再添加上方的点
                if middle - i >= 0:
                    reordered_indices.append(middle - i)
                if middle + i < total_points:
                    reordered_indices.append(middle + i)
        
        # 按重新排序的顺序遍历right_points
        for idx in reordered_indices:
            right_price, right_date, right_name = right_points[idx]
            
            # 遍历从boundary到base week之前的每一周
            for week_idx in range(len(df) - 1):
                week_date = df.index[week_idx]
                
                # 跳过已经使用过的周
                if week_date in used_weeks:
                    continue
                
                # 获取left points
                left_points = get_left_points(week_date, None, df)
                
                # 标记是否在当前周找到满足条件的点
                found_valid_point = False
                
                # 检查该周的所有点
                for i, (left_price, left_date) in enumerate(left_points):
                    left_name = f"left_point{i+1}"
                    
                    # 检查是否穿过实体
                    if not check_line_crosses_body(df, left_date, left_price, 
                                                 right_date, right_price):
                        continue
                    
                    # 检查穿越的上影线数量
                    shadow_count = count_crossed_shadows(df, left_date, left_price, 
                                                      right_date, right_price, 
                                                      base_week_date)
                    
                    if shadow_count >= 3:
                        connections.append({
                            'left_point': (left_price, left_date, left_name),
                            'right_point': (right_price, right_date, right_name),
                            'crossed_shadows': shadow_count
                        })
                        found_valid_point = True
                        used_weeks.add(week_date)  # 标记该周已使用
                        break
                
                if found_valid_point:
                    break
        
        return connections
    
    def plot(self, df, stock_id, results, debug=False, batch_mode=False):
        """
        绘制分析结果
        
        Args:
            df (pandas.DataFrame): 股票数据
            stock_id (str): 股票代码
            results (dict): 分析结果
            debug (bool): 是否显示调试信息
            batch_mode (bool): 是否为批量处理模式
        """
        from .visualization_v2 import plot_analysis_results
        plot_analysis_results(df, results, stock_id=stock_id, debug=debug, batch_mode=batch_mode)
    
    def print_analysis_results(self, df, analysis_results, debug=False):
        """打印分析结果"""
        # 将打印函数移到这里