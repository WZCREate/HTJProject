import numpy as np
from scipy.signal import find_peaks as scipy_find_peaks

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
        # 将DataFrame转换为numpy数组以提高性能
        prices = np.array([
            df['open'].values,
            df['high'].values,
            df['low'].values,
            df['close'].values
        ]).T  # shape: (n_weeks, 4)
        
        dates = df.index.values
        
        # 计算波峰 - 使用scipy的find_peaks
        peaks, _ = scipy_find_peaks(prices[:, 1], prominence=0.01)  # 使用high列
        peak_dates = dates[peaks]
        peak_prices = prices[peaks, 1]
        
        # 在base week实体上生成right points
        right_points = self._generate_base_week_points_np(prices[-1], dates[-1])
        
        # 分析连线
        connections = self._analyze_shadow_connections_np(prices, dates, right_points)
        
        return {
            'connections': connections,
            'right_points': right_points,
            'peaks': peaks,
            'peak_dates': peak_dates,
            'peak_prices': peak_prices,
            'base_week': df.iloc[-1],
            'boundary_week': df.iloc[0]
        }
    
    def _generate_base_week_points_np(self, base_prices, base_date):
        """使用numpy生成基准周的点"""
        open_price, high, low, close = base_prices
        body_high = max(open_price, close)
        body_low = min(open_price, close)
        
        # 计算向下扩展的范围
        body_height = body_high - body_low
        extended_low = body_low - body_height
        
        # 生成更密集的点
        body_points = np.linspace(body_low, body_high, 15)
        below_points = np.linspace(extended_low, body_low, 15)[:-1]
        
        # 合并所有点
        prices = np.concatenate([below_points, body_points])
        right_points = [(float(price), base_date, f'right_point{i+1}') 
                       for i, price in enumerate(prices)]
        
        return right_points
    
    def _analyze_shadow_connections_np(self, prices, dates, right_points):
        """使用numpy分析连线"""
        if len(prices) <= 1:
            return []
        
        connections = []
        used_weeks = set()
        base_date = dates[-1]
        
        # 重新排序right_points
        total_points = len(right_points)
        middle = total_points // 2
        reordered_indices = self._get_reordered_indices(total_points, middle)
        
        for idx in reordered_indices:
            right_price, right_date, right_name = right_points[idx]
            
            # 遍历从boundary到base week之前的每一周
            for week_idx in range(len(prices) - 1):
                if dates[week_idx] in used_weeks:
                    continue
                
                # 获取left points
                curr_prices = prices[week_idx]
                curr_body_high = max(curr_prices[0], curr_prices[3])
                curr_body_low = min(curr_prices[0], curr_prices[3])
                shadow_length = curr_prices[1] - curr_body_high
                
                if not self._is_valid_shadow_np(shadow_length, curr_body_high, curr_body_low):
                    continue
                
                # 生成left points
                left_points = self._generate_left_points_np(
                    curr_prices, dates[week_idx], shadow_length)
                
                found_valid_point = False
                for left_price, left_date, left_name in left_points:
                    if not self._check_line_crosses_body_np(
                        prices, dates, left_date, left_price, right_date, right_price):
                        continue
                    
                    shadow_count = self._count_crossed_shadows_np(
                        prices, dates, left_date, left_price, right_date, right_price, base_date)
                    
                    if shadow_count >= 3:
                        connections.append({
                            'left_point': (left_price, left_date, left_name),
                            'right_point': (right_price, right_date, right_name),
                            'crossed_shadows': shadow_count
                        })
                        found_valid_point = True
                        used_weeks.add(dates[week_idx])
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
    
    def _is_valid_shadow_np(self, shadow_length, body_high, body_low):
        """检查影线是否有效"""
        if shadow_length <= 0:
            return False
            
        body_height = body_high - body_low
        if body_height > 0:
            return shadow_length >= body_height * 0.2
        else:
            return shadow_length >= (body_high - body_low) * 0.1
    
    def _generate_left_points_np(self, prices, date, shadow_length):
        """生成左侧点"""
        body_high = max(prices[0], prices[3])
        num_segments = 4
        points = np.linspace(body_high, prices[1], num_segments + 1)
        return [(float(price), date, f"left_point{i+1}") 
                for i, price in enumerate(points)]
    
    def _check_line_crosses_body_np(self, prices, dates, left_date, left_price, 
                                  right_date, right_price):
        """使用numpy检查连线是否穿过实体"""
        left_idx = np.where(dates == left_date)[0][0]
        right_idx = np.where(dates == right_date)[0][0]
        
        between_prices = prices[left_idx+1:right_idx]
        if len(between_prices) == 0:
            return True
        
        days = np.arange(1, len(between_prices) + 1)
        total_days = right_idx - left_idx
        
        daily_slope = (right_price - left_price) / total_days
        line_prices = left_price + (daily_slope * days)
        
        body_high = np.maximum(between_prices[:, 0], between_prices[:, 3])
        y_extension = abs(daily_slope)
        
        extended_high = body_high + y_extension
        return not np.any(line_prices <= extended_high)
    
    def _count_crossed_shadows_np(self, prices, dates, left_date, left_price,
                                right_date, right_price, base_date):
        """使用numpy计算穿越的上影线数量"""
        left_idx = np.where(dates == left_date)[0][0]
        right_idx = np.where(dates == right_date)[0][0]
        base_idx = np.where(dates == base_date)[0][0]
        
        between_prices = prices[left_idx+1:min(right_idx, base_idx)]
        if len(between_prices) == 0:
            return 0
        
        days = np.arange(1, len(between_prices) + 1)
        total_days = right_idx - left_idx
        line_prices = left_price + ((right_price - left_price) / total_days) * days
        
        body_high = np.maximum(between_prices[:, 0], between_prices[:, 3])
        crosses = (line_prices >= body_high) & (line_prices <= between_prices[:, 1])
        
        # 寻找最长的连续穿越序列
        max_consecutive = 0
        current_consecutive = 0
        for cross in crosses:
            if cross:
                current_consecutive += 1
                max_consecutive = max(max_consecutive, current_consecutive)
            else:
                current_consecutive = 0
                
        return max_consecutive
    
    def _get_reordered_indices(self, total_points, middle):
        """
        生成从中间向两端的索引顺序
        
        Args:
            total_points: 总点数
            middle: 中间点索引
            
        Returns:
            list: 重新排序的索引列表
        """
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
        
        return reordered_indices