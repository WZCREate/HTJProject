import numpy as np

class ResistanceLineAnalyzer:
    """使用基于base week实体的阻力线分析方法"""
    
    def analyze(self, df, stock_id):
        """分析股票的阻力线"""
        # 检查数据量是否足够
        if len(df) < 3:
            return None
        
        # 将DataFrame转换为numpy数组以提高性能
        prices = np.array([
            df['open'].values,
            df['high'].values,
            df['low'].values,
            df['close'].values
        ]).T  # shape: (n_days, 4)
        
        dates = df.index.values
        
        # 获取最近三天的数据
        base_day_idx = len(prices) - 1
        base_prices = prices[base_day_idx]
        base_day_minus_1_prices = prices[base_day_idx - 1]
        base_day_minus_2_prices = prices[base_day_idx - 2]
        
        # 检查最低价条件
        if (base_prices[2] < base_day_minus_2_prices[2] or 
            base_day_minus_1_prices[2] < base_day_minus_2_prices[2]):
            return None
        
        # 生成右侧点
        right_up_points = self._generate_base_day_up_points_np(
            base_prices, dates[base_day_idx])
        
        # 分析连线
        connections = self._analyze_shadow_connections_np(
            prices, dates, right_up_points)
        
        # 生成右侧下边界点
        right_low_points = self._generate_base_day_low_points_np(
            base_prices, dates[base_day_idx])
        
        # 分析下边界连线
        low_connections = self._analyze_lower_connections_np(
            prices, dates, right_low_points)
        
        return {
            'connections': connections,
            'right_up_points': right_up_points,
            'low_connections': low_connections,
            'right_low_points': right_low_points,
            'base_day': df.iloc[-1],
            'boundary_day': df.iloc[0]
        }
    
    def _generate_base_day_up_points(self, base_day):
        """
        在最后一个交易日生成右侧上边界点
        
        Args:
            base_day: 最后一个交易日的数据，包含 'open', 'close' 列
            
        Returns:
            list: (price, date, name) 元组的列表，表示右侧上边点
        """
        body_high = max(base_day['open'], base_day['close'])
        body_low = min(base_day['open'], base_day['close'])
        base_day_date = base_day.name
        
        # 计算当天涨跌幅
        chg_percen = ((base_day['close'] - base_day['open']) / base_day['open']) * 100
        
        # 计算满足条件的x值
        max_x = (1.1 * body_low - body_high) / 2.1
        
        if max_x <= 0:
            # 如果无法找到满足条件的x，返回空列表
            return []
        
        # 使用max_x的一定比例作为实际使用的x值
        x = max_x * 0.5
        
        # 计算上边界
        upper_boundary = body_high + x
        
        # 根据涨跌幅动态确定点的数量
        num_points = max(1, int((10 - abs(chg_percen))/2)) + 1
        
        # 在实体最高点和上边界之间生成均匀分布的点
        prices = np.linspace(body_high, upper_boundary, num_points + 1)[1:]  # 排除body_high
        
        # 转换为(price, date, name)元组列表
        right_up_points = [(float(price), base_day_date, f'right_up_point{i+1}') 
                           for i, price in enumerate(prices)]
        
        return right_up_points
    
    def _analyze_shadow_connections(self, df, right_up_points):
        """分析连线"""
        if len(df) <= 7:  # 修改最小数据量要求
            return []
        
        # 获取基础数据
        base_day = df.iloc[-1]
        base_day_date = base_day.name
        
        # 存储符合条件的连线
        connections = []
        
        # 记录已经使用过的日期
        used_dates = set()
        
        # 直接按照从下到上的顺序遍历right_up_points
        for right_price, right_date, right_name in right_up_points:
            # 遍历从boundary到base day前7天的每一天
            for day_idx in range(len(df) - 7):  # 修改这里，确保最后7天不作为左侧点
                day_date = df.index[day_idx]
                
                # 跳过已经使用过的日期
                if day_date in used_dates:
                    continue
                
                # 使用numpy计算左侧点
                curr_prices = df.iloc[day_idx]
                curr_body_high = max(curr_prices['open'], curr_prices['close'])
                curr_body_low = min(curr_prices['open'], curr_prices['close'])
                body_height = curr_body_high - curr_body_low
                shadow_length = curr_prices['high'] - curr_body_high  # 上影线长度
                
                # 如果上影线太短，跳过
                if body_height > 0 and shadow_length < body_height * 0.2:
                    continue
                
                # 根据影线与实体的比例确定分段数
                if body_height > 0:
                    shadow_body_ratio = shadow_length / body_height
                    # 根据比例确定分段数，最少2段，最多8段
                    num_segments = max(2, min(8, int(shadow_body_ratio * 6)))
                else:
                    # 如果是十字星，使用固定分段数
                    num_segments = 4
                
                potential_points = []
                
                # 生成上影线上的点
                if shadow_length > 0:
                    shadow_points = np.linspace(curr_body_high, curr_prices['high'], num_segments + 1)[1:]
                    potential_points = [
                        (float(price), day_date, f"left_point_shadow{i+1}")
                        for i, price in enumerate(shadow_points)
                    ]
                
                # 标记是否在当前日期找到满足条件的点
                found_valid_point = False
                
                # 检查该日期的所有点
                for left_price, left_date, left_name in potential_points:
                    # 计算斜率
                    time_diff = (right_date - left_date).astype('timedelta64[D]').astype(np.int64)
                    if time_diff == 0:
                        continue
                        
                    slope = (right_price - left_price) / time_diff
                    
                    # 上边界斜率必须小于-0.01（或其他合适的阈值）
                    if slope >= -0.01:
                        continue
                    
                    # 继续其他检查
                    if not self._check_line_crosses_body_np(df, left_date, left_price, 
                                                          right_date, right_price):
                        continue
                    
                    # 检查穿越的上影线数量
                    shadow_count = self._count_crossed_shadows_np(df, left_date, left_price, 
                                                               right_date, right_price, 
                                                               base_day_date)
                    
                    if shadow_count >= 3:
                        connections.append({
                            'left_point': (left_price, left_date, left_name),
                            'right_point': (right_price, right_date, right_name),
                            'crossed_shadows': shadow_count
                        })
                        found_valid_point = True
                        used_dates.add(day_date)  # 标记该日期已使用
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
        from .Visual import plot_analysis_results
        plot_analysis_results(df, results, stock_id=stock_id, debug=debug, batch_mode=batch_mode)
    
    def print_analysis_results(self, df, analysis_results, debug=False):
        """打印分析结果"""
        # 将打印函数移到这里
    
    def _analyze_lower_connections(self, df, right_low_points):
        """分析下边界连线"""
        if len(df) <= 7:  # 修改最小数据量要求
            return []
        
        # 获取基础数据
        base_day = df.iloc[-1]
        base_day_date = base_day.name
        
        # 存储符合条件的连线
        low_connections = []
        
        # 记录已经使用过的日期
        used_dates = set()
        
        # 直接按照从上到下的顺序遍历right_low_points
        for right_price, right_date, right_name in right_low_points:
            # 遍历从boundary到base day前7天的每一天
            for day_idx in range(len(df) - 7):
                day_date = df.index[day_idx]
                
                # 跳过已经使用过的日期
                if day_date in used_dates:
                    continue
                
                # 使用numpy计算左侧点
                curr_prices = df.iloc[day_idx]
                curr_body_high = max(curr_prices['open'], curr_prices['close'])
                curr_body_low = min(curr_prices['open'], curr_prices['close'])
                body_height = curr_body_high - curr_body_low
                shadow_length = curr_body_low - curr_prices['low']  # 下影线长度
                
                # 如果下影线太短，跳过
                if body_height > 0 and shadow_length < body_height * 0.2:
                    continue
                
                # 根据影线与实体的比例确定分段数
                if body_height > 0:
                    shadow_body_ratio = shadow_length / body_height
                    # 根据比例确定分段数，最少2段，最多8段
                    num_segments = max(2, min(8, int(shadow_body_ratio * 6)))
                else:
                    # 如果是十字星，使用固定分段数
                    num_segments = 4
                
                potential_points = []
                
                # 生成下影线上的点
                if shadow_length > 0:
                    shadow_points = np.linspace(curr_body_low, curr_prices['low'], num_segments + 1)[1:]
                    potential_points = [
                        (float(price), day_date, f"left_low_shadow{i+1}")
                        for i, price in enumerate(shadow_points)
                    ]
                
                # 标记是否在当前日期找到满足条件的点
                found_valid_point = False
                
                # 检查该日期的所有点
                for left_price, left_date, left_name in potential_points:
                    # 计算斜率
                    time_diff = (right_date - left_date).astype('timedelta64[D]').astype(np.int64)
                    if time_diff == 0:
                        continue
                        
                    slope = (right_price - left_price) / time_diff
                    
                    # 下边界斜率必须大于0.01（或其他合适的阈值）
                    if slope <= 0.01:
                        continue
                    
                    # 继续其他检查
                    if not self._check_line_crosses_body_from_below_np(
                        df, left_date, left_price, 
                        right_date, right_price):
                        continue
                    
                    # 检查穿越的下影线数量
                    shadow_count = self._count_crossed_lower_shadows_np(
                        df, left_date, left_price,
                        right_date, right_price, 
                        base_day_date)
                    
                    if shadow_count >= 3:  # 使用与上边界相同的阈值
                        low_connections.append({
                            'left_point': (left_price, left_date, left_name),
                            'right_point': (right_price, right_date, right_name),
                            'crossed_shadows': shadow_count
                        })
                        found_valid_point = True
                        used_dates.add(day_date)
                        break
                
                if found_valid_point:
                    break
        
        return low_connections
    
    def _generate_base_day_low_points(self, base_day):
        """
        在最后一个交易日生成右侧下边界点
        
        Args:
            base_day: 最后一个交易日的数据，包含 'open', 'close' 列
            
        Returns:
            list: (price, date, name) 元组的列表，表示右侧下边点
        """
        body_high = max(base_day['open'], base_day['close'])
        body_low = min(base_day['open'], base_day['close'])
        base_day_date = base_day.name
        
        # 计算当天涨跌幅
        chg_percen = ((base_day['close'] - base_day['open']) / base_day['open']) * 100
        
        # 计算满足条件的x值（与上边界对称）
        max_x = (1.1 * body_low - body_high) / 2.1
        
        if max_x <= 0:
            # 如果无法找到满足条件的x，返回空列表
            return []
        
        # 使用max_x的一定比例作为实际使用的x值
        x = max_x * 0.5
        
        # 计算下边界
        lower_boundary = body_low - x
        
        # 根据涨跌幅动态确定点的数量
        # 涨跌幅越小，生成的点越多
        num_points = max(1, int((10 - abs(chg_percen))/2)) + 1
        
        # 在实体最低点和下边界之间生成均匀分布的点
        prices = np.linspace(body_low, lower_boundary, num_points + 1)[1:]  # 排除body_low
        
        # 转换为(price, date, name)元组列表
        right_low_points = [(float(price), base_day_date, f'right_low_point{i+1}') 
                           for i, price in enumerate(prices)]
        
        return right_low_points
    
    def _generate_base_day_up_points_np(self, base_prices, base_date):
        """使用numpy生成右侧上边界点"""
        open_price, high, low, close = base_prices
        body_high = max(open_price, close)
        body_low = min(open_price, close)
        
        # 计算当天涨跌幅
        chg_percen = ((close - open_price) / open_price) * 100
        
        # 计算满足条件的x值
        max_x = (1.1 * body_low - body_high) / 2.1
        
        if max_x <= 0:
            return []
        
        # 使用max_x的一定比例作为实际使用的x值
        x = max_x * 0.5
        
        # 计算上边界
        upper_boundary = body_high + x
        
        # 根据涨跌幅动态确定点的数量
        num_points = max(1, int((10 - abs(chg_percen))/2)) + 1
        
        # 使用numpy生成均匀分布的点
        prices = np.linspace(body_high, upper_boundary, num_points + 1)[1:]
        
        # 转换为(price, date, name)元组列表
        return [(float(price), base_date, f'right_up_point{i+1}') 
                for i, price in enumerate(prices)]
    
    def _analyze_shadow_connections_np(self, prices, dates, right_up_points):
        """使用numpy分析连线"""
        if len(prices) <= 7:
            return []
        
        # 获取基础数据
        base_day_idx = len(prices) - 1
        base_date = dates[base_day_idx]
        
        connections = []
        used_dates = set()
        
        for right_price, right_date, right_name in right_up_points:
            # 只遍历到base day前7天
            for day_idx in range(len(prices) - 7):
                if dates[day_idx] in used_dates:
                    continue
                
                # 使用numpy计算左侧点
                curr_prices = prices[day_idx]
                curr_body_high = max(curr_prices[0], curr_prices[3])
                curr_body_low = min(curr_prices[0], curr_prices[3])
                body_height = curr_body_high - curr_body_low
                shadow_length = curr_prices[1] - curr_body_high  # 上影线长度
                
                # 如果上影线太短，跳过
                if body_height > 0 and shadow_length < body_height * 0.2:
                    continue
                
                # 根据影线与实体的比例确定分段数
                if body_height > 0:
                    shadow_body_ratio = shadow_length / body_height
                    # 根据比例确定分段数，最少2段，最多8段
                    num_segments = max(2, min(8, int(shadow_body_ratio * 6)))
                else:
                    # 如果是十字星，使用固定分段数
                    num_segments = 4
                
                potential_points = []
                
                # 生成上影线上的点
                if shadow_length > 0:
                    shadow_points = np.linspace(curr_body_high, curr_prices[1], num_segments + 1)[1:]  # 移除实体上边界点
                    potential_points = [
                        (float(price), dates[day_idx], f"left_point_shadow{i+1}")
                        for i, price in enumerate(shadow_points)
                    ]
                else:
                    potential_points = []  # 如果没有上影线，则没有候选点
                
                found_valid_point = False
                for left_price, left_date, left_name in potential_points:
                    # 使用numpy进行穿越检查
                    if not self._check_line_crosses_body_np(
                        prices, dates, left_date, left_price, 
                        right_date, right_price):
                        continue
                    
                    shadow_count = self._count_crossed_shadows_np(
                        prices, dates, left_date, left_price,
                        right_date, right_price, base_date)
                    
                    if shadow_count >= 3:
                        connections.append({
                            'left_point': (left_price, left_date, left_name),
                            'right_point': (right_price, right_date, right_name),
                            'crossed_shadows': shadow_count
                        })
                        found_valid_point = True
                        used_dates.add(dates[day_idx])
                        break
                
                if found_valid_point:
                    break
        
        return connections
    
    def _check_line_crosses_body_np(self, prices, dates, left_date, left_price, right_date, right_price):
        """使用numpy检查连线是否穿过实体"""
        left_idx = np.where(dates == left_date)[0][0]
        right_idx = np.where(dates == right_date)[0][0]
        
        between_prices = prices[left_idx+1:right_idx]
        if len(between_prices) == 0:
            return True
        
        days = np.arange(1, len(between_prices) + 1)
        total_days = right_idx - left_idx
        
        # 计算斜率和连线价格
        daily_slope = (right_price - left_price) / total_days
        line_prices = left_price + (daily_slope * days)
        
        # 计算实体上边界
        body_high = np.maximum(between_prices[:, 0], between_prices[:, 3])
        
        # 计算扩展量（与原版本一致：斜率的绝对值）
        y_extension = 1 * abs(daily_slope)
        
        # 修改判断标准：如果连线价格低于任何实体上边界加上扩展量，则认为穿过了实体
        extended_body_high = body_high + y_extension
        return not np.any(line_prices <= extended_body_high)
    
    def _count_crossed_shadows_np(self, prices, dates, left_date, left_price, right_date, right_price, base_date):
        """使用numpy计算穿越的上影线数量"""
        # 获取日期索引
        left_idx = np.where(dates == left_date)[0][0]
        right_idx = np.where(dates == right_date)[0][0]
        base_idx = np.where(dates == base_date)[0][0]
        
        # 获取两点之间的所有K线
        between_prices = prices[left_idx+1:min(right_idx, base_idx)]
        if len(between_prices) == 0:
            return 0
        
        # 计算连线方程
        days = np.arange(1, len(between_prices) + 1)
        total_days = right_idx - left_idx
        line_prices = left_price + ((right_price - left_price) / total_days) * days
        
        # 计算实体上边界和上影线
        body_high = np.maximum(between_prices[:, 0], between_prices[:, 3])
        
        # 一次性计算所有穿越点
        crosses = (line_prices >= body_high) & (line_prices <= between_prices[:, 1])
        return np.sum(crosses)
    
    def _analyze_lower_connections_np(self, prices, dates, right_low_points):
        """使用numpy分析下边界连线"""
        if len(prices) <= 7:
            return []
        
        # 获取基础数据
        base_day_idx = len(prices) - 1
        base_date = dates[base_day_idx]
        
        connections = []
        used_dates = set()
        
        for right_price, right_date, right_name in right_low_points:
            # 只遍历到base day前7天
            for day_idx in range(len(prices) - 7):
                if dates[day_idx] in used_dates:
                    continue
                
                # 使用numpy计算左侧点
                curr_prices = prices[day_idx]
                curr_body_high = max(curr_prices[0], curr_prices[3])
                curr_body_low = min(curr_prices[0], curr_prices[3])
                body_height = curr_body_high - curr_body_low
                shadow_length = curr_body_low - curr_prices[2]  # 下影线长度
                
                # 如果下影线太短，跳过
                if body_height > 0 and shadow_length < body_height * 0.2:
                    continue
                
                # 根据影线与实体的比例确定分段数
                if body_height > 0:
                    shadow_body_ratio = shadow_length / body_height
                    # 根据比例确定分段数，最少2段，最多8段
                    num_segments = max(2, min(8, int(shadow_body_ratio * 6)))
                else:
                    # 如果是十字星，使用固定分段数
                    num_segments = 4
                
                potential_points = []
                
                # 生成下影线上的点
                if shadow_length > 0:
                    shadow_points = np.linspace(curr_body_low, curr_prices[2], num_segments + 1)[1:]  # 移除实体下边界点
                    potential_points = [
                        (float(price), dates[day_idx], f"left_low_shadow{i+1}")
                        for i, price in enumerate(shadow_points)
                    ]
                else:
                    potential_points = []  # 如果没有下影线，则没有候选点
                
                found_valid_point = False
                for left_price, left_date, left_name in potential_points:
                    # 计算斜率
                    time_diff = (right_date - left_date).astype('timedelta64[D]').astype(np.int64)
                    if time_diff == 0:
                        continue
                        
                    slope = (right_price - left_price) / time_diff
                    
                    # 下边界斜率必须大于0.01（或其他合适的阈值）
                    if slope <= 0.01:
                        continue
                    
                    # 继续其他检查
                    if not self._check_line_crosses_body_from_below_np(
                        prices, dates, left_date, left_price, 
                        right_date, right_price):
                        continue
                    
                    shadow_count = self._count_crossed_lower_shadows_np(
                        prices, dates, left_date, left_price,
                        right_date, right_price, base_date)
                    
                    if shadow_count >= 3:
                        connections.append({
                            'left_point': (left_price, left_date, left_name),
                            'right_point': (right_price, right_date, right_name),
                            'crossed_shadows': shadow_count
                        })
                        found_valid_point = True
                        used_dates.add(dates[day_idx])
                        break
                
                if found_valid_point:
                    break
        
        return connections
    
    def _generate_base_day_low_points_np(self, base_prices, base_date):
        """使用numpy生成右侧下边界点"""
        open_price, high, low, close = base_prices
        body_high = max(open_price, close)
        body_low = min(open_price, close)
        
        # 计算当天涨跌幅
        chg_percen = ((close - open_price) / open_price) * 100
        
        # 计算满足条件的x值
        max_x = (1.1 * body_low - body_high) / 2.1
        
        if max_x <= 0:
            return []
        
        # 使用max_x的一定比例作为实际使用的x值
        x = max_x * 0.5
        
        # 计算下边界
        lower_boundary = body_low - x
        
        # 根据涨跌幅动态确定点的数量
        num_points = max(1, int((10 - abs(chg_percen))/2)) + 1
        
        # 使用numpy生成均匀分布的点
        prices = np.linspace(body_low, lower_boundary, num_points + 1)[1:]
        
        # 转换为(price, date, name)元组列表
        return [(float(price), base_date, f'right_low_point{i+1}') 
                for i, price in enumerate(prices)]
    
    def _check_line_crosses_body_from_below_np(self, prices, dates, left_date, left_price, right_date, right_price):
        """使用numpy检查连线是否从下方穿过实体"""
        left_idx = np.where(dates == left_date)[0][0]
        right_idx = np.where(dates == right_date)[0][0]
        
        between_prices = prices[left_idx+1:right_idx]
        if len(between_prices) == 0:
            return True
        
        days = np.arange(1, len(between_prices) + 1)
        total_days = right_idx - left_idx
        
        # 计算斜率和连线价格
        daily_slope = (right_price - left_price) / total_days
        line_prices = left_price + (daily_slope * days)
        
        # 计算实体下边界
        body_low = np.minimum(between_prices[:, 0], between_prices[:, 3])
        
        # 计算扩展量（与原版本一致：斜率的绝对值）
        y_extension = 1 * abs(daily_slope)
        
        # 修改判断标准：如果连线价格高于任何实体下边界减去扩展量，则认为穿过了实体
        extended_body_low = body_low - y_extension
        return not np.any(line_prices >= extended_body_low)
    
    def _count_crossed_lower_shadows_np(self, prices, dates, left_date, left_price, right_date, right_price, base_date):
        """使用numpy计算穿越的下影线数量"""
        # 获取日期索引
        left_idx = np.where(dates == left_date)[0][0]
        right_idx = np.where(dates == right_date)[0][0]
        base_idx = np.where(dates == base_date)[0][0]
        
        # 获取两点之间的所有K线
        between_prices = prices[left_idx+1:min(right_idx, base_idx)]
        if len(between_prices) == 0:
            return 0
        
        # 计算连线方程
        days = np.arange(1, len(between_prices) + 1)
        total_days = right_idx - left_idx
        line_prices = left_price + ((right_price - left_price) / total_days) * days
        
        # 计算实体下边界和下影线
        body_low = np.minimum(between_prices[:, 0], between_prices[:, 3])
        
        # 修改判断标准：连线价格在下影线和实体下边界之间
        crosses = (line_prices >= between_prices[:, 2]) & (line_prices < body_low)
        return np.sum(crosses)