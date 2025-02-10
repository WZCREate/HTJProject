import numpy as np

def get_left_points(week_date, _, df):
    """
    获取指定周的所有可能的left points，并进行分段
    
    Args:
        week_date: 指定周的日期
        _: 占位参数，保持接口一致性
        df: 股票数据DataFrame
    
    Returns:
        list: (price, date) 元组的列表
    """
    week = df.loc[week_date]
    body_high = max(week['open'], week['close'])
    body_low = min(week['open'], week['close'])
    body_height = body_high - body_low
    shadow_length = week['high'] - body_high
    
    # 如果没有上影线，返回空列表
    if shadow_length <= 0:
        return []
    
    # 使用实体高度作为基准判断影线长度
    if body_height > 0:
        if shadow_length < body_height * 0.2:  # 影线长度小于实体高度的20%
            return []
    else:
        # 如果是十字星，使用当日振幅的一定比例作为判断标准
        day_range = week['high'] - week['low']
        if shadow_length < day_range * 0.1:  # 影线长度小于振幅的10%
            return []
    
    # 生成分段点
    num_segments = 4  # 默认4等分
    
    # 生成分段点（包括最高点和实体上沿）
    points = np.linspace(body_high, week['high'], num_segments + 1)
    
    # 转换为(price, date)元组列表，并按价格从高到低排序
    left_points = [(float(price), week_date) for price in points]
    left_points.sort(reverse=True)
    
    return left_points

def check_line_crosses_body(df, left_date, left_price, right_date, right_price):
    """
    检查连线是否低于任何K线实体的上边界（加上扩展量）
    如果连线在任何位置低于实体上边界的扩展区域，返回False
    """
    time_diff = (right_date - left_date).days
    if time_diff == 0:
        return False
        
    # 修改：以右边为原点计算斜率
    daily_slope = (left_price - right_price) / time_diff
    
    # 确保检查所有中间的K线
    left_idx = df.index.get_loc(left_date)
    right_idx = df.index.get_loc(right_date)
    
    # 使用斜率的绝对值作为扩展比例
    y_extension =  1*abs(daily_slope)
    
    # 检查所有中间的K线
    for idx in range(left_idx + 1, right_idx):
        check_date = df.index[idx]
        # 修改：计算相对于右边的时间差
        days_from_right = (right_date - check_date).days
        # 修改：使用相对于右边的时间计算线段价格
        line_price = right_price + (daily_slope * days_from_right)
        
        check_week = df.iloc[idx]
        body_high = max(check_week['open'], check_week['close'])
        
        # 扩展实体的上边界
        extended_high = body_high + y_extension
        
        # 如果连线低于扩展后的实体上边界，返回False
        if line_price <= extended_high:
            return False
    
    return True

def count_crossed_shadows(df, left_date, left_price, right_date, right_price, neighbor_week_date):
    """
    计算连线穿越的上影线数量
    
    Args:
        df: 股票数据DataFrame
        left_date: 左端点日期
        left_price: 左端点价格
        right_date: 右端点日期
        right_price: 右端点价格
        neighbor_week_date: 邻周日期
    
    Returns:
        int: 穿越的上影线数量
    """
    time_diff = (right_date - left_date).days
    if time_diff == 0:
        return 0
        
    daily_slope = (right_price - left_price) / time_diff
    crossed_count = 0
    
    for check_date in df.index:
        if left_date < check_date <= neighbor_week_date:
            days_from_left = (check_date - left_date).days
            line_price = left_price + (daily_slope * days_from_left)
            
            check_week = df.loc[check_date]
            body_top = max(check_week['open'], check_week['close'])
            
            # 如果连线的价格在上影线范围内，计数加1
            if body_top < line_price <= check_week['high']:
                crossed_count += 1
    
    return crossed_count
