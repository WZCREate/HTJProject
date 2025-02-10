import matplotlib.pyplot as plt
import mplfinance as mpf
import matplotlib.dates as mdates
import numpy as np
from datetime import timedelta
import pandas as pd

def plot_analysis_results(df, analysis_results, stock_id=None, debug=False, batch_mode=False):
    """
    绘制分析结果
    
    Args:
        df (pandas.DataFrame): 股票数据
        analysis_results (dict): 分析结果，包含right_up_points等信息
        stock_id (str): 股票代码
        debug (bool): 是否显示调试信息
        batch_mode (bool): 是否为批量处理模式
    """
    # 设置统一的点样式参数
    POINT_SIZE = 2
    POINT_ALPHA = 0.7
    LINE_ALPHA = 0.5
    LINE_WIDTH = 1
    
    # 设置图表样式
    mc = mpf.make_marketcolors(up='red',
                              down='green',
                              edge='inherit',
                              wick='inherit',
                              volume='inherit')
    s = mpf.make_mpf_style(marketcolors=mc)
    
    def find_intersection_point(upper_line, lower_line):
        """计算两条直线的交点"""
        # 上边界直线的两点
        x1, y1 = df.index.get_loc(upper_line['left_point'][1]), upper_line['left_point'][0]
        x2, y2 = df.index.get_loc(upper_line['right_point'][1]), upper_line['right_point'][0]
        
        # 下边界直线的两点
        x3, y3 = df.index.get_loc(lower_line['left_point'][1]), lower_line['left_point'][0]
        x4, y4 = df.index.get_loc(lower_line['right_point'][1]), lower_line['right_point'][0]
        
        # 计算直线斜率和截距
        k1 = (y2 - y1) / (x2 - x1)
        b1 = y1 - k1 * x1
        
        k2 = (y4 - y3) / (x4 - x3)
        b2 = y3 - k2 * x3
        
        # 计算交点
        if k1 == k2:
            return None  # 平行线
        
        x = (b2 - b1) / (k1 - k2)
        y = k1 * x + b1
        
        return x, y
    
    def extend_lines_to_intersection(ax, upper_connections, lower_connections):
        """延伸直线到交点"""
        if not upper_connections or not lower_connections:
            return
        
        # 先绘制原始的线段
        for conn in upper_connections:
            left = conn['left_point']
            right = conn['right_point']
            left_idx = df.index.get_loc(left[1])
            right_idx = df.index.get_loc(right[1])
            ax.plot([left_idx, right_idx], 
                   [left[0], right[0]], 
                   'gray', alpha=LINE_ALPHA, linewidth=LINE_WIDTH,
                   label='Upper Triangle Line' if conn == upper_connections[0] else "")
        
        for conn in lower_connections:
            left = conn['left_point']
            right = conn['right_point']
            left_idx = df.index.get_loc(left[1])
            right_idx = df.index.get_loc(right[1])
            ax.plot([left_idx, right_idx], 
                   [left[0], right[0]], 
                   'gray', alpha=LINE_ALPHA, linewidth=LINE_WIDTH,
                   label='Lower Triangle Line' if conn == lower_connections[0] else "")
        
        # 然后尝试延伸到交点
        for upper_conn in upper_connections:
            for lower_conn in lower_connections:
                intersection = find_intersection_point(upper_conn, lower_conn)
                if intersection:
                    x_intersect, y_intersect = intersection
                    
                    # 获取原始点的索引
                    upper_right_idx = df.index.get_loc(upper_conn['right_point'][1])
                    lower_right_idx = df.index.get_loc(lower_conn['right_point'][1])
                    
                    # 只绘制从右端点到交点的延伸部分
                    if x_intersect > max(upper_right_idx, lower_right_idx):
                        # 延伸上边界线
                        ax.plot([upper_right_idx, x_intersect],
                               [upper_conn['right_point'][0], y_intersect],
                               'gray', alpha=LINE_ALPHA, linewidth=LINE_WIDTH)
                        
                        # 延伸下边界线
                        ax.plot([lower_right_idx, x_intersect],
                               [lower_conn['right_point'][0], y_intersect],
                               'gray', alpha=LINE_ALPHA, linewidth=LINE_WIDTH)
    
    if batch_mode:
        # 批量模式：绘制蜡烛图和压力线
        fig, ax = plt.subplots(figsize=(12, 6))
        
        # 绘制蜡烛图
        mpf.plot(df,
                 type='candle',
                 style=s,
                 ax=ax,
                 volume=False,
                 warn_too_much_data=10000)
        
        # 设置标题和网格
        ax.set_title(f'Stock {stock_id} Triangle Pattern Analysis')
        ax.grid(True, linestyle='--', alpha=0.3)
        
        # 绘制右侧上边点
        if 'right_up_points' in analysis_results:
            for price, date, name in analysis_results['right_up_points']:
                idx = df.index.get_loc(date)
                ax.plot(idx, price, 'b.', markersize=POINT_SIZE, alpha=POINT_ALPHA)
        
        # 绘制右侧下边点
        if 'right_low_points' in analysis_results:
            if debug:
                print(f"Drawing {len(analysis_results['right_low_points'])} lower right points")
            for price, date, name in analysis_results['right_low_points']:
                idx = df.index.get_loc(date)
                ax.plot(idx, price, 'b.', markersize=POINT_SIZE, alpha=POINT_ALPHA)
        elif debug:
            print("No right_low_points found in analysis_results")
            print("Keys in analysis_results:", analysis_results.keys())
        
        # 延伸直线到交点
        extend_lines_to_intersection(ax, analysis_results['connections'], 
                                   analysis_results['low_connections'])
        
        plt.savefig(f'QA/Output/{stock_id}_triangle_analysis.png')
        plt.close(fig)
        
    else:
        # 单股模式：只绘制一个图表
        fig, ax1 = plt.subplots(figsize=(12, 6))
        
        # 绘制蜡烛图
        mpf.plot(df, type='candle', style=s, ax=ax1, volume=False)
        
        # 设置蜡烛图标题和网格
        ax1.set_title(f'Stock {stock_id} Triangle Pattern Analysis')
        ax1.grid(True, linestyle='--', alpha=0.3)
        
        # 绘制右侧上边点
        if 'right_up_points' in analysis_results:
            for price, date, name in analysis_results['right_up_points']:
                idx = df.index.get_loc(date)
                ax1.plot(idx, price, 'b.', markersize=POINT_SIZE, alpha=POINT_ALPHA, 
                        label='Upper Right Points' if name == 'right_up_point1' else "")
        
        # 绘制右侧下边点
        if 'right_low_points' in analysis_results:
            for price, date, name in analysis_results['right_low_points']:
                idx = df.index.get_loc(date)
                ax1.plot(idx, price, 'b.', markersize=POINT_SIZE, alpha=POINT_ALPHA,
                        label='Lower Right Points' if name == 'right_low_point1' else "")
        
        # 延伸直线到交点
        extend_lines_to_intersection(ax1, analysis_results['connections'], 
                                   analysis_results['low_connections'])
        
        # 设置x轴标签为日期
        x_ticks = np.arange(len(df))
        ax1.set_xticks(x_ticks[::5])
        ax1.set_xticklabels(df.index.strftime('%Y-%m-%d')[::5], rotation=45)
        
        # 标记分析区间
        base_day_idx = len(df) - 1
        ax1.axvline(x=base_day_idx, color='b', linestyle='--', alpha=0.3, label='Latest Day')
        ax1.axvline(x=0, color='b', linestyle='--', alpha=0.3, label='Start Day')
        
        ax1.legend()
        plt.tight_layout()
        plt.show()
    
    # 打印分析结果
    if not batch_mode:
        print_analysis_results(df, analysis_results, debug=debug)

def print_analysis_results(df, analysis_results, debug=False):
    """打印分析结果"""
    if debug:
        print("\n三角形形态分析:")
        print(f"最新交易日收盘价: {df.iloc[-1]['close']:.2f}")
        print(f"三角形顶点:")
        for price, date, name in analysis_results['right_points']:
            print(f"{name}: {price:.2f} @ {date.strftime('%Y-%m-%d')}")
        
        print(f"\n符合条件的三角形边数量: {len(analysis_results['connections'])}")
        
        # 只打印前5个连线示例
        print("\n三角形边示例（前5个）:")
        for i, conn in enumerate(analysis_results['connections'][:5]):
            left = conn['left_point']
            right = conn['right_point']
            print(f"边 {i+1}: {left[2]} ({left[0]:.2f} @ {left[1].strftime('%Y-%m-%d')}) "
                  f"-> {right[2]} ({right[0]:.2f} @ {right[1].strftime('%Y-%m-%d')})")
