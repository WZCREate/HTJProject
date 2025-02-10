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
        analysis_results (dict): 分析结果
        stock_id (str): 股票代码
        debug (bool): 是否显示调试信息
        batch_mode (bool): 是否为批量处理模式
    """
    # 设置图表样式
    mc = mpf.make_marketcolors(up='red',
                              down='green',
                              edge='inherit',
                              wick='inherit',
                              volume='inherit')
    s = mpf.make_mpf_style(marketcolors=mc)
    
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
        ax.set_title(f'Stock {stock_id} Weekly K-Line')
        ax.grid(True, linestyle='--', alpha=0.3)
        
        # 在蜡烛图上绘制连线
        for conn in analysis_results['connections']:
            left = conn['left_point']
            right = conn['right_point']
            
            # 获取日期对应的索引位置
            left_idx = df.index.get_loc(left[1])
            right_idx = df.index.get_loc(right[1])
            
            # 使用索引位置画线
            ax.plot([left_idx, right_idx], 
                   [left[0], right[0]], 
                   'gray', alpha=0.5, linewidth=1)
        
        plt.savefig(f'QA/Output/{stock_id}_analysis_v2.png')
        plt.close(fig)
        
    else:
        # 单股模式：绘制完整的分析图表
        # 创建包含两个子图的图表
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), height_ratios=[2, 1])
        
        # 绘制蜡烛图
        mpf.plot(df, type='candle', style=s, ax=ax1, volume=False)
        
        # 设置蜡烛图标题和网格
        ax1.set_title(f'Stock {stock_id} Weekly K-Line')
        ax1.grid(True, linestyle='--', alpha=0.3)
        
        # 设置x轴标签为周数
        x_ticks = np.arange(len(df))
        ax1.set_xticks(x_ticks[::5])
        ax1.set_xticklabels(df.index.strftime('%Y-%m-%d')[::5], rotation=45)
        
        # 标记base_week
        base_week_idx = len(df) - 1
        ax1.axvline(x=base_week_idx, color='b', linestyle='--', alpha=0.3, label='Base Week')
        ax1.axvline(x=0, color='b', linestyle='--', alpha=0.3, label='Boundary Week')
        ax1.legend()
        
        # 绘制波峰图
        ax2.plot(df.index, df['high'], 'gray', alpha=0.5, label='High Price')
        
        # 标记波峰
        ax2.plot(analysis_results['peak_dates'], 
                analysis_results['peak_prices'], 
                'ro', markersize=10, 
                markerfacecolor='none', 
                markeredgewidth=2, 
                label='Peaks')
        
        ax2.set_title('Peak Analysis')
        ax2.grid(True, linestyle='--', alpha=0.3)
        ax2.legend()
        
        # 设置x轴标签
        ax2.set_xticks(df.index[::5])
        ax2.set_xticklabels(df.index.strftime('%Y-%m-%d')[::5], rotation=45)
        
        # 在蜡烛图上绘制连线
        for conn in analysis_results['connections']:
            left = conn['left_point']
            right = conn['right_point']
            
            # 获取日期对应的索引位置
            left_idx = df.index.get_loc(left[1])
            right_idx = df.index.get_loc(right[1])
            
            # 使用索引位置画线
            ax1.plot([left_idx, right_idx], 
                    [left[0], right[0]], 
                    'gray', alpha=0.5, linewidth=1)
        
        # 调整布局
        plt.tight_layout()
        plt.show()
    
    # 打印分析结果
    if not batch_mode:
        print_analysis_results(df, analysis_results, debug=debug)

def print_analysis_results(df, analysis_results, debug=False):
    """打印分析结果"""
    if debug:
        print("\n连线分析:")
        print(f"Base Week 收盘价: {df.iloc[-1]['close']:.2f}")
        print(f"Right Points:")
        for price, date, name in analysis_results['right_points']:
            print(f"{name}: {price:.2f} @ {date.strftime('%Y-%m-%d')}")
        
        print(f"\n符合条件的连线数量: {len(analysis_results['connections'])}")
        
        # 只打印前5个连线示例
        print("\n连线示例（前5个）:")
        for i, conn in enumerate(analysis_results['connections'][:5]):
            left = conn['left_point']
            right = conn['right_point']
            print(f"连线 {i+1}: {left[2]} ({left[0]:.2f} @ {left[1].strftime('%Y-%m-%d')}) "
                  f"-> {right[2]} ({right[0]:.2f} @ {right[1].strftime('%Y-%m-%d')})")
