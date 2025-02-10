import pandas as pd
from sqlalchemy import create_engine, text
from CommonFunc.DBconnection import (
    find_config_path,
    load_config,
    set_log,
    db_con_sqlalchemy
)

class DataLoader:
    def __init__(self, config_path=None):
        """
        初始化数据加载器
        
        Args:
            config_path (str, optional): 配置文件路径。如果为None，将自动查找
        """
        if config_path is None:
            config_path, _, _ = find_config_path()
            
        self.config = load_config(config_path)
        self.logger = set_log(self.config, "weekly_data_v2.log")
        self.engine = db_con_sqlalchemy(self.config)
        
        # 读取DEBUG配置
        self.debug = self.config.get('Programs', {}).get('Week_K_Analyzer', {}).get('DEBUG', False)
    
    def __del__(self):
        """析构函数，确保数据库连接被关闭"""
        if hasattr(self, 'engine'):
            self.engine.dispose()
    
    def close(self):
        """显式关闭数据库连接"""
        if hasattr(self, 'engine'):
            self.engine.dispose()
    
    def get_stock_weekly_data(self, stock_id, weeks=80):
        """
        获取指定股票的周K数据
        
        Args:
            stock_id (str): 股票代码
            weeks (int): 获取的周数
            
        Returns:
            pandas.DataFrame: 周K数据
        """
        try:
            query = text("""
                SELECT WK_date as Date, wkn, open, high, low, close
                FROM WK 
                WHERE id = :stock_id 
                AND status = 'active'
                ORDER BY WK_date DESC 
                LIMIT :weeks
            """)
            
            df = pd.read_sql(query, self.engine, params={'stock_id': stock_id, 'weeks': weeks})
            df['Date'] = pd.to_datetime(df['Date'])
            df = df.sort_values('Date')  # 按日期升序排序
            df.set_index('Date', inplace=True)
            df.name = stock_id  # 添加股票代码作为名称
            return df
            
        except Exception as e:
            self.logger.error_print(f"获取周K数据失败: {str(e)}")
            return None
    
    def check_stock_rise(self, df, threshold=2.8):
        """
        检查股票最新一周的涨幅是否满足条件
        
        Args:
            df (pandas.DataFrame): 股票周K数据
            threshold (float): 涨幅阈值（百分比）
            
        Returns:
            tuple: (涨幅值, 是否满足条件)
        """
        latest_data = df.iloc[-1]
        weekly_change = ((latest_data['close'] - latest_data['open']) / latest_data['open']) * 100
        return weekly_change, weekly_change >= threshold
