import pandas as pd
from sqlalchemy import create_engine, text
from CommonFunc.DBconnection import (
    find_config_path,
    load_config,
    set_log,
    db_con_sqlalchemy
)
from functools import lru_cache

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
        self.logger = set_log(self.config, "Triangle.log")
        self.engine = db_con_sqlalchemy(self.config)
        
        # 读取DEBUG配置
        self.debug = self.config.get('Programs', {}).get('Triangle_Analyzer', {}).get('DEBUG', False)
        self._cache = {}
    
    def __del__(self):
        """析构函数，确保数据库连接被关闭"""
        if hasattr(self, 'engine'):
            self.engine.dispose()
    
    def close(self):
        """显式关闭数据库连接"""
        if hasattr(self, 'engine'):
            self.engine.dispose()
    
    @lru_cache(maxsize=1000)
    def get_stock_data(self, stock_id, days=150):
        """带缓存的数据获取"""
        if stock_id in self._cache:
            return self._cache[stock_id]
            
        df = self._get_stock_data_from_db(stock_id, days)
        if df is not None:
            self._cache[stock_id] = df
        return df
    
    def _get_stock_data_from_db(self, stock_id, days=150):
        """获取指定股票的K线数据"""
        try:
            # 修改SQL查询，避免使用不支持的语法
            query = text("""
                WITH DateRange AS (
                    SELECT date 
                    FROM (
                        SELECT DISTINCT date 
                        FROM StockMain 
                        WHERE id = :stock_id 
                        AND Latest = 1 
                        ORDER BY date DESC 
                        LIMIT :days
                    ) t
                )
                SELECT 
                    date,
                    FIRST_VALUE(open_price) OVER (PARTITION BY date ORDER BY Insrt_time DESC) as open,
                    FIRST_VALUE(high) OVER (PARTITION BY date ORDER BY Insrt_time DESC) as high,
                    FIRST_VALUE(low) OVER (PARTITION BY date ORDER BY Insrt_time DESC) as low,
                    FIRST_VALUE(close_price) OVER (PARTITION BY date ORDER BY Insrt_time DESC) as close
                FROM StockMain 
                WHERE id = :stock_id
                AND Latest = 1
                AND open_price IS NOT NULL
                AND date IN (SELECT date FROM DateRange)
            """)
            
            # 使用pandas的高效读取方式
            df = pd.read_sql(
                query, 
                self.engine, 
                params={'stock_id': stock_id, 'days': days},
                parse_dates=['date']
            )
            
            if df.empty:
                return None
            
            # 优化数据处理
            df = df.drop_duplicates('date').sort_values('date')
            df.set_index('date', inplace=True)
            df['wkn'] = None
            df = df.reindex(columns=['wkn', 'open', 'high', 'low', 'close'])
            df.name = stock_id
            return df
            
        except Exception as e:
            self.logger.error_print(f"获取K线数据失败: {str(e)}")
            return None
    
    def check_stock_rise(self, df, threshold=10):
        """
        检查股票最新一天的涨幅是否满足条件
        
        Args:
            df (pandas.DataFrame): 股票周K数据
            threshold (float): 涨幅阈值（百分比）
            
        Returns:
            tuple: (涨幅值, 是否满足条件)
        """
        latest_data = df.iloc[-1]
        weekly_change = ((latest_data['close'] - latest_data['open']) / latest_data['open']) * 100
        return weekly_change, weekly_change < threshold
    
    def batch_get_stock_data(self, stock_ids, days=150):
        """批量获取多只股票的数据"""
        try:
            # 构建IN查询
            stock_ids_str = ','.join([f"'{id}'" for id in stock_ids])
            query = text(f"""
                SELECT 
                    id,
                    date,
                    FIRST_VALUE(open_price) OVER (PARTITION BY id, date ORDER BY Insrt_time DESC) as open,
                    FIRST_VALUE(high) OVER (PARTITION BY id, date ORDER BY Insrt_time DESC) as high,
                    FIRST_VALUE(low) OVER (PARTITION BY id, date ORDER BY Insrt_time DESC) as low,
                    FIRST_VALUE(close_price) OVER (PARTITION BY id, date ORDER BY Insrt_time DESC) as close
                FROM StockMain 
                WHERE id IN ({stock_ids_str})
                AND Latest = 1
                AND open_price IS NOT NULL
                AND date >= (
                    SELECT date FROM (
                        SELECT DISTINCT date 
                        FROM StockMain 
                        WHERE id IN ({stock_ids_str})
                        AND Latest = 1 
                        ORDER BY date DESC 
                        LIMIT 1 OFFSET {days-1}
                    ) t
                )
            """)
            
            # 读取数据
            df = pd.read_sql(
                query,
                self.engine,
                parse_dates=['date']
            )
            
            # 处理数据
            result = {}
            for stock_id in stock_ids:
                stock_df = df[df['id'] == stock_id].copy()
                if not stock_df.empty:
                    stock_df = stock_df.drop('id', axis=1)
                    stock_df = stock_df.sort_values('date')
                    stock_df.set_index('date', inplace=True)
                    stock_df['wkn'] = None
                    stock_df = stock_df.reindex(columns=['wkn', 'open', 'high', 'low', 'close'])
                    stock_df.name = stock_id
                    result[stock_id] = stock_df
            
            return result
            
        except Exception as e:
            self.logger.error_print(f"批量获取K线数据失败: {str(e)}")
            return {}
    
    def _get_all_stock_data(self, stock_list, days=150):
        """一次性获取所有股票数据"""
        try:
            # 构建更兼容的批量查询
            stock_ids_str = ','.join([f"'{id}'" for id in stock_list])
            query = text(f"""
                WITH DateRange AS (
                    SELECT DISTINCT date 
                    FROM StockMain 
                    WHERE id IN ({stock_ids_str})
                    AND Latest = 1 
                    ORDER BY date DESC 
                    LIMIT {days}
                )
                SELECT 
                    id,
                    date,
                    FIRST_VALUE(open_price) OVER (PARTITION BY id, date ORDER BY Insrt_time DESC) as open,
                    FIRST_VALUE(high) OVER (PARTITION BY id, date ORDER BY Insrt_time DESC) as high,
                    FIRST_VALUE(low) OVER (PARTITION BY id, date ORDER BY Insrt_time DESC) as low,
                    FIRST_VALUE(close_price) OVER (PARTITION BY id, date ORDER BY Insrt_time DESC) as close
                FROM StockMain 
                WHERE id IN ({stock_ids_str})
                AND Latest = 1
                AND open_price IS NOT NULL
                AND date IN (SELECT date FROM DateRange)
            """)
            
            # 一次性读取所有数据
            df = pd.read_sql(query, self.engine, parse_dates=['date'])
            
            # 按股票分组处理数据
            result = {}
            for stock_id, group in df.groupby('id'):
                if not group.empty:
                    stock_df = group.drop('id', axis=1)
                    stock_df = stock_df.sort_values('date')
                    stock_df.set_index('date', inplace=True)
                    stock_df['wkn'] = None
                    stock_df = stock_df.reindex(columns=['wkn', 'open', 'high', 'low', 'close'])
                    stock_df.name = stock_id
                    result[stock_id] = stock_df
            
            return result
            
        except Exception as e:
            self.logger.error_print(f"批量获取K线数据失败: {str(e)}")
            return {}
