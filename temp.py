import akshare as ak

stock_zh_a_hist_df = ak.stock_zh_a_hist(
    symbol="600734",
    period="daily",
    start_date="20250501",
    end_date="20250520",
    adjust="hfq"
)
print(stock_zh_a_hist_df)