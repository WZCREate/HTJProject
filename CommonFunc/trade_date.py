#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
Date: 2024/3/25 16:30
Desc: 新浪财经-交易日历
https://finance.sina.com.cn/realstock/company/klc_td_sh.txt
此处可以用来更新 calendar.json 文件，注意末尾没有 "," 号
"""

import datetime

import pandas as pd
import requests
import py_mini_racer

from akshare.stock.cons import hk_js_decode


def tool_trade_date_hist_sina(start_date=None, save_path="trade_calendar.json") -> pd.DataFrame:
    """
    新浪财经-交易日历-历史数据
    https://finance.sina.com.cn/realstock/company/klc_td_sh.txt
    :param start_date: 开始日期，格式 YYYY-MM-DD
    :param save_path: 保存路径，默认保存为根目录下的 trade_calendar.json
    :return: 交易日历
    :rtype: pandas.DataFrame
    """
    url = "https://finance.sina.com.cn/realstock/company/klc_td_sh.txt"
    r = requests.get(url)
    js_code = py_mini_racer.MiniRacer()
    js_code.eval(hk_js_decode)
    dict_list = js_code.call("d", r.text.split("=")[1].split(";")[0].replace('"', ""))
    temp_df = pd.DataFrame(dict_list)
    temp_df.columns = ["trade_date"]
    temp_df["trade_date"] = pd.to_datetime(temp_df["trade_date"]).dt.date
    temp_list = temp_df["trade_date"].to_list()
    # 该日期是交易日，但是在新浪返回的交易日历缺失该日期，这里补充上
    temp_list.append(datetime.date(year=1992, month=5, day=4))
    temp_list.sort()
    temp_df = pd.DataFrame(temp_list, columns=["trade_date"])

    """
    # 获取从2024年开始的所有交易日历并保存
    df = tool_trade_date_hist_sina(start_date="2024-01-01")

    # 获取所有交易日历并保存到指定路径
    df = tool_trade_date_hist_sina(save_path="data/trade_calendar.json")

    # 只获取数据不保存
    df = tool_trade_date_hist_sina(save_path=None)
    """
    
    # 只添加开始日期过滤
    if start_date:
        start_date = pd.to_datetime(start_date).date()
        temp_df = temp_df[temp_df["trade_date"] >= start_date]
    
    # 保存为JSON格式
    if save_path:
        # 将日期对象转换为字符串格式 YYYY-MM-DD
        json_data = [d.strftime("%Y-%m-%d") for d in temp_df["trade_date"]]
        import json
        with open(save_path, "w") as f:
            json.dump(json_data, f, indent=2)
        print(f"交易日历已保存至: {save_path}")
        
    return temp_df


if __name__ == "__main__":
    # 获取从2024年1月1日起的所有交易日历并保存
    tool_trade_date_hist_df = tool_trade_date_hist_sina(
        start_date="2024-01-01",
        save_path="trade_calendar.json"
    )
    print(f"获取到的交易日历范围: {tool_trade_date_hist_df['trade_date'].min()} 到 {tool_trade_date_hist_df['trade_date'].max()}")
