"""
QlTrader 量化回测框架 - 工具函数模块

包含 run_backtest 入口函数和 get_price 数据获取函数。
"""

from typing import Callable, Optional, Union, List
import pandas as pd
from .config import DATA_PATH
from .engine import QlTrader


def run_backtest(
    start_date: str,
    end_date: str,
    initialize: Callable,
    handle_data: Optional[Callable] = None,
    before_trading_start: Optional[Callable] = None,
    capital_base: float = 1000000.0,
):
    """
    运行回测（用户入口函数）

    创建回测引擎并运行回测，是用户使用的主要接口。

    Args:
        start_date: 开始日期（YYYY-MM-DD格式）
        end_date: 结束日期（YYYY-MM-DD格式）
        initialize: 初始化函数，在回测开始前调用，用于设置股票池、定时任务等
        handle_data: 主逻辑函数，每个交易日调用，包含交易策略（可选，纯定时策略可不传）
        before_trading_start: 盘前函数，每天开盘前调用（可选）
        capital_base: 初始资金（默认100万）

    Returns:
        pd.DataFrame: 回测结果，包含每日的日期、总资产、现金、持仓市值、持仓明细

    Example:
        def initialize(context):
            context.set_universe(["000001.SZ", "000002.SZ"])

        def handle_data(context, data):
            order_percent(context, "000001.SZ", 0.5)

        results = run_backtest("2020-01-01", "2020-12-31", initialize, handle_data)
    """
    trader = QlTrader()
    trader.context.portfolio._starting_cash = capital_base
    trader.context.portfolio._cash = capital_base
    trader.context.portfolio._total_value = capital_base
    return trader.run(
        start_date, end_date, initialize, handle_data, before_trading_start
    )


def get_price(
    security: str,
    start_date: str,
    end_date: str,
    frequency: str = "daily",
    fields: Union[str, List[str]] = "close",
) -> pd.DataFrame:
    """
    获取历史价格数据

    从CSV文件读取指定股票的历史数据，不依赖于回测环境，可独立使用。

    Args:
        security: 股票代码
        start_date: 开始日期
        end_date: 结束日期
        frequency: 频率，目前仅支持"daily"
        fields: 数据字段（字符串或列表），默认"close"

    Returns:
        pd.DataFrame: 包含日期和指定字段的数据框

    Example:
        df = get_price("000001.SZ", "2020-01-01", "2020-12-31", fields=["close", "volume"])
    """
    file_path = DATA_PATH / f"{security}.csv"
    if not file_path.exists():
        raise FileNotFoundError(f"Data for {security} not found")

    df = pd.read_csv(file_path)
    df["date"] = pd.to_datetime(df["date"])

    # 过滤日期范围
    mask = (df["date"] >= start_date) & (df["date"] <= end_date)
    df = df[mask].copy()

    if isinstance(fields, str):
        return df[["date"] + [fields]]
    return df[["date"] + fields]
