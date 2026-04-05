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
    load_daily_basic: bool = False,
    load_moneyflow: bool = False,
    load_dividend: bool = False,
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
        load_daily_basic: 是否加载每日指标数据（PE、PB、市值等），默认False
        load_moneyflow: 是否加载资金流向数据，默认False
        load_dividend: 是否加载分红配股数据，默认False

    Returns:
        pd.DataFrame: 回测结果，包含每日的日期、总资产、现金、持仓市值、持仓明细

    Example:
        def initialize(context):
            context.set_universe(["sh600000", "sh600036"])

        def handle_data(context, data):
            # 使用每日指标数据（需要 load_daily_basic=True）
            if load_daily_basic:
                pe = data.get_daily_basic("sh600000", "pe")
                if pe < 10:
                    order_percent(context, "sh600000", 0.5)
            else:
                order_percent(context, "sh600000", 0.5)

        results = run_backtest(
            "2020-01-01", "2020-12-31",
            initialize, handle_data,
            load_daily_basic=True
        )
    """
    trader = QlTrader()
    trader.context.portfolio._starting_cash = capital_base
    trader.context.portfolio._cash = capital_base
    trader.context.portfolio._total_value = capital_base

    # 第一次调用initialize获取用户设置的股票池和定时任务
    from .models import Context
    from .orders import _set_current_context
    from .scheduler import _set_scheduler_context

    temp_context = Context()
    _set_current_context(temp_context)
    _set_scheduler_context(temp_context)
    initialize(temp_context)
    securities = temp_context.universe
    scheduled_tasks = temp_context._scheduled_tasks.copy()

    # 将设置复制到主上下文
    trader.context.set_universe(securities)
    trader.context._scheduled_tasks = scheduled_tasks

    if len(securities) == 0:
        raise ValueError("Please call set_universe() in initialize()")

    # 加载数据
    trader._load_data(
        securities, start_date, end_date, load_daily_basic, load_moneyflow, load_dividend
    )

    # 正式调用initialize
    _set_current_context(trader.context)
    _set_scheduler_context(trader.context)
    initialize(trader.context)

    results = []
    prev_date = None

    # 遍历每个交易日
    for trade_date in trader._trade_dates:
        trader.context.current_dt = trade_date
        data_obj = trader._create_data_obj(trade_date)

        # 盘前处理
        if before_trading_start:
            before_trading_start(trader.context, data_obj)

        trader.context._orders = []

        # 执行开盘定时任务
        for task in scheduled_tasks:
            if task["time_rule"] == "open":
                if trader._should_run_scheduled(task, trade_date, prev_date):
                    task["func"](trader.context, data_obj)

        # 执行开盘订单
        if len(trader.context._orders) > 0:
            trader._process_orders(data_obj, "open")
            trader.context._orders = []

        # 执行handle_data主逻辑（如果提供了的话）
        if handle_data is not None:
            _set_current_context(trader.context)
            handle_data(trader.context, data_obj)

        # 执行收盘定时任务
        for task in scheduled_tasks:
            if task["time_rule"] == "close":
                if trader._should_run_scheduled(task, trade_date, prev_date):
                    task["func"](trader.context, data_obj)

        # 执行收盘订单
        if len(trader.context._orders) > 0:
            trader._process_orders(data_obj, "close")
            trader.context._orders = []

        # 更新账户市值
        prices = {}
        for sec in securities:
            prices[sec] = data_obj.current(sec, "close")

        trader.context.portfolio.update_total_value(prices)

        # 记录当日结果
        results.append(
            {
                "date": trade_date,
                "total_value": trader.context.portfolio.total_value,
                "cash": trader.context.portfolio.cash,
                "positions_value": trader.context.portfolio.positions_value,
                "positions": {
                    k: dict(v)
                    for k, v in trader.context.portfolio.positions.positions.items()
                },
            }
        )

        prev_date = trade_date

    return pd.DataFrame(results)


def get_price(
    security: str,
    start_date: str,
    end_date: str,
    frequency: str = "daily",
    fields: Union[str, List[str]] = "close",
    as_df: bool = True,
) -> Union[pd.DataFrame, List[dict]]:
    """
    获取历史价格数据

    从CSV文件读取指定股票的历史数据，不依赖于回测环境，可独立使用。

    Args:
        security: 股票代码
        start_date: 开始日期
        end_date: 结束日期
        frequency: 频率，目前仅支持"daily"
        fields: 数据字段（字符串或列表），默认"close"
        as_df: 是否返回DataFrame格式，默认为True；若为False则返回字典格式

    Returns:
        pd.DataFrame 或 List[dict]: 包含日期和指定字段的数据

    Example:
        # 返回DataFrame（默认）
        data = get_price("000001.SZ", "2020-01-01", "2020-12-31", fields=["close", "volume"])
        # 返回字典
        data = get_price("000001.SZ", "2020-01-01", "2020-12-31", fields="close", as_df=False)
    """
    file_path = DATA_PATH / f"{security}.csv"
    if not file_path.exists():
        raise FileNotFoundError(f"Data for {security} not found")

    data_df = pd.read_csv(file_path)
    data_df["date"] = pd.to_datetime(data_df["date"])

    # 过滤日期范围
    mask = (data_df["date"] >= start_date) & (data_df["date"] <= end_date)
    data_df = data_df[mask].copy()

    if isinstance(fields, str):
        result_df = data_df[["date"] + [fields]]
    else:
        result_df = data_df[["date"] + fields]

    if as_df:
        return result_df
    else:
        # 转换为字典格式
        return result_df.to_dict(orient="records")
