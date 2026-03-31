"""
QlTrader 量化回测框架 - 订单模块

包含各种下单函数。
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .models import Context


# 全局上下文变量，用于在initialize中传递Context对象
_current_context = None


def _set_current_context(context):
    """
    设置当前全局上下文（内部使用）

    Args:
        context: Context对象
    """
    global _current_context
    _current_context = context


def order_shares(sec: str, amount: int):
    """
    按股数下单（基础下单函数）

    提交买卖订单，正数为买入，负数为卖出。实际成交在回测引擎中处理。

    Args:
        sec: 股票代码
        amount: 交易股数（正数买入，负数卖出，必须是100的整数倍）

    Example:
        order_shares("000001.SZ", 1000)  # 买入1000股
        order_shares("000001.SZ", -500)  # 卖出500股
    """
    global _current_context
    if _current_context:
        _current_context._orders.append({"sec": sec, "amount": amount})


def order_target_percent(context, sec: str, percent: float):
    """
    目标仓位下单（按总资产百分比）

    将指定股票调整到目标仓位（占总资产的比例），自动计算买卖数量。

    Args:
        context: 回测上下文
        sec: 股票代码
        percent: 目标仓位比例（0-1之间的小数，如0.5表示50%）

    Example:
        # 将股票仓位调整到总资产的50%
        order_target_percent(context, "000001.SZ", 0.5)
    """
    if context is None or not hasattr(context, "portfolio"):
        return

    # 获取所有股票当前价格
    prices = {}
    for s in context.universe:
        if s in context._data_cache:
            df = context._data_cache[s]
            date_str = context.current_dt.strftime("%Y-%m-%d")
            if date_str in df["date"].values:
                row = df[df["date"] == date_str]
                if len(row) > 0:
                    prices[s] = row.iloc[0]["close"]

    if sec not in prices or prices[sec] <= 0:
        return

    # 计算目标市值（预留0.2%滑点空间）
    target_value = context.portfolio.total_value * (1 - 0.002) * percent
    current_value = context.portfolio.positions[sec]["amount"] * prices[sec]

    # 计算需要交易的市值和股数
    trade_value = target_value - current_value
    trade_shares = int(trade_value / prices[sec] / 100) * 100

    if abs(trade_shares) >= 100:
        order_shares(sec, trade_shares)


def order_percent(context, sec: str, percent: float):
    """
    按总资产百分比下单（增量）

    买入或卖出占总资产指定比例的股票。与order_target_percent不同，
    这是增量操作，在当前持仓基础上增加或减少。

    Args:
        context: 回测上下文
        sec: 股票代码
        percent: 交易比例（正数买入，负数卖出，如0.1表示买入总资产的10%）

    Example:
        # 用总资产的10%买入股票
        order_percent(context, "000001.SZ", 0.1)
    """
    if context is None or not hasattr(context, "portfolio"):
        return

    # 获取价格
    prices = {}
    for s in context.universe:
        if s in context._data_cache:
            df = context._data_cache[s]
            date_str = context.current_dt.strftime("%Y-%m-%d")
            if date_str in df["date"].values:
                row = df[df["date"] == date_str]
                if len(row) > 0:
                    prices[s] = row.iloc[0]["close"]

    if sec not in prices or prices[sec] <= 0:
        return

    # 计算交易市值和股数
    target_value = context.portfolio.total_value * (1 - 0.002) * percent
    trade_shares = int(target_value / prices[sec] / 100) * 100

    if abs(trade_shares) >= 100:
        order_shares(sec, trade_shares)


def order_target_shares(context, sec: str, target_shares: int):
    """
    目标股数下单

    将指定股票调整到目标持仓股数，自动计算买卖数量。

    Args:
        context: 回测上下文
        sec: 股票代码
        target_shares: 目标持仓股数（必须是100的整数倍）

    Example:
        # 将股票持仓调整到1000股（不足则买入，超过则卖出）
        order_target_shares(context, "000001.SZ", 1000)
    """
    if context is None or not hasattr(context, "portfolio"):
        return

    # 计算当前持仓与目标持仓的差值
    current_shares = context.portfolio.positions[sec]["amount"]
    trade_shares = target_shares - current_shares

    # 按100股取整
    trade_shares = int(trade_shares / 100) * 100

    if abs(trade_shares) >= 100:
        order_shares(sec, trade_shares)
