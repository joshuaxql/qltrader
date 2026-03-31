"""
QlTrader 量化回测框架 - 定时任务模块

包含 schedule 函数，用于设置定时调仓任务。
"""

from typing import Callable


# 全局上下文变量
_current_context = None


def _set_scheduler_context(context):
    """设置调度器上下文（内部使用）"""
    global _current_context
    _current_context = context


def schedule(
    schedule_func: Callable, date_rule: str = "daily", time_rule: str = "open"
):
    """
    设置定时任务

    在initialize中调用，用于安排在特定时间执行的函数。
    支持按日或按月触发，在开盘或收盘时执行。

    Args:
        schedule_func: 要执行的函数，接收(context, data)两个参数
        date_rule: 日期规则，可选"daily"(每天)或"month"(每月)
        time_rule: 时间规则，可选"open"(开盘)或"close"(收盘)

    Raises:
        ValueError: 当date_rule或time_rule参数无效时
        RuntimeError: 当在initialize外调用时

    Example:
        def rebalance(context, data):
            order_target_percent(context, "000001.SZ", 0.5)

        def initialize(context):
            schedule(rebalance, date_rule="month", time_rule="open")
    """
    valid_date_rules = ["daily", "month"]
    valid_time_rules = ["open", "close"]

    if date_rule not in valid_date_rules:
        raise ValueError(
            f"date_rule must be one of {valid_date_rules}, got {date_rule}"
        )
    if time_rule not in valid_time_rules:
        raise ValueError(
            f"time_rule must be one of {valid_time_rules}, got {time_rule}"
        )

    global _current_context
    if _current_context is None:
        raise RuntimeError("schedule() can only be called during initialize()")

    _current_context._scheduled_tasks.append(
        {"func": schedule_func, "date_rule": date_rule, "time_rule": time_rule}
    )
