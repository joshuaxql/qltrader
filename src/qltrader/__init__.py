"""
QlTrader - A股量化回测框架

API设计参考Zipline风格，支持CSV格式的A股日线数据回测。

主要功能：
- 数据接口：data.current(), data.history(), data.can_trade()
- 下单接口：order_shares(), order_target_shares(), order_percent(), order_target_percent()
- 定时任务：schedule()
- 回测入口：run_backtest()
- 可视化：plot_results()
"""

__version__ = "0.1.0"

# 核心模型
from .models import Position, Portfolio, Context

# 数据访问
from .data import Data

# 下单函数
from .orders import (
    order_shares,
    order_target_shares,
    order_percent,
    order_target_percent,
)

# 定时任务
from .scheduler import schedule

# 工具函数
from .utils import run_backtest, get_price

# 可视化
from .plotting import plot_results

# 配置（可选导出）
from .config import DATA_PATH

__all__ = [
    # 模型类
    "Position",
    "Portfolio",
    "Context",
    "Data",
    # 下单函数
    "order_shares",
    "order_target_shares",
    "order_percent",
    "order_target_percent",
    # 定时任务
    "schedule",
    # 工具函数
    "run_backtest",
    "get_price",
    # 可视化
    "plot_results",
    # 配置
    "DATA_PATH",
]
