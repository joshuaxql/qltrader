"""
QlTrader 量化回测框架 - 核心数据模型

包含 Position（持仓）、Portfolio（投资组合）、Context（回测上下文）三个核心类。
"""

from typing import Dict, List, Optional
import pandas as pd
from .config import DATA_PATH


class Position:
    """
    持仓管理类

    管理单只股票或多只股票的持仓信息，包括持仓数量和平均成本。
    支持买入和卖出的持仓更新，自动计算加权平均成本。
    """

    def __init__(self):
        # 持仓字典，键为股票代码，值为包含amount和avg_cost的字典
        self.positions: Dict[str, dict] = {}

    def __getitem__(self, sec):
        """
        获取指定股票的持仓信息

        Args:
            sec: 股票代码

        Returns:
            dict: 包含amount(持仓量)和avg_cost(平均成本)的字典，
                  如果未持仓返回零值
        """
        return self.positions.get(sec, {"amount": 0, "avg_cost": 0})

    def update_position(self, sec: str, amount: float, avg_cost: float):
        """
        更新持仓信息

        买入时：增加持仓，重新计算加权平均成本
        卖出时：减少持仓，保持平均成本不变（直到清仓）

        Args:
            sec: 股票代码
            amount: 交易数量（正数买入，负数卖出）
            avg_cost: 交易价格（买入时的成交价格）
        """
        # 初始化该股票的持仓记录
        if sec not in self.positions:
            self.positions[sec] = {"amount": 0, "avg_cost": 0}

        current = self.positions[sec]

        # 买入操作
        if amount > 0:
            # 如果已有持仓（加仓），重新计算加权平均成本
            if current["amount"] > 0:
                total_cost = current["amount"] * current["avg_cost"] + amount * avg_cost
                total_amount = current["amount"] + amount
                current["avg_cost"] = total_cost / total_amount
                current["amount"] = total_amount
            else:
                # 新开仓
                current["avg_cost"] = avg_cost
                current["amount"] = amount

        # 卖出操作
        elif amount < 0:
            current["amount"] += amount  # amount为负数，实际是减仓
            # 清仓后重置平均成本
            if current["amount"] <= 0:
                current["amount"] = 0
                current["avg_cost"] = 0


class Portfolio:
    """
    投资组合类

    管理整个投资组合的资产信息，包括现金、总资产、持仓市值等。
    提供资产更新和查询功能。
    """

    def __init__(self):
        self.positions = Position()  # 持仓管理对象
        self._total_value = 1000000.0  # 总资产（初始100万）
        self._cash = 1000000.0  # 现金（初始100万）
        self._starting_cash = 1000000.0  # 初始资金

    @property
    def total_value(self):
        """总资产 = 现金 + 持仓市值"""
        return self._total_value

    @property
    def cash(self):
        """可用现金"""
        return self._cash

    @property
    def starting_cash(self):
        """初始资金"""
        return self._starting_cash

    @property
    def positions_value(self):
        """持仓市值 = 总资产 - 现金"""
        return self._total_value - self._cash

    def update_cash(self, amount: float):
        """
        更新现金余额

        Args:
            amount: 变动金额（正数增加，负数减少）
        """
        self._cash += amount

    def update_total_value(self, prices: Dict[str, float]):
        """
        根据最新价格更新总资产

        遍历所有持仓，用最新收盘价计算持仓市值，再加上现金得到总资产。

        Args:
            prices: 股票最新价格字典，键为股票代码，值为收盘价
        """
        positions_value = 0
        for sec, pos in self.positions.positions.items():
            # 只计算有持仓且有价格数据的股票
            if sec in prices and pos["amount"] > 0:
                positions_value += pos["amount"] * prices[sec]
        self._total_value = self._cash + positions_value


class Context:
    """
    回测上下文类

    提供回测过程中的全局状态管理，包括投资组合、当前时间、
    股票池、订单列表、定时任务等。在initialize和handle_data中通过context访问。
    """

    def __init__(self):
        self.portfolio = Portfolio()  # 投资组合
        self.current_data = None  # 当前数据（Data对象）
        self.current_dt = None  # 当前日期时间
        self._universe: List[str] = []  # 股票池（用户设置的交易标的）
        self._data_cache: Dict[str, pd.DataFrame] = {}  # 价格数据缓存
        self._orders: List[dict] = []  # 待执行订单列表
        self._scheduled_tasks: List[dict] = []  # 定时任务列表

    @property
    def universe(self):
        """获取当前股票池"""
        return self._universe

    def set_universe(self, securities: List[str]):
        """
        设置股票池

        必须在initialize中调用，指定回测涉及哪些股票。

        Args:
            securities: 股票代码列表
        """
        self._universe = securities

    def get_all_securities(self):
        """
        获取数据目录中所有可用的股票

        扫描DATA_PATH目录下的所有CSV文件，返回股票代码列表。

        Returns:
            List[str]: 股票代码列表（不含扩展名）
        """
        all_files = list(DATA_PATH.glob("*.csv"))
        return [f.stem for f in all_files]
