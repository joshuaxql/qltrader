"""
QlTrader 量化回测框架 - 回测引擎模块

包含 QlTrader 类，是回测的核心引擎。
"""

from datetime import datetime
from typing import Dict, List, Callable, Optional
import pandas as pd
from .config import DATA_PATH, DIVIDEND_PATH
from .models import Context
from .data import Data
from .orders import _set_current_context
from .scheduler import _set_scheduler_context


class QlTrader:
    """
    回测引擎主类

    负责整个回测流程的控制，包括数据加载、订单执行、账户更新、
    结果记录等。用户通常不直接使用此类，而是通过run_backtest函数。
    """

    def __init__(self):
        self.context = Context()  # 回测上下文
        self._price_data: Dict[str, pd.DataFrame] = {}  # 价格数据
        self._daily_basic_data: Dict[str, pd.DataFrame] = {}  # 每日指标数据
        self._moneyflow_data: Dict[str, pd.DataFrame] = {}  # 资金流向数据
        self._dividend_data: Dict[str, pd.DataFrame] = {}  # 分红配股数据
        self._trade_dates: List[datetime] = []  # 交易日列表
        self._slippage = 0.002  # 滑点（0.2%）
        self._commission = 0.0003  # 手续费（0.03%）
        self._load_extra_data = False  # 是否加载额外数据

    def _load_data(
        self,
        securities: List[str],
        start_date: str,
        end_date: str,
        load_daily_basic: bool = False,
        load_moneyflow: bool = False,
        load_dividend: bool = False,
    ):
        """
        加载回测所需数据

        从CSV文件加载指定股票在指定日期范围内的数据。

        Args:
            securities: 股票代码列表
            start_date: 开始日期（YYYY-MM-DD格式）
            end_date: 结束日期（YYYY-MM-DD格式）
            load_daily_basic: 是否加载每日指标数据
            load_moneyflow: 是否加载资金流向数据
            load_dividend: 是否加载分红配股数据
        """
        all_dates = set()
        self._load_extra_data = load_daily_basic or load_moneyflow or load_dividend

        for sec in securities:
            file_path = DATA_PATH / f"{sec}.csv"
            if not file_path.exists():
                print(f"Warning: {sec} data not found")
                continue

            df = pd.read_csv(file_path)
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

            # 过滤日期范围
            mask = (df["date"] >= start_date) & (df["date"] <= end_date)
            df = df[mask].copy()

            if len(df) == 0:
                print(f"Warning: {sec} has no data in date range")
                continue

            self._price_data[sec] = df.reset_index(drop=True)
            all_dates.update(df["date"].tolist())

            # 加载每日指标数据
            if load_daily_basic:
                basic_path = DATA_PATH.parent / "daily_basic" / f"{sec}.csv"
                if basic_path.exists():
                    basic_df = pd.read_csv(basic_path)
                    basic_df["trade_date"] = pd.to_datetime(
                        basic_df["trade_date"]
                    ).dt.strftime("%Y-%m-%d")
                    mask = (basic_df["trade_date"] >= start_date) & (
                        basic_df["trade_date"] <= end_date
                    )
                    self._daily_basic_data[sec] = (
                        basic_df[mask].copy().reset_index(drop=True)
                    )
                else:
                    print(f"Warning: {sec} daily_basic data not found")

            # 加载资金流向数据
            if load_moneyflow:
                mf_path = DATA_PATH.parent / "moneyflow" / f"{sec}.csv"
                if mf_path.exists():
                    mf_df = pd.read_csv(mf_path)
                    mf_df["trade_date"] = pd.to_datetime(
                        mf_df["trade_date"]
                    ).dt.strftime("%Y-%m-%d")
                    mask = (mf_df["trade_date"] >= start_date) & (
                        mf_df["trade_date"] <= end_date
                    )
                    self._moneyflow_data[sec] = (
                        mf_df[mask].copy().reset_index(drop=True)
                    )
                else:
                    print(f"Warning: {sec} moneyflow data not found")

            # 加载分红配股数据
            if load_dividend:
                div_path = DIVIDEND_PATH / f"{sec}.csv"
                if div_path.exists():
                    div_df = pd.read_csv(div_path)
                    div_df["trade_date"] = pd.to_datetime(
                        div_df["trade_date"]
                    ).dt.strftime("%Y-%m-%d")
                    mask = (div_df["trade_date"] >= start_date) & (
                        div_df["trade_date"] <= end_date
                    )
                    self._dividend_data[sec] = (
                        div_df[mask].copy().reset_index(drop=True)
                    )
                else:
                    print(f"Warning: {sec} dividend data not found")

        # 生成交易日列表（取所有股票都有数据的日期）
        sorted_dates = sorted(list(all_dates))
        self._trade_dates = [datetime.strptime(d, "%Y-%m-%d") for d in sorted_dates]
        self.context._data_cache = self._price_data

    def _create_data_obj(self, current_dt: datetime):
        """创建Data对象（内部使用）"""
        return Data(
            self._price_data,
            current_dt,
            daily_basic_cache=self._daily_basic_data if self._load_extra_data else None,
            moneyflow_cache=self._moneyflow_data if self._load_extra_data else None,
        )

    def _should_run_scheduled(
        self, task: dict, trade_date: datetime, prev_date: Optional[datetime]
    ) -> bool:
        """
        判断定时任务是否应该执行

        Args:
            task: 任务字典
            trade_date: 当前交易日
            prev_date: 上一交易日

        Returns:
            bool: 是否应该执行
        """
        date_rule = task["date_rule"]

        if date_rule == "daily":
            return True
        elif date_rule == "month":
            if prev_date is None:
                return True
            return trade_date.month != prev_date.month

        return False

    def run(
        self,
        start_date: str,
        end_date: str,
        initialize: Callable,
        handle_data: Optional[Callable] = None,
        before_trading_start: Optional[Callable] = None,
    ):
        """
        运行回测

        执行完整的回测流程。

        Args:
            start_date: 开始日期
            end_date: 结束日期
            initialize: 初始化函数，在回测开始前调用一次
            handle_data: 主逻辑函数，每个交易日调用（可选，纯定时策略可不传）
            before_trading_start: 盘前函数，每天开盘前调用（可选）

        Returns:
            pd.DataFrame: 回测结果，包含每日的资产信息
        """
        self._price_data = {}
        self._trade_dates = []

        # 第一次调用initialize获取用户设置的股票池和定时任务
        temp_context = Context()
        _set_current_context(temp_context)
        _set_scheduler_context(temp_context)
        initialize(temp_context)
        securities = temp_context.universe
        scheduled_tasks = temp_context._scheduled_tasks.copy()

        # 将设置复制到主上下文
        self.context.set_universe(securities)
        self.context._scheduled_tasks = scheduled_tasks

        if len(securities) == 0:
            raise ValueError("Please call set_universe() in initialize()")

        # 加载数据
        self._load_data(securities, start_date, end_date)

        # 正式调用initialize
        _set_current_context(self.context)
        _set_scheduler_context(self.context)
        initialize(self.context)

        results = []
        prev_date = None

        # 遍历每个交易日
        for trade_date in self._trade_dates:
            self.context.current_dt = trade_date
            data_obj = self._create_data_obj(trade_date)

            # 盘前处理
            if before_trading_start:
                before_trading_start(self.context, data_obj)

            self.context._orders = []

            # 执行开盘定时任务
            for task in scheduled_tasks:
                if task["time_rule"] == "open":
                    if self._should_run_scheduled(task, trade_date, prev_date):
                        task["func"](self.context, data_obj)

            # 执行开盘订单
            if len(self.context._orders) > 0:
                self._process_orders(data_obj, "open")
                self.context._orders = []

            # 执行handle_data主逻辑（如果提供了的话）
            if handle_data is not None:
                _set_current_context(self.context)
                handle_data(self.context, data_obj)

            # 执行收盘定时任务
            for task in scheduled_tasks:
                if task["time_rule"] == "close":
                    if self._should_run_scheduled(task, trade_date, prev_date):
                        task["func"](self.context, data_obj)

            # 执行收盘订单
            if len(self.context._orders) > 0:
                self._process_orders(data_obj, "close")
                self.context._orders = []

            # 处理分红配送
            self._process_dividends(trade_date)

            # 更新账户市值
            prices = {}
            for sec in securities:
                prices[sec] = data_obj.current(sec, "close")

            self.context.portfolio.update_total_value(prices)

            # 记录当日结果
            results.append(
                {
                    "date": trade_date,
                    "total_value": self.context.portfolio.total_value,
                    "cash": self.context.portfolio.cash,
                    "positions_value": self.context.portfolio.positions_value,
                    "positions": {
                        k: dict(v)
                        for k, v in self.context.portfolio.positions.positions.items()
                    },
                }
            )

            prev_date = trade_date

        return pd.DataFrame(results)

    def _process_orders(self, data_obj: Data, price_field: str = "close"):
        """
        处理订单

        执行当日提交的订单，包括价格检查、可交易性检查、资金检查、
        成交计算、手续费扣除、持仓更新等。

        Args:
            data_obj: Data对象
            price_field: 价格字段（"open"或"close"）
        """
        for order in self.context._orders:
            sec = order["sec"]
            amount = order["amount"]

            # 忽略小于100股的订单
            if abs(amount) < 100:
                continue

            # 获取交易价格
            price = data_obj.current(sec, price_field)
            import numpy as np

            if np.isnan(price) or price <= 0:
                continue

            # 检查是否可交易
            if not data_obj.can_trade(sec):
                continue

            # 按100股取整
            trade_amount = int(amount / 100) * 100

            if trade_amount > 0:
                # 买入：考虑滑点（买入价格更高）
                exec_price = price * (1 + self._slippage)
                trade_value = trade_amount * exec_price
                commission = trade_value * self._commission

                # 检查资金是否充足
                if trade_value + commission > self.context.portfolio.cash:
                    # 资金不足，计算最大可买数量
                    max_shares = int(
                        self.context.portfolio.cash
                        / (exec_price * (1 + self._commission))
                    )
                    trade_amount = int(max_shares / 100) * 100

                if trade_amount > 0:
                    exec_price = price * (1 + self._slippage)
                    trade_value = trade_amount * exec_price
                    commission = trade_value * self._commission

                    # 更新持仓和现金
                    self.context.portfolio.positions.update_position(
                        sec, trade_amount, exec_price
                    )
                    self.context.portfolio._cash -= trade_value + commission

            elif trade_amount < 0:
                # 卖出：不能超卖
                current_pos = self.context.portfolio.positions[sec]["amount"]
                trade_amount = max(trade_amount, -current_pos)

                if trade_amount < 0:
                    # 卖出：考虑滑点（卖出价格更低）
                    exec_price = price * (1 - self._slippage)
                    trade_value = abs(trade_amount) * exec_price
                    commission = trade_value * self._commission

                    # 更新持仓和现金
                    self.context.portfolio.positions.update_position(
                        sec, trade_amount, exec_price
                    )
                    self.context.portfolio._cash += trade_value - commission

    def _process_dividends(self, trade_date: datetime):
        """
        处理分红配送事件

        在除权除息日（ex_date）处理：
        - 现金分红（cash_div_tax）：每股分红金额 × 持股数
        - 送转股（stk_div）：每股送转股数 × 持股数

        Args:
            trade_date: 当前交易日
        """
        if not self._dividend_data:
            return

        date_str = trade_date.strftime("%Y-%m-%d")

        for sec, pos in self.context.portfolio.positions.positions.items():
            if pos["amount"] <= 0:
                continue

            if sec not in self._dividend_data:
                continue

            div_df = self._dividend_data[sec]
            div_rows = div_df[div_df["ex_date"] == date_str]

            if len(div_rows) == 0:
                continue

            row = div_rows.iloc[0]
            shares = pos["amount"]

            # 现金分红：每股分红（税前）× 持股数
            cash_div = row.get("cash_div_tax", 0)
            if cash_div and cash_div > 0:
                self.context.portfolio._cash += shares * cash_div

            # 送转股：每股送转股数 × 持股数
            stk_div = row.get("stk_div", 0)
            if stk_div and stk_div > 0:
                new_shares = int(shares * stk_div)
                if new_shares > 0:
                    pos["amount"] += new_shares
