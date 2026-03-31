import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Callable, Optional, Union
import warnings

warnings.filterwarnings("ignore")

# 数据文件路径，用于存储日线数据文件（CSV格式）
DATA_PATH = Path(r".\data\daily")


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


class Data:
    """
    数据访问类

    提供回测过程中的数据查询功能，包括获取当前价格、
    历史数据、判断股票是否可交易等。在handle_data和定时函数中通过data访问。
    """

    def __init__(self, data_cache: Dict[str, pd.DataFrame], current_dt: datetime):
        self._data_cache = data_cache  # 价格数据缓存
        self.current_dt = current_dt  # 当前日期时间

    def current(
        self, securities: Union[str, List[str]], fields: Union[str, List[str]] = "close"
    ):
        """
        获取当前时刻的行情数据

        获取指定股票在当前日期的指定字段值（如收盘价、开盘价等）。

        Args:
            securities: 股票代码（字符串或列表）
            fields: 数据字段（字符串或列表），默认"close"
                   可选值包括：open, high, low, close, volume等

        Returns:
            根据参数不同返回不同格式：
            - 单股单字段: 标量值（float）
            - 单股多字段: {field: value} 字典
            - 多股单字段: {sec: value} 字典
            - 多股多字段: {sec: {field: value}} 嵌套字典

        Example:
            price = data.current("000001.SZ", "close")

            prices = data.current(["000001.SZ", "000002.SZ"], "close")
        """
        # 统一转换为列表
        if isinstance(securities, str):
            securities = [securities]
        if isinstance(fields, str):
            fields = [fields]

        result = {}
        for sec in securities:
            # 检查数据是否存在
            if sec not in self._data_cache:
                result[sec] = {f: np.nan for f in fields}
                continue

            df = self._data_cache[sec]
            date_str = self.current_dt.strftime("%Y-%m-%d")

            try:
                row = df[df["date"] == date_str]
                if len(row) == 0:
                    # 当日无数据
                    result[sec] = {f: np.nan for f in fields}
                else:
                    result[sec] = {}
                    for f in fields:
                        result[sec][f] = row.iloc[0][f]
            except Exception:
                result[sec] = {f: np.nan for f in fields}

        # 根据参数数量返回不同格式
        if len(securities) == 1 and len(fields) == 1:
            return result.get(securities[0], {}).get(fields[0], np.nan)
        elif len(securities) == 1:
            return result.get(securities[0], {f: np.nan for f in fields})
        elif len(fields) == 1:
            return {
                sec: result.get(sec, {}).get(fields[0], np.nan) for sec in securities
            }
        return result

    def history(
        self,
        securities: Union[str, List[str]],
        bar_count: int,
        frequency: str = "1d",
        fields: Union[str, List[str]] = "close",
    ):
        """
        获取历史行情数据

        获取指定股票的历史数据，用于技术指标计算等。

        Args:
            securities: 股票代码（字符串或列表）
            bar_count: 获取的K线数量
            frequency: 频率，目前仅支持"1d"（日线）
            fields: 数据字段（字符串或列表），默认"close"

        Returns:
            与current()类似，但值为列表而非标量
            - 单股单字段: [value1, value2, ...] 列表
            - 单股多字段: {field: [values]} 字典
            - 多股单字段: {sec: [values]} 字典
            - 多股多字段: {sec: {field: [values]}} 嵌套字典

        Example:
            hist = data.history("000001.SZ", bar_count=20, fields="close")
        """
        if isinstance(securities, str):
            securities = [securities]
        if isinstance(fields, str):
            fields = [fields]

        result = {}
        for sec in securities:
            if sec not in self._data_cache:
                result[sec] = {f: [np.nan] * bar_count for f in fields}
                continue

            df = self._data_cache[sec].copy()

            try:
                # 设置日期索引并排序
                date_index = pd.to_datetime(df["date"])
                df = df.set_index(date_index)
                df = df.sort_index()

                # 定位当前日期在数据中的位置
                end_loc = df.index.get_indexer([self.current_dt], method="pad")[0]
                start_loc = max(0, end_loc - bar_count + 1)

                result[sec] = {}
                for f in fields:
                    values = df.iloc[start_loc : end_loc + 1][f].tolist()
                    # 数据不足时用NaN填充
                    if len(values) < bar_count:
                        values = [np.nan] * (bar_count - len(values)) + values
                    result[sec][f] = values
            except Exception:
                result[sec] = {f: [np.nan] * bar_count for f in fields}

        # 格式化返回值
        if len(securities) == 1 and len(fields) == 1:
            return result[securities[0]][fields[0]]
        elif len(securities) == 1:
            return result[securities[0]]
        elif len(fields) == 1:
            return {sec: result[sec][fields[0]] for sec in securities}
        return result

    def can_trade(
        self, securities: Union[str, List[str]]
    ) -> Union[bool, Dict[str, bool]]:
        """
        判断股票是否可以交易

        检查股票在当日是否可交易（有数据且成交量>0，不停牌）。

        Args:
            securities: 股票代码（字符串或列表）

        Returns:
            bool或Dict[str, bool]: 单股返回bool，多股返回字典

        Example:
            if data.can_trade("000001.SZ"):
                order_shares("000001.SZ", 1000)
        """
        if isinstance(securities, str):
            return self._can_trade_single(securities)

        return {sec: self._can_trade_single(sec) for sec in securities}

    def _can_trade_single(self, sec: str) -> bool:
        """单只股票交易可行性检查的内部实现"""
        if sec not in self._data_cache:
            return False

        df = self._data_cache[sec]
        date_str = self.current_dt.strftime("%Y-%m-%d")

        try:
            row = df[df["date"] == date_str]
            if len(row) == 0:
                return False
            volume = row.iloc[0]["volume"]
            # 成交量大于0表示可交易（不停牌）
            return volume > 0
        except Exception:
            return False


# 全局上下文变量，用于在initialize中传递Context对象
_current_context: Optional[Context] = None


def _set_current_context(context: Context):
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


def order_target_percent(context: Context, sec: str, percent: float):
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


def order_percent(context: Context, sec: str, percent: float):
    """
    按总资产百分比下单（增量）

    买入或卖出占总资产指定比例的股票。与order_target_percent不同，
    这是增量操作，在当前持仓基础上增加或减少。

    Args:
        context: 回测上下文
        sec: 股票代码
        percent: 交易比例（正数买入，负数卖出，如0.1表示买入总资产的10%）

    Example:
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


def order_target_shares(context: Context, sec: str, target_shares: int):
    """
    目标股数下单

    将指定股票调整到目标持仓股数，自动计算买卖数量。

    Args:
        context: 回测上下文
        sec: 股票代码
        target_shares: 目标持仓股数（必须是100的整数倍）

    Example:
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


class QlTrader:
    """
    回测引擎主类

    负责整个回测流程的控制，包括数据加载、订单执行、账户更新、
    结果记录等。用户通常不直接使用此类，而是通过run_backtest函数。
    """

    def __init__(self):
        self.context = Context()  # 回测上下文
        self._price_data: Dict[str, pd.DataFrame] = {}  # 价格数据
        self._trade_dates: List[datetime] = []  # 交易日列表
        self._slippage = 0.002  # 滑点（0.2%）
        self._commission = 0.0003  # 手续费（0.03%）

    def _load_data(self, securities: List[str], start_date: str, end_date: str):
        """
        加载回测所需数据

        从CSV文件加载指定股票在指定日期范围内的数据。

        Args:
            securities: 股票代码列表
            start_date: 开始日期（YYYY-MM-DD格式）
            end_date: 结束日期（YYYY-MM-DD格式）
        """
        all_dates = set()
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

        # 生成交易日列表（取所有股票都有数据的日期）
        sorted_dates = sorted(list(all_dates))
        self._trade_dates = [datetime.strptime(d, "%Y-%m-%d") for d in sorted_dates]
        self.context._data_cache = self._price_data

    def _create_data_obj(self, current_dt: datetime):
        """创建Data对象（内部使用）"""
        return Data(self._price_data, current_dt)

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
            trade_date: 交易日期
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


def plot_results(results: pd.DataFrame, title: str = "Backtest Results"):
    """
    绘制回测结果图表

    可视化回测结果，包括资金曲线、累计收益率，并打印统计指标。

    Args:
        results: run_backtest返回的结果DataFrame
        title: 图表标题

    Statistics:
        - 起始资金 / 结束资金
        - 总收益率
        - 年化收益率
        - 年化波动率
        - 夏普比率
        - 最大回撤

    Example:
        results = run_backtest(...)
        plot_results(results, title="My Strategy")
    """
    try:
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        import matplotlib

        # 设置中文字体支持
        matplotlib.rcParams["font.sans-serif"] = ["SimHei"]  # 使用黑体
        matplotlib.rcParams["axes.unicode_minus"] = False  # 正常显示负号

        # 创建双图布局
        fig, axes = plt.subplots(2, 1, figsize=(12, 8))

        results["date"] = pd.to_datetime(results["date"])

        # 上图：资金曲线
        axes[0].plot(
            results["date"], results["total_value"], label="Total Value", linewidth=2
        )
        axes[0].axhline(
            y=results["total_value"].iloc[0],
            color="red",
            linestyle="--",
            alpha=0.5,
            label="Starting Cash",
        )
        axes[0].set_ylabel("Portfolio Value")
        axes[0].set_title(title)
        axes[0].legend()
        axes[0].grid(True, alpha=0.3)
        axes[0].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        axes[0].xaxis.set_major_locator(mdates.MonthLocator(interval=3))

        # 下图：累计收益率
        returns = results["total_value"].pct_change().fillna(0)
        cumulative_returns = (1 + returns).cumprod() - 1
        axes[1].plot(
            results["date"],
            cumulative_returns * 100,
            label="Cumulative Return (%)",
            color="green",
            linewidth=2,
        )
        axes[1].axhline(y=0, color="black", linestyle="-", alpha=0.3)
        axes[1].set_xlabel("Date")
        axes[1].set_ylabel("Return (%)")
        axes[1].legend()
        axes[1].grid(True, alpha=0.3)
        axes[1].xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        axes[1].xaxis.set_major_locator(mdates.MonthLocator(interval=3))

        plt.tight_layout()
        plt.show()

        # 打印统计指标
        print("\n" + "=" * 50)
        print("Backtest Summary")
        print("=" * 50)
        print(f"Starting Value: {results['total_value'].iloc[0]:,.2f}")
        print(f"Ending Value: {results['total_value'].iloc[-1]:,.2f}")
        total_return = (
            results["total_value"].iloc[-1] / results["total_value"].iloc[0] - 1
        ) * 100
        print(f"Total Return: {total_return:.2f}%")

        daily_returns = results["total_value"].pct_change().dropna()
        annual_return = daily_returns.mean() * 252 * 100  # 假设252个交易日/年
        annual_vol = daily_returns.std() * np.sqrt(252) * 100
        sharpe = annual_return / annual_vol if annual_vol > 0 else 0
        max_drawdown = (
            (results["total_value"].cummax() - results["total_value"]).max()
            / results["total_value"].cummax().max()
            * 100
        )

        print(f"Annual Return: {annual_return:.2f}%")
        print(f"Annual Volatility: {annual_vol:.2f}%")
        print(f"Sharpe Ratio: {sharpe:.2f}")
        print(f"Max Drawdown: {max_drawdown:.2f}%")
        print("=" * 50)

    except ImportError:
        print("matplotlib not installed. Results summary:")
        print(results[["date", "total_value", "cash", "positions_value"]].tail())
