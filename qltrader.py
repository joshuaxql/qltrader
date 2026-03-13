import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Callable, Optional, Union
import warnings

warnings.filterwarnings("ignore")


DATA_PATH = Path(r".\data\daily")


class Position:
    def __init__(self):
        self.positions: Dict[str, dict] = {}

    def __getitem__(self, sec):
        return self.positions.get(sec, {"amount": 0, "avg_cost": 0})

    def update_position(self, sec: str, amount: float, avg_cost: float):
        if sec not in self.positions:
            self.positions[sec] = {"amount": 0, "avg_cost": 0}
        current = self.positions[sec]
        if amount > 0:
            if current["amount"] > 0:
                total_cost = current["amount"] * current["avg_cost"] + amount * avg_cost
                total_amount = current["amount"] + amount
                current["avg_cost"] = total_cost / total_amount
                current["amount"] = total_amount
            else:
                current["avg_cost"] = avg_cost
                current["amount"] = amount
        elif amount < 0:
            current["amount"] += amount
            if current["amount"] <= 0:
                current["amount"] = 0
                current["avg_cost"] = 0


class Portfolio:
    def __init__(self):
        self.positions = Position()
        self._total_value = 1000000.0
        self._cash = 1000000.0
        self._starting_cash = 1000000.0

    @property
    def total_value(self):
        return self._total_value

    @property
    def cash(self):
        return self._cash

    @property
    def starting_cash(self):
        return self._starting_cash

    @property
    def positions_value(self):
        return self._total_value - self._cash

    def update_cash(self, amount: float):
        self._cash += amount

    def update_total_value(self, prices: Dict[str, float]):
        positions_value = 0
        for sec, pos in self.positions.positions.items():
            if sec in prices and pos["amount"] > 0:
                positions_value += pos["amount"] * prices[sec]
        self._total_value = self._cash + positions_value


class Context:
    def __init__(self):
        self.portfolio = Portfolio()
        self.current_data = None
        self.current_dt = None
        self._universe: List[str] = []
        self._data_cache: Dict[str, pd.DataFrame] = {}
        self._orders: List[dict] = []
        self._scheduled_tasks: List[dict] = []

    @property
    def universe(self):
        return self._universe

    def set_universe(self, securities: List[str]):
        self._universe = securities

    def get_all_securities(self):
        all_files = list(DATA_PATH.glob("*.csv"))
        return [f.stem for f in all_files]


def schedule(
    schedule_func: Callable, date_rule: str = "daily", time_rule: str = "open"
):
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
    def __init__(self, data_cache: Dict[str, pd.DataFrame], current_dt: datetime):
        self._data_cache = data_cache
        self.current_dt = current_dt

    def current(
        self, securities: Union[str, List[str]], fields: Union[str, List[str]] = "close"
    ):
        if isinstance(securities, str):
            securities = [securities]
        if isinstance(fields, str):
            fields = [fields]

        result = {}
        for sec in securities:
            if sec not in self._data_cache:
                result[sec] = {f: np.nan for f in fields}
                continue

            df = self._data_cache[sec]
            date_str = self.current_dt.strftime("%Y-%m-%d")

            try:
                row = df[df["date"] == date_str]
                if len(row) == 0:
                    result[sec] = {f: np.nan for f in fields}
                else:
                    result[sec] = {}
                    for f in fields:
                        result[sec][f] = row.iloc[0][f]
            except:
                result[sec] = {f: np.nan for f in fields}

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
                date_index = pd.to_datetime(df["date"])
                df = df.set_index(date_index)
                df = df.sort_index()

                end_loc = df.index.get_indexer([self.current_dt], method="pad")[0]
                start_loc = max(0, end_loc - bar_count + 1)

                result[sec] = {}
                for f in fields:
                    values = df.iloc[start_loc : end_loc + 1][f].tolist()
                    if len(values) < bar_count:
                        values = [np.nan] * (bar_count - len(values)) + values
                    result[sec][f] = values
            except:
                result[sec] = {f: [np.nan] * bar_count for f in fields}

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
        if isinstance(securities, str):
            return self._can_trade_single(securities)

        return {sec: self._can_trade_single(sec) for sec in securities}

    def _can_trade_single(self, sec: str) -> bool:
        if sec not in self._data_cache:
            return False

        df = self._data_cache[sec]
        date_str = self.current_dt.strftime("%Y-%m-%d")

        try:
            row = df[df["date"] == date_str]
            if len(row) == 0:
                return False
            volume = row.iloc[0]["volume"]
            return volume > 0
        except:
            return False


_current_context: Optional[Context] = None


def _set_current_context(context: Context):
    global _current_context
    _current_context = context


def order_shares(sec: str, amount: int):
    global _current_context
    if _current_context:
        _current_context._orders.append({"sec": sec, "amount": amount})


def order_target_percent(context: Context, sec: str, percent: float):
    if context is None or not hasattr(context, "portfolio"):
        return

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

    target_value = context.portfolio.total_value * (1 - 0.002) * percent
    current_value = context.portfolio.positions[sec]["amount"] * prices[sec]

    trade_value = target_value - current_value
    trade_shares = int(trade_value / prices[sec] / 100) * 100

    if abs(trade_shares) >= 100:
        order_shares(sec, trade_shares)


def order_percent(context: Context, sec: str, percent: float):
    if context is None or not hasattr(context, "portfolio"):
        return

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

    target_value = context.portfolio.total_value * (1 - 0.002) * percent
    trade_shares = int(target_value / prices[sec] / 100) * 100

    if abs(trade_shares) >= 100:
        order_shares(sec, trade_shares)


class QlTrader:
    def __init__(self):
        self.context = Context()
        self._price_data: Dict[str, pd.DataFrame] = {}
        self._trade_dates: List[datetime] = []
        self._slippage = 0.002
        self._commission = 0.0003

    def _load_data(self, securities: List[str], start_date: str, end_date: str):
        all_dates = set()
        for sec in securities:
            file_path = DATA_PATH / f"{sec}.csv"
            if not file_path.exists():
                print(f"Warning: {sec} data not found")
                continue

            df = pd.read_csv(file_path)
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

            mask = (df["date"] >= start_date) & (df["date"] <= end_date)
            df = df[mask].copy()

            if len(df) == 0:
                print(f"Warning: {sec} has no data in date range")
                continue

            self._price_data[sec] = df.reset_index(drop=True)
            all_dates.update(df["date"].tolist())

        sorted_dates = sorted(list(all_dates))
        self._trade_dates = [datetime.strptime(d, "%Y-%m-%d") for d in sorted_dates]
        self.context._data_cache = self._price_data

    def _create_data_obj(self, current_dt: datetime):
        return Data(self._price_data, current_dt)

    def _should_run_scheduled(
        self, task: dict, trade_date: datetime, prev_date: Optional[datetime]
    ) -> bool:
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
        handle_data: Callable,
        before_trading_start: Optional[Callable] = None,
    ):
        self._price_data = {}
        self._trade_dates = []

        temp_context = Context()
        _set_current_context(temp_context)
        initialize(temp_context)
        securities = temp_context.universe
        scheduled_tasks = temp_context._scheduled_tasks.copy()

        self.context.set_universe(securities)
        self.context._scheduled_tasks = scheduled_tasks

        if len(securities) == 0:
            raise ValueError("Please call set_universe() in initialize()")

        self._load_data(securities, start_date, end_date)

        _set_current_context(self.context)
        initialize(self.context)

        results = []
        prev_date = None
        last_month = -1

        for trade_date in self._trade_dates:
            self.context.current_dt = trade_date
            data_obj = self._create_data_obj(trade_date)

            if before_trading_start:
                before_trading_start(self.context, data_obj)

            self.context._orders = []

            for task in scheduled_tasks:
                if task["time_rule"] == "open":
                    if self._should_run_scheduled(task, trade_date, prev_date):
                        task["func"](self.context, data_obj)

            if len(self.context._orders) > 0:
                self._process_orders(trade_date, data_obj, "open")
                self.context._orders = []

            _set_current_context(self.context)
            handle_data(self.context, data_obj)

            for task in scheduled_tasks:
                if task["time_rule"] == "close":
                    if self._should_run_scheduled(task, trade_date, prev_date):
                        task["func"](self.context, data_obj)

            if len(self.context._orders) > 0:
                self._process_orders(trade_date, data_obj, "close")
                self.context._orders = []

            prices = {}
            for sec in securities:
                prices[sec] = data_obj.current(sec, "close")

            self.context.portfolio.update_total_value(prices)

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

    def _process_orders(
        self, trade_date: datetime, data_obj: Data, price_field: str = "close"
    ):
        for order in self.context._orders:
            sec = order["sec"]
            amount = order["amount"]

            if abs(amount) < 100:
                continue

            price = data_obj.current(sec, price_field)
            if np.isnan(price) or price <= 0:
                continue

            if not data_obj.can_trade(sec):
                continue

            trade_amount = int(amount / 100) * 100

            if trade_amount > 0:
                exec_price = price * (1 + self._slippage)
                trade_value = trade_amount * exec_price
                commission = trade_value * self._commission

                if trade_value + commission > self.context.portfolio.cash:
                    max_shares = int(
                        self.context.portfolio.cash
                        / (exec_price * (1 + self._commission))
                    )
                    trade_amount = int(max_shares / 100) * 100

                if trade_amount > 0:
                    exec_price = price * (1 + self._slippage)
                    trade_value = trade_amount * exec_price
                    commission = trade_value * self._commission

                    self.context.portfolio.positions.update_position(
                        sec, trade_amount, exec_price
                    )
                    self.context.portfolio._cash -= trade_value + commission

            elif trade_amount < 0:
                current_pos = self.context.portfolio.positions[sec]["amount"]
                trade_amount = max(trade_amount, -current_pos)

                if trade_amount < 0:
                    exec_price = price * (1 - self._slippage)
                    trade_value = abs(trade_amount) * exec_price
                    commission = trade_value * self._commission

                    self.context.portfolio.positions.update_position(
                        sec, trade_amount, exec_price
                    )
                    self.context.portfolio._cash += trade_value - commission


def run_backtest(
    start_date: str,
    end_date: str,
    initialize: Callable,
    handle_data: Callable,
    before_trading_start: Optional[Callable] = None,
    capital_base: float = 1000000.0,
):
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
    file_path = DATA_PATH / f"{security}.csv"
    if not file_path.exists():
        raise FileNotFoundError(f"Data for {security} not found")

    df = pd.read_csv(file_path)
    df["date"] = pd.to_datetime(df["date"])

    mask = (df["date"] >= start_date) & (df["date"] <= end_date)
    df = df[mask].copy()

    if isinstance(fields, str):
        return df[["date"] + [fields]]
    return df[["date"] + fields]


def plot_results(results: pd.DataFrame, title: str = "Backtest Results"):
    try:
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        import matplotlib

        matplotlib.rcParams["font.sans-serif"] = ["SimHei"]  # 设置中文字体
        matplotlib.rcParams["axes.unicode_minus"] = False  # 正常显示负号
        fig, axes = plt.subplots(2, 1, figsize=(12, 8))

        results["date"] = pd.to_datetime(results["date"])

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
        annual_return = daily_returns.mean() * 252 * 100
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
