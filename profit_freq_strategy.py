import pandas as pd
import numpy as np
from pathlib import Path
from qltrader import run_backtest, plot_results, order_shares, schedule
import warnings

warnings.filterwarnings("ignore")

DATA_PATH = Path(r".\data\daily")
LOOKBACK_PERIOD = 40
THRESHOLD = 0.02
NUM_STOCKS = 20

_price_matrix = None
_profit_freq = None


def get_available_stocks():
    all_files = list(DATA_PATH.glob("*.csv"))
    stocks = []
    for f in all_files:
        stock = f.stem
        df = pd.read_csv(f)
        if len(df) > 0:
            stocks.append(stock)
    return stocks


def load_all_prices(securities: list, start_date: str, end_date: str):
    price_dict = {}
    dates_set = set()
    for sec in securities:
        file_path = DATA_PATH / f"{sec}.csv"
        if not file_path.exists():
            continue
        df = pd.read_csv(file_path)
        df["date"] = pd.to_datetime(df["date"])
        mask = (df["date"] >= start_date) & (df["date"] <= end_date)
        df = df[mask].copy()
        if len(df) < LOOKBACK_PERIOD + 10:
            continue
        df = df.set_index("date")
        price_dict[sec] = df[["open", "high", "low", "close", "volume"]]
        dates_set.update(df.index.tolist())

    all_dates = sorted(list(dates_set))
    price_df = pd.DataFrame({sec: data["close"] for sec, data in price_dict.items()})
    price_df = price_df.reindex(all_dates)

    open_df = pd.DataFrame({sec: data["open"] for sec, data in price_dict.items()})
    open_df = open_df.reindex(all_dates)

    return price_df, open_df, list(price_dict.keys())


def calculate_profit_frequency(price_matrix: pd.DataFrame) -> pd.DataFrame:
    returns = price_matrix.pct_change()
    market_mean = returns.mean(axis=1)
    excess_returns = returns.sub(market_mean, axis=0)
    above_threshold = (excess_returns > THRESHOLD).astype(int)
    profit_freq = (
        above_threshold.rolling(
            window=LOOKBACK_PERIOD, min_periods=LOOKBACK_PERIOD
        ).sum()
        / LOOKBACK_PERIOD
    )
    return profit_freq


def initialize(context):
    global _price_matrix, _profit_freq

    context.set_universe(list(_price_matrix.columns)[:100])

    schedule(monthly_rebalance, date_rule="month", time_rule="open")


def monthly_rebalance(context, data):
    global _price_matrix, _profit_freq

    current_dt = context.current_dt
    date_str = current_dt.strftime("%Y-%m-%d")

    if date_str not in _price_matrix.index:
        return

    pf_dates = _profit_freq.loc[:date_str]
    if len(pf_dates) == 0:
        return

    pf_row = pf_dates.iloc[-1]
    valid_stocks = pf_row.dropna()

    if len(valid_stocks) < NUM_STOCKS:
        return

    top_stocks = valid_stocks.nsmallest(NUM_STOCKS).index.tolist()

    current_positions = context.portfolio.positions.positions.copy()

    for sec, pos in current_positions.items():
        if pos["amount"] > 0 and sec not in top_stocks:
            order_shares(sec, -pos["amount"])

    total_value = context.portfolio.total_value

    for sec in top_stocks:
        if date_str in _price_matrix.index:
            price = _price_matrix.loc[date_str, sec]
            if pd.isna(price) or price <= 0:
                continue

            target_value = total_value / NUM_STOCKS * 0.98
            shares = int(target_value / price / 100) * 100

            if shares > 0:
                order_shares(sec, shares)


def backtest_profit_frequency(
    start_date: str, end_date: str, capital: float = 1000000.0
):
    global _price_matrix, _profit_freq

    print("加载股票列表...")
    securities = get_available_stocks()
    print(f"可用股票数: {len(securities)}")

    print("加载价格数据...")
    price_matrix, open_matrix, stock_list = load_all_prices(
        securities[:500], start_date, end_date
    )

    print("计算盈利频率因子...")
    _profit_freq = calculate_profit_frequency(price_matrix)
    _price_matrix = price_matrix

    print("开始回测...")

    results = run_backtest(
        start_date=start_date,
        end_date=end_date,
        initialize=initialize,
        capital_base=capital,
    )

    return results


if __name__ == "__main__":
    START_DATE = "2020-01-01"
    END_DATE = "2026-01-01"
    CAPITAL = 1000000.0

    print("=" * 50)
    print("盈利频率因子策略回测")
    print("=" * 50)
    print(f"回测区间: {START_DATE} 至 {END_DATE}")
    print(f"初始资金: {CAPITAL:,.0f}")
    print(f"持仓数量: {NUM_STOCKS} 只")
    print(f"回看期: {LOOKBACK_PERIOD} 天")
    print(f"超额收益阈值: {THRESHOLD * 100}%")
    print("=" * 50)

    results = backtest_profit_frequency(START_DATE, END_DATE, CAPITAL)

    print("\n回测完成!")
    print(f"总交易日数: {len(results)}")

    if len(results) > 0:
        results = pd.DataFrame(results)

        plot_results(results, "盈利频率因子策略回测")

        print("\n" + "=" * 50)
        print(f"起始资金: {results['total_value'].iloc[0]:,.2f}")
        print(f"结束资金: {results['total_value'].iloc[-1]:,.2f}")

        total_return = (
            results["total_value"].iloc[-1] / results["total_value"].iloc[0] - 1
        ) * 100
        print(f"总收益率: {total_return:.2f}%")

        daily_returns = results["total_value"].pct_change().dropna()
        annual_return = daily_returns.mean() * 252 * 100
        annual_vol = daily_returns.std() * np.sqrt(252) * 100
        sharpe = annual_return / annual_vol if annual_vol > 0 else 0

        print(f"年化收益率: {annual_return:.2f}%")
        print(f"年化波动率: {annual_vol:.2f}%")
        print(f"夏普比率: {sharpe:.2f}")

        cummax = results["total_value"].cummax()
        drawdown = (cummax - results["total_value"]) / cummax
        max_drawdown = drawdown.max() * 100
        print(f"最大回撤: {max_drawdown:.2f}%")
        print("=" * 50)
