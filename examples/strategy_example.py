"""
QLTrader 策略示例

本示例展示如何编写一个简单的双均线策略：
- 短期均线上穿长期均线时买入
- 短期均线下穿长期均线时卖出

使用方式：
python examples/strategy_example.py
"""

import sys
from pathlib import Path

# 添加 src 目录到 Python 路径
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from qltrader import run_backtest, show_result, order_target_percent, schedule


# 策略参数
SHORT_WINDOW = 5  # 短期均线周期
LONG_WINDOW = 20  # 长期均线周期
UNIVERSE = [
    "sh600000",
    # "sh600036",
    # "sh601318",
]  # 股票池（浦发银行、招商银行、中国平安）


def initialize(context):
    """
    初始化函数，设置股票池和定时任务
    """
    # 设置股票池
    context.set_universe(UNIVERSE)

    # 记录持仓状态
    context.position_status = {sec: 0 for sec in UNIVERSE}

    # 每日收盘执行策略
    schedule(trade, date_rule="daily", time_rule="close")

    print(f"策略初始化完成，股票池: {UNIVERSE}")
    print(f"短期均线周期: {SHORT_WINDOW}, 长期均线周期: {LONG_WINDOW}")


def trade(context, data):
    """
    交易逻辑：双均线策略
    - 短期均线上穿长期均线时买入
    - 短期均线下穿长期均线时卖出
    """
    for sec in context.universe:
        # 检查是否可交易
        if not data.can_trade(sec):
            continue

        # 获取历史收盘价
        hist = data.history(sec, bar_count=LONG_WINDOW + 1, fields="close")
        if len(hist) < LONG_WINDOW:
            continue

        # 计算均线
        short_ma = sum(hist[-SHORT_WINDOW:]) / SHORT_WINDOW
        long_ma = sum(hist[-LONG_WINDOW:]) / LONG_WINDOW

        # 前一天的均线
        prev_short_ma = sum(hist[-(SHORT_WINDOW + 1) : -1]) / SHORT_WINDOW
        long_hist = hist[-LONG_WINDOW:]
        prev_long_ma = sum(long_hist[:-1]) / (LONG_WINDOW - 1)

        # 计算持仓比例
        current_position = context.position_status.get(sec, 0)

        # 金叉买入：短期均线上穿长期均线
        if prev_short_ma <= prev_long_ma and short_ma > long_ma:
            if current_position == 0:
                print(
                    f"{context.current_dt}  金叉买入信号: {sec}, 短期MA={short_ma:.2f}, 长期MA={long_ma:.2f}"
                )
                # 等权买入
                order_target_percent(context, sec, 1.0 / len(UNIVERSE))
                context.position_status[sec] = 1

        # 死叉卖出：短期均线下穿长期均线
        elif prev_short_ma >= prev_long_ma and short_ma < long_ma:
            if current_position == 1:
                print(
                    f"{context.current_dt}  死叉卖出信号: {sec}, 短期MA={short_ma:.2f}, 长期MA={long_ma:.2f}"
                )
                # 清仓
                order_target_percent(context, sec, 0)
                context.position_status[sec] = 0


def handle_data(context, data):
    """
    日频策略函数（可选，这里使用 schedule 定时任务）
    """
    pass


if __name__ == "__main__":
    print("=" * 60)
    print("QLTrader 双均线策略示例")
    print("=" * 60)

    # 运行回测
    results = run_backtest(
        start_date="2024-01-01",
        end_date="2024-07-31",
        initialize=initialize,
        handle_data=None,  # 使用 schedule 定时任务，不需要 handle_data
        capital_base=1000000.0,  # 初始资金100万
    )

    # 展示回测结果（包含统计信息和可视化）
    show_result(
        results,
        title="双均线策略回测结果",
        is_save=True,
        output_dir="./output",
        filename="dual_ma_strategy",
    )
