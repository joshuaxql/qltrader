"""
QlTrader 量化回测框架 - 可视化模块

包含 plot_results 函数，用于绘制回测结果图表。
"""

from typing import Optional
from pathlib import Path
import pandas as pd


def plot_results(
    results: pd.DataFrame,
    title: str = "Backtest Results",
    save_path: Optional[str] = None,
):
    """
    绘制回测结果图表

    可视化回测结果，包括资金曲线、累计收益率，并打印统计指标。
    支持将图表保存到指定路径。

    Args:
        results: run_backtest返回的结果DataFrame
        title: 图表标题
        save_path: 保存图像的文件路径（可选），如"./output/my_strategy.png"
                  如果提供，图像将保存到该路径，同时仍会显示图表

    Statistics:
        - 起始资金 / 结束资金
        - 总收益率
        - 年化收益率
        - 年化波动率
        - 夏普比率
        - 最大回撤

    Example:
        results = run_backtest(...)
        # 只显示图表
        plot_results(results, title="My Strategy")
        # 显示并保存图表
        plot_results(results, title="My Strategy", save_path="./output/my_strategy.png")
    """
    try:
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        import matplotlib
        import numpy as np

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

        # 保存图像到指定路径
        if save_path is not None:
            output_dir = Path(save_path).parent
            output_dir.mkdir(parents=True, exist_ok=True)
            plt.savefig(save_path, dpi=300, bbox_inches="tight")
            print(f"图表已保存到: {save_path}")

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
