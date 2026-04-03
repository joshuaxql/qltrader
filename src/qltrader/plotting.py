"""
QlTrader 量化回测框架 - 可视化模块

包含 plot_results 和 show_result 函数，用于绘制回测结果图表。
"""

from typing import Optional
from pathlib import Path
import pandas as pd
import numpy as np


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


def show_result(
    results: pd.DataFrame,
    title: str = "回测结果",
    is_save: bool = False,
    output_dir: str = "./output",
    filename: str = "backtest_result",
):
    """
    展示回测结果（包含统计信息和可视化）

    整合统计计算、数据保存和可视化功能。

    Args:
        results: run_backtest返回的结果DataFrame
        title: 图表标题
        is_save: 是否保存结果（持仓数据CSV和图片），默认False
        output_dir: 输出目录，默认"./output"
        filename: 输出文件名（不含扩展名），默认"backtest_result"

    Returns:
        dict: 包含统计指标的字典

    Example:
        results = run_backtest(...)

        # 仅显示统计和图表
        stats = show_result(results, title="双均线策略")

        # 保存持仓数据和图片
        stats = show_result(results, title="双均线策略", is_save=True,
                           output_dir="./output", filename="dual_ma")
    """
    if len(results) == 0:
        print("警告：回测结果为空")
        return {}

    # 计算统计指标
    start_date = pd.to_datetime(results["date"].iloc[0])
    end_date = pd.to_datetime(results["date"].iloc[-1])
    days = (end_date - start_date).days

    initial_value = results["total_value"].iloc[0]
    final_value = results["total_value"].iloc[-1]
    total_return = (final_value / initial_value - 1) * 100

    # 年化收益
    annual_return = (
        ((1 + total_return / 100) ** (365 / days) - 1) * 100 if days > 0 else 0
    )

    # 日收益率
    daily_returns = results["total_value"].pct_change().dropna()

    # 年化波动率
    annual_vol = daily_returns.std() * np.sqrt(252) * 100

    # 夏普比率（假设无风险利率为3%）
    risk_free_rate = 3.0  # 年化无风险利率 %
    sharpe_ratio = (
        (annual_return - risk_free_rate) / annual_vol if annual_vol > 0 else 0
    )

    # 最大回撤
    cummax = results["total_value"].cummax()
    drawdown = (results["total_value"] - cummax) / cummax
    max_drawdown = drawdown.min() * 100

    # 打印统计信息
    print("\n" + "=" * 60)
    print(f"{'回测结果统计':^50}")
    print("=" * 60)
    print(f"回测区间: {start_date.date()} 至 {end_date.date()}")
    print(f"初始资金: {initial_value:,.2f}")
    print(f"最终资金: {final_value:,.2f}")
    print(f"累计收益: {total_return:.2f}%")
    print(f"年化收益: {annual_return:.2f}%")
    print(f"年化波动率: {annual_vol:.2f}%")
    print(f"夏普比率: {sharpe_ratio:.2f}")
    print(f"最大回撤: {max_drawdown:.2f}%")
    print("=" * 60)

    # 保存持仓数据
    if is_save:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # 保存CSV
        csv_path = output_path / f"{filename}.csv"
        results.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"\n持仓数据已保存到: {csv_path}")

    # 绘制图表（包含表格）
    try:
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
        import matplotlib

        # 设置中文字体支持
        matplotlib.rcParams["font.sans-serif"] = ["SimHei"]
        matplotlib.rcParams["axes.unicode_minus"] = False

        # 创建图表：上方资金曲线，下方回撤，右侧统计表格
        fig = plt.figure(figsize=(14, 10))
        gs = fig.add_gridspec(2, 2, width_ratios=[3, 1], height_ratios=[1, 1])

        ax1 = fig.add_subplot(gs[0, 0])  # 资金曲线
        ax2 = fig.add_subplot(gs[1, 0])  # 回撤
        ax3 = fig.add_subplot(gs[:, 1])  # 统计表格

        results["date"] = pd.to_datetime(results["date"])

        # 资金曲线
        ax1.plot(
            results["date"],
            results["total_value"],
            label="账户总值",
            linewidth=2,
            color="#1f77b4",
        )
        ax1.axhline(
            y=initial_value, color="red", linestyle="--", alpha=0.5, label="初始资金"
        )
        ax1.set_ylabel("账户价值")
        ax1.set_title(title, fontsize=14, fontweight="bold")
        ax1.legend(loc="upper left")
        ax1.grid(True, alpha=0.3)
        ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=6))

        # 回撤曲线
        ax2.fill_between(
            results["date"], drawdown * 100, 0, alpha=0.3, color="red", label="回撤"
        )
        ax2.plot(results["date"], drawdown * 100, color="red", linewidth=1)
        ax2.set_xlabel("日期")
        ax2.set_ylabel("回撤 (%)")
        ax2.legend(loc="lower left")
        ax2.grid(True, alpha=0.3)
        ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
        ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=6))

        # 统计表格
        ax3.axis("off")
        table_data = [
            ["回测区间", f"{start_date.date()}至{end_date.date()}"],
            ["初始资金", f"{initial_value:,.2f}"],
            ["最终资金", f"{final_value:,.2f}"],
            ["累计收益", f"{total_return:.2f}%"],
            ["年化收益", f"{annual_return:.2f}%"],
            ["年化波动率", f"{annual_vol:.2f}%"],
            ["夏普比率", f"{sharpe_ratio:.2f}"],
            ["最大回撤", f"{max_drawdown:.2f}%"],
        ]

        table = ax3.table(
            cellText=table_data,
            colLabels=["指标", "数值"],
            loc="center",
            cellLoc="center",
            colWidths=[0.4, 0.6],
        )
        table.auto_set_font_size(False)
        table.set_fontsize(11)
        table.scale(1.2, 1.8)

        # 设置表头样式
        for i in range(2):
            table[(0, i)].set_facecolor("#4472c4")
            table[(0, i)].set_text_props(color="white", fontweight="bold")

        # 设置表格单元格样式
        for i in range(1, len(table_data) + 1):
            for j in range(2):
                if i % 2 == 0:
                    table[(i, j)].set_facecolor("#d9e2f3")

        plt.tight_layout()

        # 保存图片
        img_path = None
        if is_save:
            output_path = Path(output_dir)
            output_path.mkdir(parents=True, exist_ok=True)
            img_path = output_path / f"{filename}.png"
            plt.savefig(img_path, dpi=300, bbox_inches="tight")
            print(f"图表已保存到: {img_path}")

        plt.show()

    except ImportError:
        print("\n警告：matplotlib 未安装，无法绘制图表")

    # 返回统计指标
    stats = {
        "start_date": start_date,
        "end_date": end_date,
        "initial_value": initial_value,
        "final_value": final_value,
        "total_return": total_return,
        "annual_return": annual_return,
        "annual_volatility": annual_vol,
        "sharpe_ratio": sharpe_ratio,
        "max_drawdown": max_drawdown,
    }

    return stats
