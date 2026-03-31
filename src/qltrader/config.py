"""
QlTrader 量化回测框架 - 配置模块

包含全局常量配置，如数据路径等。
"""

from pathlib import Path

# 数据文件路径，用于存储日线数据文件（CSV格式）
DATA_PATH = Path(r".\data\daily")
