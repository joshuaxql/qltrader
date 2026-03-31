"""
QlTrader 量化回测框架 - 数据访问模块

包含 Data 类，提供行情数据查询功能。
"""

from datetime import datetime
from typing import Dict, List, Union
import numpy as np
import pandas as pd


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
            price = data.current("000001.SZ", "close")  # 获取收盘价
            prices = data.current(["000001.SZ", "000002.SZ"], "close")  # 获取多只股票收盘价
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
            # 获取最近20天的收盘价
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
