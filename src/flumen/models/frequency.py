import re
from typing import List

from aenum import Enum
from pydantic import BaseModel

_MARKET_EXCHANGE_SLICE = slice(0, -1)
_MARKET_UNIT_SLICE = slice(-1, None)
FREQUENCY_REGEX = re.compile(r"(?P<step>\d*)(?P<unit>[A-Z]+)")
PANDAS_FREQ_UNIT = 1
MARKET_FREQ_UNIT = 2


class FrequencyUnit(Enum):
    _init_ = "value unit_type __doc__"

    S = "S", PANDAS_FREQ_UNIT, "Secondly frequency"
    T = "T", PANDAS_FREQ_UNIT, "Minutely frequency"
    H = "H", PANDAS_FREQ_UNIT, "Hourly frequency"
    D = "D", PANDAS_FREQ_UNIT, "Daily frequency"
    W = "W", PANDAS_FREQ_UNIT, "Weekly frequency"
    M = "M", PANDAS_FREQ_UNIT, "Monthly frequency"
    Q = "Q", PANDAS_FREQ_UNIT, "Quarterly frequency"
    Y = "Y", PANDAS_FREQ_UNIT, "Yearly frequency"

    # Exchange
    SSED = "SSED", MARKET_FREQ_UNIT, "SSE Daily frequency"
    SSET = "SSET", MARKET_FREQ_UNIT, "SSE Minutely frequency"
    HKEXD = "HKEXD", MARKET_FREQ_UNIT, "HKEX Daily frequency"

    @classmethod
    def values(cls) -> List[str]:
        return [i.value for i in cls.__iter__()]

    @property
    def market_exchange(self) -> str:
        if self.unit_type == MARKET_FREQ_UNIT:
            return self.value[_MARKET_EXCHANGE_SLICE]
        raise ValueError(f"Frequency unit {self.value} is not an exchange frequency")

    @property
    def market_unit(self) -> str:
        if self.unit_type == MARKET_FREQ_UNIT:
            return self.value[_MARKET_UNIT_SLICE]
        raise ValueError(f"Frequency unit {self.value} is not an exchange frequency")


class Frequency(BaseModel):
    raw_str: str
    step: int
    unit: FrequencyUnit

    @classmethod
    def from_str(cls, freq: str) -> "Frequency":
        freq = freq.upper()
        match = FREQUENCY_REGEX.match(freq)
        if match:
            step = int(match.group("step") or 1)
            unit = match.group("unit")
            if unit not in FrequencyUnit.values():
                raise ValueError(f"Invalid frequency unit: {unit}")
            return Frequency(
                raw_str=freq,
                step=step,
                unit=unit,
            )
        else:
            raise ValueError(f"Invalid frequency: {freq}")

    def to_str(self) -> str:
        if self.unit.unit_type == PANDAS_FREQ_UNIT:
            return self.raw_str
        return f"{self.step}{self.unit.market_unit}"

    def __hash__(self) -> int:
        return hash(self.raw_str)
