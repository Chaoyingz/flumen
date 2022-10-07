import typing

import pytest

from flumen.models.frequency import Frequency, FrequencyUnit


@typing.no_type_check
def test_frequency_unit_values() -> None:
    assert len(FrequencyUnit.values()) == len(FrequencyUnit)


@typing.no_type_check
def test_frequency_unit_market_exchange() -> None:
    assert FrequencyUnit.SSED.market_exchange == "SSE"
    assert FrequencyUnit.SSET.market_exchange == "SSE"
    assert FrequencyUnit.HKEXD.market_exchange == "HKEX"
    with pytest.raises(ValueError):
        FrequencyUnit.D.market_exchange


@typing.no_type_check
def test_frequency_unit_market_unit() -> None:
    assert FrequencyUnit.SSED.market_unit == "D"
    assert FrequencyUnit.SSET.market_unit == "T"
    assert FrequencyUnit.HKEXD.market_unit == "D"
    with pytest.raises(ValueError):
        FrequencyUnit.D.market_unit


@pytest.mark.parametrize(
    "freq_str, freq_obj",
    [
        ("S", Frequency(raw_str="S", step=1, unit=FrequencyUnit.S)),
        ("T", Frequency(raw_str="T", step=1, unit=FrequencyUnit.T)),
        ("H", Frequency(raw_str="H", step=1, unit=FrequencyUnit.H)),
        ("D", Frequency(raw_str="D", step=1, unit=FrequencyUnit.D)),
        ("W", Frequency(raw_str="W", step=1, unit=FrequencyUnit.W)),
        ("M", Frequency(raw_str="M", step=1, unit=FrequencyUnit.M)),
        ("Q", Frequency(raw_str="Q", step=1, unit=FrequencyUnit.Q)),
        ("Y", Frequency(raw_str="Y", step=1, unit=FrequencyUnit.Y)),
        ("SSED", Frequency(raw_str="SSED", step=1, unit=FrequencyUnit.SSED)),
        ("2S", Frequency(raw_str="2S", step=2, unit=FrequencyUnit.S)),
        ("2SSED", Frequency(raw_str="2SSED", step=2, unit=FrequencyUnit.SSED)),
    ],
)
def test_parse_frequency(freq_str: str, freq_obj: Frequency) -> None:
    assert Frequency.from_str(freq_str) == freq_obj


def test_parse_frequency_invalid() -> None:
    with pytest.raises(ValueError) as excinfo:
        Frequency.from_str("INVALID")
    assert "Invalid frequency unit: INVALID" == str(excinfo.value)
    with pytest.raises(ValueError) as excinfo:
        Frequency.from_str("2")
    assert "Invalid frequency: 2" == str(excinfo.value)
