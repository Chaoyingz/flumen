from typing import Any, Dict

import pandas as pd
from pendulum import DateTime
from pydantic import BaseModel, root_validator, validator

from flumen.frequency.datetime import date_range
from flumen.models.frequency import Frequency


class TimeSeries(BaseModel):
    entity_id: str
    feature_id: str
    freq: Frequency
    start_datetime: DateTime
    end_datetime: DateTime
    values: pd.Series

    class Config:
        arbitrary_types_allowed = True

    @root_validator
    def validate_datetime(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if values["end_datetime"] < values["start_datetime"]:
            raise ValueError("start_datetime must be less than end_datetime")
        return values

    @validator("values")
    def validate_values(cls, v: pd.Series, values: Dict[str, Any]) -> pd.Series:
        if not isinstance(v.index.dtype, pd.DatetimeTZDtype):
            raise ValueError("series index must be datetime with timezone")

        datetime_index = date_range(
            freq=values["freq"],
            start_datetime=values["start_datetime"],
            end_datetime=values["end_datetime"],
            from_tz=v.index.tz,
        )

        if v.index.tz != "UTC":
            v.index = v.index.tz_convert("UTC")

        if not v.index.equals(datetime_index):
            raise ValueError("series index must match date_range")
        return v
