from typing import Any, Dict

import pandas as pd
from pendulum import DateTime
from pydantic import BaseModel, root_validator, validator

from flumen.frequency.datetime import date_range
from flumen.models.frequency import Frequency


class Item(BaseModel):
    entity_id: str
    field_id: str
    freq: Frequency
    start_datetime: DateTime
    end_datetime: DateTime
    series: pd.Series

    class Config:
        arbitrary_types_allowed = True

    @root_validator
    def validate_datetime(cls, values: Dict[str, Any]) -> Dict[str, Any]:
        if values["end_datetime"] < values["start_datetime"]:
            raise ValueError("start_datetime must be less than end_datetime")
        return values

    @validator("series")
    def validate_series(cls, v: pd.Series, values: Dict[str, Any]) -> pd.Series:
        if v.index.dtype != "datetime64[ns, UTC]":
            raise ValueError("series index must be datetime64[ns, UTC]")
        datetime_index = date_range(
            freq=values["freq"],
            start_datetime=values["start_datetime"],
            end_datetime=values["end_datetime"],
        )
        if not v.index.equals(datetime_index):
            raise ValueError("series index must match date_range")
        return v
