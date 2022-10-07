import pathlib
from typing import Dict

import pandas as pd
import pendulum

from flumen.frequency.datetime import date_range
from flumen.models.frequency import Frequency
from flumen.storage.index_array import DatetimeIndexArrayStorage
from flumen.store.base import Store


class CalendarStore(Store):
    CALENDAR_STORAGE_EXTENSION = ".calendar"

    def __init__(
        self,
        root_uri: pathlib.Path,
    ) -> None:
        self._uri = self.get_uri(root_uri)
        self._data = self._load_exists_data()

    def _load_exists_data(self) -> Dict[Frequency, DatetimeIndexArrayStorage]:
        data: Dict[Frequency, DatetimeIndexArrayStorage] = {}
        for calendar_file in self._uri.glob(f"*{self.CALENDAR_STORAGE_EXTENSION}"):
            try:
                freq = Frequency.from_str(calendar_file.stem)
            except ValueError:
                continue
            data[freq] = DatetimeIndexArrayStorage(calendar_file)
        return data

    def get_freq_calendar_uri(self, freq: Frequency) -> pathlib.Path:
        return self._uri / f"{freq.raw_str}{self.CALENDAR_STORAGE_EXTENSION}"

    async def create(
        self,
        freq: Frequency,
        start_datetime: pendulum.DateTime,
        end_datetime: pendulum.DateTime,
    ) -> None:
        if freq in self._data:
            raise ValueError(f"Calendar for {freq} already exists")
        calendar_series = date_range(freq, start_datetime, end_datetime)
        self._data[freq] = DatetimeIndexArrayStorage(self.get_freq_calendar_uri(freq))
        self._data[freq].extend(calendar_series.values)
        await self._data[freq].save()

    async def get(
        self,
        freq: Frequency,
        start_datetime: pendulum.DateTime,
        end_datetime: pendulum.DateTime,
    ) -> pd.Series:
        if freq not in self._data:
            raise ValueError(f"Calendar for {freq} does not exist")
        return self._data[freq][start_datetime:end_datetime]

    async def update(
        self,
        freq: Frequency,
        end_datetime: pendulum.DateTime,
    ) -> None:
        if freq not in self._data:
            raise ValueError(f"Calendar for {freq} does not exist")
        current_end_datetime = pendulum.parse(self._data[freq][-1].astype(str))
        if end_datetime <= current_end_datetime:
            raise ValueError(
                f"End datetime {end_datetime} must be greater than"
                f" current end datetime {current_end_datetime}"
            )
        calendar_series = date_range(freq, current_end_datetime, end_datetime)
        self._data[freq].extend(calendar_series.values[1:])
        await self._data[freq].save()

    async def delete(
        self,
        freq: Frequency,
    ) -> None:
        if freq not in self._data:
            raise ValueError(f"Calendar for {freq} does not exist")
        del self._data[freq][:]
        await self._data[freq].save()
        del self._data[freq]
