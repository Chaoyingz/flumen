import pathlib

from flumen.models.timeseries import TimeSeries
from flumen.store.calenadar import CalendarStore


class TimeSeriesStore:
    def __init__(self, uri: str) -> None:
        self.root_uri = pathlib.Path(uri)
        self.calendar_store = CalendarStore(self.root_uri)

    async def insert_one(self, ts: TimeSeries) -> None:
        await self.calendar_store.update(
            ts.freq,
            start_datetime=ts.start_datetime,
            end_datetime=ts.end_datetime,
            upsert=True,
        )
        # calendar_series = await self.calendar_store.find(
        #     ts.freq,
        #     start_datetime=ts.start_datetime,
        #     end_datetime=ts.end_datetime,
        # )
        # print()

    async def find_one(self, ts: TimeSeries) -> TimeSeries:
        ...
