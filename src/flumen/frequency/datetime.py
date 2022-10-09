from typing import Optional

import pandas as pd
import pandas_market_calendars as mcal
import pendulum

from flumen.models.frequency import PANDAS_FREQ_UNIT, Frequency


def date_range(
    freq: Frequency,
    start_datetime: pendulum.DateTime,
    end_datetime: pendulum.DateTime,
    from_tz: Optional[str] = "UTC",
) -> pd.DatetimeIndex:

    start_datetime = start_datetime.in_tz("UTC").to_datetime_string()
    end_datetime = end_datetime.in_tz("UTC").to_datetime_string()

    if freq.unit.unit_type == PANDAS_FREQ_UNIT:
        dts = pd.date_range(start_datetime, end_datetime, freq=freq.raw_str, tz=from_tz)
    else:
        exchange = mcal.get_calendar(freq.unit.market_exchange)
        if pd.Timedelta(freq.to_str()) >= pd.Timedelta("1D"):
            dts = exchange.valid_days(
                start_date=start_datetime, end_date=end_datetime, tz=exchange.tz
            )
        else:
            schedule = exchange.schedule(start_datetime, end_datetime)
            dts = mcal.date_range(schedule, frequency=freq.to_str(), tz=exchange.tz)
    return dts.tz_convert("UTC")
