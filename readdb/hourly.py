"""
Routines to pull hourly data
"""


from main.config import *
import pandas as pd
import sqlite3


def get_hourly(start_station: str, start_period: str, end_station: str = None, end_period: str = None) -> pd.DataFrame:
    """
    Get hourly precipitation data for range of stations and periods.
    :param start_station: Start of station range
    :param start_period: Start of period range
    :param end_station: End of station range
    :param end_period: End of period range
    :return:
    """

    # Set defaults for end ranges
    if end_station is None:
        end_station = start_station
    if end_period is None:
        end_period = start_period

    querystr = """
        SELECT station, state_code, units, read_date, read_hour,
            amount, flag1, flag2, period
        FROM hourly_raw
        WHERE station BETWEEN ? AND ?
        AND period BETWEEN ? AND ?
        ORDER BY station, period, read_date, read_hour
    """

    conn: sqlite3.Connection = sqlite3.connect(sqldbname)

    df = pd.read_sql_query(querystr, conn, params=[start_station, end_station, start_period, end_period])
    conn.close()

    return df
