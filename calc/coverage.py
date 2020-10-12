"""
Calculate coverage for station periods
"""

from main.config import *
import pandas as pd
import readdb.hourly as hourly
import sqlite3


def calc_over_range(start_station: str, start_period: str, end_station: str = None, end_period: str = None):
    """
    Perform coverage calculation over a range of station-periods, saving results to SQL table.
    :param start_station:
    :param start_period:
    :param end_station: If omitted, only one station will be used.
    :param end_period: If omitted, only one period will be used.
    :return: None
    """

    # Set defaults for end ranges
    if end_station is None:
        end_station = start_station
    if end_period is None:
        end_period = start_period

    # Init
    period_list = []

    # Pull data
    df1 = hourly.get_hourly(start_station, start_period, end_station, end_period)

    # Separate by station-period
    gb1 = df1.groupby(['station', 'period'])

    # Run calc for each section, creating record for results
    for name, group in gb1:
        # Group init
        station, period = name
        missing_hours = calc_missing(group, station, period)
        units = group['units'].min()
        unit_flag = 1 if units == 'HI' else 2

        period_list.append([station, period, missing_hours, unit_flag])

    # Add records to SQL
    save_period_list(period_list)


def calc_missing(df_period: pd.DataFrame, station: str, period: str) -> int:
    """
    Calculate missing hours from entries for a given station-period.
    :param df_period: DataFrame with the data for the station-period
    :param station:
    :param period:
    :return: Number of missing hours in the period
    """
    start = None
    end = None
    missing = False
    missing_hours = 0

    for row in df_period.itertuples():
        flag1 = row.flag1
        flag2 = row.flag2

        if flag1 == 'a':
            if start is not None:
                print('Duplicate start in %s %s' % (station, period))
            else:
                start = (row.read_date, row.read_hour)
                missing = False
        elif flag1 == '[' or flag1 == '{':
            if start is not None:
                print('Duplicate start in %s %s' % (station, period))
            else:
                start = (row.read_date, row.read_hour)
                missing = True
        elif flag1 == 'A':
            if start is None:
                print('Missing start in %s %s' % (station, period))
            elif flag2 == 'Q':  # Special case--accum period can't be used
                missing = True
                end = (row.read_date, row.read_hour)
            else:
                start = None  # reset start because nothing is missing
        elif flag1 == ']' or flag1 == '}':
            end = (row.read_date, row.read_hour)
        elif flag2 in ['Q', 'q']:
            # one hour missing
            # Q only when not at end of accum
            missing_hours += 1

        # Calc missing period if needed
        if end is not None:
            missing_hours += calc_period(start, end)
            start = None
            end = None
            missing = False

    return missing_hours


def save_period_list(list1: list):
    """
    Save a period list to SQL (internal use only).
    :param list1: The list of periods.
    :return: None
    """
    df1 = pd.DataFrame(list1)
    df1.columns = ['station', 'period', 'missing_hours', 'units_flag']

    # Add period (month) length in hours
    df1['hours'] = 31 * 24
    df1.loc[df1['period'].str[5:].str.match('04|06|09|11'), 'hours'] = 30 * 24
    febs = df1['period'].str[5:] == '02'
    fours = df1['period'].str[:4].astype('int64').mod(4) == 0
    # February in leap year
    df1.loc[febs & fours, 'hours'] = 29 * 24
    # February in other years
    df1.loc[febs & (fours == False), 'hours'] = 28 * 24

    # Calc proportion of coverage
    df1['coverage'] = 1.0 - (df1['missing_hours'] / df1['hours'])

    # Add flag for Full/Partial/Missing
    df1['cov_flag'] = 'P'
    df1.loc[df1['coverage'] == 1.0, 'cov_flag'] = 'F'
    df1.loc[df1['coverage'] == 0.0, 'cov_flag'] = 'M'

    # Save to SQL
    conn: sqlite3.Connection = sqlite3.connect(sqldbname)
    # df1.to_sql('station_period_coverage', conn, if_exists='append', index=False, method='multi', chunksize=1000)
    df1.to_sql('station_period_coverage', conn, if_exists='append', index=False, chunksize=1000)
    conn.commit()
    conn.close()

    # # Test line
    # print(df1)


def calc_period(start, end) -> int:
    """
    Given two tuples for start and end hour, determine the number of hours
    in the interval (inclusive of starting and ending hour).
    :param start: A list of date (as integer) and hour (as integer in HHMM format with 00 for minutes)
    :param end: (same)
    :return: Number of hours
    """
    sd, sh = start
    ed, eh = end

    hours = int(ed - sd) * 24 + (eh - sh) // 100 + 1

    return hours
