"""
Load tables for good periods for stations
"""

from main.config import *
import sqlite3


def load_station_period_d():
    """
    Load table of periods with complete daily data.
    :return: None
    """

    querystr = '''
        INSERT INTO station_period_d
        SELECT station,
            period,
            SUM(amount),
            CASE WHEN MAX(units) = 'HI' THEN 1 ELSE 2 END AS units_flag
        FROM daily_raw dr
        WHERE NOT EXISTS (
            SELECT 1 FROM daily_raw dr2
            WHERE dr2.station = dr.station
            AND dr2.period = dr.period
            AND flag1 IN ('I','P')
        )
        GROUP BY station, period
    '''

    conn: sqlite3.Connection = sqlite3.connect(sqldbname)
    curr = conn.cursor()
    curr.execute(querystr)

    conn.commit()
    conn.close()


def load_station_period_h():
    """
    Load table of periods with complete hourly data.
    :return: None
    """
    querystr = '''
        INSERT INTO station_period
        SELECT DISTINCT station,
            period,
            SUM(amount),
            CASE WHEN MAX(units) = 'HI' THEN 1 ELSE 2 END AS units_flag
        FROM hourly_raw hr
        WHERE NOT EXISTS (
            SELECT 1 FROM hourly_raw hr2
            WHERE hr2.station = hr.station
            AND hr2.period hr.period
            AND (
                flag1 IN ('a','A',',','[',']','{','}')
                OR flag2 IN ('Q','q')
            )
        )
    '''

    conn: sqlite3.Connection = sqlite3.connect(sqldbname)
    curr = conn.cursor()
    curr.execute(querystr)

    conn.commit()
    conn.close()
