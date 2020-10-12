"""
Logic to find and fix missing error flags in daily records
"""

# from main.config import *
import sqlite3


def find_error_dates(conn: sqlite3.Connection):
    """
    Find error dates from raw data.
    This finds dates based on the daily file and runs several passes on hourly data
    to find various flags and conditions there.
    :param conn:
    :return:
    """

    curr = conn.cursor()

    # Daily
    querystr = '''
        CREATE TABLE error_days_d AS
        SELECT DISTINCT station, read_date
        FROM daily_raw dr 
        WHERE flag1 IN ('I','P')
    '''
    curr.execute(querystr)

    querystr = '''
        CREATE INDEX edd_idx
        ON error_days_d
            (station, read_date)
    '''
    curr.execute(querystr)

    # Hourly

    # Based on simple flags

    querystr = '''
        CREATE TABLE error_days_h AS
        SELECT DISTINCT station, read_date
        FROM hourly_raw hr 
        WHERE flag1 IN ('[',']','{','}')
        OR flag2 IN ('Q','q')
    '''
    curr.execute(querystr)

    querystr = '''
        CREATE INDEX edh_idx
        ON error_days_h
            (station, read_date)
    '''
    curr.execute(querystr)

    # Based on accumulation flags

    querystr = '''
        CREATE TABLE error_days_ha AS
        SELECT station, read_date, COUNT(1) AS count1
        FROM hourly_raw hr 
        WHERE flag1 in ('a', 'A')
        GROUP BY station, read_date 
    '''
    curr.execute(querystr)

    querystr = '''
        CREATE INDEX edha_idx
        ON error_days_ha
            (station, read_date)
    '''
    curr.execute(querystr)

    conn.commit()


def find_mismatches(conn: sqlite3.Connection):
    """
    Use the list of days with error to find days that are missing flags.
    :param conn: DB Connection
    :return: None
    """

    curr = conn.cursor()

    # All dates in hourly list based on simple flags

    querystr = '''
        CREATE TABLE error_days_miss AS
        SELECT station, read_date, 'F' AS flag
        FROM error_days_h edh
        WHERE NOT EXISTS (
            SELECT 1
            FROM error_days_d edd
            WHERE edd.station = edh.station
            AND edd.read_date = edh.read_date
        )
    '''
    curr.execute(querystr)

    # Accums with an odd number of hours with flags
    # If all periods stopped and started on the same day, the number would be even.

    querystr = '''
        INSERT INTO error_days_miss
        SELECT station, read_date, 'O'
        FROM error_days_ha edha
        WHERE NOT EXISTS (
            SELECT 1
            FROM error_days_d edd
            WHERE edd.station = edha.station
            AND edd.read_date = edha.read_date
        )
        AND count1 % 2 = 1
    '''
    curr.execute(querystr)

    # Accums with an even number of flags, but the first flag is the end of an accum
    # period.

    querystr = '''
        INSERT INTO error_days_miss
        SELECT station, read_date, 'E'
        FROM error_days_ha edha
        WHERE NOT EXISTS (
            SELECT 1
            FROM error_days_d edd
            WHERE edd.station = edha.station
            AND edd.read_date = edha.read_date
        )
        AND count1 count1 % 2 = 0
        AND EXISTS (
            SELECT 1
            FROM hourly_raw hr 
            WHERE hr.station = edha.station
            AND hr.read_date = edha.read_date
            AND flag1 = 'A'
            AND NOT EXISTS (
                SELECT 1
                FROM hourly_raw hr2 
                WHERE hr2.station = edha.station
                AND hr2.read_date = edha.read_date
                AND hr2.read_hour < hr.read_hour 
                AND hr2.flag1 = 'a'
            )
        )
    '''
    curr.execute(querystr)

    conn.commit()


def update_from_mismatches(conn: sqlite3.Connection):
    """
    Update daily table based on the mismatches that we have collected.
    We will use 'P' in all cases rather than determining where 'I' would match the standards.
    :param conn: DB Connection
    :return: None
    """

    curr = conn.cursor()

    querystr = '''
        UPDATE daily_raw
        SET flag1 = 'P'
        FROM error_days_miss edm
        WHERE daily_raw.station = edm.station
        AND daily_raw.read_date = edm.read_date
    '''
    curr.execute(querystr)

    conn.commit()
