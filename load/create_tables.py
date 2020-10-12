"""
Create raw tables
"""


import sqlite3
# from main.config import *


def cr_tb_hr(conn: sqlite3.Connection):
    """
    Create raw hourly table
    :param conn: DB Connection
    :return: None
    """
    cmd = '''
        CREATE TABLE hourly_raw (
            station VARCHAR(6) NOT NULL,
            state_code VARCHAR(2) NOT NULL,
            units VARCHAR(2) NOT NULL,
            read_date INTEGER,
            read_hour INTEGER,
            amount REAL,
            flag1 VARCHAR(1),
            flag2 VARCHAR(1),
            period VARCHAR(7),
            PRIMARY KEY (station, read_date, read_hour)
        )
    '''
    cur = conn.cursor()
    cur.execute(cmd)
    conn.commit()


def cr_tb_dr(conn: sqlite3.Connection):
    """
    Create raw daily table
    :param conn: DB Connection
    :return: None
    """
    cmd = '''
        CREATE TABLE daily_raw (
            station VARCHAR(6) NOT NULL,
            state_code VARCHAR(2) NOT NULL,
            units VARCHAR(2) NOT NULL,
            read_date INTEGER,
            amount REAL,
            flag1 VARCHAR(1),
            flag2 VARCHAR(1),
            period VARCHAR(7),
            PRIMARY KEY (station, read_date)
        )
    '''
    cur = conn.cursor()
    cur.execute(cmd)
    conn.commit()


def add_indexes_hr_dr(conn: sqlite3.Connection):
    """
    Add extra indexes for daily and hourly tables
    :param conn: DB Connection
    :return: None
    """

    cur = conn.cursor()

    cmd = '''
        CREATE INDEX hr_period
        ON hourly_raw (
            station, period
        )
    '''
    cur.execute(cmd)

    cmd = '''
        CREATE INDEX dr_period
        ON daily_raw (
            station, period
        )
    '''
    cur.execute(cmd)

    conn.commit()


def cr_tb_date(conn: sqlite3.Connection):
    """
    Create table for date (Integer Day to various values)
    :param conn: DB Connection
    :return: None
    """
    cmd = '''
        CREATE TABLE date (
            id INTEGER PRIMARY KEY,
            iso_date VARCHAR(10),
            year INTEGER,
            month INTEGER,
            day INTEGER,
            day_of_year INTEGER,
            period VARCHAR(7)
        )
    '''
    cur = conn.cursor()
    cur.execute(cmd)
    conn.commit()


def add_indexes_date(conn: sqlite3.Connection):
    """
    Add extra indexes for date table
    :param conn: DB Connection
    :return: None
    """

    cur = conn.cursor()

    cmd = '''
        CREATE INDEX date_period_idx
        ON date (
            period, day
        )
    '''
    cur.execute(cmd)

    conn.commit()


def cr_tb_state(conn: sqlite3.Connection):
    """
    Create a table for state codes.
    :param conn: DB Connection
    :return: None
    """
    cmd = '''
        CREATE TABLE state (
            state_code VARCHAR(2) PRIMARY KEY,
            state_abbr VARCHAR(2),
            state_name VARCHAR(25)
        )
    '''
    cur = conn.cursor()
    cur.execute(cmd)
    conn.commit()


def add_indexes_state(conn: sqlite3.Connection):
    """
    Add extra indexes for date table
    :param conn: DB Connection
    :return: None
    """

    cur = conn.cursor()

    cmd = '''
        CREATE INDEX state_abbr_idx
        ON state (
            state_abbr
        )
    '''
    cur.execute(cmd)

    conn.commit()


def cr_tb_station_hist(conn: sqlite3.Connection):
    """
    Create a table for station history data.
    :param conn: DB Connection
    :return: None
    """
    cmd = '''
        CREATE TABLE station_hist (
           source_id VARCHAR(20),
           source VARCHAR(10),
           begin_date INTEGER,
           end_date INTEGER,
           station_status VARCHAR(20),
           ncdcstn_id VARCHAR(20),
           coop_id VARCHAR(20),
           ghcnd_id VARCHAR(20),    
           name_pr_sh VARCHAR(30),
           name_coop_sh VARCHAR(30),
           nws_climate_div VARCHAR(10),
           state VARCHAR(10),
           county VARCHAR(50),
           nws_st_code VARCHAR(2),
           fips_country_code VARCHAR(2),
           nws_region VARCHAR(30),
           elev_ground REAL,
           elev_barom REAL,
           lat REAL,
           lon REAL,
           relocation VARCHAR(62),
           utc_offset REAL,
           ghcnmlt_id VARCHAR(20),
           county_fips_code VARCHAR(5),
           igra_id VARCHAR(30),
           hpd_id  VARCHAR(20)
        )
    '''
    cur = conn.cursor()
    cur.execute(cmd)
    conn.commit()


def add_indexes_station_hist(conn: sqlite3.Connection):
    """
    Add indexes for station history
    :param conn: DB Connection
    :return: None
    """

    cur = conn.cursor()

    cmd = '''
        CREATE INDEX src_dt
        ON station_hist
        (
            [source],
            begin_date
        )
    '''
    cur.execute(cmd)

    cmd = '''
        CREATE INDEX coop_dt
        ON station_hist
        (
            coop_id,
            begin_date
        )
    '''
    cur.execute(cmd)

    conn.commit()


def cr_tb_station_mast(conn: sqlite3.Connection):
    """
    Create the station master table from the latest record for each station
    in the history file.  Since stations are allowed to move only very
    limited distances without getting a new number, this should work for our
    purposes.
    :param conn: DB Connection
    :return: None
    """
    cmd = '''
        CREATE TABLE station AS
        SELECT DISTINCT dr.station,
                        state,
                        nws_st_code,
                        fips_country_code,
                        elev_ground,
                        lat,
                        lon,
                        utc_offset,
                        county_fips_code
        FROM station_hist sh,
        (SELECT DISTINCT station FROM daily_raw) dr
        WHERE dr.station = sh.coop_id
        AND sh.begin_date = (
            SELECT MAX(sh1.begin_date)
            FROM station_hist sh1
            WHERE sh1.coop_id = sh.coop_id
        )
    '''
    cur = conn.cursor()
    cur.execute(cmd)
    conn.commit()


def add_indexes_station_mast(conn: sqlite3.Connection):
    """
    Add index for station table
    :param conn: DB Connection
    :return: None
    """

    cur = conn.cursor()

    cmd = '''
        CREATE INDEX station_idx
        ON station
        (
            station
        )
    '''
    cur.execute(cmd)

    conn.commit()


def cr_tb_period_coverage(conn: sqlite3.Connection):
    """
    Create the station period coverage table.
    This table is used to track the coverage we have for each
    station-period in the dataset.
    :param conn: DB Connection
    :return: None
    """
    cmd = '''
        CREATE TABLE station_period_coverage (
                station VARCHAR(6) NOT NULL,
                period VARCHAR(7) NOT NULL,
                hours INTEGER,
                missing_hours INTEGER,
                coverage REAL,
                cov_flag CHAR(1),
                units_flag INTEGER,
                PRIMARY KEY (station, period)
            );
    '''
    cur = conn.cursor()
    cur.execute(cmd)
    conn.commit()


def cr_tb_s_period_d(conn: sqlite3.Connection):
    """
    Create the station period table for periods with complete daily data.
    :param conn: DB Connection
    :return: None
    """
    cmd = '''
        CREATE TABLE station_period_d (
            station VARCHAR(6),
            period VARCHAR(7),
            amount INTEGER,
            units_flag INTEGER,
            PRIMARY KEY (station, period)
        )
    '''
    cur = conn.cursor()
    cur.execute(cmd)
    conn.commit()


def cr_tb_s_year_d(conn: sqlite3.Connection):
    """
    Create the station year table for years with complete daily data.
    :param conn: DB Connection
    :return: None
    """
    cmd = '''
        CREATE TABLE station_year_d (
            station VARCHAR(6),
            year INTEGER,
            amount INTEGER,
            units_flag INTEGER,
            PRIMARY KEY (station, year)
        )
    '''
    cur = conn.cursor()
    cur.execute(cmd)
    conn.commit()


def cr_tb_s_period_h(conn: sqlite3.Connection):
    """
    Create the station period table for periods with complete hourly data.
    :param conn: DB Connection
    :return: None
    """
    cmd = '''
        CREATE TABLE station_period_h (
            station VARCHAR(6),
            period VARCHAR(7),
            amount INTEGER,
            units_flag INTEGER,
            PRIMARY KEY (station, period)
        )
    '''
    cur = conn.cursor()
    cur.execute(cmd)
    conn.commit()


def cr_tb_s_year_h(conn: sqlite3.Connection):
    """
    Create the station year table for years with complete hourly data.
    :param conn: DB Connection
    :return: None
    """
    cmd = '''
        CREATE TABLE station_year_h (
            station VARCHAR(6),
            year INTEGER,
            amount INTEGER,
            units_flag INTEGER,
            PRIMARY KEY (station, year)
        )
    '''
    cur = conn.cursor()
    cur.execute(cmd)
    conn.commit()


# main (test)

# conn1 = sqlite3.connect(sqldbname)
# cr_tb_1(conn1)
# cr_tb_date(conn1)
# cr_tb_states(conn1)
# cr_tb_station_hist(conn1)

# conn1.close()
