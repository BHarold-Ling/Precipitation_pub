"""
Load DB files from text files and other sources
"""

from datetime import date, timedelta

from load.PrecipLine import PrecipLine
from load.StationLine import StationLine
import sqlite3
import pandas as pd


def import_precip_file(fname, conn: sqlite3.Connection):
    """
    Import a single raw precipitation data file.
    Old style using SQL insert command.
    :param fname: Filename
    :param conn: DB connection
    :return: None
    """
    insert_sql = '''
        INSERT INTO hourly_raw
            (
                station,
                state_code,
                units,
                period,
                read_date,
                read_hour,
                amount,
                flag1,
                flag2
            )
            VALUES (?,?,?,?,?,?,?,?,?)
    '''
    insert_sql_d = '''
        INSERT INTO daily_raw
            (
                station,
                state_code,
                units,
                period,
                read_date,
                amount,
                flag1,
                flag2
            )
            VALUES (?,?,?,?,?,?,?,?)
    '''
    cur = conn.cursor()
    recordset = []
    recordset_d = []
    setsize = 0

    infile = open(fname)
    line = infile.readline()
    while line > " ":
        pl = PrecipLine(line)
        d = pl.date1
        d1 = "{}-{}-{}".format(d[0:4], d[4:6], d[8:10])
        d2 = date.fromisoformat(d1).toordinal()
        period = d1[:7]
        arglist_start = [
            pl.station,
            pl.state_code,
            pl.units,
            period,
            d2,
        ]
        for measure in pl.details:
            arglist = arglist_start + list(measure)
            # cur.execute(insert_sql, arglist)
            recordset.append(arglist)
            setsize += 1

        # Daily
        arglist_d = arglist_start + [
            pl.daily_tot,
            pl.daily_flag1,
            pl.daily_flag2
        ]
        recordset_d.append(arglist_d)

        if setsize > 10000:
            cur.executemany(insert_sql, recordset)
            cur.executemany(insert_sql_d, recordset_d)
            recordset = []
            recordset_d = []
            setsize = 0

        # conn.commit()
        line = infile.readline()

    # final sql execute
    if len(recordset) > 0:
        cur.executemany(insert_sql, recordset)
        cur.executemany(insert_sql_d, recordset_d)

    infile.close()
    conn.commit()


def import_precip_file_df(fname, conn: sqlite3.Connection):
    """
    Import a single raw precipitation data file.
    New style using pandas.
    This was slower for me, perhaps because method='multi' was failing for me with sqlite3.
    :param fname: Filename
    :param conn: DB connection
    :return: None
    """
    columns_h = [
        'station',
        'state_code',
        'units',
        'period',
        'read_date',
        'read_hour',
        'amount',
        'flag1',
        'flag2'
    ]
    columns_d = columns_h.copy()
    columns_d.remove('read_hour')
    recordset = []
    recordset_d = []
    setsize = 0

    infile = open(fname)
    line = infile.readline()
    while line > " ":
        pl = PrecipLine(line)
        d = pl.date1
        d1 = "{}-{}-{}".format(d[0:4], d[4:6], d[8:10])
        d2 = date.fromisoformat(d1).toordinal()
        period = d1[:7]
        arglist_start = [
            pl.station,
            pl.state_code,
            pl.units,
            period,
            d2,
        ]
        for measure in pl.details:
            arglist = arglist_start + list(measure)
            # cur.execute(insert_sql, arglist)
            recordset.append(arglist)
            setsize += 1

        # Daily
        arglist_d = arglist_start + [
            pl.daily_tot,
            pl.daily_flag1,
            pl.daily_flag2
        ]
        recordset_d.append(arglist_d)

        if setsize > 10000:
            # Write hourly
            df_h = pd.DataFrame(recordset)
            df_h.columns = columns_h
            df_h.to_sql('hourly_raw', conn, if_exists='append', index=False, chunksize=10000)
            # Write daily
            df_d = pd.DataFrame(recordset_d)
            df_d.columns = columns_d
            df_d.to_sql('daily_raw', conn, if_exists='append', index=False, chunksize=10000)
            # Reset lists
            recordset = []
            recordset_d = []
            setsize = 0

        # conn.commit()
        line = infile.readline()

    # final sql execute
    if len(recordset) > 0:
        # Write hourly
        df_h = pd.DataFrame(recordset)
        df_h.columns = columns_h
        df_h.to_sql('hourly_raw', conn, if_exists='append', index=False, chunksize=1000)
        # Write daily
        df_d = pd.DataFrame(recordset_d)
        df_d.columns = columns_d
        df_d.to_sql('daily_raw', conn, if_exists='append', index=False, chunksize=1000)

    infile.close()
    conn.commit()


def load_dates(start_dt: date, end_dt: date, conn: sqlite3.Connection):
    """
    Load a range of dates with their related values.
    :param start_dt: Start date of range
    :param end_dt: End date of range
    :param conn: DB connection
    :return: None
    """
    insert_sql = '''
        INSERT INTO date
            (
                id,
                iso_date,
                year,
                month,
                day,
                day_of_year,
                period
            )
            VALUES (
                ?,
                ?,?,?,?,?,?
            ) 
    '''
    curr = conn.cursor()

    oneday = timedelta(days=1)
    workdate = start_dt
    values = []

    while workdate <= end_dt:
        date_int = workdate.toordinal()
        iso_date = workdate.isoformat()
        period = iso_date[:7]
        year = workdate.year
        month = workdate.month
        day = workdate.day
        day_of_year = int(workdate.strftime("%j"))
        # curr.execute(insert_sql, (iso_date, iso_date, year, month, day, day_of_year))
        values.append((date_int, iso_date, year, month, day, day_of_year, period))
        workdate += oneday

    curr.executemany(insert_sql, values)
    conn.commit()


def import_station_file(fname, conn: sqlite3.Connection):
    """
    Import raw station history text file into SQL.
    :param fname: Filename
    :param conn: DB connection
    :return: None
    """
    insert_sql = '''
        INSERT INTO station_hist
            (
                source_id,
                source,
                begin_date,
                end_date,
                station_status,
                ncdcstn_id,
                coop_id,
                ghcnd_id,
                name_pr_sh,
                name_coop_sh,
                nws_climate_div,
                state,
                county,
                nws_st_code,
                fips_country_code,
                nws_region,
                elev_ground,
                elev_barom,
                lat,
                lon,
                relocation,
                utc_offset,
                ghcnmlt_id,
                county_fips_code,
                igra_id,
                hpd_id            
            )
            VALUES (
                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
            )
    '''
    cur = conn.cursor()
    recordset = []
    setsize = 0

    infile = open(fname)
    line = infile.readline()
    while line > " ":
        st = StationLine(line)
        begin_dt = st.begin_date
        begin_dt1 = "{}-{}-{}".format(begin_dt[0:4], begin_dt[4:6], begin_dt[6:8])
        begin_dt_int = date.fromisoformat(begin_dt1).toordinal()
        end_dt = st.end_date
        end_dt1 = "{}-{}-{}".format(end_dt[0:4], end_dt[4:6], end_dt[6:8])
        end_dt_int = date.fromisoformat(end_dt1).toordinal()
        arglist = [
            st.source_id,
            st.source,
            begin_dt_int,
            end_dt_int,
            st.station_status,
            st.ncdcstn_id,
            st.coop_id,
            st.ghcnd_id,
            st.name_pr_sh,
            st.name_coop_sh,
            st.nws_climate_div,
            st.state,
            st.county,
            st.nws_st_code,
            st.fips_country_code,
            st.nws_region,
            st.elev_ground,
            st.elev_barom,
            st.lat,
            st.lon,
            st.relocation,
            st.utc_offset,
            st.ghcnmlt_id,
            st.county_fips_code,
            st.igra_id,
            st.hpd_id
        ]

        recordset.append(arglist)
        setsize += 1
        if setsize > 1000:
            cur.executemany(insert_sql, recordset)
            recordset = []
            setsize = 0
        line = infile.readline()

    # final sql execute
    if len(recordset) > 0:
        cur.executemany(insert_sql, recordset)

    infile.close()
    conn.commit()


def fill_states(conn: sqlite3.Connection):
    """
    Fill states from hardcoded values.
    :param conn: DB Connection
    :return: None
    """

    insert_sql = """
        INSERT INTO state
            (state_code, state_abbr, state_name)
        VALUES
            ('01', 'AL', 'Alabama'),
            ('02', 'AZ', 'Arizona'),
            ('03', 'AR', 'Arkansas'),
            ('04', 'CA', 'California'),
            ('05', 'CO', 'Colorado'),
            ('06', 'CT', 'Connecticut'),
            ('07', 'DE', 'Delaware'),
            ('08', 'FL', 'Florida'),
            ('09', 'GA', 'Georgia'),
            ('10', 'ID', 'Idaho'),
            ('11', 'IL', 'Illinois'),
            ('12', 'IN', 'Indiana'),
            ('13', 'IA', 'Iowa'),
            ('14', 'KS', 'Kansas'),
            ('15', 'KY', 'Kentucky'),
            ('16', 'LA', 'Louisiana'),
            ('17', 'ME', 'Maine'),
            ('18', 'MD', 'Maryland'),
            ('19', 'MA', 'Massachusetts'),
            ('20', 'MI', 'Michigan'),
            ('21', 'MN', 'Minnesota'),
            ('22', 'MS', 'Mississippi'),
            ('23', 'MO', 'Missouri'),
            ('24', 'MT', 'Montana'),
            ('25', 'NE', 'Nebraska'),
            ('26', 'NV', 'Nevada'),
            ('27', 'NH', 'New Hampshire'),
            ('28', 'NJ', 'New Jersey'),
            ('29', 'NM', 'New Mexico'),
            ('30', 'NY', 'New York'),
            ('31', 'NC', 'North Carolina'),
            ('32', 'ND', 'North Dakota'),
            ('33', 'OH', 'Ohio'),
            ('34', 'OK', 'Oklahoma'),
            ('35', 'OR', 'Oregon'),
            ('36', 'PA', 'Pennsylvania'),
            ('37', 'RI', 'Rhode Island'),
            ('38', 'SC', 'South Carolina'),
            ('39', 'SD', 'South Dakota'),
            ('40', 'TN', 'Tennessee'),
            ('41', 'TX', 'Texas'),
            ('42', 'UT', 'Utah'),
            ('43', 'VT', 'Vermont'),
            ('44', 'VA', 'Virginia'),
            ('45', 'WA', 'Washington'),
            ('46', 'WV', 'West Virginia'),
            ('47', 'WI', 'Wisconsin'),
            ('48', 'WY', 'Wyoming'),
            ('50', 'AK', 'Alaska'),
            ('51', 'HI', 'Hawaii'),
            ('66', 'PR', 'Puerto Rico'),
            ('67', 'VI', 'Virgin Islands'),
            ('91', 'XX', 'Pacific Islands')
    """

    curr = conn.cursor()
    curr.execute(insert_sql)
    conn.commit()
