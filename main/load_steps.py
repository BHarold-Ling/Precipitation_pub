"""
Steps to load data
US Hourly Precipitation v1 (DSI 3240)
Landing page: https://catalog.data.gov/dataset/u-s-hourly-precipitation-data
"""

# For interactive work uncomment the next line.  This will allow reloading the other libraries.
# from importlib import reload
from main.config import *
import load.create_tables as create_tables
import calc.record_count as record_count
import load.db_load as db_load
import load.station_period as station_period
import calc.coverage as coverage
import fix.error_flag_fix as error_flag_fix

import time
import glob
import pandas as pd
from datetime import date, datetime
import sqlite3


print("Starting")
print(datetime.now().strftime("%H:%M"))
conn: sqlite3.Connection = sqlite3.connect(sqldbname)

# Create the raw event tables
create_tables.cr_tb_hr(conn)
create_tables.cr_tb_dr(conn)

# Fill the raw event tables

# Load the recent files (using the faster, traditional method)

print("Load files by month")
print(datetime.now().strftime("%H:%M"))
s1 = time.perf_counter()
for fname in glob.iglob(join(raw_data_dir, '*.dat')):
    db_load.import_precip_file(fname, conn)
e1 = time.perf_counter()
print(f"Load of files by month took {e1-s1:0.1f} seconds.")

# The code above took 5 min on my system.

# This is the fastest way I found to unarchive the annual files by state (1999 - 2011).
# It is many times faster than unarchiving to all the original
# file names.  The command is provided on two lines here.  To run it, we must remove the comment characters
# and to join the lines.  This example is only for a subset of the years.  Change the years in the list to handle
# other years.

# for %y in (2003 2002 2001 2000 1999) do for %f in (*%y.tar.Z) do "c:\program files\7-zip\7z" x -so %f |
# "c:\program files\7-zip\7z" e -si -ttar -aos -so >>..\%y.txt

# NOTE: I received errors indicating that the pipe was already closed a few times, but the line totals were still
# correct.

# Make sure that this command is not run twice for the same year, or it will double up the file, and the load program
# will encounter duplicate keys.

# Load the older files (using the faster, traditional method)
# These were originally stored as one file per station/year, but the command we used above extracted them to one file
# per year with all stations in it.

print("Load files by year")
print(datetime.now().strftime("%H:%M"))
s1 = time.perf_counter()
for fname in glob.iglob(join(raw_data_dir, '*.txt')):
    db_load.import_precip_file(fname, conn)
e1 = time.perf_counter()
print(f"Load of files by year took {e1-s1:0.1f} seconds.")

# The code above took about 14 minutes on my system.

# # These are commands to clean up the raw tables if we need to retry.
# cur = conn.cursor()
# cur.execute("delete from hourly_raw")
# cur.execute("delete from daily_raw")
# conn.commit()

# Add secondary indexes on tables (station period)

print("Adding indexes")
print(datetime.now().strftime("%H:%M"))
create_tables.add_indexes_hr_dr(conn)

# Check on totals

# Here are the record totals that I found for 1999-2013.  These have been checked multiple ways.

# Year	Day_Cnt	Hour_Cnt
# 1999	229045	732564
# 2000	236580	745933
# 2001	237366	760050
# 2002	232385	765458
# 2003	248909	829556
# 2004	239814	809461
# 2005	232347	781775
# 2006	233490	778088
# 2007	230593	753663
# 2008	236190	797741
# 2009	239136	821712
# 2010	222705	778793
# 2011	213433	747329
# 2012	201932	706220
# 2013	218838	802346
# Total	3452763	11610689

# Here is the logic to check the numbers

print("Checking the counts")
print(datetime.now().strftime("%H:%M"))

# Get results from text files
print("  from text files")
record_count.count_records_range('1999', '2013')

# Get results from SQL files

print("  from SQL tables")
querystr = """
    SELECT SUBSTR(period,1,4) as year, COUNT(1)
    FROM hourly_raw
    GROUP BY year
    ORDER BY year
"""

print("Hourly")
df = pd.read_sql_query(querystr, conn)
print(df)
print(f"Sum: {df['COUNT(1)'].sum():d}")

querystr = """
    SELECT SUBSTR(period,1,4) as year, COUNT(1)
    FROM daily_raw
    GROUP BY year
    ORDER BY year
"""

print("Daily")
df = pd.read_sql_query(querystr, conn)
print(df)
print(f"Sum: {df['COUNT(1)'].sum():d}")

# Load master data

# Stations

# The station data is available as a history file, reflecting the changes to the station master data over time.
# We will only want the latest data for our work, so we will create a second table that has one record per station
# for the stations in the precipitation data.

# This data is available at https://data.nodc.noaa.gov/cgi-bin/iso?id=gov.noaa.ncdc:C00771.  Be sure to update the
# name of the text file in config.py.  The name is changed from time to time as the history file is updated.

## History table

print("Import station history")
print(datetime.now().strftime("%H:%M"))
create_tables.cr_tb_station_hist(conn)

db_load.import_station_file(station_hist_fname, conn)

create_tables.add_indexes_station_hist(conn)

## Table of most recent rows

print("Create station master")
# This creates the table from a SELECT, so we don't need to fill it separately.
create_tables.cr_tb_station_mast(conn)
create_tables.add_indexes_station_mast(conn)

# Dates

print("Create table of dates")
print(datetime.now().strftime("%H:%M"))
create_tables.cr_tb_date(conn)
db_load.load_dates(date(1960, 1, 1), date(1998, 12, 31), conn)
create_tables.add_indexes_date(conn)

# States

print("Create table of states")
print(datetime.now().strftime("%H:%M"))
create_tables.cr_tb_state(conn)
db_load.fill_states(conn)
create_tables.add_indexes_state(conn)


# Data Cleaning

# Fixes to flags

# This data has errors in 2007 Q1: some days are missing the error flags that they would normally have to reflect the
# errors in the hourly data.

print("Finding and Fixing Error Flags")
print(datetime.now().strftime("%H:%M"))
## Gather list of missing daily errors
error_flag_fix.find_error_dates(conn)
error_flag_fix.find_mismatches(conn)
## Fix data
error_flag_fix.update_from_mismatches(conn)

# Coverage by station period

print("Coverage by station/period")
print(datetime.now().strftime("%H:%M"))
create_tables.cr_tb_period_coverage(conn)
for i in range(1999, 2014):
    coverage.calc_over_range('000000', '%d-01' % i, '999999', '%d-12' % i)


# Building Derived Tables

# Periods with complete days
#   This includes days which have an accumulated amount for more than one hour.
#   It does not include accumulations that run across multiple days.

print("Finding periods with complete days")
print(datetime.now().strftime("%H:%M"))
create_tables.cr_tb_s_period_d(conn)
station_period.load_station_period_d()

# Periods with complete hours
#   This does not include any periods with accumulations.

print("Finding periods with complete hours")
print(datetime.now().strftime("%H:%M"))
create_tables.cr_tb_s_period_h(conn)
station_period.load_station_period_h()

print("Ending")
print(datetime.now().strftime("%H:%M"))