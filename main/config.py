"""
Configuration file.  The directories need to be adjusted for your installation.
"""

from os.path import join

month_list = ("jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec")

# Directories - adjust as needed for your installation.

# Here is the default structure:
#
# /Precipitation
#   work_data
#   hourly_data
#      orig
#   mshr_enhanced

# work_base_dir could be relative to your working directory.  In my setup, all other data is under this one directory.
work_base_dir = r"/Precipitation"
# The work data directory holds the SQL file.
work_data_dir = join(work_base_dir, "work_data")
# The raw data directory holds the precipitation text files.
raw_data_dir = join(work_base_dir, "hourlydata")
# The orig data directory is used for downloading the compressed archive files.
orig_data_dir = join(raw_data_dir, "orig")
# The station data directory holds  the station history text file.
station_data_dir = join(work_base_dir, "mshr_enhanced")

# Filenames

# Station history filename is likely to need adjustment to the date portion.
station_hist_fname = join(station_data_dir, "MSHR_Enhanced_201911.txt")
# SQL DB Name (for sqlite)
sqldbname = join(work_data_dir, "precip.sqlite")
