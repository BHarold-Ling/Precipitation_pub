"""
Count number of records and number of hours in the input text files.
"""

from main.config import *
import glob
import numpy as np


def count_records(year: str):
    """
    Count the number of records in all files for a year.  Prints the totals for the year.
    :param year: Year of files to count.
    :return:
    """
    grand_totals = [0, 0]
    # The older data is in yearly files (1999 - 2011)
    yearly_files = list(glob.iglob(join(raw_data_dir, year + ".txt")))
    # The newer data is in monthly files (2011 - 2013; 2011 is split with part in the yearly files)
    monthly_files = list(glob.iglob(join(raw_data_dir, '3240*' + year + '.dat')))
    all_files = yearly_files + monthly_files
    for fname in all_files:
        file_totals = record_count_check(fname)
        grand_totals = np.add(grand_totals, file_totals)
    print(year, grand_totals[0], grand_totals[1])


def count_records_range(start_year: str, end_year: str):
    """
    Process all files for a range of years.
    :param start_year: Starting year
    :param end_year: Ending year
    :return: None
    """
    for yearint in range(int(start_year), int(end_year)+1):
        year = str(yearint)
        count_records(year)


def record_count_check(fname):
    """
    Count the number of records (days) and the number of hours for a single file.
    :param fname: File name to check
    :return: Counts of days and hours
    """
    daycnt = 0
    hourcnt = 0
    infile = open(fname)
    line = infile.readline()
    while line > " ":
        daycnt += 1
        hourcnt += int(line[27:30]) - 1
        line = infile.readline()

    infile.close()
    return daycnt, hourcnt
