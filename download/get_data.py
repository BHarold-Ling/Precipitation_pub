from main.config import *
import ftplib
import os


def get_hourly_by_year():
    """
    Find all the hourly precipitation files for each state and year and download them.
    :return:
    """
    ftp = ftplib.FTP('ftp.ncdc.noaa.gov')
    # ftp.set_debuglevel(1)
    ftp.login()
    ftp.cwd("/pub/data/hourly_precip-3240/01")

    for statenum in range(1, 99):
        statestr = "{:02d}".format(statenum)
        cnt = 0
        try:
            ftp.cwd("../" + statestr)
            for yearnum in range(1999, 2012):
                year = str(yearnum)
                filen = "3240_" + statestr + "_" + year + "-" + year + ".tar.Z"
                infilen = join(orig_data_dir, filen)
                # if ftp.size(filen) != None: # This raised an error, but not investigated
                try:
                    with open(infilen, "wb") as infile:
                        ftp.retrbinary("RETR " + filen, infile.write)
                    cnt += 1
                except ftplib.all_errors:
                    os.remove(infilen)
        except ftplib.error_perm:
            pass

        # Report if anything was found/pulled for the state code
        if cnt > 0:
            print("Pulled " + statestr)
        else:
            print("Skipped " + statestr)

    ftp.quit()
    ftp.close()

# main

# get_hourly_by_year()
