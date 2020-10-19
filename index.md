# U.S. Hourly Precipitation

by Bruce Harold  
October 2020

## Synopsis

The purpose of this project is to illustrate how to load and use the U.S. Hourly Precipitation data set from data.gov.
For this project, we will use the data from 1999-2013.
We will start with the basics of importing and understanding the data.  I intend to add analyses in later versions.

**Note:** In this project we will be using the dataset DSI 3240 which is inactive; 
there is another dataset that is active and has
the most recent information.  We will use DSI 2340 because it is better known, has smaller files, and is easier to
use.  There is more information about the new data set at the end of this document.

## Source and Documents

The catalog entry for this dataset is at <https://catalog.data.gov/dataset/u-s-hourly-precipitation-data>.

The FTP site for the files we will use is <ftp://ftp.ncdc.noaa.gov/pub/data/hourly_precip-3240/>.

We will be using the files from the FTP site.  There are other ways to pull the information, and they provide different
formats.  The formats on the FTP site are explained in the document on the same site: dsi3240.pdf (or dsi3240.doc).
The readme.txt file explains the organization of the files on the site; there are monthly files that include all
stations for the recent data (Dec 2011 to Dec 2013), and annual files by state for the range 1999-2011.  The annual
files for 2011 exclude December since that is in the monthly files.  Each state may also have a single file 
that combines all years before 1999.  We will not use files before 1999 in this project.

## Preparing Your System

Before downloading, please review and modify main/config.py for your preferred directories and create the
 directories on your system.

## Loading Data

### Downloading and uncompressing

The files can be downloaded using a web browser, but an FTP client is usually more efficient.  Because of the large
number of state directories, I have provided a Python script to download all the yearly files by state from 
1999 to 2011.  This will download more than 600 files from more than 50 directories.

    import download.get_data as get_data
    get_data.get_hourly_by_year()

The monthly files for 2011-2013 are not compressed and can be placed directly in Precipitation/hourlydata.  The
compressed yearly files could be placed in Precipitation/hourlydata/orig.

The yearly files can be extracted from the compressed files using tar (in Linux) or 7-Zip (in Windows).  If we 
uncompress to
the original filenames, we will have large numbers of small files that will be less efficient to process.
The code in this project expects
a single file for each year instead, though this is easy to change.
On Windows, the uncompressing process is also
much quicker this way.  Here is the command to do this with 7-Zip:

    for %y in (2003 2002 2001 2000 1999) do for %f in (*%y.tar.Z) do "c:\program files\7-zip\7z" x -so %f | "c:\program files\7-zip\7z" e -si -ttar -aos -so >>..\%y.txt

Using tar, this command would work:

    for y in 2003 2002 2001 2000 1999; do for f in *$y.tar.Z; do tar xvOZf $f; done >../$y.txt; done

You will want to do this for all years from 1999-2011, not just the few years that I show in these commands.

The station history file can be downloaded from the page 
<https://data.nodc.noaa.gov/cgi-bin/iso?id=gov.noaa.ncdc:C00771>.

Extract the text file in the compressed station history file to Precipitation/mshr_enhanced.

### Records

The precipitation record format is documented in dsi3240.pdf on the FTP site.  Each line represents one day for one station.  The lines
have a few fixed fields followed by a variable number of hourly readings (from one to 24) and finally a daily total.
One of the fixed position fields tells how many hourly readings are included in the record.  There is always a line
for the first day of the month, but other days are only included if there were hourly readings (or errors) on that
day. 

### Steps

You will need to check the config.py file again to update the name of the station history file since that is changed
from time to time to reflect a new creation date.

The detailed steps for loading the data into a sqlite database are in main/load_steps.py.  Although this could be run
in one pass as a script, I usually run it in chunks by copying the commands to a Python console.

This script took about one hour to run on a laptop with an Intel(R) Core(TM) i5-3337U CPU @ 1.80 GHz, 6 GB
of memory and a 5400 RPM hard drive.

## Software Versions

This was run with:

Python 3.7.4 (default, Aug  9 2019, 18:34:13) [MSC v.1915 64 bit (AMD64)] on win32

anaconda 2019.10

conda 4.8.5

pandas 0.25.1

## Licensing

This project is provided under the MIT License.  See the file LICENSE. 

No explicit licensing was provided for this dataset, but it appears to be a U.S. Government Work.  See 
<http://www.usa.gov/publicdomain/label/1.0/>.

## Newer Precipitation Data

The newer precipitation data set (known as version 2) is not as easy to find.  It has no entry in data.gov, as far
as I can determine.  This set is much larger because it has every day and hour covered by the measurements, including
the many days that had zero inches of precipitation.  It has fewer files to download which is convenient if you want
all data, but cumbersome if you want only a few years' data.

The landing page is <https://data.nodc.noaa.gov/cgi-bin/iso?id=gov.noaa.ncdc:C00988#>.

The data can be downloaded from <https://www.ncei.noaa.gov/data/coop-hourly-precipitation/v2/>.

The file CHPD-v2-ATBD-20181023.pdf on this site has a helpful explanation of the
measurement process and the data cleanup that is done before the data is shared.
