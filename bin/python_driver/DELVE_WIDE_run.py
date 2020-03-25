# Edited on 200324 by PSF
# To run this script:
#  source runAll.bash <band> <date>
# where <band> is the filter band, and
#       <date> is a string descriptor for this run
#              (e.g., the date in the form 20191127)
# E.g.:
#  source runAll.bash i 20191127a

# given ra dec limits and a bin number and yaml config file run the calibration for a subset of dataself.


# Initial setup...
import numpy as np
import pandas as pd
import sys
import os
current_dir=os.getcwd()
sys.path.append(current_dir+"../../bin/python_driver/")
import DELVE_Calib_query_fnal as DELVE_query
import argparse

def main():
    """Create command line arguments"""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--RaMin',
                      help='Min of RA bin',
                      default='178')
    parser.add_argument('--RaMax',
                      help='Max of RA bin',
                      default='180')
    parser.add_argument('--DecMin',
                      help='Min of RA bin',
                      default='-22')
    parser.add_argument('--DecMax',
                      help='Max of Dec bin',
                      default='-20')
    parser.add_argument('--BinIndex',
                      help='Index number of the Bin',
                      default='999')
    args = vars(parser.parse_args()) # making it a python dict explicitly
    do_db_query=True
    do_grab_refcat2="true"
    do_runConvert="true"
    do_concat="true"
    do_match="true"
    do_calc_zps="true"
    prefix="test"
    bandList="u g r i z Y"
    # The 1st argument is DATE, which is used for naming the log files...
    # (Default value is "test".)
    print "prefix is "+prefix

    # Optional command line argument is for the filter band to be used.
    # If not given, assume all filter bands (u,g,r,i,z,Y)
    # (Default value is "all", which later translates to "u g r i z Y").
    # Not all script sections below make use of BAND.
    print "band list is "  + bandList

    # args={"RaMin": "178",
    #       "RaMax": "180",
    #       "DecMin": "-22",
    #       "DecMax": "-20",
    #       "BinIndex": "69"}

    # 1. we call DELVE_Calib_fileimgexp_query_fnal.py
    if do_db_query == True:
            print "Doing database query"
            #os.remove("DELVE_query_wide"+prefix".log")
            DELVE_query.do_db_querys(args)
    # 2. we call DELVE_grab_relevant_refcat2_data.py

    # 3. Then for each band we call
if __name__ == "__main__":
    main()
