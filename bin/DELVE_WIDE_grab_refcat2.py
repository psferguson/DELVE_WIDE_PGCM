#!/usr/bin/env python
"""
    DELVE_grab_relevant_refcat2_data.py

    Example:

    DELVE_grab_relevant_refcat2_data.py --help

    DELVE_grab_relevant_refcat2_data.py --inputFile inputFile.csv --outputFile outputFile.csv --verbose 2

    It is assumed that inputFile.csv contains columns RAC1, DECC1, RAC2, DECC2, RAC3, DECC3, RAC4, DECC4,
    like in the DESDM image table...

    WARNING:  avoid using on ginormous regions.  It seems to handle SDSSDR13 coverage area well, though.

    """

##################################
#

def grab_relevant_refcat2_data(args):

    import numpy as np
    import os
    import sys
    import datetime
    import pandas as pd
    import healpixTools

    if args["verbose"] > 0:
        print "grab_relevant_refcat2_data"

    inputFile = args["grab_refcat_inputFile"]+args["BinIndex"]+".csv"
    outputFile = args["grab_refcat_outputFile"]+args["BinIndex"]+".csv" #'DELVE_Calib_expinfo_fnal'+bin_index+'.csv'

    # Location of healpixelized ATLAS-REFCAT2 catalog...
    dirName_refcat2 = '/data/des40.b/data/dtucker/ATLAS-REFCAT2/AllamFormat'

    # Does input file exist for this band?
    if os.path.isfile(inputFile)==False:
        print """input file %s does not exist.  Exiting...""" % (inputFile)
        return 1


    # Read selected columns from inputFile into a pandas DataFrame...
    if args["verbose"] > 0:
        print """Reading in RA,DEC columns from %s as a pandas DataFrame...""" % (inputFile)
        print datetime.datetime.now()
    try:
        df = pd.read_csv(inputFile, usecols=['RAC1', 'DECC1', 'RAC2', 'DECC2', 'RAC3', 'DECC3', 'RAC4', 'DECC4', 'RA_CENT', 'DEC_CENT'])
    except:
        print """Cannot read columns RAC1,DECC1,RAC2,DECC2,RAC3,DECC3,RAC4,DECC4 from input file"""
        print """Don't forget that the pandas read_csv usecols option is case-sensitive"""
        print "Exiting..."
        return 1

    if args["verbose"] > 0:
        print datetime.datetime.now()
        print

    #  Third, add healpix (nside=8) column and find list of unique healpix values...
    df['HPX8_1'] = healpixTools.getipix(8, df['RAC1'], df['DECC1'])
    df['HPX8_2'] = healpixTools.getipix(8, df['RAC2'], df['DECC2'])
    df['HPX8_3'] = healpixTools.getipix(8, df['RAC3'], df['DECC3'])
    df['HPX8_4'] = healpixTools.getipix(8, df['RAC4'], df['DECC4'])
    df['HPX8_CENT'] = healpixTools.getipix(8, df['RA_CENT'], df['DEC_CENT'])

    hpx_array_1 = df['HPX8_1'].unique()
    hpx_array_2 = df['HPX8_2'].unique()
    hpx_array_3 = df['HPX8_3'].unique()
    hpx_array_4 = df['HPX8_4'].unique()
    hpx_array_CENT = df['HPX8_CENT'].unique()

    hpx8_array = np.concatenate((hpx_array_1,hpx_array_2,hpx_array_3,hpx_array_4,hpx_array_CENT), axis=None)
    hpx8_array = np.sort(np.unique(hpx8_array))

    if args["verbose"] > 0:
        print 'hpx8_array: ', hpx8_array

    # We no longer need df...
    del df

    all_files = []
    for hpx8 in hpx8_array:
        myfile="""%s/ATLAS_REFCAT2_%d.tmp.csv""" % (dirName_refcat2, hpx8)
        if os.path.isfile(myfile)==False:
            print """%s does not exist.  Skipping...""" % (myfile)
        else:
            all_files.append(myfile)

    if args["verbose"] > 0:
        print 'Reading in columns from the various ATLAS REFCAT2 healpix files...'
        print datetime.datetime.now()
    # Trick from
    #  http://stackoverflow.com/questions/20906474/import-multiple-csv-files-into-pandas-and-concatenate-into-one-dataframe
    #df = pd.concat((pd.read_csv(f) for f in all_files))
    df = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)
    if args["verbose"] > 0:
        print datetime.datetime.now()
        print

    # Rename Dec column as DEC...
    # (Trick from S Allam)
    df.rename(columns={'Dec':'DEC'},inplace=True)

    # Create RA_WRAP column...
    df.loc[:, 'RA_WRAP'] = df.loc[:, 'RA']

    # If RA_WRAP > 180., subtract 360...
    mask = ( df.RA_WRAP > 180. )
    df.loc[mask,'RA_WRAP'] = df.loc[mask,'RA_WRAP'] - 360.

    # Sort by RA_WRAP...
    df.sort_values(by='RA_WRAP', ascending=True, inplace=True)

    # Write to outputFile...
    if args["verbose"] > 0:
        print """Writing output file %s...""" % (outputFile)
        print datetime.datetime.now()
    df.to_csv(outputFile, index=False, float_format='%.6f')
    if args["verbose"] > 0:
        print datetime.datetime.now()
        print

    # Delete dataframe...
    del df

    return 0

##################################

def main():

    import argparse
    import time

    """Create command line arguments"""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--inputFile', help='name of the input CSV file', default='input.csv')
    parser.add_argument('--outputFile', help='name of the output CSV file', default='output.csv')
    parser.add_argument('--verbose', help='verbosity level of output to screen (0,1,2,...)', default=0, type=int)
    args = vars(parser.parse_args())

    if int(args["verbose"]) > 0: print args

    status = grab_relevant_refcat2_data(args)

    return status


##################################

if __name__ == "__main__":
    main()

##################################
