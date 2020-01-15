#!/usr/bin/env python
"""
    DECamFullCat_id_gaiadr2_data.py

    Example:
    
    DECamFullCat_id_gaiadr2_data.py --help

    DECamFullCat_id_gaiadr2_data.py --inputFile inputFile.csv --outputFile outputFile.lis --verbose 2

    It is assumed that inputFile.csv contains columns ALPHAWIN_J2000, DELTAWIN_J2000...

    """

##################################

def main():

    import argparse
    import time

    """Create command line arguments"""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--inputFile', help='name of the input CSV file', default='input.csv')
    parser.add_argument('--outputFile', help='name of the output list file', default='output.lis')
    parser.add_argument('--verbose', help='verbosity level of output to screen (0,1,2,...)', default=0, type=int)
    args = parser.parse_args()

    if args.verbose > 0: print args

    status = decam_fullcat_grab_id_gaiadr2_data(args)

    return status


##################################
# 

def decam_fullcat_grab_id_gaiadr2_data(args):

    import numpy as np 
    import os
    import sys
    import datetime
    import pandas as pd
    from astropy.table import Table

    if args.verbose > 0:
        print "decam_fullcat_grab_id_gaiadr2_data"

    inputFile = args.inputFile
    outputFile = args.outputFile

    # Location of healpixelized GAIADR2 catalog...
    gaiaDir = '/data/des40.b/data/gaia/dr2/healpix'

    # Does input file exist for this band?
    if os.path.isfile(inputFile)==False:
        print """input file %s does not exist.  Exiting...""" % (inputFile)
        return 1


    # Read selected columns from inputFile into a pandas DataFrame...
    if args.verbose > 0:
        print """Reading in ALPHAWIN_J2000,DELTAWIN_J2000 columns from %s as astropy Tablee...""" % (inputFile)
        print datetime.datetime.now()
    try:
        t = Table.read(inputFile)
        list_of_tuples = list(zip(t['ALPHAWIN_J2000'],t['DELTAWIN_J2000']))
        df = pd.DataFrame(list_of_tuples, columns=['ALPHAWIN_J2000','DELTAWIN_J2000'])
    except:
        print """Cannot create pandas dataframe with ALPHAWIN_J2000,DELTAWIN_J2000 columns from input file"""
        print "Exiting..."
        return 1

    if args.verbose > 0:
        print datetime.datetime.now()
        print

    #  Third, add healpix (nside=32) column and find list of unique healpix values...
    df['HPX32'] = getipix(32, df['ALPHAWIN_J2000'], df['DELTAWIN_J2000'], False)

    hpx32_array = df['HPX32'].unique()

    if args.verbose > 0:
        print 'hpx32_array: ', hpx32_array
    
    # We no longer need df...
    del df

    # Write to outputFile...
    if args.verbose > 0:
        print """Writing output file %s...""" % (outputFile)
        print datetime.datetime.now()
    fout = open(outputFile, 'w')
    for hpx32 in hpx32_array:
        gaiaFile = """%s/GaiaSource_%05d.fits""" % (gaiaDir, hpx32)
        if os.path.isfile(gaiaFile)==False:
            print """%s does not exist.  Skipping...""" % (myfile)
        else:
            print gaiaFile
            fout.write(gaiaFile+'\n')
    fout.close()

    return 0



# Healpix tools.
#  From Sahar Allam (2016.07.08)
##################################
def radec2thetaphi(ra, dec):
    import numpy as np
    return (90-dec)*np.pi/180., ra*np.pi/180.

##################################
#DESDM uses nside=128, nest=True
#Alex Drlica Wagner's healpixelated Gaia DR2 on des40 uses nside=32, nest=False
def getipix(nside,ra,dec,nest=True):
    import healpy as hp
    theta, phi = radec2thetaphi(ra, dec)
    ipix = hp.pixelfunc.ang2pix(nside, theta, phi, nest)
    return ipix

##################################



##################################

if __name__ == "__main__":
    main()

##################################
