#!/usr/bin/env python

# Initial setup...
import numpy as np
import pandas as pd
import math
import glob
import os
import matplotlib.pyplot as plt
from astropy.io import fits
import fitsio
import healpy as hp
import healpixTools


#--------------------------------------------------------------------------
# Main code.

def main():

    import argparse

    """Create command line arguments"""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--inputFile', help='name of the input file', default='D00827408_i_c34_r4087p01_red-fullcat.fits')
    parser.add_argument('--outputFile', help='name of the output file', default='D00827408_i_c34_r4087p01_red-fullcat.csv')
    parser.add_argument('--verbose', help='verbosity level of output to screen (0,1,2,...)', default=0, type=int)
    args = parser.parse_args()

    if args.verbose > 0: print args

    status = DELVE_Calib_se_objects_fnal(args)

    return status

#--------------------------------------------------------------------------

def DELVE_Calib_se_objects_fnal(args):

    inputFile=args["se_input_file"]
    outputFile=args["se_output_file"]
    df_cat = catfits_to_catdf(inputFile)
    df_cat.to_csv(outputFile, index=False)

    #print 'Finis!'

#--------------------------------------------------------------------------

# Using suggesions from Eric Neilsen's notes page:
# http://des-ops.fnal.gov:8080/notes/neilsen/pandasExamples/pandas_examples.html#orgheadline6
def catfits_to_catdf(inputFile):
    npcat = fitsio.read(inputFile, columns=['NUMBER','X_IMAGE','Y_IMAGE','ALPHAWIN_J2000','DELTAWIN_J2000',
                                            'FLUX_PSF','FLUXERR_PSF','FLUX_APER','FLUXERR_APER',
                                            'CLASS_STAR','SPREAD_MODEL','SPREADERR_MODEL','FLAGS'], ext=2)
    npcat = npcat.byteswap().newbyteorder()

    inputFileBaseName = os.path.basename(inputFile)
    number = npcat['NUMBER']
    x_image = npcat['X_IMAGE']
    y_image = npcat['Y_IMAGE']
    alphawin_j2000 = npcat['ALPHAWIN_J2000']
    deltawin_j2000 = npcat['DELTAWIN_J2000']
    flux_psf = npcat['FLUX_PSF']
    fluxerr_psf = npcat['FLUXERR_PSF']
    class_star = npcat['CLASS_STAR']
    spread_model = npcat['SPREAD_MODEL']
    spreaderr_model = npcat['SPREADERR_MODEL']
    flags = npcat['FLAGS']

    flux_aper = {}
    fluxerr_aper = {}
    for i in range(12):
        aper_n = i+1
        #print npfluxaper['FLUX_APER'][0,i]
        flux_aper[aper_n] = npcat['FLUX_APER'][:,i]
        fluxerr_aper[aper_n] = npcat['FLUXERR_APER'][:,i]

    df_cat = pd.DataFrame({    'FILENAME':inputFileBaseName, \
                               'NUMBER':number, \
                               'X_IMAGE':x_image, \
                               'Y_IMAGE':y_image, \
                               'RA':alphawin_j2000, \
                               'DEC':deltawin_j2000, \
                               'FLUX_PSF':flux_psf, \
                               'FLUXERR_PSF':fluxerr_psf, \
                               'FLUX_APER_01':flux_aper[1],'FLUXERR_APER_01':fluxerr_aper[1], \
                               'FLUX_APER_02':flux_aper[2],'FLUXERR_APER_02':fluxerr_aper[2], \
                               'FLUX_APER_03':flux_aper[3],'FLUXERR_APER_03':fluxerr_aper[3], \
                               'FLUX_APER_04':flux_aper[4],'FLUXERR_APER_04':fluxerr_aper[4], \
                               'FLUX_APER_05':flux_aper[5],'FLUXERR_APER_05':fluxerr_aper[5], \
                               'FLUX_APER_06':flux_aper[6],'FLUXERR_APER_06':fluxerr_aper[6], \
                               'FLUX_APER_07':flux_aper[7],'FLUXERR_APER_07':fluxerr_aper[7], \
                               'FLUX_APER_08':flux_aper[8],'FLUXERR_APER_08':fluxerr_aper[8], \
                               'FLUX_APER_09':flux_aper[9],'FLUXERR_APER_09':fluxerr_aper[9], \
                               'FLUX_APER_10':flux_aper[10],'FLUXERR_APER_10':fluxerr_aper[10], \
                               'FLUX_APER_11':flux_aper[11],'FLUXERR_APER_11':fluxerr_aper[11], \
                               'FLUX_APER_12':flux_aper[12],'FLUXERR_APER_12':fluxerr_aper[12], \
                               'CLASS_STAR':class_star, \
                               'SPREAD_MODEL':spread_model, \
                               'SPREADERR_MODEL':spreaderr_model, \
                               'FLAGS':flags \
                               })

    df_cat['RA_WRAP'] = df_cat['RA']
    ra_wrap_mask = (df_cat.RA_WRAP >= 180.)
    df_cat[ra_wrap_mask]['RA_WRAP'] = df_cat[ra_wrap_mask]['RA_WRAP'] - 360.

    cols = ['FILENAME',
            'NUMBER','X_IMAGE','Y_IMAGE','RA_WRAP','RA','DEC',
            'FLUX_PSF','FLUXERR_PSF',
            'FLUX_APER_01','FLUX_APER_02','FLUX_APER_03','FLUX_APER_04','FLUX_APER_05','FLUX_APER_06',
            'FLUX_APER_07','FLUX_APER_08','FLUX_APER_09','FLUX_APER_10','FLUX_APER_11','FLUX_APER_12',
            'FLUXERR_APER_01','FLUXERR_APER_02','FLUXERR_APER_03','FLUXERR_APER_04','FLUXERR_APER_05','FLUXERR_APER_06',
            'FLUXERR_APER_07','FLUXERR_APER_08','FLUXERR_APER_09','FLUXERR_APER_10','FLUXERR_APER_11','FLUXERR_APER_12',
            'CLASS_STAR','SPREAD_MODEL','SPREADERR_MODEL','FLAGS']
    df_cat = df_cat[cols]

    df_cat = df_cat.sort_values(by=['RA'])

    return df_cat

#--------------------------------------------------------------------------

if __name__ == "__main__":
    main()

#--------------------------------------------------------------------------
