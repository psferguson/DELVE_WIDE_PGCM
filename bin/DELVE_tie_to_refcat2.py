#!/usr/bin/env python
"""
    DELVE_tie_to_stds.py

    Generic version of tie_to_fgcm_stds.py.

    Example:
    
    DELVE_tie_to_stds.py --help

    DELVE_tie_to_stds.py --inputFile inputFile.csv --outputFile outputFile.csv --verbose 2
    
    """

##################################

def main():

    import argparse
    import time

    """Create command line arguments"""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--inputFile', 
                        help='name of the input CSV file', 
                        default='input.csv')
    parser.add_argument('--outputFile', 
                        help='name of the output CSV file', 
                        default='output.csv')
    parser.add_argument('--band', 
                        help='filter band to calibrate (u, g, r, i, z, or Y)', 
                        default='g')
    parser.add_argument('--fluxObsColName', 
                        help='name of the column in inputFile containing the observed flux', 
                        default='FLUX_APER_08_2')
    parser.add_argument('--fluxerrObsColName', 
                        help='name of the column in inputFile containing the observed fluxerr', 
                        default='FLUXERR_APER_08_2')
    parser.add_argument('--aggFieldColName', 
                        help='name of the column in inputFile containing the field to aggregate', 
                        default='FILENAME_2')
    parser.add_argument('--verbose', 
                        help='verbosity level of output to screen (0,1,2,...)', 
                        default=0, type=int)
    args = parser.parse_args()

    # NEED TO ADD SANITY CHECKS ON FILTER BAND, ETC.

    if args.verbose > 0: print args

    status = DELVE_tie_to_stds(args)


##################################
# 

def DELVE_tie_to_stds(args):

    import numpy as np 
    import os
    import sys
    import datetime
    import pandas as pd

    inputFile = args.inputFile
    outputFile = args.outputFile
    band = args.band
    fluxObsColName = args.fluxObsColName
    fluxerrObsColName = args.fluxerrObsColName
    aggFieldColName = args.aggFieldColName

    #magATLASColName = """%s_1""" % (band.upper())
    #magerrATLASColName = """D%s_1""" % (band.upper())

    reqColList = ['G_1','R_1','I_1','Z_1','DG_1','DR_1','DI_1','DZ_1',fluxObsColName,fluxerrObsColName,aggFieldColName]

    # Does the input file exist?
    if os.path.isfile(inputFile)==False:
        print """DELVE_tie_to_stds input file %s does not exist.  Exiting...""" % (inputFile)
        return 1

    # Read inputFile into a pandas DataFrame...
    print datetime.datetime.now()
    print """Reading in %s as a pandas DataFrame...""" % (inputFile)
    dataFrame = pd.read_csv(inputFile)
    print datetime.datetime.now()
    print

    reqColFlag = 0
    colList = dataFrame.columns.tolist()
    for reqCol in reqColList:
        if reqCol not in colList:
            print """ERROR:  Required column %s is not in the header""" % (reqCol)
            reqColFlag = 1
    if reqColFlag == 1:
        print """Missing required columns in header of %s...  Exiting now!""" (inputFile)
        return 1

    # Transform ATLAS-REFCAT2 mags into DES mags for this filter band...
    if band is 'g':
        # g_des = g_ps + 0.0994*(g-r)_ps - 0.0076    -0.2 < (g-r)_ps <= 1.2
        dataFrame['g_des'] = dataFrame['G_1'] + 0.0994*(dataFrame['G_1']-dataFrame['R_1']) - 0.0076
        dataFrame['gerr_des'] = dataFrame['DG_1']  # temporary
        magStdColName = 'g_des'
        magerrStdColName = 'gerr_des'
        mask1 = ( (dataFrame['G_1']-dataFrame['R_1']) > -0.2) 
        mask2 = ( (dataFrame['G_1']-dataFrame['R_1']) <= 1.2)
        mask = (mask1 & mask2)
        dataFrame = dataFrame[mask].copy()
    elif band is 'r':
        # r_des = r_ps - 0.1335*(g-r)_ps + 0.0189    -0.2 < (g-r)_ps <= 1.2
        dataFrame['r_des'] = dataFrame['R_1'] - 0.1335*(dataFrame['G_1']-dataFrame['R_1']) + 0.0189
        dataFrame['rerr_des'] = dataFrame['DR_1']  # temporary
        magStdColName = 'r_des'
        magerrStdColName = 'rerr_des'
        mask1 = ( (dataFrame['G_1']-dataFrame['R_1']) > -0.2) 
        mask2 = ( (dataFrame['G_1']-dataFrame['R_1']) <= 1.2)
        mask = (mask1 & mask2)
        dataFrame = dataFrame[mask].copy()
    elif band is 'i':
        # i_des = i_ps - 0.3407*(i-z)_ps + 0.0026    -0.2 < (i-z)_ps <= 0.3
        dataFrame['i_des'] = dataFrame['I_1'] - 0.3407*(dataFrame['I_1']-dataFrame['Z_1']) + 0.0026
        dataFrame['ierr_des'] = dataFrame['DI_1']  # temporary
        magStdColName = 'i_des'
        magerrStdColName = 'ierr_des'
        mask1 = ( (dataFrame['I_1']-dataFrame['Z_1']) > -0.2) 
        mask2 = ( (dataFrame['I_1']-dataFrame['Z_1']) <= 0.3)
        mask = (mask1 & mask2)
        dataFrame = dataFrame[mask].copy()
    elif band is 'z':
        # z_des = z_ps - 0.2575*(i-z)_ps - 0.0074    -0.2 < (i-z)_ps <= 0.3
        dataFrame['z_des'] = dataFrame['Z_1'] - 0.2575*(dataFrame['I_1']-dataFrame['Z_1']) - 0.0074
        dataFrame['zerr_des'] = dataFrame['DZ_1']  # temporary
        magStdColName = 'z_des'
        magerrStdColName = 'zerr_des'
        mask1 = ( (dataFrame['I_1']-dataFrame['Z_1']) > -0.2) 
        mask2 = ( (dataFrame['I_1']-dataFrame['Z_1']) <= 0.3)
        mask = (mask1 & mask2)
        dataFrame = dataFrame[mask].copy()
    elif band is 'Y':
        # Y_des = z_ps - 0.6032*(i-z)_ps + 0.0185    -0.2 < (i-z)_ps <= 0.3
        dataFrame['Y_des'] = dataFrame['Z_1'] - 0.6032*(dataFrame['I_1']-dataFrame['Z_1']) + 0.0185
        dataFrame['Yerr_des'] = dataFrame['DZ_1']  # temporary
        magStdColName = 'Y_des'
        magerrStdColName = 'Yerr_des'
        mask1 = ( (dataFrame['I_1']-dataFrame['Z_1']) > -0.2) 
        mask2 = ( (dataFrame['I_1']-dataFrame['Z_1']) <= 0.3)
        mask = (mask1 & mask2)
        dataFrame = dataFrame[mask].copy()
    else:
        print """Filter band %s is not currently handled...  Exitig now!""" % (band)
        return 1


    # Add a 'MAG_OBS' column and a 'MAG_DIFF' column to the pandas DataFrame...
    dataFrame['MAG_OBS'] = -2.5*np.log10(dataFrame[fluxObsColName])
    dataFrame['MAG_DIFF'] = dataFrame[magStdColName]-dataFrame['MAG_OBS']


    ###############################################
    # Aggregate by aggFieldColName
    ###############################################

    # Make a copy of original dataFrame...
    df = dataFrame.copy()

    # Create an initial mask...
    mask1 = ( (df[magStdColName] >= 0.) & (df[magStdColName] <= 25.) )
    mask1 = ( mask1 & (df['FLUX_PSF_2'] > 10.) & (df['FLAGS_2'] < 2) & (np.abs(df['SPREAD_MODEL_2']) < 0.01))
    if magerrStdColName != 'None':  
        mask1 = ( mask1 & (df[magerrStdColName] < 0.1) )
    magPsfDiffGlobalMedian = df[mask1]['MAG_DIFF'].median()
    magPsfDiffMin = magPsfDiffGlobalMedian - 5.0
    magPsfDiffMax = magPsfDiffGlobalMedian + 5.0
    mask2 = ( (df['MAG_DIFF'] > magPsfDiffMin) & (df['MAG_DIFF'] < magPsfDiffMax) )
    mask = mask1 & mask2

    # Iterate over the copy of dataFrame 3 times, removing outliers...
    #  We are using "Method 2/Group by item" from
    #  http://nbviewer.jupyter.org/urls/bitbucket.org/hrojas/learn-pandas/raw/master/lessons/07%20-%20Lesson.ipynb
    print "Sigma-clipping..."
    niter = 0
    for i in range(3):

        niter = i + 1
        print """   iter%d...""" % ( niter )

        # make a copy of original df, and then delete the old one...
        newdf = df[mask].copy()
        del df

        # group by aggFieldColName...
        grpnewdf = newdf.groupby([aggFieldColName])
        
        # add/update new columns to newdf
        print datetime.datetime.now()
        newdf['Outlier']  = grpnewdf['MAG_DIFF'].transform( lambda x: abs(x-x.mean()) > 3.00*x.std() )
        #newdf['Outlier']  = grpnewdf['MAG_DIFF'].transform( lambda x: abs(x-x.mean()) > 2.00*x.std() )
        print datetime.datetime.now()
        del grpnewdf
        print datetime.datetime.now()
        #print newdf

        nrows = newdf['MAG_DIFF'].size
        print """  Number of rows remaining:  %d""" % ( nrows )

        df = newdf
        mask = ( df['Outlier'] == False )  


    # Perform pandas grouping/aggregating functions on sigma-clipped Data Frame...
    print datetime.datetime.now()
    print 'Performing grouping/aggregating functions on sigma-clipped pandas DataFrame...'
    groupedDataFrame = df.groupby([aggFieldColName])
    magPsfZeroMedian = groupedDataFrame['MAG_DIFF'].median()
    magPsfZeroMean = groupedDataFrame['MAG_DIFF'].mean()
    magPsfZeroStd = groupedDataFrame['MAG_DIFF'].std()
    magPsfZeroNum = groupedDataFrame['MAG_DIFF'].count()
    magPsfZeroErr = magPsfZeroStd/np.sqrt(magPsfZeroNum-1)
    print datetime.datetime.now()
    print

    # Rename these pandas series...
    magPsfZeroMedian.name = 'MAG_ZERO_MEDIAN'
    magPsfZeroMean.name = 'MAG_ZERO_MEAN'
    magPsfZeroStd.name = 'MAG_ZERO_STD'
    magPsfZeroNum.name = 'MAG_ZERO_NUM'
    magPsfZeroErr.name = 'MAG_ZERO_MEAN_ERR'

    # Also, calculate group medians for all columns in df that have a numerical data type...
    numericalColList = df.select_dtypes(include=[np.number]).columns.tolist()
    groupedDataMedian = {}
    for numericalCol in numericalColList:
        groupedDataMedian[numericalCol] = groupedDataFrame[numericalCol].median()
        groupedDataMedian[numericalCol].name = """%s_MEDIAN""" % (numericalCol)

    # Create new data frame containing all the relevant aggregate quantities
    #newDataFrame = pd.concat( [magPsfZeroMedian, magPsfZeroMean, magPsfZeroStd, \
    #                           magPsfZeroErr, magPsfZeroNum], \
    #                           join='outer', axis=1 )
    seriesList = []
    for numericalCol in numericalColList:
        seriesList.append(groupedDataMedian[numericalCol])
    seriesList.extend([magPsfZeroMedian, magPsfZeroMean, magPsfZeroStd, \
                               magPsfZeroErr, magPsfZeroNum])
    #print seriesList
    newDataFrame = pd.concat( seriesList, join='outer', axis=1 )
                               

    #newDataFrame.index.rename('FILENAME', inplace=True)

    # Saving catname-based results to output files...
    print datetime.datetime.now()
    print """Writing %s output file (using pandas to_csv method)...""" % (outputFile)
    newDataFrame.to_csv(outputFile, float_format='%.4f')
    print datetime.datetime.now()
    print

    return 0


##################################

if __name__ == "__main__":
    main()

##################################

