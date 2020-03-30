#!/usr/bin/env python

# Initial setup...
import numpy as np
import pandas as pd
import math
import glob
import os
import matplotlib.pyplot as plt


#--------------------------------------------------------------------------
# Main code.

def main():

    import argparse

    """Create command line arguments"""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--inputFile', help='name of the input file', default='DELVE_Calib_expimgfileinfo_fnal_DEEP.csv')
    parser.add_argument('--outputFile', help='name of the output file', default='rawData.DELVE_Calib_DEEP.csv')
    parser.add_argument('--catDirName', help='name of the directory where the individual catalog files are located', default='.')
    parser.add_argument('--band', help='name of the filter band', default='g')
    parser.add_argument('--verbose', help='verbosity level of output to screen (0,1,2,...)', default=0, type=int)
    args = parser.parse_args()

    if args.verbose > 0: print args

    status = DELVE_Calib_concat_se_objects_fnal(args)

    return status

#--------------------------------------------------------------------------

def DELVE_Calib_concat_se_objects_fnal(args):

    inputFile=args["concat_input_file"]
    outputFile=args["concat_output_file"]
    catDirName=args["concat_dir_name"]
    band=args["band"]

    df_input = pd.read_csv(inputFile)

    df_input['FILENAME'] = df_input['FILEPATH'].apply(lambda x: os.path.basename(x))

    fileList = []
    mask = (df_input.BAND == band)
    for index, row in df_input[mask].iterrows():
        fileName = row['FILENAME']
        fileName = os.path.splitext(fileName)[0]
        fileName = """%s/%s.csv""" % (catDirName, fileName)
        if os.path.isfile(fileName):
            fileList.append(fileName)
            print fileName
        else:
            print fileName+':  BAD'

    if args["verbose"]>0:
        print 'Concatenating individual files...'
    df_seobj = pd.concat(pd.read_csv(f) for f in fileList)

    if args["verbose"]>0:
        print df_seobj.RA.size

    if args["verbose"]>0:
        print 'Merging exp/imginfo with se_obj info...'
    df_merge = pd.merge(df_input, df_seobj, \
                            on=['FILENAME'], \
                            how='inner', \
                            suffixes=('','_y')).reset_index(drop=True)

    if args["verbose"]>0:
        print 'Sorting merged file by RA_WRAP...'
    df_merge = df_merge.sort_values(by=['RA_WRAP'])

    if args["verbose"]>0:
        print 'Outputting merged file...'
    df_merge.to_csv(outputFile, index=False)


    return 0

#--------------------------------------------------------------------------

if __name__ == "__main__":
    main()

#--------------------------------------------------------------------------
