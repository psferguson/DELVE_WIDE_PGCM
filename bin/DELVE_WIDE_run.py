# Edited on 200324 by PSF
# python ../../bin/python_driver/DELVE_WIDE_run.py --RaMin 178 --RaMax 180 --DecMin -22 --DecMax -20 --BinIndex 70

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
    parser.add_argument('--Config',nargs='?')
    args = parser.parse_args() # making it a python dict explicitly
    if args.config:
        config = yaml.load(open(args.config))
    else:
	raise ValueError('Config File Argument Required')
    config={'do_db_query':True,
            'do_grab_refcat2':True,
            'do_runConvert':True,
            'do_concat':True,
            'do_match':True
            'do_calc_zps':True
            'prefix':"test",
            'band_list':["u", "g", "r", "i", "z", "Y"],
            'verbose':1}
    config["RaMin"]=args.RaMin
    config["RaMax"]=args.RaMax
    config["DecMin"]=args.DecMin
    config["DecMax"]=args.DecMax
    config["BinIndex"]=args.BinIndex


    # do_db_query=True
    # do_grab_refcat2=True
    # do_runConvert="true"
    # do_concat="true"
    # do_match="true"
    # do_calc_zps="true"
    # prefix="test"
    # band_list=["u", "g", "r", "i", "z", "Y"]
    # verbose=1
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
    if config["do_db_query"] == True:
            print "Doing database query"
            #os.remove("DELVE_query_wide"+prefix".log")
            DELVE_query.do_db_querys(config)
    # 2. we call DELVE_grab_relevant_refcat2_data.py
    if config["do_grab_refcat2"] == True:
            print "grabbing refcat"
            #input file= 'DELVE_Calib_filepaths_fnal_'
            #outputfile= ’ATLAS_REFCAT_2.DELVE_WIDE_'
            #bin index
            # verbose
            DELVE_grab_refcat2.do_grab_refcat2(config)
    ############## this is where we split into filters ##############
    # if we do that will set band equal to whatever and not have for loops
    if config["do_runConvert"] == True:
            print "Converting to CSV cats"
            if os.path.isfile(inputFile)==False:
                print """input file %s does not exist.  Exiting...""" % (inputFile)
                return 1\
            filepaths_file='DELVE_Calib_filepaths_fnal_'+bin_index+'.csv'
            df_filepaths=pd.read_csv(filepaths_file, usecols=['FILEPATH'])
            run_con_config=config.copy()
            for band in band_list:
                subset=df_filepaths["FILEPATH"][df_filepaths["FILEPATH"].str[-22]==band]
                run_con_config["band"]==band
                if len(subset) > 0:
                    for input_file in df_filepaths["FILEPATH"]:
                        trunc_pos=inputFile.rfind("/")+1
                        output_file="./Downloads/"+inputFile[trunc_pos:-5]+'.csv'
                        run_con_config["se_input_file"]=input_file
                        run_con_config["se_output_file"]=output_file
                        DELVE_se_obj.DELVE_Calib_se_objects_fnal(config)
    if config["do_concat"] == True:
        concat_config=config.copy()
        ../../bin/DELVE_Calib_concat_se_objects_fnal.py \
    	    --inputFile DELVE_Calib_expimgfileinfo_fnal.csv \
    	    --outputFile rawData.DELVE_Calib_DEEP.$band.csv \
    	    --band $band \
    	    --catDirName ./Downloads \
    	    > DELVE_Calib_concat_se_objects_fnal.$date.$band.log
    if config["do_match"] == True:
        ./../bin/DELVE_matchSortedStdObsCats.py \
    	    --inputStdStarCatFile ATLAS_REFCAT_2.DELVE_DEEP_area.csv \
    	    --inputObsCatFile rawData.DELVE_Calib_DEEP.$band.csv \
    	    --racolStdStarCatFile 50 --deccolStdStarCatFile 2 \
    	    --racolObsCatFile 30 --deccolObsCatFile 32 \
    	    --outputMatchFile matched-rawdata_refcat2_DEEP.$band.csv \
    	    --matchTolerance 2.0 --verbose 2 \
    	    > DELVE_matchSortedStdObsCats.$date.$band.log  2>&1
    if config["do_calc_zps"] == True:
        ../../bin/DELVE_tie_to_refcat2.py \
    	    --inputFile matched-rawdata_refcat2_DEEP.$band.csv \
    	    --band $band \
    	    --fluxObsColName FLUX_APER_08_2 \
    	    --fluxerrObsColName FLUXERR_APER_08_2 \
    	    --aggFieldColName FILENAME_2 \
    	    --outputFile zps.matched-rawdata_refcat2_DEEP.$band.csv \
    	    > DELVE_tie_to_refcat2.$date.$band.log  2>&1
if __name__ == "__main__":
    main()
