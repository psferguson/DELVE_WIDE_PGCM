# Edited on 200324 by PSF
# python ../../bin/python_driver/DELVE_WIDE_run.py --RaMin 178 --RaMax 180 --DecMin -22 --DecMax -20 --BinIndex 70

# given ra dec limits and a bin number and yaml config file run the calibration for a subset of dataself.


# Initial setup...
import numpy as np
import pandas as pd
import sys
import os
current_dir=os.getcwd()
sys.path.append(current_dir+"../../bin/")
import DELVE_WIDE_query_fnal as DELVE_query
import DELVE_WIDE_grab_refcat2 as DELVE_grab_refcat
import DELVE_WIDE_se_objects_fnal as DELVE_se_obj
import DELVE_WIDE_concat_se_objects_fnal as DELVE_concat_se_obj
import DELVE_matchSortedStdObsCats as DELVE_match
import DELVE_tie_to_stds as calc_zps
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
            'do_grab_refcat2':False,
            'do_runConvert':False,
            'do_concat':False,
            'do_match':False,
            'do_calc_zps':False,
            'prefix':"test",
            'band_list':["g"]#["u", "g", "r", "i", "z", "Y"],
            'verbose':1,
            #### db query  parameters
            'query_filepaths_outfile_prefix':'DELVE_Calib_filepaths_bin',
            'query_expinfo_outfile_prefix':'DELVE_Calib_expinfo_fnal_',
            'query_imginfo_outfile_prefix':'DELVE_Calib_imginfo_fnal_bin',
            'query_expimgfileinfo_outfile_prefix':'DELVE_Calib_expimgfileinfo_fnal_bin',
            #### grab refcat params
            'grab_refcat_outputFile':'ATLAS_REFCAT_2.DELVE_WIDE_',

            #concat se obj param
            'concat_dir_name':'./Downloads'
            #### Match config parameters
            'racolStdStarCatFile': 50,
            'deccolStdStarCatFile':2,
            'racolObsCatFile':30,
            'deccolObsCatFile':32,
            'matchTolerance': 2.0
            #### zps calc parameters
            'fluxObsColName':'FLUX_APER_08_2',
     	    'fluxerrObsColName':'FLUXERR_APER_08_2',
     	    'aggFieldColName' : 'FILENAME_2',
            }
    config["RaMin"]=args.RaMin
    config["RaMax"]=args.RaMax
    config["DecMin"]=args.DecMin
    config["DecMax"]=args.DecMax
    config["BinIndex"]=args.BinIndex
    config["grab_refcat_inputFile"]=config["query_filepaths_outfile_prefix"]


    print "prefix is "+prefix

    # Optional command line argument is for the filter band to be used.
    # If not given, assume all filter bands (u,g,r,i,z,Y)
    # (Default value is "all", which later translates to "u g r i z Y").
    # Not all script sections below make use of BAND.
    print "band list is "  + bandList


    # 1. we call DELVE_Calib_fileimgexp_query_fnal.py
    if config["do_db_query"] == True:
            print "Doing database query"
            #os.remove("DELVE_query_wide"+prefix".log")
            status=DELVE_query.do_db_querys(config)
    # 2. we call DELVE_grab_relevant_refcat2_data.py
    if config["do_grab_refcat2"] == True:
            print "grabbing refcat"
            #input file= 'DELVE_Calib_filepaths_fnal_'
            #outputfile= ’ATLAS_REFCAT_2.DELVE_WIDE_'
            #bin index
            # verbose
            status=DELVE_grab_refcat.do_grab_refcat2(config)
    ############## this is where we split into filters ##############
    #################################################################
    #################################################################
    # if we do that will set band equal to whatever and not have for loops
    for band in config["band_list"]:
        config["band"]=band
        if config["do_runConvert"] == True:
            #this replaces the symbolic link step
            print "Converting to CSV cats"

            if os.path.isfile(inputFile)==False:
                print """input file %s does not exist.  Exiting...""" % (inputFile)
                return 1\
            filepaths_file='DELVE_Calib_filepaths_fnal_'+bin_index+'.csv'
            df_filepaths=pd.read_csv(filepaths_file, usecols=['FILEPATH'])
            run_con_config=config.copy()
            subset=df_filepaths["FILEPATH"][df_filepaths["FILEPATH"].str[-22]==band]

            if len(subset) > 0:
                for input_file in df_filepaths["FILEPATH"]:
                    trunc_pos=inputFile.rfind("/")+1
                    output_file="./Downloads/"+inputFile[trunc_pos:-5]+'.csv'
                    run_con_config["se_input_file"]=input_file
                    run_con_config["se_output_file"]=output_file
                    status=DELVE_se_obj.DELVE_Calib_se_objects_fnal(config)


        if config["do_concat"] == True:
            concat_config=config.copy()

            concat_config["concat_input_file"]=config["query_expimgfileinfo_outfile_prefix"]+concat_config["BinIndex"]+'.csv'
            concat_config["concat_output_file"]='rawData.DELVE_Calib.'+config["band"].+config["BinIndex"]+'.csv

            status=DELVE_concat_se_obj.DELVE_Calib_concat_se_objects_fnal(concat_config)
            # ../../bin/DELVE_Calib_concat_se_objects_fnal.py \
        	#     --inputFile DELVE_Calib_expimgfileinfo_fnal.csv \
        	#     --outputFile rawData.DELVE_Calib_DEEP.$band.csv \
        	#     --band $band \
        	#     --catDirName ./Downloads \
        	#     > DELVE_Calib_concat_se_objects_fnal.$date.$band.log
        if config["do_match"] == True:
                match_config=config.copy()

                match_config["verbose"]=2
                match_config["match_input_std_cat"]=config["grab_refcat_outputFile"]+config["BinIndex"]+'.csv'
                match_config["match_input_obs_cat"]='rawData.DELVE_Calib.'+config["band"].+config["BinIndex"]+'.csv
                match_config["match_output_file"]='matched-rawdata_refcat2_DEEP.'+config["band"].+config["BinIndex"]+'.csv

                status=DELVE_match.DELVE_matchSortedStdObsCats(match_config)


        if config["do_calc_zps"] == True:
                calc_zps_config=config.copy()
                calc_zps_config["zps_input_file"]='matched-rawdata_refcat2_DEEP.'+config["band"].+config["BinIndex"]+'.csv
                calc_zps_config["zps_output_file"]='zps.matched-rawdata_refcat2_WIDE'+config["band"].+config["BinIndex"]+'.csv
                status=DELVE_calc_zps.DELVE_tie_to_stds(calc_zps_config)
if __name__ == "__main__":
    main()
