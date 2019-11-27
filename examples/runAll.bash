#date=20191125
#band=i

# Optional command line argument is for the filter band to be used.
# If not given, assume all filter bands (u,g,r,i,z,Y)
# (Default value is "all", which later translates to "u g r i z Y").
# Not all script sections below make use of BAND.
BAND=${1:-all}
echo "BAND is "$BAND
if [ "$BAND" = "all" ] ; then
    bandList="u g r i z Y"
else
    bandList=$BAND
fi
echo "bandList is "$bandList

# The 2nd argument is DATE, which is used for naming the log files...
# (Default value is "test".)
DATE=${2:-test}
date=$DATE
echo "date is "$date


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# Preliminaries
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

# Set up necessary products...
#source  /usrdevel/dp0/dtucker/setup_DECam_PGCM.bash

# Set location of TMPDIR to a subdirectory with lots of space
# (to deal with very large files when using, e.g., unix sort)...
tmpdirName=$TMPDIR
echo "Setting TMPDIR to ./TmpDir..."
if [ ! -d TmpDir ] ; then 
    mkdir TmpDir
fi
export TMPDIR=./TmpDir

# if QA subdirectory does not yet exist, create it...
if [ ! -d QA ] ; then 
    mkdir QA
fi

# Location of STILTS directory...
STILTS_DIR=/usrdevel/dp0/dtucker/STILTS/latest


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# Logical parameters for running different sections of bash script:
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

###############################################
do_db_query="true"
do_grab_refcat2="true"
do_runConvert="true"
do_concat="true"
do_match="true"
do_calc_zps="true"


# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# Steps to photometric ZPs...
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 

# First couple steps are not filter-band-specific...

if [ "$do_db_query" = "true" ] ; then

    echo 
    echo DELVE_Calib_fileimgexp_query_fnal_Starlink2.py
    echo 

    rm -f DELVE_Calib_fileimgexp_query_fnal_Starlink2.$date.log

    ../../bin/DELVE_Calib_fileimgexp_query_fnal_Starlink2.py \
	> DELVE_Calib_fileimgexp_query_fnal_Starlink2.$date.log  2>&1

fi


if [ "$do_grab_refcat2" = "true" ] ; then

    echo 
    echo DELVE_grab_relevant_refcat2_data.py
    echo 

    rm -f DELVE_grab_relevant_refcat2_data.$date.log

    ../../bin/DELVE_grab_relevant_refcat2_data.py \
	--inputFile DELVE_Calib_expimgfileinfo_fnal.csv \
	--outputFile ATLAS_REFCAT_2.DELVE_DEEP_area.csv \
	> DELVE_grab_relevant_refcat2_data.$date.log  2>&1

fi


# Remaining steps are filter-band-specific (mostly for parallelizing the process)...

if [ "$do_runConvert" = "true" ] ; then

    for band in $bandList; do

	echo 
	echo runConvert_se_objectsFile_fnal.bash $band 
	echo 

	rm -f runConvert_se_objectsFile_fnal.$date.$band.log

	source ../../bin/runConvert_se_objectsFile_fnal.bash $band \
	    > runConvert_se_objectsFile_fnal.$date.$band.log  2>&1

    done

fi

if [ "$do_concat" = "true" ] ; then

    for band in $bandList; do

	echo 
	echo DELVE_Calib_concat_se_objects_fnal.py $band
	echo 

	rm -f DELVE_Calib_concat_se_objects_fnal.$date.$band.log

	../../bin/DELVE_Calib_concat_se_objects_fnal.py \
	    --inputFile DELVE_Calib_expimgfileinfo_fnal.csv \
	    --outputFile rawData.DELVE_Calib_DEEP.$band.csv \
	    --band $band \
	    > DELVE_Calib_concat_se_objects_fnal.$date.$band.log

    done

fi


if [ "$do_match" = "true" ] ; then

    for band in $bandList; do

	echo 
	echo DELVE_matchSortedStdObsCats.py $band
	echo 

	rm -f DELVE_matchSortedStdObsCats.$date.$band.log \

	../../bin/DELVE_matchSortedStdObsCats.py \
	    --inputStdStarCatFile ATLAS_REFCAT_2.DELVE_DEEP_area.csv \
	    --inputObsCatFile rawData.DELVE_Calib_DEEP.$band.csv \
	    --racolStdStarCatFile 50 --deccolStdStarCatFile 2 \
	    --racolObsCatFile 30 --deccolObsCatFile 32 \
	    --outputMatchFile matched-rawdata_refcat2_DEEP.$band.csv \
	    --matchTolerance 2.0 --verbose 2 \
	    > DELVE_matchSortedStdObsCats.$date.$band.log  2>&1 

    done

fi 


if [ "$do_calc_zps" = "true" ] ; then

    for band in $bandList; do

	echo 
	echo DELVE_tie_to_refcat2.py $band
	echo 

	rm -f DELVE_tie_to_refcat2.$date.$band.log

	../../bin/DELVE_tie_to_refcat2.py \
	    --inputFile matched-rawdata_refcat2_DEEP.$band.csv \
	    --band $band \
	    --fluxObsColName FLUX_APER_08_2 \
	    --fluxerrObsColName FLUXERR_APER_08_2 \
	    --aggFieldColName FILENAME_2 \
	    --outputFile zps.matched-rawdata_refcat2_DEEP.$band.csv \
	    > DELVE_tie_to_refcat2.$date.$band.log  2>&1 

    done

fi
