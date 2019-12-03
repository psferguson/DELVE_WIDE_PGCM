# Optional command line argument is for the filter band to be used.
# If not given, assume all filter bands (u,g,r,i,z,Y)
BAND=${1:-all}
echo "BAND is "$BAND

if [ "$BAND" = "all" ] ; then
    bandList="u g r i z Y"
else
    bandList=$BAND
fi

for band in $bandList; do 

    for inputFile in Downloads/D*_$band\_*_fullcat.fits; do

	outputFile=$inputFile
	outputFile=${outputFile%.fits}
	# The following line was removed so that the outputFile will also be in the Downloads subdirectory (otherwise, outputFile would be in the current directory):
	#outputFile=${outputFile##*/}
	outputFile=$outputFile.csv

	echo $inputFile, $outputFile

	../../bin/DELVE_Calib_se_objects_fnal.py \
	    --inputFile=$inputFile \
	    --outputFile=$outputFile

    done

done

