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

    #inputFile=../../Downloads/NCSA/D00827408_i_c34_r4087p01_red-fullcat.fits
    for inputFile in ../../Downloads/NCSA/D*_$band\_*_red-fullcat.fits; do

	outputFile=$inputFile
	outputFile=${outputFile%.fits}
	outputFile=${outputFile##*/}
	outputFile=$outputFile.csv

	echo $inputFile, $outputFile

	../../bin/DELVE_Calib_se_objects_ncsa.py \
	    --inputFile=$inputFile \
	    --outputFile=$outputFile

    done

done

