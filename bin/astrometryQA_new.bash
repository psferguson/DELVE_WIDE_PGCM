# To run:
#  source astrometryQA_new.bash <expnum>
#
# E.g., 
#  source astrometryQA_new.bash 908895

# Location of STILTS directory...
STILTS_DIR=/usrdevel/dp0/dtucker/STILTS/latest
#STILTS_DIR=/Users/dtucker/Software/STILTS/latest

expnum=$1
groupnum=$(($expnum/100))
dirName=/data/des50.b/data/BLISS/$groupnum\00/$expnum
# Special location, for Starlink exposure:
#dirName=/data/des51.a/data/kuropat/devel/se_test1/$expnum
echo $expnum
echo $groupnum
echo $dirName

echo 
echo "Copy data to current directory..."
rsync -avz $dirName/*fullcat.fits .

echo
echo "Create list of *fullcat.fits files to combine..."
rm -f inlist.$expnum.lis
ls -1 D00$expnum\_*_fullcat.fits | awk '{print $1"#2"}' > inlist.$expnum.lis

echo 
echo "Combine *fullcat.fits files into a single FITS file..."
$STILTS_DIR/stilts tcat \
    in=@inlist.$expnum.lis ifmt=fits \
    out=./D00$expnum\_fullcat.fits ofmt=fits

## Use cdsskymatch to match combined FITS file with Gaia DR2...
## (WARNING:  I HAVE CONNECTION ISSUES WITH THE CDS SERVER ON THE DES CLUSTER.
##            I DID THE REST OF THIS SCRIPT ON MY LAPTOP.)
#$STILTS_DIR/stilts cdsskymatch \
#    cdstable=I/345/gaia2 find=best \
#    in=D00913418_g_ALL_r1p1_fullcat.fits \
#    ra=ALPHAWIN_J2000 dec=DELTAWIN_J2000 \
#    radius=2.0 blocksize=1000 \
#    icmd=progress \
#    out=match_gaiadr2_D00913418_g_ALL_r1p1_fullcat.fits


echo
echo "Identify Gaia DR2 FITS files on disk that overlap the DECam exposure..."
rm -f D00$expnum\_fullcat.lis 
./DECamFullCat_id_gaiadr2_data.py \
    --inputFile D00$expnum\_fullcat.fits \
    --outputFile D00$expnum\_fullcat.lis \
    --verbose 3

echo 
echo "Combine Gaia DR2 FITS files into a single FITS file..."
$STILTS_DIR/stilts tcat \
    in=@D00$expnum\_fullcat.lis ifmt=fits \
    out=gaiadr2_D00$expnum\_fullcat.fits ofmt=fits

echo 
echo "Match DECam data to Gaia DR2 data..."
$STILTS_DIR/stilts tskymatch2 \
    in1=D00$expnum\_fullcat.fits \
    ra1=ALPHAWIN_J2000 dec1=DELTAWIN_J2000 \
    in2=gaiadr2_D00$expnum\_fullcat.fits \
    ra2=RA dec2=DEC \
    error=2.0 find=best \
    out=match_gaiadr2_D00$expnum\_fullcat.fits

echo 
echo "Create sky plot of matched objects, color coding the match by the separation in arcsec..."
$STILTS_DIR/stilts plot2sky \
   reflectlon=false sex=false \
   auxmap=rainbow auxflip=true auxmin=0.0 auxmax=2.0 \
   auxvisible=true auxlabel='separation [arcsec]' \
   legend=false \
   layer=Mark \
   in=match_gaiadr2_D00$expnum\_fullcat.fits \
   lon=RA lat=DEC \
   aux=Separation \
   shading=aux \
   out=match_gaiadr2_D00$expnum\_fullcat.skyplot.png

echo
echo "Create histogram plot of matched objects..."
$STILTS_DIR/stilts plot2plane \
   xlabel='Separation [arcsec]' ylabel='Number' \
   grid=true \
   xmin=0 xmax=2 ymin=0 \
   legend=false \
   layer=Histogram \
   in=match_gaiadr2_D00$expnum\_fullcat.fits \
   x=Separation \
   out=match_gaiadr2_D00$expnum\_fullcat.hist.png

echo "Finis!"
