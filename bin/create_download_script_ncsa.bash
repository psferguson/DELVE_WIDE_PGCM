if [ -f download.sh ] ; then
    mv download.sh download.sh~
fi

echo "#!/usr/bin/env bash" > download.sh
echo >> download.sh
echo "USER=$1                   # replace with your user name" >> download.sh
echo "PASSWD=$2                 # replace with your password" >> download.sh
echo "ROOT=`pwd`                # specify where you want the files downloaded (default is current directory)" >> download.sh
echo "OPTIONS="--create-dirs"   # specify any flags for the wget process" >> download.sh

awk -F, 'NR>1 {print "curl -u \$USER:\$PASSWD \$OPTIONS" $9

chmod +x download.sh



