#!/bin/sh

for i in */; do    ## For every file in this directory...
 if [ -d "$i" ]    ## ..it it is a directory ...
  then VAR="${i::-1}"; cd $VAR; rm *.shp.xml *.html; ## save in VAR its name W/O "/"
##
for suffix in a as fm l m ne sia sip v; do ## for each suffix..
  mkdir $suffix
  mv $VAR$suffix.* $suffix

  ogr2ogr -t_srs EPSG:4326 -f GeoJSON $suffix.json $suffix/$VAR$suffix.shp
  topojson -o ${suffix}_t.json $suffix.json -p
  rm -r $suffix

done			## Make a dir under its name and move corresponding files

##
  cd ..
 fi
done
