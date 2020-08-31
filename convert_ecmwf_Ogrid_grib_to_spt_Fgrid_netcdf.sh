#!/bin/sh

# Requires wgrib and cdo with grib1 and netcdf4 support.
# Based on ENSCO.  MJS ERDC/CRREL

    cat $GRIB_FILE >> $COMB_FILE # do this for each GRIB file in the time range you want (put it in a loop but you’ll have to figure out the exact filenames
    wgrib $COMB_FILE | sed 's/kpds7=//' | sort -t: -k14.25n -k13n | \
        grep "$FCST_GREP" | wgrib -i -s -grib $COMB_FILE -o $GRIB_DIR"/"$ENS_NUM.runoff.grb
    cdo -s -f nc4 -t ecmwf setgridtype,regular $GRIB_DIR"/"$ENS_NUM.runoff.grb $GRIB_DIR"/"$ENS_NUM.runoff.nc
