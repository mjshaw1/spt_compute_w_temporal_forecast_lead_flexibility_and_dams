#!/bin/bash
#
# Requires cdo with grib1 and netCDF4 support.
# MJS, ERDC/CRREL
#
# Among various ways to slice and dice batches of ECMWF runoff files; provided in case varying formats come in, various perspectives/interests in conversion.  This was a first step
# while trying to figure out what CHL, ENSCO, customer really needed and wanted (without access to ENSCO or customer to hone in at the time).
#
# Assume can’t know domain boundaries on low side, so can’t decimate by that to save space/bandwidth, so must just send all files – for 5 day forecast - to then do specifics on high side.
# Assume you’ve pulled all grib from ECMWF on low side for latest model cycle to directory; so tar.gz of first 5 days of forecast 3 hourly files, send off to other side and tar zxvf’ed on high side.
# Assume ECMWF is already doing a check on whether all files are present and no times skipped (so no need for error trap on that on unclass side - but maybe a chance of a transfer hiccup to check for on the class side?).
# Assume cdo is properly installed and loaded on high side.
# Assume nothing else is ... (no NCO, wgrib, Python IRIS, etc).
# Assume no need to put into RAPID CF compliance - though the ECMWF nc files it looks like Mark refers to on the UROC shared drive as a model to "stop" at are not in that form?
#
# Low side: tar zcvf latest_ecmwf_lores.tar.gz `ls A1E* | head -41`; secure transfer >
# Low side: tar zcvf latest_ecmwf_hires.tar.gz `ls A1D* | head -41`; secure transfer >
# High side: tar zxvf latest_ecmwf_lores.tar.gz
# High side: tar zxvf latest_ecmwf_hires.tar.gz
#
# Step by step for ease of read (?).
## But first, it seems sometimes there might be gz'ed grib files in the bunch:
if [ -e *.gz ]; then gunzip *.gz ; fi
# 1  Grib files are 3 hourly files of 51 ensemble grib records.  Pull the 51 LOW RES records of first 41 timesteps/files in batch into individual files time/rec to then merge into 51 ensemble member timeseries files of 41 timesteps in each file.
for i in `ls A1E*`; do for j in {1..51}; do cdo -selrec,$j $i $i.$j; done; done
# 2 Clean up orig files, leaving 51 ensemble files.
rm A1E*001
# 3 Now, for each LOW RES ensemble member, merge all times' files under that record/ensemble member into 51 timeseries files for each ensemble member containing 41 timesteps.
for i in {1..51}; do cdo merge *.$i $i.gr; done
# 4 Clean up 51 sets of ensemble/time files, leaving the gr files.
for i in {1..51}; do rm *.$i; done
# 5 Make 51 LOW RES netcdf ensemble members of the 51 grib1s.
for i in {1..51}; do cdo -f nc copy $i.gr $i.nc; done
# 6 Clean up gr files, leaving the nc files.
for i in {1..51}; do rm $i.gr; done
#
# 7 Merge the HIGH RES files into one high res timeseries as ensemble member 52 timeseries of 41 steps.
cdo merge `ls A1D*` 52.gr
# 8 Clean up orig timestep files, leave gr file.
rm A1D*
# 9 Make the 52nd HIGH RES ensemble member netcdf of the 52nd grib.
cdo -f nc copy 52.gr 52.nc
# 10 Clean up gr file, leave nc file.
rm 52.gr
# 11 Make sure variable names suit in all 52 ensemble member timeseries nc files.
for i in {1..52}; do cdo chname,var205,RO $i.nc $i.temp.nc; done
# 12 Clean up the nc files with gribby var names.
for i in {1..52}; do rm $i.nc; done
# Lucky 13 - better test this!  Make sure attributes suit in all 52 ensemble member timeseries files.
for i in {1..52}; do cdo setattribute,RO@units=m,RO@long_name=Runoff,RO@grid_type=gaussian $i.temp.nc $i.runoff.nc ; done
# 14
# Clean up temp files that didn't have all the other attributes.
for i in {1..52}; do rm $i.temp.nc; done
 
exit 0
 
# Some more cdo for other interests if needed:

#cdo seltimestep,1/41 1.runoff.nc 1.firststeps.nc # Coarse files are 3 hourly.
#cdo -f grb copy 1.firststeps.nc 1.firststeps.gr
#cdo seltimestep,1/90/3 52.runoff.nc 52.runoff.90.nc # Fine files are 1 hourly for first 90 steps, then 3 hourly.
#cdo seltimestep,91/101 52.runoff.nc 52.runoff.91-101.nc # 90 + 3*10 = 120
#cdo mergetime 52.runoff.9*nc 52.firststeps.nc
#cdo -f grb copy 52.firststeps.nc 52.firststeps.gr
 
#cdo sellonlatbox,-100,40,-110,45 in.nc out.nc
 
