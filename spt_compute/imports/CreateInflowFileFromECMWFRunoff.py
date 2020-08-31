'''-------------------------------------------------------------------------------
 Tool Name:   CreateInflowFileFromECMWFRunoff
 Source Name: CreateInflowFileFromECMWFRunoff.py
 Version:     ArcGIS 10.3
 Author:      Environmental Systems Research Institute Inc.
 Updated by:  Alan D. Snow, US Army ERDC
 Description: Creates RAPID inflow file based on the WRF_Hydro land model output
              and the weight table previously created.
 History:     Initial coding - 10/21/2014, version 1.0
 Updated:     Version 1.0, 10/23/2014, modified names of tool and parameters
              Version 1.0, 10/28/2014, added data validation
              Version 1.0, 10/30/2014, initial version completed
              Version 1.1, 11/05/2014, modified the algorithm for extracting runoff
                variable from the netcdf dataset to improve computation efficiency
              Version 1.2, 02/03/2015, bug fixing - output netcdf3-classic instead
                of netcdf4 as the format of RAPID inflow file
              Version 1.2, 02/03/2015, bug fixing - calculate inflow assuming that
                ECMWF runoff data is cumulative instead of incremental through time
              Version 1.3, 02/28/2019, revised durations to use a variable temporal
			    range. CJB ENSCO, MJS ERDC CRREL
-------------------------------------------------------------------------------'''
import netCDF4 as NET
import numpy as NUM
import csv
from io import open

class CreateInflowFileFromECMWFRunoff(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.label = "Create Inflow File From ECMWF Runoff"
        self.description = ("Creates RAPID NetCDF input of water inflow " +
                       "based on ECMWF runoff results and previously created weight table.")
        self.canRunInBackground = False
        #CJB self.header_wt = ['StreamID', 'area_sqm', 'lon_index', 'lat_index', 'npoints']
        self.header_wt = ['rivid', 'area_sqm', 'lon_index', 'lat_index', 'npoints']
        #SDR added new structure to fit new ecmwf ##.runoff.nc file order
        #self.dims_oi = [['lon', 'lat', 'time'], ['longitude', 'latitude', 'time']]
        self.dims_oi = [['lon', 'lat', 'time'], ['longitude', 'latitude', 'time'], ['time','lon','lat']] # Line Added/Modified CJB 20190108
        #self.vars_oi = [["lon", "lat", "time", "RO"], ['longitude', 'latitude', 'time', 'ro']]
        self.vars_oi = [["lon", "lat", "time", "RO"], ['longitude', 'latitude', 'time', 'ro'], ["time", "lon", "lat", "RO"]] # Line Added/Modified CJB 20190108
        self.length_time = {"LowRes": 61, "Low3HrRes": 40, "LowResFull": 85,"HighRes": 125, "High3HrRes":3}  # *** MJS What is High3HrRes for?  Doesn't seem to be used.
        #self.length_time = {"LowResFull": 85,"HighRes": 125}
        self.length_time_opt = {"LowRes-6hr": 60, "LowRes-3hr": 40,
                                "LowResFull-3hr-Sub": 48, "LowResFull-6hr-Sub": 36,
                                "HighRes-1hr": 90, "HighRes-3hr": 48, "HighRes-6hr": 40,  # *** MJS HighRes-3hr was changed to 40 before; why?
                                "HighRes-3hr-Sub": 18, "HighRes-6hr-Sub": 16}
        self.errorMessages = ["Missing Variable 'time'",
                              "Incorrect dimensions in the input ECMWF runoff file.",
                              "Incorrect variables in the input ECMWF runoff file.",
                              "Incorrect time variable in the input ECMWF runoff file",
                              "Incorrect number of columns in the weight table",
                              "No or incorrect header in the weight table",
                              "Incorrect sequence of rows in the weight table"]

    def dataValidation(self, in_nc):
        """Check the necessary dimensions and variables in the input netcdf data"""
        vars_oi_index = None
        data_nc = NET.Dataset(in_nc)
        dims = list(data_nc.dimensions)
        if dims not in self.dims_oi:
            raise Exception(self.errorMessages[1])

        vars = list(data_nc.variables)

        if vars == self.vars_oi[0]:
            vars_oi_index = 0
        elif vars == self.vars_oi[1]:
            vars_oi_index = 1
        elif vars == self.vars_oi[2]: # Line Added/Modified CJB 20190108
            vars_oi_index = 2         # Line Added/Modified CJB 20190108
        else:    
            raise Exception(self.errorMessages[2])

        return vars_oi_index


    def dataIdentify(self, in_nc):
        """Check if the data is Ensemble 1-51 (low resolution) or 52 (high resolution)"""
        data_nc = NET.Dataset(in_nc)
        time = data_nc.variables['time'][:]
        diff = NUM.unique(NUM.diff(time))
        data_nc.close()
        #time_interval_highres = NUM.array([1.0,3.0,6.0],dtype=float)
        #time_interval_lowres_full = NUM.array([3.0, 6.0],dtype=float)
        #time_interval_lowres = NUM.array([6.0],dtype=float)
        #time_interval_lowres_3Hr = NUM.array([3.0],dtype=float)
		
        time_interval_HRES1 = NUM.array([1.0],dtype=float)            # Line Added/Modified CJB 20190108
        time_interval_HRES13 = NUM.array([1.0,3.0],dtype=float)       # Line Added/Modified CJB 20190108
        time_interval_HRES136 = NUM.array([1.0,3.0,6.0],dtype=float)  # Line Added/Modified CJB 20190108
        time_interval_ENS3 = NUM.array([3.0],dtype=float)             # Line Added/Modified CJB 20190108
        time_interval_ENS36 = NUM.array([3.0,6.0],dtype=float)        # Line Added/Modified CJB 20190108
        time_interval_ENS6 = NUM.array([6.0],dtype=float)             # Line Added/Modified CJB 20190108


        #print "SDR - diff:", diff, time_interval_highres, time_interval_lowres_full, time_interval_lowres
        #if NUM.array_equal(diff, time_interval_highres):
        #    return "HighRes"
        #elif NUM.array_equal(diff, time_interval_lowres_full):
        #    return "LowResFull"
        #elif NUM.array_equal(diff, time_interval_lowres):
        #    return "LowRes"
        #elif NUM.array_equal(diff, time_interval_lowres_3Hr):
        #    return "Low3HrRes"
        #else:
        #    return None
			
        if NUM.array_equal(diff, time_interval_HRES1):     # Line Added/Modified CJB 20190108
            return "HRES1"                                 # Line Added/Modified CJB 20190108
        elif NUM.array_equal(diff, time_interval_HRES13):  # Line Added/Modified CJB 20190108
            return "HRES13"                                # Line Added/Modified CJB 20190108
        elif NUM.array_equal(diff, time_interval_HRES136): # Line Added/Modified CJB 20190108
            return "HRES136"                               # Line Added/Modified CJB 20190108
        elif NUM.array_equal(diff, time_interval_ENS3):    # Line Added/Modified CJB 20190108
           return "ENS3"                                   # Line Added/Modified CJB 20190108
        elif NUM.array_equal(diff, time_interval_ENS36):   # Line Added/Modified CJB 20190108
           return "ENS36"                                  # Line Added/Modified CJB 20190108
        elif NUM.array_equal(diff, time_interval_ENS6):    # Line Added/Modified MJS, CJB 20190108
           return "ENS6"                                   # Line Added/Modified CJB 20190108
        else:                                              # Line Added/Modified CJB 20190108
            return None                                    # Line Added/Modified CJB 20190108
            
    def getGridName(self, in_nc, high_res=False):
        """Return name of grid"""
        if high_res:
            return 'ecmwf_t1279'
        return 'ecmwf_tco639'
        #if high_res:                                   # Line Added/Modified CJB 20190108
            #return 'ecmwf_HRES_F'                      # Line Added/Modified CJB 20190108
        #else:                                          # MJS 20190108
        #return 'ecmwf_ENS_F'                       # Line Added/Modified MJS, CJB 20190108
		
    def getTimeSize(self, in_nc):                      # Line Added/Modified CJB 20190108
        """Return time size"""                         # Line Added/Modified MJS 20190108
        data_in_nc = NET.Dataset(in_nc)                # Line Added/Modified CJB 20190108
        time = data_in_nc.variables['time'][:]         # Line Added/Modified CJB 20190108
        size_time = len(time)                          # Line Added/Modified CJB 20190108
        data_in_nc.close()                                # Line Added/Modified CJB 20190108
        return size_time                               # Line Added/Modified CJB 20190108
		
    def execute(self, in_nc, in_weight_table, out_nc, grid_name, conversion_flag, in_time_interval="6hr"): # modified this line CJB 20190218
                                                                                                           # MJS I might consider netCDF4.Dataset.variables['RO'].units
                                                                                                           # and upstream correction of the cdo grid conversion units attribute.
        """The source code of the tool."""

        # Validate the netcdf dataset
        vars_oi_index = self.dataValidation(in_nc)
        
        """get conversion factor the flag is used to differentiate forecasts converted 
           to netCDF from GRIB and the original netCDF. They both use the same weight tables
           but the original netCDF is in mm whereas the stock GRIB forecasts are in meters.
           Set the conversion_flag in the run.py configuration file.
        """
        if conversion_flag: # Line Added CJB 20190218
            conversion_factor = 1.0 #Line Modified CJB 20190218
        elif grid_name == 'ecmwf_t1279' or grid_name == 'ecmwf_tco639': # Line Modified CJB 20190218
            #if grid_name == 'ecmwf_HRES_F' or grid_name == 'ecmwf_ENS_F': # Line Added/Modified CJB 20190108
            #new grids in mm instead of m
            conversion_factor = 0.001
        else: #set the conversion factor to 1 for everything else (data is in m but legacy installations do not have a flag) Line Added CJB 20190218
            conversion_factor = 1.0 # Line Added CJB 20190218
                                    # MJS I might consider netCDF4.Dataset.variables['RO'].units
                                    # and upstream correction of the cdo grid conversion units attribute.
        # identify if the input netcdf data is the High Resolution data with three different time intervals
        id_data = self.dataIdentify(in_nc)
        if id_data is None:
            raise Exception(self.errorMessages[3])

        ''' Read the netcdf dataset'''
        data_in_nc = NET.Dataset(in_nc)
        time = data_in_nc.variables['time'][:]

        # Check the size of time variable in the netcdf data
        if len(time) == 0:    # *** MJS This change seems like it is too loose an error trap; should it account for instances when nc file time var is != in length with id_data lenght?
            raise Exception(self.errorMessages[3])
            #if len(time) != self.length_time[id_data]:
            #    raise Exception(self.errorMessages[3])

        ''' Read the weight table '''
        print("Reading the weight table...", in_weight_table)
        dict_list = {self.header_wt[0]:[], self.header_wt[1]:[], self.header_wt[2]:[],
                     self.header_wt[3]:[], self.header_wt[4]:[]}

        with open(in_weight_table, "r") as csvfile:
            reader = csv.reader(csvfile)
            count = 0
            for row in reader:
                if count == 0:
                    #check number of columns in the weight table
                    if len(row) < len(self.header_wt):
                        raise Exception(self.errorMessages[4])
                    #check header
                    if row[1:len(self.header_wt)] != self.header_wt[1:]:
                        raise Exception(self.errorMessages[5])
                    count += 1
                else:
                    for i in range(len(self.header_wt)):
                       dict_list[self.header_wt[i]].append(row[i])
                    count += 1

        ''' Calculate water inflows
            as a reminder, the first 91 time steps are T=0 to T=90 and are 1-hourly for HRES
		    the next 18 time steps for HRES are T=93 to T=144 at 3-hourly
            then the final 16 time steps are T=150 to T=240 at 6-hourly for a total of 125 records
			For ENS, the first 49 time steps are T=0 to T=144 at 3-hourly
			the final 35 time steps are T=150 to T=360 at 6-hourly for a total of 84 records
        '''
			
        print("Calculating water inflows...")
		
        ''' 
        added the next section  CJB 20180122 
        '''

		# Get the overall number of time steps
        size_time = self.getTimeSize(in_nc) #CJB 20180122
        # Determine the size of time steps in each group (1-hourly, 3-hourly, and/or 6-hourly)
        if id_data == "HRES1": # T <= 90 
            time_size = (size_time - 1)
        elif id_data == "HRES13": # 93 <= T <= 144
            if in_time_interval == "1hr":
                time_size = self.length_time_opt["HighRes-1hr"]
            else:
                time_size = (size_time - self.length_time_opt["HighRes-1hr"] - 1)
        elif id_data == "HRES136": # 150 <= T <= 240
            if in_time_interval == "1hr":
                time_size = self.length_time_opt["HighRes-1hr"]
            elif in_time_interval == "3hr": # MJS Doesn't seem to be a case used currently, but added just in case later need.
                time_size = self.length_time_opt["HighRes-3hr-sub"] # MJS This is HRES136, i.e., if for some reason in ecmwf_rapid_multi a 3 hr is asked for for this case, it should still have the 3hr_sub number of times
            elif in_time_interval == "3hr_subset":
                time_size = self.length_time_opt["HighRes-3hr-Sub"]
            else:
                time_size = (size_time - self.length_time_opt["HighRes-1hr"] - self.length_time_opt["HighRes-3hr-Sub"] - 1)
        elif id_data == "ENS3": # T <= 144
            time_size = (size_time - 1)
        elif id_data == "ENS36": # 150 <= T <= 360
            if in_time_interval == "3hr_subset":
                time_size = self.length_time_opt["LowResFull-3hr-Sub"]
            else:
                time_size = (size_time - self.length_time_opt["LowResFull-3hr-Sub"] - 1)
        else: # id_data == "ENS6": # T <= 360 but all 6-hourly
            time_size = (size_time - 1)
        #else: # something is wrong and need to throw an error message - likely a corrupt forecast file
        #    raise Exception(self.errorMessages[3])
        #''' end of added section CJB 20180122 
        #'''

        #if id_data == "LowRes":
        #            size_time = self.length_time_opt["LowRes-6hr"]
        #elif id_data == "Low3HrRes":
        #    size_time = self.length_time_opt["LowRes-3hr"]
        #elif id_data == "LowResFull":
        #    if in_time_interval == "3hr_subset":
        #        size_time = self.length_time_opt["LowResFull-3hr-Sub"]
        #    elif in_time_interval == "6hr_subset":
        #        size_time = self.length_time_opt["LowResFull-6hr-Sub"]
        #    else:
        #        size_time = self.length_time_opt["LowRes-6hr"]
        #else: #HighRes
        #    if in_time_interval == "1hr":
        #        size_time = self.length_time_opt["HighRes-1hr"]
        #    elif in_time_interval == "3hr":
        #        size_time = self.length_time_opt["HighRes-3hr"]
        #    elif in_time_interval == "3hr_subset":
        #        size_time = self.length_time_opt["HighRes-3hr-Sub"]
        #    elif in_time_interval == "6hr_subset":
        #        size_time = self.length_time_opt["HighRes-6hr-Sub"]
        #    else:
        #        size_time = self.length_time_opt["HighRes-6hr"]

        size_streamID = len(set(dict_list[self.header_wt[0]]))

        # Create output inflow netcdf data
        # data_out_nc = NET.Dataset(out_nc, "w") # by default format = "NETCDF4"
        data_out_nc = NET.Dataset(out_nc, "w", format = "NETCDF3_CLASSIC")
        #dim_Time = data_out_nc.createDimension('Time', size_time)
        dim_Time = data_out_nc.createDimension('Time', time_size)
        dim_RiverID = data_out_nc.createDimension('rivid', size_streamID)
        var_m3_riv = data_out_nc.createVariable('m3_riv', 'f4', 
                                                ('Time', 'rivid'),
                                                fill_value=0)
                                                
        #data_temp = NUM.empty(shape = [size_time, size_streamID])
        data_temp = NUM.empty(shape = [time_size, size_streamID])

        lon_ind_all = [int(i) for i in dict_list[self.header_wt[2]]]
        lat_ind_all = [int(j) for j in dict_list[self.header_wt[3]]]

        # Obtain a subset of  runoff data based on the indices in the weight table
        min_lon_ind_all = min(lon_ind_all)
        max_lon_ind_all = max(lon_ind_all)
        min_lat_ind_all = min(lat_ind_all)
        max_lat_ind_all = max(lat_ind_all)

        # self.vars_oi[vars_oi_index][3] = RO; get that variable's 3D structure (time, lat_index, lon_index) ready to reshape into 2D (time, lat_index x lon_index)
        data_subset_all = data_in_nc.variables[self.vars_oi[vars_oi_index][3]][:, min_lat_ind_all:max_lat_ind_all+1, min_lon_ind_all:max_lon_ind_all+1]
        len_time_subset_all = data_subset_all.shape[0]
        len_lat_subset_all = data_subset_all.shape[1]
        len_lon_subset_all = data_subset_all.shape[2]
        data_subset_all = data_subset_all.reshape(len_time_subset_all, (len_lat_subset_all * len_lon_subset_all))

        # compute new indices based on the data_subset_all
        index_new = []
        for r in range(0,count-1):
            ind_lat_orig = lat_ind_all[r]
            ind_lon_orig = lon_ind_all[r]
            index_new.append((ind_lat_orig - min_lat_ind_all)*len_lon_subset_all + (ind_lon_orig - min_lon_ind_all))

        # obtain a new subset of data
        data_subset_new = data_subset_all[:,index_new]*conversion_factor

        # start compute inflow
        pointer = 0
        for s in range(0, size_streamID):
            npoints = int(dict_list[self.header_wt[4]][pointer])
            # Check if all npoints points correspond to the same streamID
            if len(set(dict_list[self.header_wt[0]][pointer : (pointer + npoints)])) != 1:
                print("ROW INDEX {0}".format(pointer))
                print("RIVID {0}".format(dict_list[self.header_wt[0]][pointer]))
                raise Exception(self.errorMessages[2])

            area_sqm_npoints = [float(k) for k in dict_list[self.header_wt[1]][pointer : (pointer + npoints)]]
            area_sqm_npoints = NUM.array(area_sqm_npoints)
            area_sqm_npoints = area_sqm_npoints.reshape(1, npoints)
            data_goal = data_subset_new[:, pointer:(pointer + npoints)]
            
            
            #remove noise from data
            data_goal[data_goal<=0.00001] = 0

            ''' IMPORTANT NOTE: runoff variable in ECMWF dataset is cumulative instead of incremental through time
            '''
            # For data with Low Resolution, there's only one time interval 6 hrs
            if id_data == "ENS6": # Line Added/Modified CJB 20190108
                #ro_stream = data_goal * area_sqm_npoints
                ro_stream = NUM.subtract(data_goal[1:,],data_goal[:-1,]) * area_sqm_npoints
            elif id_data == "ENS3": # there's only one time interval 3 hrs  # Line Added/Modified CJB 20190108
                #ro_stream = data_goal * area_sqm_npoints
                ro_stream = NUM.subtract(data_goal[1:,],data_goal[:-1,]) * area_sqm_npoints # Line Added/Modified CJB 20190108
            elif id_data == "HRES1": # there's only one time interval 1 hrs # Line Added/Modified CJB 20190108
                #ro_stream = data_goal * area_sqm_npoints
                ro_stream = NUM.subtract(data_goal[1:,],data_goal[:-1,]) * area_sqm_npoints # Line Added/Modified CJB 20190108	
                #For data with the full version of Low Resolution, from Hour 0 to 144 (the first 49 time points) are of 3 hr time interval,
                # then from Hour 144 to 360 (36 time points) are of 6 hour time interval
            elif id_data == "ENS36": # Line Added/Modified CJB 20190108
                if in_time_interval == "3hr_subset":
                    #use only the 3hr time interval
                    ro_stream = NUM.subtract(data_goal[1:49,], data_goal[:48,]) * area_sqm_npoints
                elif in_time_interval == "6hr_subset":
                    #use only the 6hr time interval
                    ro_stream = NUM.subtract(data_goal[49:,], data_goal[48:-1,]) * area_sqm_npoints
                else: #"LowRes-6hr"
                ######################################################
                # MJS Always assume this case will have a full ECMWF 240
                # hour forecast to work with.  It's actually never re-
                # quested by ecmwf_rapid_multiprocess anyhow.
                ######################################################
                    #convert all to 6hr
                    # calculate time series of 6 hr data from 3 hr data
                    ro_6hr_a = NUM.subtract(data_goal[2:49:2,], data_goal[:48:2,])
                    # get the time series of 6 hr data
                    ro_6hr_b = NUM.subtract(data_goal[49:,], data_goal[48:-1,])
                    # concatenate all time series
                    ro_stream = NUM.concatenate([ro_6hr_a, ro_6hr_b]) * area_sqm_npoints
            #For data with High Resolution, from Hour 0 to 90 (the first 91 time points) are of 1 hr time interval,
            # then from Hour 90 to 144 (18 time points) are of 3 hour time interval, and from Hour 144 to 240 (16 time points)
            # are of 6 hour time interval
            ##########################################################
            # MJS The following should handle id_data = HRES13 and HRES136
            ##########################################################
            else:
                if in_time_interval == "1hr":
                    #ro_stream = NUM.subtract(data_goal[1:91,],data_goal[:90,]) * area_sqm_npoints
                    ro_stream = NUM.subtract(data_goal[1:1+time_size,],data_goal[:time_size,]) * area_sqm_npoints # Line Added/Modified CJB, MJS 20190108
                elif in_time_interval == "3hr": # MJS HRES 3hr not currently used
                    # calculate time series of 3 hr data from 1 hr data
                    ro_3hr_a = NUM.subtract(data_goal[3:91:3,],data_goal[:88:3,])
                    # get the time series of 3 hr data
                    #ro_3hr_b = NUM.subtract(data_goal[91:109,], data_goal[90:108,])
                    ro_3hr_b = NUM.subtract(data_goal[91:91+time_size,], data_goal[90:90+time_size,]) # MJS modified again; seems no case for this, but just in case later... Line Added/Modified CJB 20190108
                    # concatenate all time series
                    ro_stream = NUM.concatenate([ro_3hr_a, ro_3hr_b]) * area_sqm_npoints
                elif in_time_interval == "3hr_subset":
                    #use only the 3hr time interval
                    #ro_stream = NUM.subtract(data_goal[91:109,], data_goal[90:108,]) * area_sqm_npoints
                    ro_stream = NUM.subtract(data_goal[91:91+time_size,], data_goal[90:90+time_size,]) * area_sqm_npoints # MJS modified again; needs to handle HRES13 that might not have complete 3hr set... Line Added/Modified CJB 20190108
                elif in_time_interval == "6hr_subset":
                    #use only the 6hr time interval
                    ro_stream = NUM.subtract(data_goal[109:,], data_goal[108:-1,]) * area_sqm_npoints
                ######################################################
                # MJS Always assume this case will have a full ECMWF 240 
                # hour forecast to work with.  It's actually never re-
                # quested by ecmwf_rapid_multiprocess anyhow.
                ######################################################
                else: # in_time_interval == "6hr"
                    #arcpy.AddMessage("6hr")
                    # calculate time series of 6 hr data from 1 hr data
                    ro_6hr_a = NUM.subtract(data_goal[6:91:6,], data_goal[:85:6,])
                    # calculate time series of 6 hr data from 3 hr data
                    ro_6hr_b = NUM.subtract(data_goal[92:109:2,], data_goal[90:107:2,])
                    # get the time series of 6 hr data
                    ro_6hr_c = NUM.subtract(data_goal[109:,], data_goal[108:-1,])
                    # concatenate all time series
                    ro_stream = NUM.concatenate([ro_6hr_a, ro_6hr_b, ro_6hr_c]) * area_sqm_npoints
                    
            #remove negative values
            ro_stream[ro_stream<0] = 0
            data_temp[:,s] = ro_stream.sum(axis = 1)

            pointer += npoints


        '''Write inflow data'''
        print("Writing inflow data...")
        var_m3_riv[:] = data_temp
        # close the input and output netcdf datasets
        data_in_nc.close()
        data_out_nc.close()
