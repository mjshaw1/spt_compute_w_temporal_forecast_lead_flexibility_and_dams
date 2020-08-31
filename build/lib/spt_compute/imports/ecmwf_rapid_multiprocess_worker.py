# -*- coding: utf-8 -*-
##
##  ecmwf_rapid_multiprocess_worker.py
##  spt_compute
##
##  Created by Alan D. Snow.
##  Copyright © 2015-2017 Alan D Snow. All rights reserved.
##  License: BSD 3-Clause
"""
              Version 1.3, 01/09/2019, revised durations to use a variable temporal
			    range. CJB ENSCO, MJS ERDC CRREL
"""

import datetime
import os
from RAPIDpy import RAPID
from RAPIDpy.postprocess import ConvertRAPIDOutputToCF
from shutil import move, rmtree
import traceback

#local imports
from .CreateInflowFileFromECMWFRunoff import CreateInflowFileFromECMWFRunoff
from .helper_functions import (case_insensitive_file_search,
                               get_ensemble_number_from_forecast,
                               CaptureStdOutToLog)
import tarfile

# ----------------------------------------------------------------------------------------
# HELPER FUNCTIONS
# ----------------------------------------------------------------------------------------
def upload_single_forecast(job_info, data_manager):
    """
    Uploads a single forecast file to CKAN
    """
    print("Uploading {0} {1} {2} {3}".format(job_info['watershed'],
                                             job_info['subbasin'],
                                             job_info['forecast_date_timestep'],
                                             job_info['ensemble_number']))

    # Upload to CKAN
    data_manager.initialize_run_ecmwf(job_info['watershed'], job_info['subbasin'], job_info['forecast_date_timestep'])
    data_manager.update_resource_ensemble_number(job_info['ensemble_number'])
    # upload file
    try:
        # tar.gz file
        output_tar_file = os.path.join(job_info['master_watershed_outflow_directory'],
                                       "%s.tar.gz" % data_manager.resource_name)
        if not os.path.exists(output_tar_file):
            with tarfile.open(output_tar_file, "w:gz") as tar:
                tar.add(job_info['outflow_file_name'], arcname=os.path.basename(job_info['outflow_file_name']))
        return_data = data_manager.upload_resource(output_tar_file)
        if not return_data['success']:
            print(return_data)
            print("Attempting to upload again")
            return_data = data_manager.upload_resource(output_tar_file)
            if not return_data['success']:
                print(return_data)
            else:
                print("Upload success")
        else:
            print("Upload success")
    except Exception as ex:
        print(ex)
        pass
        # remove tar.gz file
    os.remove(output_tar_file)

                              
#------------------------------------------------------------------------------
#functions
#------------------------------------------------------------------------------
def ecmwf_rapid_multiprocess_worker(node_path, rapid_input_directory,
                                    ecmwf_forecast, forecast_date_timestep, 
                                    watershed, subbasin, rapid_executable_location, 
                                    init_flow, initialization_time_step, conversion_flag, # modified this line CJB 20190218
                                                                                           # MJS I might consider a netCDF4.Dataset.variables['RO'].units check in SPT,
                                                                                           # and a correction of units attributes in the upstream cdo processing.
                                                                                           #
# dam arguments added, MJS 8/23/2020
                                    BS_opt_dam,IS_dam_tot,IS_dam_use,
                                    dam_tot_id_file,dam_use_id_file,dam_file):
    """
    Multiprocess worker function
    """
    print("In ecmwf_rapid_multiprocess_worker")

    time_start_all = datetime.datetime.utcnow()

    os.chdir(node_path)

    ensemble_number = get_ensemble_number_from_forecast(ecmwf_forecast)

    def remove_file(file_name):
        """
        remove file
        """
        try:
            os.remove(file_name)
        except OSError:
            pass

    #prepare ECMWF file for RAPID
    print("INFO: Running all ECMWF downscaling for watershed: {0}-{1} {2} {3}"
          .format(watershed,
                  subbasin,
                  forecast_date_timestep,
                  ensemble_number))

    #set up RAPID manager
    rapid_connect_file=case_insensitive_file_search(rapid_input_directory,
                                                    r'rapid_connect\.csv')

    rapid_manager = RAPID(
        rapid_executable_location=rapid_executable_location,
        rapid_connect_file=rapid_connect_file,
        riv_bas_id_file=case_insensitive_file_search(rapid_input_directory,
                                                     r'riv_bas_id.*?\.csv'),
        k_file=case_insensitive_file_search(rapid_input_directory,
                                            r'k\.csv'),
        x_file=case_insensitive_file_search(rapid_input_directory,
                                            r'x\.csv'),
        ZS_dtM=3*60*60, #RAPID internal loop time interval
    )

    # check for forcing flows
    try:
        rapid_manager.update_parameters(
            Qfor_file=case_insensitive_file_search(rapid_input_directory,
                                                   r'qfor\.csv'),
            for_tot_id_file=case_insensitive_file_search(rapid_input_directory,
                                                         r'for_tot_id\.csv'),
            for_use_id_file=case_insensitive_file_search(rapid_input_directory,
                                                         r'for_use_id\.csv'),
            ZS_dtF=3*60*60, # forcing time interval
            BS_opt_for=True
        )
    except Exception:
        print('WARNING: Forcing files not found. Skipping forcing ...')
        pass


    rapid_manager.update_reach_number_data()

    outflow_file_name = os.path.join(node_path,
                                     'Qout_%s_%s_%s.nc' % (watershed.lower(), 
                                                           subbasin.lower(), 
                                                           ensemble_number))

    qinit_file = ""
    BS_opt_Qinit = False
    print(init_flow)
    if(init_flow):
        #check for qinit file
        past_date = (datetime.datetime.strptime(forecast_date_timestep[:11],"%Y%m%d.%H") - \
#                     datetime.timedelta(hours=initialization_time_step)).strftime("%Y%m%dt%H")
                     datetime.timedelta(hours=12)).strftime("%Y%m%dt%H")
        print("Past date:" ,past_date)
        qinit_file = os.path.join(rapid_input_directory, 'Qinit_%s.csv' % past_date)
        BS_opt_Qinit = qinit_file and os.path.exists(qinit_file)
        if not BS_opt_Qinit:
            print("Error: {0} not found. Not initializing ...".format(qinit_file))
            qinit_file = ""

            
    try:
        comid_lat_lon_z_file = case_insensitive_file_search(rapid_input_directory,
                                                            r'comid_lat_lon_z.*?\.csv')
    except Exception:
        comid_lat_lon_z_file = ""
        print("WARNING: comid_lat_lon_z_file not found. Not adding lat/lon/z to output file ...")

    RAPIDinflowECMWF_tool = CreateInflowFileFromECMWFRunoff()
    forecast_resolution = RAPIDinflowECMWF_tool.dataIdentify(ecmwf_forecast)
    time_step_count = RAPIDinflowECMWF_tool.getTimeSize(ecmwf_forecast) #new line 20190108 CJB
    print(forecast_resolution)
    """ 
    NOTE: time_step_count is the total count of forecast steps, not periods 
    """ # 20190108 CJB
    #determine weight table from resolution
    if forecast_resolution == "HRES136": #new line 20190108 CJB 150 <= T <= 240
        #if forecast_resolution == "HighRes":
        #HIGH RES
        grid_name = RAPIDinflowECMWF_tool.getGridName(ecmwf_forecast, high_res=True)
        #generate inflows for each timestep
        weight_table_file = case_insensitive_file_search(rapid_input_directory,
                                                         r'weight_{0}\.csv'.format(grid_name))

        inflow_file_name_1hr = os.path.join(node_path, 'm3_riv_bas_1hr_%s.nc' % ensemble_number)
        inflow_file_name_3hr = os.path.join(node_path, 'm3_riv_bas_3hr_%s.nc' % ensemble_number)
        inflow_file_name_6hr = os.path.join(node_path, 'm3_riv_bas_6hr_%s.nc' % ensemble_number)
        qinit_3hr_file = os.path.join(node_path, 'Qinit_3hr.csv')
        qinit_6hr_file = os.path.join(node_path, 'Qinit_6hr.csv')
        try:

            RAPIDinflowECMWF_tool.execute(ecmwf_forecast, 
                                          weight_table_file, 
                                          inflow_file_name_1hr,
                                          grid_name,
                                          conversion_flag, # added this line CJB 20190218
                                          "1hr")

            #from Hour 0 to 90 (the first 91 time points) are of 1 hr time interval
            periods_1hr = 90 #new line 20190108 CJB 
            interval_1hr = 1*60*60 #1hr
            duration_1hr = periods_1hr*interval_1hr #90hrs # modified line 20190108 CJB, MJS
            rapid_manager.update_parameters(ZS_TauR=interval_1hr, #duration of routing procedure (time step of runoff data)
                                            ZS_dtR=15*60, #internal routing time step
                                            ZS_TauM=duration_1hr, #total simulation time
                                            ZS_dtM=interval_1hr, #RAPID internal loop time interval
                                            ZS_dtF=interval_1hr, # forcing time interval
                                            Vlat_file=inflow_file_name_1hr,
                                            Qout_file=outflow_file_name,
                                            Qinit_file=qinit_file,
                                            BS_opt_Qinit=BS_opt_Qinit,
                                            BS_opt_dam = BS_opt_dam,
                                            IS_dam_tot = IS_dam_tot,
                                            IS_dam_use = IS_dam_use,
                                            dam_tot_id_file = dam_tot_id_file,
                                            dam_use_id_file = dam_use_id_file,
                                            dam_file = dam_file)
            rapid_manager.run()
            #generate Qinit from 1hr
            rapid_manager.generate_qinit_from_past_qout(qinit_3hr_file)

            #then from Hour 90 to 144 (19 time points) are of 3 hour time interval
            RAPIDinflowECMWF_tool.execute(ecmwf_forecast, 
                                          weight_table_file, 
                                          inflow_file_name_3hr,
                                          grid_name,
                                          conversion_flag, # added this line CJB 20190218
                                          "3hr_subset")

            periods_3hr = 18 #new line 20190108 CJB
            interval_3hr = 3*60*60 #3hr
            duration_3hr = periods_3hr*interval_3hr #54hrs # modified line 20190108 CJB, MJS
            qout_3hr = os.path.join(node_path,'Qout_3hr.nc')
            rapid_manager.update_parameters(ZS_TauR=interval_3hr, #duration of routing procedure (time step of runoff data)
                                            ZS_dtR=15*60, #internal routing time step
                                            ZS_TauM=duration_3hr, #total simulation time 
                                            ZS_dtM=interval_3hr, #RAPID internal loop time interval
                                            ZS_dtF=interval_3hr,  # forcing time interval
                                            Vlat_file=inflow_file_name_3hr,
                                            Qout_file=qout_3hr,
                                            BS_opt_dam = BS_opt_dam, 
                                            IS_dam_tot = IS_dam_tot,
                                            IS_dam_use = IS_dam_use,
                                            dam_tot_id_file = dam_tot_id_file,
                                            dam_use_id_file = dam_use_id_file,
                                            dam_file = dam_file)
            rapid_manager.run()

            #generate Qinit from 3hr
            rapid_manager.generate_qinit_from_past_qout(qinit_6hr_file)

            #from Hour 144 to 240 (15 time points) are of 6 hour time interval
            RAPIDinflowECMWF_tool.execute(ecmwf_forecast, 
                                          weight_table_file, 
                                          inflow_file_name_6hr,
                                          grid_name,
                                          conversion_flag, # added this line CJB 20190218
                                          "6hr_subset")

            periods_6hr = (time_step_count -1) - periods_3hr - periods_1hr #new line 20190108 CJB, MJS
            interval_6hr = 6*60*60 #6hr
            duration_6hr = periods_6hr*interval_6hr #new line 20190108 CJB, MJS 
            qout_6hr = os.path.join(node_path,'Qout_6hr.nc')
            rapid_manager.update_parameters(ZS_TauR=interval_6hr, #duration of routing procedure (time step of runoff data)
                                            ZS_dtR=15*60, #internal routing time step
                                            ZS_TauM=duration_6hr, #total simulation time 
                                            ZS_dtM=interval_6hr, #RAPID internal loop time interval
                                            ZS_dtF=interval_6hr,  # forcing time interval
                                            Vlat_file=inflow_file_name_6hr,
                                            Qout_file=qout_6hr,
                                            BS_opt_dam = BS_opt_dam, 
                                            IS_dam_tot = IS_dam_tot,
                                            IS_dam_use = IS_dam_use,
                                            dam_tot_id_file = dam_tot_id_file,
                                            dam_use_id_file = dam_use_id_file,
                                            dam_file = dam_file)
            rapid_manager.run()

            #Merge all files together at the end
            cv = ConvertRAPIDOutputToCF(rapid_output_file=[outflow_file_name, qout_3hr, qout_6hr],
                                        start_datetime=datetime.datetime.strptime(forecast_date_timestep[:11], "%Y%m%d.%H"),
                                        time_step=[interval_1hr, interval_3hr, interval_6hr],
                                        qinit_file=qinit_file,
                                        comid_lat_lon_z_file=comid_lat_lon_z_file,
                                        rapid_connect_file=rapid_connect_file,
                                        project_name="ECMWF-RAPID Predicted flows by US Army ERDC",
                                        output_id_dim_name='rivid',
                                        output_flow_var_name='Qout',
                                        print_debug=False)

            cv.convert()

        except Exception:
            remove_file(qinit_3hr_file)
            remove_file(qinit_6hr_file)
            remove_file(inflow_file_name_1hr)
            remove_file(inflow_file_name_3hr)
            remove_file(inflow_file_name_6hr)
            traceback.print_exc()
            raise

        remove_file(qinit_3hr_file)
        remove_file(qinit_6hr_file)
        remove_file(inflow_file_name_1hr)
        remove_file(inflow_file_name_3hr)
        remove_file(inflow_file_name_6hr)

    if forecast_resolution == "HRES13": #new line 20190108 CJB 93 <= T <= 144
        #if forecast_resolution == "HighRes":
	    #HIGH RES
        grid_name = RAPIDinflowECMWF_tool.getGridName(ecmwf_forecast, high_res=True)
        #generate inflows for each timestep
        weight_table_file = case_insensitive_file_search(rapid_input_directory,
                                                         r'weight_{0}\.csv'.format(grid_name))

        inflow_file_name_1hr = os.path.join(node_path, 'm3_riv_bas_1hr_%s.nc' % ensemble_number)
        inflow_file_name_3hr = os.path.join(node_path, 'm3_riv_bas_3hr_%s.nc' % ensemble_number)
        #inflow_file_name_6hr = os.path.join(node_path, 'm3_riv_bas_6hr_%s.nc' % ensemble_number)
        qinit_3hr_file = os.path.join(node_path, 'Qinit_3hr.csv')
        #qinit_6hr_file = os.path.join(node_path, 'Qinit_6hr.csv')
        
        
        try:
        
            RAPIDinflowECMWF_tool.execute(ecmwf_forecast, 
                                          weight_table_file, 
                                          inflow_file_name_1hr,
                                          grid_name,
                                          conversion_flag, # added this line CJB 20190218
                                          "1hr")

            #from Hour 0 to 90 (the first 91 time points) are of 1 hr time interval
            periods_1hr = 90 #new line 20190108 CJB
            interval_1hr = 1*60*60 #1hr
            duration_1hr = periods_1hr*60*60 #90hrs # modified line 20190108 CJB
            rapid_manager.update_parameters(ZS_TauR=interval_1hr, #duration of routing procedure (time step of runoff data)
                                            ZS_dtR=15*60, #internal routing time step
                                            ZS_TauM=duration_1hr, #total simulation time
                                            ZS_dtM=interval_1hr, #RAPID internal loop time interval
                                            ZS_dtF=interval_1hr, # forcing time interval
                                            Vlat_file=inflow_file_name_1hr,
                                            Qout_file=outflow_file_name,
                                            Qinit_file=qinit_file,
                                            BS_opt_Qinit=BS_opt_Qinit,
                                            BS_opt_dam = BS_opt_dam, 
                                            IS_dam_tot = IS_dam_tot,
                                            IS_dam_use = IS_dam_use,
                                            dam_tot_id_file = dam_tot_id_file,
                                            dam_use_id_file = dam_use_id_file,
                                            dam_file = dam_file)
            rapid_manager.run()

            #generate Qinit from 1hr
            rapid_manager.generate_qinit_from_past_qout(qinit_3hr_file)

            #then from Hour 90 to 144 (19 time points) are of 3 hour time interval
            RAPIDinflowECMWF_tool.execute(ecmwf_forecast, 
                                          weight_table_file, 
                                          inflow_file_name_3hr,
                                          grid_name,
                                          conversion_flag, # added this line CJB 20190218
                                          "3hr_subset")

            periods_3hr = (time_step_count - 1) - periods_1hr #new line 20190108 CJB, MJS
            interval_3hr = 3*60*60 #3hr
            duration_3hr = periods_3hr*interval_3hr #new line 20190108 CJB, MJS
            qout_3hr = os.path.join(node_path,'Qout_3hr.nc')
            rapid_manager.update_parameters(ZS_TauR=interval_3hr, #duration of routing procedure (time step of runoff data)
                                            ZS_dtR=15*60, #internal routing time step
                                            ZS_TauM=duration_3hr, #total simulation time 
                                            ZS_dtM=interval_3hr, #RAPID internal loop time interval
                                            ZS_dtF=interval_3hr,  # forcing time interval
                                            Vlat_file=inflow_file_name_3hr,
                                            Qout_file=qout_3hr,
                                            BS_opt_dam = BS_opt_dam, 
                                            IS_dam_tot = IS_dam_tot,
                                            IS_dam_use = IS_dam_use,
                                            dam_tot_id_file = dam_tot_id_file,
                                            dam_use_id_file = dam_use_id_file,
                                            dam_file = dam_file)
            rapid_manager.run()

            #Merge all files together at the end
            cv = ConvertRAPIDOutputToCF(rapid_output_file=[outflow_file_name, qout_3hr], 
                                        start_datetime=datetime.datetime.strptime(forecast_date_timestep[:11], "%Y%m%d.%H"), 
                                        time_step=[interval_1hr, interval_3hr], 
                                        qinit_file=qinit_file, 
                                        comid_lat_lon_z_file=comid_lat_lon_z_file,
                                        rapid_connect_file=rapid_connect_file, 
                                        project_name="ECMWF-RAPID Predicted flows by US Army ERDC", 
                                        output_id_dim_name='rivid',
                                        output_flow_var_name='Qout',
                                        print_debug=False)
            cv.convert()

        except Exception:
            remove_file(qinit_3hr_file)
            remove_file(inflow_file_name_1hr)
            remove_file(inflow_file_name_3hr)
            traceback.print_exc()
            raise

        remove_file(qinit_3hr_file)
        remove_file(inflow_file_name_1hr)
        remove_file(inflow_file_name_3hr)

    if forecast_resolution == "HRES1": #new line 20190108 CJB 0 <= T <= 90
        #if forecast_resolution == "HighRes":
	    #HIGH RES
        grid_name = RAPIDinflowECMWF_tool.getGridName(ecmwf_forecast, high_res=True)
        #generate inflows for each timestep
        weight_table_file = case_insensitive_file_search(rapid_input_directory,
                                                         r'weight_{0}\.csv'.format(grid_name))

        inflow_file_name_1hr = os.path.join(node_path, 'm3_riv_bas_1hr_%s.nc' % ensemble_number)
        #inflow_file_name_3hr = os.path.join(node_path, 'm3_riv_bas_3hr_%s.nc' % ensemble_number)
        #inflow_file_name_6hr = os.path.join(node_path, 'm3_riv_bas_6hr_%s.nc' % ensemble_number)
        #qinit_3hr_file = os.path.join(node_path, 'Qinit_3hr.csv')
        #qinit_6hr_file = os.path.join(node_path, 'Qinit_6hr.csv')

        try:

            RAPIDinflowECMWF_tool.execute(ecmwf_forecast, 
                                          weight_table_file, 
                                          inflow_file_name_1hr,
                                          grid_name,
                                          conversion_flag, # added this line CJB 20190218
                                          "1hr")

            #from Hour 0 to 90 (the first 91 time points) are of 1 hr time interval
            interval_1hr = 1*60*60 #1hr
            duration_1hr = (time_step_count - 1)*interval_1hr #new line 20190108 CJB, MJS
            rapid_manager.update_parameters(ZS_TauR=interval_1hr, #duration of routing procedure (time step of runoff data)
                                            ZS_dtR=15*60, #internal routing time step
                                            ZS_TauM=duration_1hr, #total simulation time
                                            ZS_dtM=interval_1hr, #RAPID internal loop time interval
                                            ZS_dtF=interval_1hr, # forcing time interval
                                            Vlat_file=inflow_file_name_1hr,
                                            Qout_file=outflow_file_name,
                                            Qinit_file=qinit_file,
                                            BS_opt_Qinit=BS_opt_Qinit,
                                            BS_opt_dam = BS_opt_dam, 
                                            IS_dam_tot = IS_dam_tot,
                                            IS_dam_use = IS_dam_use,
                                            dam_tot_id_file = dam_tot_id_file,
                                            dam_use_id_file = dam_use_id_file,
                                            dam_file = dam_file)

            rapid_manager.run()

            #Merge all files together at the end
            cv = ConvertRAPIDOutputToCF(rapid_output_file=[outflow_file_name], 
                                        start_datetime=datetime.datetime.strptime(forecast_date_timestep[:11], "%Y%m%d.%H"), 
                                        time_step=[interval_1hr], 
                                        qinit_file=qinit_file, 
                                        comid_lat_lon_z_file=comid_lat_lon_z_file,
                                        rapid_connect_file=rapid_connect_file, 
                                        project_name="ECMWF-RAPID Predicted flows by US Army ERDC", 
                                        output_id_dim_name='rivid',
                                        output_flow_var_name='Qout',
                                        print_debug=False)

            cv.convert()

        except Exception:
            remove_file(inflow_file_name_1hr)
            traceback.print_exc()
            raise

        remove_file(inflow_file_name_1hr)

    elif forecast_resolution == "ENS36": #new line 20190108 CJB 150 <= T <= 360
        #elif forecast_resolution == "LowResFull":
        #LOW RES - 3hr and 6hr timesteps
        grid_name = RAPIDinflowECMWF_tool.getGridName(ecmwf_forecast, high_res=False)
        #generate inflows for each timestep
        weight_table_file = case_insensitive_file_search(rapid_input_directory,
                                                         r'weight_{0}\.csv'.format(grid_name))

        inflow_file_name_3hr = os.path.join(node_path, 'm3_riv_bas_3hr_%s.nc' % ensemble_number)
        inflow_file_name_6hr = os.path.join(node_path, 'm3_riv_bas_6hr_%s.nc' % ensemble_number)
        qinit_6hr_file = os.path.join(node_path, 'Qinit_6hr.csv')

        try:

            RAPIDinflowECMWF_tool.execute(ecmwf_forecast, 
                                          weight_table_file, 
                                          inflow_file_name_3hr,
                                          grid_name,
                                          conversion_flag, # added this line CJB 20190218
                                          "3hr_subset")

            #from Hour 0 to 144 (the first 49 time points) are of 3 hr time interval
            periods_3hr = 48
            interval_3hr = 3*60*60 #3hr
            duration_3hr = periods_3hr*interval_3hr #new line 20190108 CJB, MJS
            rapid_manager.update_parameters(ZS_TauR=interval_3hr, #duration of routing procedure (time step of runoff data)
                                            ZS_dtR=15*60, #internal routing time step
                                            ZS_TauM=duration_3hr, #total simulation time 
                                            ZS_dtM=interval_3hr, #RAPID internal loop time interval
                                            ZS_dtF=interval_3hr,  # forcing time interval
                                            Vlat_file=inflow_file_name_3hr,
                                            Qout_file=outflow_file_name,
                                            Qinit_file=qinit_file,
                                            BS_opt_Qinit=BS_opt_Qinit,
                                            BS_opt_dam = BS_opt_dam, 
                                            IS_dam_tot = IS_dam_tot,
                                            IS_dam_use = IS_dam_use,
                                            dam_tot_id_file = dam_tot_id_file,
                                            dam_use_id_file = dam_use_id_file,
                                            dam_file = dam_file)
            rapid_manager.run()

            #generate Qinit from 3hr
            rapid_manager.generate_qinit_from_past_qout(qinit_6hr_file)

            #from Hour 144 to 360 (36 time points) are of 6 hour time interval
            RAPIDinflowECMWF_tool.execute(ecmwf_forecast, 
                                          weight_table_file, 
                                          inflow_file_name_6hr,
                                          grid_name,
                                          conversion_flag, # added this line CJB 20190218
                                          "6hr_subset")

            periods_6hr = (time_step_count - 1) - periods_3hr #new line 20190108 CJB, MJS
            interval_6hr = 6*60*60 #6hr
            duration_6hr = periods_6hr*interval_6hr #new line 20190108 CJB, MJS
            qout_6hr = os.path.join(node_path,'Qout_6hr.nc')
            rapid_manager.update_parameters(ZS_TauR=interval_6hr, #duration of routing procedure (time step of runoff data)
                                            ZS_dtR=15*60, #internal routing time step
                                            ZS_TauM=duration_6hr, #total simulation time 
                                            ZS_dtM=interval_6hr, #RAPID internal loop time interval
                                            ZS_dtF=interval_6hr,  # forcing time interval
                                            Vlat_file=inflow_file_name_6hr,
                                            Qout_file=qout_6hr,
                                            BS_opt_dam = BS_opt_dam, 
                                            IS_dam_tot = IS_dam_tot,
                                            IS_dam_use = IS_dam_use,
                                            dam_tot_id_file = dam_tot_id_file,
                                            dam_use_id_file = dam_use_id_file,
                                            dam_file = dam_file)
            rapid_manager.run()

            #Merge all files together at the end
            cv = ConvertRAPIDOutputToCF(rapid_output_file=[outflow_file_name, qout_6hr], 
                                        start_datetime=datetime.datetime.strptime(forecast_date_timestep[:11], "%Y%m%d.%H"), 
                                        time_step=[interval_3hr, interval_6hr], 
                                        qinit_file=qinit_file, 
                                        comid_lat_lon_z_file=comid_lat_lon_z_file,
                                        rapid_connect_file=rapid_connect_file, 
                                        project_name="ECMWF-RAPID Predicted flows by US Army ERDC", 
                                        output_id_dim_name='rivid',
                                        output_flow_var_name='Qout',
                                        print_debug=False)
            cv.convert()

        except Exception:
            remove_file(qinit_6hr_file)
            remove_file(inflow_file_name_3hr)
            remove_file(inflow_file_name_6hr)
            traceback.print_exc()
            raise

        remove_file(qinit_6hr_file)
        remove_file(inflow_file_name_3hr)
        remove_file(inflow_file_name_6hr)

    elif forecast_resolution == "ENS3": #new line 20190108 CJB 0 <= T <= 144
        #elif forecast_resolution == "LowResFull":
        #LOW RES - 3hr and 6hr timesteps
        grid_name = RAPIDinflowECMWF_tool.getGridName(ecmwf_forecast, high_res=False)
        #generate inflows for each timestep
        weight_table_file = case_insensitive_file_search(rapid_input_directory,
                                                         r'weight_{0}\.csv'.format(grid_name))

        inflow_file_name_3hr = os.path.join(node_path, 'm3_riv_bas_3hr_%s.nc' % ensemble_number)
        #inflow_file_name_6hr = os.path.join(node_path, 'm3_riv_bas_6hr_%s.nc' % ensemble_number)
        #qinit_6hr_file = os.path.join(node_path, 'Qinit_6hr.csv')

        try:

            RAPIDinflowECMWF_tool.execute(ecmwf_forecast, 
                                          weight_table_file, 
                                          inflow_file_name_3hr,
                                          grid_name,
                                          conversion_flag, # added this line CJB 20190218
                                          "3hr_subset")

            #from Hour 0 to 144 (the first 49 time points) are of 3 hr time interval
            periods_3hr = time_step_count - 1 #new line 20190108 CJB, MJS
            interval_3hr = 3*60*60 #3hr
            duration_3hr = periods_3hr*interval_3hr #new line 20190108 CJB, MJS
            rapid_manager.update_parameters(ZS_TauR=interval_3hr, #duration of routing procedure (time step of runoff data)
                                            ZS_dtR=15*60, #internal routing time step
                                            ZS_TauM=duration_3hr, #total simulation time 
                                            ZS_dtM=interval_3hr, #RAPID internal loop time interval
                                            ZS_dtF=interval_3hr,  # forcing time interval
                                            Vlat_file=inflow_file_name_3hr,
                                            Qout_file=outflow_file_name,
                                            Qinit_file=qinit_file,
                                            BS_opt_Qinit=BS_opt_Qinit,
                                            BS_opt_dam = BS_opt_dam, 
                                            IS_dam_tot = IS_dam_tot,
                                            IS_dam_use = IS_dam_use,
                                            dam_tot_id_file = dam_tot_id_file,
                                            dam_use_id_file = dam_use_id_file,
                                            dam_file = dam_file)
            rapid_manager.run()

            #Merge all files together at the end
            cv = ConvertRAPIDOutputToCF(rapid_output_file=[outflow_file_name], 
                                        start_datetime=datetime.datetime.strptime(forecast_date_timestep[:11], "%Y%m%d.%H"), 
                                        time_step=[interval_3hr], 
                                        qinit_file=qinit_file, 
                                        comid_lat_lon_z_file=comid_lat_lon_z_file,
                                        rapid_connect_file=rapid_connect_file, 
                                        project_name="ECMWF-RAPID Predicted flows by US Army ERDC", 
                                        output_id_dim_name='rivid',
                                        output_flow_var_name='Qout',
                                        print_debug=False)

            cv.convert()
            #rapid_manager.make_output_CF_compliant(simulation_start_datetime=datetime.datetime.strptime(forecast_date_timestep[:11], "%Y%m%d.%H"),
            #                                       comid_lat_lon_z_file=comid_lat_lon_z_file,
            #                                       project_name="ECMWF-RAPID Predicted flows by US Army ERDC")
        except Exception:
            remove_file(inflow_file_name_3hr)
            traceback.print_exc()
            raise

        #remove_file(inflow_file_name_3hr)

    elif forecast_resolution == "ENS6":
        #LOW RES - 6hr only
        inflow_file_name = os.path.join(node_path, 'm3_riv_bas_%s.nc' % ensemble_number)

        grid_name = RAPIDinflowECMWF_tool.getGridName(ecmwf_forecast, high_res=False)
        #generate inflows for each timestep
        weight_table_file = case_insensitive_file_search(rapid_input_directory,
                                                         r'weight_{0}\.csv'.format(grid_name))

        try:

            print("INFO: Converting ECMWF inflow ...")
            RAPIDinflowECMWF_tool.execute(ecmwf_forecast, 
                                          weight_table_file, 
                                          inflow_file_name,
                                          conversion_flag, # added this line CJB 20190218
                                          grid_name)

            periods_6hr = time_step_count - 1 #new line 20190108 CJB, MJS
            interval = 6*60*60 #6hr
            duration = periods_6hr*interval #new line 20190108 CJB 
            rapid_manager.update_parameters(ZS_TauR=interval, #duration of routing procedure (time step of runoff data)
                                            ZS_dtR=15*60, #internal routing time step
                                            ZS_TauM=duration, #total simulation time 
                                            Vlat_file=inflow_file_name,
                                            Qout_file=outflow_file_name,
                                            Qinit_file=qinit_file,
                                            BS_opt_Qinit=BS_opt_Qinit,
                                            BS_opt_dam = BS_opt_dam, 
                                            IS_dam_tot = IS_dam_tot,
                                            IS_dam_use = IS_dam_use,
                                            dam_tot_id_file = dam_tot_id_file,
                                            dam_use_id_file = dam_use_id_file,
                                            dam_file = dam_file)
    
            rapid_manager.run()

            rapid_manager.make_output_CF_compliant(simulation_start_datetime=datetime.datetime.strptime(forecast_date_timestep[:11], "%Y%m%d.%H"),
                                                   comid_lat_lon_z_file=comid_lat_lon_z_file,
                                                   project_name="ECMWF-RAPID Predicted flows by US Army ERDC")

        except Exception:
            remove_file(inflow_file_name)
            traceback.print_exc()
            raise
            
        #clean up
        remove_file(inflow_file_name)

    #else:
    #    raise Exception("ERROR: invalid forecast resolution ...")
        
    time_stop_all = datetime.datetime.utcnow()
    print("INFO: Total time to compute: {0}".format(time_stop_all-time_start_all))

def run_ecmwf_rapid_multiprocess_worker(watershed_jobs_info, job):
    """
    Duplicate HTCondor behavior for multiprocess worker
    """
    print('In run_ecmwf_rapid_multiprocess_worker')
    ecmwf_forecast = job[0]
    forecast_date_timestep = job[1]
    watershed = job[2]
    subbasin = job[3]
    rapid_executable_location = job[4]
    initialize_flows = job[5]
    job_name = job[6]
    master_rapid_outflow_file = job[7]
    rapid_input_directory = job[8] 
    mp_execute_directory = job[9]
    subprocess_forecast_log_dir = job[10]
    watershed_job_index = job[11]
    initialization_time_step = job[12] # modified this line CJB 20190218
    conversion_flag = job[13] # added this line CJB 20190218; MJS see note on correcting unit attribute to mm when mm in netCDF4 and using that in the code here rather
                               # than leaving to user to know to do this and how.
#    initialization_time_step = args[13] # modified this line CJB 20190218
# dam args added, MJS 8/23/2020
    BS_opt_dam = job[14]
    IS_dam_tot = job[15]
    IS_dam_use = job[16]
    dam_tot_id_file = job[17]
    dam_use_id_file = job[18]
    dam_file =job[19]
    # ecmwf_forecast = args[0]
    # forecast_date_timestep = args[1]
    # watershed = args[2]
    # subbasin = args[3]
    # rapid_executable_location = args[4]
    # initialize_flows = args[5]
    # job_name = args[6]
    # master_rapid_outflow_file = args[7]
    # rapid_input_directory = args[8] 
    # mp_execute_directory = args[9]
    # subprocess_forecast_log_dir = args[10]
    # watershed_job_index = args[11]
    # conversion_flag = job [12] # added this line CJB, MJS 20190218
    # initialization_time_step = args[12] 
    
    with CaptureStdOutToLog(os.path.join(subprocess_forecast_log_dir, "{0}.log".format(job_name))):
        #create folder to run job
        execute_directory = os.path.join(mp_execute_directory, job_name)
        try:
            os.mkdir(execute_directory)
        except OSError:
            pass
        
        try:
            ecmwf_rapid_multiprocess_worker(execute_directory, rapid_input_directory,
                                            ecmwf_forecast, forecast_date_timestep, 
                                            watershed, subbasin, rapid_executable_location, 
                                            initialize_flows, initialization_time_step, conversion_flag, # modified this line CJB 20190218
# dam args added, MJS 8/23/2020
                                            BS_opt_dam,IS_dam_tot,IS_dam_use,
                                            dam_tot_id_file,dam_use_id_file,dam_file)
             
            #move output file from compute node to master location
            node_rapid_outflow_file = os.path.join(execute_directory, 
                                                   os.path.basename(master_rapid_outflow_file))
                                                   
            move(node_rapid_outflow_file, master_rapid_outflow_file)
            rmtree(execute_directory)
            # added this to try to upload forecast as it is generated
            # upload_forecast = upload_single_forecast(watershed_jobs_info[watershed_job_index], watershed_jobs_info[watershed_job_index]['data_manager'])
        except Exception:
            rmtree(execute_directory)
            traceback.print_exc()
            raise
    return watershed_job_index
    
