
# coding: utf-8

# In[1]:

from MesoPy import Meso
get_ipython().magic(u'matplotlib inline')
import matplotlib
import numpy as np
import matplotlib.pyplot as plt
import json
import ast

import xray
import pandas as pd
from netCDF4 import Dataset
from netCDF4 import num2date, date2num
from datetime import datetime, timedelta
import pytz

# OS interaction
import sys
import os

####### USER INPUT ##################

# Your Meswest token
# Email MesoWest API <mesowestapi@gmail.com> to request a API token
m = Meso(api_token='your token here')

# Path and file name to create
ncfilename = os.path.normpath("path and file name here")

# Manually select stations
#sta_id = [stations['STATION'][x]['STID'] for x in range(stations['SUMMARY']['NUMBER_OF_OBJECTS'])]
#sta_id = ['tsr18','D4827','NBDNB','TTANN']
#sta_id = ['tsr18','tfran','talpe','sno38','rkee','teast']
#sta_id = ['ksea','ftaw1','d4827','c5316','tfran','talpe','sno38','cppw1','pefw1','keln']

# Summit and East
#sta_id = ['sno38','talpe','rkee','e3732']
# West Summit East
sta_id = ['ksea','sno38','keln']

# Select stations from lat lon
#stations = m.station_list(radius=[-122.34169,47.604528,100]) #, county='King')
#stations = m.station_list(bbox=[-120,40,-119,41]) #, county='King')
#stations = m.station_list(bbox=[-121.89743,47.139731,-120.839996,47.599189]) #, county='King')

# Define variable names to extract
# (print allstationdata to see what is available)
Vars_ext = ['air_temp_set_1','wind_speed_set_1','wind_direction_set_1']

# Define Time period
StartDate = datetime(2010,10,1,0,0) 
EndDate   = datetime(2014,9,30,23,0)

#####################################


# In[2]:

####### Functions ##################


# In[3]:

def Get_data(m,sta_id,Vars_ext,StartDate,EndDate):
    
    
    #### Function for combining xray data variables into a single array with new labeled dimension
    def combinevars(ds_in,dat_vars,new_dim_name='new_dim',combinevarname='new_var'):
        ds_out = xray.Dataset()
        ds_out = xray.concat([ds_in[dv] for dv in dat_vars],dim='new_dim')
        ds_out = ds_out.rename({'new_dim': new_dim_name})
        ds_out.coords[new_dim_name] = dat_vars
        ds_out.name = combinevarname

        return ds_out

    # Grab all time series data from all stations for a given date range
    # Seems to only allow two years of data...
    allstationdata = m.timeseries_obs(stid=sta_id, start=StartDate, end=EndDate)
    N_sta = len(sta_id)
    
    #print allstationdata
    
    # Check we downloaded all requested station data
    if allstationdata['SUMMARY']['NUMBER_OF_OBJECTS'] != N_sta:
        print allstationdata['SUMMARY']['NUMBER_OF_OBJECTS']
        print N_sta
        print [ast.literal_eval(json.dumps(allstationdata['STATION'][cs]['NAME'])) for cs in range(0,allstationdata['SUMMARY']['NUMBER_OF_OBJECTS'])]
        raise ValueError('Did not find all stations, check names')
        
        # Get Station Info
    Elev  = [ast.literal_eval(json.dumps(allstationdata['STATION'][cs]['ELEVATION'])) for cs in range(0,N_sta)]
    Lat   = [ast.literal_eval(json.dumps(allstationdata['STATION'][cs]['LATITUDE'])) for cs in range(0,N_sta)]
    Lon   = [ast.literal_eval(json.dumps(allstationdata['STATION'][cs]['LONGITUDE'])) for cs in range(0,N_sta)]
    NAME  = [ast.literal_eval(json.dumps(allstationdata['STATION'][cs]['NAME'])) for cs in range(0,N_sta)]
    
    # Get timestamp timeseries for all stations (may be different lengths and different time steps)
    timestamp = []
    [timestamp.append(ob['OBSERVATIONS']['date_time']) for ob in allstationdata['STATION']]
    
    # Loop through each variable to extract
    DS_list = [] # Empty list of each dataset containing one variable
    for cVar in Vars_ext:
        # Get timeseries of data for all stations
        temp_list = []
        [temp_list.append(ob['OBSERVATIONS'][cVar]) for ob in allstationdata['STATION']]


        # Make dictionary of site and xray data array
        dict1 = {}
        for csta in range(0,len(sta_id)):
            c_t = [datetime.strptime(ast.literal_eval(json.dumps(timestamp[csta][cd])), '%Y-%m-%dT%H:%M:%SZ') for cd in range(len(timestamp[csta]))]
            dict1[sta_id[csta]] = xray.DataArray(np.array(temp_list[csta]), coords=[c_t], dims=['time'])

        # Make it a dataset
        ds_temp_Var = xray.Dataset(dict1)

        # Resample to common time step
        # Data contains mix of 15, 10, and 5 min data
        ds_temp_Var_1hr = ds_temp_Var.resample(freq='H',dim='time',how='mean',label='right')

        # Combine stations
        DS_list.append(combinevars(ds_temp_Var_1hr,sta_id,new_dim_name='site',combinevarname=cVar))
        
    # Make dictionary list
    DIC1 = dict(zip([cv.name for cv in DS_list],DS_list))
    
    # Combine all Datasets
    ds_ALL = xray.Dataset(DIC1)
    
    # Fill in descriptive variables
    ds_ALL.coords['lat'] = ('site',[float(x) for x in Lat])
    ds_ALL.coords['lon'] = ('site',[float(x) for x in Lon])
    ds_ALL.coords['elev'] = ('site',[float(x) for x in Elev])
    ds_ALL.coords['sta_name'] = ('site',NAME)
    
    
    return ds_ALL


# In[4]:

# MesoWest only allows ~2 years of data to be downloaded at a time
# Therefore, we split up our requests into year chunks

# Number of years we requested
N_years = int((EndDate-StartDate).total_seconds()/(365*24*60*60))

Year_rng = pd.date_range(start=StartDate, periods=N_years, freq=pd.DateOffset(years=1))
#s = pd.Series(Year_rng)
# For each calender year
c_DS = []
for cY in range(1,len(Year_rng)):
    c_date_S = Year_rng[cY-1].strftime('%Y%m%d%H%M') # format needed '201210010000'
    c_date_E = Year_rng[cY].strftime('%Y%m%d%H%M') # format needed '201210010000'
    print 'Downloading data ' + c_date_S + ' through ' + c_date_E
    #print c_date_E

    c_DS.append(Get_data(m,sta_id,Vars_ext,c_date_S,c_date_E))

# Get last period of data (fence post)
print 'Downloading data ' + Year_rng[-1].strftime('%Y%m%d%H%M') + ' through ' + EndDate.strftime('%Y%m%d%H%M')
c_DS.append(Get_data(m,sta_id,Vars_ext,Year_rng[-1].strftime('%Y%m%d%H%M'),EndDate.strftime('%Y%m%d%H%M')))
print 'Finished downloading data'


# In[5]:

## Combine Datasets by time
ds_ALL = xray.concat(c_DS,dim='time')


# In[6]:

## Output to netcdf
ds_ALL.to_netcdf(ncfilename)

