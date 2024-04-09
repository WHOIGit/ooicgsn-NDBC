import numpy as np
import pandas as pd
import os
import pytz
import datetime as dt
import csv
import re
import gsw


# +
# Set the indicies of the different METBK data
class NDBC():

    def __init__(self, station_id, deploy_id, WMO, currentTime, startTime,
                 data_map, name_map):
        self.station_id = station_id
        self.deploy_id = deploy_id
        self.WMO = WMO
        self.now = currentTime
        self.startTime = startTime
        self.data_map = data_map
        self.name_map = name_map
        
    
    def parse_data_to_xml(self, data):
        """
        Function which takes in the 10-minute average buoy data,
        the station name, and two dictionaries which map the buoy
        column names to the xml tags, and outputs an xml file in
        the NDBC format.

        Returns:
            xml - a properly constructed xml file in the NDBC
            format for the given buoy data
        """

        # Start the xml file
        xml = ['<?xml version="1.0" encoding="ISO-8859-1"?>']

        # Iterate through the data
        for index in data.index:

            # Get the data associated with a row in the dataframe
            row = data.loc[index]

            # Reset a dictionary of the data
            xml_data = {}
            for key in self.data_map.keys():
                xml_data.update({key: self.data_map.get(key)})

            # Parse the data into the data dictionary
            for key in xml_data.keys():
                # Get the column name which corresponds to the ndbc tag
                column = self.name_map.get(key)
                # Check that the column was returned from the ERDDAP server
                if column in row.index:
                    value = row[column]
                    # If a nan, just leave it the default -9999
                    if str(value) == 'nan':
                        pass
                    else:
                        xml_data[key] = value
                # If no data, leave it as default -9999
                else:
                    pass

            # Write the parsed data to the xml file
            # Start the message
            xml.append('<message>')

            # Add in the station id
            xml.append(f'  <station>{self.WMO}</station>')

            # Get the time index
            time = row.name.strftime('%m/%d/%Y %H:%M:%S')
            xml.append(f'  <date>{time}</date>')

            # Missing fill value
            missing = str(-9999)
            xml.append(f'  <missing>{missing}</missing>')

            # Roundtime
            xml.append('  <roundtime>no</roundtime>')

            # Start of the data
            xml.append('  <met>')

            # Add in each data piece
            for tag in xml_data.keys():
                # Get the value
                value = xml_data.get(tag)
                value = str(value)
                # Add the data to the xml file
                xml.append(f'    <{tag}>{value}</{tag}>')

            # Finish off the message
            xml.append('  </met>')
            xml.append('</message>')

        # Return the results
        return xml
    
    
    def create_empty_dataset(self):
        """
        Create a dataset of all nans if there is no data available
        for the requested dataset in the given time period.
        """
        # Create an empty array
        empty_array = np.empty((2, len(merged_data.columns)))
        empty_array[:] = np.nan

        # Create a dataset with the empty data
        empty_df = pd.DataFrame(data=empty_array, columns=merged_data.columns,
                                index=[self.startTime, self.now])
        empty_df.index.name = 'TIMESTAMP'

        # Resample the empty dataset to 10-minute averages
        empty_df = empty_df.resample('10T').mean()

        return empty_df
    
        


class METBK():
    def __init__(self):
        self.METBK_DATA = {
            'TIMESTAMP': [],
            'BAROMETRIC_PRESSURE': [],
            'RELATIVE_HUMIDITY': [],
            'AIR_TEMPERATURE': [],
            'LONGWAVE_IRRADIANCE': [],
            'PRECIPITATION': [],
            'SEA_SURFACE_TEMPERATURE': [],
            'SEA_SURFACE_CONDUCTIVITY': [],
            'SHORTWAVE_IRRADIANCE': [],
            'WIND_EASTWARD': [],
            'WIND_NORTHWARD': [],
        }
        
        self.METBK_DATA_INDEX = {
            'TIMESTAMP': 0,
            'BAROMETRIC_PRESSURE': 1,
            'RELATIVE_HUMIDITY': 2,
            'AIR_TEMPERATURE': 3,
            'LONGWAVE_IRRADIANCE': 4,
            'PRECIPITATION': 5,
            'SEA_SURFACE_TEMPERATURE': 6,
            'SEA_SURFACE_CONDUCTIVITY': 7,
            'SHORTWAVE_IRRADIANCE': 8,
            'WIND_EASTWARD': 9,
            'WIND_NORTHWARD': 10,
        }
        
        self.METBK_DATA_PATTERN = (r'(-*\d+\.\d+|NaN)' +  # BPR
                       '\s*(-*\d+\.\d+|NaN)' +  # RH %
                       '\s*(-*\d+\.\d+|NaN)' +  # RH temp
                       '\s*(-*\d+\.\d+|NaN)' +  # LWR
                       '\s*(-*\d+\.\d+|NaN)' +  # PRC
                       '\s*(-*\d+\.\d+|NaN)' +  # ST
                       '\s*(-*\d+\.\d+|NaN)' +  # SC
                       '\s*(-*\d+\.\d+|NaN)' +  # SWR
                       '\s*(-*\d+\.\d+|NaN)' +  # We
                       '\s*(-*\d+\.\d+|NaN)' +  # Wn
                       '.*?' + '\n')  # throw away batteries

        self.SAMPLE_TIMESTAMP_PATTERN = (r'\d{4}/\d{2}/\d{2}' +      # Date in yyyy/mm/dd
                            '\s*\d{2}:\d{2}:\d{2}.\d+') # Time in HH:MM:SS.fff  
    
    def parse_metbk(self, raw_data):
        """
        Parses a line of METBK data into the individual sensor components
        
        Parameters
        ----------
        raw_data: (str)
            The opened data from a raw data file that has been read line-by-line
            
        Returns
        -------
        self.DATA: (dict)
            A dictionary of the parsed raw data stored into the applicable
            measurements
        """

        for line in raw_data:
            if line is not None:
                # Check if the line contains data
                try:
                    float(line.split()[-1])
                    # Now, replace Na with NaN
                    line = re.sub(r'Na ', 'NaN', line)
                    # Next, match the timestamp
                    timestamp = re.findall(self.SAMPLE_TIMESTAMP_PATTERN, line)
                    # Remove the timestamp from the data string
                    line = re.sub(timestamp[0], '', line)
                    # Get the data
                    raw_data = re.findall(self.METBK_DATA_PATTERN, line)[0]

                except:
                    # Check that there is parseable timestamp
                    timestamp = re.findall(self.SAMPLE_TIMESTAMP_PATTERN, line)
                    if len(timestamp) != 0:
                        # Create an empty array of all NaNs
                        raw_data = ['NaN']*10
                    else:
                        # There is no useful information in the line
                        line = None

                # Append the timestamp to the start of the list
                if line is not None:
                    raw_data = list(raw_data)
                    raw_data.insert(0, timestamp[0])

                    # Now we can start putting the data into the data dictionary
                    for key in self.METBK_DATA.keys():
                        # Get the index of the data
                        index = self.METBK_DATA_INDEX.get(key)
                        self.METBK_DATA[key].append(raw_data[index])
                    
                    
    def process_data(self, mooring):
        """
        Process the parsed METBK data into a dataframe with derived variables
        
        This function takes in the parsed METBK data, converts it to a dataframe,
        indexes via time, converts data types from strings, and then resamples
        the data into 10 minute averages. Then, it derives the practical salinity
        from the conductivity and temperature, adjusts the barometric pressure to
        sea-surface-equivalent, and derives the absolute wind speed (m/s) and 
        wind direction (degrees) from the north and east wind vector components
        
        Parameters
        ----------
        self.METBK_DATA: dict
            The parsed METBK data stored in a dictionary
        mooring: (str)
            A string of the mooring name, e.g. GI01SUMO
            
        Returns
        -------
        df: pandas.DataFrame
            A pandas DataFrame with the METBK data with the METBK_DATA dict keys as
            column headers, indexed by time, resampled to 10-minute averages, and 
            the derived variables.
        """
    
        # First, stick it into a dataframe
        df = pd.DataFrame(self.METBK_DATA)

        # Next, convert types
        df["TIMESTAMP"] = df["TIMESTAMP"].apply(lambda x: pd.to_datetime(x))
        df.set_index(keys=["TIMESTAMP"], inplace=True)
        df = df.applymap(float)

        # Bin into 10-minute increments
        df = df.resample('10T').mean()

        # Calculate practical salinity
        C = df["SEA_SURFACE_CONDUCTIVITY"]
        T = df["SEA_SURFACE_TEMPERATURE"]
        P = 1
        df["SEA_SURFACE_PRACTICAL_SALINITY"] = calculate_practical_salinity(C, T, P)

        # Adjust the barometric pressure to sea-level
        # First get the height of the sensor
        if mooring.startswith("GI"):
            height = 5.05
        elif mooring.startswith("CP"):
            height = 4.05
        else:
            pass
        df["SEA_LEVEL_PRESSURE"] = adjust_pressure_to_sea_level(
            df["BAROMETRIC_PRESSURE"], 
            df["AIR_TEMPERATURE"], 
            height)

        # Calculate the wind speed
        df['WIND_SPEED'] = calculate_wind_speed(
            df['WIND_EASTWARD'],
            df['WIND_NORTHWARD'])

        # Calculate the wind direction
        df['WIND_DIRECTION'] = calculate_wind_direction(
            df['WIND_EASTWARD'],
            df['WIND_NORTHWARD'])
        
        # Adjust for wind directionsthat are outside 0-360
        df['WIND_DIRECTION'] = df["WIND_DIRECTION"].apply(
                        lambda x: x+360 if x < 0 else x)

        return df
    
    

                    
class WAVSS():
    
    def __init__(self):
        
        self.WAVSS_DATA = {
            'TIMESTAMP': [],
            'INSTRUMENT_DATE': [],
            'INSTRUMENT_TIME': [],
            'INSTRUMENT_SERIAL': [],
            'BUOY_ID': [],
            'LATITUDE': [],
            'LONGITUDE': [],
            'N_ZERO_CROSSINGS': [],
            'AVERAGE_WAVE_HEIGHT': [],
            'MEAN_SPECTRAL_PERIOD': [],
            'MAXIMUM_WAVE_HEIGHT': [],
            'SIGNIFICANT_WAVE_HEIGHT': [],
            'SIGNIFICANT_PERIOD': [],
            'AVERAGE_HEIGHT_10TH_HIGHEST': [],
            'AVERAGE_PERIOD_10TH_HIGHEST': [],
            'MEAN_WAVE_PERIOD': [],
            'PEAK_PERIOD': [],
            'TP5': [],
            'HMO': [],
            'MEAN_DIRECTION': [],
            'MEAN_SPREAD': []
        }
        
        self.WAVSS_DATA_INDEX = {
            'TIMESTAMP': 0,
            'INSTRUMENT_DATE': 2,
            'INSTRUMENT_TIME': 3,
            'INSTRUMENT_SERIAL': 4,
            'BUOY_ID': 5,
            'LATITUDE': 6,
            'LONGITUDE': 7,
            'N_ZERO_CROSSINGS': 8,
            'AVERAGE_WAVE_HEIGHT': 9,
            'MEAN_SPECTRAL_PERIOD': 10,
            'MAXIMUM_WAVE_HEIGHT': 11,
            'SIGNIFICANT_WAVE_HEIGHT': 12,
            'SIGNIFICANT_PERIOD': 13,
            'AVERAGE_HEIGHT_10TH_HIGHEST': 14,
            'AVERAGE_PERIOD_10TH_HIGHEST': 15,
            'MEAN_WAVE_PERIOD': 16,
            'PEAK_PERIOD': 17,
            'TP5': 18,
            'HMO': 19,
            'MEAN_DIRECTION': 20,
            'MEAN_SPREAD': 21
        }
        
        
    
    def parse_wavss(self, raw_data):
        """
        Parse the raw_data into the different measurements
        
        Parameters
        ----------
        raw_data: (str)
            A string of each line of data from the instrument raw data file

            
        Returns
        -------
        self.DATA: (dict)
            A dictionary of the parsed raw data stored into the applicable
            measurements
            
        """
        for line in raw_data:
            
            # Check that its a wave_statistics measurement
            if '$TSPWA' not in line:
                continue

            # Dump everything after the "*"
            line = re.sub(r'\*.*', '', line, flags=re.DOTALL)

            # Split the data
            line = re.split(r' \$|,', line)

            # Check that it is a full data record. If not, return none
            if len(line) != 22:
                continue

            # Parse the raw data into the data dictionary based on index
            for key in self.WAVSS_DATA_INDEX.keys():
                # Get the index of a particular measurement
                index = self.WAVSS_DATA_INDEX.get(key)

                # Put the parsed raw data into the data dictionary
                self.WAVSS_DATA[key].append(line[index])

        # Next, check if the data record is full. If not, fill in with two empty datapoints
        if len(self.WAVSS_DATA['TIMESTAMP']) == 0:
            currentTime = pd.Timestamp.now(tz='UTC').replace(tzinfo=None)
            startTime = currentTime.replace(minute=0, second=0, microsecond=0) - dt.timedelta(hours=4)
            for key in self.WAVSS_DATA.keys():
                if key == 'TIMESTAMP':
                    self.WAVSS_DATA[key] = [startTime, currentTime]
                else:
                    self.WAVSS_DATA[key] = [np.nan, np.nan]
                
                
    def process_data(self):
        """
        Process the parsed WAVSS data into a dataframe resampled to 10-minute avg

        This function takes in the parsed WAVSS data, converts it to a dataframe,
        indexes via time, converts data types from strings, and then resamples
        the data into 10 minute averages. 

        Parameters
        ----------
        self.WAVSS_DATA: dict
            The parsed METBK data stored in a dictionary

        Returns
        -------
        df: pandas.DataFrame
            A pandas DataFrame with the METBK data with the METBK_DATA dict keys as
            column headers, indexed by time, resampled to 10-minute averages, and 
            the derived variables.
        """
        # First, stick it into a dataframe
        df = pd.DataFrame(self.WAVSS_DATA)
        df.drop(columns=["BUOY_ID", "LATITUDE", "LONGITUDE"], inplace=True)

        # Next, convert types
        df["TIMESTAMP"] = df["TIMESTAMP"].apply(lambda x: pd.to_datetime(x))
        df.set_index(keys=["TIMESTAMP"], inplace=True)
        df = df.applymap(float)

        # Bin into 10-minute increments
        df = df.resample('10T').mean()

        return df        

        
# Functions after the parsing
def adjust_pressure_to_sea_level(pres, temp, height):
    """Adjust barometric presure to sea-level."""
    temp = temp + 273.15
    slp = pres / np.exp(-height / (temp * 29.263))
    return slp


def calculate_wind_speed(eastward, northward):
    """Calculate absolute wind speed from component wind vector."""
    u = np.square(eastward)
    v = np.square(northward)
    wind_speed = np.sqrt(u + v)
    return wind_speed


def calculate_wind_direction(eastward, northward):
    """Calculate met wind direction from component wind vectors."""
    u = eastward
    v = northward
    wind_direction = 180/np.pi * np.arctan2(-u, -v)
    return wind_direction


def calculate_practical_salinity(C, T, P):
    """Calculate the practical salinity using TEOS-10"""
    C = np.atleast_1d(C)
    T = np.atleast_1d(T)
    SP = gsw.SP_from_C(C*10, T, P)
    return SP


def get_files(BASE_PATH, buoy, deployment):
    """
    Gets the files for all the available met and wavess sensors for a given buoy and deployment
    """
    metbk1 = []
    metbk2 = []
    wavss = []

    # Filepath
    FILEPATH = "/".join((BASE_PATH.rstrip("/"), buoy, deployment))
    
    for root, dirs, files in os.walk(FILEPATH):          
        if "metbk1" in root:
            for f in files:
                if f.endswith(".log"):
                    metbk1.append(os.path.join(root, f))
        elif "metbk2" in root:
            for f in files:
                if f.endswith(".log"):
                    metbk2.append(os.path.join(root, f))
        elif "metbk" in root and "metbk1" not in root and "metbk2" not in root:
            for f in files:
                if f.endswith(".log"):
                    metbk1.append(os.path.join(root, f))
        elif "wavss" in root:
            for f in files:
                if f.endswith(".log"):
                    wavss.append(os.path.join(root, f))
        else:
            pass

    return sorted(metbk1), sorted(metbk2), sorted(wavss)


def add_header_prefix(df, prefix):
    """Adds a prefix to the columns in the dataframe"""
    
    for col in df.columns:
        df.rename(columns={col: prefix + col}, inplace=True)
    return df


# Global Irminger Surface Mooring
gi01sumo_data_map = {
    # Data variables
    'atmp1': -9999,
    'atmp2': -9999,
    'baro1': -9999,
    'baro2': -9999,
    'lwrad': -9999,
    'rrh': -9999,
    'srad1': -9999,
    'wspd1': -9999,
    'wspd2': -9999,
    'wdir1': -9999,
    'wdir2': -9999,
    'wtmp1': -9999,
    'wtmp2': -9999,
    'tp001': -9999,
    'tp002': -9999,
    'sp001': -9999,
    'sp002': -9999,
    'dompd': -9999,
    'mwdir': -9999,
    'wvhgt': -9999,
    # Fixed constants
    'dp001': 0.95,
    'dp002': 1.15,
    'fm64iii': 830,
    'fm64k1': 7,
    'fm64k2': 1
}

gi01sumo_name_map = {
    'atmp1': 'METBK1 AIR_TEMPERATURE',
    'atmp2': 'METBK2 AIR_TEMPERATURE',
    'baro1': 'METBK1 SEA_LEVEL_PRESSURE',
    'baro2': 'METBK2 SEA_LEVEL_PRESSURE',
    'lwrad': 'METBK1 LONGWAVE_IRRADIANCE',
    'rrh':   'METBK1 RELATIVE_HUMIDITY',
    'srad1': 'METBK1 SHORTWAVE_IRRADIANCE',
    'wspd1': 'METBK1 WIND_SPEED',
    'wspd2': 'METBK2 WIND_SPEED',
    'wdir1': 'METBK1 WIND_DIRECTION',
    'wdir2': 'METBK2 WIND_DIRECTION',
    'wtmp1': 'METBK1 SEA_SURFACE_TEMPERATURE',
    'wtmp2': 'METBK2 SEA_SURFACE_TEMPERATURE',
    'tp001': 'METBK1 SEA_SURFACE_TEMPERATURE',
    'tp002': 'METBK2 SEA_SURFACE_TEMPERATURE',
    'sp001': 'METBK1 SEA_SURFACE_PRACTICAL_SALINITY',
    'sp002': 'METBK2 SEA_SURFACE_PRACTICAL_SALINITY',
    'dompd': 'WAVSS SIGNIFICANT_PERIOD',
    'mwdir': 'WAVSS MEAN_DIRECTION',
    'wvhgt': 'WAVSS SIGNIFICANT_WAVE_HEIGHT',
}

# Coastal Pioneer - MAB Central Surface Mooring
cp10cnsm_data_map = {
    # Data variables
    'atmp1': -9999,
    'atmp2': -9999,
    'baro1': -9999,
    'baro2': -9999,
    'lwrad': -9999,
    'rrh': -9999,
    'srad1': -9999,
    'wspd1': -9999,
    'wspd2': -9999,
    'wdir1': -9999,
    'wdir2': -9999,
    'wtmp1': -9999,
    'wtmp2': -9999,
    'tp001': -9999,
    'tp002': -9999,
    'sp001': -9999,
    'sp002': -9999,
    'dompd': -9999,
    'mwdir': -9999,
    'wvhgt': -9999,
    # Fixed constants
    'dp001': 0.95,
    'dp002': 1.15,
    'fm64iii': 830,
    'fm64k1': 7,
    'fm64k2': 1
}

cp10cnsm_name_map = {
    'atmp1': 'METBK1 AIR_TEMPERATURE',
    'atmp2': 'METBK2 AIR_TEMPERATURE',
    'baro1': 'METBK1 SEA_LEVEL_PRESSURE',
    'baro2': 'METBK2 SEA_LEVEL_PRESSURE',
    'lwrad': 'METBK1 LONGWAVE_IRRADIANCE',
    'rrh':   'METBK1 RELATIVE_HUMIDITY',
    'srad1': 'METBK1 SHORTWAVE_IRRADIANCE',
    'wspd1': 'METBK1 WIND_SPEED',
    'wspd2': 'METBK2 WIND_SPEED',
    'wdir1': 'METBK1 WIND_DIRECTION',
    'wdir2': 'METBK2 WIND_DIRECTION',
    'wtmp1': 'METBK1 SEA_SURFACE_TEMPERATURE',
    'wtmp2': 'METBK2 SEA_SURFACE_TEMPERATURE',
    'tp001': 'METBK1 SEA_SURFACE_TEMPERATURE',
    'tp002': 'METBK2 SEA_SURFACE_TEMPERATURE',
    'sp001': 'METBK1 SEA_SURFACE_PRACTICAL_SALINITY',
    'sp002': 'METBK2 SEA_SURFACE_PRACTICAL_SALINITY',
    'dompd': 'WAVSS SIGNIFICANT_PERIOD',
    'mwdir': 'WAVSS MEAN_DIRECTION',
    'wvhgt': 'WAVSS SIGNIFICANT_WAVE_HEIGHT',
}

# Coast Pioneer - MAB Northern Surface Mooring
cp11nosm_data_map = {
    # Data variables
    'atmp1': -9999,
    'baro1': -9999,
    'lwrad': -9999,
    'rrh': -9999,
    'srad1': -9999,
    'wdir1': -9999,
    'wtmp1': -9999,
    'tp001': -9999,
    'sp001': -9999,
    'dompd': -9999,
    'mwdir': -9999,
    'wvhgt': -9999,
    # Fixed constants
    'dp001': 0.95,
    'fm64iii': 830,
    'fm64k1': 7,
    'fm64k2': 1
}

cp11nosm_name_map = {
    'atmp1': 'METBK1 AIR_TEMPERATURE',
    'baro1': 'METBK1 SEA_LEVEL_PRESSURE',
    'lwrad': 'METBK1 LONGWAVE_IRRADIANCE',
    'rrh':   'METBK1 RELATIVE_HUMIDITY',
    'srad1': 'METBK1 SHORTWAVE_IRRADIANCE',
    'wspd1': 'METBK1 WIND_SPEED',
    'wdir1': 'METBK1 WIND_DIRECTION',
    'wtmp1': 'METBK1 SEA_SURFACE_TEMPERATURE',
    'tp001': 'METBK1 SEA_SURFACE_TEMPERATURE',
    'sp001': 'METBK1 SEA_SURFACE_PRACTICAL_SALINITY',
    'dompd': 'WAVSS SIGNIFICANT_PERIOD',
    'mwdir': 'WAVSS MEAN_DIRECTION',
    'wvhgt': 'WAVSS SIGNIFICANT_WAVE_HEIGHT',
}

# Coast Pioneer - MAB Southern Surface Mooring
cp11sosm_data_map = {
    # Data variables
    'atmp1': -9999,
    'baro1': -9999,
    'lwrad': -9999,
    'rrh': -9999,
    'srad1': -9999,
    'wdir1': -9999,
    'wtmp1': -9999,
    'tp001': -9999,
    'sp001': -9999,
    'dompd': -9999,
    'mwdir': -9999,
    'wvhgt': -9999,
    # Fixed constants
    'dp001': 0.95,
    'fm64iii': 830,
    'fm64k1': 7,
    'fm64k2': 1
}

cp11sosm_name_map = {
    'atmp1': 'METBK1 AIR_TEMPERATURE',
    'baro1': 'METBK1 SEA_LEVEL_PRESSURE',
    'lwrad': 'METBK1 LONGWAVE_IRRADIANCE',
    'rrh':   'METBK1 RELATIVE_HUMIDITY',
    'srad1': 'METBK1 SHORTWAVE_IRRADIANCE',
    'wspd1': 'METBK1 WIND_SPEED',
    'wdir1': 'METBK1 WIND_DIRECTION',
    'wtmp1': 'METBK1 SEA_SURFACE_TEMPERATURE',
    'tp001': 'METBK1 SEA_SURFACE_TEMPERATURE',
    'sp001': 'METBK1 SEA_SURFACE_PRACTICAL_SALINITY',
    'dompd': 'WAVSS SIGNIFICANT_PERIOD',
    'mwdir': 'WAVSS MEAN_DIRECTION',
    'wvhgt': 'WAVSS SIGNIFICANT_WAVE_HEIGHT',
}

#BASE_PATH = 'data/rawdata-west.oceanobservatories.org/files/'
BASE_PATH = '/mnt/cg-data/raw/'
# -

if __name__ == '__main__':
    # Data directory path
    #dataPath = 'data'
    dataPath = '/home/ooiuser/ndbc/data'

    # Get the last 2-hours of data
    currentTime = pd.Timestamp.now(tz='UTC')
    startTime = currentTime.replace(minute=0, second=0, microsecond=0) - dt.timedelta(hours=4)
    timestamp = currentTime.strftime('%Y%m%d%H%M%S')

    # =========================================================================
    # Initialize the GI01SUMO BUOY dataset
    SUMO = NDBC('GI01SUMO', 'D00010', '44078', currentTime, startTime,
                gi01sumo_data_map, gi01sumo_name_map)

    # Initialize the parser objects
    metbk1 = METBK()
    metbk2 = METBK()
    wavss = WAVSS()

    # Get the files and select for the last two
    metbk1_files, metbk2_files, wavss_files = get_files(BASE_PATH, 'GI01SUMO', 'D00010')

    # Load and parse the data, using only the last two available files
    for file in sorted(metbk1_files[-2:]):
        try:
            with open(file) as f:
                raw_data = f.readlines()
                metbk1.parse_metbk(raw_data)
        except:
            pass

    for file in sorted(metbk2_files[-2:]):
        try:
            with open(file) as f:
                raw_data = f.readlines()
                metbk2.parse_metbk(raw_data)
        except:
            pass    

    for file in sorted(wavss_files[-2:]):
        try:
            with open(file) as f:
                raw_data = f.readlines()
                wavss.parse_wavss(raw_data)
        except:
            pass

    # Next, process the data into dataframes
    df_metbk1 = metbk1.process_data('GI01SUMO')
    df_metbk2 = metbk2.process_data('GI01SUMO')
    df_wavss = wavss.process_data()

    # Add the headers
    df_metbk1 = add_header_prefix(df_metbk1, "METBK1 ")
    df_metbk2 = add_header_prefix(df_metbk2, "METBK2 ")
    df_wavss = add_header_prefix(df_wavss, "WAVSS ")

    # Merge the datasets
    merged_data = df_metbk1.merge(df_metbk2, how="outer", left_index=True, right_index=True).merge(df_wavss, how="outer", left_index=True, right_index=True)

    # Fill in missing met data that is only reported for one sensor
    merged_data["METBK1 RELATIVE_HUMIDITY"] = merged_data["METBK1 RELATIVE_HUMIDITY"].fillna(merged_data["METBK2 RELATIVE_HUMIDITY"])
    merged_data["METBK1 LONGWAVE_IRRADIANCE"] = merged_data["METBK1 LONGWAVE_IRRADIANCE"].fillna(merged_data["METBK2 LONGWAVE_IRRADIANCE"])
    merged_data["METBK1 SHORTWAVE_IRRADIANCE"] = merged_data["METBK1 SHORTWAVE_IRRADIANCE"].fillna(merged_data["METBK2 SHORTWAVE_IRRADIANCE"])

    # Filter the data for only the most recent data
    merged_data = merged_data.tz_localize('UTC')
    mask = merged_data.index >= startTime
    merged_data = merged_data[mask]
    if merged_data.empty:
        merged_data = SUMO.create_empty_dataset()

    # Parse the data to xml
    SUMO.xml = SUMO.parse_data_to_xml(merged_data)

    # Write the data out to a file
    with open(f'{dataPath}/{SUMO.WMO}_{timestamp}.xml', 'w') as file:
        for line in SUMO.xml:
            file.write(f'{line}\n')


    # =========================================================================
    # Initialize the CP10CNSM BUOY dataset
    CNSM = NDBC('CP10CNSM', 'D00001', '41082', currentTime, startTime,
                cp10cnsm_data_map, cp10cnsm_name_map)

    # Initialize the parser objects
    metbk1 = METBK()
    metbk2 = METBK()
    wavss = WAVSS()

    # Get the files and select for the last two
    metbk1_files, metbk2_files, wavss_files = get_files(BASE_PATH, 'CP10CNSM', 'D00001')

    # Load and parse the data, using only the last two available files
    for file in sorted(metbk1_files[-2:]):
        try:
            with open(file) as f:
                raw_data = f.readlines()
                metbk1.parse_metbk(raw_data)
        except:
            pass

    for file in sorted(metbk2_files[-2:]):
        try:
            with open(file) as f:
                raw_data = f.readlines()
                metbk2.parse_metbk(raw_data)
        except:
            pass    

    for file in sorted(wavss_files[-2:]):
        try:
            with open(file) as f:
                raw_data = f.readlines()
                wavss.parse_wavss(raw_data)
        except:
            pass

    # Next, process the data into dataframes
    df_metbk1 = metbk1.process_data('CP10CNSM')
    df_metbk2 = metbk2.process_data('CP10CSNM')
    df_wavss = wavss.process_data()

    # Add the headers
    df_metbk1 = add_header_prefix(df_metbk1, "METBK1 ")
    df_metbk2 = add_header_prefix(df_metbk2, "METBK2 ")
    df_wavss = add_header_prefix(df_wavss, "WAVSS ")

    # Merge the datasets
    merged_data = df_metbk1.merge(df_metbk2, how="outer", left_index=True, right_index=True).merge(df_wavss, how="outer", left_index=True, right_index=True)

    # Fill in missing met data that is only reported for one sensor
    merged_data["METBK1 RELATIVE_HUMIDITY"] = merged_data["METBK1 RELATIVE_HUMIDITY"].fillna(merged_data["METBK2 RELATIVE_HUMIDITY"])
    merged_data["METBK1 LONGWAVE_IRRADIANCE"] = merged_data["METBK1 LONGWAVE_IRRADIANCE"].fillna(merged_data["METBK2 LONGWAVE_IRRADIANCE"])
    merged_data["METBK1 SHORTWAVE_IRRADIANCE"] = merged_data["METBK1 SHORTWAVE_IRRADIANCE"].fillna(merged_data["METBK2 SHORTWAVE_IRRADIANCE"])

    # Filter the data for only the most recent data
    merged_data = merged_data.tz_localize('UTC')
    mask = merged_data.index >= startTime
    merged_data = merged_data[mask]
    if merged_data.empty:
        merged_data = CNSM.create_empty_dataset()

    # Parse the data to xml
    CNSM.xml = CNSM.parse_data_to_xml(merged_data)

    # Write the data out to a file
    with open(f'{dataPath}/{CNSM.WMO}_{timestamp}.xml', 'w') as file:
        for line in CNSM.xml:
            file.write(f'{line}\n')

    # =========================================================================
    # Initialize the CP11NOSM BUOY dataset
    NOSM = NDBC('CP11NOSM', 'D00001', '44079', currentTime, startTime,
                cp11nosm_data_map, cp11nosm_name_map)

    # Initialize the parser objects
    metbk1 = METBK()
    wavss = WAVSS()

    # Get the files and select for the last two
    metbk1_files, metbk2_files, wavss_files = get_files(BASE_PATH, 'CP11NOSM', 'D00001')

    # Load and parse the data, using only the last two available files
    for file in sorted(metbk1_files[-2:]):
        try:
            with open(file) as f:
                raw_data = f.readlines()
                metbk1.parse_metbk(raw_data)
        except:
            pass

    for file in sorted(wavss_files[-2:]):
        try:
            with open(file) as f:
                raw_data = f.readlines()
                wavss.parse_wavss(raw_data)
        except:
            pass

    # Next, process the data into dataframes
    df_metbk1 = metbk1.process_data('CP11NOSM')
    df_wavss = wavss.process_data()

    # Add the headers
    df_metbk1 = add_header_prefix(df_metbk1, "METBK1 ")
    df_wavss = add_header_prefix(df_wavss, "WAVSS ")

    # Merge the datasets
    merged_data = df_metbk1.merge(df_wavss, how="outer", left_index=True, right_index=True)

    # Fill in missing met data that is only reported for one sensor
    # No second METBK sensor on NOSM

    # Filter the data for only the most recent data
    merged_data = merged_data.tz_localize('UTC')
    mask = merged_data.index >= startTime
    merged_data = merged_data[mask]
    if merged_data.empty:
        merged_data = NOSM.create_empty_dataset()

    # Parse the data to xml
    NOSM.xml = NOSM.parse_data_to_xml(merged_data)

    # Write the data out to a file
    with open(f'{dataPath}/{NOSM.WMO}_{timestamp}.xml', 'w') as file:
        for line in NOSM.xml:
            file.write(f'{line}\n')

   # =========================================================================
    # Initialize the CP11NOSM BUOY dataset
    SOSM = NDBC('CP11SOSM', 'D00001', '41083', currentTime, startTime,
                cp11sosm_data_map, cp11sosm_name_map)

    # Initialize the parser objects
    metbk1 = METBK()
    wavss = WAVSS()

    # Get the files and select for the last two
    metbk1_files, metbk2_files, wavss_files = get_files(BASE_PATH, 'CP11SOSM', 'D00001')

    # Load and parse the data, using only the last two available files
    for file in sorted(metbk1_files[-2:]):
        try:
            with open(file) as f:
                raw_data = f.readlines()
                metbk1.parse_metbk(raw_data)
        except:
            pass

    for file in sorted(wavss_files[-2:]):
        try:
            with open(file) as f:
                raw_data = f.readlines()
                wavss.parse_wavss(raw_data)
        except:
            pass

    # Next, process the data into dataframes
    df_metbk1 = metbk1.process_data('CP11SOSM')
    df_wavss = wavss.process_data()

    # Add the headers
    df_metbk1 = add_header_prefix(df_metbk1, "METBK1 ")
    df_wavss = add_header_prefix(df_wavss, "WAVSS ")

    # Merge the datasets
    merged_data = df_metbk1.merge(df_wavss, how="outer", left_index=True, right_index=True)

    # Fill in missing met data that is only reported for one sensor
    # No second METBK sensor on NOSM

    # Filter the data for only the most recent data
    merged_data = merged_data.tz_localize('UTC')
    mask = merged_data.index >= startTime
    merged_data = merged_data[mask]
    if merged_data.empty:
        merged_data = SOSM.create_empty_dataset()

    # Parse the data to xml
    SOSM.xml = SOSM.parse_data_to_xml(merged_data)

    # Write the data out to a file
    with open(f'{dataPath}/{SOSM.WMO}_{timestamp}.xml', 'w') as file:
        for line in SOSM.xml:
            file.write(f'{line}\n')