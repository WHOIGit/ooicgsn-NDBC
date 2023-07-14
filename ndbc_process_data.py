import pytz
import datetime as dt
import pandas as pd
import numpy as np
from erddapy import ERDDAP
import warnings
warnings.filterwarnings("ignore")


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

    def adjust_pressure_to_sea_level(self, pres, temp, height):
        """Adjust barometric presure to sea-level."""
        temp = temp + 273.15
        slp = pres / np.exp(-height / (temp * 29.263))
        return slp

    def calculate_wind_speed(self, eastward, northward):
        """Calculate absolute wind speed from component wind vector."""
        u = np.square(eastward)
        v = np.square(northward)
        wind_speed = np.sqrt(u + v)
        return wind_speed

    def calculate_wind_direction(self, eastward, northward):
        """Calculate met wind direction from component wind vectors."""
        u = eastward
        v = northward
        wind_direction = 180/np.pi * np.arctan2(-u, -v)
        return wind_direction

    def _connect_erddap(self, server="https://cgsn-dashboard.whoi.edu/erddap",
                        protocol="tabledap"):
        """Connect to the erddap server."""
        self._erddap = ERDDAP(
            server=server,
            protocol=protocol
        )

    def list_datasets(self):
        """Get the available datasets for the ERDDAP server."""
        # First, make the connection
        self._connect_erddap()
        # Next, get the datasets
        datasets = pd.read_csv(self._erddap.get_search_url(
                               search_for=self.station_id,
                               response='csv'))['Dataset ID']
        return datasets

    def get_dataset(self, dataset):
        """Get the data for specified datasets."""
        # First, have to re-establish the erddap connection
        self._connect_erddap()

        # Next, get the data for a dataset
        self._erddap.dataset_id = dataset

        # Only want the variables with standard names
        variables = self._erddap.get_var_by_attr(
            standard_name=lambda v: v is not None)
        self._erddap.variables = variables

        # Limit the data request to the current deployment
        self._erddap.constraints = {
            'deploy_id=': self.deploy_id,
        #    'time>=': self.startTime.strftime('%Y-%m-%dT%H:%M:%SZ')
        }

        try:
            # Download the data
            data = self._erddap.to_pandas(
                index_col='time (UTC)',
                parse_dates=True
            )

            # Sometimes it just returns an empty dataframe instead of an error
            if data.size == 0:
                data = self._create_empty_dataset()

        except:
            # If there is no available data in the requested time window, need
            # to create an empty dataframe of the data
            data = self._create_empty_dataset()

        # Return the dataset data
        return data

    def process_METBK_data(self, df, freq='10T'):
        """Process the METBK into the correct format and values for NDBC."""
        # Resample the data
        df_binned = df.resample(freq).mean()

        # Check that barometric pressure
        if 'barometric_pressure (mbar)' in df_binned.columns:
            # Adjust the barometric pressure to sea-level
            df_binned['sea_level_pressure (hPa)'] = self.adjust_pressure_to_sea_level(
                df_binned['barometric_pressure (mbar)'],
                df_binned['air_temperature (degrees_Celsius)'], 4.05)
        else:
            df_binned['sea_level_pressure (hPa)'] = np.nan

        # Check that the wind vector components are in the dataframe
        if 'eastward_wind_velocity (m s-1)' in df_binned.columns:
            # Calculate the wind speed
            df_binned['wind speed (m/s)'] = self.calculate_wind_speed(
                df_binned['eastward_wind_velocity (m s-1)'],
                df_binned['northward_wind_velocity (m s-1)'])

            # Calculate the wind direction
            df_binned['wind direction'] = self.calculate_wind_direction(
                df_binned['eastward_wind_velocity (m s-1)'],
                df_binned['northward_wind_velocity (m s-1)'])
            df_binned['wind direction'] = df_binned["wind direction"].apply(
                lambda x: x+360 if x < 0 else x)

            # Don't need cardinal direction -> want direction in degrees
            # df_binned["wind direction"] = df_binned["wind direction"].apply(
            #   lambda x: self.get_cardinal_direction(np.round(x, decimals=2)))
        else:
            df_binned['wind speed (m/s)'] = np.nan
            df_binned['wind direction'] = np.nan

        # Return the processed data
        return df_binned

    def process_WAVSS_data(self, df, freq='10T'):
        """Much simpler function for processing the WAVSS data."""
        # Resample the data
        df_binned = df.resample(freq).mean()

        # Return the data
        return df_binned

    def _create_empty_dataset(self):
        """
        Create a dataset of all nans if there is no data available for
        the requested dataset in the given time period.
        """
        # Get the units for the corresponding variables
        info_url = self._erddap.get_info_url(
            dataset_id=self._erddap.dataset_id,
            response='csv')
        info = pd.read_csv(info_url)
        units = info[info['Attribute Name'] == 'units']

        # Now, add the units to the variable names
        columns = []
        for var in self._erddap.variables:
            unit = units[units['Variable Name'] == var]['Value'].values
            if len(unit) == 0:
                columns.append(f'{var}')
            elif var == 'time':
                pass
            else:
                columns.append(f'{var} ({unit[0]})')

        # Create an array of nans to fill out the empty dataframe
        empty_array = np.empty((2, len(columns)))
        empty_array[:] = np.nan

        # Put the empty array into a dataframe
        empty_df = pd.DataFrame(data=empty_array, columns=columns,
                                index=[self.startTime, self.now])
        empty_df.index.name = 'time (UTC)'

        return empty_df

    def process_datasets(self, datasets):
        """Process the data for individual datasets."""
        self.datasets = datasets

        # Get the data for the individual datasets
        for dset in self.datasets.keys():
            self.datasets.update({dset: self.get_dataset(dset)})

        # Process the data
        for dset in self.datasets.keys():
            if 'METBK' in dset:
                self.datasets[dset] = self.process_METBK_data(self.datasets[dset])
            else:
                self.datasets[dset] = self.process_WAVSS_data(self.datasets[dset])

        # Add a header to the data in the datasets
        for key in self.datasets.keys():
            header = key.split('-', 2)[-1]
            for col in self.datasets.get(key).columns:
                self.datasets.get(key).rename(
                    columns={col: ' '.join((header, col))}, inplace=True)

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

# =============================================================================
# Set the name and field mapping of the ERDDAP dataset field names to the NDBC
# xml data tags.


# Pioneer Central Surface Mooring
cp01cnsm_data_map = {
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

cp01cnsm_name_map = {
    'atmp1': 'METBK-01-1 air_temperature (degrees_Celsius)',
    'atmp2': 'METBK-02-1 air_temperature (degrees_Celsius)',
    'baro1': 'METBK-01-1 sea_level_pressure (hPa)',
    'baro2': 'METBK-02-1 sea_level_pressure (hPa)',
    'lwrad': 'METBK-01-1 longwave_irradiance (W m-2)',
    'rrh':   'METBK-01-1 relative_humidity (percent)',
    'srad1': 'METBK-01-1 shortwave_irradiance (W m-2)',
    'wspd1': 'METBK-01-1 wind speed (m/s)',
    'wspd2': 'METBK-02-1 wind speed (m/s)',
    'wdir1': 'METBK-01-1 wind direction',
    'wdir2': 'METBK-02-1 wind direction',
    'wtmp1': 'METBK-01-1 sea_surface_temperature (degrees_Celsius)',
    'wtmp2': 'METBK-02-1 sea_surface_temperature (degrees_Celsius)',
    'tp001': 'METBK-01-1 sea_surface_temperature (degrees_Celsius)',
    'tp002': 'METBK-02-1 sea_surface_temperature (degrees_Celsius)',
    'sp001': 'METBK-01-1 psu (PSU)',
    'sp002': 'METBK-02-1 psu (PSU)',
    'dompd': 'WAVSS-01-1 significant_wave_period',
    'mwdir': 'WAVSS-01-1 mean_wave_direction',
    'wvhgt': 'WAVSS-01-1 significant_wave_height',
}

# Pioneer Inshore Surface Mooring
cp03issm_data_map = {
    # Data variables
    'atmp1': -9999,
    'baro1': -9999,
    'lwrad': -9999,
    'rrh': -9999,
    'srad1': -9999,
    'wspd1': -9999,
    'wdir1': -9999,
    'wtmp1': -9999,
    'tp001': -9999,
    'sp001': -9999,
    # Fixed constants
    'dp001': 0.95,
    'fm64iii': 830,
    'fm64k1': 7,
    'fm64k2': 1
}

cp03issm_name_map = {
    'atmp1': 'METBK-01-1 air_temperature (degrees_Celsius)',
    'baro1': 'METBK-01-1 sea_level_pressure (hPa)',
    'lwrad': 'METBK-01-1 longwave_irradiance (W m-2)',
    'rrh':   'METBK-01-1 relative_humidity (percent)',
    'srad1': 'METBK-01-1 shortwave_irradiance (W m-2)',
    'wspd1': 'METBK-01-1 wind speed (m/s)',
    'wdir1': 'METBK-01-1 wind direction',
    'wtmp1': 'METBK-01-1 sea_surface_temperature (degrees_Celsius)',
    'tp001': 'METBK-01-1 sea_surface_temperature (degrees_Celsius)',
    'sp001': 'METBK-01-1 psu (PSU)',
}

# Pioneer Offshore Surface Mooring
cp04ossm_data_map = {
    # Data variables
    'atmp1': -9999,
    'baro1': -9999,
    'lwrad': -9999,
    'rrh': -9999,
    'srad1': -9999,
    'wspd1': -9999,
    'wdir1': -9999,
    'wtmp1': -9999,
    'tp001': -9999,
    'sp001': -9999,
    # Fixed constants
    'dp001': 0.95,
    'fm64iii': 830,
    'fm64k1': 7,
    'fm64k2': 1
}

cp04ossm_name_map = {
    'atmp1': 'METBK-01-1 air_temperature (degrees_Celsius)',
    'baro1': 'METBK-01-1 sea_level_pressure (hPa)',
    'lwrad': 'METBK-01-1 longwave_irradiance (W m-2)',
    'rrh':   'METBK-01-1 relative_humidity (percent)',
    'srad1': 'METBK-01-1 shortwave_irradiance (W m-2)',
    'wspd1': 'METBK-01-1 wind speed (m/s)',
    'wdir1': 'METBK-01-1 wind direction',
    'wtmp1': 'METBK-01-1 sea_surface_temperature (degrees_Celsius)',
    'tp001': 'METBK-01-1 sea_surface_temperature (degrees_Celsius)',
    'sp001': 'METBK-01-1 psu (PSU)',
}

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
    'atmp1': 'METBK-01-1 air_temperature (degrees_Celsius)',
    'atmp2': 'METBK-02-1 air_temperature (degrees_Celsius)',
    'baro1': 'METBK-01-1 sea_level_pressure (hPa)',
    'baro2': 'METBK-02-1 sea_level_pressure (hPa)',
    'lwrad': 'METBK-01-1 longwave_irradiance (W m-2)',
    'rrh':   'METBK-01-1 relative_humidity (percent)',
    'srad1': 'METBK-01-1 shortwave_irradiance (W m-2)',
    'wspd1': 'METBK-01-1 wind speed (m/s)',
    'wspd2': 'METBK-02-1 wind speed (m/s)',
    'wdir1': 'METBK-01-1 wind direction',
    'wdir2': 'METBK-02-1 wind direction',
    'wtmp1': 'METBK-01-1 sea_surface_temperature (degrees_Celsius)',
    'wtmp2': 'METBK-02-1 sea_surface_temperature (degrees_Celsius)',
    'tp001': 'METBK-01-1 sea_surface_temperature (degrees_Celsius)',
    'tp002': 'METBK-02-1 sea_surface_temperature (degrees_Celsius)',
    'sp001': 'METBK-01-1 psu (PSU)',
    'sp002': 'METBK-02-1 psu (PSU)',
    'dompd': 'WAVSS-01-1 significant_wave_period',
    'mwdir': 'WAVSS-01-1 mean_wave_direction',
    'wvhgt': 'WAVSS-01-1 significant_wave_height',
}

# =============================================================================
if __name__ == '__main__':
    # Data directory path
    #dataPath = '/home/ooiuser/ndbc/data'
    dataPath = "data"
    # Get the last 24-hours of data
    currentTime = dt.datetime.now(tz=pytz.UTC)
    startTime = currentTime - dt.timedelta(hours=4)
    timestamp = currentTime.strftime('%Y%m%d%H%M%S')

    # =========================================================================
    # Initialize the CP01CNSM BUOY dataset
    # Pioneer - NES Array has been deprecated
    #CNSM = NDBC('CP01CNSM', 'D0016', '44076', currentTime, startTime,
    #            cp01cnsm_data_map, cp01cnsm_name_map)

    # Get the data for the Buoy
    #datasets = {
    #    'CP01CNSM-BUOY-METBK-01-1': None,
    #    'CP01CNSM-BUOY-METBK-02-1': None,
    #    'CP01CNSM-BUOY-WAVSS-01-1': None,
    #}

    # Process the datasets
    #CNSM.process_datasets(datasets)

    # Merge the datasets
    #CNSM.data = pd.DataFrame()
    #for dset in CNSM.datasets.keys():
    #    CNSM.data = CNSM.data.merge(CNSM.datasets.get(dset), how='outer',
    #                                left_index=True, right_index=True)

    # Create xml data
    #CNSM.xml = CNSM.parse_data_to_xml(CNSM.data)

    # Write the data out to a file
    #with open(f'{dataPath}/{CNSM.WMO}_{timestamp}.xml', 'w') as file:
    #    for line in CNSM.xml:
    #        file.write(f'{line}\n')

    # =========================================================================
    # Initialize the CP03ISSM BUOY Dataset
    # Pioneer - NES array has been deprecated
    #ISSM = NDBC('CP03ISSM', 'D0015', '44075', currentTime, startTime,
    #            cp03issm_data_map, cp03issm_name_map)

    # Get the data for the Buoy
    #datasets = {
    #    'CP03ISSM-BUOY-METBK-01-1': None,
    #}

    # Process the datasets
    #ISSM.process_datasets(datasets)

    # Merge the datasets
    #ISSM.data = pd.DataFrame()
    #for dset in ISSM.datasets.keys():
     #   ISSM.data = ISSM.data.merge(ISSM.datasets.get(dset), how='outer',
     #                               left_index=True, right_index=True)

    # Create xml data
    #ISSM.xml = ISSM.parse_data_to_xml(ISSM.data)

    # Write the data out to a file
    #with open(f'{dataPath}/{ISSM.WMO}_{timestamp}.xml', 'w') as file:
    #    for line in ISSM.xml:
    #        file.write(f'{line}\n')

    # =========================================================================
    # Initialize the CP04OSSM BUOY Dataset
    # Pioneer - NES array has been deprecated
    #OSSM = NDBC('CP04OSSM', 'D0015', '44077', currentTime, startTime,
    #            cp04ossm_data_map, cp04ossm_name_map)

    # Get the data for the Buoy
    #datasets = {
    #    'CP04OSSM-BUOY-METBK-01-1': None,
    #}

    # Process the datasets
    #OSSM.process_datasets(datasets)

    # Merge the datasets
    #OSSM.data = pd.DataFrame()
    #for dset in OSSM.datasets.keys():
    #    OSSM.data = OSSM.data.merge(OSSM.datasets.get(dset), how='outer',
    #                                left_index=True, right_index=True)

    # Create xml data
    #OSSM.xml = OSSM.parse_data_to_xml(OSSM.data)

    # Write the data out to a file
    #with open(f'{dataPath}/{OSSM.WMO}_{timestamp}.xml', 'w') as file:
    #    for line in OSSM.xml:
    #        file.write(f'{line}\n')

    # =========================================================================
    # Initialize the GI01SUMO BUOY dataset
    SUMO = NDBC('GI01SUMO', 'D0009', '44078', currentTime, startTime,
                gi01sumo_data_map, gi01sumo_name_map)

    # Get the data for the Buoy
    datasets = {
        'GI01SUMO-BUOY-METBK-01-1': None,
        'GI01SUMO-BUOY-METBK-02-1': None,
        'GI01SUMO-BUOY-WAVSS-01-1': None,
    }

    # Process the datasets
    SUMO.process_datasets(datasets)

    # Merge the datasets
    SUMO.data = pd.DataFrame()
    for dset in SUMO.datasets.keys():
        SUMO.data = SUMO.data.merge(SUMO.datasets.get(dset), how='outer',
                                    left_index=True, right_index=True)
        
    # Filter the data for the last four hours
    SUMO.data = SUMO.data.loc[slice(startTime, currentTime)]

    # Filter out any missing data
    SUMO.data = SUMO.data.dropna(how='all')

    # Create xml data
    SUMO.xml = SUMO.parse_data_to_xml(SUMO.data)

    # Write the data out to a file
    with open(f'{dataPath}/{SUMO.WMO}_{timestamp}.xml', 'w') as file:
        for line in SUMO.xml:
            file.write(f'{line}\n')


