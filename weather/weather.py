import pandas as pd
import glob, os


class Weather:

    def __init__(self, source_path, destination_path):
        self.source_path = source_path
        self.destination_path = destination_path
        self.weather_df = None
        self.combined_weather_df = None
        self.generate_weather_df()

    def generate_weather_df(self):
        self.read_weather_csv_data_files()

    def read_weather_csv_data_files(self):
        '''
        Read required data columns from csv files source location and concatenate them
        in to a panda dataframe.
        '''
        files = glob.glob(os.path.join(self.source_path, "*.csv"))
        weather_data_csv = [pd.read_csv(f, usecols=["ObservationDate", "ScreenTemperature", "Region"]
                                        , converters={'ScreenTemperature': lambda x: (x.replace('-99', '0')),
                                                      'ObservationDate': lambda x: (pd.Timestamp(x).date())}
                                        ) for f in files]
        weather_data_csv_files = pd.concat(weather_data_csv, ignore_index=True)
        weather_data_csv_files['ScreenTemperature'] = pd.to_numeric(weather_data_csv_files['ScreenTemperature'])
        weather_data_csv_files.style.format({'ScreenTemperature': '{:,.1f}'.format})
        self.save_df_to_parquet_file(weather_data_csv_files)
        return weather_data_csv_files

    def save_df_to_parquet_file(self, weather_data_csv):
        '''
        Save panda dataframe to parquet file to output path
        :param weather_data_csv.
        '''
        output_path = os.path.join(self.destination_path, 'weather_data.parquet')
        weather_data_csv.to_parquet(output_path)
        self.load_parquet_file_to_df()

    def load_parquet_file_to_df(self):
        '''
        Load data to panda dataframe from parquet file
        '''
        self.weather_df = pd.read_parquet(self.destination_path)

    def hottest_day_date(self):
        '''
        Calculate and hottest day date
        :return: series: pandas series returning hottest day date
        '''
        grouped_by_date = self.weather_df.groupby(['ObservationDate'], as_index=False)
        average_temperature_for_dates = grouped_by_date.mean()
        hottest_day_date = average_temperature_for_dates[['ObservationDate']].loc[
            average_temperature_for_dates['ScreenTemperature'].idxmax()]
        return hottest_day_date

    def hottest_day_average_temperature(self):
        '''
        Calculate and hottest day temperature
        :return: series: pandas series returning hottest day temperature
        '''
        grouped_by_date = self.weather_df.groupby(['ObservationDate'], as_index=False)
        average_temperature_for_dates = grouped_by_date.mean()
        hottest_day_temperature = average_temperature_for_dates[['ScreenTemperature']].loc[
            average_temperature_for_dates['ScreenTemperature'].idxmax()]
        return hottest_day_temperature

    def hottest_day_average_temperature_by_region(self):
        '''
        Calculate and hottest day temperature by region
        :return: series: pandas series returning hottest day by region
        '''
        grouped_by_date_region = self.weather_df.groupby(['ObservationDate', 'Region'], as_index=False)
        average_temperature_for_dates_region = grouped_by_date_region.mean()
        hottest_day_temperature_region = average_temperature_for_dates_region[['ObservationDate', 'Region']].loc[
            average_temperature_for_dates_region['ScreenTemperature'].idxmax()]
        return hottest_day_temperature_region

    def get_results(self):
        '''
        Concatinate all series data to a Pandas dataframe
        :return: dataframe: pandas dataframe returning hottest day date
                            hottest day temperature and hottest day by region
        '''
        pd.set_option('display.max_columns', 3)
        self.combined_weather_df = pd.concat([self.hottest_day_date(), self.hottest_day_average_temperature(),
                                              self.hottest_day_average_temperature_by_region()], sort=True, axis=1,
                                             ignore_index=True)

        self.combined_weather_df.rename(columns={self.combined_weather_df.columns[0]: "hottest_day_date",
                                                 self.combined_weather_df.columns[1]: "hottest_day_temperature",
                                                 self.combined_weather_df.columns[2]: "hottest_day_by_region"}
                                        , inplace=True)
        return self.combined_weather_df
