from unittest import TestCase
from weather import weather
import datetime
from numpy import nan
import os


class TestWeather(TestCase):

    def setUp(self):
        self.source_path = "../tests/data_files/csv_weather_data/"
        self.destination_path = "../tests/data_files/parquet_weather_data/"
        self.weather = weather.Weather(self.source_path, self.destination_path)

    def test_save_df_to_parquet_file(self):
        output_file_path = os.path.join(self.destination_path, 'weather_data.parquet')
        if os.path.exists(output_file_path):
            os.remove(output_file_path)
        self.weather.generate_weather_df()
        assert os.path.exists(output_file_path)

    def test_load_parquet_file_to_df(self):
        self.weather.load_parquet_file_to_df()
        assert not self.weather.weather_df.empty

    def test_hottest_day_date(self):
        assert self.weather.hottest_day_date().ObservationDate == datetime.date(2016, 2, 1)

    def test_hottest_day_average_temperature(self):
        assert self.weather.hottest_day_average_temperature().ScreenTemperature == 9.283333333333333

    def test_hottest_day_average_temperature_by_region(self):
        assert self.weather.hottest_day_average_temperature_by_region().ObservationDate == datetime.date(2016, 2, 1)
        assert self.weather.hottest_day_average_temperature_by_region().Region == 'Yorkshire & Humber'

    def test_get_results(self):
        expected_as_dict = {
            'hottest_day_date': {'ObservationDate': datetime.date(2016, 2, 1), 'Region': nan, 'ScreenTemperature': nan},
            'hottest_day_temperature': {'ObservationDate': nan, 'Region': nan,
                                                'ScreenTemperature': 9.283333333333333},
            'hottest_day_by_region': {'ObservationDate': datetime.date(2016, 2, 1),
                                                          'Region': 'Yorkshire & Humber', 'ScreenTemperature': nan}}
        assert(sorted(self.weather.get_results().to_dict()) == sorted(expected_as_dict))
