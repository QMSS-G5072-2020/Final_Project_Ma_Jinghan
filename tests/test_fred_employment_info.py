from fred_employment_info import __version__
from fred_employment_info import fred_employment_info
import pandas as pd
import pytest
import os




def test_version():
    assert __version__ == '0.1.0'

def test_fred_function():
	df = fred_employment_info.fred_function(api_key=api_key,series_id='GNPCA',observation_start='1950-01-01')
	assert type(df) == pd.core.frame.DataFrame

def test_find_series():
	df = fred_employment_info.find_series(api_key=api_key,category_id='32243')
	assert type(df) == pd.core.frame.DataFrame

def test_observations_table_jobopenings():
	df = fred_employment_info.observations_table_jobopenings(api_key=api_key)
	assert type(df) == pd.core.frame.DataFrame

def test_observations_table_hire():
	df = fred_employment_info.observations_table_hire(api_key=api_key)
	assert type(df) == pd.core.frame.DataFrame

def test_observations_table_layoff():
	df = fred_employment_info.observations_table_layoff(api_key=api_key)
	assert type(df) == pd.core.frame.DataFrame

def test_combination_information():
    df = fred_employment_info.combination_information(api_key=api_key,condition='mean')
    expected = (6, 3)
    actual = df.shape
    assert actual == expected

def test_observations_table_payroll():
	df = fred_employment_info.observations_table_payroll(api_key=api_key)
	assert type(df) == pd.core.frame.DataFrame

def test_find_series_jobopenings():
     expected = ['JTS2300JOL','JTS3000JOL','JTS4000JOL','JTS540099JOL','JTS7000JOL','JTU510099JOL']
     actual = fred_employment_info.find_series_jobopenings(api_key = api_key)
     assert actual == expected
