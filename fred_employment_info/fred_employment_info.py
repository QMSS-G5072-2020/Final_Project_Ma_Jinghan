import requests
import os
import pandas as pd
import json
import matplotlib.pyplot as plt
import re
import numpy as np
from requests.exceptions import HTTPError
import os


def fred_function(**kwargs):
    """
    Using this function can collect data from FRED API, check the status of the request the server returns and inform the user of any errors.
    Besides, it can also return a dataset to collect the series of data we need and transform them into a dataframe.

    Parameters
    ----------
    api_key : a string to put your API key
    series_id : a string of the id for a series. Such as 'GNPC96' for quarterly 'Real Gross National Product';'GNPCA'for annualy 'Real Gross National Product'.
    realtime_start : YYYY-MM-DD formatted string, optional, default: today's date
    realtime_end : YYYY-MM-DD formatted string, optional, default: today's date
    observation_start : YYYY-MM-DD formatted string, optional, default: 1776-07-04 (earliest available)
    observation_end : YYYY-MM-DD formatted string, optional, default: 9999-12-31 (latest available)

    Returns
    -------
    A dataframe of the data collected from API.

    Examples
    --------
    >>> fred_function(api_key=api_key,series_id='GNPCA',observation_start='1950-01-01')
    <DataFrame>
       It contains annually Real Gross National Product value from 1950-01-01 to today.
    """

    params = kwargs
    try:
        r = requests.get('https://api.stlouisfed.org/fred/series/observations?file_type=json',params = params)
        r.raise_for_status()
    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')
    except Exception as err:
        print(f'Other error occurred: {err}')
    else:
        fred_json = r.json()
        fred_json_df = pd.DataFrame(fred_json['observations'])
        return fred_json_df


def find_series(**kwargs):
    """
    Using this function can collect series id from FRED API in the form of a dataframe.

    Parameters
    ----------
    api_key : a string to put your API key
    category_id : a integer of the id for a category. Such as '32243' for job openings category.

    Returns
    -------
    A dataframe of series id collected from API.

    Examples
    --------
    >>> api_key=api_key
    >>> category_id='32243'
    >>> find_series(api_key=api_key,category_id='32243')
    <Data Frame>
    """

    params = kwargs
    r = requests.get('https://api.stlouisfed.org/fred/category/series?file_type=json',params = params)
    fred_json = r.json()
    fred_json_df = pd.DataFrame(fred_json['seriess'])
    return fred_json_df


def observations_table_payroll(api_key):
    """
    Using this function can give the payroll for 6 industries.

    Parameters
    ----------
    api_key : a string to put your API key

    Returns
    -------
    A dataframe with column names : 'Professional & Business', 'Construction', 'Financial Activities', 'Manufacturing','Service Providing','Trade Transportation and Utilities'.
    Index name : month from 2002-04-01 till present


    Examples
    --------
    >>> api_key=api_key
    >>> observations_table_payroll(api_key=api_key)
    <Datefram> with columns 'Professional & Business', 'Construction', 'Financial Activities', 'Manufacturing','Service Providing','Trade Transportation and Utilities'
    	date		Professional & Business	Construction	Financial Activities	Manufacturing	Service Providing	Trade Transportation and Utilities
    0	2002-04-01	16044.497				6728.243		7931.101				15385.275		86443.870			25453.870
    1	2002-05-01	16055.289				6721.990		7934.215				15344.723		86477.847			25436.681

    """
    params = {'api_key':api_key,'category_id':32250}
    r_npp = requests.get('https://api.stlouisfed.org/fred/category/series?file_type=json',params = params)
    fred_json_npp = r_npp.json()
    fred_json_df_npp = pd.DataFrame(fred_json_npp['seriess'])
    groupdata_npp =[i for i in fred_json_df_npp['id'] if re.search(r'NPP(MNF|CON|SPT|BUS|TTU|FIN)', i)]
    params = {'api_key':api_key,'series_id':'NPPMNF','observation_start':'2002-04-01'}
    rr = requests.get('https://api.stlouisfed.org/fred/series/observations?file_type=json',params = params)
    fred_json = rr.json()
    fred_json_df= pd.DataFrame(fred_json['observations'])
    df = fred_json_df[['date']]
    for i in groupdata_npp:
        series_number = i
        params = {'api_key':api_key,'series_id':series_number,'observation_start':'2002-04-01'}
        r = requests.get('https://api.stlouisfed.org/fred/series/observations?file_type=json',params = params)
        fred_json = r.json()
        fred_json_df = pd.DataFrame(fred_json['observations'])
        fred_json_df_select = fred_json_df[['value']]
        df = pd.concat([df, fred_json_df_select], axis=1)
    df.columns=['date', 'Professional & Business', 'Construction', 'Financial Activities', 'Manufacturing','Service Providing','Trade Transportation and Utilities']
    return df


def find_series_jobopenings(api_key):
    """
    Using this function can collect the series id of 6 industries jop opening.
    Parameters
    ----------
    api_key : a string to put your API key

    Returns
    -------
    A list of series id collected from API.

    Examples
    --------
    >>> api_key=api_key
    >>> find_series_jobopenings(api_key=api_key)
    ['JTS2300JOL',
     'JTS3000JOL',
     'JTS4000JOL',
     'JTS540099JOL',
     'JTS7000JOL',
     'JTU510099JOL']
    """
    params = {'api_key':api_key}
    r = requests.get('https://api.stlouisfed.org/fred/category/series?file_type=json&category_id=32243',params = params)
    fred_json = r.json()
    fred_json_df = pd.DataFrame(fred_json['seriess'])
    groupdata =[i for i in fred_json_df['id'] if re.search(r'JT(S3000|S2300|S7000|S540099|S4000|U510099)JOL', i)]
    return groupdata


def observations_table_jobopenings(api_key):
    """
    Using this function can collect 6 industries jop opening times for each month.
    Parameters
    ----------
    api_key : a string to put your API key
    series_id : a string of the id for a series. Such as 'GNPC96' for quarterly 'Real Gross National Product';'GNPCA'for annualy 'Real Gross National Product'.
    realtime_start : YYYY-MM-DD formatted string, optional, default: today's date
    realtime_end : YYYY-MM-DD formatted string, optional, default: today's date
    observation_start : YYYY-MM-DD formatted string, optional, default: 2000-12-01 (earliest available)
    observation_end : YYYY-MM-DD formatted string, optional, default: 9999-12-31 (latest available)

    Returns
    -------
    A dataframe of jop openings for 6 induestries collected from API.

    Examples
    --------
    >>> api_key=api_key
    >>> observations_table_jobopenings(api_key=api_key)
    <Datefram> with columns 'Construction', 'Manufacturing','Trade Transportation and Utilities', 'Professional & Business','Service Providing', 'Financial Activities'
    	date		Construction	Manufacturing	Trade Transportation and Utilities	Professional & Business	Service Providing	Financial Activities
    0	2000-12-01	172.0			374.0			766.0								871.0					566.0				250.0
    1	2001-01-01	223.0			491.0			965.0								888.0					654.0				298.0
    """
    params =  {'api_key':api_key,'category_id':32243}
    r = requests.get('https://api.stlouisfed.org/fred/category/series?file_type=json',params = params)
    fred_json = r.json()
    fred_json_df = pd.DataFrame(fred_json['seriess'])
    groupdata_jts =[i for i in fred_json_df['id'] if re.search(r'JT(S3000|S2300|S7000|S540099|S4000|U510099)JOL', i)]
    params = {'api_key':api_key,'series_id':'JTS2300JOL','observation_start':'2000-12-01'}
    rr = requests.get('https://api.stlouisfed.org/fred/series/observations?file_type=json',params = params)
    fred_json = rr.json()
    fred_json_df= pd.DataFrame(fred_json['observations'])
    df_jol = fred_json_df[['date']]
    for i in groupdata_jts:
        series_number = i
        params = {'api_key':api_key,'series_id':series_number,'observation_start':'2000-12-01'}
        r = requests.get('https://api.stlouisfed.org/fred/series/observations?file_type=json',params = params)
        fred_json = r.json()
        fred_json_df = pd.DataFrame(fred_json['observations'],dtype=np.float)
        fred_json_df_select = fred_json_df[['value']]
        df_jol = pd.concat([df_jol, fred_json_df_select], axis=1)
    df_jol.columns=['date', 'Construction', 'Manufacturing','Trade Transportation and Utilities', 'Professional & Business','Service Providing', 'Financial Activities']
    return df_jol


def observations_table_hire(api_key):
    """
    Using this function can collect 6 industries monthly hired number of people.
    Parameters
    ----------
    api_key : a string to put your API key.

    Returns
    -------
    A dataframe of hired people for 6 induestries collected from API.

    Examples
    --------
    >>> api_key=api_key
    >>> observations_table_hire(api_key=api_key)
    <Datefram> with columns 'Construction', 'Manufacturing','Trade Transportation and Utilities', 'Professional & Business','Service Providing', 'Financial Activities'
    			date			Construction	Manufacturing	Trade Transportation and Utilities	Professional & Business	Service Providing	Financial Activities
    0			2000-12-01		414				504				1276								966						890					193
    1			2001-01-01		495				498				1181								1129					919					350

    """
    params =  {'api_key':api_key,'category_id':32245}
    r_hire = requests.get('https://api.stlouisfed.org/fred/category/series?file_type=json',params = params)
    fred_json_hire = r_hire.json()
    fred_json_df_hire = pd.DataFrame(fred_json_hire['seriess'])
    groupdata_hire =[i for i in fred_json_df_hire['id'] if re.search(r'JT(S3000|S2300|S7000|S540099|S4000|U510099)HIL', i)]
    params = {'api_key':api_key,'series_id':'JTS2300HIL','observation_start':'2000-12-01'}
    rr = requests.get('https://api.stlouisfed.org/fred/series/observations?file_type=json',params = params)
    fred_json = rr.json()
    fred_json_df= pd.DataFrame(fred_json['observations'],dtype=np.float)
    df_hire = fred_json_df[['date']]

    for i in groupdata_hire:
        series_number = i
        params = {'api_key':api_key,'series_id':series_number,'observation_start':'2000-12-01'}
        r = requests.get('https://api.stlouisfed.org/fred/series/observations?file_type=json',params = params)
        fred_json = r.json()
        fred_json_df = pd.DataFrame(fred_json['observations'],dtype=np.float)
        fred_json_df_select = fred_json_df[['value']]
        df_hire = pd.concat([df_hire, fred_json_df_select], axis=1)
    df_hire.columns=['date', 'Construction', 'Manufacturing','Trade Transportation and Utilities', 'Professional & Business','Service Providing', 'Financial Activities']
    return df_hire


def observations_table_layoff(api_key):
    """
    Using this function can collect 6 industries monthly layoff and discharged number of people.

    Parameters
    ----------
    api_key : a string to put your API key

    Returns
    -------
    A dataframe of hired people for 6 induestries collected from API.

    Examples
    --------
    >>> api_key=api_key
    >>> observations_table_hire(api_key=api_key)
    <Datefram> with columns 'Construction', 'Manufacturing','Trade Transportation and Utilities', 'Professional & Business','Service Providing', 'Financial Activities'
    	date		Construction	Manufacturing	Trade Transportation and Utilities	Professional & Business	Service Providing	Financial Activities
    0	2000-12-01	384.0			232.0			678.0								28.0					504.0				199.0
    1	2001-01-01	348.0			304.0			813.0								114.0					726.0				265.0
    """
    api_key=api_key
    params =  {'api_key':api_key,'category_id':32248}
    r_layoff = requests.get('https://api.stlouisfed.org/fred/category/series?file_type=json',params = params)
    fred_json_layoff = r_layoff.json()
    fred_json_df_layoff = pd.DataFrame(fred_json_layoff['seriess'])
    groupdata_layoff =[i for i in fred_json_df_layoff['id'] if re.search(r'JTU(3000|2300|7000|540099|4000|510099)LDL', i)]
    params = {'api_key':api_key,'series_id':'JTS2300HIL','observation_start':'2000-12-01'}
    rr = requests.get('https://api.stlouisfed.org/fred/series/observations?file_type=json',params = params)
    fred_json = rr.json()
    fred_json_df= pd.DataFrame(fred_json['observations'])
    df_layoff = fred_json_df[['date']]
    for i in groupdata_layoff:
        series_number = i
        params = {'api_key':api_key,'series_id':series_number,'observation_start':'2000-12-01'}
        r = requests.get('https://api.stlouisfed.org/fred/series/observations?file_type=json',params = params)
        fred_json = r.json()
        fred_json_df = pd.DataFrame(fred_json['observations'],dtype=np.float)
        fred_json_df_select = fred_json_df[['value']]
        df_layoff = pd.concat([df_layoff, fred_json_df_select], axis=1)
    df_layoff.columns=['date', 'Construction', 'Manufacturing','Trade Transportation and Utilities', 'Professional & Business','Service Providing', 'Financial Activities']
    return df_layoff

def combination_information(api_key,condition):
    """
    Using this function can collect 6 industries overall descriptive statistics information.
    Including: maximum, average, minimum, count, standard deviation, 25%, 50% and 75%

    Parameters
    ----------
    api_key : a string to put your API key
    condition : choose from "max","mean","min","count","std","25%","50%","75%", it can return maximum, average, minimum, count, standard deviation, 25%, 50% and 75%

    Returns
    -------
    A dataframe with index names : 'Construction', 'Manufacturing','Trade Transportation and Utilities', 'Professional & Business','Service Providing', 'Financial Activities'.
    Column name : 'job openings','hire','layoff'


    Examples
    --------
    >>> api_key=api_key
    >>> combination_information(api_key=api_key,condition='mean')
    <Datefram> with index 'Construction', 'Manufacturing','Trade Transportation and Utilities', 'Professional & Business','Service Providing', 'Financial Activities'
    										job openings			hire	layoff
    	Construction						156.0					391.0	236.0
    	Manufacturing						298.0					323.0	156.0
    	Trade Transportation and Utilities	819.0					1041.0	375.0
    	Professional & Business				853.0					960.0	68.0
    	Service Providing					581.0					885.0	414.0
    	Financial Activities				278.0					208.0	316.0
    """
    api_key=api_key
    if condition in ["mean","max","min","25%","50%","75%","count","std"]:
        hire=observations_table_hire(api_key).describe()
        a_hire = round(hire.loc[[condition]])
        jol=observations_table_jobopenings(api_key).describe()
        a_jol = round(jol.loc[[condition]])
        layoff=observations_table_layoff(api_key).describe()
        a_layoff = round(layoff.loc[[condition]])
        a_summary=pd.concat([a_jol,a_hire,a_layoff], ignore_index=True)
        a_summary.index = ['job openings','hire','layoff']
        a_t = a_summary.transpose()
        return a_t
    else:
        return print('Warning: Please reassign the condition from "mean","max","min","25%","50%","75%","count","std"')


def rankings(dataframe,method):
    """
    Using this function can give the ranking for 6 industries using the number of the their job openings, hire, layoff and discharges.
    ranging from 1 to 6. Using max or min method to rank.

    Parameters
    ----------
    api_key : a string to put your API key
    method : choose from "max","min".

    Returns
    -------
    A dataframe with column names : 'Construction', 'Manufacturing','Trade Transportation and Utilities', 'Professional & Business','Service Providing', 'Financial Activities'.
    Index name : 'job openings','hire','layoff'


    Examples
    --------
    >>> api_key=api_key
    >>> mean = combination_information(api_key=api_key,condition='mean')
    >>> rankings(mean,"max")
    <Datefram> with columns 'Construction', 'Manufacturing','Trade Transportation and Utilities', 'Professional & Business','Service Providing', 'Financial Activities'
    										job openings			hire	layoff
    	Construction						156.0					391.0	236.0
    	Manufacturing						298.0					323.0	156.0
    	Trade Transportation and Utilities	819.0					1041.0	375.0
    	Professional & Business				853.0					960.0	68.0
    	Service Providing					581.0					885.0	414.0
    	Financial Activities				278.0					208.0	316.0

    """
    if method in ['max','min']:
        rank = dataframe.rank(ascending=True,method = method)
        return rank
    else:
        print('Warning: Please reassign method for the function. You can choose from "max" and "min"')
