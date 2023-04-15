# -*- coding: utf-8 -*-
"""
Created on Tue Nov 22 14:59:53 2022

@author: frank
"""
import pandas as pd
import sodapy
from sodapy import Socrata 
import os
from functools import reduce
base_path = r'C:\Users\frank\OneDrive\Documents\GitHub\final-project-fbjoseph123\Data'
client = Socrata('chronicdata.cdc.gov', None)
results = client.get("swc5-untb", limit=188457)
places_data = pd.DataFrame.from_records(results)
path = os.path.join(base_path,'SVI2020_US.csv')
SVI_data = pd.read_csv(path)
path2 = os.path.join(base_path,'svi_interactive_map.csv')
SVI_dashboard = pd.read_csv(path2)
path3 = os.path.join(base_path,'SDOH_2019_COUNTY_1_0.xlsx')
full_xlsx_file = pd.ExcelFile(path3)
AHRQ_data = pd.read_excel(full_xlsx_file,'Data')
#Unable to pivot places_data from long to wide because of “ValueError: Index contains duplicate entries, 
#cannot reshape”. The typical workaround is to use the aggfunc argument in pd.pivot_table but that results
#in averaging values or summing them which defeats the purpose. Instead the data was subsetted and merged. 
depression_outcomes = places_data[(
    places_data['measure'] == 'Depression among adults aged >=18 years') & (
        places_data['data_value_type'] == 'Age-adjusted prevalence')]
depression_outcomes = depression_outcomes.rename(columns={'data_value':'depression_percentage'})   
obesity_outcomes = places_data[(
    places_data['measure'] == 'Obesity among adults aged >=18 years') & (
        places_data['data_value_type'] == 'Age-adjusted prevalence')]
obesity_outcomes = obesity_outcomes.rename(columns={'data_value':'obesity_percentage'})
cancer_outcomes = places_data[(
    places_data['measure'] == 'Cancer (excluding skin cancer) among adults aged >=18 years') & (
        places_data['data_value_type'] == 'Age-adjusted prevalence')] 
cancer_outcomes = cancer_outcomes.rename(columns={'data_value':'cancer_percentage'})
heart_disease_outcomes = places_data[(
    places_data['measure'] == 'Coronary heart disease among adults aged >=18 years') & (
        places_data['data_value_type'] == 'Age-adjusted prevalence')]     
heart_disease_outcomes = heart_disease_outcomes.rename(columns={'data_value':'heart_disease_percentage'})
outcomes_list = [depression_outcomes, obesity_outcomes, cancer_outcomes, heart_disease_outcomes]
health_outcomes = reduce(
    lambda left,right: pd.merge(left,right, on=[
        'year','stateabbr','statedesc','locationname','datasource','totalpopulation',
        'categoryid','datavaluetypeid']),outcomes_list)
health_outcomes = health_outcomes[['statedesc','locationname','locationid_x',
                                  'depression_percentage','totalpopulation','obesity_percentage',
                                  'cancer_percentage','heart_disease_percentage','geolocation_x']]
health_outcomes = health_outcomes.T.groupby(level=0).first().T
new_health_outcomes_columns = {'statedesc':'state','locationname':'county','locationid_x':'fips_code'}
health_outcomes.rename(columns=new_health_outcomes_columns, inplace=True)
health_outcomes[['state','county']] = health_outcomes[['state','county']].astype(str)
health_outcomes['fips_code'] = health_outcomes['fips_code'].astype(int)

SVI_dashboard['COUNTY'] = SVI_dashboard['COUNTY'].str.replace('County','')
SVI_dashboard = SVI_dashboard[['STATE','FIPS','COUNTY','SPL_THEME1','RPL_THEME1','SPL_THEME2','RPL_THEME2', 
                               'SPL_THEME3', 'RPL_THEME3', 'SPL_THEME4', 'RPL_THEME4', 'SPL_THEMES', 
                               'RPL_THEMES']]
new_SVI_column_names = {'SPL_THEME1':'socioeconomic_sum','RPL_THEME1':'socioeconomic_score',
                        'SPL_THEME2':'household_characteristics_sum',
                        'RPL_THEME2':'household_characteristics_score',
                        'SPL_THEME3':'racial_minority_status_sum',
                        'RPL_THEME3':'racial_minority_status_score','SPL_THEME4':'housing_and_transportation_sum',
                        'RPL_THEME4':'housing_and_transportation_score','SPL_THEMES':'svi_sum',
                        'RPL_THEMES':'svi_score','STATE':'state','COUNTY':'county','FIPS':'fips_code'}
SVI_dashboard.rename(columns=new_SVI_column_names, inplace=True)

AHRQ_data['COUNTY'] = AHRQ_data['COUNTY'].str.replace('County','')
AHRQ_data = AHRQ_data[['COUNTYFIPS','STATE','COUNTY','CRE_RATE_RISK0','CRE_RATE_RISK12','CRE_RATE_RISK3']]
new_AHRQ_column_names = {'CRE_RATE_RISK0':'zero_risk_factors_percent_of_individuals',
                         'CRE_RATE_RISK12':'one_to_two_risk_factors_percent_of_individuals','CRE_RATE_RISK3':
                             'three_or_more_risk_factors_percent_of_individuals','COUNTYFIPS':'fips_code',
                             'STATE':'state','COUNTY':'county'}
AHRQ_data.rename(columns=new_AHRQ_column_names, inplace=True)

predictors_and_outcomes = [health_outcomes, SVI_dashboard, AHRQ_data]
final_merge = reduce(
    lambda left,right: pd.merge(left,right, on=['fips_code']),predictors_and_outcomes)
final_merge = final_merge[['state','county','fips_code','cancer_percentage','depression_percentage',
                               'heart_disease_percentage','obesity_percentage','socioeconomic_sum',
                               'socioeconomic_score','household_characteristics_sum',
                               'household_characteristics_score','racial_minority_status_sum',
                               'racial_minority_status_score','housing_and_transportation_sum',
                               'housing_and_transportation_score','svi_sum','svi_score',
                               'zero_risk_factors_percent_of_individuals','one_to_two_risk_factors_percent_of_individuals',
                               'three_or_more_risk_factors_percent_of_individuals','geolocation_x']]
final_merge['fips_code'] = final_merge['fips_code'].apply('="{}"'.format)
final_merge.to_csv('final_merge2.csv', index=False)
#https://stackoverflow.com/questions/26521266/using-pandas-to-pd-read-excel-for-multiple-worksheets-of-the-same-workbook
#https://stackoverflow.com/questions/23668427/pandas-three-way-joining-multiple-dataframes-on-columns
#https://stackoverflow.com/questions/51778480/remove-certain-string-from-entire-column-in-pandas-dataframe
#https://github.com/bokeh/bokeh/issues/4796
#https://stackoverflow.com/questions/40938312/python-pandas-refer-to-column-with
#https://github.com/bokeh/bokeh/issues/4796