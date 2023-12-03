import os
import pandas as pd
import requests
from sqlalchemy import create_engine

def datasource1():
    url1 = 'https://raw.githubusercontent.com/washingtonpost/data-2C-beyond-the-limit-usa/main/data/processed/model_state.csv'
    url2 = 'https://raw.githubusercontent.com/washingtonpost/data-2C-beyond-the-limit-usa/main/data/processed/climdiv_state_year.csv'
    
    model_state = pd.read_csv(url1)
    model_state = model_state[['fips','STATE_NAME']]
    
    state_climate = pd.read_csv(url2)
    state_climate = state_climate[(state_climate['year'] >= 2010) & (state_climate['year'] <= 2019)].reset_index(drop=True)
    state_climate = state_climate.drop(state_climate.columns[2], axis=1)
    state_climate = pd.merge(state_climate, model_state, on='fips', how='left')
    return state_climate

def datasource2():
    dataset = 'https://www2.census.gov/programs-surveys/popest/tables/2010-2019/state/totals/nst-est2019-01.xlsx'
    df = pd.read_excel(dataset, header=3)
    return df

def clean_ds1(state_climate):
    state_climate = state_climate.drop(state_climate.columns[0], axis=1)
    state_climate = state_climate[['STATE_NAME','year','tempc']]
    state_climate = state_climate.rename(columns={'STATE_NAME': 'State','tempc':'Temperature in Celsius'})
    state_climate = state_climate.pivot(index='State', columns='year', values='Temperature in Celsius').reset_index()
    state_climate.index.name = None
    return state_climate

def clean_ds2(state_population, state_climate):
    state_population.rename(columns={'Unnamed: 0': 'State'}, inplace=True)
    state_population = state_population.drop(state_population.columns[[1, 2]], axis=1)
    state_population['State'] = state_population['State'].str.strip('.')
    state_population= state_population[state_population['State'].isin(state_climate['State'].values)].reset_index(drop=True)
    return state_population

def merge_ds(state_climate, state_population):
    merged_data = pd.merge(state_population, state_climate, on='State', suffixes=('_pop', '_climate'))
    return merged_data

def store_data(df,table_name):
    current_directory = os.getcwd()
    data_folder_path = os.path.join(os.path.abspath(os.path.join(current_directory, os.pardir)), 'data')
    if not os.path.exists(data_folder_path):
        os.makedirs(data_folder_path)
    
    db_path = os.path.join(data_folder_path, 'pop_climate.db')
    engine = create_engine(f'sqlite:///{db_path}')
    df.to_sql(name=table_name, con=engine, index=False, if_exists='replace')


def main():
    state_climate = datasource1()
    state_population = datasource2()
    state_climate = clean_ds1(state_climate)
    state_population = clean_ds2(state_population,state_climate)

    state_population_climate = merge_ds(state_climate,state_population)
    store_data(state_climate,'state_climate')
    store_data(state_population,'state_population')
    store_data(state_population_climate,'state_population_climate')
	

if __name__ == "__main__":
    main()
