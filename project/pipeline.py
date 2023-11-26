import opendatasets as od
import os
import pandas as pd
import requests
from sqlalchemy import create_engine

def download_csv(url, save_path):
    response = requests.get(url)
    if response.status_code == 200:
        with open(save_path, 'wb') as file:
            file.write(response.content)
        print(f"CSV file downloaded successfully to {save_path}")
    else:
        print(f"Failed to download CSV file. Status code: {response.status_code}")


def st_climate():
    github_url = 'https://raw.githubusercontent.com/washingtonpost/data-2C-beyond-the-limit-usa/main/data/processed/model_state.csv'
    model_state_file = 'model_state.csv'
    download_csv(github_url, model_state_file)

    model_state = pd.read_csv('model_state.csv')
    model_state = model_state[['fips','STATE_NAME']]

    github_url = 'https://raw.githubusercontent.com/washingtonpost/data-2C-beyond-the-limit-usa/main/data/processed/climdiv_state_year.csv'
    climdiv_state_year_file = 'climdiv_state_year.csv'
    download_csv(github_url, climdiv_state_year_file)

    state_climate = pd.read_csv('climdiv_state_year.csv')
    state_climate = state_climate[(state_climate['year'] >= 2010) & (state_climate['year'] <= 2019)].reset_index(drop=True)
    state_climate = state_climate.drop(state_climate.columns[2], axis=1)
    state_climate = pd.merge(state_climate, model_state, on='fips', how='left')
    state_climate = state_climate.drop(state_climate.columns[0], axis=1)
    state_climate = state_climate[['STATE_NAME','year','tempc']]
    state_climate = state_climate.rename(columns={'STATE_NAME': 'State','tempc':'Temperature in Celsius'})
    state_climate = state_climate.pivot(index='State', columns='year', values='Temperature in Celsius').reset_index()
    state_climate.index.name = None

    return state_climate

def st_pop(state_climate):
    dataset = 'https://www2.census.gov/programs-surveys/popest/tables/2010-2019/state/totals/nst-est2019-01.xlsx'
    od.download(dataset)

    file_path = 'nst-est2019-01.xlsx'
    state_population = pd.read_excel(file_path, header=3)
    state_population.rename(columns={'Unnamed: 0': 'State'}, inplace=True)
    state_population = state_population.drop(state_population.columns[[1, 2]], axis=1)
    state_population['State'] = state_population['State'].str.strip('.')
    state_population= state_population[state_population['State'].isin(state_climate['State'].values)].reset_index(drop=True)

    return state_population

def store_data(state_climate, state_population):
    current_directory = os.getcwd()
    data_folder_path = os.path.join(os.path.abspath(os.path.join(current_directory, os.pardir)), 'data')
    if not os.path.exists(data_folder_path):
        os.makedirs(data_folder_path)
    
    db_path = os.path.join(data_folder_path, 'state_pop_climate.db')
    engine = create_engine(f'sqlite:///{db_path}')
    state_climate.to_sql(name='state_climate', con=engine, index=False, if_exists='replace')
    state_population.to_sql(name='state_population', con=engine, index=False, if_exists='replace')

def main():
    sc = st_climate()
    sp = st_pop(sc)
    store_data(sc,sp)

if __name__ == "__main__":
    main()
