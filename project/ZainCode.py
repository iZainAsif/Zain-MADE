import opendatasets as od
import requests
import os
import sqlite3
import pandas as pd

class Pipeline:        
    def creating_SQL(self):
        dataset = 'https://www2.census.gov/programs-surveys/popest/tables/2010-2019/state/totals/nst-est2019-01.xlsx'
        od.download(dataset)
        file_path = 'nst-est2019-01.xlsx'
        state_pop_df = pd.read_excel(file_path)
        state_pop_df.columns
        new_column_names = {'table with row headers in column A and column headers in rows 3 through 4. (leading dots indicate sub-parts)': 'State_Name','Unnamed: 12': 'Population_Estimate_2019'}
        state_pop_df = state_pop_df.rename(columns=new_column_names)
        state_pop_df=state_pop_df.drop(columns=['Unnamed: 1','Unnamed: 2','Unnamed: 3','Unnamed: 4','Unnamed: 5','Unnamed: 6','Unnamed: 7','Unnamed: 8','Unnamed: 9','Unnamed: 10','Unnamed: 11'])
        state_pop_df = state_pop_df.iloc[3:61]
        state_pop_df = state_pop_df.reset_index(drop=True)
        github_url = 'https://raw.githubusercontent.com/washingtonpost/data-2C-beyond-the-limit-usa/main/data/raw/co-est2018-alldata.csv'
        response = requests.get(github_url)
        if response.status_code == 200:
            with open('co-est2018-alldata.csv', 'wb') as file:
                file.write(response.content)
            Climate_USA_df = pd.read_csv('co-est2018-alldata.csv', encoding='latin1')  # Assuming 'latin1' encoding, you can try 'ISO-8859-1' as well
        else:
            print(f"Failed to download the file. Status code: {response.status_code}")
        current_dir = os.getcwd()
        data_dir = os.path.join(os.path.dirname(current_dir), 'data')
        os.makedirs(data_dir, exist_ok=True)
        db_path = os.path.join(data_dir, 'state_population.sqlite')
        db_path = os.path.join(data_dir, 'Climate_USA.sqlite')  
        conn = sqlite3.connect(db_path)
        state_pop_df.to_sql('state_population', conn, index=False, if_exists='replace')
        Climate_USA_df.to_sql('Climate_USA', conn, index=False, if_exists='replace')
        conn.close()
        
        
            
def main():
    test = Pipeline()
    test.creating_SQL()

if __name__ == "__main__":
    main()
