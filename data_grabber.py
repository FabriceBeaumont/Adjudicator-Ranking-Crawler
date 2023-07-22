from typing import List, Dict, Tuple, Set
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys

import numpy as np
import re
import time
from dataclasses import dataclass

import pandas as pd

# Local files.
import constants as c

@dataclass
class WEBSITE_DATA:    
    def __init__(self, url: str):
        self.tournament_url: str        = url
        self.tournament_title: str      = None
        # Data to fill out.
        self.competition_name: str      = ''
        self.adjudicator_ids: List[str] = []
        self.organiser: str             = ''
        self.host: str                  = ''
        self.tournament_director: str   = ''
        self.assessors: str             = ''
        self.protocol: str              = ''
        self.music: str                 = ''
        # Dictionary mapping the assigned ids in this competition to the 
        # id -> tuple(tuple(surname, name), affiliated dance club)
        self.adjudicators: pd.DataFrame     = None
        self.startlist: pd.DataFrame        = None
        self.results_finals: pd.DataFrame   = None
        self.results_rounds: pd.DataFrame   = None
        
        self._crawl_tournament_competitions()

    def update_adjudicator_df(self, adjudicators_name_club: List[Tuple[str, str]]):
        # Read the current dataframe.
        adjudicators_df = pd.read_csv(c.get_adjudicator_links_csv_path())
        # Add adjudicators which are unknown (defined as surname, name, club triple).
        for adjudicator_data in adjudicators_name_club:
            name = adjudicator_data[0]
            club = adjudicator_data[1]
            
            # Check if adjudicator is already known:    
            x = adjudicators_df.loc[(adjudicators_df[c.C.NAME.value] == name) & (adjudicators_df[c.C.Club.value] == club)]
            # Only add, if not already known.
            if len(x) == 0:   
                new_row = pd.DataFrame({c.C.NAME.value:name,c.C.CLUB.value:club}, index=[0])
                adjudicators_df = pd.concat([new_row,adjudicators_df.loc[:]]).reset_index(drop=True)

        # Update the dataframe.
        adjudicators_df.to_csv(c.get_adjudicator_links_csv_path(), index=False)
    
    def _crawl_tournament_competitions(self):
        # It is more difficult to read the TopTurnier data using Beautiful soup.
        # Thus here I use chrome modules to load the site.
        # chrome_options = Options()
        service = Service()
        options = webdriver.ChromeOptions()
        driver = webdriver.Chrome(service=service, options=options)
        
        # driver = webdriver.Chrome()
        driver.get(self.tournament_url)
        # Grab data.
        self.tournament_title = driver.title
        menu_html = driver.page_source
        print(menu_html)
        
        # Collect the anchor of the actual site where the data is stored.
        regexp = re.compile(r'src="(.*)"')
        menu_url = regexp.search(menu_html).group(1)
        
        # Switch the driver to the url of the data site.
        driver.get(self.tournament_url.replace(c.C.URL_KEYWORDS.value[0], menu_url))
        time.sleep(1)
        tables_html = driver.page_source
        # Close the current browser.
        driver.quit()  

        # Save the data.
        data_dfs = pd.read_html(tables_html)
        self._process_data(data_dfs)

    def _process_data(self, table_dfs):        
        def _parse_general_information(df):
            # Separate the columns of general information, and the list of adjudicators.
            split_id = np.where(df.iloc[:, 1].isnull().values == True)[0][0]
            general_info = df.iloc[:split_id, 1]
            # Extract general information.
            self.organiser, self.host, self.tournament_director, self.assessors, self.protocol, self.music = general_info
            # Extract adjudicators and their ids.
            a = list(df.iloc[split_id+1:, 1])
            adjudicator_ids = [x[:1] for x in list(df.iloc[split_id+1:, 0])]

            # Consruct an adjudicators dataframe for this competition.
            for adjudicator_id, adjudicator_and_club in zip(adjudicator_ids, a):
                ac: List[str] = adjudicator_and_club.split(" ")
                adjudicator_name: str   = " ".join(ac[0:2])
                club: str               = " ".join(ac[2:])
                # Add the data to the adjudicators dict of this competition.
                self.adjudicators[adjudicator_id] = tuple([adjudicator_name, club])

            # After processing the adjudicators data, add it to the global dataframe collecting all adjudicator data.
            self.update_adjudicator_df(list(self.adjudicators.values()))
                        
        def _parse_start_list_df(df):
            # Interpret the first row as header.
            df.columns = df.iloc[0]
            # Delete the first row - since its now header.
            df = df[1:]
            # Split the 'couples' column into men and woman column.            
            man_lady_names = df[c.C.COUPLE.value].str.split(" / ", expand = True)
            man_lady_names.rename(columns={0 :c.C.MAN.value, 1 :c.C.LADY.value}, inplace=True )
            # Delete the old 'couples' column.
            df = df.drop(columns=[c.C.COUPLE.value])
            # Add the new men-lady columns.
            df = df.join(man_lady_names)
            # Save the constructed dataframe.
            self.startlist = df[[c.C.NR.value, c.C.MAN.value, c.C.MAN.value, c.C.LADY.value, c.C.CLUB.value]]

        def _parse_finals_df(df):
            # Drop the 'Finals'-row.
            df = df[1:]
            # Set the first row as headder, and drop it.
            df.columns = df.iloc[0]
            df = df[1:]

            # Reduce the couple data to their number.
            tmp_df = df['Paar/Club'].str.extract(r'(\(\d*\))', expand=True)        
            tmp_df.rename(columns={0 :'Nr.'}, inplace=True)
            df = df.drop(columns=['Paar/Club'])
            self.results_finals = df.join(tmp_df)

            # Separate the dance rankings and their majority.
            dance_cols = self.results_finals.columns[1:-2]
            dance_dfs = []
            for c in dance_cols:
                tmp_df = self.results_finals[c].str.split(r'(\d*\s)', expand=True)
                tmp_df = tmp_df.drop(tmp_df.columns[0],axis=1)
                tmp_df.rename(columns={1 :c, 2: f"{c}_majority"}, inplace=True)
                dance_dfs.append(tmp_df)
                # Replace the unsplit column by the two new ones created from them.
                self.results_finals = self.results_finals.drop(columns=[c])
                self.results_finals = self.results_finals.join(tmp_df)

        def _parse_rounds_df(df):            
            # Drop the 'Finals'-row.
            all_results = all_results[1:]
            # Set the first row as headder, and drop it.
            all_results.columns = all_results.iloc[0]
            all_results = all_results[1:]

            all_results.columns.values[0] = c.C.RANK.value
            all_results.columns.values[1] = c.C.COUPLE.value
            all_results.columns.values[2] = c.C.CLUB.value
            all_results = all_results.drop(c.C.CLUB.value, axis=1)
            # Extract the couples number from the name, number column.
            tmp_df = all_results[c.C.COUPLE.value].str.extract(r'(\(\d*\))', expand=True)    
            tmp_df.rename(columns={0 : c.C.NR.value}, inplace=True)
            all_results = all_results.drop(c.C.COUPLE.value, axis=1)
            self.results_rounds = all_results.join(tmp_df)
        
        # Dataframe 1 - Competition name.
        self.competition_name = table_dfs[0][0][0]

        # Dataframe 2 - General information. Cover page.
        df_general_info = table_dfs[1]
        _parse_general_information(df_general_info)
        
        # Dataframe 3 - Attending athletes. Start list.
        startlist_df = table_dfs[2]
        _parse_start_list_df(startlist_df)

        # Dataframe 3 - Finals result list.
        finals_df = table_dfs[3]
        _parse_finals_df(finals_df)

        # Dataframe 4 - Other rounds resutls list.
        rounds_df = table_dfs[4]
        _parse_rounds_df(rounds_df)

        # TODO: Procress one by one. Test processing so far for multiple test urls.
        # TODO: Extract ranking into np.array for processing.
        # TODO: Are more df needed? Check with url open.
        # Dataframe 5 - Result with classification.
        all_results = table_dfs[5]

        # Dataframe 6 - Final round table.
        all_results1 = table_dfs[6]

        # Dataframe 7 - Overall classification table.
        all_results2 = table_dfs[7]
        
    
if __name__=="__main__":    
    competition_df = pd.read_csv(c.get_competition_links_csv_path())
    print(competition_df)

    # TODO: extract first site    
    pass


    # TODO: new script. Add comp id. Ad processed column
    # TODO: Process comp df: id, name, place, date, class, judges
    # TODO: Process judges:  id, name, club, list of comp ids           -> ID: SurnNamClub
    # TODO: Process dancers: id, name, class, club, list of comp ids, country  -> ID: SurnNamClub
        # TODO: Sparse columns for each class