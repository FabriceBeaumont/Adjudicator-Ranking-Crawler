from typing import List, Dict, Tuple, Set
from math import isnan
import time
from dateutil import parser
import time
import pandas as pd
from enum import Enum
import re

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from io import StringIO

# CHROME_WEBDRIVER_PATH = "/home/fabrice/Documents/PROGRAMMING/ChromeWebDrive/chromedriver"
CHROME_WEBDRIVER_PATH = "/usr/lib/chromium-browser/chromedriver"

# Local files.
import languages as l
import dataframes

# List of possible data tables for competitions:
class DfNames(Enum):
    general_info: str   = 'General information'
    adjudicators: str   = 'Adjudicators'
    ranking_list: str   = 'Ranking list'
    finals: str         = 'Finals'
    qualifications: str = 'Qualifications'


class CompetitionReader():
    URL_ENDING = '/index.htm'

    def __init__(self, url: str):
        self.url = url
        if not url.endswith(self.URL_ENDING):
            print(f"!!! URL {url} is NOT a competition-url!")
            return None

        # Competition values:
        self.comp_name: str       = None
        self.comp_date: str       = None
        self.comp_class: str      = None
        self.comp_title: str      = None
        self.comp_age_group: str  = None
        self.comp_level: str      = None
        self.comp_discipline: str = None
        self.comp_organiser: str  = None
        self.language_name: str        = l.LanguageNames.english
        self.comp_was_cancelled: bool      = True

        # Target DataFrames parsed and edited from the original competition webpage.
        self.df_general_info: pd.DataFrame   = None
        self.df_adjudicators: pd.DataFrame   = None
        self.df_ranking_list: pd.DataFrame   = None
        self.df_finals: pd.DataFrame         = None
        self.df_qualifications: pd.DataFrame = None        
        
        # Initial list of raw DataFrames, to read from a competition url.
        self._raw_data_dfs: List[pd.DataFrame] = []
        # Dictionary of indices, where to find which kind of dataframe in the raw list of DataFrames.
        self._data_df_indices_dict: Dict[str, List[int]] = {
            DfNames.general_info:   [],
            DfNames.adjudicators:   [],
            DfNames.ranking_list:   [],
            DfNames.finals:         [],
            DfNames.qualifications: [],
        }

        # Keys in the language of this competition webpage.
        self.c: l.LanguageConstants = None

    def _read_title_and_html(self) -> Tuple[str, str]:
        title: str = None
        html: str  = None

        # Start a Chrome driver to open the selected url.
        service = Service()
        options = webdriver.ChromeOptions()
        driver = webdriver.Chrome(service=service, options=options)
        # From the original link, get the content of 'title'.
        driver.get(self.url)
        # Wait a second since the menu has to build itself before it can be collected.
        time.sleep(.1)
        # Extract the webpage title (consists of competition date and class).
        title: str = driver.title
        
        # To get the data tables, switch tab.
        driver.get(self.url.replace(self.URL_ENDING, "/menu.htm#id2"))
        # Wait a second since the menu has to build itself before it can be collected.
        time.sleep(.1)
        html: str = driver.page_source
        # Since the download is complete now, close the driver.
        driver.close()

        return title, html

    def _parse_title(self, title_str: str, html: str) -> bool:
        parsing_complete: bool = False
        
        # If the title was not found in the original 'title'-attribute, try parsing it from the entire html itself.
        if title_str is None:
            match_date_and_class = re.search(r'<[t|T]itle>(.*?)</[t|T]itle>', html)
            if match_date_and_class:
                title_str = match_date_and_class.group(1)

        # Check if parsing worked so far.
        if title_str is None or html is None: return parsing_complete
            
        # Parse the competition date and with it determine the language.
        date: str = None
        for language_constants in l.LANGUAGE_CONSTANTS:
            match_date = re.search(language_constants.date_format, title_str)
            if match_date:
                date = match_date.group(1)
                self.language_name = language_constants.name
                self.c = l.get_constants_in_language(self.language_name)            

        # Check if parsing worked so far.
        if date is None: return parsing_complete
        
        # Extract the competition age group and level from the title.
        self.comp_class = title_str.replace(f"{date} ", '')        
        matches = re.search(r'(.*) ([EDCBAS]) (.*)', self.comp_class)
        if matches:
            self.comp_age_group  = matches.group(1)
            self.comp_level      = matches.group(2)
            self.comp_discipline = matches.group(3)
        
        # Regardless what dateformat was extracted, save it in german format.
        parsed_date = parser.parse(date)
        german_date_format = '%d.%m.%Y'
        self.comp_date = parsed_date.strftime(german_date_format)

        # Extract the competition title.
        match_title = re.search(r'<td>(.*?)</td>', html)
        if match_title:
            self.comp_title = match_title.group(1)
        else:
            return parsing_complete

        parsing_complete = True
        return parsing_complete

    def _parse_general_information_df(self) -> bool:
        try:
            df_general_info = self._raw_data_dfs[self._data_df_indices_dict[DfNames.general_info][0]].copy()
            # Rename the columns to more meaningful names.
            df_general_info.columns = [self.c.CATEGORY, self.c.VALUE]
            # Remove colon in the category column if present.
            df_general_info[self.c.CATEGORY] = df_general_info[self.c.CATEGORY].str.replace(':', '')
            # Split the gereral information table into the tournament information and the adjudicator list.
            split_id = df_general_info[df_general_info[self.c.CATEGORY] == self.c.KEY_ADJUDICATOR].index[0]
            df_general_info.set_index(self.c.CATEGORY)
            # The second part is actually the adjudicators table.
            df_adjudicators = pd.DataFrame(df_general_info.iloc[split_id+1:, :])            
            # The first part is truly the general information table.
            df_general_info = pd.DataFrame(df_general_info.iloc[:split_id, :])
            
            # Continue with the general inforamtion by extracting the organizer.
            organiser_series = df_general_info.loc[df_general_info[self.c.CATEGORY] == self.c.KEY_ORGANZIER, self.c.VALUE]
            # If this information is not given, use the master of ceremony instead.
            moc_series = df_general_info.loc[df_general_info[self.c.CATEGORY] == self.c.KEY_MASTER_OF_CEREMONY, self.c.VALUE]
            # If the extracted information is a 'Nan' value, it is interpreted as type float.
            if not organiser_series.empty and isinstance(organiser_series.iloc[0], str):
                self.comp_organiser = organiser_series.iloc[0]
            else:
                if not moc_series.empty and isinstance(moc_series.iloc[0], str):
                    self.comp_organiser = moc_series.iloc[0]
                else:
                    self.comp_organiser = 'Competition cancelled'
                    self.comp_was_cancelled = True
                    return None, None
            
            # Add more data to the general info.
            additional_info_rows: Dict[str, str] = {
                self.c.CATEGORY: ['Date', 'Title', 'Class'],
                self.c.VALUE:    [self.comp_date, self.comp_title, self.comp_class]
            }            
            # Save the DataFrame.
            self.df_general_info = pd.concat([df_general_info, pd.DataFrame(additional_info_rows)], axis=0, ignore_index=True)
            
            # Process the dict of adjudicators.
            # TODO: When parsing continues, use the other tables information to separate adjudicator from club, and then store them in the global dataframe.
            # Create a DataFrame to hold the adjudicator information.
            # adjudicator_dict_list: List[Dict[str, str]] = [] 
            # full_names_and_clubs: List[str] = []
            # names: List[str] = []
            # surnames: List[str] = []
            # clubs: List[str] = []

            # for _, row in df_adjudicators.iterrows():
            #     # Separate the adjudicator names and their clubs.
            #     adjudicator_id = row[self.c.CATEGORY]
            #     full_name_and_club = row[self.c.VALUE]
            #     ac = full_name_and_club.split(" ")
            #     full_name, club = tuple(ac[0:2]), " ".join(ac[2:])
            #     name    = full_name[0].replace(',', '')
            #     surname = full_name[1].replace(',', '')
            #     # Save them as tuple in a dict, using the adjudicator index.
            #     adjudicator_dict_list.append({
            #         self.c.ID: adjudicator_id, 
            #         self.c.NAME: name, 
            #         self.c.SURNAME: surname,
            #         self.c.CLUB: club
            #     })
            #     full_names_and_clubs.append(full_name)
            #     names.append(name)
            #     surnames.append(surname)
            #     clubs.append(club)

            # Update the gloabl list of known adjudicators.
            # global_adjudicator_df = dataframes.AdjudicatorDf()
            # global_adjudicator_df.add_urls_to_dict(full_names_and_clubs, names, surnames, clubs)
            
            # Save the DataFrame.
            self.df_adjudicators = pd.DataFrame(df_adjudicators)

        except Exception as e:
            print(f"Error when reading the general information table:\n{e}")
            self.comp_was_cancelled = True
            return None, None
        
        return None, None

    def read_comp_info(self, general_only: bool = False) -> Dict[str, str]:
        
        # Load and read the information on the url.
        title, html = self._read_title_and_html()

        parsing_complete = self._parse_title(title, html)
        if not parsing_complete: 
            self.comp_was_cancelled = True
            return None
        
        # Now read the data tables from the html.
        try:
            self._raw_data_dfs = pd.read_html(StringIO(html))
        except ValueError as e:
            # In this case either the url is invalid, or the competition just got cancelled and 
            # no further data was there to be stored. Assume the latter.
            self.comp_was_cancelled = True
            return None
        
        # Read the first dataframe and identify the right data tables and extract the data.
        self.comp_was_cancelled = self.assign_data_dfs()
        
        # Parse the first table, containing general information and the adjudicator list.
        self._parse_general_information_df()

        if self.comp_was_cancelled or general_only: return None
        # Process the dict of adjudicators.
        # TODO: CONTINUE with parsing of all tables here. (in subfunctions)

    def assign_data_dfs(self, verbose: bool=False) -> bool:
        cancelled: bool = True
        try:
            # The first table is expected to be the competiton name in all cases.
            self.comp_name = self._raw_data_dfs[0][0][0]
            # If at most a general information table is found, consider the competition to be cancelled.

            # Iterate over all found tables and assign their indices to keywords.
            for i in range(1, len(self._raw_data_dfs)):
                table = self._raw_data_dfs[i]

                # Test for different criteria to sort the tables into the dictionary.
                # GENERAL INFORMATION table.
                if str(table[0][0]).replace(':', '') in self.c.get_organizer_keys():
                    if verbose: print(f"Found general info\t\tin table {i}")
                    self._data_df_indices_dict[DfNames.general_info].append(i)
                    continue
                
                # RANKING LIST table - finals section.
                if len(table[0]) > 1 and str(table[0][0]) == self.c.KEY_FINAL:
                    if verbose: print(f"Found ranking list (finals)\tin table {i}")
                    self._data_df_indices_dict[DfNames.ranking_list].append(i)
                    cancelled = False
                    continue

                # RANKING LIST table - all qualification rounds sections.
                if len(table[0]) > 1 and str(table[0][1]).endswith(self.c.KEY_ROUND):
                    if verbose: print(f"Found ranking list (round)\tin table {i}")
                    self._data_df_indices_dict[DfNames.ranking_list].append(i)
                    cancelled = False
                    continue
                
                # FINALS table (containing the scating system and placements for each couple from each adjudicator).
                if len(table[0]) > 1 and str(table[1][0]) == self.c.KEY_ADJUDICATOR:
                    if verbose: print(f"Found finals scating\t\tin table {i}")
                    self._data_df_indices_dict[DfNames.finals].append(i)
                    cancelled = False
                    continue
                
                # QUALIFICATIONS table (containing points for all couples).
                if len(table[0]) > 1 and str(table[0][1]) == self.c.KEY_ADJUDICATOR:
                    if verbose: print(f"Found qualifications\t\tin table {i}")
                    self._data_df_indices_dict[DfNames.qualifications].append(i)
                    cancelled = False
                    continue

                if verbose: print(f"Did not process table {i}!")
        except Exception as e:
            print("!!! Error while assigning the dataframes:\n{e}")
                
        return cancelled

def run_test() -> bool:
    # Test: Competition without tournament site:
    url='https://www.tanzsport.de/files/tanzsport/ergebnisse/2019/om-jun2bstd/index.htm'
    cr = CompetitionReader(url)
    cr.read_comp_info()
    succesful: bool = (cr.url == url) & \
                      (cr.comp_age_group == 'Jun.II') & \
                      (cr.comp_date == '20.04.2019') & \
                      (cr.comp_discipline == 'Standard') & \
                      (cr.comp_level == 'B') & \
                      (cr.comp_name == 'DTV-Ranglistenturnier - Jun II B Std - Braunschweig') & \
                      (cr.comp_organiser == 'Nds. Tanzsportverband e.V., NTV') & \
                      (cr.comp_title == 'DTV-Ranglistenturnier - Jun II B Std - Braunschweig') & \
                      (cr.language_name == l.LanguageNames.german) & \
                      (cr.comp_was_cancelled == False) & \
                      (list(cr.df_adjudicators[cr.c.VALUE]) == ['Rüdiger Knaack Braunschweiger TSC', 'Gabor-Istvan Hoffmann TSZ Blau-Gold Casino, Darmstadt', 'Torsten Flentge TC Schwarz-Silber Halle', 'Anja Rausche-Schramm TSA d. 1. SC Norderstedt', 'Kerstin Stettner Tanzsportgemeinschaft Fürth', 'Dr. Holger Schilling TSV Grün-Gold Erfurt', 'Rainer Kopf TSC Grün-Gold Speyer'])
    print(f"Test was {'' if succesful else 'not '}succesful on {url=}")

    # Test: Modern competition:
    url='https://ergebnisse.tanzsportkreis-sankt-augustin.de/2015/TSK_220215//0-ot_hgrdstd/index.htm'
    cr = CompetitionReader(url)
    cr.read_comp_info()
    succesful: bool = (cr.url == url) & \
                      (cr.comp_age_group == 'Hgr.') & \
                      (cr.comp_date == '22.02.2015') & \
                      (cr.comp_discipline == 'Standard') & \
                      (cr.comp_level == 'D') & \
                      (cr.comp_name == 'Sportturnier - Sankt Augustin') & \
                      (cr.comp_organiser == 'TSK Sankt Augustin') & \
                      (cr.comp_title == 'Sportturnier - Sankt Augustin') & \
                      (cr.language_name == l.LanguageNames.german) & \
                      (cr.comp_was_cancelled == False) & \
                      (list(cr.df_adjudicators[cr.c.VALUE]) == ['Monika Gräf TGC Rot-Weiß-Porz e.V.', 'Klaus Luckas VTG Grün-Gold Recklinghausen e.V.', 'Dimitrios Nicolos TSK Sankt Augustin e.V.', 'Jörg Vahlert TSA d. Bonner TV 1860 e.V.', 'Michael Wunnenberg Grün-Gold Casino Wuppertal e.V.'])
    print(f"Test was {'' if succesful else 'not '}succesful on {url=}")

    # Test: Cancelled competition:
    url='https://ergebnisse.tanzsportkreis-sankt-augustin.de/2023/23-08-12_TSK_Sommerturniere/8-1208_ot_mas1dlat/index.htm'
    cr = CompetitionReader(url)
    cr.read_comp_info()
    succesful: bool = (cr.url == url) & \
                      (cr.comp_age_group == 'Mas.I') & \
                      (cr.comp_date == '08.12.2023') & \
                      (cr.comp_discipline == 'Latein') & \
                      (cr.comp_level == 'D') & \
                      (cr.comp_name == 'Mas I D Lat - Sankt Augustin') & \
                      (cr.comp_organiser == 'TSK Sankt Augustin e.V.') & \
                      (cr.comp_title == 'Mas I D Lat - Sankt Augustin') & \
                      (cr.language_name == l.LanguageNames.german) & \
                      (cr.comp_was_cancelled == True) & \
                      (list(cr.df_adjudicators[cr.c.VALUE]) == [])
    print(f"Test was {'' if succesful else 'not '}succesful on {url=}")


if __name__=="__main__":
    run_test()