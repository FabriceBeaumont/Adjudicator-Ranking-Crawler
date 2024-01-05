from typing import List, Dict, Tuple, Set
import numpy as np
import time
from datetime import datetime
from dateutil import parser
from dataclasses import dataclass
import time
from datetime import date
from collections import deque
import pandas as pd
from pathlib import Path
from abc import ABC
from enum import Enum
import re

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
import requests
from bs4 import BeautifulSoup
from io import StringIO

# CHROME_WEBDRIVER_PATH = "/home/fabrice/Documents/PROGRAMMING/ChromeWebDrive/chromedriver"
CHROME_WEBDRIVER_PATH = "/usr/lib/chromium-browser/chromedriver"

# For retries
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Local files.


class LanguageConstants(ABC):
    # Local keywords.
    COMPETITION_NAME: str      = 'Competition name'
    COMPETITION_LINK: str      = 'Competition link' 
    TOURNAMENT_NAME: str       = 'Tournament name'
    TOURNAMENT_LINK: str       = 'Tournament link'
    URL: str            = 'URL'
    BASE_URL: str       = 'Base url'
    DATE: str           = 'Crawl date'
    ID: str             = 'Tournament id'
    PROCESSED: str      = 'Processed'
    COMP_SCOPE: str     = 'Scope'
    SURNAME: str        = 'Surname'
    NAME: str           = 'Name'
    LEADER: str         = 'Leader'
    FOLLOWER: str       = 'Follower'
    CLUB: str           = 'Club'
    # The rank refers to the placement in the ranking (first place until last place).
    RANK: str           = 'Rank'
    # The placement refers to any points in any computation while computing the ranks.
    PLACEMENT: str      = 'Placement'

    DANCE: str          = 'Dance'
    ROUND: str          = 'Round'
    GRADE: str          = 'Grade'

    CATEGORY: str       = 'Category'
    VALUE: str          = 'Value'
    
    SUM: str            = 'Sum'
    QUALI: str          = 'Qualified'
    NR_ROUNDS: str      = 'Nr. Rounds + Final'   
    NR_ADJDCTRS: str    = 'Nr. Adjudicators'
    NR_COUPLES: str     = 'Nr. Couples'
    
    KEY_ORGANZIER: str          = 'Organizer:'
    KEY_MASTER_OF_CEREMONY: str = 'Master of Ceremony:' 
    COUPLE: str                 = 'Couple'
    PLACEMENT: str              = 'Placement'
    # The number is the identifyer number of the couple in the tournament.
    NR: str                     = 'Nr.'
    KEY_FINAL: str              = 'Final'
    KEY_ROUND: str              = 'round'
    KEY_ADJUDICATOR: str        = 'Adjudicator'
    KEY_COUPLE: str             = 'Couple'

    LW_l = "Waltz"    
    LW_s = "SW"
    TG_l = "Tango"
    TG_s = "TG"
    WW_l = "V. Waltz"
    WW_s = "VW"
    SF_l = "Slowfox"
    SF_s = "SF"
    QS_l = "Quickstep"
    QS_s = "QS"
    SB_l = "Samba"
    SB_s = "SB"
    CC_l = "Cha Cha"
    CC_s = "CC"
    RB_l = "Rumba"
    RB_s = "RB"
    PD_l = "Paso Doble"
    PD_s = "PD"
    JV_l = "Jive"
    JV_s = "JV"

    def __init__(self):
        pass

    @classmethod
    def get_organizer_keys(self) -> List[str]:
        return [self.KEY_ORGANZIER, self.KEY_MASTER_OF_CEREMONY]
    
    @classmethod
    def get_dancenames_short(self) -> List[str]:
        return [self.LW_s, self.TG_s, self.WW_s, self.SF_s, self.QS_s, self.SB_s, self.CC_s, self.RB_s, self.PD_s, self.JV_s]
    
    @classmethod
    def parse_dance_name(self, dance_name: str) -> str:
        if dance_name == self.LW_l: return self.LW_s
        if dance_name == self.TG_l: return self.TG_s
        if dance_name == self.WW_l: return self.WW_s
        if dance_name == self.SF_l: return self.SF_s
        if dance_name == self.QS_l: return self.QS_s
        if dance_name == self.SB_l: return self.SB_s
        if dance_name == self.CC_l: return self.CC_s
        if dance_name == self.RB_l: return self.RB_s
        if dance_name == self.PD_l: return self.PD_s
        if dance_name == self.JV_l: return self.JV_s

class Constants_en(LanguageConstants):
    def __init__(self):
        super().__init__()

class Constants_de(LanguageConstants):    
    # Define the dance names in german.
    LW_l = "Langsamer Walzer"
    LW_s = "LW"
    WW_l = "Wiener Walzer"
    WW_s = "WW"

    KEY_ORGANZIER   = 'Veranstalter:'    
    KEY_MASTER_OF_CEREMONY = 'Ausrichter:'
    COUPLE          = 'Paar'
    PLACEMENT       = 'Platz'
    ROUND           = 'Runde'
    NR              = 'Nr.'
    KEY_FINAL       = 'Endrunde'
    KEY_ROUND       = 'runde'
    KEY_ADJUDICATOR = 'Wertungsrichter'
    KEY_COUPLE      = 'Paar/Club'

    def __init__(self):
        super().__init__()

def get_constants_in_language(language: str) -> LanguageConstants:
    if language == 'de':
        return Constants_de()
    else:
        return Constants_en()

# List of possible data tables for competitions:
class DfNames(Enum):
    general_info: str   = 'General informations'
    adjudicators: str   = 'Adjudicators'
    ranking_list: str   = 'Ranking list'
    finals: str         = 'Finals'
    qualifications: str = 'Qualifications'


class CompetitionReader():

    def __init__(self, url: str):
        self.url = url

        # Competition values:
        self.comp_name: str       = None
        self.comp_date: str       = None
        self.comp_class: str      = None
        self.comp_title: str      = None
        self.comp_age_group: str  = None
        self.comp_level: str      = None
        self.comp_discipline: str = None
        self.is_english: bool     = False
        self.comp_organiser: str          = None

        # Target dataframes to read from a competition url.
        self.df_general_info: pd.DataFrame   = None
        self.df_adjudicators: pd.DataFrame   = None
        self.df_ranking_list: pd.DataFrame   = None
        self.df_finals: pd.DataFrame         = None
        self.df_qualifications: pd.DataFrame = None        
        
        # Initial list of dataframes, to read from a competition url.
        self.raw_data_dfs: List[pd.DataFrame] = []
        # Dictionary of indices, where to find which kind of dataframe in the raw data df list.
        self.data_df_indices_dict: Dict[str, List[int]] = {
            DfNames.general_info:   [],
            DfNames.adjudicators:   [],
            DfNames.ranking_list:   [],
            DfNames.finals:         [],
            DfNames.qualifications: [],
        }

        # Depending on the language of the competition site, use specific keywords.
        # They are saved in an instance of language constants.
        self.c: LanguageConstants = None

    def read_comp_info(self, general_only: bool = False) -> Dict[str, str]:
        # Start a Chrome driver to open the selected url.
        service = Service()
        options = webdriver.ChromeOptions()
        driver = webdriver.Chrome(service=service, options=options)
    
        driver.get(self.url.replace('index.htm', "menu.htm#id2"))
        # Wait a second since the menu has to build itself before it can be collected.
        time.sleep(1)            
        html: str = driver.page_source
        
        # Extract the title, class and date of the competition.
        # German format:    dd.mm.yyyy
        # English format:   dd/Mon/yyyy
        comp_date_and_class: str = driver.title
        match_date_de = re.search(r'\b(\d{2}\.\d{2}.\d{4})\b', comp_date_and_class)
        match_date_en = re.search(r'(\d{2}/[a-zA-Z]+/\d{4})', comp_date_and_class)

        if match_date_de:
            date = match_date_de.group(1)
        elif match_date_en:
            date = match_date_en.group(1)
            self.is_english = True
        
        self.c = get_constants_in_language('de')
        if self.is_english:
            self.c = get_constants_in_language('en')

        # Extract the competition age group and level.
        comp_class = comp_date_and_class.replace(f"{date} ", '')
        matches = re.search(r'(.*) ([EDCBAS]) (.*)', comp_class)
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
        
        self.raw_data_dfs.append(pd.read_html(StringIO(html)))
        # It may be that the data is stored in a nested DataFrame. In this case extract it.
        if len(self.raw_data_dfs) == 1: self.raw_data_dfs = self.raw_data_dfs[0]

        # Since the download is complete now, close the driver.
        driver.close()

        # Now identify the right data tables and extract the data.
        self.assign_data_dfs()

        # Read the first dataframe.
        df_general_info = self.raw_data_dfs[self.data_df_indices_dict[DfNames.general_info][0]].copy()
        # Split the gereral information table into the tournament information and the adjudicator list.
        split_id = np.where(df_general_info.iloc[:, 1].isnull() == True)[0][0]
        # Extract the remaining rows as information about the adjudicators.
        adjudicator_names = list(df_general_info.iloc[split_id+1:, 1])
        adjudicator_ids = [x[:1] for x in list(df_general_info.iloc[split_id+1:, 0])]
        adjudicators = dict(zip(adjudicator_ids, adjudicator_names))

        # Extract the data into individual variables.
        df_general_info = pd.DataFrame(df_general_info.iloc[:split_id, :])
        df_general_info.set_index([0])
        df_general_info.columns = [self.c.CATEGORY, self.c.VALUE]
        df_general_info = df_general_info[1:]
        # Remove colon in the category column if present.
        df_general_info[self.c.CATEGORY] = df_general_info[self.c.CATEGORY].str.replace(':', '')
        # The first row contains the category organizer (column '0') and its value (column '1').
        self.comp_organiser = df_general_info.iloc[0][1]

        # Add more data to the general info.
        additional_info: Dict[str, str] = {}
        additional_info['Date'] = self.comp_date
        additional_info['Title'] = self.comp_title
        additional_info['Class'] = self.comp_class

        # Save the dataframe.
        self.df_general_info = df_general_info

        if general_only: return None
        # Process the dict of adjudicators.
        # TODO: CONTINUE with parsing of all tables here. (in subfunctions)

    def assign_data_dfs(self, verbose: bool=False) -> None:
        # The first table is expected to be the competiton name in all cases.
        self.comp_name = self.raw_data_dfs[0][0][0]

        # Iterate over all found tables and assign their indices to keywords.
        for i in range(1, len(self.raw_data_dfs)):
            table = self.raw_data_dfs[i]

            # Test for different criteria to sort the tables into the dictionary.
            # GENERAL INFORMATION table.
            if table[0][0] in self.c.get_organizer_keys():
                if verbose: print(f"Found general info\t\tin table {i}")
                self.data_df_indices_dict[DfNames.general_info].append(i)
                continue
            
            # RANKING LIST table - finals section.
            if len(table[0]) > 1 and str(table[0][0]) == self.c.KEY_FINAL:
                if verbose: print(f"Found ranking list (finals)\tin table {i}")
                self.data_df_indices_dict[DfNames.ranking_list].append(i)
                continue

            # RANKING LIST table - all qualification rounds sections.
            if len(table[0]) > 1 and str(table[0][1]).endswith(self.c.KEY_ROUND):
                if verbose: print(f"Found ranking list (round)\tin table {i}")
                self.data_df_indices_dict[DfNames.ranking_list].append(i)
                continue
            
            # FINALS table (containing the scating system and placements for each couple from each adjudicator).
            if len(table[0]) > 1 and str(table[1][0]) == self.c.KEY_ADJUDICATOR:
                if verbose: print(f"Found finals scating\t\tin table {i}")
                self.data_df_indices_dict[DfNames.finals].append(i)
                continue
            
            # QUALIFICATIONS table (containing points for all couples).
            if len(table[0]) > 1 and str(table[0][1]) == self.c.KEY_ADJUDICATOR:
                if verbose: print(f"Found qualifications\t\tin table {i}")
                self.data_df_indices_dict[DfNames.qualifications].append(i)
                continue

            if verbose: print(f"Did not process table {i}!")


if __name__=="__main__":
    url='https://www.tanzsport.de/files/tanzsport/ergebnisse/2019/om-jun2bstd/index.htm'
    competition_reader = CompetitionReader(url)
    competition_reader.read_comp_info()

    pass