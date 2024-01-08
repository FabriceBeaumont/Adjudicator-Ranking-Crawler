from typing import List, Dict, Tuple, Set
from math import isnan
import time
from dateutil import parser
import time
import pandas as pd
from enum import Enum
from pathlib import Path
import re

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from io import StringIO

# CHROME_WEBDRIVER_PATH = "/home/fabrice/Documents/PROGRAMMING/ChromeWebDrive/chromedriver"
CHROME_WEBDRIVER_PATH = "/usr/lib/chromium-browser/chromedriver"

# Local files.
import languages as l
import dataframes
import filenames as f

# List of possible data tables for competitions:
class DfNames(Enum):
    general_info: str   = 'General information'
    adjudicators: str   = 'Adjudicators'
    ranking_list: str   = 'Ranking list'
    finals: str         = 'Finals'
    qualifications: str = 'Qualifications'


class CompetitionReader():
    URL_ENDING = '/index.htm'

    # Definitions of column names.
    cCATEGORY: str  = 'Category'
    cVALUE: str     = 'Value'
    cDATE: str      = 'Date'
    cTITLE: str     = 'Title'
    cCLASS: str     = 'Class'
    cPLACEMENT: str = 'Placement'
    cCOUPLE: str    = 'Couple'
    cCLUB: str      = 'Club'
    cNR: str        = 'Nr.'
    cROUND: str     = 'Round'
    cLEADER: str    = 'Leader'
    cFOLLOWER: str  = 'Follower'
    cDANCE: str     = 'Dance'
    cSUM: str       = 'Sum'
    cQUALI: str     = 'Qualified'

    # Define some row keywords used in the general information table.
    rNR_ROUNDS: str   = 'Nr. Rounds'
    rNR_ADJDCTRS: str = 'Nr. Adjudicators'
    rNR_COUPLES: str  = 'Nr. Couples'


    def __init__(self, url: str, save_to_file: bool = False):
        self.url = url
        self.save_to_file: bool = save_to_file
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
        # The number of rounds refers to all rounds which are NOT the final.
        self.nr_adjudicators: int = None
        self.nr_rounds: int       = None
        self.nr_couples: int      = None

        # Target DataFrames parsed and edited from the original competition webpage.
        self.df_general_info: pd.DataFrame   = None
        self.df_adjudicators: pd.DataFrame   = None
        self.df_ranking_list: pd.DataFrame   = None
        self.df_finals: pd.DataFrame         = None
        self.df_qualifications: List[pd.DataFrame] = []
        
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
            df_general_info.columns = [self.cCATEGORY, self.cVALUE]
            # Remove colon in the category column if present.
            df_general_info[self.cCATEGORY] = df_general_info[self.cCATEGORY].str.replace(':', '')
            # Split the gereral information table into the tournament information and the adjudicator list.
            split_id = df_general_info[df_general_info[self.cCATEGORY] == self.c.KEY_ADJUDICATOR].index[0]
            df_general_info.set_index(self.cCATEGORY)
            # The second part is actually the adjudicators table.
            df_adjudicators = pd.DataFrame(df_general_info.iloc[split_id+1:, :])
            self.nr_adjudicators = len(df_adjudicators)
            # The first part is truly the general information table.
            df_general_info = pd.DataFrame(df_general_info.iloc[:split_id, :])
            
            # Continue with the general inforamtion by extracting the organizer.
            organiser_series = df_general_info.loc[df_general_info[self.cCATEGORY] == self.c.KEY_ORGANZIER, self.c.VALUE]
            # If this information is not given, use the master of ceremony instead.
            moc_series = df_general_info.loc[df_general_info[self.cCATEGORY] == self.c.KEY_MASTER_OF_CEREMONY, self.c.VALUE]
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
                self.cCATEGORY: [self.cDATE, self.cTITLE, self.cCLASS],
                self.cVALUE:    [self.comp_date, self.comp_title, self.comp_class]
            }            
            # Save the DataFrame.
            self.df_general_info = pd.concat([df_general_info, pd.DataFrame(additional_info_rows)], axis=0, ignore_index=True)
            if self.save_to_file: self.df_general_info.to_csv(Path(f.DIR_DATA, f.FN.CSV_GENERAL_INFO))
            
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
            if self.save_to_file: self.df_adjudicators.to_csv(Path(f.DIR_DATA, f.FN.CSV_ADJUDICATORS))

        except Exception as e:
            print(f"Error when parsing the general information table:\n{e}")
            self.comp_was_cancelled = True
            return False
        
        return True

    # TODO: Debug this function.
    def _parse_ranking_list_df(self) -> bool:
        try:
            # There may be several tables involved when constructing the ranking list.
            # Start with the first one as initialization and add the others one by one.
            df_ranking_finals = self._raw_data_dfs[DfNames.ranking_list][0].copy()
            # Drop the first row which contains only the key word 'Finals'.
            df_ranking_finals = df_ranking_finals[1:]
            # Set the first row as headder, and drop it form the data body.
            df_ranking_finals.columns = df_ranking_finals.iloc[0]
            df_ranking_finals = df_ranking_finals[1:]

            # Since the placements of the finals are redundant information and will be extracted from the finals table,
            # delete it here. In case that there were more rounds this allows to easily join the other rankings to this table.
            df_ranking_finals.drop(columns=self.c.get_dancenames_short() + ['PZ'], inplace=True, errors='ignore')
            # Split the club name from the couples name (and their number).
            df_ranking_finals[[self.c.COUPLE, self.c.CLUB]] = df_ranking_finals[self.c.KEY_COUPLE].str.split(")", expand = True)
            df_ranking_finals.drop(columns=[self.c.KEY_COUPLE], inplace=True)
            df_ranking_finals[[self.c.COUPLE, self.c.NR]] = df_ranking_finals[self.c.COUPLE].str.split("(", expand = True)
            df_ranking_finals[[self.c.ROUND]] = self.c.KEY_FINAL
            df_ranking_finals = df_ranking_finals[[self.c.PLACEMENT, self.c.ROUND, self.c.NR, self.c.COUPLE, self.c.CLUB]]
            

            # Now add to this DataFrame all remainin tables one by one.
            self.nr_rounds = 0
            for i in range(1, len(self._raw_data_dfs[DfNames.ranking_list])):
                df_ranking_other = self._raw_data_dfs[DfNames.ranking_list][i].copy()
                # Drop all rows which contain only NaN values. These are originally used to separate the tables.
                df_ranking_other.dropna(axis=0, how='all', inplace=True)
                # Rename the columns.
                df_ranking_other.columns = [self.cPLACEMENT, self.cCOUPLE, self.cCLUB]

                # Split the club name from the couples name (and their number).
                df_ranking_other[[self.cNR]]     = df_ranking_other[self.cCOUPLE].str.extract(r'\((\d*)\)', expand = True)
                df_ranking_other[[self.cCOUPLE]] = df_ranking_other[self.cCOUPLE].str.extract(r'(.*)\(', expand = True)

                # Collect the row indices that contain names instead of placements. These are the round names.
                df_ranking_other.reset_index(inplace=True)
                round_rows = df_ranking_other[df_ranking_other[self.cPLACEMENT].str.endswith(self.c.KEY_ROUND)]
                # Add to the total number of rounds. This excludes the finals!
                self.nr_rounds += len(round_rows)
                round_list: List[Tuple[int, str]] = [(i, round_rows[self.cPLACEMENT][i]) for i in round_rows.index] + [(len(df_ranking_other), '')]

                # Mark all following rows with the corresponding round name in a new column.
                df_ranking_other[[self.cROUND]] = round_list[0][1]
                for i in range(len(round_list) - 1):
                    start_index, round_name = round_list[i]
                    end_index = round_list[i + 1][0]
                    df_ranking_other.iloc[start_index : end_index, df_ranking_other.columns.get_loc(self.c.ROUND)] = round_name

                # Now delete the rows with the round names. 
                # Notice that we ignore the last entry since this is an artificial one to finish the for-loop above.
                for i, _ in round_list[:-1]:
                    df_ranking_other.drop(i, inplace=True)

                # Rearrange the column order.
                df_ranking_other = df_ranking_other[[self.cPLACEMENT, self.cROUND, self.cNR, self.cCOUPLE, self.cCLUB]]
                # Append the dataframe to the first one(s).
                df_ranking_finals = pd.concat([df_ranking_finals, df_ranking_other], axis=0, ignore_index=True)

            # Add two more columns for the separated names of the leader and the follower.
            df_ranking_finals[[self.cLEADER, self.cFOLLOWER]] = df_ranking_finals[self.cCOUPLE].str.split(" / ", expand = True)
            # Initialize the number of couples based on the ranking list.
            self.nr_couples: int = len(df_ranking_finals)
            # Set the couples number as row index.
            df_ranking_finals.set_index(self.cNR, inplace=True)

            # Finally the whole table is asembled. Store it.
            self.df_ranking_list = df_ranking_finals        
            if self.save_to_file: self.df_ranking_list.to_csv(Path(f.DIR_DATA, f.FN.CSV_RANKING_LIST))

        except Exception as e:
            print(f"Error when parsing the ranking list table:\n{e}")
            self.comp_was_cancelled = True
            return False
        
        return True

    # TODO: Debug this function.
    def _parse_finals_df(self) -> bool:
        try:
            df_finals = self._raw_data_dfs[DfNames.finals][0]            
            dance_name_rows = df_finals[df_finals[0].isna()]
            dance_name_ids = dance_name_rows.index.to_list() + [len(df_finals.index)]

            # Add a column to display the dance for which the rankings are for. 
            # This will replace the horizonal headlines indicating the dances - which will be deleted afterwards.
            df_finals[[self.cDANCE]] = ''
            df_finals[self.cDANCE].iloc[1] = self.cDANCE
            for i in range(len(dance_name_ids) - 1):
                start_index = dance_name_ids[i]
                dance_name = self.c.parse_dance_name(dance_name_rows.loc[start_index, 1], C)
                end_index = dance_name_ids[i+1] - 1
                df_finals.loc[start_index:end_index, self.cDANCE] = dance_name

            # Now delete the rows with the dance names. 
            # Notice that we ignore the last entry since this is an artificial one to finish the for-loop above.
            for i in dance_name_ids[:-1]:
                df_finals.drop(i, inplace=True)

            # Replace the couple names by only their number.
            df_finals[[self.cNR]] = df_finals.iloc[:, 0].str.extract(r'(\d+)\D*', expand = True)
            df_finals[self.cNR].iloc[1] = self.cNR
            df_finals = df_finals.iloc[:, 1:]

            # Replace the adjudicator names by only their reference letter.
            for i, value in enumerate(df_finals.iloc[1, :]):
                if value == "1.": 
                    break

                df_finals.iloc[1, i] = value[:1]

            # Delete the first line which only denotes where the adjudicators and results columns are - same as the second row does.
            df_finals = df_finals.iloc[1:]
            # Set the now first row as header.
            df_finals.columns = df_finals.iloc[0]
            df_finals = df_finals[1:]

            # Use the couple number as index column.
            df_finals.set_index(self.cNR, drop=True, inplace=True)
            df_finals.index.name = self.cNR

            # Finally the whole table is asembled. Store it.
            self.df_finals = df_finals
            if self.save_to_file: self.df_finals.to_csv(Path(f.DIR_DATA, f.FN.CSV_FINALS))

        except Exception as e:
            print(f"Error when parsing the finals table:\n{e}")
            self.comp_was_cancelled = True
            return False
        
        return True
    
    # TODO: Debug this function.
    def _parse_qualification_round_df(self) -> bool:
        row_id_marks: int = 0
        row_id_sums: int  = 1
        row_id_quali: int = 2
        row_id_round_offset: int = 3
        try:
            n: int = len(self._raw_data_dfs[DfNames.qualifications])
            df_qualifications = self._raw_data_dfs[DfNames.qualifications][0]            
            if n > 1:
                for i in range(1, n):
                    # Drop the first column since it is already present from the first dataframe.
                    df_tmp = self._raw_data_dfs[DfNames.qualifications][i].iloc[:, 1:]
                    # Append the dataframe to the rest.
                    df_qualifications = pd.concat([df_qualifications, df_tmp], axis=1)

            # Delete the first row since it does not really contain information
            df_qualifications = df_qualifications[1:]
            # Because of the concatenation the column indices could be messed up. Reset them to a range.
            df_qualifications.columns = range(len(df_qualifications.columns))

            # Delete all rows where all values are NaN.
            df_qualifications.dropna(axis=1, how='all', inplace=True)

            # Depending on the size of the tournament, the names of the competitors are perhaps repeated.
            # In this case frop this row since it is redundant.
            df_qualifications.drop(df_qualifications.loc[df_qualifications.iloc[:, 0] == self.c.COUPLE_NR].index, inplace=True)


            # Replace the adjudicator names by only their reference letter.
            # Replace the couple names by only their number.
            couple_nrs = df_qualifications.iloc[0, :].str.extract(r'(\d+)\D*')
            couple_nrs_list = couple_nrs[0].tolist()
            couple_nrs_list[0] = self.cNR
            df_qualifications.columns = couple_nrs_list
            # Drop the first row since it contains only the names of the couples and is thus 
            # equivalent to the new column names.
            df_qualifications = df_qualifications[1:]

            # After parsing all qualification rounds, save them in individual tables for each round.
            for r in range(self.nr_rounds):
                # Split the values and map them to new columns. These correspond to the marks of the adjudicaotrs in order.
                round_marks = df_qualifications.iloc[row_id_marks + r * row_id_round_offset, 1:]
                round_marks.replace(float('Nan'), str('- ' * self.nr_adjudicators)[:-1], inplace=True)
                df_round_marks = round_marks.str.split(' ', expand=True)
                df_round_marks.columns = self.get_adjudicator_ids() # TODO
                df_round_marks[self.cNR] = list(df_qualifications.columns)[1:]
                df_round_marks.set_index(self.cNR, drop=True, inplace=True)
                df_round_marks.index.name = self.cNR

                # Etract the sums.
                sums = df_qualifications.iloc[row_id_sums + r * row_id_round_offset, 1:]
                sums.replace(float('NaN'), -1, inplace=True)
                df_round_marks[self.cSUM] = sums

                # Extract the qualification indications, and replace the values by binary ones.
                quali = df_qualifications.iloc[row_id_quali + r * row_id_round_offset, 1:]
                quali.replace(float('NaN'), False, inplace=True)
                quali.replace('X', True, inplace=True)
                # Add the sums as column to the DataFrame.
                df_round_marks[self.cQUALI] = quali

                # Store this round.
                self.df_qualifications.append(df_round_marks)
                if self.save_to_file: self.df_qualifications.to_csv(Path(f.DIR_DATA, f.FN.CSV_QUALI_ROUNDX.format(r)))
                
        except Exception as e:
            print(f"Error when parsing the finals table:\n{e}")
            self.comp_was_cancelled = True
            return False
        
        return True

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
        self._parse_ranking_list_df()

        self._parse_finals_df()

        self._parse_qualification_round_df()

        # Add additional information to the general information table.
        additional_info: List[Dict[str, str]] = {
            self.rNR_ROUNDS:    self.nr_rounds + 1,   
            self.rNR_ADJDCTRS:  self.nr_adjudicators, 
            self.rNR_COUPLES:   self.nr_couples
        }
        df_additional_info = pd.DataFrame(list(additional_info.items()), columns=[self.cCATEGORY, self.cVALUE])

        self.df_general_info = pd.concat([self.df_general_info, df_additional_info])
        if self.save_to_file: self.df_general_info.to_csv(Path(f.DIR_DATA, f.FN.CSV_GENERAL_INFO))        

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