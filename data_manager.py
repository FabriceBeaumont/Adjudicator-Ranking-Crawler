from typing import List, Dict, Tuple, Set
from urllib.parse import urlparse
import pandas as pd
import os
from datetime import date

import data_manager as c
import web_crawler as wc
import competition_reader as comp_reader
import filenames as f

class ScopeList():
    LOCAL: str          = "Local"           # TSK
    REGIONAL: str       = "Regional"        # Landesmeisterschaft
    NATIONAL: str       = "National"        # DTV
    INTERNATIONAL: str  = "International"   # WDSF (auch Rangliste w.g. vieler internationaler Wertungsrichter)

class FederalStateList():
    BADEN_WUERTTEMBERG: str = 'Baden-Wuerttemberg'
    BAYERN: str             = 'Bayern'
    BERLIN: str             = 'Berlin'
    BRANDENBURG: str        = 'Brandenburg'
    BREMEN: str             = 'Bremen'
    HAMBURG: str            = 'Hamburg'
    HESSEN: str             = 'Hessen'
    MECKLENBURG_VORPOMMERN: str = 'Mecklenburg-Vorpommern'
    NIEDERSACHSEN: str      = 'Niedersachsen'
    NRW: str                = 'Nordrhein-Westfalen'
    RHEINLAND_PFALZ: str    = 'Rheinland-Pfalz'
    SAARLAND: str           = 'Saarland'
    SACHSEN: str            = 'Sachsen'
    SACHSEN_ANHALT: str     = 'Sachsen-Anhalt'
    SCHLESWIG_HOLSTEIN: str = 'Schleswig-Holstein'
    THUERINGEN: str         = 'Thüringen'

class ClubsTable():
    """ This class defines the DataFrame to hold urls to clubs in Germany.
        The class is able to load, save and append to the DataFrame.
    """
    _instance = None
    # Define the columns of the DataFrame.
    cURL: str            = 'Url'
    cFEDERAL_STATE: str  = 'Federal state'    
    cORIGIN: str         = 'Origin'
    cFOUND_COMPS: str    = 'Found competitions'
    cCRAWL_DATE: str     = 'Crawl date'

    COLUMNS: str            = [cURL, cFEDERAL_STATE, cORIGIN, cFOUND_COMPS, cCRAWL_DATE]
    PATH: str               = f"{f.DIR_DATA}/{f.FN.CSV_CLUB}"
    CLUB_HINTS_PATH: str    = f"{f.DIR_DATA}/{f.FN.CSV_FIND_CLUBS}"

    BAD_URL_SAMPLE: List[str]     = ['login', 'Login', 'LOGIN']
    BAD_LINK_PREFIXES: List[str]  = ['mailto', 'tel', '#', '?s']
    BAD_LINK_POSTFIXES: List[str] = ['.jpy', '.png', '.pdf']

    # There is only one such table, thus this class follows the Singleton pattern.
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ClubsTable, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._initialized = True

        # To read the already existant data use the DataFrame.
        self.df= self._load_df()
        self.df_changed: bool = False
        # For edit operations (adding new urls), use the dictionary version.
        # Do not forget to save the edits by calling the 'save_df' method.
        self.df_dict_list: List[Dict[str, str]] = self.df.to_dict(orient='records')
        self.df_dict_changed: bool = False

    def _load_df(self) -> pd.DataFrame:
        if not os.path.exists(self.PATH): 
            return pd.DataFrame(columns=self.COLUMNS)
        return pd.read_csv(self.PATH)
    
    def url_is_known(self, url: str) -> bool:
        return url in [d[self.cURL] for d in self.df_dict_list]

    def get_clubs_from_federal_state(self, state: FederalStateList) -> List[str]:
        return self.df[self.df[self.cFEDERAL_STATE] == state.value][self.cURL].tolist()

    def get_club_hint_df(self) -> pd.DataFrame:
        if not os.path.exists(self.CLUB_HINTS_PATH): return pd.DataFrame(columns=[self.cURL, self.cFEDERAL_STATE, self.cCRAWL_DATE])
        return pd.read_csv(self.CLUB_HINTS_PATH)
    
    def add_url_to_dict(self, url: str, federal_state: str, origin: str, crawl_date: str) -> bool:
        # If the 'url' is already presend in the dictoinary, do not add it.
        if self.url_is_known(url): return False
        
        # When adding a new url, the default value for if a competition has been found there is 'False' - as in 'not yet'.
        found_comps: bool = None

        self.df_dict_list.append(dict(zip(self.COLUMNS, [url, federal_state, origin, found_comps, crawl_date])))
        self.df_dict_changed = True
        return True
    
    def add_urls_to_dict(self, urls: List[str], federal_states: List[str], origins: List[str], crawl_date: str) -> int:        
        equal_lenghts = [len(urls) == len(x) for x in [federal_states, origins]]
        if not all(equal_lenghts):
            print("Got lists with different lenghts! Noting was added.")
            return 0
        
        new_urls_ctr: int = 0
        for i, u in enumerate(urls):
            is_new = self.add_url_to_dict(u, federal_states[i], origins[i], crawl_date)
            if is_new: new_urls_ctr += 1
        return new_urls_ctr
    
    def update_df_dict(self) -> None:
        ''' Update the dictionary using the data from the DataFrame. '''
        self.df_dict_list = self.df.to_dict(orient='records')
        self.df_changed = False
        self.df_dict_changed = False

    def update_df(self) -> None:
        ''' Update the DataFrame using the data from the dictionary. '''
        self.df = pd.DataFrame(self.df_dict_list)
        self.df_changed = False
        self.df_dict_changed = False

    def save_df_dict(self) -> None:
        self.update_df()
        print(f"Saving clubs dataframe (dict) to file: '{self.PATH}'")
        self.df.to_csv(self.PATH, index=False)

    def save_df(self) -> None:
        self.update_df_dict()
        print(f"Saving clubs dataframe to file: '{self.PATH}'")
        self.df.to_csv(self.PATH, index=False)

    def save(self, prioritise_df: bool = True) -> None:
        if self.df_changed and not self.df_dict_changed:
            self.save_df()
        elif not self.df_changed and self.df_dict_changed:
            self.save_df_dict()
        elif prioritise_df:
            self.save_df()
        else: 
            self.save_df_dict()

    def found_competition_on_club(self, club_site: str, found_comp: bool=True) -> bool:
        if club_site not in self.df[self.cURL].tolist(): return False

        self.df.loc[self.df[self.cURL] == club_site, self.cFOUND_COMPS] = found_comp
        self.df_changed = True
        return True

    def get_filtered_df(self, club_urls: List[str] = None, federal_states: List[str] = None, origins: List[str] = None, found_comp: bool = None ) -> pd.DataFrame:
        target_df = self.df
        if club_urls is not None:
            target_df = target_df.loc[target_df[c.ClubsTable.cURL].isin(club_urls)]
        if federal_states is not None:
            target_df = target_df.loc[target_df[c.ClubsTable.cFEDERAL_STATE].isin(federal_states)]
        if origins is not None:
            target_df = target_df.loc[target_df[c.ClubsTable.cORIGIN].isin(origins)]
        if found_comp is not None:
            target_df = target_df.loc[target_df[c.ClubsTable.cFOUND_COMPS] == found_comp]
        return target_df

    def update_clubs_using_hints(self) -> int:
        new_urls_ctr: int = 0
        nr_club_sites: int = 0
        # Get the crawl date of possible new entries.
        crawl_date: str = date.today().strftime(c.TIME_STR_FORMAT)
        # Iterate over all club hint urls.
        for i, row in self.get_club_hint_df().iterrows():
            url = row[self.cURL]
            federal_state = row[self.cFEDERAL_STATE]

            print(f"Crawling for new clubs on webpage '{url}'...")
            # Crawl for any links, that point away from the website.
            club_sites, _ = wc.crawl_href_links_on_webpage(
                url=url, 
                url_must_not_contain_any=[c.get_site_name_from_url(url)] + c.ClubsTable.BAD_URL_SAMPLE, 
                forbidden_url_prefixes=c.ClubsTable.BAD_LINK_PREFIXES, 
                forbidden_url_postfixes=c.ClubsTable.BAD_LINK_POSTFIXES
            )
            # Add the sites to the dict.
            n_club_sites = len(club_sites)
            nr_club_sites += n_club_sites
            federal_states = [federal_state] * n_club_sites
            origins = [url] * n_club_sites
            tmp_ctr = self.add_urls_to_dict(club_sites, federal_states, origins, crawl_date)
            print(f"Found {tmp_ctr} new possible club websites here!")
            print()
            new_urls_ctr += tmp_ctr
        
        # Create the new DataFrame only now, after adding rows, 
        # since dynamically adding rows to a DataFrame leads pandas to expensively allocate memory on the fly.
        self.save_df_dict()
        print()
        print(f"Found {new_urls_ctr} new possible club websites on {nr_club_sites} club sites in total!")
        return new_urls_ctr

class Competition():
    
    def __init__(self, url: str, scope: str, federal_state: str, origin: str, club: str, crawl_date: str, 
                        comp_date: str, comp_organizer: str, comp_moc: str, comp_age_group: str, comp_level: str):
        
        self.url: str = url
        self.scope: str = scope
        self.federal_state: str = federal_state
        self.origin: str = origin
        self.club: str = club
        self.crawl_date: str = crawl_date
        self.comp_date: str = comp_date
        self.comp_organizer: str = comp_organizer
        self.comp_moc: str = comp_moc
        self.comp_age_group: str = comp_age_group
        self.comp_level: str = comp_level

        # TODO: Add function: Get all couples
        # TODO: Add function: Get all adjudicators
        # TODO: Add function: Add couples without repetition to couples.csv
        # TODO: Add function: Add adjudicators without repetition to adjudicator.csv
    
    def load_results_from_url(self):
        pass

    def save_results_to_dir(self, dir_name: str = None):
        pass

class CompetitionsTable():
    """ This class defines the DataFrame to hold urls to websites where tournament sites are stored.
        The class is able to load, save and append to the table.
    """
    _instance = None
    # Define the columns of the DataFrame.
    cURL: str            = 'Url'
    cSCOPE: str          = 'Scope'
    cFEDERAL_STATE: str  = 'Federal State'
    cORIGIN: str         = 'Origin'
    cCLUB: str           = 'Club'
    cCOMPETITION_ID: str = 'Competition ID'
    cTOURNAMENT_ID: str  = 'Tournament ID'
    cCRAWL_DATE: str     = 'Crawl Date'

    cCOMP_DATE: str               = 'Comp. Date'
    cCOMP_NAME: str               = 'Comp. Name'
    cCOMP_ORGANISER: str          = 'Comp. Organiser'
    cCOMP_CLASS: str              = 'Comp. Class'
    cCOMP_AGE_GROUP: str          = 'Comp. Age Group'
    cCOMP_LEVEL: str              = 'Comp. Level'
    cCOMP_DISCIPLINE: str         = 'Comp. Discipline'    
    cWDSF: str                    = 'is WDSF'
    cCANCELLED: str               = 'Got cancelled'

    cPROCESSED: str               = 'Processed'
    cCOMMENT: str                 = 'Comment'

    COLUMNS: str            = [cURL, cSCOPE, cFEDERAL_STATE, cORIGIN, cCLUB, cTOURNAMENT_ID, cCRAWL_DATE,
                               cCOMP_DATE, cCOMP_NAME, cCOMP_ORGANISER, cCOMP_CLASS, cCOMP_AGE_GROUP, cCOMP_LEVEL, cCOMP_DISCIPLINE, 
                               cWDSF, cPROCESSED, cCANCELLED, cCOMMENT]
    PATH: str               = f"{f.DIR_DATA}/{f.FN.CSV_TOURNAMENTS}"
    CLUB_HINTS_PATH: str    = f"{f.DIR_DATA}/{f.FN.CSV_FIND_CLUBS}"

    # To identify that a site is a competition site, look for this url-ending.
    KEY_COMP_URL_ENDS: List[str]    = ['/index.htm']
    # Notice that there may be empty competition links, since all competitions are displayed in an array of two columns.
    # Filter these invalid links.
    KEY_FORBIDDEN_ANCHORS: List[str] = ['index.htm']
    # To identify that a site contains links to tournaments.
    KEY_URL_PARTS: List[str]    = ['urnier', 'rgebnisse', 'ompetition', 'esults']
    # To identify that a site contains competition results.
    KEY_CONTENTS: List[str]     = ['content="TopTurnier', 'TopTurnierDigital', 'topturnier.css']
    BAD_URL_SAMPLE: List[str]   = ['login', 'Login', 'LOGIN', 'http://www.TopTurnier.de']
    BAD_LINK_PREFIXES: List[str]    = ['mailto', 'tel', '#', '?s']
    BAD_LINK_POSTFIXES: List[str]   = ['.jpy', '.png', '.pdf']

    # There is only one such table, thus this class follows the Singleton pattern.
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(CompetitionsTable, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._initialized = True

        self._tournament_id_ctr: int = None

        # To read the already existant data use the DataFrame.
        self.df = self._load_df()
        self.df_changed: bool = False
        self._update_tournament_id_ctr()
        # For edit operations (adding new urls), use the dictionary version.
        # Do not forget to save the edits by calling the 'save_df' method.
        self.df_dict_list = self.df.to_dict(orient='records')
        self.df_dict_changed: bool = False

    def _load_df(self) -> pd.DataFrame:
        if not os.path.exists(self.PATH) or os.path.getsize(self.PATH) < 2: 
            return pd.DataFrame(columns=self.COLUMNS)
        return pd.read_csv(self.PATH)

    def _update_tournament_id_ctr(self) -> None:
        max_id = 0
        if not self.df.empty:
            # Try to get the maximum value in the cTOURNAMENT_ID column            
            max_id = self.df[self.cTOURNAMENT_ID].max()        
        self._tournament_id_ctr = max_id + 1

    def get_tournament_id(self) -> int:
        self._tournament_id_ctr += 1
        return self._tournament_id_ctr - 1

    def url_is_known_in_df_dict_list(self, url: str) -> bool:
        return url in [d[self.cURL] for d in self.df_dict_list]
    
    def comp_is_known_in_df(self, comp_organiser: str, comp_age_group: str, comp_class) -> Tuple[bool, List[int]]:
        is_present = (self.df[self.cCOMP_ORGANISER] == comp_organiser) & \
            (self.df[self.cCOMP_AGE_GROUP] == comp_age_group) & \
            (self.df[self.cCOMP_CLASS] == comp_class)

        # Check if all conditions are satisfied in any row
        comp_ids = [i for i, is_hit in enumerate(is_present) if is_hit]
        return is_present.any(), comp_ids

    def get_local_urls_list(self) -> List[str]:
        return self.df[self.df[self.cSCOPE] == ScopeList.LOCAL][self.cURL].tolist()
    
    def get_regional_urls_list(self) -> List[str]:
        return self.df[self.df[self.cSCOPE] == ScopeList.REGIONAL][self.cURL].tolist()

    def get_national_urls_list(self) -> List[str]:
        return self.df[self.df[self.cSCOPE] == ScopeList.NATIONAL][self.cURL].tolist()

    def get_club_hint_df(self) -> pd.DataFrame:
        if not os.path.exists(self.CLUB_HINTS_PATH): return pd.DataFrame(columns=[self.cURL, self.cFEDERAL_STATE, self.cCRAWL_DATE])
        return pd.read_csv(self.CLUB_HINTS_PATH)
    
    def add_url_to_dict(self, url: str, scope: str, federal_state: str, origin: str, club: str, crawl_date: str, 
                        comp_date: str, comp_name: str, comp_organizer: str, comp_class: str, comp_age_group: str, comp_level: str,
                        comp_discipline: str, is_english: bool, cancelled: bool) -> bool:
        # If the 'url' is already presend in the dictoinary, do not add it.
        if self.url_is_known_in_df_dict_list(url): return False

        self.df_dict_list.append(dict(zip(self.COLUMNS, [url, scope, federal_state, origin, club, self.get_tournament_id(), crawl_date,
                                                         comp_date, comp_name, comp_organizer, comp_class, comp_age_group, comp_level, comp_discipline, is_english, False, cancelled, ''])))
        
        self.df_dict_changed = True
        return True
    
    def update_df_dict(self) -> None:
        ''' Update the dictionary using the data from the DataFrame. '''
        self.df_dict_list = self.df.to_dict(orient='records')
        self.df_changed = False
        self.df_dict_changed = False

    def update_df(self) -> None:
        ''' Update the DataFrame using the data from the dictionary. '''
        self.df = pd.DataFrame(self.df_dict_list)
        self.df_changed = False
        self.df_dict_changed = False

    def save_df_dict(self) -> None:
        self.update_df()
        print(f"Saving clubs dataframe (dict) to file: '{self.PATH}'")
        self.df.to_csv(self.PATH, index=False)

    def save_df(self) -> None:
        self.update_df_dict()
        print(f"Saving clubs dataframe to file: '{self.PATH}'")
        self.df.to_csv(self.PATH, index=False)

    def save(self, prioritise_df: bool = True) -> None:
        if self.df_changed and not self.df_dict_changed:
            self.save_df()
        elif not self.df_changed and self.df_dict_changed:
            self.save_df_dict()
        elif prioritise_df:
            self.save_df()
        else: 
            self.save_df_dict()

    def add_competitions_to_dict(self, urls: List[str], scope, federal_state, origin, club, crawl_date) -> int:
        new_comp_ctr: int = 0
        for url in urls:
            # If the url is listed, is has been processed. 
            if self.url_is_known_in_df_dict_list(url): continue

            cr = comp_reader.CompetitionReader(url)
            cr.read_comp_info()

            # If the url is not present, there may still be the same competition present with another url.
            # Assert that there cannot be two competitions with same age group, level, discipline and organizer 
            # at the same date. If such a competition is already listed, append the new url to remarks and continue.
            # Notice that this test is based on the DataFrame, while we first add the list of urls to the dictionary list.
            # We assert that in this list no two competitions are listed twice.
            comp_is_known, comp_ids = self.comp_is_known_in_df(cr.comp_organiser, cr.comp_age_group, cr.comp_class)

            if comp_is_known:
                # In this case a competition has been found with two different urls. Add the url to the origins value.
                self.update_df()
                for i in comp_ids:
                    if url not in self.df.loc[i, self.cCOMMENT]:
                        self.df.loc[i, self.cCOMMENT] = self.df.loc[i, self.cCOMMENT] + ', ' + origin
                self.update_df_dict()
                continue
            
            # If neither the url nor the compeitions are known, add the competition to the dict.
            self.add_url_to_dict(url=url, scope=scope, federal_state=federal_state, origin=origin, club=club, crawl_date=crawl_date,
                                 comp_date=cr.comp_date, comp_name=cr.comp_name, comp_organizer=cr.comp_organiser,
                                 comp_class=cr.comp_class, comp_age_group=cr.comp_age_group, comp_level=cr.comp_level,
                                 comp_discipline=cr.comp_discipline, is_english=cr.language_name, cancelled=cr.comp_was_cancelled)

            new_comp_ctr += 1
        
        self.update_df()
        return new_comp_ctr            

    def update_tournaments_using_clubs(self, 
                                       select_clubs: List[str] = None, 
                                       select_federal_states: List[str] = None,
                                       select_origins: List[str] = None,
                                       select_found_comp: bool = None,
                                       update_clubs_list: bool = False, 
                                       verbose: bool = False) -> int:
        """ Crawling for tournament sites involves two steps:
            1. Craw a club site for any urls on it ('candidates'), that may leed to a site containing all tournaments.
            2. Craw the candidates for actual tournament sites.
        """
        clubs = ClubsTable()
        if update_clubs_list:
            clubs.update_clubs_using_hints()
        
        crawl_date: str = date.today().strftime(c.TIME_STR_FORMAT)        
        candidate_sites: List[str] = []
        # Initialize counters.
        comp_ctr: int      = 0
        new_comp_ctr: int  = 0
        tour_ctr: int      = 0
        new_tour_ctr: int  = 0
        club_ctr: int      = 0

        # Filter the clubs.
        # If any rows from the clubs table are specified, filter for them first.
        target_df = clubs.get_filtered_df(select_clubs, select_federal_states, select_origins, select_found_comp)

        # Iterate over all club sites.
        num_clubs = len(target_df)
        for _, row in target_df.iterrows():
            club_ctr += 1
            # Extract the data from the DataFrame row.
            club_url: str       = row[clubs.cURL]
            federal_state: str  = row[clubs.cFEDERAL_STATE]
            # Counters for this club.
            club_comp_ctr: int     = 0
            new_club_comp_ctr: int = 0
            club_tour_ctr: int     = 0
            new_club_tour_ctr: int = 0

            print_percentage: str = f"{club_ctr}\{num_clubs}, {club_ctr/num_clubs*100:.2f}%"
            print(f"[{print_percentage}] Searching for results sites on club webpage: '{get_site_name_from_url(club_url)}'")
            
            # On the club website, try to find a webpage, which links to all tournaments of the club.
            # Therefore crawl all links on the website and check for keywords in the url.
            candidate_sites, _ = wc.crawl_href_links_on_webpage(
                url=club_url,
                url_shall_contain_some=c.CompetitionsTable.KEY_URL_PARTS,
                url_must_not_contain_any=c.CompetitionsTable.BAD_URL_SAMPLE,
                forbidden_url_prefixes=c.CompetitionsTable.BAD_LINK_PREFIXES,
                forbidden_url_postfixes=c.CompetitionsTable.BAD_LINK_POSTFIXES,
                search_recursively=True,
                recursion_depth=2,
                verbose=verbose
            )
            num_candidates = len(candidate_sites)
            print(f"Saved {num_candidates} possible candidates for competition result sites from website '{club_url}'\n")

            # Next, test the candidate links on the site - they should link to tournament sites which are identified by a content keyword.
            rejected_club_sites: List[str] = []
            for j, candidate_url in enumerate(candidate_sites):
                print(f"[{print_percentage} => {j}\{num_candidates}, {j/num_candidates*100:.2f}%] Searching for tournament links on candidate webpage : '{candidate_url}'")
                tmp_tournament_sites: List[str] = []
                tmp_tournament_sites, rejected_sites = wc.crawl_href_links_on_webpage(
                    url=candidate_url,            
                    website_contains_content_some=c.CompetitionsTable.KEY_CONTENTS,                    
                    url_must_not_contain_any=c.CompetitionsTable.BAD_URL_SAMPLE + rejected_club_sites,
                    forbidden_url_prefixes=c.CompetitionsTable.BAD_LINK_PREFIXES,
                    forbidden_url_postfixes=c.CompetitionsTable.BAD_LINK_POSTFIXES,
                    verbose=verbose
                )
                # Since all candidates from this club url may link to similar sites,
                # keep track of those, that are rejected based on their content, since this test is time consuming.
                rejected_club_sites += rejected_sites

                # If the tournament site is actually already a competition site, assume that no tompetition site exists.
                found_comp: bool = False
                for url in tmp_tournament_sites:                    
                    if url.endswith(tuple(c.CompetitionsTable.KEY_COMP_URL_ENDS)):
                        is_new_ctr = self.add_competitions_to_dict([url],
                                                                   scope=c.ScopeList.LOCAL, 
                                                                   federal_state=federal_state,
                                                                   origin=candidate_url,
                                                                   club=club_url,
                                                                   crawl_date=crawl_date
                        )
                        club_comp_ctr += 1
                        new_club_comp_ctr += is_new_ctr
                        club_tour_ctr += 1
                        new_club_tour_ctr += is_new_ctr
                        found_comp: bool = False
                    else:
                        # From the tournament site, get all competition sites.
                        competition_sites: List[str] = []
                        competition_sites, _ = wc.crawl_href_links_on_webpage(
                            url=url,
                            url_shall_contain_all=c.CompetitionsTable.KEY_COMP_URL_ENDS,
                            website_contains_content_some=c.CompetitionsTable.KEY_CONTENTS,                    
                            url_must_not_contain_any=c.CompetitionsTable.BAD_URL_SAMPLE + rejected_club_sites,
                            forbidden_url_prefixes=c.CompetitionsTable.BAD_LINK_PREFIXES,
                            forbidden_url_postfixes=c.CompetitionsTable.BAD_LINK_POSTFIXES,
                            forbidden_anchors=c.CompetitionsTable.KEY_FORBIDDEN_ANCHORS,
                            verbose=verbose
                        )
                        if len(competition_sites) == 0: continue
                        is_new_ctr = self.add_competitions_to_dict(competition_sites,
                                                                   scope=c.ScopeList.LOCAL, 
                                                                   federal_state=federal_state,
                                                                   origin=url,
                                                                   club=club_url,
                                                                   crawl_date=crawl_date
                        )
                        club_comp_ctr += len(competition_sites)
                        new_club_comp_ctr += is_new_ctr
                        club_tour_ctr += 1
                        if is_new_ctr > 0: new_club_tour_ctr += 1
                        found_comp: bool = False

                # Mark the club site as a site with competition sites.
                clubs.found_competition_on_club(candidate_url, found_comp=found_comp)

                print(f"In club '{club_url}:'")
                print(f"Found {club_comp_ctr} competition{'s' if club_comp_ctr>1 else ''} ({new_club_comp_ctr} new)\n")
                print(f"Found {club_tour_ctr} competition{'s' if club_tour_ctr>1 else ''} ({new_club_tour_ctr} new)\n")
                comp_ctr += club_comp_ctr
                new_comp_ctr += new_club_comp_ctr
                tour_ctr += club_tour_ctr
                new_tour_ctr += new_club_tour_ctr

            # Save both the club table (possibly with new marks for competition sites), and the tournament table.
            print("#########################")
            print(">>> INTERMEDIATE SAVE <<<")
            print("#########################")
            clubs.save_df()
            self.save()

        print(f"Saved {comp_ctr} tournament webpages links in total ({new_comp_ctr} new).")
        print(f"Saved {tour_ctr} tournament webpages links in total ({new_tour_ctr} new).")
        return comp_ctr

class CompetitionsDf():
    """ This class defines the DataFrame to hold competition links.
        It is able to load, save and create to the table.
    """
    _instance = None
    # Define the columns of the DataFrame.
    cURL: str            = 'Url'
    cSCOPE: str          = 'Scope'
    cFEDERAL_STATE: str  = 'Federal state'
    cORIGIN: str         = 'Origin'
    cCLUB: str           = 'Club'
    cCRAWL_DATE: str     = 'Crawl date'
    cCOMPETITION_ID: str = 'Competition ID'
    cTOURNAMENT_ID: str  = 'Tournament ID'
    cWDSF: str           = 'is WDSF'
    cCLASS_AGE: str      = 'Age group'
    cCLASS_LEVEL: str    = 'Level'
    cPROCESSED: str      = 'Processed'

    COLUMNS: str    = [cTOURNAMENT_ID, cCOMPETITION_ID, cURL, cORIGIN, cCRAWL_DATE, cPROCESSED]
    PATH: str       = f"{f.DIR_DATA}/{f.FN.CSV_COMPETITION}"

    # To identify that a site points to competition sites.
    KEY_URL_ANCHOR: str  = '/index.htm'
    KEY_CONTENT: str     = 'content="TopTurnier"'
    BAD_URL_SAMPLE: List[str]  = ['login', 'Login', 'LOGIN', 'http://www.TopTurnier.de']
    BAD_LINK_PREFIXES: List[str] = ['mailto:', 'tel:', '#', '?s']
    BAD_LINK_POSTFIXES: List[str] = ['.jpg', '.png', '.pdf']

    # There is only one such table, thus this class follows the Singleton pattern.
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(CompetitionsDf, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._initialized = True

        self._competition_id_ctr: int = None

        # To read the already existant data use the DataFrame.
        self.df = self._load_df()
        self.df_changed: bool = False
        self._update_competition_id_ctr()
        # For edit operations (adding new urls), use the dictionary version.
        # Do not forget to save the edits by calling the 'save_df' method.
        self.df_dict_list = self.df.to_dict(orient='records')
        self.df_dict_changed: bool = False

    def _load_df(self) -> pd.DataFrame:
        if not os.path.exists(self.PATH) or os.path.getsize(self.PATH) < 2: 
            return pd.DataFrame(columns=self.COLUMNS)
        return pd.read_csv(self.PATH)

    def _update_competition_id_ctr(self) -> None:
        max_id = 0
        if not self.df.empty:
            # Try to get the maximum value in the cTOURNAMENT_ID column            
            max_id = self.df[self.cCOMPETITION_ID].max()        
        
        self._competition_id_ctr = max_id + 1

    def get_competition_id(self) -> int:
        self._competition_id_ctr += 1
        return self._competition_id_ctr - 1

    def url_is_known(self, url: str) -> bool:
        return url in [d[self.cURL] for d in self.df_dict_list]

    def get_local_comp_urls_list(self) -> List[str]:
        return self.df[self.df[self.cSCOPE] == ScopeList.LOCAL][self.cURL].tolist()
    
    def get_regional_urls_list(self) -> List[str]:
        return self.df[self.df[self.cSCOPE] == ScopeList.REGIONAL][self.cURL].tolist()

    def get_national_urls_list(self) -> List[str]:
        return self.df[self.df[self.cSCOPE] == ScopeList.NATIONAL][self.cURL].tolist()

    def get_club_hint_df(self) -> pd.DataFrame:
        if not os.path.exists(self.CLUB_HINTS_PATH): return pd.DataFrame(columns=[self.cURL, self.cFEDERAL_STATE, self.cCRAWL_DATE])
        return pd.read_csv(self.CLUB_HINTS_PATH)
    
    def add_url_to_dict(self, url: str, scope: str, federal_state: str, origin: str, club: str, crawl_date: str) -> bool:
        # If the 'url' is already presend in the dictoinary, do not add it.
        if self.url_is_known(url): return False
        
        self.df_dict_list.append(dict(zip(self.COLUMNS, [url, scope, federal_state,  origin, club, crawl_date])))
        self.df_dict_changed = True
        return True
    
    def add_urls_to_dict(self, urls: List[str], scope: str, federal_states: List[str], origins: List[str], clubs: List[str], crawl_date: str) -> int:
        equal_lenghts = [len(urls) == len(x) for x in [federal_states, origins, clubs]]
        if not all(equal_lenghts):
            print("Got lists with different lenghts! Noting was added.")
            return 0
        
        new_urls_ctr: int = 0
        for i, u in enumerate(urls):
            is_new = self.add_url_to_dict(u, scope, federal_states[i], origins[i], clubs[i], crawl_date)
            if is_new: new_urls_ctr += 1
        return new_urls_ctr

    def update_df_dict(self) -> None:
        ''' Update the dictionary using the data from the DataFrame. '''
        self.df_dict_list = self.df.to_dict(orient='records')
        self.df_changed = False
        self.df_dict_changed = False

    def update_df(self) -> None:
        ''' Update the DataFrame using the data from the dictionary. '''
        self.df = pd.DataFrame(self.df_dict_list)
        self.df_changed = False
        self.df_dict_changed = False

    def save_df_dict(self) -> None:
        self.update_df()
        print(f"Saving clubs dataframe (dict) to file: '{self.PATH}'")
        self.df.to_csv(self.PATH, index=False)

    def save_df(self) -> None:
        self.update_df_dict()
        print(f"Saving clubs dataframe to file: '{self.PATH}'")
        self.df.to_csv(self.PATH, index=False)

    def save(self, prioritise_df: bool = True) -> None:
        if self.df_changed and not self.df_dict_changed:
            self.save_df()
        elif not self.df_changed and self.df_dict_changed:
            self.save_df_dict()
        elif prioritise_df:
            self.save_df()
        else: 
            self.save_df_dict()
    
    def update_tournaments_using_clubs(self, 
                                       select_scopes: List[str] = None, 
                                       select_federal_states: List[str] = None,
                                       select_origins: List[str] = None,
                                       select_clubs: List[str] = None,
                                       update_tournament_list: bool = False,
                                       update_clubs_list: bool = False, 
                                       verbose: bool = False) -> int:
        """ Crawling for tournament sites involves two steps:
            1. Craw a club site for any urls on it ('candidates'), that may leed to a site containing all tournaments.
            2. Craw the candidates for actual tournament sites.
        """
        tournaments = CompetitionsTable()
        if update_tournament_list:
            tournaments.update_tournaments_using_clubs(update_clubs_list=update_clubs_list)

        # TODO: if tournament site is competition site, clone origin and add competition site to df

class AdjudicatorDf():
    _instance = None
    # Define the columns of the DataFrame.
    cFULL_NAME_AND_CLUB: str = 'Full name and club'
    cNAME: str               = 'Name'
    cSURNAME: str            = 'Surname'
    cCLUB: str               = 'Club'
    cID: str                 = 'Id'
    
    COLUMNS: str = [cFULL_NAME_AND_CLUB, cNAME, cSURNAME, cCLUB, cID]
    PATH: str    = f"{f.DIR_DATA}/{f.FN.CSV_ADJUDICATORS}"
    
    # There is only one such table, thus this class follows the Singleton pattern.
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(AdjudicatorDf, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._initialized = True

        self._competition_id_ctr: int = None

        # To read the already existant data use the DataFrame.
        self.df = self._load_df()
        self._update_adjudicator_id_ctr()

    def _load_df(self) -> pd.DataFrame:
        if not os.path.exists(self.PATH) or os.path.getsize(self.PATH) < 2: 
            return pd.DataFrame(columns=self.COLUMNS)
        return pd.read_csv(self.PATH)

    def _update_adjudicator_id_ctr(self) -> None:
        max_id = 0
        if not self.df.empty:
            # Try to get the maximum value in the cTOURNAMENT_ID column            
            max_id = self.df[self.cID].max()        
        
        self._competition_id_ctr = max_id + 1

    def get_adjudicator_id(self) -> int:
        self._competition_id_ctr += 1
        return self._competition_id_ctr - 1

    def save_df(self) -> None:        
        print(f"Saving clubs dataframe to file: '{self.PATH}'")
        self.df.to_csv(self.PATH, index=False)

    def is_known(self, full_name_and_club: str) -> Tuple[bool, List[int]]:
        is_known = (self.df[self.cFULL_NAME_AND_CLUB] == full_name_and_club)        
        # Check if all conditions are satisfied in any row
        comp_ids = [i for i, is_hit in enumerate(is_known) if is_hit]
        return is_known.any(), comp_ids
            
    def add_urls_to_dict(self, full_names_and_clubs: List[str], names: List[str], surnames: List[str], clubs: List[str]) -> int:
        equal_lenghts = [len(names) == len(x) for x in [surnames, clubs]]
        if not all(equal_lenghts):
            print("Got lists with different lenghts! Noting was added.")
            return 0
        
        new_urls_ctr: int = 0
        new_entries: List[Dict[str, str]] = []
        for i, full_name_and_club in enumerate(full_names_and_clubs):
            if self.is_known(full_name_and_club): 
                continue
            else:
                # Add the new adjudicator.
                new_entries.append({
                    self.cFULL_NAME_AND_CLUB: full_name_and_club, 
                    self.cNAME: names[i], 
                    self.cSURNAME: surnames[i], 
                    self.cCLUB: clubs[i],
                    self.cID: self.get_adjudicator_id()
                })
                new_urls_ctr += 1

        # Update the dictionary and save the changes.
        self.df = pd.concat([self.df, pd.DataFrame(new_entries)], axis=0, ignore_index=True)
        self.save_df()
        return new_urls_ctr


def get_site_name_from_url(url: str) -> str:
    """Strips a given url to the name of the main domain and return it."""
    return urlparse(url).netloc

def largest_common_prefix_path(l: List[str]) -> str:
    """ Returns the largest common prefix path from a list of strings. 
        This can be used to find the common origin from a list of paths or the
        common url path from a list of urls.
    """
    def condition(p: str) -> bool:
        for x in p:
            if not x.startswith(p):
                return False
        return True

    if len(l) == 0: return None

    anchors = l[0].split("/")
    anchor_limit = 1
    
    prefix = "".join(anchors[:anchor_limit])

    if not condition(prefix):
        return None
    
    while condition(prefix):
        anchor_limit += 1
        prefix = "".join(anchors[:anchor_limit])

    return "".join(anchors[:anchor_limit-1])



if __name__ == '__main__':
    tournaments = CompetitionsTable()
    tournaments.update_tournaments_using_clubs(select_clubs=['http://www.tsk-sankt-augustin.de'])