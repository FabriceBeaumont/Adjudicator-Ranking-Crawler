from typing import List, Dict, Tuple, Set
from enum import Enum
from urllib.parse import urlparse
import pandas as pd
import os
from abc import ABC, abstractmethod

DIR_RESULTS: str        = "results"
DIR_CRAWLED_SITES: str  = "crawled_websites"
DIR_COMP_TABLES: str    = "comp_tables"
DIR_CRAWLED_LINKS: str  = "crawled_links"

class FilenameList(ABC):
    # Filenames for tables in a single tournament.
    GENERAL_INFO: str = "general_info.csv"
    QUALI_ROUNDX: str = "quali_round{}.csv"
    FINALS: str       = "finals.csv"
    RANKING_LIST: str = "ranking.csv"
    ADJUDICATORS: str = "adjudicators.csv"
    # Filenames to maintain lists of urls.
    FIND_CLUBS: str         = "find_clubs.csv"
    CLUB_LIST: str          = "clubs.csv"
    FIND_TOURNAMENTS: str   = "find_tournaments.csv"
    COMPETITION_LIST: str   = "competitions.csv"

class ScopeList(ABC):
    LOCAL: str          = "Local"           # TSK
    REGIONAL: str       = "Regional"        # Landesmeisterschaft
    NATIONAL: str       = "National"        # DTV
    INTERNATIONAL: str  = "International"   # WDSF (auch Rangliste w.g. vieler internationaler Wertungsrichter)

class FederalStateList(ABC):
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


class ClubsDf(ABC):
    """ This class defines the DataFrame to hold urls to clubs in Germany.
        The class is able to load, save and append to the DataFrame.
    """
    _instance = None
    # Define the columns of the DataFrame.
    cURL: str            = 'Url'
    cFEDERAL_STATE: str  = 'Federal state'
    cCRAWL_DATE: str     = 'Crawl date'

    COLUMNS: str            = [cURL, cFEDERAL_STATE, cCRAWL_DATE]
    PATH: str               = f"{DIR_RESULTS}/{FilenameList.CLUB_LIST}"
    CLUB_HINTS_PATH: str    = f"{DIR_RESULTS}/{FilenameList.FIND_CLUBS}"

    BAD_LINK_PREFIXES: List[str] = ['mailto', 'tel', '#', '?s']
    BAD_LINK_POSTFIXES: List[str] = ['.jpy', '.png']

    # There is only one such table, thus this class follows the Singleton pattern.
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(FindTournamentsDf, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._initialized = True

        # To read the already existant data use the DataFrame.
        self.df= self._load_df()
        # For edit operations (adding new urls), use the dictionary version.
        # Do not forger to save the edits by calling the 'save_df' method.
        self.df_dict = self.df.to_dict(orient='records')

    def _load_df(self) -> pd.DataFrame:
        if not os.path.exists(self.PATH): 
            return pd.DataFrame(columns=self.COLUMNS)
        return pd.read_csv(self.PATH)
    
    def url_is_known(self, url: str) -> bool:
        return url in [d[self.cURL] for d in self.df_dict]

    def get_clubs_from_federal_state(self, state: FederalStateList) -> List[str]:
        return self.df[self.df[self.cFEDERAL_STATE] == state.value][self.cURL].tolist()

    def get_club_hint_df(self) -> pd.DataFrame:
        if not os.path.exists(self.CLUB_HINTS_PATH): return pd.DataFrame(columns=[self.cURL, self.cFEDERAL_STATE, self.cCRAWL_DATE])
        return pd.read_csv(self.CLUB_HINTS_PATH)
    
    def add_url(self, url: str, federal_state: str, crawl_date: str) -> None:
        # If the 'url' is already presend in the dictoinary, do not add it.
        if self.url_is_known(url): return None
        
        self.df_dict.append(dict(self.COLUMNS, [url, federal_state, crawl_date]))
        return None

    def save_df(self) -> None:
        # Create the new DataFrame only now, after adding rows, 
        # since dynamically adding rows to a DataFrame leads pandas to expensively allocate memory on the fly.
        self.df = pd.DataFrame(self.df_dict)
        self.df.to_csv(self.PATH, index=False)

class FindTournamentsDf(ABC):
    """ This class defines the DataFrame to hold urls to websites where tournament sites are stored.
        The class is able to load, save and append to the table.
    """
    _instance = None
    # Define the columns of the DataFrame.
    cURL: str            = 'Url'
    cSCOPE: str          = 'Scope'
    cFEDERAL_STATE: str  = 'Federal state'
    cCRAWL_DATE: str     = 'Crawl date'
    cORIGIN: str         = 'Origin'

    COLUMNS: str            = [cURL, cSCOPE, cFEDERAL_STATE, cCRAWL_DATE, cORIGIN]
    PATH: str               = f"{DIR_RESULTS}/{FilenameList.FIND_TOURNAMENTS}"
    CLUB_HINTS_PATH: str    = f"{DIR_RESULTS}/{FilenameList.FIND_CLUBS}"

    # To identify that a site contains competition results.
    KEY_CONTENT: str     = '<meta name="GENERATOR" content="TopTurnier">'
    BAD_LINK_PREFIXES: List[str] = ['mailto', 'tel', '#', '?s']
    BAD_LINK_POSTFIXES: List[str] = ['.jpy', '.png']

    # There is only one such table, thus this class follows the Singleton pattern.
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(FindTournamentsDf, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._initialized = True

        # To read the already existant data use the DataFrame.
        self.df= self._load_df()
        # For edit operations (adding new urls), use the dictionary version.
        # Do not forger to save the edits by calling the 'save_df' method.
        self.df_dict = self.df.to_dict(orient='records')

    def _load_df(self) -> pd.DataFrame:
        if not os.path.exists(self.PATH): 
            return pd.DataFrame(columns=self.COLUMNS)
        return pd.read_csv(self.PATH)

    def url_is_known(self, url: str) -> bool:
        return url in [d[self.cURL] for d in self.df_dict]

    def get_local_urls_list(self) -> List[str]:
        return self.df[self.df[self.cSCOPE] == ScopeList.LOCAL][self.cURL].tolist()
    
    def get_regional_urls_list(self) -> List[str]:
        return self.df[self.df[self.cSCOPE] == ScopeList.REGIONAL][self.cURL].tolist()

    def get_national_urls_list(self) -> List[str]:
        return self.df[self.df[self.cSCOPE] == ScopeList.NATIONAL][self.cURL].tolist()

    def get_club_hint_df(self) -> pd.DataFrame:
        if not os.path.exists(self.CLUB_HINTS_PATH): return pd.DataFrame(columns=[self.cURL, self.cFEDERAL_STATE, self.cCRAWL_DATE])
        return pd.read_csv(self.CLUB_HINTS_PATH)
    
    def add_url(self, url: str, scope: str, federal_state: str, crawl_date: str, origin: str) -> None:
        # If the 'url' is already presend in the dictoinary, do not add it.
        if self.url_is_known(url): return None
        
        self.df_dict.append(dict(self.COLUMNS, [url, scope, federal_state, crawl_date, origin]))
        return None

    def save_df(self) -> None:
        # Create the new DataFrame only now, after adding rows, 
        # since dynamically adding rows to a DataFrame leads pandas to expensively allocate memory on the fly.
        self.df = pd.DataFrame(self.df_dict)
        self.df.to_csv(self.PATH, index=False)

class CompetitionsDf(ABC):
    """ This class defines the DataFrame to hold competition links.
        It is able to load, save and create to the table.
    """
    _instance = None
    # Define the columns of the DataFrame.
    cURL: str            = 'Url'
    cORIGIN: str         = 'Origin'
    cCRAWL_DATE: str     = 'Crawl date'
    cCOMPETITION_ID: str = 'Competition ID'
    cTOURNAMENT_ID: str  = 'Tournament ID'
    cWDSF: str           = 'is WDSF'
    cPROCESSED: str      = 'Processed'

    COLUMNS: str    = [cTOURNAMENT_ID, cCOMPETITION_ID, cURL, cORIGIN, cCRAWL_DATE, cPROCESSED]
    PATH: str       = f"{DIR_RESULTS}/{FilenameList.COMPETITION_LIST}"

    # To identify that a site points to competition sites.
    KEY_URL_ANCHOR: str  = '/index.htm'
    BAD_LINK_PREFIXES: List[str] = ['mailto', 'tel', '#', '?s']
    BAD_LINK_POSTFIXES: List[str] = ['.jpy', '.png']

    # There is only one such table, thus this class follows the Singleton pattern.
    def __new__(cls):
            if cls._instance is None:
                cls._instance = super(CompetitionsDf, cls).__new__(cls)
            return cls._instance

    def __init__(self):
        if not hasattr(self, '_initialized'):
            self._initialized = True

            self.df = self._init_df()

    def _init_df(self) -> pd.DataFrame:
        if not os.path.exists(self.PATH):
            return pd.DataFrame(columns=self.COLUMNS)
        return pd.read_csv(self.PATH)

class C_international(ABC):
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

class C_en(C_international):
    def __init__(self):
        super().__init__()

class C_de(C_international):    
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


def get_constants_in_language(language: str) -> C_international:
    if language == 'de':
        return C_de()
    else:
        return C_en()

# def parse_dance_name(dance_name: str, const_class) -> str:
#     if dance_name == const_class.LW_l: return const_class.LW_s
#     if dance_name == const_class.TG_l: return const_class.TG_s
#     if dance_name == const_class.WW_l: return const_class.WW_s
#     if dance_name == const_class.SF_l: return const_class.SF_s
#     if dance_name == const_class.QS_l: return const_class.QS_s
#     if dance_name == const_class.SB_l: return const_class.SB_s
#     if dance_name == const_class.CC_l: return const_class.CC_s
#     if dance_name == const_class.RB_l: return const_class.RB_s
#     if dance_name == const_class.PD_l: return const_class.PD_s
#     if dance_name == const_class.JV_l: return const_class.JV_s

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

# def get_tournament_links_df() -> pd.DataFrame:
#     return pd.read_csv(get_competition_links_csv_path())

# def get_competition_links_csv_path() -> str:
#     return f"{results_dir}/competition_links.csv"

# def get_adjudicator_links_csv_path() -> str:
#     return f"{results_dir}/adjudicator_links.csv"

# def get_athletes_links_csv_path() -> str:
#     return f"{results_dir}/athletes_links.csv"

# def get_all_url_list_path(url: str) -> str:
#     return f"{get_url_list_path(url)}/all_urls.npy"

# def get_positive_url_path(url: str) -> str:
#     return f"{get_url_list_path(url)}/known_hits.npy"

# def get_negative_urls_path(url: str) -> str:
#     return f"{get_url_list_path(url)}/negatives.npy"