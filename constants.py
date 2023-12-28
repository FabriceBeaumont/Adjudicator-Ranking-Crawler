from typing import List, Dict, Tuple, Set
from enum import Enum
from urllib.parse import urlparse
from abc import ABC, abstractmethod

results_dir: str = "results"

url_main = "https://www.tanzsport.de/de/sportwelt/ergebnisse"

urls_main = [
    "https://www.tanzsportkreis-sankt-augustin.de/",
    "https://ttcrotgoldkoeln.de/",
    "https://www.tscbruehl.de/",
    "https://askania-tsc.de/",
    "http://www.rgc-nuernberg.de/",
    "https://www.gsc-muenchen.de/",
    "https://www.blau-silber-berlin.de/",
    "https://www.ggcbremen.de/",
    "https://www.rot-weiss-leipzig.de/",
    "https://blau-gold-darmstadt.de/",
    "https://www.tszdresden.de/",
    "https://www.boston-club.de/",
    "https://tszmittelrhein.de/",
]

urls_hot = [
    "https://www.tanzsportkreis-sankt-augustin.de/veranstaltungen/turnierergebnisse/",
    "https://ttcrotgoldkoeln.de/turniere/",
    "https://www.tscbruehl.de/index.php/service/turnierergebnisse",
    "https://askania-tsc.de/turnierergebnisse/",
    "http://www.rgc-nuernberg.de/seite/422510/turnierergebnisse.html",
    "https://www.gsc-muenchen.de/turniertanz/turnierergebnisse",
    "https://www.blau-silber-berlin.de/",
    "https://www.ggcbremen.de/de/ergebnisse",
    "https://www.rot-weiss-leipzig.de/veranstaltungen/turniere.html",
    "https://blau-gold-darmstadt.de/turniere/",
    "https://www.tszdresden.de/wp-contentuploadsarchiv/",
    "https://www.boston-club.de/",
    "https://tszmittelrhein.de/",
]

class filenames(Enum):
    general_info = "general_info.csv"
    quali_roundx = "quali_round{}.csv"
    finals       = "finals.csv"
    ranking_lst  = "ranking.csv"
    adjudicators = "adjudicators.csv"

class C_all(ABC):
    # Local keywords
    COMP_NAME: str      = 'Competition name'
    COMP_LINK: str      = 'Competition link' 
    TOUR_NAME: str      = 'Tournament name'
    TOUR_LINK: str      = 'Tournament link'
    BASE_URL: str       = 'Base url'
    DATE: str           = 'Crawl date'
    ID: str             = 'Tournament id'
    PROCESSED: str      = 'Processed'
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

    KEY_CONTENT: List[str]      = ["TopTurnier"]
    KEY_URL_ANCHOR: List[str]   = ['/index.htm']
    
    KEY_ORGANZIER: str          = 'Organizer:'
    KEY_MASTER_OF_CEREMONY: str = 'Master of Ceremony:' 
    COUPLE: str                 = 'Couple'
    PLACEMENT: str              = 'Placement'
    # The number is the identifyer number of the couple in the tournament.
    NR: str                     = 'Nr.'
    KEY_FINAL: str              = 'Final'
    KEY_ROUND: str              = 'round'
    KEY_ADJUDICATOR: str        = 'Adjudicator'

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

class C_en(C_all):
    def __init__(self):
        super().__init__()

class C_de(C_all):    
    # Define the dance names in german.
    LW_l = "Langsamer Walzer"
    LW_s = "LW"
    WW_l = "Wiener Walzer"
    WW_s = "WW"

    KEY_ORGANZIER = 'Veranstalter:'    
    KEY_MASTER_OF_CEREMONY = 'Ausrichter:'
    COUPLE = 'Paar'
    PLACEMENT = 'Platz'
    ROUND = 'Runde'
    NR = 'Nr.'
    KEY_FINAL = 'Endrunde'
    KEY_ROUND = 'runde'
    KEY_ADJUDICATOR = 'Wertungsrichter'

    def __init__(self):
        super().__init__()


get_constants_in_language = {
    'de': C_de(),
    'en': C_en()
}

def parse_dance_name(dance_name: str, const_class) -> str:
    if dance_name == const_class.LW_l: return const_class.LW_s
    if dance_name == const_class.TG_l: return const_class.TG_s
    if dance_name == const_class.WW_l: return const_class.WW_s
    if dance_name == const_class.SF_l: return const_class.SF_s
    if dance_name == const_class.QS_l: return const_class.QS_s
    if dance_name == const_class.SB_l: return const_class.SB_s
    if dance_name == const_class.CC_l: return const_class.CC_s
    if dance_name == const_class.RB_l: return const_class.RB_s
    if dance_name == const_class.PD_l: return const_class.PD_s
    if dance_name == const_class.JV_l: return const_class.JV_s


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

def get_results_dir_name() -> str:
    return results_dir

def get_url_list_path(url: str) -> str:
    return f"{results_dir}/{get_site_name_from_url(url)}"

def get_tournament_links_csv_path() -> str:
    return f"{results_dir}/tournament_links.csv"

def get_competition_links_csv_path() -> str:
    return f"{results_dir}/competition_links.csv"

def get_adjudicator_links_csv_path() -> str:
    return f"{results_dir}/adjudicator_links.csv"

def get_athletes_links_csv_path() -> str:
    return f"{results_dir}/athletes_links.csv"

def get_all_url_list_path(url: str) -> str:
    return f"{get_url_list_path(url)}/all_urls.npy"

def get_positive_url_path(url: str) -> str:
    return f"{get_url_list_path(url)}/known_hits.npy"

def get_negative_urls_path(url: str) -> str:
    return f"{get_url_list_path(url)}/negatives.npy"