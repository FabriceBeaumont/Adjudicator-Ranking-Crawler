from typing import List, Dict, Tuple, Set
from enum import Enum
from urllib.parse import urlparse

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

class C(Enum):
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
    CLUB: str           = 'Club'
    COUPLE: str         = 'Paar'
    PLACEMENT: str      = 'Platz'
    NR: str             = 'Nr.'
    MAN: str            = 'Man'
    LADY: str           = 'Lady'
    RANK: str           = 'Rank'
    GRADE: str          = 'Grade'
    CATEGORY: str       = 'Category'
    VALUE: str          = 'Value'
    KEYWORD_FINAL: str  = 'Endrunde'
    CONTENT_KEYWORDS: List[str] = ["TopTurnier"]
    URL_KEYWORDS: List[str]     = ['/index.htm']

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