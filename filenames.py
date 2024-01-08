DIR_DATA: str           = "data"
DIR_CRAWLED_SITES: str  = "crawled_websites"
DIR_COMP_TABLES: str    = "comp_tables"
DIR_CRAWLED_LINKS: str  = "crawled_links"

# Time format for date stamps as file prefixes.
TIME_STR_FORMAT: str    = "%B %d, %Y"

class FN():
    # Filenames for tables in a single tournament.
    CSV_GENERAL_INFO: str = "general_info.csv"
    CSV_QUALI_ROUNDX: str = "quali_round{}.csv"
    CSV_QUALIFICATIONS: str = "qualifications.csv"
    CSV_FINALS: str       = "finals.csv"
    CSV_RANKING_LIST: str = "ranking.csv"
    CSV_ADJUDICATORS: str = "adjudicators.csv"
    # Filenames to maintain lists of urls.
    CSV_FIND_CLUBS: str   = "find_clubs.csv"
    CSV_CLUB: str         = "clubs.csv"
    CSV_TOURNAMENTS: str  = "tournaments.csv"
    CSV_COMPETITION: str  = "competitions.csv"