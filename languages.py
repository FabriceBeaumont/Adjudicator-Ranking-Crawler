from typing import List, Dict, Tuple, Set
from abc import ABC

class LanguageNames():
    english = 'en'
    german = 'de'

class LanguageConstants(ABC):
    name: str           = LanguageNames.english
    # English format: dd/Mon/yyyy
    date_format: str    = r'(\d{2}/[a-zA-Z]+/\d{4})'
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
    
    KEY_ORGANZIER: str          = 'Organizer'
    KEY_MASTER_OF_CEREMONY: str = 'Master of Ceremony' 
    COUPLE: str                 = 'Couple'
    PLACEMENT: str              = 'Placement'
    # The number is the identifyer number of the couple in the tournament.
    NR: str                     = 'Nr.'
    COUPLE_NR: str              = 'Startnumber'
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
    name = LanguageNames.english
    def __init__(self):
        super().__init__()

class Constants_de(LanguageConstants):
    name = LanguageNames.german
    # German format: dd.mm.yyyy.
    date_format = r'\b(\d{2}\.\d{2}.\d{4})\b'
    # Define the dance names in german.
    LW_l = "Langsamer Walzer"
    LW_s = "LW"
    WW_l = "Wiener Walzer"
    WW_s = "WW"

    KEY_ORGANZIER   = 'Veranstalter'
    KEY_MASTER_OF_CEREMONY = 'Ausrichter'
    COUPLE          = 'Paar'
    PLACEMENT       = 'Platz'
    ROUND           = 'Runde'
    NR              = 'Nr.'
    COUPLE_NR       = 'Startnummer'
    KEY_FINAL       = 'Endrunde'
    KEY_ROUND       = 'runde'
    KEY_ADJUDICATOR = 'Wertungsrichter'
    KEY_COUPLE      = 'Paar/Club'

    def __init__(self):
        super().__init__()

LANGUAGE_CONSTANTS = [Constants_en, Constants_de]

def get_constants_in_language(language: str) -> LanguageConstants:
    for constants in LANGUAGE_CONSTANTS:
        if language == constants.name:
            return constants
    
    return Constants_en()
