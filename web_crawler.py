from typing import List, Dict, Tuple, Set

import requests
from bs4 import BeautifulSoup
from pathlib import Path

from datetime import date
import pandas as pd
import os
import numpy as np

from tqdm import tqdm

# For retries.
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import time
from collections import deque

# Local imports.
import dataframes as c

def webpage_is_html(source_code: str) -> bool:
    # Note that this only contains the lower case variation! For both 'doctype' and 'htm' ('html') there
    # exist uppercase variations independently. Thus use the 'lower()' function on the test string first.
    return source_code[:15].lower().startswith('<!doctype htm')

def check_webpage_content(url: str, content_list: List[str]) -> bool:
        # If no desired content is specified, it is accepted by default.
        if len(content_list) == 0:
            return True
        
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=1.0)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)
    
        try:
            source_code = session.get(url)
            # Only expect html sites.
            if not webpage_is_html(source_code.text[:20]): return False
            # Parse the content of the website.
            link_soup = BeautifulSoup(source_code.text, "html.parser")

            # At this point it is already assumed that the link shall be saved. Negate this, if a content keyword is missing.
            contains_content = [s in str(link_soup) for s in content_list]
            return any(contains_content)
                            
        except Exception as e:
            print(f"Error: On site '{url}', could not process link '{url}'\n>>> {e}")
            time.sleep(.2)
            return False

def crawl_href_links_on_webpage(
        url: str,         
        website_contains_content_some: List[str] = [], 
        url_shall_contain_all: List[str] = [], 
        url_shall_contain_some: List[str] = [],
        url_must_not_contain_any: List[str] = [],
        forbidden_url_prefixes: List[str] = [], 
        forbidden_url_postfixes: List[str] = [],
        forbidden_anchors: List[str] = [], 
        reconnection_trys: int = 3,
        search_recursively: bool = False,
        recursion_depth: int = 3,
        verbose: bool = False
    ) -> Tuple[List[str], List[str]]:
    """ Crawls the webpage with the given url and collects all links on it.
        Use 'content_keyword' to demand that the links point to websites containing this keyword.
        Use 'url_shall_contain' or 'url_must_not_contain' to demant that the links contain or do not contain the keyword in their url.
        Use 'forbidden_url_prefixes' or 'forbidden_url_postfixes' to exclude urls with specific starts or endings.
        Use 'recursive_search' to allow crawling the found links as well. The implementation is not recursive (to avoid opening many crawler sessions) but the search is.
        # TODO: TRY RECURISVE (no deque needed)
        This function also maintains the lists of accepted and discarded links of the crawler class.            
        It returns the newly found accepted links, and the entire list of accepted links.

        Example usage:
        - Pass a tournament site and find all TopTurnier competition sites on it.
        - Pass a website listing dance clubs and find all websites to dance clubs on it.
    """
    def check_url(u: str) -> Tuple[bool, str]:
        """ Test the given url, if it fulfills the set url critera. 
            Returns the decision.
        """            
        contains_some_desired = True
        
        if len(url_shall_contain_some) > 0:
            contains_some_desired = any([s in u for s in url_shall_contain_some])
        
        contains_all_desired    = all([s in u for s in url_shall_contain_all])
        contains_none_forbidden = not any([s in u for s in url_must_not_contain_any])
        prefix_is_allowed       = not u.startswith(tuple(forbidden_url_prefixes))
        postfix_is_allowed      = not u.endswith(tuple(forbidden_url_postfixes))

        info_log: str = ''
        info_log += 'ulr does not contain all desired elements' if not contains_all_desired else ''
        info_log += 'url does not contain any desired elements' if not contains_some_desired else ''
        info_log += 'url contains some forbidden elements' if not contains_none_forbidden else ''
        info_log += 'url does not have desired prefix' if not prefix_is_allowed else ''
        info_log += 'url does not have desired postfix' if not postfix_is_allowed else ''            
        return contains_all_desired and contains_some_desired and contains_none_forbidden and prefix_is_allowed and postfix_is_allowed, info_log
        
    recursion_depth: int = 0

    # Organize all anchors (all local URLs) in a queue.
    webpage_links_search_queue = deque()
    webpage_links_search_queue.append((url, recursion_depth))
    webpage_links_ctr: int = 0
    searched_links_and_depth: Dict[str, int] = {}
    # Since the test for content is time consuming, save the rejected webpages.
    # Depending on the application they could be used for another function call.
    links_rejected_by_content: List[str] = []

    # Collect the targeted links. Also 
    target_links: List[str] = []
            
    # In case the host rejects to many calls, set up retries.
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=1.0)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)

    reconnection_ctr: int = reconnection_trys
    start_time = time.time()

    while len(webpage_links_search_queue) > 0 and not (reconnection_ctr < 0):
        # Get the next webpage to be searched.
        webpage_link, depth = webpage_links_search_queue.pop()
        webpage_name: str = c.get_site_name_from_url(webpage_link)
        # Do not process this link, if the link has been discoverd by clicking through already 'link_depth' many links.
        if depth > recursion_depth: continue

        # Try reading the webpage.
        try:
            m = len(webpage_links_search_queue)
            if verbose: print(f"Processing website '{webpage_name}'... [{webpage_links_ctr}\{m}, {webpage_links_ctr/m*100:.2f}% ]")
            # Extract the source code.
            source_code = session.get(webpage_link)
            # Only expect html sites.
            if not webpage_is_html(source_code.text[:20]): continue
            # Read the source code.
            soup = BeautifulSoup(source_code.text, "html.parser")

            # Iterate through all anchors on this website. Determine in a second step if they are hyperreferences.
            local_anchors = soup.find_all("a")
            local_links = list(set([a.attrs['href'] for a in local_anchors if "href" in a.attrs]))
            
            local_links.sort()
            local_link_depth: int = depth + 1
            n: int = len(local_links)
            # for i, local_link in enumerate(local_links):
            progress_bar = tqdm(range(0, n), unit=" links", desc=f"Processing {webpage_name}...:")
            for i in progress_bar:
                local_link = local_links[i]
                progress_bar.update()
                progress_bar.set_postfix_str(c.get_site_name_from_url(local_link))
                # First filtering of the links.
                if local_link.startswith(tuple(forbidden_url_prefixes)) or local_link in forbidden_anchors:
                    continue
                # If the href is a local anchor, add the global link structure.                
                if not local_link.startswith(('http', 'www')): 
                    if not local_link.startswith('/'):
                        local_link = f"{webpage_link}/{local_link}"
                    else:
                        local_link = f"{webpage_link}{local_link}"

                if verbose: print("\r" + " " * 100 + f"\rLink [{i}\{n}, {i/n*100:.2f}% ]: '{local_link[:100]}'", end="")
                
                # First, if the link is already known, do not process it twice.
                # This behaviour changes if recursive search is on, and the link was now reached quicker than before.
                if local_link in searched_links_and_depth.keys():
                    if verbose: print(f" => Already searched", end="")
                    # If this link was now found again, but with less clicks than before, 
                    # and recursive search is on, allow searching it again.
                    prev_depth = searched_links_and_depth.get(local_link)
                    if search_recursively and prev_depth > local_link_depth:
                        # Forget that the link is already known and search it again. 
                        # It will be added to the dict outside of these if-statements.
                        searched_links_and_depth.pop(local_link)
                    else: 
                        # Otherwise continue with the next link since this link has already been processed.
                        continue
                
                # Add the link to the already searched ones.
                searched_links_and_depth[local_link] = local_link_depth
                # If the recursive search is activated, add the link to the search queue.
                if search_recursively:
                    webpage_links_search_queue.appendleft((local_link, local_link_depth))

                # Now check if the link is a target link.
                link_url_is_accepted: bool = False
                link_content_is_accepted: bool = False

                # Test the link for the desired url features.
                link_url_is_accepted, reason = check_url(local_link)
                if verbose: print(f" => {'A' if link_url_is_accepted else 'Una'}ccepted url{'('+reason+')' if reason != '' else ''}", end="")
                if not link_url_is_accepted: continue
                                        
                # Test if the content of the links website is as desired.
                # Therefore first crawl content of the linked website.
                link_content_is_accepted = check_webpage_content(local_link, website_contains_content_some)
                if verbose: print(f" => {'D' if link_content_is_accepted else 'Und'}esired content", end="")
                if not link_content_is_accepted: 
                    links_rejected_by_content.append(local_link)
                    continue
                
                # Save the link.                
                target_links.append(local_link)
                    
            # Update the connection counter such that the while loop ends after this succesful connection.
            reconnection_ctr = -2

        except Exception as e:
            print(f"Error: Could not completely process site '{url}'\n>>> {e}")
            time.sleep(.1)
            # Decrement the connection counter and try once more.
            reconnection_ctr -= 1
        
        webpage_links_ctr += 1
        if verbose: print("\r", end="")

    runtime = time.time() - start_time
    print(f"\rLinks found: {len(target_links)},\tAccepted {len(target_links)},\tRuntime: {runtime:0.2f} sec" + " " * 50)
    return target_links, links_rejected_by_content

def crawl_club_sites(url: str = 'https://tnw.de/verband/vereine/'):
    # Since we are looking for links to other club sites, these urls must not contain the site name of the current url.
    club_sites, _ = crawl_href_links_on_webpage(
        url=url, 
        url_must_not_contain_any=[c.get_site_name_from_url(url)], 
        forbidden_url_prefixes=c.ClubsTable.BAD_LINK_PREFIXES, 
        forbidden_url_postfixes=c.ClubsTable.BAD_LINK_POSTFIXES,
        verbose=True
    )
    for s in club_sites: print(s)
    print(f"Saved {len(club_sites)} valid links from site '{url}':")
    return club_sites

def crawl_tournament_collection_site(url: str = 'https://www.tanzsportkreis-sankt-augustin.de/'):
    # First try to find a webpage, which links to all tournaments of the club.
    # Therefore crawl all links on the website and check for keywords in the url.
    candidates, _ = crawl_href_links_on_webpage(
        url=url,
        url_shall_contain_some=c.CompetitionsTable.KEY_CONTENTS,
        forbidden_url_prefixes=c.CompetitionsTable.BAD_LINK_PREFIXES,
        forbidden_url_postfixes=c.CompetitionsTable.BAD_LINK_POSTFIXES,
        search_recursively=True,
        recursion_depth=2,
        verbose=True
    )
    for s in candidates: print(s)
    print(f"Saved {len(candidates)} possible candidates for competition result sites from website '{url}'")

    print()

    # Next, test the links on the site - they should link to tournament sites which contain a keyword.
    tournament_sites: List[str] = []
    tournament_collection_sites: List[str] = []
    rejected_club_sites: List[str] = []
    for candidate in candidates[-1:]:
        print(f"Searching for tournament links on candidate webpage: {candidate}")
        tmp_tournament_sites: List[str] = []
        rejected_urls: List[str] = []
        tmp_tournament_sites, rejected_urls = crawl_href_links_on_webpage(
            url=candidate,            
            website_contains_content_some=c.CompetitionsTable.KEY_CONTENTS,
            url_must_not_contain_any=rejected_club_sites,
            forbidden_url_prefixes=c.CompetitionsTable.BAD_LINK_PREFIXES,
            forbidden_url_postfixes=c.CompetitionsTable.BAD_LINK_POSTFIXES,
            verbose=True
        )
        rejected_club_sites += rejected_urls
        if len(tmp_tournament_sites) > 1:
            tournament_sites += tmp_tournament_sites
            tournament_collection_sites.append(candidate)

    for s in tournament_sites: print(s)
    print(f"Saved {len(tournament_sites)} tournament webpages links")
    print(f"from the tournament collection webpage{'s ' if len(tournament_collection_sites)>1 else ' '}\n")
    for x in tournament_collection_sites: print(f"{x}\n")
    return tournament_sites

def crawl_competition_site(url: str = 'https://ergebnisse.tanzsportkreis-sankt-augustin.de/2023/23-12-16_17_TSK%20CFP/'):
    competition_sites, _ = crawl_href_links_on_webpage(
        url=url,
        website_contains_content_some=c.CompetitionsTable.KEY_CONTENTS,
        forbidden_url_prefixes=c.CompetitionsTable.BAD_LINK_PREFIXES,
        forbidden_url_postfixes=c.CompetitionsTable.BAD_LINK_POSTFIXES,
        verbose=True    
    )
    for s in competition_sites: print(s)
    print(f"Saved {len(competition_sites)} competition webpage from tournament webpage '{url}'")
    return competition_sites


if __name__ == "__main__":
    # Tests:
    # crawl_club_sites() == 174
    # crawl_tournament_collection_site() == 8
    # crawl_competition_site() == 18

    url = 'https://www.tanzsportkreis-sankt-augustin.de/'#'https://ltvbremen.de/verband/vereine'
    club_sites, _ = crawl_href_links_on_webpage(
        url=url, 
        url_must_not_contain_any=[c.get_site_name_from_url(url)], 
        forbidden_url_prefixes=c.ClubsTable.BAD_LINK_PREFIXES, 
        forbidden_url_postfixes=c.ClubsTable.BAD_LINK_POSTFIXES
    )