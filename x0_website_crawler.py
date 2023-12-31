from typing import List, Dict, Tuple, Set

import requests
from bs4 import BeautifulSoup
from pathlib import Path

from datetime import date
import pandas as pd
import os
import numpy as np

# For retries.
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import time
from collections import deque

# Local imports.
import constants as c

def crawl_href_links_on_webpage(
        url: str, 
        website_contains_content_all: str = [], 
        url_shall_contain_all: List[str] = [], 
        url_shall_contain_some: List[str] = [],
        url_must_not_contain_any: List[str] = [],
        forbidden_url_prefixes: List[str] = [], 
        forbidden_url_postfixes: List[str] = [], 
        reconnection_trys: int = 3,
        search_recursively: bool = False,
        link_depth: int = 0,
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
    def check_url(u: str) -> bool:
        """ Test the given url, if it fulfills the set url critera. 
            Returns the decision.
        """            
        contains_some_desired   = True
        if len(url_shall_contain_some) > 0:
            contains_some_desired = any([s in u for s in url_shall_contain_some])
        
        contains_all_desired    = all([s in u for s in url_shall_contain_all])
        contains_none_forbidden = not any([s in u for s in url_must_not_contain_any])
        allowed_url_prefix      = not u.startswith(tuple(forbidden_url_prefixes))
        allowed_url_postfix     = not u.endswith(tuple(forbidden_url_postfixes))

        if verbose: 
            str = 'ulr does not contain all desired ' if not contains_all_desired else ''
            str += 'url does not contain any desired' if not contains_some_desired else ''
            str += 'url contains some forbidden' if not contains_none_forbidden else ''
            str += 'url does not have desired prefix' if not allowed_url_prefix else ''
            str += 'url does not have desired postfix' if not allowed_url_postfix else ''
            print(str, end="")
        return contains_all_desired and contains_some_desired and contains_none_forbidden and allowed_url_prefix and allowed_url_postfix

    def check_webpage_content(u) -> bool:
        # If no desired content is specified, it is accepted by default.
        if len(website_contains_content_all) == 0:
            return True
        try:
            source_code = session.get(u)                    
            link_soup = BeautifulSoup(source_code.text, "html.parser")

            # At this point it is already assumed that the link shall be saved. Negate this, if a content keyword is missing.
            return all([s in str(link_soup) for s in website_contains_content_all])
                            
        except Exception as e:
            print(f"Error: On site '{url}', could not process link '{u}'\n>>> {e}")
            time.sleep(.2)
            return False

    link_depth: int = 0

    # Organize all anchors (all local URLs) in a queue.
    webpage_search_queue = deque()
    webpage_search_queue.append((url, link_depth))
    searched_links_and_depth: Dict[str, int] = {}

    # Collect the targeted links. Also 
    target_links: List[str] = []
            
    # In case the host rejects to many calls, set up retries.
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=1.0)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)

    reconnection_ctr: int = reconnection_trys
    start_time = time.time()

    while len(webpage_search_queue) > 0 and not (reconnection_ctr < 0):
        # Get the next webpage to be searched.
        webpage, depth = webpage_search_queue.pop()
        # Do not process this link, if the link has been discoverd by clicking through already 'link_depth' many links.
        if depth > link_depth: continue

        # Try reading the webpage.
        try:                
            # Extract the source code.
            source_code = session.get(webpage)
            # Read the source code.
            soup = BeautifulSoup(source_code.text, "html.parser")

            # Iterate through all anchors on this website. Determine in a second step if they are hyperreferences.
            local_anchors = soup.find_all("a")
            local_links = list(set([a.attrs['href'] for a in local_anchors if "href" in a.attrs]))
            local_links.sort()
            local_link_depth: int = depth + 1
            for local_link in local_links:
                if verbose: print("\r" + " " * 150 + f"\rLink: '{local_link}'", end="")
                
                # First, if the link is already known, do not process it twice.
                # This behaviour changes if recursive search is on, and the link was now reached quicker than before.
                if local_link in searched_links_and_depth.keys():
                    if verbose: print(f" => Already searched", end="")
                    # If this link was now found again, but with less clicks than before, 
                    # and recursive search is on, allow searching it again.
                    _, prev_depth = searched_links_and_depth.get(local_link)
                    if search_recursively and prev_depth > local_link_depth:
                        # Forget that the link is already known and search it again.
                        searched_links_and_depth.pop(local_link)
                    else: 
                        # Otherwise continue with the next link since this link has already been processed.
                        continue
                
                # Add the link to the already searched ones.
                searched_links_and_depth[local_link] = local_link_depth
                # If the recursive search is activated, add the link to the search queue.
                if search_recursively:
                    webpage_search_queue.appendleft((local_link, local_link_depth))

                # Now check if the link is a target link.
                link_url_is_accepted: bool = False
                link_content_is_accepted: bool = False

                # Test the link for the desired url features.
                link_url_is_accepted = check_url(local_link)
                if verbose: print(f" => Desired url", end="")
                                        
                # Test if the content of the links website is as desired.
                # Therefore first crawl content of the linked website.
                link_content_is_accepted = check_webpage_content(local_link)
                if verbose: print(f" => {'D' if link_content_is_accepted else 'Und'}esired content", end="")

                # Save the link.
                if link_url_is_accepted and link_content_is_accepted:
                    target_links.append(local_link)
                    
            # Update the connection counter such that the while loop ends after this succesful connection.
            reconnection_ctr = -2

        except Exception as e:
            print(f"Error: Could not completely process site '{url}'\n>>>{e}")
            time.sleep(.1)
            # Decrement the connection counter and try once more.
            reconnection_ctr -= 1
                
        if verbose: print()

    runtime = time.time() - start_time
    print(f"\nLinks found: {len(target_links)}\tAccepted {len(target_links)}")
    print(f"Runtime: {runtime: 0.2f} sec\t({runtime / max(1, len(target_links)): 0.2f} sec per link, {runtime / max(1, len(target_links)): 0.2f} sec per accepted link)")

    return target_links

def crawl_club_sites(url: str = 'https://tnw.de/verband/vereine/'):
    # Since we are looking for links to other club sites, these urls must not contain the site name of the current url.
    club_sites = crawl_href_links_on_webpage(
        url=url, 
        url_must_not_contain_any=[c.get_site_name_from_url(url)], 
        forbidden_url_prefixes=c.ClubsDf.BAD_LINK_PREFIXES, 
        forbidden_url_postfixes=c.ClubsDf.BAD_LINK_POSTFIXES,
        verbose=True
    )
    print(f"Saved {len(club_sites)} valid links from site '{url}':")
    for s in club_sites: print(s)



def crawl_tournament_collection_site(url: str = 'https://www.tanzsportkreis-sankt-augustin.de/'):
    tournament_collection_sites = crawl_href_links_on_webpage(
        url=url,
        url_shall_contain_some=['urnier', 'rgebnisse', 'ompetition', 'esults'],
        forbidden_url_prefixes=c.ClubsDf.BAD_LINK_PREFIXES,
        forbidden_url_postfixes=c.ClubsDf.BAD_LINK_POSTFIXES,
        verbose=True
    )
    print(f"Saved {len(tournament_collection_sites)} valid links ({len(tournament_collection_sites)} new ones) from site '{url}'")


def crawl_tournament_sites(url: str):
    tournament_sites = crawl_href_links_on_webpage(
        url=url,
        website_contains_content_all=[c.FindTournamentsDf.KEY_CONTENT],
        forbidden_url_prefixes=c.FindTournamentsDf.BAD_LINK_PREFIXES,
        forbidden_url_postfixes=c.FindTournamentsDf.BAD_LINK_POSTFIXES,
        verbose=True    
    )
    print(f"From site '{url}' got:\n{tournament_sites}")


if __name__ == "__main__":
    crawl_club_sites()  
    # TODO: Correct usage. Write a test for this outcome for two sites. C
    # TODO ontinue to test the other functions
    # TODO: THen write the files manager as a separate main function an use the DataFrame classes. 