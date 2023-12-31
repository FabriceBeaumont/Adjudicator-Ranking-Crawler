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

class WebCrawler():

    def __init__(self, url: str):
        self.base_url: str = url

    def find_local_anchors(self, soup) -> List[str]:
        """Reads the soup and returns all hyperreferences which build on the start anchor."""
        local_anchors: str[str] = set()
        
        # Iterate through the soup and find all hyperreferences (html tag <"a href">)
        for link in soup.find_all("a"):
            local_link: str = link.attrs["href"] if "href" in link.attrs else ""

            # Strip the anchor of the base url.
            if local_link.startswith(self.base_url):
                anchor = local_link[len(self.base_url) :]

                # If not collected already, add it to the others.
                if anchor not in local_anchors:
                    local_anchors.add(anchor)

        return local_anchors

    def find_links_to_websites_containing_html_keyword(self, urls: List[str], content_keyword: str = None, url_keywords: List[str] = None, bad_link_prefixes: List[str] = None, bad_link_postfixes: List[str] = None, reconnection_trys: int = 2, known_good_links: List[str] = [], known_bad_links: List[str] = []) -> Tuple[List[str], List[str]]:
        """Crawls the urls and collects all links, which point to a website which has the 'key_word' in its html."""

        new_links_ctr: int = 0
        
        # In case the host rejects to many calls, set up retries.
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=1.0)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)

        # Iterate over all given urls and crawl for links.
        for i, tmp_url in enumerate(urls):
            site_progress_str = f"Site ({i+1}/{len(urls)}, {i+1 // len(urls) * 100}%)"
            reconnection_ctr: int = reconnection_trys
            while not (reconnection_ctr < 0):
                try:                
                    # Extract the source code.
                    source_code = session.get(tmp_url)
                    # Read the source code.
                    soup = BeautifulSoup(source_code.text, "html.parser")

                    # Iterate through all links on this website. 
                    # Collect links that point at sites which contain the 'key_word'. 
                    local_links = list(set(soup.find_all("a")))
                    for link_id, link in enumerate(local_links):
                        save_link: bool = False
                        links_progress_str = f"Links ({link_id+1}/{len(local_links)}, {link_id+1 // len(local_links) * 100}%)"
                        if "href" in link.attrs:
                            # Ignore download-links.
                            if link.attrs.get("class") == 'download-link':
                                continue
                            
                            local_link: str = link.attrs["href"]
                            if not local_link.startswith("https://"):
                                local_link = f"{tmp_url}/{local_link}"

                            # If the 'url_keyword' is set, use it as saving condition.
                            if url_keywords is not None and all([s in local_link for s in url_keywords]):
                                save_link = True

                            # If the 'bad_link_prefixes' or 'bad_link_posfixes' are set, use them to prevent unnecessary processing.
                            # Ignore links that have bad prefixes.
                            valid: bool = True
                            if bad_link_prefixes is not None and local_link.startswith(tuple(bad_link_prefixes)):
                                valid = False
                                save_link = False
                            if bad_link_postfixes is not None and local_link.startswith(tuple(bad_link_postfixes)):
                                valid = False
                                save_link = False

                            # If the 'known_bad_links' or 'known_good_links' are set, use them to prevent unnecessary processing.
                            # Ignore links that have already been processed (possibly earlyer).
                            unknown: bool = True
                            if known_bad_links is not None and local_link in known_bad_links:
                                unknown = False
                                save_link = False
                            elif known_good_links is not None and local_link in known_good_links:
                                unknown = False
                                save_link = False

                            # If 'content_keyword' is set, check if its present in the page and them it as saving condition.
                            # Therefore get another crawler to crawl the website of the link.
                            if valid and unknown and content_keyword is not None:
                                try:
                                    source_code = session.get(local_link)                    
                                    link_soup = BeautifulSoup(source_code.text, "html.parser")
                                            
                                    if all([s in str(link_soup) for s in [content_keyword]]):
                                        save_link = True
                                        
                                except Exception as e:
                                    known_bad_links.append(local_link)
                                    print(f"Error: On site {tmp_url}, could not process link '{local_link}'\n{e}")
                                    time.sleep(1)

                            # Save the link.
                            if save_link:
                                known_good_links.append(local_link)
                                new_links_ctr += 1
                            else: 
                                known_bad_links.append(local_link)

                            # Print a progess string.
                            print(f"\r{site_progress_str}>>{links_progress_str}->{local_link[:70]}...\t(Found {new_links_ctr} new)", end="")
                    # Update the connection-try counter.
                    reconnection_ctr = -1                            
                except Exception as e:
                    print(f"Error: Could not completely process site '{tmp_url}'\n{e}")
                    time.sleep(1)
                    reconnection_ctr -= 1

        return known_good_links, known_bad_links

    def crawl_website_links(self, start_anchor: str = "/", reconnection_trys: int = 2, link_depth: int = 3, verbose: bool = False) -> List[str]:
        """Returns a list of all URLs found by following links on the passed base url and subsites, anchored on it.        

        Args:
            start_anchor (str, optional): URL anchor to start constructing new subsites. Defaults to "/".
            recusrion_depth (int, optional): Depth on how many URLs will be followed in order to find target sites. Defaults to 5.
            verbose (bool, optional): If True - print more statements will be printed to the console. Defaults to False.

        Returns:
            List[str]: A list of found URLs.
        """
        # Initial print to offset '\r'.
        print()
        
        # Organize all anchors (all local URLs) in a queue.
        search_anchors = deque()
        search_anchors.append((start_anchor, 0))
        
        # Assemble the first url to search.
        # All urls will be build from the base url. This does not allow to jump to other websites during the recursive crawl, 
        # but allows to find links to them.
        url_depth_dict: Dict[str, int] = {self.base_url + start_anchor: 1}        

        # In case the host rejects to many calls, set up retries.
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=1.0)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("https://", adapter)

        reconnection_ctr: int = reconnection_trys
        # While the queue is not empty and the connection holds, crawl for urls.
        while not (len(search_anchors) < 1 or reconnection_ctr < 0):
            # Fetch search anchors.
            search_anchor, depth = search_anchors.pop()
            # Do not continue, if the reached link has been discoverd by clicking through 'link_depth' many links.
            if depth > link_depth:
                continue

            if verbose: print(f"\nSearching in '{self.base_url + search_anchor}' (search depth: {depth}) ...")
            try:
                # Extract the source code.
                source_code = session.get(self.base_url + search_anchor)
                # Read the source code.
                soup = BeautifulSoup(source_code.text, "html.parser")
                # Extract anchors from the source code.
                anchors: List[str] = self.find_local_anchors(soup)

                # Store unknown local anchors to feed the queue and the list of resulting urls.
                if len(anchors) == 0:
                    continue

                new_urls: List[str] = []
                # Iterate through all anchors, construct the url and add it to the search list.
                for anchor in anchors:
                    # Assemble the current url.
                    tmp_url: str = self.base_url + anchor

                    # For the next search iteration, only use anchors, which do not link to a file - and thus do not have a suffix.
                    # Like for example '.html', '.pdf',...
                    # Also do not search already known URLs twice.
                    if Path(anchor).suffix != "" or tmp_url in url_depth_dict.keys():
                        continue
                    else:
                        search_anchors.appendleft((anchor, depth + 1))

                    url_depth_dict[tmp_url] = depth + 1
                    new_urls.append(tmp_url)

                print(f"\r{len(url_depth_dict)} urls found.\t{len(search_anchors)} pending"+" "*10, end="")
            except Exception as e:
                print(e)
                print(f"Error while searching in '{self.base_url + search_anchor}':\n{e}")
                time.sleep(2)
                reconnection_ctr -= 1

        # Tailing print to offset '\r'.
        print()
        if verbose:
            print(f"\n({len(url_depth_dict)}) All results: ")
            for u in url_depth_dict:
                print(u)

        return list(url_depth_dict.keys())


def crawl_links_to_keyword_sites(
        base_url: str, 
        content_keyword: str, 
        bad_link_prefixes: List[str] = None, 
        bad_link_postfixes: List[str] = None,
        verbose: bool = False
    ) -> pd.DataFrame:
    """This function crawls for links within the website of the 'base_url'. 
    It then selects the links which point to sites, whose html contains the 'content_keyword'.
    An example usage is to use the url 'https://https://www.tanzsport.de/de/sportwelt/ergebnisse' as input,
    and receive a list of urls on this site, pointing to competitions or tournaments.

    Args:
        base_url (str): Url of the website that is crawled.
        content_keyword (str): Keyword that indicates, if a target website has been found.
        try_to_load_existing (bool, optional): If true, the local files are searched for already known hits. Only new hits will be returned. Defaults to True.
        verbose (bool, optional): If true, more verbose terminal prints will be shown. Defaults to False.

    Returns:
        pd.DataFrame: A dataframe containing the list of urls to websites with the 'content_keyword'. 
        Also the original sites, where links to these urls have been found, and when they were crawled.
    """

    # First, try to read already saved urls.
    url_list: List[str] = None
    wc = WebCrawler(base_url)

    # If no urls saved jet, generate them.
    if url_list is None or len(url_list) == 0:
        # Call the crawl function to get lists of urls.
        print(f"Crawling for hyperrefs on the site {base_url}...", end="")
        url_list: List[str] = wc.crawl_website_links(verbose=verbose)     
        print(f"found {len(url_list)}")
        np.save(c.get_all_url_list_path(base_url), np.array(url_list))
        
    # Go through the urls and check for target sites.
    # Try to load the already scanned links.
    known_positives: List[str] = []
    if os.path.exists(c.get_positive_url_path(base_url)):
        known_positives = np.load(c.get_positive_url_path(base_url), allow_pickle=True).tolist()

    # Try to load list of already checked invalid links.
    known_negatives: List[str] = []
    if os.path.exists(c.get_negative_urls_path(base_url)):
        known_negatives = np.load(c.get_negative_urls_path(base_url), allow_pickle=True).tolist()    

    # Find links on the website, which point to websites that contain the keyword.
    print(f"Crawling for target sites with keyword '{content_keyword}' and accessed from the found urls.")
    target_sites, no_target_sites = wc.find_links_to_websites_containing_html_keyword(url_list, content_keyword=content_keyword, bad_link_prefixes=bad_link_prefixes, bad_link_postfixes=bad_link_postfixes, known_good_links=known_positives, known_bad_links=known_negatives)
    # In order to speed up the future processing of this url, save now the already known targets, and not no-target urls to file.
    np.save(c.get_positive_url_path(base_url), target_sites)
    np.save(c.get_negative_urls_path(base_url), no_target_sites)
    
    # Merge the new dataframe with the existing one - if it exists.
    df_path = c.get_tournament_links_csv_path()
    if os.path.exists(df_path):
        old_df = pd.read_csv(df_path)
        len_old_df: int = len(old_df)
    else:
        old_df = None
        len_old_df: int = 0

    # Store the computed target site urls to a csv file.
    new_df = pd.DataFrame(target_sites, columns=[c.C_international.TOURNAMENT_LINK])
    new_df[c.C_international.BASE_URL] = base_url
    new_df[c.C_international.DATE] = date.today()    
    new_df[c.C_international.ID] = new_df.index + len_old_df
    new_df[c.C_international.PROCESSED] = 0

    result = pd.concat([old_df, new_df])
    result.to_csv(df_path, index=False)
    
    return new_df


def crawl_competiton_links_from_tournament_links():
    # Load the tournament dataframe.
    if not os.path.exists(c.get_tournament_links_csv_path()):
        print("No tournament links have been saved yet!")
        return
    tournament_df  = pd.read_csv(c.get_tournament_links_csv_path())
    # Load the competition dataframe.
    competition_df = pd.DataFrame(columns = [c.C_international.COMPETITION_LINK, c.C_international.TOURNAMENT_LINK, c.C_international.ID])
    if os.path.exists(c.get_competition_links_csv_path()):
        competition_df = pd.read_csv(c.get_competition_links_csv_path())
    
    # Iterate through all tournament links, that have not been processed already.    
    unprocessed_df = tournament_df[tournament_df[c.C_international.PROCESSED] == 0][[c.C_international.TOURNAMENT_LINK, c.C_international.ID]]
    n: int   = len(unprocessed_df)
    n_ctr: int = 1
    t_progress_str: str = f"Processing unprocessed tournaments ({n_ctr}/{n} - {n_ctr // n * 100}%)"
    for tournament_link, tournament_id in unprocessed_df.itertuples(index=False):
        wc = WebCrawler(tournament_link)
        competition_link_list, _ = wc.find_links_to_websites_containing_html_keyword([tournament_link], url_keywords=c.C_international.KEY_URL_ANCHOR)
        m: int = len(competition_link_list)
        m_ctr: int = 1        
        for competition_link in competition_link_list:
            c_progress_str: str = f"competitions ({m_ctr}/{m} - {m_ctr // m * 100}%)"
            print(f"{t_progress_str} -- {c_progress_str}")
            competition_row = {c.C_international.COMPETITION_LINK: competition_link, c.C_international.TOURNAMENT_LINK: tournament_link, c.C_international.ID: tournament_id}
            competition_df = competition_df.concat(competition_row, ignore_index=True)            
            m_ctr += 1

        # Mark the tournament as processed.
        tournament_df.loc[tournament_df[c.C_international.TOURNAMENT_LINK] == 2, [c.C_international.PROCESSED]] = 1
        n_ctr += 1

    # Save the updated tournament dataframe (with new progress values set).
    tournament_df.to_csv(c.get_tournament_links_csv_path(), index=False)
    # Save the updated competition dataframe.
    competition_df.to_csv(c.get_competition_links_csv_path(), index=False)


if __name__ == "__main__":
    url_hints = c.get_url_hints_df()
    url_local     = url_hints['Scope' == c.ScopeList.LOCAL]
    url_regionals = url_hints['Scope' == c.ScopeList.REGIONAL]
    url_nationals = url_hints['Scope' == c.ScopeList.NATIONAL]

    competition_links = c.get_tournament_links_df()
    known_urls: List[str] = competition_links[c.C_international.TOURNAMENT_LINK]

    # Update the list of competition links using the national results database ('DTV').
    # TODO
    for url in url_nationals:
        crawl_links_to_keyword_sites(
            base_url=url, 
            content_keyword=c.CrawlerConstants.KEY_CONTENT, 
            bad_link_prefixes=c.CrawlerConstants.BAD_LINK_PREFIXES, 
            bad_link_postfixes=c.CrawlerConstants.BAD_LINK_POSTFIXES
        )


    # Update the list of competition links using the regional results database (e.g. 'tnw.de').
    # TODO

    # Update the list of competition links using the websites of local clubs (e.g. 'TSK Sankt Augustin')
    # TODO

    # Report the amount of newly crawled urls to console.
    # TODO
    # TODO: Use tqdm