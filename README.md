# Notes on development

- Code test functions for the .py files
- Complete an entire data processing of at least one TopTurnier page
- Complete at least two measurements for the adjudicators
- Complete at least one measurement of uncertaintiy for the competition couples

# Notes on usage

Use the script `website_crawler.py` to crawl for suitable websites containing competition results.
It is recommended to use the website []() instead, since most competition results can be found there.
The crawling process will save the key data (webadress, tournament date, processed state) in the file
    `results/competition_links.csv`

Use the script `data_grabber.py` to load the tournament data from all not already processed websites stored in the 
file `results/competition_links.csv`. THe data processing includes reading and saving the resutls to the global
file xyz, and computing a ranking according to all known measurement functions.

Use the script `evaluations.ipynb` to generate plots for visualization of the data. This includes plots for individual
competitor couples or adjudicators.