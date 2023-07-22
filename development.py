from typing import List, Dict, Tuple
import numpy as np
import os

import website_crawler as webcrawler
import constants as c

def read_url_npy(path) -> List[str]:
    url_list: List[str] = np.load(path, allow_pickle=True).tolist()
    return url_list

if __name__ == "__main__":
    substrings = ['a', 'adc', 'TopT']
    test = "Thisisadc aTopT"

    if all([x in test for x in substrings]):
        print(True)
    else:
        print(False)
    pass