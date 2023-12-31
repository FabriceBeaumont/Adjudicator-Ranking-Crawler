from typing import List, Dict, Tuple, Set
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys

import numpy as np
import re
import time
from dataclasses import dataclass

import pandas as pd

# Local files.
import constants as c
