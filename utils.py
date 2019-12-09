import datetime
import itertools
import os
import re
import requests
import sys
import time
from copy import deepcopy
from collections import namedtuple, defaultdict

from bs4 import BeautifulSoup as BS
from tenacity import retry, stop_after_attempt
import simplejson as json
from selenium import webdriver

ArticleMeta = namedtuple('ArticleMeta', ['url', 'date', 'time', 'category', 'title', 'content'])

class NewsCrawler(object):
    
    def __init__(self, output_dir, total_days, start_date=datetime.date.today()):
        
        self.session = requests.Session()
        self.headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6",
            "Connection": "keep-alive",
            "Referer": "https://www.google.com.tw/",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "\
                          "Chrome/69.0.3497.92 Safari/537.36"
        }
        self.scroll_pause_time = 0.3
        self.driver = webdriver.PhantomJS(executable_path='../phantomjs-2.1.1-linux-x86_64/bin/phantomjs')
        self.output_dir = output_dir
        self.total_days = total_days
        self.start_date = start_date
        self.newslinks = set()
    
    @retry(stop=stop_after_attempt(3))
    def get_bsObj(self, url):
        
        req = self.session.get(url, headers=self.headers)
        if req.url != url:
            return None
        bsObj = BS(req.text, "html.parser")
        return bsObj
    
    def get_bsObj_scroll_down(self, url):

        self.driver.get(url)
        # Get scroll height
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        while True:
            # Scroll down to bottom
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            # Wait to load page
            time.sleep(self.scroll_pause_time)
            # Calculate new scroll height and compare with last scroll height
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        return BS(self.driver.page_source, "html.parser")
    
    def date_generator(self):
        
        date = self.start_date
        for _ in range(self.total_days):
            yield str(date)
            date = date - datetime.timedelta(days=1)
