import configparser
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

config = configparser.ConfigParser()
config.read('../config.ini')

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
        self.driver = webdriver.PhantomJS(executable_path=config['Dependency']['PhantomJSPath'])
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
    
    @retry(stop=stop_after_attempt(0),
           retry_error_callback=lambda x: None)
    def parse_category(self, newspage):
        raise NotImplementedError
    
    @retry(stop=stop_after_attempt(0),
           retry_error_callback=lambda x: None)
    def parse_title(self, newspage):
        raise NotImplementedError
    
    @retry(stop=stop_after_attempt(0),
           retry_error_callback=lambda x: None)
    def parse_article(self, newspage):
        raise NotImplementedError
    
    @retry(stop=stop_after_attempt(0),
           retry_error_callback=lambda x: (None, None))
    def parse_date_time(self, newspage):
        raise NotImplementedError

    def is_valid_newspage(self, newspage):
        raise NotImplementedError

    def saved_filename(self, url):
        raise NotImplementedError

    def parse_page_attribute(self, newslink, newspage):

        date_str, time_str = self.parse_date_time(newspage)
        category = self.parse_category(newspage)
        title = self.parse_title(newspage)
        text = self.parse_article(newspage)

        if text is None or date_str is None or time_str is None:
            return None

        article_meta = ArticleMeta(
            url=newslink,
            date=date_str,
            time=time_str,
            category=category,
            title=title,
            content=text
        )

        return article_meta
    
    def get_page_attribute_from_link(self, newslink):
        if newslink in self.newslinks:
            print('Duplicated URL:', newslink)
            return None
        else:
            self.newslinks.add(newslink)

        page = self.get_bsObj(newslink)
        if not self.is_valid_newspage(page):
            print('Invalid or Redirected Page:', newslink)
            return None
        
        article = self.parse_page_attribute(newslink, page)          
        if article is None:
            print('Invalid or Redirected Page:', newslink)
            return None
        
        print('Successful Scraping URL:', newslink)
        return article
    
    def save_article_meta(self, article):
        output_dir_with_date = os.path.join(self.output_dir, article.date)
        os.makedirs(output_dir_with_date ,exist_ok=True)
        filename = os.path.join(output_dir_with_date, self.saved_filename(article.url))
        with open(filename, 'w+', encoding='utf-8') as f:
            json.dump(article._asdict(), f, ensure_ascii=False, indent=4)