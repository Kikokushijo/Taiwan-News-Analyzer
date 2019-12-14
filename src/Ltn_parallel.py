import contextlib
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

from utils import ArticleMeta, NewsCrawler

class LtnNewsCrawler(NewsCrawler):

    def date_newslist_generator(self, start_date, end_date, days=90):
        
        chunk_start_date = start_date
        chunk_end_date = chunk_start_date - datetime.timedelta(days=days-1)
        while chunk_start_date >= end_date:
            
            yield "https://news.ltn.com.tw/search?keyword=&conditions=and&start_time=%s&end_time=%s" % (chunk_end_date, chunk_start_date)
            
            chunk_end_date -= datetime.timedelta(days=days)
            chunk_start_date -= datetime.timedelta(days=days)

        return BS(self.driver.page_source, "html.parser")
    
    def newslink_generator(self):
        
        for newslist_url in self.date_newslist_generator(self.start_date, self.end_date):
            
            for page_num in itertools.count(1):
                newslist_page = self.get_bsObj(newslist_url + "&page=" + str(page_num))
                tits = newslist_page.findAll('a', class_='tit')
                if not tits:
                    break
                else:
                    for tit in tits:
                        yield tit['href']

    @retry(stop=stop_after_attempt(0),
           retry_error_callback=lambda x: None)
    def parse_category(self, newspage):
        return newspage.find('title').text.split(' - ')[1]
    
    @retry(stop=stop_after_attempt(0),
           retry_error_callback=lambda x: None)
    def parse_title(self, newspage):
        return newspage.find('title').text.split(' - ')[0]
    
    @retry(stop=stop_after_attempt(0),
           retry_error_callback=lambda x: None)
    def parse_article(self, newspage):
        paragraphs = []
        for paragraph in newspage.find('div', class_='text').findChildren("p" , recursive=False):
            if paragraph.findChild() or paragraph.has_attr('class'):
                continue
            else:
                paragraphs.append(paragraph.text)
        return '\n'.join(paragraphs)
    
    @retry(stop=stop_after_attempt(0),
           retry_error_callback=lambda x: (None, None))
    def parse_date_time(self, newspage):
        # workaround
        datetime_string = newspage.find('span', class_='time').text.strip()   
        dt = datetime.datetime.strptime(datetime_string, "%Y-%m-%d %H:%M:%S")
        return str(dt.date()), str(dt.time())

    def is_valid_newspage(self, bsObj):
        
        if bsObj is None:
            return False
        
        try:
            if '錯誤' in bsObj.find('title').text:
                return False
            else:
                return True
        except:
            return True
    
    def saved_filename(self, url):
        return '-'.join(url.split('/')[-2:]) + '.json'
    
    def crawl_and_save(self):

        for newslink in self.newslink_generator():
            
            article = self.get_page_attribute_from_link(newslink)
            if article is None:
                continue

            self.save_article_meta(article)

year, month, day = [int(i) for i in input('Start Date:').split()]
crawler = LtnNewsCrawler(
    output_dir='../news/ltn',
    total_days=90,
    start_date=datetime.date(year=year, month=month, day=day)
)
crawler.crawl_and_save()