
# coding: utf-8

# In[ ]:

from collections import defaultdict, Counter
import concurrent.futures
from functools import reduce
import glob
import csv
import os

import pandas as pd
import simplejson as json
import jieba


# In[ ]:

words_filename = '../words/words.csv'
word_segmentation_filename = '../words/bunshimondai.txt'
blacklist_filename = '../words/black_list.txt'
pluslist_filename = '../words/plus_list.txt'
jieba_dict_filename = '../words/dict.txt.big'
jieba.set_dictionary(jieba_dict_filename)


# In[ ]:

with open(word_segmentation_filename, 'r', encoding='utf-8') as f:
    for line in f:
        jieba.add_word(line.strip(), freq=10)


# In[ ]:

def wordlist_to_wordset(word_list):
    words = []
    for word_group in word_list:
        if type(word_group) is str:
            words.extend([c.strip() for c in word_group.split('/')])
    return set(words)

def build_tw_cn_dict(tw_word_list, cn_word_list):
    
    tw2cn = defaultdict(list)
    cn2tw = defaultdict(list)
    
    for tw_word_group, cn_word_group in zip(tw_word_list, cn_word_list):
        if type(tw_word_group) is str:
            tw_words = [c.strip() for c in tw_word_group.split('/')]
        else:
            tw_words = [None]
        
        if type(cn_word_group) is str:
            cn_words = [c.strip() for c in cn_word_group.split('/')]
        else:
            tw_words = [None]
            
        for tw_word in tw_words:
            for cn_word in cn_words:
                tw2cn[tw_word].append(cn_word)
                cn2tw[cn_word].append(tw_word)
    
    return tw2cn, cn2tw

def new_defined_wordset(tw_word_list, cn_word_list):
    
    wordset = set()
    for tw_word_group, cn_word_group in zip(tw_word_list, cn_word_list):
        if type(tw_word_group) is str and type(cn_word_group) is str:
            tw_words = [c.strip() for c in tw_word_group.split('/')]
            cn_words = set([c.strip() for c in cn_word_group.split('/')])
            for cn_word in cn_words:
                for tw_word in tw_words:
                    if cn_word in tw_word:
                        break
                else:
                    wordset.add(cn_word)
    
    wordset -= wordlist_to_wordset(tw_word_list)
    return wordset

def read_list_file(filename):
    word_set = set()
    with open(filename, 'r', encoding='utf-8') as f:
        for line in f:
            word_set.add(line.strip())
    return word_set


# In[ ]:

df = pd.read_csv(words_filename)
cn_word_list = df['cn_word'].tolist()
tw_word_list = df['tw_word'].tolist()
cn_word_set = wordlist_to_wordset(cn_word_list)
tw_word_set = wordlist_to_wordset(tw_word_list)
word_set = cn_word_set - tw_word_set - read_list_file(blacklist_filename) | read_list_file(pluslist_filename)
tw2cn, cn2tw = build_tw_cn_dict(tw_word_list, cn_word_list)
filenames = glob.glob('../news/*/*/*')


# In[ ]:

def find_cn_words(article):
    cn_word_count = Counter()
    seg_list = jieba.cut(article, cut_all=False)
    seg_counter = Counter(seg_list)
    
    for key in set(seg_counter) - word_set:
        seg_counter[key] = 0
    seg_counter = +seg_counter
    
    return seg_counter


# In[ ]:

def find_cn_words_from_filename(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        article = json.load(f)
        result = find_cn_words(article['content'])
#         if result:
#             print(article['url'], result)
    split_filename = os.path.normpath(filename).split(os.sep)
    media = split_filename[split_filename.index('news') + 1]
    return Counter(result), [article['url'], media, article['date'], article['category'], counter_to_string(Counter(result))]


# In[ ]:

def counter_to_string(counter):
    word_freq_tup_pairs = counter.most_common()
    word_freq_str_pairs = ['%s:%d' % (word, freq) for word, freq in word_freq_tup_pairs]
    return '/'.join(word_freq_str_pairs)


# In[ ]:

def parallel_count(filenames):
    
    freq_counter = Counter()
    csv_rows = []

    with concurrent.futures.ProcessPoolExecutor(max_workers=16) as executor:
        for idx, (counter, csv_row) in enumerate(executor.map(find_cn_words_from_filename, filenames, chunksize=30000), 1):
            freq_counter += counter
            csv_rows.append(csv_row)
            if idx % 10000 == 0:
                print('Has completed %d tasks' % idx)
    
    return freq_counter, csv_rows


# In[ ]:

if __name__ == '__main__':
    freq_counter, csv_rows = parallel_count(filenames[:1000])
    os.makedirs('../result', exist_ok=True)
    with open('../result/article_words.csv', 'w+', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['URL', 'Media', 'Date', 'Category', 'Word Frequency'])
        
        for csv_row in csv_rows:
            writer.writerow(csv_row)
    

