#!/usr/bin/env python
# coding: utf-8

# In[2]:


import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)
import json
import matplotlib
import matplotlib.pyplot as plt
from matplotlib import cm
from datetime import datetime
import glob
import seaborn as sns
import re
import os
import calmap

files = [i for i in glob.glob('../Dataset/2020-2023/*.{}'.format('csv'))]
files.remove('../Dataset/2020-2023/JP_youtube_trending_data.csv')
files.remove('../Dataset/2020-2023/KR_youtube_trending_data.csv')
files.remove('../Dataset/2020-2023/MX_youtube_trending_data.csv')
files.remove('../Dataset/2020-2023/RU_youtube_trending_data.csv')
sorted(files)

others = ['../Dataset/2020-2023/JP_youtube_trending_data.csv', 
          '../Dataset/2020-2023/KR_youtube_trending_data.csv',
          '../Dataset/2020-2023/MX_youtube_trending_data.csv',
          '../Dataset/2020-2023/RU_youtube_trending_data.csv']

dfs = list()
for csv in files:
    df = pd.read_csv(csv, index_col='video_id')
    df['country'] = csv[21:23]
    dfs.append(df)

for csv in others:
    df = pd.read_csv(csv, index_col='video_id', encoding="ISO-8859-1")
    df['country'] = csv[21:23]
    dfs.append(df)

my_df = pd.concat(dfs)

my_df['trending_date']=pd.to_datetime(my_df['trending_date'],format='%Y-%m-%dT%H:%M:%SZ')
my_df['publishedAt']=pd.to_datetime(my_df['publishedAt'],format='%Y-%m-%dT%H:%M:%SZ')

my_df = my_df[my_df['trending_date'].notnull()]
my_df = my_df[my_df['publishedAt'].notnull()]

my_df = my_df.dropna(how='any',inplace=False, axis = 0)

my_df.insert(4, 'publish_date', my_df['publishedAt'].dt.date)
my_df['publishedAt'] = my_df['publishedAt'].dt.time

my_df_full = my_df.reset_index().sort_values('trending_date').set_index('video_id')
my_df = my_df.reset_index().sort_values('trending_date').drop_duplicates('video_id',keep='last').set_index('video_id')
my_df.head()


# In[5]:


fre_df = pd.DataFrame(my_df_full.groupby([my_df_full.index,'country']).count()['title'].sort_values(ascending=False)).reset_index()
fre_df.head(), fre_df.tail()

video_list,max_list = list(),list()
country_list = my_df.groupby(['country']).count().index

for c in country_list:
    video_list.append(fre_df[fre_df['country']==c]['title'].value_counts().sort_index())
    max_list.append(max(fre_df[fre_df['country']==c]['title'].value_counts().sort_index().index))
    
fig, [ax0, ax1, ax2, ax3, ax4, ax5, ax6, ax7, ax8, ax9, ax10] = plt.subplots(nrows=11,figsize=(15, 30))
st = fig.suptitle("How long a video trend in different countries?", fontsize=32)
st.set_y(0.9)
for i, pt in enumerate([ax0, ax1, ax2, ax3, ax4, ax5, ax6, ax7, ax8, ax9, ax10]):
    pt.plot(video_list[i].index, video_list[i])
    pt.spines['right'].set_visible(False)
    pt.spines['top'].set_visible(False)
    pt.set_xlabel("appearances",fontsize=14)
    pt.set_ylabel(country_list[i],fontsize=24)
    pt.axes.set_xlim(1, max(max_list))

plt.subplots_adjust(hspace=0.2)
plt.subplots_adjust(wspace=0)

