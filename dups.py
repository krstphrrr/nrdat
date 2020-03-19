
import os, sqlalchemy
import os.path
import pandas as pd
import numpy as np
from utils import db, sql_str, config, Acc
from sqlalchemy import create_engine, DDL
import sqlalchemy_access as sa_a
from psycopg2 import sql
from tqdm import tqdm
from datetime import date
import urllib
import pyodbc as pyo

from index import pg_send, df_builder_for_2004, df_builder_for_2009
from for2011_2016 import first_round


path=os.environ['NRIDAT']
dirs = os.listdir(path)

firstp, secondp, thirdp, fourthp = [os.path.join(path,i) for i in dirs]

accesspath = os.path.join(fourthp,'Raw data dump', 'target_mdb.accdb')
"""
df creation
2004
"""
f = df_builder_for_2004(firstp,'RangeChange2004-2008')
f.extract_fields('2004')
f.append_fields('2004')

first = f.dfs.copy()

"""
2009
"""
s = df_builder_for_2009(firstp,'RangeChange2009-2015')
s.extract_fields('2009')
s.append_fields('2009')

second = s.dfs.copy()
"""
2011
"""
t = first_round(secondp, 'range2011-2016')
t.extract_fields('2009') # file is called 2009-2016 NRI Range Data Dump Columns
t.append_fields('2011')
third = t.dfs.copy()
third
"""
2013
"""
c = first_round(thirdp, 'range2011-2016')
c.extract_fields('2009')
c.append_fields('pasture2013')
fourth = c.dfs.copy()
fourth
"""
2017
"""

l = first_round(fourthp, 'rangepasture2017_2018')
l.extract_fields('2018')
l.append_fields('rangepasture2017')
fifth = l.dfs.copy()
fifth
"""
getting whole table
"""


tname = 'concern'
df1=first[tname].copy(deep=True)
df2=second[tname].copy(deep=True)
df3 = third[tname].copy(deep=True)
df4 = fourth[tname].copy(deep=True)
df5 = fifth[tname].copy(deep=True)

df1.shape
df2.shape
df3.shape
df4.shape
df5.shape

wow = appender(df1,df2,df3,df4,df5,tablename=tname)

wow.a()

wow.fix()



wow.fixed.to_sql(name='concern', con=engine, index=False)

df_send(wow.fixed, 'concern')

def df_send(selectdf, tablename):
    df = selectdf
    # engine = create_engine(sql_str(config()))
    def chunker(seq, size):
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))
    try:
        engine = create_engine(sql_str(config()))
        chunksize = int(len(df) / 10)
        tqdm.write(f'sending {tablename} to pg...')
        # only_once = set()
        # onthefly = {}

        with tqdm(total=len(df)) as pbar:
            for i, cdf in enumerate(chunker(df,chunksize)):
                replace = "replace" if i == 0 else "append"
                cdf.to_sql(name=f'{tablename}', con=engine,index=False, if_exists=replace)
            pbar.update(chunksize)
        tqdm.write(f'{tablename} sent to db')
    except Exception as e:
        print(e)









class appender:
    in_dfs = {}
    tbl = None
    unrepeater = set()
    final_df = None
    count = 1
    fixed = None
    def __init__(self,*df, tablename):
        self.tbl = tablename
        for i in df:
            if f'{self.tbl}{self.count}' not in self.unrepeater:
                self.unrepeater.add(f'{self.tbl}{self.count}')
                self.in_dfs.update({f'{self.tbl}{self.count}':i})
                self.count+=1
    def a(self):
        order = [j for j in self.in_dfs.keys()]
        first_df = self.in_dfs[order[0]]
        self.final_df = first_df
        for i in enumerate(self.in_dfs):
            if i[0]!=0:
                self.final_df = self.final_df.append(self.in_dfs[i[1]], ignore_index=True)
    def fix(self):
        self.fixed = self.final_df[~self.final_df.duplicated()]
