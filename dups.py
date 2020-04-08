

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
import sqlalchemy_access as sa_a

from index import pg_send, df_builder_for_2004, df_builder_for_2009, drop_all, ret_access
from for2011_2016 import first_round
from index import type_lookup

path=os.environ['NRIDAT']
dirs = os.listdir(path)

firstp, secondp, thirdp, fourthp = [os.path.join(path,i) for i in dirs]

accesspath = os.path.join(firstp,'Raw data dump', 'target_mdb.accdb')
# accesspath = r"C:\Users\kbonefont\Desktop\dfsend\disturbance.accdb"
# acc_path = os.path.join(firstp, 'concern.accdb')



"""
df creation
2004
"""
f = df_builder_for_2004(firstp,'RangeChange2004-2008')
f.extract_fields('2004')
f.append_fields('2004')

f.dfs['pointcoordinates']
# f.dfs['point']
# f.fields_dict['POINTCOORDINATES']
import copy
first = copy.deepcopy(f.dfs)
# f.dfs['concern']
# len(f.dfs['concern'].PrimaryKey[0])

# f.dfs['concern']
"""
2009
"""
s = df_builder_for_2009(firstp,'RangeChange2009-2015')
s.extract_fields('2009')
s.append_fields('2009')
# s.dfs['pointcoordinates']
second = copy.deepcopy(s.dfs)
# s.dfs['point']
# s.dfs['concern']
"""
2011
"""

t = first_round(secondp, 'range2011-2016')
t.extract_fields('2009')
# t.temp_coords
# t.full_coords_test
# pd.concat([t.temp_coords, t.full_coords_test], axis=1, join="inner")

 # file is called 2009-2016 NRI Range Data Dump Columns
t.append_fields('2011')
t.
t.full_coords_test
t.dfs['pointcoordinates']
third = copy.deepcopy(t.dfs)

t.dfs['pointcoordinates']

third = pd.concat([t.dfs['coordinates'],t.dfs['pointcoordinates']], axis=1, join="inner")
third.iloc[:,-1]
third.columns
third.iloc[:,13]
third.drop(index=[12], axis=1)
third = third.iloc[:,0:13]
third

t.dfs['pointcoordinates']
# t.dfs['concern']
"""
2013
"""
os.listdir(thirdp)
c = first_round(thirdp, 'range2011-2016')
c.extract_fields('2009')
c.append_fields('pasture2013')
fourth = copy.deepcopy(c.dfs)
c.dfs['point']
c.dfs['coordinates']
c.dfs['pointcoordinates']

cuarto = pd.concat([c.dfs['coordinates'],c.dfs['pointcoordinates']], axis=1, join="inner")
cuarto = cuarto.iloc[:,0:13]


"""
2017
"""

l = first_round(fourthp, 'rangepasture2017_2018')
l.extract_fields('2018')
l.append_fields('rangepasture2017')
fifth = copy.deepcopy(l.dfs)
l.dfs['point']
l.dfs['coordinates']
l.dfs['pointcoordinates']


# l.dfs['concern']
"""
getting whole table
"""

class appender:
    in_dfs = {}
    tbl = None
    unrepeater = set()
    final_df = None
    count = 1
    fixed = None
    def __init__(self,*df, tablename):
        self.in_dfs = {}
        self.unrepeater = set()
        self.final_df = None
        self.count = 1
        self.fixed = None
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
        for each_col in self.final_df.columns:
            # if (self.final_df[each_col].dtype!=np.float64) and (self.final_df[each_col].dtype!=np.int64):
            #     # print(self.final_df[each_col],self.final_df[each_col].dtype)
            #
            #     self.final_df[each_col] = self.final_df[each_col].astype(object).apply(lambda i: i.strip() if pd.isnull(i)!=True else i)
        # per table fixes
            if self.tbl.find('ecosite')!=-1:
                self.final_df.replace('',np.nan)
                self.final_df['START_MARK'] = self.final_df['START_MARK'].astype('Int64')
                self.final_df['END_MARK'] = self.final_df['END_MARK'].astype('Int64')
            # if self.tbl.find('pastureheights')!=-1:

            elif ('ecosite' not in self.tbl) and (self.final_df[each_col].dtype!=np.float64) and (self.final_df[each_col].dtype!=np.int64):
            #     # print(self.final_df[each_col],self.final_df[each_col].dtype)
            #
                self.final_df[each_col] = self.final_df[each_col].astype(object).apply(lambda i: i.strip() if pd.isnull(i)!=True else i)




        # self.fixed = self.final_df[~self.final_df.duplicated()]

        # self.final_df =self.final_df.replace(np.nan, '') ### need to change nulls ? maybe not
        self.fixed = self.final_df.drop_duplicates(subset=['SURVEY','STATE', 'COUNTY', 'PSU', 'POINT'], keep='first')

"""
q's:
- which fields should be used to find dups
- try sending to postgres or access w nulls instead of empty spaces

"""


def df_send(selectdf, tablename, acc = None, pg=None):
    df = selectdf
    # engine = create_engine(sql_str(config()))
    cxn = ret_access(acc_path)
    def chunker(seq, size):
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))
    try:
        engine = create_engine(sql_str(config()))
        chunksize = int(len(df) / 10)
        tqdm.write(f'sending {tablename} to pg...')
        only_once = set()
        onthefly = {}

        with tqdm(total=len(df)) as pbar:
            for i, cdf in enumerate(chunker(df,chunksize)):
                replace = "replace" if i == 0 else "append"
                """"""

                t = type_lookup(cdf,tablename,4,fourthp)

                temptypes = t.list
                templengths = t.length

                def alchemy_ret(type,len=None):
                    """
                    function that takes a type(numeric or character+length) returns
                    a sqlalchemy/pg compatible type
                    """
                    if (type=='numeric') and (len==None):
                        return sqlalchemy.types.Float(precision=3, asdecimal=True)
                    elif (type=='character') and (len!=None):
                        return sqlalchemy.types.VARCHAR(length=len)


                for key in temptypes:
                    """
                    creating custom dictionary per table to map pandas types to pg
                    """
                    state_key = ["STATE", "COUNTY"]
                    if 'PrimaryKey' not in temptypes:
                        onthefly.update({"PrimaryKey":sa_a.ShortText(17)})
                    if 'FIPSPSUPNT' not in temptypes:
                        onthefly.update({"FIPSPSUPNT":sa_a.ShortText(13)})
                    if key not in only_once:
                        only_once.add(key)

                        if temptypes[key]=='numeric':
                            onthefly.update({f'{key}':alchemy_ret(temptypes[key])})
                            for k in state_key:
                                if k == "STATE":
                                    onthefly.update({f'{k}':alchemy_ret('character',2)})
                                if k=="COUNTY":
                                    onthefly.update({f'{k}':alchemy_ret('character',3)})

                        if temptypes[key]=='character':
                            onthefly.update({f'{key}':alchemy_ret(temptypes[key],templengths[key])})

                            if key == "PTNOTE":
                                onthefly.update({"PTNOTE":sqlalchemy.types.Text})
                            # if key != "PrimaryKey":
                            #     onthefly.update({"PrimaryKey":sa_a.ShortText(17)})
                """"""
                if (acc!=False) and (pg!=False):
                    cdf.to_sql(name=f'{tablename}', con=engine,index=False, if_exists=replace, dtype=onthefly)
                    cdf.to_sql(name=f'{tablename}', con=ret_access(acc_path),index=False, if_exists=replace, dtype=onthefly)

                elif (acc!=False) and (pg==False):
                    cdf.to_sql(name=f'{tablename}', con=ret_access(acc_path),index=False, if_exists=replace, dtype=onthefly)

                elif (acc==False) and (pg!=False):

                    cdf.to_sql(name=f'{tablename}', con=engine,index=False, if_exists=replace, dtype=onthefly)
                elif (acc==False) and (pg==False):
                    dir = os.path.join(mainpath,'csvs')
                    if not os.path.exists(dir):
                        os.mkdir(dir)
                    df.to_csv(os.path.join(dir,f'{tablename}.csv'),index=False)
                else:
                    tqdm.write("Please set the access/pg booleans in pg_send's arguments")
                pbar.update(chunksize)
            tqdm._instances.clear()
        tqdm.write(f'{tablename} sent to db')
    except Exception as e:
        print(e)

"""
needs to be generalized into class/function
"""

class join_machine:
    """
    still stores old instance data for some reason..
    """
    tname=None
    w_dups = None
    no_dups = None
    def __init__(self,tablename):
        joined = None
        self.tname = tablename
        full_list = []
        try:
            df1 =first[tname].copy(deep=True)
            full_list.append(df1)
        except Exception as e:
            # print(e)
            df1 = None
        try:
            df2 =second[tname].copy(deep=True)
            full_list.append(df2)
        except Exception as e:
            # print(e)
            df2 = None

        try:
            df3 = third[tname].copy(deep=True)
            full_list.append(df3)
        except Exception as e:
            # print(e)
            df3  = None

        try:
            df4 = fourth[tname].copy(deep=True)
            full_list.append(df4)
        except Exception as e:
            # print(e)
            df4 = None

        try:
            df5 = fifth[tname].copy(deep=True)
            full_list.append(df5)
        except Exception as e:
            # print(e)
            df5 = None
        joined = appender(full_list,tablename=self.tname)
        joined.a()
        self.w_dups = joined.final_df
        joined.fix()
        self.no_dups = joined.fixed

del(j)

j = join_machine('ESFSG')
j.w_dups
j.no_dups
acc_path = os.path.join(firstp, 'mdbs','ready.accdb')

tname = 'concern'
df1=first[tname].copy(deep=True)
df2=second[tname].copy(deep=True)
df3 = third[tname].copy(deep=True)
df4 = fourth[tname].copy(deep=True)
df5 = fifth[tname].copy(deep=True)

pd.concat([df1,df2,df3,df4,df5]).drop_duplicates()
#concern
concern = pd.concat([df1,df2,df3,df4,df5]).drop_duplicates().copy(deep=True)
len(concern.columns)
concern.columns[5:27]
for field in concern.columns[5:27]:
    concern[field] = concern[field].apply(lambda x: 1 if x=='Y' else (0 if x=='N' else x) )
df_send(concern,'concern', acc=True, pg=False)


poinc = pd.concat([df1,df3,df4,df5]).drop_duplicates().copy(deep=True)

pks = concern['PrimaryKey']
notin = []
for i in concern['PSU'].unique():
    if i in poinc['PrimaryKey']:
        notin.append(i)

 #soilhorizon

 pd.concat([df2,df3]).shape[0]
pd.concat([df2,df3]).drop_duplicates().shape[0]
np.sum(18576-11541)
 pd.concat([df2,df3,df4]).shape[0]
pd.concat([df2,df3,df4]).drop_duplicates().shape[0]
np.sum(21824-14789)
pd.concat([df2,df3,df4,df5]).drop_duplicates()

soil = pd.concat([df2,df3,df4,df5]).drop_duplicates()
soil1['DEPTH_UNIT'] = soil['DEPTH'].apply(lambda x: 'in' if pd.isnull(x)!=True else x)
soil1 = soil.iloc[:,:7]
soil2 = soil.iloc[:,7:]
soilf = pd.concat([soil1,soil2], axis=1)
df_send(soilf, 'soilhorizon', acc=True, pg=False)








os.listdir(firstp)
from index import header_fetch
p1 = r"C:\Users\kbonefont\Desktop\NRI\extracted\2004-2015 Rangeland Change Database with Weights\RangeChange2004-2008\soildisag.txt"
h1 = header_fetch(firstp)
h1.files
h1.pull(r"2004-2008 NRI Range Change Data Dump Columns.xlsx", 'SOILDISAG')
h1.fields
d1 = pd.read_csv(p1, sep='|', names=h1.fields)

p2 = r"C:\Users\kbonefont\Desktop\NRI\extracted\2004-2015 Rangeland Change Database with Weights\RangeChange2009-2015\soildisag.txt"

d2 = pd.read_csv(p2, sep='|', names=h1.fields)

p3 = r"C:\Users\kbonefont\Desktop\NRI\extracted\2011-2016 NRI Rangeland Database\Raw data dump\range2011-2016\soildisag.txt"

d3 = pd.read_csv(p3, sep='|', names=h1.fields)

p4 = r"C:\Users\kbonefont\Desktop\NRI\extracted\2013-2016 NRI Pasture Database\Raw data dump\pasture2013-2016\soildisag.txt"

d4 = pd.read_csv(p4, sep='|', names=h1.fields)

p5 = r"C:\Users\kbonefont\Desktop\NRI\extracted\2017-2018 Range and Pasture\Raw data dump\rangepasture2017_2018\soildisag.txt"

d5 = pd.read_csv(p5, sep='|', names=h1.fields)
pd.concat([d1,d2,d3,d4,d5]).columns


pd.concat([d1,d2,d3,d4,d5]).drop_duplicates(subset=['SURVEY','STATE','COUNTY','PSU','POINT'])











# production

pd.concat([df1,df2,df3,df5]).drop_duplicates()
df_send(pd.concat([df1,df2,df3,df5]).drop_duplicates(), 'production', acc=True, pg=False)

# PTNOTE
pd.concat([df1,df2,df3,df4,df5]).drop_duplicates()
df_send(pd.concat([df1,df2,df3,df4,df5]).drop_duplicates(),'soildisag',acc=True, pg=False)

# rangehealth
pd.concat([df1,df2,df3,df5]).drop_duplicates()
df_send(pd.concat([df1,df2,df3,df5]).drop_duplicates(),'rangehealth',acc=True, pg=False)

df1['SURVEY'].unique()
df2.SURVEY.unique()
df3.SURVEY.unique()
df4.SURVEY.unique()
df5.SURVEY.unique()
practice=pd.concat([df1,df2,df3,df4,df5], ignore_index=True).drop_duplicates(subset=['PrimaryKey'])
pd.concat([df1,pd.concat([df2,df3,df4]).drop_duplicates(), df5])

practice.P528A[0]

## practice shapeup,
mask = pd.isnull(practice.P528A)!=True
practice['P528'][mask] = practice['P528A'][mask]
practice.drop(columns=['P528A'])
# still missing 4 rows
df_send(practice, 'practice', acc=True, pg=False)
# production




# first check if years overlap
df1
df1.SURVEY.unique()
df2.SURVEY.unique()
df3.SURVEY.unique()
df4.SURVEY.unique()
fourteen = pd.concat([df2,df3]).drop_duplicates(subset=[i for i in pklist if i not in nolist])
fifty = pd.concat([df1,df2,df3,df4,df5]).drop_duplicates()
gint=pd.concat([df2,df3,df4]).drop_duplicates()



















pd.concat([df1,df3,df4,df5]).drop_duplicates()
pd.concat([df2,df3]).drop_duplicates()
plantcensus = pd.concat([pd.concat([df2,df3]).drop_duplicates(),df4,df5]).drop_duplicates()
df_send(df1, 'plantheight', acc=True, pg=False)
# dealing with pastureheights: removing duplicates from year ranges that may overlap
height = pd.concat([df2,df3,df4]).drop_duplicates()
height2 = pd.concat([height,df5],ignore_index=True)
# dividing sets to place the columns correctly
height_1 = height2[['SURVEY', 'STATE', 'COUNTY', 'PSU', 'POINT', 'TRANSECT', 'DISTANCE','HPLANT','HEIGHT']]
height_2 = height2[['WPLANT', 'WHEIGHT']]
height_tail = height2[['PrimaryKey', 'FIPSPSUPNT', 'DBKey']]

height['WHEIGHT'].unique()
# converting inches to fractions of feet - still need to take care of  plus signs
h2 = height_2.copy(deep=True)
# pseudo: round the converted value to three decimal points if its not a null, has a digit, there's no plus sign and  has 'in'
# else just round nonconverted value to three decimal points if its not null has a digit, theres no plus  but has 'ft' in value
h2['preheight2']=height2['WHEIGHT'].apply(lambda x: round((float(x.split()[0])*0.083333),3) if pd.isnull(x)!=True and
                                    (any([y.isdigit() for y in x])==True) and
                                    (any(['+' in z for z in x])!=True) and
                                    ('in' in x.split()) else (round(float(x.split()[0]),3) if pd.isnull(x)!=True and
                                        (any([y.isdigit() for y in x])==True) and
                                        (any(['+' in z for z in x])!=True) and
                                        ('ft' in x.split()) else x) )
# h2['preheight2'].unique()
h2['preunit2'] = height2['WHEIGHT'].apply(lambda x: 'ft' if (any([y.isalpha() for y in x])==True) and
                                           ('in' in x.split()) and
                                           (len(x.split())<=2) else (x.split()[1] if ('ft' in x) and (pd.isnull(x)!=True) else x ) )
# trying to fix the plus signs
str = '60+ ft'
float(str.split()[0].replace('+',''))
# height[['+' in x for x in height['WHEIGHT']]]
# this column will have both floats from previous conversion, remnant strings with '+' signs, and null values
# target all the nulls with isinstance() instead of just nulls with pd.isnull()
h2['preheight2']=h2['preheight2'].apply(lambda x: float(x.split()[0].replace('+','')) if (isinstance(x,float)!=True) and
                                        ('+' in x.split()) else x)

# fixing the 'None' value in the column 'WPLANT'
h2['WPLANT'] = h2['WPLANT'].apply(lambda x: '' if ('None' in x) else x)


# height_1['HEIGHT'].apply(lambda x: round((float(x.split()[0])*0.083333),3) if (any([y.isdigit() for y in x])==True) and (any(['+' in z for z in x])!=True) else x )
# height2['HEIGHT'].apply(lambda x: 'ft' if (any([y.isalpha() for y in x])==True) and
#                                            ('in' in x.split()) and
#                                            (len(x.split())<=2) else x )

h1=height_1.copy(deep=True)

h1['preheight']=height2['HEIGHT'].apply(lambda x: round((float(x.split()[0])*0.083333),3) if pd.isnull(x)!=True and
                                    (any([y.isdigit() for y in x])==True) and
                                    (any(['+' in z for z in x])!=True) and
                                    ('in' in x.split()) else (round(float(x.split()[0]),3) if pd.isnull(x)!=True and
                                        (any([y.isdigit() for y in x])==True) and
                                        (any(['+' in z for z in x])!=True) and
                                        ('ft' in x.split()) else x) )
h1['preunit'] = height2['HEIGHT'].apply(lambda x: 'ft' if (any([y.isalpha() for y in x])==True) and
                                           ('in' in x.split()) and
                                           (len(x.split())<=2) else (x.split()[1] if ('ft' in x) and  (pd.isnull(x)!=True) else x ) )

h1['preheight']=h1['preheight'].apply(lambda x: float(x.split()[0].replace('+','')) if (isinstance(x,float)!=True) and
                                    ('+' in x.split()) else x)

h1['HPLANT'] = h1['HPLANT'].apply(lambda x: '' if ('None' in x) else x)
pastureheight['HPLANT'] = pastureheight['HPLANT'].apply(lambda x: '' if ('None' in x) else x)
# both preunits have 0's
# h1[['ft' in x for x in h1['HEIGHT']]]
# h1[['0' in x for x in h1['preunit']]]
# h2[h2['preheight2']=='0']
h1['preunit']=h1['preunit'].apply(lambda x: '' if (x=='0') else x)
h2['preunit2']=h2['preunit2'].apply(lambda x: '' if (x=='0') else x)
# h1[h1['preheight']=='0']

h1=h1.rename(columns={'HEIGHT':'HEIGHT_OLD'})
h1=h1.rename(columns={'preheight':'HEIGHT', 'preunit':'HEIGHT_UNIT'})

h2=h2.rename(columns={'WHEIGHT':'WHEIGHT_OLD'})
h2=h2.rename(columns={'preheight2':'WHEIGHT', 'preunit2':'WHEIGHT_UNIT'})

pastureheight= pd.concat([h1,h2,height_tail], axis=1)
# pasture heights = done

pheight = df1.copy(deep=True)
ph1=pheight.iloc[:,0:33]
ph2=pheight.iloc[:,33:]

ph1['HEIGHT_UNIT'] = 'ft'
df_send(pd.concat([ph1,ph2], axis=1), 'plantheight_f', acc=True, pg=False)


pheight.columns




height2['HEIGHT_UNIT']=height2['HEIGHT'].apply(lambda x: x.split(' ')[1] if (pd.isnull(x)!=True) and len(x.split(' '))==2 else x).unique()
height2['HEIGHT'].apply(lambda x: round((float(x.split()[0])*0.083333),3) if pd.isnull(x)!=True and (any([y.isdigit() for y in x])==True) and (any(['+' in z for z in x])!=True) else x )




height2['HEIGHT_UNIT'].unique()

f=height2['HEIGHT'].apply(lambda x: "{0},{1}".format(x.split(' ')[0],x.split(' ')[1] ) if (pd.isnull(x)!=True) and len(x.split(' '))==2 else x)
f.unique()
pd.isnull(height2['HEIGHT'][0])
['+' in i for i in str2]
from decimal import *
getcontext().prec = 4

(Decimal(str.split()[0])*Decimal(0.08333333333) )
str.split()[0]
any([i.isdigit() for i in str])

float(str.split()[0])
height.columns












df_send(pastureheight, 'pastureheight', acc=True, pg=False)

fifty.SURVEY.unique()
df4.SURVEY.unique()

df5.SURVEY.unique()
df1.shape
df2.shape
df3.shape
df4.shape
df5.shape
del(wow)
wow
wow = appender(df1, fourteen,df4,df5,tablename=tname)
wow = appender(df1, tablename= tname)

wow.fix()
wow.fixed
wow.final_df[['SURVEY', 'STATE', 'COUNTY', 'PSU','POINT']][~wow.final_df[['SURVEY', 'STATE', 'COUNTY', 'PSU','POINT']].duplicated()]
nolist = ['PrimaryKey', 'FIPSPSUPNT', 'DBKey']
pklist = ['SURVEY', 'STATE', 'COUNTY', 'PSU', 'POINT']
wow.final_df.drop_duplicates(subset=[i for i in pklist if i not in nolist]).duplicated()
pd.concat([df1,fourteen,df4,df5]).drop_duplicates(subset=[i for i in pd.concat([df1,fourteen,df4,df5]).columns if i not in nolist])
dfs = {'disturbance':pd.concat([df1,fourteen,df4,df5])}
pg_send(firstp, accesspath,dfs,'disturbance',access=True, pg=False, whichdbkey=2 )
pd.concat([fifty, df4,df5]).duplicated().any()
df_send(pd.concat([fifty, df4,df5]), 'esfsg', acc=True, pg=False)
[i for i in wow.final_df.SURVEY.unique()]
[i for i in wow.final_df.STATE.unique()]
emp = set()
[emp.add(len(i)) for i in wow.final_df.POINT.unique() if len(i) not in emp]


wow.a()
nolist = ['PrimaryKey', 'FIPSPSUPNT', 'DBKey']
wow.final_df.columns
wow.final_df.drop_duplicates(subset=[i for i in df1.columns if i not in nolist], keep='first')
wow.final_df..unique()
wow.fix()
wow.fixed
wow.final_df[wow.final_df.duplicated()]
wow.fixed
# wow.fixed.to_sql(name='concern', con=engine, index=False)
drop_all(a=True)
df_send(wow.fixed, 'concern', acc=True, pg=False)
pd.read_csv()
