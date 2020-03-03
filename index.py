import os, sqlalchemy
import os.path
import pandas as pd
import numpy as np
from utils import db, sql_str, config, Acc
from sqlalchemy import create_engine
import sqlalchemy_access as sa_a
from psycopg2 import sql
from tqdm import tqdm
from datetime import date
date.today().year

import urllib
import pyodbc as pyo

def ret_access(whichmdb):
    MDB = whichmdb
    DRV = '{Microsoft Access Driver (*.mdb, *.accdb)}'
    mdb_string = r"DRIVER={};DBQ={};".format(DRV,MDB)
    connection_url = f"access+pyodbc:///?odbc_connect={urllib.parse.quote_plus(mdb_string)}"
    engine = create_engine(connection_url)
    return engine

test = dfs['statenm'].copy(deep=True)
pth = os.path.join(firstp,"RangeChange2004-2008.accdb")
connection_string = (
r'DRV={Microsoft Access Driver (*.mdb, *.accdb)};'
r'DBQ=C:\\Users\\kbonefont\\Desktop\\NRI\\extracted\\2004-2015 Rangeland Change Database with Weights\\RangeChange2004-2008.accdb;'
)
pyocon = pyo.connect(ret_access(os.path.join(firstp,"RangeChange2004-2008.accdb")), autocommit=True)


ret_access(os.path.join(firstp,"RangeChange2004-2008.accdb"))

test.to_sql("test",con=ret_access(os.path.join(firstp,"RangeChange2004-2008.accdb")))

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

try:
    chunksize = int(len(test) / 10)
    tqdm.write(f'sending nri_test to access...')
    with tqdm(total=len(test)) as pbar:
        for i, cdf in enumerate(chunker(test,chunksize)):
            replace = "replace" if i == 0 else "append"
            cdf.to_sql(name='NRI_Test', con=ret_access(os.path.join(firstp,"target_mdb.accdb")),index=False, if_exists='append')
            pbar.update(chunksize)
        tqdm._instances.clear()

    tqdm.write('nri_test up in access')
except Exception as e:
    print(e)



"""
type_lookup: tool used to go through the nri explanations file and create
internal dictionaries with the data one wants based on the nri_dataframe, the
table of interest, and the dbkey/daterange of data. currently configured to
store 'list' which holds {fieldname:fieldtype} and 'length' which holds
{fieldname:fieldsize}. designed to be used by the function 'pg_send'

header_fetch: tool used to pull fieldnames. it is instantiated with a directory,
and will pull all files, and subdirectories and store them in the class' properties.
the pull method requires an excel file as an argument, and will successfully
pull fieldnames either from nri coordinates excel file or nri columns dump file.
designed to be used when creating dictionaries with the dataframes to be sent to
pg.

pg_send: using a supplied tablename, it will send a pandas dataframe to pg.
if the table already exists, it will append the data to the already existing
table. if the new table has extra columns, the function will create the new
column and add it in postgres before appending the data. the tool uses type_lookup
to bring data from the nri explanations table when creating the postgres one.
TODO's:
- assert types on newly created columns?
- state should be 2 characters long and if a rowvalue has less, a zero should
be appended in the extra space.
- county should be 3 characters long and same. zeroes should fill values w less
- all tables should have a PrimaryKey = first 5 fields concatenated
- data_source should be DBKey = NRI + Date of ingestion


drop_all: pulls all table names currently on pg, uses that list to drop them all

col_check: dev tool to pull and store unique values. for instance if a text
column has numbers but some values are whitespaces of different sizes, they
can be investigated


"""
## getting in the first file dir
path=os.environ['NRIDAT']
dirs = os.listdir(path)
firstp = os.path.join(path,dirs[0])

class type_lookup:
    df = None
    tbl = None
    target = None
    list = {}
    length = {}

    dbkey = {
        1:'RangeChange2004-2008',
        2:'RangeChange2009-2015',
        3:'range2011-2016',
        4:'rangepasture2017_2018'
    }

    def __init__(self,df,tablename,dbkeyindex):

        mainp = os.path.dirname(os.path.dirname(firstp))
        expl = os.listdir(os.path.dirname(os.path.dirname(firstp)))[-1]
        exp_file = os.path.join(mainp,expl)

        self.df = df
        self.tbl = tablename
        key = self.dbkey[dbkeyindex]

        nri_explanations = pd.read_csv(exp_file)

        is_target = nri_explanations['TABLE.NAME'] == f'{tablename.upper()}'
        self.target = nri_explanations[is_target]
        for i in self.df.columns:
            temprow = self.target[(self.target['FIELD.NAME']==i) & (self.target['DBKey']==f'{key}')  ]

            packed = temprow["DATA.TYPE"].values
            lengths = temprow["FIELD.SIZE"].values
            # self.length = temprow['FIELD.SIZE']
            # print(packed)
            for j in packed:
                self.list.update({ i:f'{j}'})
            for k in lengths:
                self.length.update({i:k})

## tool for pulling fieldnames
class header_fetch:
    all = None
    dir = None
    dirs = None
    files = []
    fields = []

    def __init__(self, targetdir):
        self.dir = targetdir
        self.all = [i for i in os.listdir(targetdir)]
        self.files = [i for i in os.listdir(targetdir) if i.endswith('.xlsx')==True]
        self.dirs = [i for i in os.listdir(targetdir) if os.path.splitext(i)[1]=='']

    def pull(self,file, col=None):
        self.fields = []

        if (file in self.files) and (file.find('Coordinates')!=-1):
            temphead = pd.read_excel(os.path.join(self.dir,file))
            for i in temphead['Field name']:
                self.fields.append(i)

        elif (file in self.files) and ('Coordinates' not in file):
            full = pd.read_excel(os.path.join(self.dir,file))
            is_concern = full['Table name']==f'{col}'
            temphead = full[is_concern]
            for i in temphead['Field name']:
                self.fields.append(i)

        elif (file.endswith('.csv')) and ('Coordinates' not in file):
            full = pd.read_csv(os.path.join(self.dir,file))
            is_target = full['TABLE.NAME']==f'{col}'
            temphead = full[is_target]
            for i in temphead['FIELD.NAME']:
                self.fields.append(i)

        else:
            print('file is not in supplied directory')


def dbkey_gen(df,newfield, *fields):
    df[f'{newfield}'] = (df[[f'{field.strip()}' for field in fields]].astype('str')).sum(axis=1)
# test
#
# test2 = tempdf.copy(deep=True)


# dbkey_gen(test,'PrimaryKey', 'SURVEY', 'STATE', 'COUNTY','PSU','POINT')

# pg_send('countynm')
# test = dfs['countynm']
def pg_send(tablename):

    cursor = db.str.cursor()
    df = dfs[f'{tablename}']
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
                t = type_lookup(cdf,tablename,2)
                temptypes = t.list
                templengths = t.length

                def alchemy_ret(type,len=None):
                    if (type=='numeric') and (len==None):
                        return sqlalchemy.types.Float(precision=3, asdecimal=True)
                    elif (type=='character') and (len!=None):
                        return sqlalchemy.types.VARCHAR(length=len)

                for key in temptypes:
                    state_key = ["STATE", "COUNTY"]
                    if key not in only_once:
                        only_once.add(key)

                        if temptypes[key]=='numeric':
                            # print('found numeric')
                            onthefly.update({f'{key}':alchemy_ret(temptypes[key])})
                            for k in state_key:
                                if k == "STATE":
                                    onthefly.update({f'{k}':alchemy_ret('character',2)})
                                if k=="COUNTY":
                                    onthefly.update({f'{k}':alchemy_ret('character',3)})
                            if key == "PTNOTE":
                                onthefly.update({"PTNOTE":sa_a.LongText})
                        if temptypes[key]=='character':
                            # print('found character')
                            onthefly.update({f'{key}':alchemy_ret(temptypes[key],templengths[key])})

                cdf.to_sql(name=f'{tablename}_NRI_Test', con=engine,index=False, if_exists='append', dtype=onthefly)
                cdf.to_sql(name=f'{tablename}_NRI_Test', con=ret_access(os.path.join(firstp,"target_mdb.accdb")),index=False, if_exists='append', dtype=onthefly)
                pbar.update(chunksize)
            tqdm._instances.clear()

        tqdm.write(f'{tablename} up in pg')

    except Exception as e:
        print('mismatch between the columns in database table and supplied table')

        df = dfs[f'{tablename}']
        dbdf = pd.read_sql(f' SELECT * FROM "{tablename}_NRI_Test" LIMIT 1', db.str)

        if len(df.columns.tolist())>1:
            for item in df.columns.tolist():
                if item not in dbdf.columns.tolist():
                    print(f'{item} is not in db')
                    vartype = {
                                'int64':'int',
                                "object":'text',
                                'datetime64[ns]':'timestamp',
                                'bool':'boolean',
                                'float64':'float'
                                }
                    cursor.execute("""
                                ALTER TABLE "%s" ADD COLUMN "%s" %s
                                """ % (f'{tablename}_NRI_Test',f'{item}',vartype[f'{df[f"{item}"].dtype}'].upper()))
                    db.str.commit()

        # print(f'reattempting ingest of table {tablename}')
        engine = create_engine(sql_str(config()))
        chunksize = int(len(df) / 10)
        tqdm.write(f'reattempting ingest of table {tablename}')
        with tqdm(total=len(df)) as pbar:

            for i, cdf in enumerate(chunker(df,chunksize)):
                replace = "replace" if i == 0 else "append"

                t = type_lookup(cdf,tablename,2)
                temptypes = t.list
                templengths = t.length

                def alchemy_ret(type,len=None):
                    if (type=='numeric') and (len==None):
                        return sqlalchemy.types.Float(precision=3, asdecimal=True)
                    elif (type=='character') and (len!=None):
                        return sqlalchemy.types.VARCHAR(length=len)
                for key in temptypes:
                    state_key = ["STATE", "COUNTY"]
                    if key not in only_once:
                        only_once.add(key)
                        if temptypes[key]=='numeric':

                            onthefly.update({f'{key}':alchemy_ret(temptypes[key])})
                            for k in state_key:
                                if k == "STATE":
                                    onthefly.update({f'{k}':alchemy_ret('character',2)})
                                if k=="COUNTY":
                                    onthefly.update({f'{k}':alchemy_ret('character',3)})
                            if key == "PTNOTE":
                                onthefly.update({"PTNOTE":sa_a.LongText})
                        if temptypes[key]=='character':

                            onthefly.update({f'{key}':alchemy_ret(temptypes[key],templengths[key])})

                cdf.to_sql(name=f'{tablename}_NRI_Test', con=engine,index=False, if_exists='append', dtype=onthefly)
                cdf.to_sql(name=f'{tablename}_NRI_Test', con=ret_access(os.path.join(firstp,"target_mdb.accdb")),index=False, if_exists='append', dtype=onthefly)
                pbar.update(chunksize)
                tqdm._instances.clear()

        tqdm.write(f'{tablename} up in pg')

#### testing chunker

# for table in dfs.keys():
#     pg_send(table)

#


def drop_all():
    con = db.str
    cur = db.str.cursor()

    tablelist = []
    cur.execute("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")
    for table in cur.fetchall():
        tablelist.append(table[0])
    try:
        for tb in tablelist:
            cur.execute(
            sql.SQL('DROP TABLE IF EXISTS nritest.public.{0}').format(
                     sql.Identifier(tb))
            )
            con.commit()
            print(f'successfully dropped {tb}')

    except Exception as e:
        con = db.str
        cur = db.str.cursor()
        print(e)



##### setting it up
test = dfs['production'].copy(deep=True)
weirdos = set()
for i in test['DRY_WEIGHT_PERCENT'].apply(lambda i: i.strip()).apply(lambda i: np.nan if i.isdigit()!=True else i):
    if i not in weirdos:
        weirdos.add(i)

fdict = {}
dfs = {}
bsnm = os.path.basename(firstp).replace(' ','')
hfetch = header_fetch(firstp)
tablelist =['CONCERN','COUNTYNM','DISTURBANCE','ECOSITE','GINTERCEPT', 'GPS','PINTERCEPT','POINT','PRACTICE','PRODUCTION','PTNOTE','RANGEHEALTH', 'SOILDISAG','STATENM']
# probs = ['AW0125NESW', 'AW0375NESW', 'AW0625NESW', 'AW1125NESW', 'AW1375NESW', 'AW0125NWSE', 'AW0375NWSE', 'AW0625NWSE', 'AW1125NWSE', 'AW1375NWSE']
# percents = ['DRY_WEIGHT_PERCENT', 'UNGRAZED_PERCENT', 'GROWTH_PERCENT', 'CLIMATE_PERCENT']
##### for rangechange2004 !!!!
##### fieldnames
for file in os.listdir(firstp):
    if (file.find('Point Coordinates')!=-1) and (file.startswith('~$')==False) and (file.endswith('.xlsx')==True):
        hfetch.pull(file)
        fdict.update({'coordinates':hfetch.fields})

    if (file.find('2004')!=-1) and(file.find('Dump Columns')!=-1) and (file.startswith('~$')==False) and (file.endswith('.xlsx')==True):
        for table in tablelist:
            hfetch.pull(file, table)
            fdict.update({f'{table}':hfetch.fields})

# tempdf
# str = "e"
# str.isalpha()
##### joining fieldnames w data
for file in os.listdir(firstp):
    if (file.find('Coordinates')!=-1) and (file.endswith('.xlsx')==False):
        for item in os.listdir(os.path.join(firstp,file)):
            if item.find('pointcoordinates')!=-1:
                tempdf =pd.read_csv(os.path.join(firstp,file,item), sep='|', index_col=False, names=fdict['coordinates'])
                # fixing coordinates for example
                if 'ELEVATION' in tempdf.columns:
                    tempdf['ELEVATION'] = tempdf['ELEVATION'].apply(lambda i: np.nan if '     ' in i else i)
                tempdf['TARGET_LONGITUDE'] = tempdf['TARGET_LONGITUDE'].map(lambda i: i*(-1))
                tempdf['FIELD_LONGITUDE'] = tempdf['FIELD_LONGITUDE'].apply(lambda i: '-'+i if '          ' not in i else i)

                t = type_lookup(tempdf, os.path.splitext(item)[0], 1)
                for field in tempdf.columns:
                    if (t.list[field]=="numeric") and (tempdf[field].dtype!=np.float64) and (tempdf[field].dtype!=np.int64):
                        tempdf[field] = tempdf[field].apply(lambda i: i.strip())
                        tempdf[field] = pd.to_numeric(tempdf[field])

                less_fields = ['statenm','countynm']
                if os.path.splitext(item)[0] not in less_fields:
                    # print(item)
                    dbkey_gen(tempdf, 'PrimaryKey', 'SURVEY', 'STATE', 'COUNTY','PSU','POINT')
                    dbkey_gen(tempdf, 'NEW_FIELD_UNIT', 'STATE', 'COUNTY','PSU','POINT')
                if 'COUNTY' in tempdf.columns:
                    tempdf['COUNTY'] = tempdf['COUNTY'].map(lambda x: f'{x:0>3}')

                if 'STATE' in tempdf.columns:
                    tempdf['STATE'] = tempdf['STATE'].map(lambda x: f'{x:0>2}')
                tempdf['DBKey'] = ''.join(['NRI_',f'{date.today().year}'])


                # tempdf['data_source'] = bsnm

                dfs.update({'coordinates':tempdf})

    if (file.find('RangeChange2004')!=-1) and (file.endswith('.xlsx')==False):
        for item in os.listdir(os.path.join(firstp, file)):
            if os.path.splitext(item)[0].upper() in tablelist:

                tempdf = pd.read_csv(os.path.join(firstp,file,item), sep='|', index_col=False,low_memory=False, names=fdict[os.path.splitext(item)[0].upper()])
                # tempdf['data_source'] = bsnm
                if item.lower()=='production.txt':
                    t = type_lookup(tempdf, os.path.splitext(item)[0], 1)

                    for field in tempdf.columns:
                        # print(field)
                        if (t.list[field]=="numeric") and (tempdf[field].dtype!=np.float64) and (tempdf[field].dtype!=np.int64):
                            tempdf[field] = tempdf[field].apply(lambda i: i.strip())

                for field in tempdf.columns:
                    stay_in_varchar = ['STATE', 'COUNTY']
                    t = type_lookup(tempdf, os.path.splitext(item)[0], 1)
                    if (t.list[field]=="numeric") and (tempdf[field].dtype!=np.float64) and (tempdf[field].dtype!=np.int64):
                        tempdf[field] = tempdf[field].apply(lambda i: i.strip())

                    if t.list[field]=="numeric" and field not in stay_in_varchar:
                        tempdf[field] = pd.to_numeric(tempdf[field])

                less_fields = ['statenm','countynm']
                if os.path.splitext(item)[0] not in less_fields:
                    # print(item)
                    dbkey_gen(tempdf, 'PrimaryKey', 'SURVEY', 'STATE', 'COUNTY','PSU','POINT')
                    dbkey_gen(tempdf, 'FIPSPSUPNT', 'STATE', 'COUNTY','PSU','POINT')

                if 'COUNTY' in tempdf.columns:
                # if tempdf['COUNTY'].any():
                    tempdf['COUNTY'] = tempdf['COUNTY'].map(lambda x: f'{x:0>3}')
                    # tempdf['COUNTY'] =
                if 'STATE' in tempdf.columns:
                # if tempdf['STATE'].any():
                    tempdf['STATE'] = tempdf['STATE'].map(lambda x: f'{x:0>2}')
                tempdf['DBKey'] = ''.join(['NRI_',f'{date.today().year}'])


                dfs.update({f'{os.path.splitext(item)[0]}':tempdf})


class col_check:
    unique = set()
    test = None
    probs = set()

    def __init__(self, df, column):
        self.test = df.copy(deep=True)
        for i in self.test[f'{column}']:
            if (' ' in i) and (i[0].isdigit()==False):
                self.probs.add(i)
            self.unique.add(i)


test.columns
for field in test.columns:
    test[field] = test[field].apply(lambda i: i.strip() )
    test[field] = test[field].apply(lambda i: np.nan if (i[0]!='-') or (i[0].isdigit()!=True) in i else i )
test['DRY_WEIGHT_PERCENT'].apply(lambda i: 'sfdf' if (i[0]!='-') or (i[0].isdigit()!=True) else i )
test.dtypes
test['ALLOW_PERCENT'][1][0].isdigit()
c = col_check(test, 'DRY_WEIGHT_PERCENT')
c.probs
str[0].isdigit()
str[0]=='-'
str = '1    '
if ('    ' in str) and (str[0]!='-'):
    print('ok')
else:
    print('no')
c.probs
tempdf[j] = tempdf[j].apply(lambda i: np.nan if ('   ' in i) or ('     ' in i) and (i[0]!='-') else i )
test['DRY_WEIGHT_PERCENT']=test['DRY_WEIGHT_PERCENT'].apply(lambda i: i.strip())
test['DRY_WEIGHT_PERCENT'].apply(lambda i: print(i) if (i[0]!='-') or (i.isdigit()!=True) else i )


problems = {}
t = type_lookup(test, 'production', 1)
t.list
for i in test.columns:
    if (i == "PrimaryKey") or (i=="DBKey"):
        pass
    elif (t.list[i]=="numeric") and (test[i].dtype!=np.float64) and (test[i].dtype!=np.int64):
        c = col_check(test, i)
        problems.update({i:c.probs})


str.strip()


test

dfs



test = dfs['production'].copy(deep=True)
c = col_check()

unique = set()

for i in test.items():
    print(i[1])

for i in test['ALLOW_PRODUCTION']:
    if i not in unique:
        unique.add(i)
test['ALLOW_PRODUCTION']
# for prob in probs:
#     if test[prob].dtype!= np.float64:
#         print(prob)
# from datetime import datetime
#
# test['DBKey'] = ''.join(['NRI_',f'{datetime.now()}'])



# dbkey_gen(test,'DBKey', 'SURVEY', 'STATE', 'COUNTY','PSU','POINT')
#
# test['COUNTY'].map(lambda x: f'{x:0>3}')
# test['STATE'].map(lambda x: f'{x:0>2}')
# if 'COUNTY' in test.columns:
#     print('p')
# pg_send('coordinates')
drop_all()
tblist = []
len(tablelist)
tablelist.append('coordinates')
for i in tablelist:
    tblist.append(i)
for table in tablelist[7:]:
    pg_send(table.lower())
for table in dfs.keys():
    pg_send(table)

##### for rangechange2009+
##### fieldnames + esfsg, pastureheights, plantcensus, soilhorizon
fdict = {}
dfs= {}
tablelist2 = tablelist
newbies = ['ESFSG','PASTUREHEIGHTS', 'PLANTCENSUS', 'SOILHORIZON']
probs = ['AW0125NESW', 'AW0375NESW', 'AW0625NESW', 'AW1125NESW', 'AW1375NESW', 'AW0125NWSE', 'AW0375NWSE', 'AW0625NWSE', 'AW1125NWSE', 'AW1375NWSE']

for i in newbies:
    tablelist2.append(i)
#### getting fieldnames
for file in os.listdir(firstp):
    if (file.find('2009')!=-1) and(file.find('Dump Columns')!=-1) and (file.startswith('~$')==False) and (file.endswith('.xlsx')==True):
        for table in tablelist2:
            hfetch.pull(file, table)

            fdict.update({f'{table}':hfetch.fields})
#### getting full 'frames
probs = ['AW0125NESW', 'AW0375NESW', 'AW0625NESW', 'AW1125NESW', 'AW1375NESW', 'AW0125NWSE', 'AW0375NWSE', 'AW0625NWSE', 'AW1125NWSE', 'AW1375NWSE']


for file in os.listdir(firstp):
    if (file.find('RangeChange2009')!=-1) and (file.endswith('.xlsx')==False):
        for item in os.listdir(os.path.join(firstp, file)):
            if os.path.splitext(item)[0].upper() in tablelist2:
                tempdf = pd.read_csv(os.path.join(firstp,file,item), sep='|', index_col=False,low_memory=False, names=fdict[os.path.splitext(item)[0].upper()])
                tempdf['data_source'] = bsnm
                if item.lower()=='production.txt':
                    for prob in probs:
                        tempdf[prob] = tempdf[prob].apply(lambda i: np.nan if '       ' in i else i )
                        tempdf[prob] = pd.to_numeric(tempdf[prob])
                ### catching random whitespace
                if 'ELEVATION' in tempdf.columns:
                    tempdf['ELEVATION'] = tempdf['ELEVATION'].apply(lambda i: np.nan if '     ' in i else i)

                if 'SOIL_CONFIDENCE_RATING' in tempdf.columns:
                    tempdf['SOIL_CONFIDENCE_RATING'] = tempdf['SOIL_CONFIDENCE_RATING'].apply(lambda i: np.nan if ' ' in i else i)
                tempdf['RECON_WEIGHT'] = tempdf['RECON_WEIGHT'].apply(lambda i: np.nan if '        ' in i else i )



                tempdf['RECON_WEIGHT'] = pd.to_numeric(tempdf['RECON_WEIGHT'])

                less_fields = ['statenm','countynm']
                if os.path.splitext(item)[0] not in less_fields:
                    # print(item)
                    dbkey_gen(tempdf, 'PrimaryKey', 'SURVEY', 'STATE', 'COUNTY','PSU','POINT')

                if 'COUNTY' in tempdf.columns:
                # if tempdf['COUNTY'].any():
                    tempdf['COUNTY'] = tempdf['COUNTY'].map(lambda x: f'{x:0>3}')
                if 'STATE' in tempdf.columns:
                # if tempdf['STATE'].any():
                    tempdf['STATE'] = tempdf['STATE'].map(lambda x: f'{x:0>2}')
                tempdf['DBKey'] = ''.join(['NRI_',f'{date.today()}'])

                dfs.update({f'{os.path.splitext(item)[0]}':tempdf})


# test = dfs['gps'].copy(deep=True)
# #first: the column is text/object, get a null in the space first? yes, the 8 spaces cannot be coerced
# import numpy as np
#
# test['RECON_WEIGHT'] = test['RECON_WEIGHT'].apply(lambda i: np.nan if '        ' in i else i )
# test['RECON_WEIGHT'] = pd.to_numeric(test['RECON_WEIGHT'])
#
#

class col_check:
    unique = set()
    test = None
    probs = set()

    def __init__(self, df, column):
        self.test = df.copy(deep=True)
        for i in self.test[f'{column}']:
            if (' ' in i) and (i[0].isdigit()==False):
                self.probs.add(i)
            self.unique.add(i)
#
ccheck = col_check(test,)
ccheck.probs
# problist = {}
#
# for i in probs:
#     ccheck = col_check(test, i)
#     problist.update({f'{i}':ccheck.probs})



#
# ccheck.unique



def to_excel(str):
    temp = []
    for wordindex in range(0,len(str.split('.'))):
        if wordindex==0:
            firstword = str.split('.')[wordindex].lower().capitalize()
            temp.append(firstword)
        else:
            therest = str.split('.')[wordindex].lower()
            temp.append(therest)

    return (' ').join(temp)


def to_csv(str):
    temp = []
    for i in str.replace(' ', '.').split('.'):
        temp.append(i.upper())
    return '.'.join(temp)





### all to_sql functions needd the DTYPES argument to force the PG types that
### NRI wants!!!


#### already have the tablename equalizer, just need to
### look up by [ table, column] the nri explanations
### once a match is found, use a fixed dictionary to
### translate between the types in the nri explanations
### and sqlalchemy.

### probably will need to create a dictionary on the fly for each tables
### as they are all of different sizes

"""
typedict = {}
fun(tablename)
for all the columns in newtable.columns:
    if fixedcolumn is in oldcolumns:


"""
















#
