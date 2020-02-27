import os
import os.path
import pandas as pd
import numpy as np
from utils import db, sql_str, config
from sqlalchemy import create_engine
from psycopg2 import sql
from tqdm import tqdm
## getting in the first file dir
path=os.environ['NRIDAT']
dirs = os.listdir(path)
firstp = os.path.join(path,dirs[0])
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


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


# pg_send('disturbance')
# dfs['esfsg']
# test = dfs['point'].copy(deep=True)
# unique = set()
# for i in test['OWN']:
#     unique.add(i)
# unique2 = set()
#
# for i in test['PLOT_SIZE_HERB']:
#     unique2.add(i)
#
#
pg_send('countynm')

def pg_send(tablename):

    cursor = db.str.cursor()
    df = dfs[f'{tablename}']

    try:
        engine = create_engine(sql_str(config()))
        chunksize = int(len(df) / 10)
        tqdm.write(f'sending {tablename} to pg...')

        with tqdm(total=len(df)) as pbar:

            for i, cdf in enumerate(chunker(df,chunksize)):
                replace = "replace" if i == 0 else "append"
                t = type_lookup(cdf,tablename,2)
                temptypes = t.list
                # print(temptypes)
                tablemap = {
                    'numeric': sqlalchemy.types.Float(precision=3, asdecimal=True),
                    'character': sqlalchemy.types.VARCHAR(length=self.length)
                }

                #
                onthefly = {}
                for j,k in temptypes





                #
                # cdf.to_sql(name=f'{tablename}_NRI_Test', con=engine,index=False, if_exists='append', dtypes=onthefly)
                # pbar.update(chunksize)
                # tqdm._instances.clear()

            # print(f'{tablename} up in pg')
        tqdm.write(f'{tablename} up in pg')
        # df.to_sql(name=f'{tablename}_NRI_Test', con=engine,index=False, if_exists='append', chunksize=1000,method=None)
        # print(f'{tablename} up in pg')
    except Exception as e:
        print('mismatch between the columns in database table and supplied table')


        df = dfs[f'{tablename}']
        if tablename == 'point':
            cursor.execute("""
                        ALTER TABLE "%s" ALTER COLUMN "%s" TYPE %s
                        """ % (f'{tablename}_NRI_Test','OWN','text'))
            db.str.commit()

        if tablename == 'point':
            cursor.execute("""
                        ALTER TABLE "%s" ALTER COLUMN "%s" TYPE %s
                        """ % (f'{tablename}_NRI_Test','PLOT_SIZE_HERB','text'))
            db.str.commit()
        ## colcheck
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
                cdf.to_sql(name=f'{tablename}_NRI_Test', con=engine,index=False, if_exists='append')
                pbar.update(chunksize)
                tqdm._instances.clear()

            # print(f'{tablename} up in pg')
        tqdm.write(f'{tablename} up in pg')

# pg_send('pintercept')
#### testing chunker

# del dfs['pintercept']
# for table in dfs.keys():
#     pg_send(table)

#
# drop_all()
drop_all()
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
            # tbl = f'{tb}_NRI_Test'
            # print(tb)
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
fdict = {}
dfs = {}
bsnm = os.path.basename(firstp).replace(' ','')
hfetch = header_fetch(firstp)
tablelist =['CONCERN','COUNTYNM','DISTURBANCE','ECOSITE','GINTERCEPT', 'GPS','PINTERCEPT','POINT','PRACTICE','PRODUCTION','PTNOTE','RANGEHEALTH', 'SOILDISAG','STATENM']
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

##### joining fieldnames w data
for file in os.listdir(firstp):
    if (file.find('Coordinates')!=-1) and (file.endswith('.xlsx')==False):
        for item in os.listdir(os.path.join(firstp,file)):
            if item.find('pointcoordinates')!=-1:
                tempdf =pd.read_csv(os.path.join(firstp,file,item), sep='|', index_col=False, names=fdict['coordinates'])
                # fixing coordinates for example
                tempdf['TARGET_LONGITUDE'] = tempdf['TARGET_LONGITUDE'].map(lambda i: i*(-1))
                tempdf['FIELD_LONGITUDE'] = tempdf['FIELD_LONGITUDE'].apply(lambda i: '-'+i if '          ' not in i else i)
                # tempdf['FIELD_LONGITUDE'] = tempdf['FIELD_LONGITUDE'].map(lambda i: i*(-1))
                tempdf['data_source'] = bsnm

                dfs.update({'coordinates':tempdf})

    if (file.find('RangeChange2004')!=-1) and (file.endswith('.xlsx')==False):
        for item in os.listdir(os.path.join(firstp, file)):
            if os.path.splitext(item)[0].upper() in tablelist:

                tempdf = pd.read_csv(os.path.join(firstp,file,item), sep='|', index_col=False,low_memory=False, names=fdict[os.path.splitext(item)[0].upper()])
                tempdf['data_source'] = bsnm
                dfs.update({f'{os.path.splitext(item)[0]}':tempdf})



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
                    tempdf['RECON_WEIGHT'] = tempdf['RECON_WEIGHT'].apply(lambda i: np.nan if '        ' in i else i )
                    tempdf['RECON_WEIGHT'] = pd.to_numeric(tempdf['RECON_WEIGHT'])
                dfs.update({f'{os.path.splitext(item)[0]}':tempdf})


# test = dfs['production'].copy(deep=True)
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
# ccheck = col_check(test,'AW0125NESW')
#
# problist = {}
#
# for i in probs:
#     ccheck = col_check(test, i)
#     problist.update({f'{i}':ccheck.probs})



#
# ccheck.unique
test = dfs['production'].copy(deep=True)
test['AW0625NESW'] = test['RECON_WEIGHT'].apply(lambda i: np.nan if '        ' in i else i )

probs = ['AW0125NESW', 'AW0375NESW', 'AW0625NESW', 'AW1125NESW', 'AW1375NESW', 'AW0125NWSE', 'AW0375NWSE', 'AW0625NWSE', 'AW1125NWSE', 'AW1375NWSE']

test.dtypes
tablelist2.sort()
for table in tablelist2[12:]:
    pg_send(table.lower())



for table in dfs.keys():
    print(table)
    pg_send(table)



# pg_send()

# drop_all()
#
firstp



# pd.read_csv(exp_file)

str = "TABLE.NAME"
trns(str)

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


emp = []
str = 'Table name something'
to_csv(str)

for i in str.replace(' ', '.').split('.'):


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
st = set()
test = pd.read_csv(exp_file)

t = {}
for i in test['DATA.TYPE']:
    if i not in st:
        st.add(i)


test = dfs['statenm'].copy(deep=True)

type_lookup(test, 'concern')
for i in test.itertuples():
    print(getattr(i, 'STATE'))
    if i.STATE == 4:
        print(i)
    if test.items().index(i)==True:
        print(test.items().index(i))
dbkey = {
    1:'RangeChange2004-2008',
    2:'RangeChange2009-2015',
    3:'range2011-2016',
    4:'rangepasture2017_2018'
}

import sqlalchemy
class type_lookup:
    df = None
    tbl = None
    target = None
    list = {}
    length = None

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
            # tablemap = {
            #     'numeric': sqlalchemy.types.Float(precision=3, asdecimal=True),
            #     'character': sqlalchemy.types.VARCHAR(length=self.length)
            # }
            packed = temprow["DATA.TYPE"].values
            self.length = temprow['FIELD.SIZE']
            for j in packed:

                self.list.update({ i:f'{j}'})
            # if i in self.target['FIELD.NAME']:
            #     print(i)
            #     is_field = self.target['FIELD.NAME']==f'{i}'
            #     temprow = self.target[is_field]
            #     print(temprow)
            # if i.Index == 0:
            #     print(getattr(i, 'FIELD.NAME'), getattr(i, 'DATA.TYPE'),getattr(i, 'FIELD.SIZE'))


test = dfs['statenm'].copy(deep=True)
test
type_lookup(test,'statenm',4)
type_lookup.list















#
