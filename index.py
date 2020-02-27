import os
import os.path
import pandas as pd
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
dfs['esfsg']
pg_send('point')
def pg_send(tablename):
    # print(f'sending {tablename} to pg...')
    cursor = db.str.cursor()
    df = dfs[f'{tablename}']

    ### TODO: figure out where in the function this
    ### this if could be best placed at?
    ### after finding an error?? test individually

    ### works but now need to check PLOT_SIZE_HERB at
    ### original data: apparently there's a string '    ' (4 spaces long)
    ### CHECK

    try:
        engine = create_engine(sql_str(config()))
        chunksize = int(len(df) / 10)
        tqdm.write(f'sending {tablename} to pg...')
        with tqdm(total=len(df)) as pbar:

            for i, cdf in enumerate(chunker(df,chunksize)):
                replace = "replace" if i == 0 else "append"

                cdf.to_sql(name=f'{tablename}_NRI_Test', con=engine,index=False, if_exists='append')
                pbar.update(chunksize)
                tqdm._instances.clear()

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

        print(f'reattempting ingest of table {tablename}')
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
for i in newbies:
    tablelist2.append(i)
#### getting fieldnames
for file in os.listdir(firstp):
    if (file.find('2009')!=-1) and(file.find('Dump Columns')!=-1) and (file.startswith('~$')==False) and (file.endswith('.xlsx')==True):
        for table in tablelist2:
            hfetch.pull(file, table)

            fdict.update({f'{table}':hfetch.fields})
#### getting full 'frames
for file in os.listdir(firstp):
    if (file.find('RangeChange2009')!=-1) and (file.endswith('.xlsx')==False):
        for item in os.listdir(os.path.join(firstp, file)):
            if os.path.splitext(item)[0].upper() in tablelist2:
                tempdf = pd.read_csv(os.path.join(firstp,file,item), sep='|', index_col=False,low_memory=False, names=fdict[os.path.splitext(item)[0].upper()])
                tempdf['data_source'] = bsnm
                dfs.update({f'{os.path.splitext(item)[0]}':tempdf})

for table in dfs.keys():
    pg_send(table)



# pg_send()

drop_all()

















#
