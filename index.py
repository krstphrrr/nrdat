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
X assert types on newly created columns
X state should be 2 characters long and if a rowvalue has less, a zero should
be appended in the extra space.
X county should be 3 characters long and same. zeroes should fill values w less
X all tables should have a PrimaryKey = first 5 fields concatenated
X data_source should be DBKey = NRI + Date of ingestion


drop_all: pulls all table names currently on pg, uses that list to drop them all

col_check: dev tool to pull and store unique values. for instance if a text
column has numbers but some values are whitespaces of different sizes, they
can be investigated

ret_access: function where you supply a path to an access database file, and
it returns a sql alchemy engine


"""

## getting in the first file dir
path=os.environ['NRIDAT']
dirs = os.listdir(path)
firstp = os.path.join(path,dirs[0])
accesspath =os.path.join(firstp,"target_mdb.accdb")
# a = Acc(accesspath)
# a_con = a.con

def ret_access(whichmdb):
    MDB = whichmdb
    DRV = '{Microsoft Access Driver (*.mdb, *.accdb)}'
    mdb_string = r"DRIVER={};DBQ={};".format(DRV,MDB)
    connection_url = f"access+pyodbc:///?odbc_connect={urllib.parse.quote_plus(mdb_string)}"
    engine = create_engine(connection_url)
    return engine


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

    def __init__(self,df,tablename,dbkeyindex, path):

        mainp = os.path.dirname(os.path.dirname(path))
        expl = os.listdir(os.path.dirname(os.path.dirname(path)))[-1]
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


def pg_send(mainpath,acc_path, dict, tablename, access = False, pg=False):
    """
    usage:
    pg_send('target_directory_path', 'path_to_access_file', target_dictionary, 'target_table')
    todo:
    - switching off/on access and pg
    """
    con = db.str
    cursor = con.cursor()
    #access path changes!
    cxn = ret_access(acc_path)
    df = dict[f'{tablename}']
    def chunker(seq, size):
        return (seq[pos:pos + size] for pos in range(0, len(seq), size))

    try:
        engine = create_engine(sql_str(config()))
        chunksize = int(len(df) / 10)
        tqdm.write(f'sending {tablename} to pg...')
        only_once = set()
        onthefly = {}

        with tqdm(total=len(df)) as pbar:
            """
            loop to use the progress bar on each iteration
            """

            for i, cdf in enumerate(chunker(df,chunksize)):
                replace = "replace" if i == 0 else "append"
                t = type_lookup(cdf,tablename,2,mainpath)
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
                # checking access/pg switch
                if (access!=False) and (pg!=False):
                    cdf.to_sql(name=f'{tablename}', con=engine,index=False, if_exists='append', dtype=onthefly)
                    cdf.to_sql(name=f'{tablename}', con=ret_access(acc_path),index=False, if_exists='append', dtype=onthefly)

                elif (access!=False) and (pg==False):
                    cdf.to_sql(name=f'{tablename}', con=ret_access(acc_path),index=False, if_exists='append', dtype=onthefly)

                elif (access==False) and (pg!=False):
                    pass
                    # cdf.to_sql(name=f'{tablename}', con=engine,index=False, if_exists='append', dtype=onthefly)
                elif (access==False) and (pg==False):
                    dir = os.path.join(mainpath,'csvs')
                    if not os.path.exists(dir):
                        os.mkdir(dir)
                    df.to_csv(os.path.join(dir,f'{tablename}.csv'),index=False)
                else:
                    tqdm.write("Please set the access/pg booleans in pg_send's arguments")
                    # print("Please set the access/pg booleans in pg_send's arguments")

                pbar.update(chunksize)

            tqdm._instances.clear()

        tqdm.write(f'{tablename} sent to db')

    except Exception as e:
        print('mismatch between the columns in database table and supplied table')
        # cxn = ret_access(accesspath)

        df = dict[f'{tablename}']
        print('checking length of columns vector...')
        cont = None
        pg = None
        acc = None
        if (access!=False) and (pg!=False): # both true
            cont = True
            pg = True
            acc = True
            dbdf = pd.read_sql(f' SELECT * FROM "{tablename}" LIMIT 1', con)
        elif (access==False) and (pg!=False): # access false , pg true
            cont = True
            pg=True
            acc = False
            dbdf = pd.read_sql(f' SELECT * FROM "{tablename}" LIMIT 1', con)
        elif access!=False and (pg==False): # only access is true
            cont = True
            pg = False
            acc = True
            dbdf = pd.read_sql(f"SELECT TOP 1 * FROM {tablename}", cxn)
        else:
            cont = False
            pg = False
            acc = False

            print("Please set the access/pg booleans in pg_send's arguments")


        if cont is True:
            print(len(df.columns.tolist()))
            # if len(df.columns.tolist())>1:
            #     try:
            #         for item in df.columns.tolist():
            #             if item not in dbdf.columns.tolist():
            #                 print(f'{item} is not in db')
            #                 # vartype = {
            #                 #             'int64':'int',
            #                 #             "object":'text',
            #                 #             'datetime64[ns]':'timestamp',
            #                 #             'bool':'boolean',
            #                 #             'float64':'float'
            #                 #             }
            #
            #                 if pg is True:
            #                     print(f'creating {item} column on pg...')
            #                     # cursor.execute("""
            #                     #             ALTER TABLE "%s" ADD COLUMN "%s" %s
            #                     #             """ % (f'{tablename}',f'{item}',vartype[f'{df[f"{item}"].dtype}'].upper()))
            #                     # con.commit()
            #                 if acc is True:
            #                     print(f'creating {item} column on access..')
            #                 # print(cxn,f'{tablename}', f'{item}',vartype[f'{df[f"{item}"].dtype}'].upper() )
            #                     # cxn.execute(DDL("ALTER TABLE {0} ADD COLUMN {1} {2}".format(f'{tablename}',f'{item}',vartype[f'{df[f"{item}"].dtype}'].upper() )))
            #     except Exception as e:
            #         print(e)
            #         print('something went wrong')
            #         cxn = ret_access(acc_path)
            #         con = db.str
            #         cursor = db.str.cursor()

            # engine = create_engine(sql_str(config()))
            # chunksize = int(len(df) / 10)
            # tqdm.write(f'reattempting ingest of table {tablename}')
            # with tqdm(total=len(df)) as pbar:
            #
            #     for i, cdf in enumerate(chunker(df,chunksize)):
            #         replace = "replace" if i == 0 else "append"
            #
            #         t = type_lookup(cdf,tablename,2,mainpath)
            #         temptypes = t.list
            #         templengths = t.length
            #
            #         def alchemy_ret(type,len=None):
            #             if (type=='numeric') and (len==None):
            #                 return sqlalchemy.types.Float(precision=3, asdecimal=True)
            #             elif (type=='character') and (len!=None):
            #                 return sqlalchemy.types.VARCHAR(length=len)
            #         for key in temptypes:
            #             state_key = ["STATE", "COUNTY"]
            #             if key not in only_once:
            #                 only_once.add(key)
            #                 if temptypes[key]=='numeric':
            #
            #                     onthefly.update({f'{key}':alchemy_ret(temptypes[key])})
            #                     for k in state_key:
            #                         if k == "STATE":
            #                             onthefly.update({f'{k}':alchemy_ret('character',2)})
            #                         if k=="COUNTY":
            #                             onthefly.update({f'{k}':alchemy_ret('character',3)})
            #
            #                 if temptypes[key]=='character':
            #                     onthefly.update({f'{key}':alchemy_ret(temptypes[key],templengths[key])})
            #                     if key == "PTNOTE":
            #                         onthefly.update({"PTNOTE":sqlalchemy.types.Text})
            #
            #         if (access!=False) and (pg!=False):
            #             cdf.to_sql(name=f'{tablename}', con=engine,index=False, if_exists='append', dtype=onthefly)
            #             cdf.to_sql(name=f'{tablename}', con=ret_access(acc_path),index=False, if_exists='append', dtype=onthefly)
            #
            #         elif (access!=False) and (pg==False):
            #             cdf.to_sql(name=f'{tablename}', con=ret_access(acc_path),index=False, if_exists='append', dtype=onthefly)
            #
            #         elif (access==False) and (pg!=False):
            #             cdf.to_sql(name=f'{tablename}', con=engine,index=False, if_exists='append', dtype=onthefly)
            #         elif (access==False) and (pg==False):
            #             dir = os.path.join(mainpath,'csvs')
            #             if not os.path.exists(dir):
            #                 os.mkdir(dir)
            #             df.to_csv(os.path.join(dir,f'{tablename}.csv'),index=False)
            #         else:
            #             print("Please set the access/pg booleans in pg_send's arguments")
            #         pbar.update(chunksize)
            #         tqdm._instances.clear()
            #
            # tqdm.write(f'{tablename} up in pg')
        else:
            print('data ingest aborted.')

df = pd.DataFrame({
"col1":["  ", "2.0", ".","Dr. Martin 3rd"],
"col2":["a","b","c","d"]
})

df["col1"].apply(lambda i: "" if ('.' in i) and (any([(j.isalpha()) or (j.isdigit()) for j in i])!=True) else i)

f = df_builder_for_2004(firstp, 'RangeChange2004-2008')
f.extract_fields('2004')
f.expl
# [i for i in f.df[f.df['DBKey']==]['TABLE.NAME'].unique()]
f.fields_dict

f.tablelist

f.append_fields('2004')

drop_all(a=True)
def drop_all(specifictable = None, a=False):
    con = db.str
    cur = db.str.cursor()
    cxn = ret_access(accesspath)

    tablelist = []
    cur.execute("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")
    for table in cur.fetchall():
        tablelist.append(table[0])
    if specifictable!=None and specifictable in tablelist:
        try:
            cur.execute(
            sql.SQL('DROP TABLE IF EXISTS nritest.public.{0}').format(
                     sql.Identifier(specifictable))
            )
            con.commit()
            if a==True:
                cxn.execute(DDL("DROP TABLE {0}".format(f'{specifictable}')))

            print(f'successfully dropped {specifictable}')

        except Exception as e:
            con = db.str
            cur = db.str.cursor()
            print(e)
    elif specifictable is None and a is True:
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

class df_builder_for_2004:
    fields_dict = {}
    dfs = {}
    dbkcount = 0

    dbkeys = None
    path = None
    mainp = None
    expl = None
    df = None
    # realp = None
    temp_coords = None

    tablelist = []
    _dbkey = {
        'RangeChange2004-2008':1,
        'RangeChange2009-2015':2,
        'range2011-2016':3,
        'rangepasture2017_2018':4
    }
    dbkey = None




    def __init__(self, path, dbkey):
        """
        usage:
        -> instance = df_builder_for_2004('target_directory', 1)
            - populates tablelist

        'Raw data dump' does not exist here, so self.realp is unnecessary

        """
        self.path = path
        self.mainp = os.path.dirname(os.path.dirname(path))
        # self.realp = os.path.join(path,'Raw data dump')
        self.expl = os.listdir(self.mainp)[-1]
        self.df = pd.read_csv(os.path.join(self.mainp, self.expl))
        self.dbkeys = {key:value for (key,value) in enumerate([i for i in self.df['DBKey'].unique()])}
        if dbkey in self._dbkey.keys():
            self.dbkey = self._dbkey[dbkey]

        self.tablelist = [i for i in self.df[self.df['DBKey']==f'{dbkey}']['TABLE.NAME'].unique()]

    def extract_fields(self, findable_string):
        """
        usage:
           instance = df_builder_for_2004('target_directory', 1)
        -> instance.extract_fields('2004')
            - populates instance.fields_dict with fields
           instance.append_fields('RangeChange2004')


        """
        for file in os.listdir(self.path):
            if (file.find('Point Coordinates')!=-1) and (file.startswith('~$')==False) and (file.endswith('.xlsx')==True):
                header = header_fetch(self.path)
                header.pull(file)
                self.fields_dict.update({'POINTCOORDINATES':header.fields})

            if (file.find('2004')!=-1) and(file.find('Dump Columns')!=-1) and (file.startswith('~$')==False) and (file.endswith('.xlsx')==True):
                for table in self.tablelist:
                    header = header_fetch(self.path)
                    header.pull(file, table)
                    self.fields_dict.update({f'{table}':header.fields})

    def append_fields(self, findable_string):
        """
        usage:
           instance = df_builder_for_2004('target_directory', 1)
           instance.extract_fields('2004')
        -> instance.append_fields('RangeChange2004')
            - populates instance.dfs with all the dataframes
        """

        for file in os.listdir(self.path):
            if (file.find('Coordinates')!=-1) and (file.endswith('.xlsx')==False):
                for item in os.listdir(os.path.join(self.path,file)):
                    if item.find('pointcoordinates')!=-1:
                        tempdf =pd.read_csv(os.path.join(self.path,file,item), sep='|', index_col=False, names=self.fields_dict['POINTCOORDINATES'])

                        t = type_lookup(tempdf, os.path.splitext(item)[0], self.dbkey, self.path)
                        fix_longitudes = ['TARGET_LONGITUDE','FIELD_LONGITUDE']
                        for field in tempdf.columns:
                            if (t.list[field]=="numeric") and (tempdf[field].dtype!=np.float64) and (tempdf[field].dtype!=np.int64):
                                tempdf[field] = tempdf[field].apply(lambda i: i.strip())
                                tempdf[field] = pd.to_numeric(tempdf[field])

                            if field in fix_longitudes:
                                tempdf[field] = tempdf[field].map(lambda i: i*(-1))

                        less_fields = ['statenm','countynm']
                        if os.path.splitext(item)[0] not in less_fields:

                            dbkey_gen(tempdf, 'PrimaryKey', 'SURVEY', 'STATE', 'COUNTY','PSU','POINT')
                            dbkey_gen(tempdf, 'FIPSPSUPNT', 'STATE', 'COUNTY','PSU','POINT')
                        if 'COUNTY' in tempdf.columns:
                            tempdf['COUNTY'] = tempdf['COUNTY'].map(lambda x: f'{x:0>3}')

                        if 'STATE' in tempdf.columns:
                            tempdf['STATE'] = tempdf['STATE'].map(lambda x: f'{x:0>2}')
                        tempdf['DBKey'] = ''.join(['NRI_',f'{date.today().year}'])
                        self.temp_coords = tempdf.copy(deep=True)
                        self.dfs.update({'pointcoordinates':tempdf})

            if (file.find(findable_string)!=-1) and (file.endswith('.xlsx')==False):

                for item in os.listdir(os.path.join(self.path, file)):
                    # print(os.path.splitext(item)[0].upper(), self.tablelist)
                    if os.path.splitext(item)[0].upper() in self.tablelist:
                        # print(item)

                        tempdf = pd.read_csv(os.path.join(self.path,file,item), sep='|', index_col=False,low_memory=False, names=self.fields_dict[os.path.splitext(item)[0].upper()])

                        # for all fields, if a field is numeric in lookup AND (not np.float or np.int), strip whitespace!
                        # after stripping, if numeric and not in "stay_in_varchar" list, convert to pandas numeric!
                        # empty spaces should automatically change into np.nan / compatible null values

                        for field in tempdf.columns:
                            stay_in_varchar = ['STATE', 'COUNTY']

                            t = type_lookup(tempdf, os.path.splitext(item)[0], self.dbkey, self.path)
                            if (t.list[field]=="numeric") and (tempdf[field].dtype!=np.float64) and (tempdf[field].dtype!=np.int64):
                                tempdf[field] = tempdf[field].apply(lambda i: i.strip())

                            if t.list[field]=="numeric" and field not in stay_in_varchar:
                                tempdf[field] = pd.to_numeric(tempdf[field])

                            # for fields with dots in them..
                            dot_list = ['HIT1','HIT2','HIT3', 'HIT4', 'HIT5', 'HIT6']
                            if field in dot_list:
                                tempdf[field] = tempdf[field].apply(lambda i: "" if ('.' in i) and (any([(j.isalpha()) or (j.isdigit()) for j in i])!=True) else i)

                        # for all tables not in "less_fields" list, create two new fields
                        less_fields = ['statenm','countynm']
                        if os.path.splitext(item)[0] not in less_fields:
                            # print(item)
                            dbkey_gen(tempdf, 'PrimaryKey', 'SURVEY', 'STATE', 'COUNTY','PSU','POINT')
                            dbkey_gen(tempdf, 'FIPSPSUPNT', 'STATE', 'COUNTY','PSU','POINT')

                        # if table has field 'COUNTY', fill with leading zeroes
                        if 'COUNTY' in tempdf.columns:
                            tempdf['COUNTY'] = tempdf['COUNTY'].map(lambda x: f'{x:0>3}')
                        # if table has field 'STATE', fill with leading zeroes
                        if 'STATE' in tempdf.columns:
                            tempdf['STATE'] = tempdf['STATE'].map(lambda x: f'{x:0>2}')
                        # create simple dbkey field
                        tempdf['DBKey'] = ''.join(['NRI_',f'{date.today().year}'])

                        if 'point' in item:
                            # adding landuse from points table to coords
                            point_slice = tempdf[['LANDUSE', 'PrimaryKey']].copy(deep=True)
                            coords_dup = pd.concat([self.temp_coords,point_slice], axis=1, join="inner")
                            coords_full = coords_dup.loc[:,~coords_dup.columns.duplicated()]
                            self.dfs.update({'pointcoordinates': coords_full})
                        self.dfs.update({f'{os.path.splitext(item)[0]}':tempdf})










t = df_builder_for_2009(firstp,'RangeChange2009-2015')
t.extract_fields('2009')

t.append_fields('2009')
t.dfs
for table in t.dfs.keys():
    if 'pintercept' in table:
        pass
    else:
        pg_send(firstp,accesspath, t.dfs, table, access=False, pg=True)

pg_send(firstp, accesspath, f.dfs, 'pintercept', access=False, pg=True)



##### for rangechange2009+
##### fieldnames + esfsg, pastureheights, plantcensus, soilhorizon

class df_builder_for_2009:
    fields_dict = {}
    dfs = {}
    dbkcount = 0

    dbkeys = None
    path = None
    mainp = None
    expl = None
    df = None
    # realp = None
    # temp_coords = None

    tablelist = []
    _dbkey = {
        'RangeChange2004-2008':1,
        'RangeChange2009-2015':2,
        'range2011-2016':3,
        'rangepasture2017_2018':4
    }
    dbkey = None

    def __init__(self, path, dbkey):
        """
        usage:
        -> instance = df_builder_for_2009('target_directory', 2)
            - populates tablelist

        'Raw data dump' does not exist here, so self.realp is unnecessary

        """
        self.path = path
        self.mainp = os.path.dirname(os.path.dirname(path))
        # self.realp = os.path.join(path,'Raw data dump')
        self.expl = os.listdir(self.mainp)[-1]
        self.df = pd.read_csv(os.path.join(self.mainp, self.expl))
        self.dbkeys = {key:value for (key,value) in enumerate([i for i in self.df['DBKey'].unique()])}
        if dbkey in self._dbkey.keys():
            self.dbkey = self._dbkey[dbkey]
        self.tablelist = [i for i in self.df[self.df['DBKey']==f'{dbkey}']['TABLE.NAME'].unique()]

    def extract_fields(self, findable_string):
        """
        usage:
           instance = df_builder_for_2009('target_directory', 2)
        -> instance.extract_fields('2009')
            - populates instance.fields_dict with fields
           instance.append_fields('RangeChange2009')


        """
        for file in os.listdir(self.path):
            # if (file.find('Point Coordinates')!=-1) and (file.startswith('~$')==False) and (file.endswith('.xlsx')==True):
            #     header = header_fetch(self.path)
            #     header.pull(file)
            #     self.fields_dict.update({'pointcoordinates':header.fields})

            if (file.find(f'{findable_string}')!=-1) and(file.find('Dump Columns')!=-1) and (file.startswith('~$')==False) and (file.endswith('.xlsx')==True):
                for table in self.tablelist:
                    header = header_fetch(self.path)
                    header.pull(file, table)
                    self.fields_dict.update({f'{table}':header.fields})

    def append_fields(self, findable_string):
        """
        usage:
           instance = df_builder_for_2009('target_directory', 1)
           instance.extract_fields('2009')
        -> instance.append_fields('RangeChange2009')
            - populates instance.dfs with all the dataframes



        ///// temp_coords attribute not created with this dataset!!!!
        """

        for file in os.listdir(self.path):
            """ No coordinates file!
            """
            if (file.find(findable_string)!=-1) and (file.endswith('.xlsx')==False):
                for item in os.listdir(os.path.join(self.path, file)):
                    if os.path.splitext(item)[0].upper() in self.tablelist:


                        tempdf = pd.read_csv(os.path.join(self.path,file,item), sep='|', index_col=False,low_memory=False, names=self.fields_dict[os.path.splitext(item)[0].upper()])

                        # for all fields, if a field is numeric in lookup AND (not np.float or np.int), strip whitespace!
                        # after stripping, if numeric and not in "stay_in_varchar" list, convert to pandas numeric!
                        # empty spaces should automatically change into np.nan / compatible null values

                        for field in tempdf.columns:
                            stay_in_varchar = ['STATE', 'COUNTY']

                            t = type_lookup(tempdf, os.path.splitext(item)[0], self.dbkey, self.path)
                            if (t.list[field]=="numeric") and (tempdf[field].dtype!=np.float64) and (tempdf[field].dtype!=np.int64):
                                tempdf[field] = tempdf[field].apply(lambda i: i.strip())

                            if t.list[field]=="numeric" and field not in stay_in_varchar:
                                if 'SAGEBRUSH_SHAPE' in field:
                                    tempdf[field] = tempdf[field].apply(lambda i: np.nan if ('.' in i) and (any([j.isdigit() for j in i])!=True) else i)
                                tempdf[field] = pd.to_numeric(tempdf[field])

                        # for all tables not in "less_fields" list, create two new fields
                        less_fields = ['statenm','countynm']
                        if os.path.splitext(item)[0] not in less_fields:
                            # print(item)
                            dbkey_gen(tempdf, 'PrimaryKey', 'SURVEY', 'STATE', 'COUNTY','PSU','POINT')
                            dbkey_gen(tempdf, 'FIPSPSUPNT', 'STATE', 'COUNTY','PSU','POINT')

                        # if table has field 'COUNTY', fill with leading zeroes
                        if 'COUNTY' in tempdf.columns:
                            tempdf['COUNTY'] = tempdf['COUNTY'].map(lambda x: f'{x:0>3}')
                        # if table has field 'STATE', fill with leading zeroes
                        if 'STATE' in tempdf.columns:
                            tempdf['STATE'] = tempdf['STATE'].map(lambda x: f'{x:0>2}')
                        # create simple dbkey field
                        tempdf['DBKey'] = ''.join(['NRI_',f'{date.today().year}'])

                        # if 'point' in item:
                        #     # adding landuse from points table to coords
                        #     point_slice = tempdf[['LANDUSE', 'PrimaryKey']].copy(deep=True)
                        #     coords_dup = pd.concat([self.temp_coords,point_slice], axis=1, join="inner")
                        #     coords_full = coords_dup.loc[:,~coords_dup.columns.duplicated()]
                        #     self.dfs.update({'pointcoordinates': coords_full})
                        self.dfs.update({f'{os.path.splitext(item)[0]}':tempdf})

t = df_builder_for_2009(firstp,2)
t.extract_fields('2009')
t.append_fields('2009')
t.fields_dict
for table in f.dfs.keys():
    if 'pintercept' in table:
        pass
    else:
        pg_send(firstp,accesspath, f.dfs, table, access=False, pg=True)

pg_send(firstp, accesspath, f.dfs, 'pintercept', access=False, pg=True)

class col_check:
    unique = set()
    test = None
    probs = set()

    def __init__(self, df, column):
        self.test = df.copy(deep=True)
        for i in self.test[f'{column}'].astype('str'):
            if (' ' in i) and (i[0].isdigit()==False):
                self.probs.add(i)
            self.unique.add(i)
