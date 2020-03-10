import os
import os.path
import pandas as pd
import numpy as np
from datetime import date
from index import header_fetch, ret_access, type_lookup, dbkey_gen, pg_send

path=os.environ['NRIDAT']
dirs = os.listdir(path)
secondp = os.path.join(path,dirs[1])
# os.listdir(secondp)
accesspath = os.path.join(secondp,'Raw data dump','target_mdb.accdb')
# ret_access(accesspath)
"""
########### 2009-2016 or 2011-2016??
1. type_lookup now requires path argument when initializing
2.
 what about indicators
 getting in the first file dir
"""
# f.path

# secondp
f = first_round(secondp,'range2011-2016' )
f.extract_fields('2009')
# for file in os.listdir(f.realp):
#     if (file.find('Point Coordinates')!=-1) and (file.startswith('~$')==False) and (file.endswith('.xlsx')==True):
#         hfetch.pull(file)
f.append_fields('2011')
f.dfs.keys()

for table in f.dfs.keys():
    pg_send(secondp,accesspath, f.dfs, table)

f.dfs # <-- dictionary for pg_send
pg_send(accesspath, '')
h = header_fetch(f.realp)
h.all
h.dir
h.files
os.listdir(os.path.join(secondp, "Raw data dump"))
h
class first_round:
    fields_dict = {}
    dfs = {}
    dbkcount = 0

    dbkeys = None
    path = None
    mainp = None
    expl = None
    df = None
    realp = None
    temp_coords =None

    tablelist = []

    def __init__(self, path, dbkey):
        self.path = path
        self.mainp = os.path.dirname(os.path.dirname(path))
        self.realp = os.path.join(path,'Raw data dump')
        self.expl = os.listdir(self.mainp)[-1]
        self.df = pd.read_csv(os.path.join(self.mainp, self.expl))
        self.dbkeys = {key:value for (key,value) in enumerate([i for i in self.df['DBKey'].unique()])}
        self.tablelist = [i for i in self.df[self.df['DBKey']==f'{dbkey}']['TABLE.NAME'].unique()]

    def extract_fields(self, findable_string):
        for file in os.listdir(self.realp):
            if (file.find('Point Coordinates')!=-1) and (file.startswith('~$')==False) and (file.endswith('.xlsx')==True):
                header = header_fetch(self.realp)
                header.pull(file)
                self.fields_dict.update({'pointcoordinates':header.fields})
            # use '2009'
            if (file.find(f'{findable_string}')!=-1) and(file.find('Dump Columns')!=-1) and (file.startswith('~$')==False) and (file.endswith('.xlsx')==True):
                for table in self.tablelist:
                    header = header_fetch(self.realp)
                    header.pull(file, table)
                    self.fields_dict.update({f'{table}':header.fields})

    def append_fields(self, findable_string):
        """
        use findable string in directory that holds the

        """

        for file in os.listdir(self.realp):
            if (file.find('Coordinates')!=-1) and (file.endswith('.xlsx')==False) and (file.endswith('.zip')==False):
                for item in os.listdir(os.path.join(self.realp,file)):
                    if item.find('pointcoordinates')!=-1:
                        tempdf =pd.read_csv(os.path.join(self.realp,file,item), sep='|', index_col=False, names=self.fields_dict['pointcoordinates'] )

                        t = type_lookup(tempdf, os.path.splitext(item)[0], 2, self.path)
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
                        self.dfs.update({'coordinates':tempdf})
            # use range2011
            if (file.find(f'{findable_string}')!=-1) and (file.endswith('.xlsx')==False) and ('PointCoordinates' not in file) and (file.endswith('.zip')==False):
                for item in os.listdir(os.path.join(self.realp, file)):
                    if os.path.splitext(item)[0].upper() in self.tablelist:
                        tempdf = pd.read_csv(os.path.join(self.realp,file,item), sep='|', index_col=False,low_memory=False, names=self.fields_dict[os.path.splitext(item)[0].upper()])
                        # for all fields, if a field is numeric in lookup AND (not np.float or np.int), strip whitespace!
                        # after stripping, if numeric and not in "stay_in_varchar" list, convert to pandas numeric!
                        # empty spaces should automatically change into np.nan / compatible null values

                        for field in tempdf.columns:
                            stay_in_varchar = ['STATE', 'COUNTY']

                            t = type_lookup(tempdf, os.path.splitext(item)[0], 2, self.path)
                            if (t.list[field]=="numeric") and (tempdf[field].dtype!=np.float64) and (tempdf[field].dtype!=np.int64):
                                tempdf[field] = tempdf[field].apply(lambda i: i.strip())

                            if t.list[field]=="numeric" and field not in stay_in_varchar:
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

                        if 'point' in item:
                            # adding landuse from points table to coords

                            point_slice = tempdf[['LANDUSE', 'PrimaryKey']].copy(deep=True)
                            point_slice
                            coords_dup = pd.concat([self.temp_coords,point_slice], axis=1, join="inner")
                            coords_full = coords_dup.loc[:,~coords_dup.columns.duplicated()]

                            self.dfs.update({'pointcoordinates': coords_full})


                        self.dfs.update({f'{os.path.splitext(item)[0]}':tempdf})




os.listdir(secondp)
f = first_round(secondp,'range2011-2016' )
f.extract_fields('2009')
f.append_fields('2011')
f.dfs
f.fields_dict.keys()


# path=os.environ['NRIDAT']
# dirs = os.listdir(path)
# secondp = os.path.join(path,dirs[1])
#
# ##### setting it up
# fdict = {}
# dfs = {}
# bsnm = os.path.basename(secondp).replace(' ','')
#
# realpath = os.path.join(secondp,'Raw data dump')
# hfetch = header_fetch(realpath)
#
# ## fieldnames
# tablelist =
# tablelist =['CONCERN','COUNTYNM','DISTURBANCE','ESFSG','GINTERCEPT', 'GPS','PASTUREHEIGHTS','PINTERCEPT','PLANTCENSUS','POINT','PRACTICE','PRODUCTION','PTNOTE','RANGEHEALTH', 'SOILDISAG','SOILHORIZON','STATENM']
#
# for file in os.listdir(realpath):
#     if (file.find('Point Coordinates')!=-1) and (file.startswith('~$')==False) and (file.endswith('.xlsx')==True):
#         hfetch.pull(file)
#         fdict.update({'pointcoordinates':hfetch.fields})
#
#     if (file.find('2009')!=-1) and(file.find('Dump Columns')!=-1) and (file.startswith('~$')==False) and (file.endswith('.xlsx')==True):
#         for table in tablelist:
#             hfetch.pull(file, table)
#             fdict.update({f'{table}':hfetch.fields})
#


# coords = None
### joining back
# for file in os.listdir(realpath):
#     if (file.find('Coordinates')!=-1) and (file.endswith('.xlsx')==False) and (file.endswith('.zip')==False):
#         for item in os.listdir(os.path.join(realpath,file)):
#             if item.find('pointcoordinates')!=-1:
#                 ## opportunity to modify df
#
#                 tempdf =pd.read_csv(os.path.join(realpath,file,item), sep='|', index_col=False, names=fdict['pointcoordinates'] )
#
#                 # fixing coordinates for example
#                 # tempdf['TARGET_LONGITUDE'] = tempdf['TARGET_LONGITUDE'].map(lambda i: i*(-1))
#                 # tempdf['FIELD_LONGITUDE'] = tempdf['FIELD_LONGITUDE'].apply(lambda i: '-'+i if '          ' not in i else i)
#                 # tempdf['data_source'] = bsnm
#                 t = type_lookup(tempdf, os.path.splitext(item)[0], 2, secondp)
#                 fix_longitudes = ['TARGET_LONGITUDE','FIELD_LONGITUDE']
#                 for field in tempdf.columns:
#                     if (t.list[field]=="numeric") and (tempdf[field].dtype!=np.float64) and (tempdf[field].dtype!=np.int64):
#                         tempdf[field] = tempdf[field].apply(lambda i: i.strip())
#                         tempdf[field] = pd.to_numeric(tempdf[field])
#
#                     if field in fix_longitudes:
#                         tempdf[field] = tempdf[field].map(lambda i: i*(-1))
#
#                 less_fields = ['statenm','countynm']
#                 if os.path.splitext(item)[0] not in less_fields:
#                     # print(item)
#                     dbkey_gen(tempdf, 'PrimaryKey', 'SURVEY', 'STATE', 'COUNTY','PSU','POINT')
#                     dbkey_gen(tempdf, 'FIPSPSUPNT', 'STATE', 'COUNTY','PSU','POINT')
#                 if 'COUNTY' in tempdf.columns:
#                     tempdf['COUNTY'] = tempdf['COUNTY'].map(lambda x: f'{x:0>3}')
#
#                 if 'STATE' in tempdf.columns:
#                     tempdf['STATE'] = tempdf['STATE'].map(lambda x: f'{x:0>2}')
#                 tempdf['DBKey'] = ''.join(['NRI_',f'{date.today().year}'])
#
#
#                 # tempdf['data_source'] = bsnm
#                 coords = tempdf.copy(deep=True)
#                 # store
#                 dfs.update({'coordinates':tempdf})
#
#     if (file.find('range2011')!=-1) and (file.endswith('.xlsx')==False) and ('PointCoordinates' not in file) and (file.endswith('.zip')==False):
#         for item in os.listdir(os.path.join(realpath, file)):
#             if os.path.splitext(item)[0].upper() in tablelist:
#                 tempdf = pd.read_csv(os.path.join(realpath,file,item), sep='|', index_col=False,low_memory=False, names=fdict[os.path.splitext(item)[0].upper()])
#                 # for all fields, if a field is numeric in lookup AND (not np.float or np.int), strip whitespace!
#                 # after stripping, if numeric and not in "stay_in_varchar" list, convert to pandas numeric!
#                 # empty spaces should automatically change into np.nan / compatible null values
#
#                 for field in tempdf.columns:
#                     stay_in_varchar = ['STATE', 'COUNTY']
#
#                     t = type_lookup(tempdf, os.path.splitext(item)[0], 1, secondp)
#                     if (t.list[field]=="numeric") and (tempdf[field].dtype!=np.float64) and (tempdf[field].dtype!=np.int64):
#                         tempdf[field] = tempdf[field].apply(lambda i: i.strip())
#
#                     if t.list[field]=="numeric" and field not in stay_in_varchar:
#                         tempdf[field] = pd.to_numeric(tempdf[field])
#
#                 # for all tables not in "less_fields" list, create two new fields
#                 less_fields = ['statenm','countynm']
#                 if os.path.splitext(item)[0] not in less_fields:
#                     # print(item)
#                     dbkey_gen(tempdf, 'PrimaryKey', 'SURVEY', 'STATE', 'COUNTY','PSU','POINT')
#                     dbkey_gen(tempdf, 'FIPSPSUPNT', 'STATE', 'COUNTY','PSU','POINT')
#
#                 # if table has field 'COUNTY', fill with leading zeroes
#                 if 'COUNTY' in tempdf.columns:
#                     tempdf['COUNTY'] = tempdf['COUNTY'].map(lambda x: f'{x:0>3}')
#                 # if table has field 'STATE', fill with leading zeroes
#                 if 'STATE' in tempdf.columns:
#                     tempdf['STATE'] = tempdf['STATE'].map(lambda x: f'{x:0>2}')
#                 # create simple dbkey field
#                 tempdf['DBKey'] = ''.join(['NRI_',f'{date.today().year}'])
#
#                 if 'point' in item:
#                     # adding landuse from points table to coords
#
#                     point_slice = tempdf[['LANDUSE', 'PrimaryKey']].copy(deep=True)
#                     point_slice
#                     coords_dup = pd.concat([coords,point_slice], axis=1, join="inner")
#                     coords_full = coords_dup.loc[:,~coords_dup.columns.duplicated()]
#
#                     dfs.update({'pointcoordinates': coords_full})
#
#
#                 dfs.update({f'{os.path.splitext(item)[0]}':tempdf})
