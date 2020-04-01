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
accesspath = os.path.join(secondp,'Raw data dump','target_mdb.accdb') # use if accesspath exists else pass
# ret_access(accesspath)
"""
########### 2009-2016 or 2011-2016??

"""
#
# f = first_round(secondp,'range2011-2016' )
# f.extract_fields('2009')
#
# f.append_fields('2011')
# # # f.dfs

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
    tdf = None
    _dbkey = {
        'RangeChange2004-2008':1,
        'RangeChange2009-2015':2,
        'range2011-2016':3,
        'rangepasture2017_2018':4
    }

    """
    custom rools
    """
    set_2018 = None
    dbkey = None


    tablelist = []

    def __init__(self, path, dbkey):
        self.path = path
        self.mainp = os.path.dirname(os.path.dirname(path))
        self.realp = os.path.join(path,'Raw data dump')
        self.expl = os.listdir(self.mainp)[-1]
        self.df = pd.read_csv(os.path.join(self.mainp, self.expl))
        self.dbkeys = {key:value for (key,value) in enumerate([i for i in self.df['DBKey'].unique()])}
        self.tablelist = [i for i in self.df[self.df['DBKey']==f'{dbkey}']['TABLE.NAME'].unique()]
        if dbkey in self._dbkey.keys():
            self.dbkey = self._dbkey[dbkey]
        if 'rangepasture2017_2018' in dbkey:
            self.set_2018 = '.csv'
        else:
            self.set_2018 = '.xlsx'

    def extract_fields(self, findable_string):
        for file in os.listdir(self.realp):
            if (file.find('Point Coordinates')!=-1) and (file.startswith('~$')==False) and (file.endswith('.xlsx')==True):
                header = header_fetch(self.realp)
                header.pull(file)
                self.fields_dict.update({'pointcoordinates':header.fields})
            # use '2009'

            if (file.find(f'{findable_string}')!=-1) and(file.find('Dump Columns')!=-1) and (file.startswith('~$')==False) and (file.endswith(f'{self.set_2018}')==True):
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
                        # self.tdf = tempdf

                        t = type_lookup(tempdf, os.path.splitext(item)[0], self.dbkey, self.path)
                        fix_longitudes = ['TARGET_LONGITUDE','FIELD_LONGITUDE']
                        for field in tempdf.columns:
                            # print(field,t.list[field],tempdf[field].dtype, "no filter")
                            if (t.list[field]=="numeric") and (tempdf[field].dtype!=np.float64) and (tempdf[field].dtype!=np.int64):
                                # print(field, t.list[field], tempdf[field].dtype, "filtered")
                                tempdf[field] = tempdf[field].apply(lambda i: i.strip())
                                tempdf[field] = pd.to_numeric(tempdf[field])
                                # print(field, t.list[field], tempdf[field].dtype, "filtered pt 2")

                            if field in fix_longitudes:
                                tempdf[field] = tempdf[field].map(lambda i: i*(-1))
                                # print(field, t.list[field], tempdf[field].dtype, "filtered pt 3")


                        if 'COUNTY' in tempdf.columns:
                            tempdf['COUNTY'] = tempdf['COUNTY'].map(lambda x: f'{x:0>3}')

                        if 'STATE' in tempdf.columns:
                            tempdf['STATE'] = tempdf['STATE'].map(lambda x: f'{x:0>2}')

                        less_fields = ['statenm','countynm']
                        if os.path.splitext(item)[0] not in less_fields:

                            dbkey_gen(tempdf, 'PrimaryKey', 'SURVEY', 'STATE', 'COUNTY','PSU','POINT')
                            dbkey_gen(tempdf, 'FIPSPSUPNT', 'STATE', 'COUNTY','PSU','POINT')
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

                            dot_list = ['HIT1','HIT2','HIT3', 'HIT4', 'HIT5', 'HIT6', 'NONSOIL']
                            if field in dot_list:
                                tempdf[field] = tempdf[field].apply(lambda i: "" if ('.' in i) and (any([(j.isalpha()) or (j.isdigit()) for j in i])!=True) else i)

                            ##### STRIP ANYWAY
                            if tempdf[field].dtype==np.object:
                                tempdf[field] = tempdf[field].apply(lambda i: i.strip() if type(i)!=float else i)





                        # if table has field 'COUNTY', fill with leading zeroes
                        if 'COUNTY' in tempdf.columns:
                            tempdf['COUNTY'] = tempdf['COUNTY'].map(lambda x: f'{x:0>3}')
                        # if table has field 'STATE', fill with leading zeroes
                        if 'STATE' in tempdf.columns:
                            tempdf['STATE'] = tempdf['STATE'].map(lambda x: f'{x:0>2}')

                        # for all tables not in "less_fields" list, create two new fields
                        less_fields = ['statenm','countynm']
                        if os.path.splitext(item)[0] not in less_fields:
                            # print(item)
                            dbkey_gen(tempdf, 'PrimaryKey', 'SURVEY', 'STATE', 'COUNTY','PSU','POINT')
                            dbkey_gen(tempdf, 'FIPSPSUPNT', 'STATE', 'COUNTY','PSU','POINT')
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

                    else:
                        print(os.path.splitext(item)[0].upper())
                        ## if the tables are not in tablelist

# for table in f.dfs.keys():
#     if 'pintercept' in table:
#         pass
#     else:
#         pg_send(secondp,accesspath, f.dfs, table, access=True, pg=False)
# pg_send(secondp, accesspath, f.dfs, 'pintercept', access=True, pg=False)
