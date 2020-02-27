import os
import os.path
import pandas as pd
from index import header_fetch


########### 2009-2016 or 2011-2016??
## what about indicators
## getting in the first file dir
path=os.environ['NRIDAT']
dirs = os.listdir(path)
secondp = os.path.join(path,dirs[1])

##### setting it up
fdict = {}
dfs = {}
bsnm = os.path.basename(secondp).replace(' ','')

realpath = os.path.join(secondp,'Raw data dump')
hfetch = header_fetch(realpath)

## fieldnames
tablelist =['CONCERN','COUNTYNM','DISTURBANCE','ESFSG','GINTERCEPT', 'GPS','PASTUREHEIGHTS','PINTERCEPT','PLANTCENSUS','POINT','PRACTICE','PRODUCTION','PTNOTE','RANGEHEALTH', 'SOILDISAG','SOILHORIZON','STATENM']
for file in os.listdir(realpath):
    if (file.find('Point Coordinates')!=-1) and (file.startswith('~$')==False) and (file.endswith('.xlsx')==True):
        hfetch.pull(file)
        fdict.update({'coordinates':hfetch.fields})

    if (file.find('2009')!=-1) and(file.find('Dump Columns')!=-1) and (file.startswith('~$')==False) and (file.endswith('.xlsx')==True):
        for table in tablelist:
            hfetch.pull(file, table)
            fdict.update({f'{table}':hfetch.fields})
### joining back
for file in os.listdir(realpath):
    if (file.find('Coordinates')!=-1) and (file.endswith('.xlsx')==False) and (file.endswith('.zip')==False):
        for item in os.listdir(os.path.join(realpath,file)):
            if item.find('pointcoordinates')!=-1:
                ## opportunity to modify df
                tempdf =pd.read_csv(os.path.join(realpath,file,item), sep='|', index_col=False, names=fdict['coordinates'] )
                # fixing coordinates for example
                tempdf['TARGET_LONGITUDE'] = tempdf['TARGET_LONGITUDE'].map(lambda i: i*(-1))
                tempdf['FIELD_LONGITUDE'] = tempdf['FIELD_LONGITUDE'].apply(lambda i: '-'+i if '          ' not in i else i)
                tempdf['data_source'] = bsnm
                # store
                dfs.update({'coordinates':tempdf})

    if (file.find('range2011')!=-1) and (file.endswith('.xlsx')==False) and ('PointCoordinates' not in file) and (file.endswith('.zip')==False):
        for item in os.listdir(os.path.join(realpath, file)):
            if os.path.splitext(item)[0].upper() in tablelist:
                tempdf = pd.read_csv(os.path.join(realpath,file,item), sep='|', index_col=False,low_memory=False, names=fdict[os.path.splitext(item)[0].upper()])
                tempdf['data_source'] = bsnm
                dfs.update({f'{os.path.splitext(item)[0]}':tempdf})
