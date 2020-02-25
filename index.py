import os
import os.path
import pandas as pd

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
            full = pd.read_excel(os.path.join(firstp,file))
            is_concern = full['Table name']==f'{col}'
            temphead = full[is_concern]
            for i in temphead['Field name']:
                self.fields.append(i)
        else:
            print('file is not in supplied directory')


##### setting it up
fdict = {}
dfs = {}

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
                dfs.update({'coordinates':pd.read_csv(os.path.join(firstp,file,item), sep='|', index_col=False, names=fdict['coordinates'] )})
                header=False
    if (file.find('RangeChange2004')!=-1) and (file.endswith('.xlsx')==False):
        for item in os.listdir(os.path.join(firstp, file)):
            if os.path.splitext(item)[0].upper() in tablelist:
                dfs.update({f'{os.path.splitext(item)[0]}':pd.read_csv(os.path.join(firstp,file,item), sep='|', index_col=False,low_memory=False, names=fdict[os.path.splitext(item)[0].upper()])})
