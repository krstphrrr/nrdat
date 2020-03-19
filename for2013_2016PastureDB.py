import os
import os.path
import pandas as pd
from index import header_fetch
from for2011_2016 import first_round
from index import pg_send, ret_access

## what about indicators
## getting in the first file dir
path=os.environ['NRIDAT']
dirs = os.listdir(path)
thirdp = os.path.join(path,dirs[2])
accesspath = os.path.join(thirdp,'Raw data dump', 'target_mdb.accdb')

t = first_round(thirdp,'range2011-2016')
t.extract_fields('2009')
# t.fields_dict
t.append_fields('pasture2013')
t.dfs['countynm']
for table in t.dfs.keys():
    if 'pintercept' in table:
        pass
    else:
        pg_send(thirdp,accesspath, t.dfs, table, access=True, pg=False)

pg_send(thirdp, accesspath, t.dfs, 'pintercept', access=True, pg=False)
