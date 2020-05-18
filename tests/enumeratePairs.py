import pandas as pd
import sys
from tqdm import tqdm
from collections import Counter
df = pd.read_parquet(sys.argv[1])

events =[]
for i,sid,numDays,code,_ in df.itertuples():
    events.append((sid,numDays,code))

window = 30
matrix = Counter()
bar = tqdm(total=len(events))
for i in range(len(events)-1):
    sid, day, code = events[i]
    for j in range(i+1, len(events)):
        nsid, nday,ncode = events[j]
        if sid != nsid: break
        if nday - day > window: break
        if ncode < code: matrix[(ncode,code)]+=1
        else: matrix[(code, ncode)]+=1
    bar.update(1)


for i in matrix:
    print("%s,%s,%s"%(i[0],i[1], matrix[i]))
