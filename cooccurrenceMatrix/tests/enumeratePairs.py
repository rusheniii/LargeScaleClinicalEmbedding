import pandas as pd
import sys
from tqdm import tqdm
from collections import Counter
df = pd.read_parquet(sys.argv[1])
df_other = pd.read_parquet(sys.argv[2])
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
        if code < ncode: matrix[(code,ncode)]+=1
        else: matrix[(ncode, code)]+=1
    bar.update(1)


errors = 0
for _,i,j,count in df_other.itertuples():
    if matrix[(i,j)]!=count:
        print("matrix[(%s,%s)] %s!=%s"%(i,j,count,matrix[(i,j)]))
        errors+=1
print(errors)
