import random
import numpy as np
import pandas as pd
data = []
random.seed(42)
for i in range(1,100):
    for j in range(1,100):
        if random.random() < .15: 
            data.append([np.int32(i),np.int32(j),np.int64(i+j)])
df = pd.DataFrame(data,columns=["i","j","count"]).astype({"i":np.int32,"j":np.int32,"count":np.int64})
print(df.dtypes)
df.to_parquet("test.parquet")
