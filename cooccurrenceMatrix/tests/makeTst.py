

import random
random.seed(100)
for i in range(1,100):
    day=1
    for j in range(1,100):
        if random.random() < .15: day+=1
        print("%s,%s,%s,1"%(i,day,random.randint(1000,1000000)))
