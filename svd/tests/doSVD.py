import sys 
import slepc4py
import numpy as np 
from scipy.sparse import csr_matrix
slepc4py.init(sys.argv)
matrixPath = sys.argv[-1]
print(matrixPath)

from petsc4py import PETSc 
from slepc4py import SLEPc 
from math import log2 
import pandas 
from collections import Counter 

alpha = 1 
df = pandas.read_parquet(matrixPath)

n=max(df["i"].max(),df["j"].max())+1

counts = Counter()
total =0
for row in df.itertuples():
    _,i,j,count = row 
    total+=count 
    counts[i] += count 
    #counts[j] += count 

def PMI(wcCount, totalPairs, wCount, cCount, alpha):
    return log2(wcCount * (totalPairs**alpha) / (wCount * (cCount**alpha)))
def PPMI(*args, **kwargs):
    return max(PMI(*args,**kwargs),0)

print("TOTAL PAIRS: ",total)
print("SUM(99,c') =",counts[99])
print("SUM(2,c') =",counts[2])
print("LOG = ", log2(101*(total**alpha)/(counts[99]*(counts[2]**alpha))))
data = []
rowIndex = []
columnIndex =[] 

for row in df.itertuples():
    _,i,j,wcCount = row 
    wCount = counts[i]
    cCount = counts[j]
    ppmi = PPMI(wcCount, total, wCount, cCount,alpha=alpha)
    data.append(ppmi)
    rowIndex.append(i)
    columnIndex.append(j)

csr = csr_matrix( (data, (rowIndex,columnIndex)), shape=(n,n))
print("Doing SVD")
print("PPMI[99,2]=",csr[99,2])
A= PETSc.Mat().createAIJ(size=(n,n), csr=(csr.indptr, csr.indices, csr.data))
A.assemble()

svd = SLEPc.SVD()
svd.create()

svd.setOperator(A)
svd.setType(svd.Type.TRLANCZOS)
svd.setDimensions(nsv=50,ncv=100)
svd.setFromOptions()
svd.solve()

iterations = svd.getIterationNumber()
nsv,ncv,mpd = svd.getDimensions()
nconv = svd.getConverged()
v,u = A.createVecs()

U=[]
V=[]
singularValues = []
for i in range(nconv):
    sigma = svd.getSingularTriplet(i,u,v)
    U.append(np.copy(u.getArray()))
    V.append(np.copy(v.getArray()))
    singularValues.append(sigma)

print(singularValues)
U_np = np.column_stack(U)
V_np = np.stack(V)

csrU = csr_matrix(U_np)
n,m = csrU.shape
U= PETSc.Mat().createAIJ(size=(n,m), csr=(csrU.indptr, csrU.indices, csrU.data))
U.assemble()

ascii_viewer = PETSc.Viewer().createBinary("U.test.petsc", "w")
U.view(ascii_viewer)
