import sys 
import slepc4py
from scipy.sparse import csr_matrix
from scipy.spatial.distance import cosine
slepc4py.init(sys.argv)
import numpy as np
from petsc4py import PETSc 
from slepc4py import SLEPc  
import pickle


ascii_viewer = PETSc.Viewer().createBinary(sys.argv[-2], "r")
U = PETSc.Mat().load(ascii_viewer)

ascii_viewer = PETSc.Viewer().createBinary(sys.argv[-1], "r")
U_cpp = PETSc.Mat().load(ascii_viewer)

indptr,indices,data = U.getValuesCSR()
m1 = csr_matrix( (data,indices,indptr) ).todense()
indptr,indices,data = U_cpp.getValuesCSR()
m2 = csr_matrix( (data,indices,indptr) ).todense()

n,m = m1.shape
assert m1.shape==m2.shape
vectors1 = []
vectors2 = []
for i in range(m):
    c1i =[j for sub in m1[:,i].tolist() for j in sub]
    c2i =[j for sub in m2[:,i].tolist() for j in sub]
    vectors1.append(  c1i )
    vectors2.append(  c2i )
    #print(vectors1[-1])
print(len(vectors1),len(vectors2))

vectors1 = sorted(vectors1)
vectors2 = sorted(vectors2)
fails=0
for i in range(n):
    v1 = vectors1[i]
    v2 = vectors2[i]
    #print(v1,v2)
    theta = cosine(v1,v2)
    if theta > .01:
        fails+=1
        print(i,theta)
print("NUMBER FAILS", fails)

