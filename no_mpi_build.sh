mkdir build
cd build
wget https://github.com/petsc/petsc/archive/v3.12.5.tar.gz
tar -xvf v3.12.5.tar.gz
cd petsc-3.12.5
export CFLAGS="-isystem $CONDA_PREFIX/include -O2 -fPIC"
export FFLAGS="-isystem $CONDA_PREFIX/include -O2 -fPIC"
./configure CFLAGS="$CFLAGS" FFLAGS="$FFLAGS" LDFLAGS="$LDFLAGS" CXX=$CXX CC=$GCC FC=$FC --with-mpi=0 --with-openmp=1 --with-openblas-dir="$CONDA_PREFIX" --prefix="$CONDA_PREFIX" --with-x=0 --download-sowing-cc=$GCC --download-sowing-cxx=$CXX --with-64-bit-indices=1
LOCAL_PETSC_DIR="`cat configure.log | grep 'PETSC_DIR:' | cut -d':' -f 2 | xargs`"
LOCAL_PETSC_ARCH="`cat configure.log | grep 'PETSC_ARCH:' | cut -d':' -f 2 | xargs`"
make PETSC_DIR=$LOCAL_PETSC_DIR PETSC_ARCH=$LOCAL_PETSC_ARCH all
make PETSC_DIR=$LOCAL_PETSC_DIR PETSC_ARCH=$LOCAL_PETSC_ARCH install
cd ..
export PETSC_DIR="$CONDA_PREFIX"
export PETSC_ARCH=""
wget https://slepc.upv.es/download/distrib/slepc-3.12.2.tar.gz
tar -xvf slepc-3.12.2.tar.gz
cd slepc-3.12.2
./configure --prefix="$CONDA_PREFIX"
LOCAL_SLEPC_DIR=${PWD}
make SLEPC_DIR=$LOCAL_SLEPC_DIR PETSC_DIR=$PETSC_DIR
make SLEPC_DIR=$LOCAL_SLEPC_DIR PETSC_DIR=$PETSC_DIR install
