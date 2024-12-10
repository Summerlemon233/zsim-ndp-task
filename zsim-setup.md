# ZSim Setup

## File Structure
```
|- NDP-PROJECT
    |- zsim/
    |- zsim-env/
        |- bin
        |- include
        |- lib
        |- share
        |- pin-3.11
        |- zsim3-thirdparty-src
            |- libpin3c_missing
            |- pincrt_build
                |- hdf5
                |- libconfig
                |- ...
```
From now on, we assume we are at the `NDP_PROJECT/` folder, which is referred to by `$` in the following steps. The actual name and path of `NDP_PROJECT/` can be specified arbitrarily.

## Steps

### Install Dependencies of ZSim
- `mkdir zsim-env && cd zsim-env`
- Install pin-3 (We use pin-3.28)
    - `wget https://software.intel.com/sites/landingpage/pintool/downloads/pin-3.28-98749-g6643ecee5-gcc-linux.tar.gz`
    - `tar -xvf pin-3.28-98749-g6643ecee5-gcc-linux.tar.gz && mv pin-3.28-98749-g6643ecee5-gcc-linux pin-3.28` 
    - Updata $PINPATH in .bashrc (remember to update it if you change the path to pin)
        - ` echo "export PINPATH=$(pwd)/pin-3.28" >> ~/.bashrc`
        - `source ~/.bashrc`
- Clone `pincrt_build`
    - `git clone --recursive git@github.com:tsinghua-ideal/pincrt_build.git`
<!-- - Copy `gcc_scripts` to `zsim-env`
    - `cp -r pincrt_build/gcc_scripts gcc_scripts` -->
<!-- - Clone and Install `libpin3c_missing`
    - `git clone git@github.com:tsinghua-ideal/libpin3c_missing.git && cd libpin3c_missing`
    - `make && make install` (`libpin3c_missing.a` will be installed in `$/lib`)
    - `cd ../` -->
- Install PinCRT
    - `cd pincrt_build`
    - **libpincrtpatch**:
        - `cd libpincrtpatch`
        - `make && make install`
        - Now `libpincrtpatch.so` is installed in `$/zsim-env/lib`
        - `cd ../`
    - **hdf5**:
        - `cd hdf5`
        - get the original hdf5 code
            - `wget https://support.hdfgroup.org/ftp/HDF5/releases/hdf5-1.12/hdf5-1.12.0/src/hdf5-1.12.0.tar.gz`
            <!-- - `tar -xvf hdf5-1.10.5.tar.gz && mv hdf5-1.10.5/* ./ && rm -rf hdf5-1.10.5/` -->
            - `tar -xvf hdf5-1.12.0.tar.gz && cd hdf5-1.12.0`
        - `../run_configure.sh`
        - `make -j16 && make install`
        - `cd ../../`
    - **libconfig**:
        - `cd libconfig`
        - get the original libconfig code
            - `wget https://hyperrealm.github.io/libconfig/dist/libconfig-1.7.3.tar.gz`
            - `tar -xvf libconfig-1.7.3.tar.gz && cd libconfig-1.7.3`
        - `../run_configure.sh`
        - `make -j8 && make install`
        - `cd ../../`
    - **zlib**:
        - `cd zlib`
        - get the original zlib code
            - `wget https://zlib.net/fossils/zlib-1.2.13.tar.gz`
            - `tar -xvf zlib-1.2.13.tar.gz && cd zlib-1.2.13`
        - `../run_configure.sh`
        - `make && make install`
        - `cd ../../`
    - **mbedtls**:
        - `cd mbedtls`
        - get the original mbedtls code
            - `wget https://github.com/Mbed-TLS/mbedtls/releases/download/mbedtls-3.6.2/mbedtls-3.6.2.tar.bz2`
            - `tar -jxvf mbedtls-3.6.2.tar.bz2 && cd mbedtls-3.6.2`
        - `../run_make.sh -j16`
        - `make install`
        - `cd ../../`
    - Update environment variables:
        - `cd ../` (now at `$/zsim-env/`)
        - `echo "export PINCRTPATCHPATH=$(pwd)/lib" >> ~/.bashrc`
        - `echo "export LIBCONFIGPATH=$(pwd)/" >> ~/.bashrc`
        - `echo "export HDF5PATH=$(pwd)/" >> ~/.bashrc`
        - `echo "export MBEDTLSPATH=$(pwd)/" >> ~/.bashrc`
        - `source ~/.bashrc`



### Clone and Compile ZSim
- `git clone git@github.com:CriusT/zsim.git`
- `cd zsim`
<!-- - `git checkout -b ndp origin/ndp` -->
- `scons -j16`

### Running ZSim
#### Simple Demo
- `cd zsim && ./build/opt/zsim ./test/simple.cfg`
#### DDR Bank NDP
- `cd zsim`
- generate patchRoot
    - `mkdir myPatchRoot`
    - `python3 ./misc/patchRoot/gen_hetero_patch_root.py /zsim-ndp/zsim-ndp-master/myPatchRoot/128c128n --bn 128 --bc 128`
    - `python3 ./misc/patchRoot/gen_hybridndp_patch_root.py /zsim-ndp/zsim-ndp-master/myPatchRoot/16c64n --bc 16 --bn 16 --sn 48`
    - `python3 ./misc/patchRoot/gen_hybridndp_patch_root.py /zsim-ndp/zsim-ndp-master/myPatchRoot/64c128n --bc 64 --bn 64 --sn 64`
    python /zsim-ndp/zsim-ndp-master/misc/patchRoot/gen_hybridndp_patch_root.py /zsim-ndp/zsim-ndp-master/myPatchRoot/2c4n --bc 2 --bn 2 --sn 2
    python3 ./misc/patchRoot/gen_hetero_patch_root.py /zsim-ndp/zsim-ndp-task/myPatchRoots/patchRoot-64 --bc 64 --bn 64
    python3 ./misc/patchRoot/gen_hetero_patch_root.py /zsim-ndp/zsim-ndp-task/myPatchRoots/patchRoot-128 --bn 128 --bc 128
- run zsim
    - modify the `process0.patchRoot` path in `./test/test_ndp1.cfg` to the path of `./myPatchRoot/128c/128n`
    - `/zsim-ndp/zsim-ndp-master/build/opt/zsim /zsim-ndp/zsim-ndp-master/tests/test_ndp1.cfg`
    - `/zsim-ndp/zsim-ndp-task/build/opt/zsim /zsim-ndp/zsim-ndp-task/tests/test_ndp_64.cfg`
    
      `./build/opt/zsim ./tests/test_ndp2.cfg`
    - `./build/opt/zsim ./tests/numa_libnuma.cfg`
    - `./build/opt/zsim ./tests/numa_syscall.cfg`


<!-- ## Possible Errors and Solutions
- **Error**: Undefined reference to `__addvdi3`: 
    - **Solution**: wrong version of hdf5. should use hdf5 1.10.5.
- **Tip**: Do not include <bits/signum.h> directly; use <signal.h> instead.
- **Error**: `./build/opt/zsim`: error while loading shared libraries: libm-dynamic.so: cannot open shared object file: No such file or director.
    - **Solution**: `$/zsim/SConstruct`添加`env["LINKFLAGS"] = "-Wl,--no-as-needed"` -->
