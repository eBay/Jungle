#!/bin/bash
set -ex

. manifest.sh

RECOMPILE_FDB=true

if [ -d third_party/forestdb ]; then
    git submodule update
    pushd third_party/forestdb
    if [ $(git rev-parse HEAD) == ${FORESTDB_COMMIT} ]; then
        RECOMPILE_FDB=false
    fi
    if [ ${FORCE_COMPILE_DEPENDENCIES} == true ]; then
        RECOMPILE_FDB=true
    fi
    popd
fi

if [ ! -f third_party/forestdb/CMakeLists.txt ]; then
    git submodule update --init
fi

pushd third_party/forestdb/
if [ ${RECOMPILE_FDB} == true ]; then
    git pull origin master
    git reset --hard ${FORESTDB_COMMIT}
    rm -rf ./build
    mkdir build
    cd build
    cmake -DSNAPPY_OPTION=Disable ../
    make static_lib $1
    cd ..
fi
popd
