#!/bin/bash
set -ex

. manifest.sh

RECOMPILE_FDB=true

if [ -d third_party/forestdb ]; then
    set +e
    LAST_COMPILED_COMMIT=$(cat ./third_party/forestdb/build/last_compiled_commit)
    set -e
    git submodule update
    pushd third_party/forestdb
    if [ ${LAST_COMPILED_COMMIT} == ${FORESTDB_COMMIT} ]; then
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
    cmake ../
    make static_lib $1    
    cd ..
    echo $(git rev-parse HEAD) > ./build/last_compiled_commit
fi
popd
