cd /tmp/libhdfs3

mkdir build
pushd build
../bootstrap
make
make install
popd
