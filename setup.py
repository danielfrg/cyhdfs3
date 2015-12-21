from distutils.core import setup
from distutils.extension import Extension

import versioneer
from Cython.Build import cythonize

ext = Extension(name="chdfs",
                sources=["libhdfs3py/chdfs.pyx"],
                include_dirs=["/usr/local/include/hdfs"],
                library_dirs=["/usr/local/lib/"],
                libraries=["hdfs3"],
                )

setup(
    name="libhdfs3",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    ext_modules=cythonize(ext),
)
