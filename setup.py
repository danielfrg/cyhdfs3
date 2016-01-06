from distutils.core import setup
from setuptools import find_packages
from distutils.extension import Extension

import versioneer

ext_modules = [
    Extension(name="cyhdfs3.cyhdfs3",
              sources=["cyhdfs3/cyhdfs3.c"],
              include_dirs=["/usr/local/include/hdfs"],
              library_dirs=["/usr/local/lib/"],
              libraries=["hdfs3"],
            )
]

cmdclass = versioneer.get_cmdclass()

setup(
    name="cyhdfs3",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    license='Apache License Version 2.0, January 2004',
    packages=find_packages(),
    ext_modules=ext_modules,
    entry_points='''
        [console_scripts]
        hdfs3=cyhdfs3.cli:main
    '''
)
