import os
import versioneer

from distutils.core import setup
from setuptools import find_packages
from distutils.extension import Extension
from distutils.command.sdist import sdist as _sdist


USE_CYTHON = True if 'USE_CYTHON'in os.environ else False


# Make sure the compiled Cython files in the distribution are up-to-date
class sdist(_sdist):
    def run(self):
        from Cython.Build import cythonize
        cythonize(['cyhdfs3/*.pyx'])
        _sdist.run(self)


source = "cyhdfs3/cyhdfs3" + (".pyx" if USE_CYTHON else ".c")
ext_modules = [
    Extension(name="cyhdfs3.cyhdfs3",
              sources=[source],
              include_dirs=["/usr/local/include/hdfs"],
              library_dirs=["/usr/local/lib/"],
              libraries=["hdfs3"],
            )
]

if USE_CYTHON:
    from Cython.Build import cythonize
    ext_modules = cythonize(ext_modules)

with open('requirements.txt') as f:
    required = f.read().splitlines()

cmdclass = versioneer.get_cmdclass()
cmdclass['sdist'] = sdist

setup(
    name="cyhdfs3",
    version=versioneer.get_version(),
    cmdclass=cmdclass,
    license='Apache License Version 2.0, January 2004',
    install_requires=required,
    packages=find_packages(),
    ext_modules=ext_modules,
    entry_points='''
        [console_scripts]
        hdfs3=cyhdfs3.cli:main
    ''',
)
