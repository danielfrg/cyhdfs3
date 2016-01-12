import os
import sys
import versioneer

from distutils.core import setup
from setuptools import find_packages
from distutils.extension import Extension
from distutils.command.sdist import sdist as _sdist
from distutils.command.install import install as _install


try:
    import numpy as np
except:
    print("ERROR: Numpy not found, please install numpy")
    sys.exit(1)

USE_CYTHON = ("--cython" in sys.argv) or ("USE_CYTHON" in os.environ)
CYTHON_INSTALLED = False

try:
    import Cython
    CYTHON_INSTALLED = True
except:
    print("ERROR: Cython flag was given but cython was not found")
    sys.exit(1)


#
source_pyx = "cyhdfs3/cyhdfs3.pyx"
source_c = "cyhdfs3/cyhdfs3.c"
if not os.path.exists(source_c):
    if CYTHON_INSTALLED:
        print("Generated `.c` files not found will default to use cython")
        USE_CYTHON = True
    else:
        print("ERROR: Generated `.c` files not found and Cython not installed, please install cython")
        sys.exit(1)

if USE_CYTHON:
    source = source_pyx
else:
    source = source_c

if USE_CYTHON:
    from distutils.extension import Extension
    from Cython.Compiler.Options import directive_defaults
    directive_defaults["linetrace"] = True
    directive_defaults["embedsignature"] = True

    macros = [("CYTHON_TRACE", "1")]
else:
    macros = []

include_dirs = ["/usr/local/include", "/usr/local/include/hdfs"]
include_dirs.append(np.get_include())
library_dirs = ["/usr/local/lib/"]

# If conda PREFIX is present add conda paths
prefix = os.getenv("PREFIX", None)
if prefix is not None:
    include_dirs.append(os.path.join(prefix, "include"))
    include_dirs.append(os.path.join(prefix, "include", "hdfs"))
    library_dirs.append(os.path.join(prefix, "lib"))


ext_modules = [
    Extension(name="cyhdfs3.cyhdfs3",
                sources=[source],
                include_dirs=include_dirs,
                library_dirs=library_dirs,
                libraries=["hdfs3", "avro", "m", "snappy"],
                define_macros=macros
            )
]

# Versioneer class
cmdclass = versioneer.get_cmdclass()

# Cythonize on `sdist`: Always to make sure the compiled Cython files in the pkg are up-to-date
class sdist(_sdist):
    def run(self):
        from Cython.Build import cythonize
        cythonize(["cyhdfs3/*.pyx"])
        _sdist.run(self)
cmdclass["sdist"] = sdist

# Cythonize on `install`: If specified
class install(_install):
    def run(self):
        if USE_CYTHON:
            from Cython.Build import cythonize
            ext_modules = cythonize(ext_modules)
        _install.run(self)
cmdclass["install"] = install

with open("requirements.txt") as f:
    required = f.read().splitlines()

setup(
    name="cyhdfs3",
    version=versioneer.get_version(),
    author="Daniel Rodriguez",
    author_email="df.rodriguez143@gmail.com",
    url="https://github.com/danielfrg/cyhdfs3",
    cmdclass=cmdclass,
    license="Apache License Version 2.0, January 2004",
    install_requires=required,
    packages=find_packages(),
    ext_modules=ext_modules,
    entry_points="""
        [console_scripts]
        hdfs3=cyhdfs3.cli:main
    """,
)
