To upload a new version:
1. git tag a new version: git tag v1.x.x
2. Cythonize and package: python setup.py sdist
3. `twine upload dist/*`
