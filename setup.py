from setuptools import setup

from pipenv.project import Project
from pipenv.utils import convert_deps_to_pip

pfile = Project(chdir=False).parsed_pipfile
requirements = convert_deps_to_pip(pfile['packages'], r=False)

setup(
    name='sqlchecker',
    version='0.0.1',
    install_requires=requirements,
    py_modules=['sqlchecker']
)
