from setuptools import setup, find_packages
from distutils.util import convert_path


meta = {}
meta_path = convert_path('doppel/__meta__.py')
with open(meta_path) as meta_file:
    exec(meta_file.read(), meta)


def parse_requirements(file):
    with open(file) as fp:
        _requires = fp.read()
    return [e.strip() for e in _requires.split('\n') if len(e)]


setup(
    name=meta['__name__'],
    packages=find_packages(exclude=["tests"]),
    version=meta['__version__'],
    author=meta['__author__'],
    license=meta['__license__'],
    classifiers=meta['__classifiers__'],
    setup_requires=['setuptools', 'wheel'],
    install_requires=parse_requirements('requirements.txt'),
    package_data={
        'doppel.aws': ['awslogs/*.conf']
    }
)