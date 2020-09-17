from setuptools import setup, find_packages
from distutils.util import convert_path


packages = find_packages()
module = packages[0]


meta_ns = {}
meta_path = convert_path(module + '/__meta__.py')
with open(meta_path) as meta_file:
    exec(meta_file.read(), meta_ns)


def parse_requirements(file):
    with open(file) as fp:
        _requires = fp.read()
    return [e.strip() for e in _requires.split('\n') if len(e)]


setup(
    name=meta_ns['__name__'],
    version=meta_ns['__version__'],
    author=meta_ns['__author__'],
    license=meta_ns['__license__'],
    classifiers=meta_ns['__classifiers__'],
    setup_requires=['setuptools', 'wheel'],
    install_requires=parse_requirements('requirements.txt')
)