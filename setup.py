from setuptools import setup, find_packages


def parse_requirements(file):
    with open(file) as fp:
        _requires = fp.read()
    return [e.strip() for e in _requires.split('\n') if len(e)]


setup(
    name='aws-doppel',
    packages=find_packages(exclude=["tests"]),
    version='0.0.1',
    author='Fabien Vavrand',
    license='MIT',
    classifiers=[
        'Development Status :: 4 - Beta',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python'
        'Programming Language :: Python :: 3.7'
    ],
    setup_requires=['setuptools', 'wheel'],
    install_requires=parse_requirements('requirements.txt'),
    package_data={
        'doppel.aws': ['awslogs/*.conf']
    }
)