import os
from setuptools import setup, find_packages
from multiflow.version import __version__


def read_content(filename):
    with open(os.path.join(os.path.dirname(__file__), filename)) as f:
        content = f.read()

    return content


setup(
    name='multiflow',
    version=__version__,
    description='A Python multithreading library for data processing pipelines, data streaming, etc.',
    long_description=read_content('README.md'),
    long_description_content_type='text/markdown',
    author='Nathaniel Young',
    author_email='',
    maintainer='Nathaniel Young',
    maintainer_email='',
    url='https://github.com/nyoungstudios/multiflow',
    license='MIT',
    packages=find_packages(exclude=('tests',)),
    classifiers=(
        'Intended Audience :: Developers',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Topic :: Scientific/Engineering',
        'Topic :: System :: Distributed Computing',
        'Topic :: Utilities'
    ),
    keywords='multiflow multithreading data processing streaming concurrent futures python'
)
