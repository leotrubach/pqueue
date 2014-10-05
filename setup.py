# -*- coding=utf-8
import os
from setuptools import setup

with open(os.path.join(os.path.dirname(__file__), 'README.md')) as readme:
    README = readme.read()

# allow setup.py to be run from any path
os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='pqueue',
    version='0.1',
    packages=['pqueue'],
    install_requires=['six', 'nose'],
    include_package_data=True,
    license='BSD License',  # example license
    description='Robust file-based persistent queue in python',
    long_description=README,
    url='http://www.example.com/',
    author='Leo Trubach',
    author_email='leotrubach@gmail.com'
)