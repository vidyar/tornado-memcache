#!/usr/bin/env python
# -*- mode: python; coding: utf-8 -*-

from setuptools import setup, find_packages

from torncache import __version__

setup(
    name='torncache',
    version=__version__,
    author=['Charles Gordon', 'David P. Novakovic', 'Carlos Martín'],
    author_email='inean.es@gmail.com',
    packages=find_packages(),
    tests_require=['tornado', 'unittest2'],
    install_requires=['tornado'],
    test_suite='unittest2.collector',
    description='Async driver for memcache and tornado.',
    long_description=open('README.md').read(),
    license='Apache License 2.0',
    url='https://github.com/inean/tornado-memcache',
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'License :: OSI Approved :: Apache Software License',
        'Topic :: Database',
    ],
)
