#!/usr/bin/env python

from setuptools import setup

setup(name='Ledger',
      version='0.0.1',
      description='Immutable Ledger python library',
      url='https://github.com/evernym/ledger',
      install_requires=['pymongo', ],
      setup_requires=['pytest-runner', ],
      tests_require=['pytest', ],
      packages=['ledger', ],

      )
