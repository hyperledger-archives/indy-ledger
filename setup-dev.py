#!/usr/bin/env python

from setuptools import setup

setup(name='Ledger-dev',
      version='0.0.2',
      description='Immutable Ledger python library',
      url='https://github.com/evernym/ledger-priv',
      install_requires=['pymongo', ],
      setup_requires=['pytest-runner', ],
      tests_require=['pytest', ],
      packages=['ledger', ]
      )
