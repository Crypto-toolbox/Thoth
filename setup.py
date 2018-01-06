from setuptools import setup

VERSION = '1.0'

setup(name='thoth',
      version=VERSION,
      description='Cryptocurrency Trading System Data Cluster Module',
      author='Nils Diefenbach',
      author_email='23okrs20+gitlab@mykolab.com',
      packages=['thoth', 'thoth/connectors'],
      test_suite='nose.collector', tests_require=['nose', 'cython'],
      classifiers=['Development Status :: 3 - Alpha',
                   'Intended Audience :: Financial and Insurance Industry',
                   'License :: Other/Proprietary License',
                   'Operating System :: POSIX :: Linux',
                   'Programming Language :: Python :: 3 :: Only',
                   'Topic :: Office/Business :: Financial :: Investment'],
      install_requires=['hermes-zmq', 'websocket-client', 'requests', 'autobahn', 'pysher'],
      package_data={'': ['*.md', '*.rst']})
