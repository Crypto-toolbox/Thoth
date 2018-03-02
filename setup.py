from setuptools import setup, find_packages

VERSION = '1.0'

setup(name='thoth',
      version=VERSION,
      description='Crypto Exchange Websocket Framework',
      author='Nils Diefenbach',
      author_email='23okrs20+gitlab@mykolab.com',
      packages=find_packages(exclude=['contrib', 'docs', 'tests']),
      test_suite='nose.collector', tests_require=['nose', 'pytest', 'cython'],
      classifiers=['Development Status :: 3 - Alpha',
                   'Intended Audience :: Financial and Insurance Industry',
                   'License :: Other/Proprietary License',
                   'Operating System :: POSIX :: Linux',
                   'Programming Language :: Python :: 3 :: Only',
                   'Topic :: Office/Business :: Financial :: Investment'],
      install_requires=['hermes-zmq', 'websocket-client', 'requests', 'autobahn', 'pysher'],
      package_data={'': ['*.md', '*.rst']})
