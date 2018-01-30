from distutils.core import setup

VERSION = '1.0'

setup(name='thoth',
      version=VERSION,
      description='Crypto Exchange Websocket Framework',
      author='Nils Diefenbach',
      author_email='23okrs20+gitlab@mykolab.com',
      packages=['thoth', 'thoth/connectors', 'thoth/core'],
      test_suite='nose.collector', tests_require=['nose', 'cython'],
      classifiers=['Development Status :: 3 - Alpha',
                   'Intended Audience :: Financial and Insurance Industry',
                   'License :: Other/Proprietary License',
                   'Operating System :: POSIX :: Linux',
                   'Programming Language :: Python :: 3 :: Only',
                   'Topic :: Office/Business :: Financial :: Investment'],
      install_requires=['hermes-zmq', 'pyzmq', 'requests', 'pysher'],
      package_data={'': ['*.md', '*.rst']})
