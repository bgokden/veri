from setuptools import setup, find_packages

from os import path
from io import open

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(name='veriservice',
      version='0.0.1',
      description='Python client for Veri',
      long_description=long_description,
      long_description_content_type='text/markdown',
      url='http://github.com/veri/client/python',
      author='Berk Gokden',
      author_email='berkgokden@gmail.com',
      license='MIT',
      keywords='veri service python client',
      packages=find_packages(exclude=['tests*']),
      install_requires=['grpcio-tools','googleapis-common-protos'])
