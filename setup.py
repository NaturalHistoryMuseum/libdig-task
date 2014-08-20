
import os
from setuptools import setup, find_packages

version = '0.1'

setup(
    name='libdig',
    version=version,
    description='Library digitisation import pipeline',
    author='Ben Scott',
    author_email='ben@benscott.co.uk',
    license='Apache License 2.0',
    packages=[
        'libdig',
    ]
)