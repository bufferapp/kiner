# !/usr/bin/env python

from setuptools import setup

setup(
    name='kiner',
    packages=['kiner'],
    version='0.6.2',
    description='Python AWS Kinesis Producer',
    author='David Gasquez',
    license='MIT',
    author_email='davidgasquez@buffer.com',
    url='https://github.com/bufferapp/kiner',
    keywords=['kinesis', 'producer', 'aws'],
    install_requires=['boto3']
)
